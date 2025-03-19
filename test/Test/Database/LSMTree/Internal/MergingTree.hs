{-# OPTIONS_GHC -Wno-orphans #-}

module Test.Database.LSMTree.Internal.MergingTree (tests) where

import           Control.ActionRegistry
import           Control.Exception (bracket)
import           Control.Monad.Class.MonadAsync as Async
import           Control.RefCount
import           Data.Arena (newArenaManager)
import           Data.Coerce (coerce)
import           Data.Foldable (toList)
import           Data.List.NonEmpty (NonEmpty)
import           Data.Map (Map)
import qualified Data.Map as Map
import           Data.Traversable (for)
import qualified Data.Vector as V
import           Database.LSMTree.Extras.MergingRunData
import           Database.LSMTree.Extras.MergingTreeData
import           Database.LSMTree.Extras.RunData
import           Database.LSMTree.Internal.BlobRef
import           Database.LSMTree.Internal.Entry (Entry)
import qualified Database.LSMTree.Internal.Entry as Entry
import qualified Database.LSMTree.Internal.Index as Index
import qualified Database.LSMTree.Internal.Lookup as Lookup
import qualified Database.LSMTree.Internal.MergingRun as MR
import           Database.LSMTree.Internal.MergingTree
import           Database.LSMTree.Internal.MergingTree.Lookup
import qualified Database.LSMTree.Internal.Paths as Paths
import qualified Database.LSMTree.Internal.Run as Run
import qualified Database.LSMTree.Internal.RunAcc as RunAcc
import qualified Database.LSMTree.Internal.RunBuilder as RunBuilder
import           Database.LSMTree.Internal.Serialise
import           Database.LSMTree.Internal.UniqCounter
import qualified System.FS.API as FS
import qualified System.FS.BlockIO.API as FS
import qualified System.FS.Sim.MockFS as MockFS
import           Test.QuickCheck
import           Test.Tasty
import           Test.Tasty.QuickCheck
import           Test.Util.FS (propNoOpenHandles, withSimHasBlockIO)

tests :: TestTree
tests = testGroup "Test.Database.LSMTree.Internal.MergingTree"
    [ testProperty "prop_isStructurallyEmpty" prop_isStructurallyEmpty
    , testProperty "prop_lookupTree" $ \keys mtd ->
        ioProperty $
          withSimHasBlockIO propNoOpenHandles MockFS.empty $ \hfs hbio _ ->
            prop_lookupTree hfs hbio keys mtd
    , testProperty "prop_supplyCredits" $ \threshold credits mtd ->
        ioProperty $
          withSimHasBlockIO propNoOpenHandles MockFS.empty $ \hfs hbio _ ->
            prop_supplyCredits hfs hbio threshold credits mtd
    ]

runParams :: RunBuilder.RunParams
runParams =
    RunBuilder.RunParams {
      runParamCaching = RunBuilder.CacheRunData,
      runParamAlloc   = RunAcc.RunAllocFixed 10,
      runParamIndex   = Index.Ordinary
    }

-- | Check that the merging tree constructor functions preserve the property
-- that if the inputs are obviously empty, the output is also obviously empty.
--
prop_isStructurallyEmpty :: EmptyMergingTree -> Property
prop_isStructurallyEmpty emt =
    ioProperty $
      bracket (mkEmptyMergingTree emt)
              releaseRef
              isStructurallyEmpty

-- | An expression to specify the shape of an empty 'MergingTree'
--
data EmptyMergingTree = ObviouslyEmptyLevelMerge
                      | ObviouslyEmptyUnionMerge
                      | NonObviouslyEmptyLevelMerge EmptyMergingTree
                      | NonObviouslyEmptyUnionMerge [EmptyMergingTree]
  deriving stock (Eq, Show)

instance Arbitrary EmptyMergingTree where
    arbitrary =
      sized $ \sz ->
        frequency $
        take (1 + sz)
        [ (1, pure ObviouslyEmptyLevelMerge)
        , (1, pure ObviouslyEmptyUnionMerge)
        , (2, NonObviouslyEmptyLevelMerge <$> resize (sz `div` 2) arbitrary)
        , (2, NonObviouslyEmptyUnionMerge <$> resize (sz `div` 2) arbitrary)
        ]
    shrink ObviouslyEmptyLevelMerge         = []
    shrink ObviouslyEmptyUnionMerge         = [ObviouslyEmptyLevelMerge]
    shrink (NonObviouslyEmptyLevelMerge mt) = ObviouslyEmptyLevelMerge
                                            : [ NonObviouslyEmptyLevelMerge mt'
                                              | mt' <- shrink mt ]
    shrink (NonObviouslyEmptyUnionMerge mt) = ObviouslyEmptyUnionMerge
                                            : [ NonObviouslyEmptyUnionMerge mt'
                                              | mt' <- shrink mt ]

mkEmptyMergingTree :: EmptyMergingTree -> IO (Ref (MergingTree IO h))
mkEmptyMergingTree ObviouslyEmptyLevelMerge = newPendingLevelMerge [] Nothing
mkEmptyMergingTree ObviouslyEmptyUnionMerge = newPendingUnionMerge []
mkEmptyMergingTree (NonObviouslyEmptyLevelMerge emt) = do
    mt  <- mkEmptyMergingTree emt
    mt' <- newPendingLevelMerge [] (Just mt)
    releaseRef mt
    return mt'
mkEmptyMergingTree (NonObviouslyEmptyUnionMerge emts) = do
    mts <- mapM mkEmptyMergingTree emts
    mt' <- newPendingUnionMerge mts
    mapM_ releaseRef mts
    return mt'

{-------------------------------------------------------------------------------
  Lookup
-------------------------------------------------------------------------------}

prop_lookupTree ::
     forall h.
     FS.HasFS IO h
  -> FS.HasBlockIO IO h
  -> V.Vector SerialisedKey
  -> MergingTreeData SerialisedKey SerialisedValue SerialisedBlob
  -> IO Property
prop_lookupTree hfs hbio keys mtd = do
    let path = FS.mkFsPath []
    counter <- newUniqCounter 0
    withMergingTree hfs hbio resolveVal runParams path counter mtd $ \tree -> do
      arenaManager <- newArenaManager
      withActionRegistry $ \reg -> do
        res <- fetchBlobs =<< lookupsIO reg arenaManager tree
        return $
          normalise res
            === normalise (modelLookup (modelFoldMergingTree mtd) keys)
  where
    fetchBlobs ::
         V.Vector (Maybe (Entry v (WeakBlobRef IO h)))
      -> IO (V.Vector (Maybe (Entry v SerialisedBlob)))
    fetchBlobs = traverse (traverse (traverse (readWeakBlobRef hfs)))

    -- trees are always in the last level, there is no distinction between
    -- (Nothing and Just Delete), (Insert and Mupsert)
    normalise = V.map toLookupResult

    toLookupResult Nothing  = Nothing
    toLookupResult (Just e) = case e of
      Entry.Insert v           -> Just (v, Nothing)
      Entry.InsertWithBlob v b -> Just (v, Just b)
      Entry.Mupdate v          -> Just (v, Nothing)
      Entry.Delete             -> Nothing

    lookupsIO reg mgr tree =
        isStructurallyEmpty tree >>= \case
          True ->
            -- if the tree was empty, then the model should also have no results
            return $ V.map (const Nothing) keys
          False -> do
            batches <- buildLookupTree reg tree
            releaseLookupTree reg batches  -- only happens at the end
            results <- traverse (performLookups mgr) batches
            foldLookupTree resolveVal results

    performLookups mgr runs =
        Async.async $
          Lookup.lookupsIOWithoutWriteBuffer
            hbio
            mgr
            resolveVal
            runs
            (fmap (\(DeRef r) -> Run.runFilter   r) runs)
            (fmap (\(DeRef r) -> Run.runIndex    r) runs)
            (fmap (\(DeRef r) -> Run.runKOpsFile r) runs)
            keys

type SerialisedEntry = Entry SerialisedValue SerialisedBlob
type LookupAcc' = V.Vector (Maybe (Entry SerialisedValue SerialisedBlob))

modelLookup :: Map SerialisedKey SerialisedEntry -> V.Vector SerialisedKey -> LookupAcc'
modelLookup m = V.map (\k -> Map.lookup k m)

modelFoldMergingTree :: SerialisedMergingTreeData -> Map SerialisedKey SerialisedEntry
modelFoldMergingTree = goMergingTree
  where
    goMergingTree :: SerialisedMergingTreeData -> Map SerialisedKey SerialisedEntry
    goMergingTree = \case
        CompletedTreeMergeData r ->
          unRunData r
        OngoingTreeMergeData mr ->
          goMergingRun mr
        PendingLevelMergeData prs t ->
          modelMerge MR.MergeLevel (map goPreExistingRun prs <> map goMergingTree (toList t))
        PendingUnionMergeData ts ->
          modelMerge MR.MergeUnion (map goMergingTree ts)

    goPreExistingRun = \case
        PreExistingRunData r -> unRunData r
        PreExistingMergingRunData mr -> goMergingRun mr

    goMergingRun :: MR.IsMergeType t => SerialisedMergingRunData t -> Map SerialisedKey SerialisedEntry
    goMergingRun = \case
        CompletedMergeData _ r -> unRunData r
        OngoingMergeData mt rs -> modelMerge mt (map (unRunData . toRunData) rs)

modelMerge :: (Ord k, MR.IsMergeType t) => t -> [Map k SerialisedEntry] -> Map k SerialisedEntry
modelMerge mt = handleDeletes . Map.unionsWith (combine resolveVal)
  where
    handleDeletes = if MR.isLastLevel mt then Map.filter (/= Entry.Delete) else id
    combine = if MR.isUnion mt then Entry.combineUnion else Entry.combine

resolveVal :: SerialisedValue -> SerialisedValue -> SerialisedValue
resolveVal (SerialisedValue x) (SerialisedValue y) = SerialisedValue (x <> y)

{-------------------------------------------------------------------------------
  Supplying Credits
-------------------------------------------------------------------------------}

prop_supplyCredits ::
     forall h.
     FS.HasFS IO h
  -> FS.HasBlockIO IO h
  -> MR.CreditThreshold
  -> NonEmpty MR.MergeCredits
  -> MergingTreeData SerialisedKey SerialisedValue SerialisedBlob
  -> IO Property
prop_supplyCredits hfs hbio threshold credits mtd = do
    FS.createDirectory hfs setupPath
    FS.createDirectory hfs (FS.mkFsPath ["active"])
    counter <- newUniqCounter 0
    withMergingTree hfs hbio resolveVal runParams setupPath counter mtd $ \tree -> do
      props <- for credits $ \c -> do
        (MR.MergeDebt debt, _) <- remainingMergeDebt tree
        leftovers <-
          supplyCredits hfs hbio resolveVal runParams threshold root counter tree c
        (MR.MergeDebt debt', _) <- remainingMergeDebt tree
        return $
          counterexample (show (debt, leftovers, debt')) $ conjoin [
              counterexample "negative values" $
                debt >= 0 && leftovers >= 0 && debt' >= 0
            , counterexample "did not reduce debt sufficiently" $
                debt' <= debt - (c - leftovers)
            ]
      return (conjoin (toList props))
  where
    root = Paths.SessionRoot (FS.mkFsPath [])
    setupPath = FS.mkFsPath ["setup"]  -- separate dir, so it doesn't clash

instance Arbitrary MR.MergeCredits where
  arbitrary = MR.MergeCredits . getPositive <$> arbitrary
  shrink (MR.MergeCredits c) = [MR.MergeCredits c' | c' <- shrink c, c' > 0]

instance Arbitrary MR.CreditThreshold where
  arbitrary = coerce (arbitrary @MR.MergeCredits)
  shrink = coerce (shrink @MR.MergeCredits)
  -- TODO: does this make sense? in a way a larger threshold is "simpler".
