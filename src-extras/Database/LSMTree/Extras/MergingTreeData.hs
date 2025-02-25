-- | Utilities for generating 'MergingTree's. Tests and benchmarks should
-- preferably use these utilities instead of (re-)defining their own.
module Database.LSMTree.Extras.MergingTreeData (
    -- * Flush merging trees
    withMergingTree
  , unsafeFlushMergingTree
    -- * MergingTreeData
  , MergingTreeData (..)
  , PreExistingRunData (..)
  , mergingTreeDataInvariant
  , mapMergingTreeData
  , SerialisedMergingTreeData
  , serialiseMergingTreeData
    -- * QuickCheck
  , labelMergingTreeData
  , genMergingTreeData
  , shrinkMergingTreeData
  ) where

import           Control.Exception (assert, bracket)
import           Control.Monad
import           Control.RefCount
import           Data.Foldable (for_, toList)
import           Database.LSMTree.Extras (showPowersOf)
import           Database.LSMTree.Extras.Generators ()
import           Database.LSMTree.Extras.MergingRunData
import           Database.LSMTree.Extras.RunData
import           Database.LSMTree.Internal.Index (IndexType)
import           Database.LSMTree.Internal.Lookup (ResolveSerialisedValue)
import qualified Database.LSMTree.Internal.MergingRun as MR
import           Database.LSMTree.Internal.MergingTree (MergingTree)
import qualified Database.LSMTree.Internal.MergingTree as MT
import           Database.LSMTree.Internal.Serialise
import           Database.LSMTree.Internal.UniqCounter
import qualified System.FS.API as FS
import           System.FS.API (HasFS)
import           System.FS.BlockIO.API (HasBlockIO)
import           Test.QuickCheck as QC

{-------------------------------------------------------------------------------
  Flush runs
-------------------------------------------------------------------------------}

-- | Create a temporary 'Run' using 'unsafeFlushAsWriteBuffer'.
withMergingTree ::
     HasFS IO h
  -> HasBlockIO IO h
  -> ResolveSerialisedValue
  -> IndexType
  -> FS.FsPath
  -> UniqCounter IO
  -> SerialisedMergingTreeData
  -> (Ref (MergingTree IO h) -> IO a)
  -> IO a
withMergingTree hfs hbio resolve indexType path counter mrd = do
    bracket
      (unsafeFlushMergingTree hfs hbio resolve indexType path counter mrd)
      releaseRef

-- | Flush serialised run data to disk as if it were a write buffer.
--
-- This might leak resources if not run with asynchronous exceptions masked.
-- Use helper functions like 'withMergingTree' instead.
--
-- Use of this function should be paired with a 'releaseRef'.
unsafeFlushMergingTree ::
     HasFS IO h
  -> HasBlockIO IO h
  -> ResolveSerialisedValue
  -> IndexType
  -> FS.FsPath
  -> UniqCounter IO
  -> SerialisedMergingTreeData
  -> IO (Ref (MergingTree IO h))
unsafeFlushMergingTree hfs hbio resolve indexType path counter = \case
    CompletedTreeMergeData rd -> do
      run <- unsafeFlushRun hfs hbio indexType path counter rd
      tree <- MT.mkMergingTree . MT.CompletedTreeMerge =<< dupRef run
      releaseRef run
      return tree
    OngoingTreeMergeData mrd -> do
      mr <- unsafeFlushMergingRun hfs hbio resolve indexType path counter mrd
      tree <- MT.mkMergingTree . MT.OngoingTreeMerge =<< dupRef mr
      releaseRef mr
      return tree
    PendingLevelMergeData prds mtd -> do
      prs <- forM prds $ \case
        PreExistingRunData rd -> MT.PreExistingRun
          <$> unsafeFlushRun hfs hbio indexType path counter rd
        PreExistingMergingRunData mrd -> MT.PreExistingMergingRun
          <$> unsafeFlushMergingRun hfs hbio resolve indexType path counter mrd
      mt <- forM mtd $
        unsafeFlushMergingTree hfs hbio resolve indexType path counter
      tree <- MT.newPendingLevelMerge prs mt
      forM_ prs $ \case
        MT.PreExistingRun r         -> releaseRef r
        MT.PreExistingMergingRun mr -> releaseRef mr
      forM_ mt $ releaseRef
      return tree
    PendingUnionMergeData mtds -> do
      mts <- forM mtds $
        unsafeFlushMergingTree hfs hbio resolve indexType path counter
      tree <- MT.newPendingUnionMerge mts
      forM_ mts $ releaseRef
      return tree

{-------------------------------------------------------------------------------
  MergingTreeData
-------------------------------------------------------------------------------}

-- | A data structure suitable for creating arbitrary 'MergingTree's.
--
-- Note: 'b ~ Void' should rule out blobs.
data MergingTreeData k v b =
    CompletedTreeMergeData (RunData k v b)
  | OngoingTreeMergeData (MergingRunData MR.TreeMergeType k v b)
  | PendingLevelMergeData
      [PreExistingRunData k v b]
      (Maybe (MergingTreeData k v b))  -- ^ not both empty!
  | PendingUnionMergeData [MergingTreeData k v b]  -- ^ at least 2 children
  deriving stock (Show, Eq)

data PreExistingRunData k v b =
    PreExistingRunData (RunData k v b)
  | PreExistingMergingRunData (MergingRunData MR.LevelMergeType k v b)
  deriving stock (Show, Eq)

-- | See @treeInvariant@ in prototype.
mergingTreeDataInvariant :: MergingTreeData k v b -> Either String ()
mergingTreeDataInvariant = \case
    CompletedTreeMergeData _rd ->
      Right ()
    OngoingTreeMergeData mr ->
      mergingRunDataInvariant mr
    PendingLevelMergeData prs t -> do
      assertI "pending level merges have at least one input" $
        length prs + length t > 0
      for_ prs $ \case
        PreExistingRunData        _r -> Right ()
        PreExistingMergingRunData mr -> mergingRunDataInvariant mr
      for_ t mergingTreeDataInvariant
    PendingUnionMergeData ts -> do
      assertI "pending union merges are non-trivial (at least two inputs)" $
        length ts > 1
      for_ ts mergingTreeDataInvariant
  where
    assertI msg False = Left msg
    assertI _   True  = Right ()

mapMergingTreeData ::
     Ord k'
  => (k -> k') -> (v -> v') -> (b -> b')
  -> MergingTreeData k v b -> MergingTreeData k' v' b'
mapMergingTreeData f g h = go
  where
    go = \case
        CompletedTreeMergeData r ->
          CompletedTreeMergeData $ mapRunData f g h r
        OngoingTreeMergeData mr ->
          OngoingTreeMergeData $ mapMergingRunData f g h mr
        PendingLevelMergeData prs t ->
          PendingLevelMergeData
            (map (mapPreExistingRunData f g h) prs)
            (fmap go t)
        PendingUnionMergeData ts ->
          PendingUnionMergeData $ fmap go ts

mapPreExistingRunData ::
     Ord k'
  => (k -> k') -> (v -> v') -> (b -> b')
  -> PreExistingRunData k v b -> PreExistingRunData k' v' b'
mapPreExistingRunData f g h = \case
    PreExistingRunData r ->
      PreExistingRunData (mapRunData f g h r)
    PreExistingMergingRunData mr ->
      PreExistingMergingRunData (mapMergingRunData f g h mr)

type SerialisedMergingTreeData =
    MergingTreeData SerialisedKey SerialisedValue SerialisedBlob

type SerialisedPreExistingRunData =
    PreExistingRunData SerialisedKey SerialisedValue SerialisedBlob

serialiseMergingTreeData ::
     (SerialiseKey k, SerialiseValue v, SerialiseValue b)
  => MergingTreeData k v b -> SerialisedMergingTreeData
serialiseMergingTreeData =
    mapMergingTreeData serialiseKey serialiseValue serialiseBlob

{-------------------------------------------------------------------------------
  QuickCheck
-------------------------------------------------------------------------------}

labelMergingTreeData :: SerialisedMergingTreeData -> Property -> Property
labelMergingTreeData = \rd ->
    tabulate "tree depth" [showPowersOf 2 (depthTree rd)] . go rd
  where
    go (CompletedTreeMergeData rd) =
          tabulate "merging tree state" ["CompletedTreeMerge"]
        . labelRunData rd
    go (OngoingTreeMergeData mrd) =
          tabulate "merging tree state" ["OngoingTreeMerge"]
        . labelMergingRunData mrd
    go (PendingLevelMergeData prds mtd) =
          tabulate "merging tree state" ["PendingLevelMerge"]
        . foldr ((.) . labelPreExistingRunData) id prds
        . maybe id go mtd
    go (PendingUnionMergeData mtds) =
          tabulate "merging tree state" ["PendingUnionMerge"]
        . foldr ((.) . go) id mtds

    -- the longest path from the root to a run
    depthTree = (+1) . \case  -- maximum depth of children
        CompletedTreeMergeData _ -> 0
        OngoingTreeMergeData _   -> 0
        PendingLevelMergeData prds mtds ->
          maximum (0 : fmap (const 1) prds ++ map depthTree (toList mtds))
        PendingUnionMergeData mtds ->
          maximum (0 : map depthTree mtds)


labelPreExistingRunData :: SerialisedPreExistingRunData -> Property -> Property
labelPreExistingRunData (PreExistingRunData rd)         = labelRunData rd
labelPreExistingRunData (PreExistingMergingRunData mrd) = labelMergingRunData mrd

instance ( Ord k, Arbitrary k, Arbitrary v, Arbitrary b
         ) => Arbitrary (MergingTreeData k v b) where
  arbitrary = genMergingTreeData arbitrary arbitrary arbitrary
  shrink = shrinkMergingTreeData shrink shrink shrink

genMergingTreeData ::
     forall k v b. Ord k
  => Gen k
  -> Gen v
  -> Gen b
  -> Gen (MergingTreeData k v b)
genMergingTreeData genKey genVal genBlob = QC.sized $ \s -> do
    go =<< QC.chooseInt (1, max 1 s)
  where
    -- n is the number of PMergingTree constructors + pre-existing runs
    go n
      | n < 1
      = error ("arbitrary T: n == " <> show n)

      | n == 1
      = QC.frequency
          [ (1, CompletedTreeMergeData <$> genRunData genKey genVal genBlob)
          , (1, OngoingTreeMergeData <$> genMergingRunData genKey genVal genBlob)
          ]

      | otherwise
      = QC.frequency
          [ (1, do
              -- pending level merge without child
              preExisting <- QC.vectorOf (n - 1) $  -- 1 for constructor itself
                genPreExistingRunData genKey genVal genBlob
              return (PendingLevelMergeData preExisting Nothing))
          , (1, do
              -- pending level merge with child
              numPreExisting <- QC.chooseInt (0, min 20 (n - 2))
              preExisting <- QC.vectorOf numPreExisting $
                genPreExistingRunData genKey genVal genBlob
              tree <- go (n - numPreExisting - 1)
              return (PendingLevelMergeData preExisting (Just tree)))
          , (2, do
              -- pending union merge
              ns <- QC.shuffle =<< arbitraryPartition2 n
              PendingUnionMergeData <$> traverse go ns)
          ]

-- Split into at least two smaller positive numbers. The input needs to be
-- greater than or equal to 2.
arbitraryPartition2 :: Int -> QC.Gen [Int]
arbitraryPartition2 n = assert (n >= 2) $ do
    first <- QC.chooseInt (1, n-1)
    (first :) <$> arbitraryPartition (n - first)

-- Split into smaller positive numbers.
arbitraryPartition :: Int -> QC.Gen [Int]
arbitraryPartition n
      | n <  1 = return []
      | n == 1 = return [1]
      | otherwise = do
        first <- QC.chooseInt (1, n)
        (first :) <$> arbitraryPartition (n - first)

-- TODO: Would it be useful to shrink by merging subtrees into a single run?
-- This would simplify the tree while preserving many errors that depend on the
-- specific content of the tree. See prototype tests.
shrinkMergingTreeData ::
     Ord k
  => (k -> [k])
  -> (v -> [v])
  -> (b -> [b])
  -> MergingTreeData k v b
  -> [MergingTreeData k v b]
shrinkMergingTreeData shrinkKey shrinkVal shrinkBlob = \case
  CompletedTreeMergeData r ->
    [ CompletedTreeMergeData r'
    | r' <- shrinkRunData shrinkKey shrinkVal shrinkBlob r
    ]
  OngoingTreeMergeData mr ->
    [ OngoingTreeMergeData mr'
    | mr' <- shrinkMergingRunData shrinkKey shrinkVal shrinkBlob mr
    ]
  PendingLevelMergeData prs t ->
    {- HLINT ignore "Use catMaybes" -}
    [ t' | Just t' <- [t] ]
    <>
    -- move into regular levels
    [ PendingLevelMergeData (prs ++ [PreExistingRunData r]) Nothing
    | Just (CompletedTreeMergeData r) <- [t]
    ]
    <>
    [ PendingLevelMergeData prs' t'
    | (prs', t') <-
        liftShrink2
          (liftShrink (shrinkPreExistingRunData shrinkKey shrinkVal shrinkBlob))
          (liftShrink (shrinkMergingTreeData shrinkKey shrinkVal shrinkBlob))
          (prs, t)
    , length prs' + length t' > 0
    ]
  PendingUnionMergeData ts ->
    ts
    <>
    [ PendingUnionMergeData ts'
    | ts' <- liftShrink (shrinkMergingTreeData shrinkKey shrinkVal shrinkBlob) ts
    , length ts' > 1
    ]

genPreExistingRunData ::
     forall k v b. Ord k
  => Gen k
  -> Gen v
  -> Gen b
  -> Gen (PreExistingRunData k v b)
genPreExistingRunData genKey genVal genBlob =
    QC.oneof
      [ PreExistingRunData <$> genRunData genKey genVal genBlob
      , PreExistingMergingRunData <$> genMergingRunData genKey genVal genBlob
      ]

shrinkPreExistingRunData ::
     Ord k
  => (k -> [k])
  -> (v -> [v])
  -> (b -> [b])
  -> PreExistingRunData k v b
  -> [PreExistingRunData k v b]
shrinkPreExistingRunData shrinkKey shrinkVal shrinkBlob = \case
    PreExistingRunData r ->
      [ PreExistingRunData r'
      | r' <- shrinkRunData shrinkKey shrinkVal shrinkBlob r
      ]
    PreExistingMergingRunData mr ->
      [ PreExistingMergingRunData mr'
      | mr' <- shrinkMergingRunData shrinkKey shrinkVal shrinkBlob mr
      ]
