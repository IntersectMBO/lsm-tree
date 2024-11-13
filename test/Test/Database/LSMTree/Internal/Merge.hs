{-# LANGUAGE LambdaCase #-}

module Test.Database.LSMTree.Internal.Merge (tests) where

import           Data.Bifoldable (bifoldMap)
import qualified Data.BloomFilter as Bloom
import           Data.Foldable (traverse_)
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import           Data.Maybe (isJust)
import qualified Data.Vector as V
import           Database.LSMTree.Extras
import           Database.LSMTree.Extras.Generators (KeyForIndexCompact)
import           Database.LSMTree.Extras.RunData
import qualified Database.LSMTree.Internal.BlobFile as BlobFile
import qualified Database.LSMTree.Internal.Entry as Entry
import qualified Database.LSMTree.Internal.Merge as Merge
import           Database.LSMTree.Internal.PageAcc (entryWouldFitInPage)
import           Database.LSMTree.Internal.Paths (RunFsPaths (..),
                     pathsForRunFiles)
import qualified Database.LSMTree.Internal.Run as Run
import           Database.LSMTree.Internal.RunAcc (RunBloomFilterAlloc (..))
import           Database.LSMTree.Internal.RunNumber
import           Database.LSMTree.Internal.Serialise
import qualified System.FS.API as FS
import qualified System.FS.API.Lazy as FS
import qualified System.FS.BlockIO.API as FS
import qualified System.FS.BlockIO.Sim as FsSim
import qualified System.FS.Sim.Error as FsSim
import qualified System.FS.Sim.MockFS as FsSim
import           Test.Database.LSMTree.Internal.RunReader (readKOps)
import           Test.QuickCheck
import           Test.Tasty
import           Test.Tasty.QuickCheck

tests :: TestTree
tests = testGroup "Test.Database.LSMTree.Internal.Merge"
    [ testProperty "prop_MergeDistributes" $ \level stepSize wbs ->
        ioPropertyWithMockFS $ \fs hbio ->
          prop_MergeDistributes fs hbio level stepSize wbs
    , testProperty "prop_AbortMerge" $ \level stepSize wbs ->
        ioPropertyWithMockFS $ \fs hbio ->
          prop_AbortMerge fs hbio level stepSize wbs
    ]
  where
    ioPropertyWithMockFS ::
         Testable p
      => (FS.HasFS IO FsSim.HandleMock -> FS.HasBlockIO IO FsSim.HandleMock -> IO p)
      -> Property
    ioPropertyWithMockFS prop = ioProperty $ do
        (res, mockFS) <-
          FsSim.runSimErrorFS FsSim.empty FsSim.emptyErrors $ \_ fs -> do
            hbio <- FsSim.fromHasFS fs
            prop fs hbio
        return $ res
            .&&. counterexample "open handles"
                   (FsSim.numOpenHandles mockFS === 0)

-- | Creating multiple runs from write buffers and merging them leads to the
-- same run as merging the write buffers and creating a run.
--
-- @mergeRuns . map flush === flush . mergeWriteBuffers@
prop_MergeDistributes ::
     FS.HasFS IO h ->
     FS.HasBlockIO IO h ->
     Merge.Level ->
     StepSize ->
     SmallList (RunData KeyForIndexCompact SerialisedValue SerialisedBlob) ->
     IO Property
prop_MergeDistributes fs hbio level stepSize (SmallList rds) =
    withRuns fs hbio (V.fromList (zip (simplePaths [10..]) rds')) $ \runs -> do
      let stepsNeeded = sum (map (Map.size . unRunData) rds)
      (stepsDone, lhs) <- mergeRuns fs hbio level (RunNumber 0) runs stepSize
      withRun fs hbio (simplePath 1) (RunData $ mergeWriteBuffers level $ fmap unRunData rds') $ \rhs -> do

        lhsKOpsFile <- FS.hGetAll fs (Run.runKOpsFile lhs)
        lhsBlobFile <- FS.hGetAll fs (BlobFile.blobFileHandle (Run.runBlobFile lhs))
        rhsKOpsFile <- FS.hGetAll fs (Run.runKOpsFile rhs)
        rhsBlobFile <- FS.hGetAll fs (BlobFile.blobFileHandle (Run.runBlobFile rhs))

        lhsKOps <- readKOps Nothing lhs
        rhsKOps <- readKOps Nothing rhs

        -- cleanup
        Run.removeReference lhs

        return $ stats $
              counterexample "numEntries"
              (Run.runNumEntries lhs === Run.runNumEntries rhs)
          .&&. -- we can't just test bloom filter equality, their sizes may differ.
              counterexample "runFilter"
              (Bloom.length (Run.runFilter lhs) >= Bloom.length (Run.runFilter rhs))
          .&&. -- the index is equal, but only because range finder precision is
              -- always 0 for the numbers of entries we are dealing with.
              counterexample "runIndex"
              (Run.runIndex lhs === Run.runIndex rhs)
          .&&. counterexample "kops"
              (lhsKOps === rhsKOps)
          .&&. counterexample "kopsFile"
              (lhsKOpsFile === rhsKOpsFile)
          .&&. counterexample "blobFile"
              (lhsBlobFile === rhsBlobFile)
          .&&. counterexample ("step counting")
              (stepsDone === stepsNeeded)
  where
    stats = tabulate "value size" (map (showPowersOf10 . sizeofValue) vals)
          . tabulate "entry type" (map (takeWhile (/= ' ') . show . snd) kops)
          . label (if any isLarge kops then "has large k/op" else "no large k/op")
          . label ("number of runs: " <> showPowersOf 2 (length rds'))
    rds' = fmap serialiseRunData rds
    kops = foldMap (Map.toList . unRunData) rds'
    vals = concatMap (bifoldMap pure mempty . snd) kops
    isLarge = uncurry entryWouldFitInPage

-- | After merging for a few steps, we can prematurely abort the merge, which
-- should clean up properly.
prop_AbortMerge ::
     FS.HasFS IO h ->
     FS.HasBlockIO IO h ->
     Merge.Level ->
     StepSize ->
     SmallList (RunData KeyForIndexCompact SerialisedValue SerialisedBlob) ->
     IO Property
prop_AbortMerge fs hbio level (Positive stepSize) (SmallList wbs) =
    withRuns fs hbio (V.fromList (zip (simplePaths [10..]) wbs')) $ \runs -> do
      let path0 = simplePath 0
      mergeToClose <- makeInProgressMerge path0 runs
      traverse_ Merge.abort mergeToClose

      filesExist <- traverse (FS.doesFileExist fs) (pathsForRunFiles path0)

      return $
        counterexample ("run files exist: " <> show filesExist) $
          isJust mergeToClose ==> all not filesExist
  where
    wbs' = fmap serialiseRunData wbs

    makeInProgressMerge path runs =
      Merge.new fs hbio Run.CacheRunData (RunAllocFixed 10) level mappendValues path runs >>= \case
        Nothing -> return Nothing  -- not in progress
        Just merge -> do
          -- just do a few steps once, ideally not completing the merge
          Merge.steps merge stepSize >>= \case
            (_, Merge.MergeDone) -> do
              Merge.abort merge  -- run not needed
              return Nothing  -- not in progress
            (_, Merge.MergeInProgress) ->
              return (Just merge)

{-------------------------------------------------------------------------------
  Utilities
-------------------------------------------------------------------------------}

type StepSize = Positive Int

mergeRuns ::
     FS.HasFS IO h ->
     FS.HasBlockIO IO h ->
     Merge.Level ->
     RunNumber ->
     V.Vector (Run.Run IO h) ->
     StepSize ->
     IO (Int, Run.Run IO h)
mergeRuns fs hbio level runNumber runs (Positive stepSize) = do
    Merge.new fs hbio Run.CacheRunData (RunAllocFixed 10) level mappendValues
              (RunFsPaths (FS.mkFsPath []) runNumber) runs >>= \case
      Nothing -> (,) 0 <$> unsafeFlushAsWriteBuffer fs hbio
                             (RunFsPaths (FS.mkFsPath []) runNumber) (RunData Map.empty)
      Just m  -> Merge.stepsToCompletionCounted m stepSize

type SerialisedEntry = Entry.Entry SerialisedValue SerialisedBlob

mergeWriteBuffers :: Merge.Level
                  -> [Map SerialisedKey SerialisedEntry]
                  ->  Map SerialisedKey SerialisedEntry
mergeWriteBuffers level =
        (if level == Merge.LastLevel then Map.filter (not . isDelete) else id)
      . Map.unionsWith (Entry.combine mappendValues)
  where
    isDelete Entry.Delete = True
    isDelete _            = False

mappendValues :: SerialisedValue -> SerialisedValue -> SerialisedValue
mappendValues (SerialisedValue x) (SerialisedValue y) = SerialisedValue (x <> y)

newtype SmallList a = SmallList { getSmallList :: [a] }
  deriving stock (Show, Eq)
  deriving newtype (Functor, Foldable)

-- | Skewed towards short lists, but still generates longer ones.
instance Arbitrary a => Arbitrary (SmallList a) where
  arbitrary = do
      ub <- sized $ \s -> chooseInt (5, s `div` 3)
      n <- chooseInt (1, ub)
      SmallList <$> vectorOf n arbitrary

  shrink = fmap SmallList . shrink . getSmallList
