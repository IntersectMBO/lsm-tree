module Test.Database.LSMTree.Internal.Merge (tests) where

import           Control.Exception (evaluate)
import           Control.RefCount
import           Data.Bifoldable (bifoldMap)
import qualified Data.BloomFilter.Blocked as Bloom
import           Data.Foldable (traverse_)
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import           Data.Maybe (isJust)
import qualified Data.Vector as V
import           Database.LSMTree.Extras
import           Database.LSMTree.Extras.RunData
import qualified Database.LSMTree.Internal.BlobFile as BlobFile
import qualified Database.LSMTree.Internal.Entry as Entry
import qualified Database.LSMTree.Internal.Index as Index (IndexType (Ordinary))
import           Database.LSMTree.Internal.Merge (MergeType (..))
import qualified Database.LSMTree.Internal.Merge as Merge
import           Database.LSMTree.Internal.PageAcc (entryWouldFitInPage)
import           Database.LSMTree.Internal.Paths (RunFsPaths (..),
                     pathsForRunFiles)
import qualified Database.LSMTree.Internal.Run as Run
import qualified Database.LSMTree.Internal.RunAcc as RunAcc
import qualified Database.LSMTree.Internal.RunBuilder as RunBuilder
import           Database.LSMTree.Internal.RunNumber
import           Database.LSMTree.Internal.Serialise
import           Database.LSMTree.Internal.UniqCounter
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
    [ testProperty "prop_MergeDistributes" $ \mergeType stepSize rds ->
        ioPropertyWithMockFS $ \fs hbio ->
          prop_MergeDistributes fs hbio mergeType stepSize rds
    , testProperty "prop_AbortMerge" $ \level stepSize rds ->
        ioPropertyWithMockFS $ \fs hbio ->
          prop_AbortMerge fs hbio level stepSize rds
    ]
  where
    ioPropertyWithMockFS ::
         Testable p
      => (FS.HasFS IO FsSim.HandleMock -> FS.HasBlockIO IO FsSim.HandleMock -> IO p)
      -> Property
    ioPropertyWithMockFS prop = ioProperty $ do
        (res, mockFS, _) <- FsSim.runSimErrorHasBlockIO FsSim.empty FsSim.emptyErrors prop
        pure $ res
            .&&. counterexample "open handles"
                   (FsSim.numOpenHandles mockFS === 0)

runParams :: RunBuilder.RunParams
runParams =
    RunBuilder.RunParams {
      runParamCaching = RunBuilder.CacheRunData,
      runParamAlloc   = RunAcc.RunAllocFixed 10,
      runParamIndex   = Index.Ordinary
    }

testSalt :: Bloom.Salt
testSalt = 4

-- | Creating multiple runs from write buffers and merging them leads to the
-- same run as merging the write buffers and creating a run.
--
-- @mergeRuns . map flush === flush . mergeWriteBuffers@
prop_MergeDistributes ::
     FS.HasFS IO h ->
     FS.HasBlockIO IO h ->
     MergeType ->
     StepSize ->
     SmallList (RunData SerialisedKey SerialisedValue SerialisedBlob) ->
     IO Property
prop_MergeDistributes fs hbio mergeType stepSize (SmallList rds) = do
    let path = FS.mkFsPath []
    counter <- newUniqCounter 0
    withRuns fs hbio testSalt runParams path counter rds' $ \runs -> do
      let stepsNeeded = sum (map (Map.size . unRunData) rds)

      fsPathLhs <- RunFsPaths path . uniqueToRunNumber <$> incrUniqCounter counter
      (stepsDone, lhs) <- mergeRuns fs hbio mergeType stepSize fsPathLhs runs
      let runData = RunData $ mergeWriteBuffers mergeType $ fmap unRunData rds'
      withRun fs hbio testSalt runParams path counter runData $ \rhs -> do

        (lhsSize, lhsFilter, lhsIndex, lhsKOps,
         lhsKOpsFileContent, lhsBlobFileContent) <- getRunContent lhs

        (rhsSize, rhsFilter, rhsIndex, rhsKOps,
         rhsKOpsFileContent, rhsBlobFileContent) <- getRunContent rhs

        -- cleanup
        releaseRef lhs

        pure $ stats $
              counterexample "numEntries"
              (lhsSize === rhsSize)
          .&&. -- we can't just test bloom filter equality, their sizes may differ.
              counterexample "runFilter"
              (   Bloom.sizeBits (Bloom.size lhsFilter)
               >= Bloom.sizeBits (Bloom.size rhsFilter))
          .&&. -- the index is equal, but only because range finder precision is
              -- always 0 for the numbers of entries we are dealing with.
              counterexample "runIndex"
              (lhsIndex === rhsIndex)
          .&&. counterexample "kops"
              (lhsKOps === rhsKOps)
          .&&. counterexample "kopsFile"
              (lhsKOpsFileContent === rhsKOpsFileContent)
          .&&. counterexample "blobFile"
              (lhsBlobFileContent === rhsBlobFileContent)
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
    isLarge = not . uncurry entryWouldFitInPage

    getRunContent run@(DeRef Run.Run {
                         Run.runFilter,
                         Run.runIndex,
                         Run.runKOpsFile,
                         Run.runBlobFile
                       }) = do
      runSize         <- evaluate (Run.size run)
      runKOps         <- readKOps Nothing run
      kopsFileContent <- FS.hGetAll fs runKOpsFile
      blobFileContent <- withRef runBlobFile $
                         FS.hGetAll fs . BlobFile.blobFileHandle
      pure ( runSize
             , runFilter
             , runIndex
             , runKOps
             , kopsFileContent
             , blobFileContent
             )

-- | After merging for a few steps, we can prematurely abort the merge, which
-- should clean up properly.
prop_AbortMerge ::
     FS.HasFS IO h ->
     FS.HasBlockIO IO h ->
     MergeType ->
     StepSize ->
     SmallList (RunData SerialisedKey SerialisedValue SerialisedBlob) ->
     IO Property
prop_AbortMerge fs hbio mergeType (Positive stepSize) (SmallList wbs) = do
    let path = FS.mkFsPath []
    let pathOut = RunFsPaths path (RunNumber 0)
    counter <- newUniqCounter 1
    withRuns fs hbio testSalt runParams path counter wbs' $ \runs -> do
      mergeToClose <- makeInProgressMerge pathOut runs
      traverse_ Merge.abort mergeToClose

      filesExist <- traverse (FS.doesFileExist fs) (pathsForRunFiles pathOut)

      pure $
        counterexample ("run files exist: " <> show filesExist) $
          isJust mergeToClose ==> all not filesExist
  where
    wbs' = fmap serialiseRunData wbs

    makeInProgressMerge path runs =
      Merge.new fs hbio testSalt runParams mergeType resolveVal
                path (V.fromList runs) >>= \case
        Nothing -> pure Nothing  -- not in progress
        Just merge -> do
          -- just do a few steps once, ideally not completing the merge
          Merge.steps merge stepSize >>= \case
            (_, Merge.MergeDone) -> do
              Merge.abort merge  -- run not needed
              pure Nothing  -- not in progress
            (_, Merge.MergeInProgress) ->
              pure (Just merge)

{-------------------------------------------------------------------------------
  Utilities
-------------------------------------------------------------------------------}

type StepSize = Positive Int

mergeRuns ::
     FS.HasFS IO h ->
     FS.HasBlockIO IO h ->
     MergeType ->
     StepSize ->
     RunFsPaths ->
     [Ref (Run.Run IO h)] ->
     IO (Int, Ref (Run.Run IO h))
mergeRuns fs hbio mergeType (Positive stepSize) fsPath runs = do
    Merge.new fs hbio testSalt runParams mergeType resolveVal
              fsPath (V.fromList runs)
      >>= \case
        Just m  -> Merge.stepsToCompletionCounted m stepSize
        Nothing -> (,) 0 <$> unsafeCreateRunAt fs hbio testSalt runParams fsPath
                               (RunData Map.empty)

type SerialisedEntry = Entry.Entry SerialisedValue SerialisedBlob

mergeWriteBuffers :: MergeType
                  -> [Map SerialisedKey SerialisedEntry]
                  ->  Map SerialisedKey SerialisedEntry
mergeWriteBuffers = \case
    MergeTypeMidLevel  -> Map.unionsWith (Entry.combine resolveVal)
    MergeTypeLastLevel -> Map.filter (not . isDelete)
                        . Map.unionsWith (Entry.combine resolveVal)
    MergeTypeUnion     -> Map.filter (not . isDelete)
                        . Map.unionsWith (Entry.combineUnion resolveVal)
  where
    isDelete Entry.Delete = True
    isDelete _            = False

resolveVal :: ResolveSerialisedValue
resolveVal (SerialisedValue x) (SerialisedValue y) = SerialisedValue (x <> y)

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
