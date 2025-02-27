-- | Utilities for generating 'MergingRun's. Tests and benchmarks should
-- preferably use these utilities instead of (re-)defining their own.
module Database.LSMTree.Extras.MergingRunData (
    -- * Flush merging runs
    withMergingRun
  , unsafeFlushMergingRun
    -- * MergingRunData
  , MergingRunData (..)
  , mergingRunDataInvariant
  , mapMergingRunData
  , SerialisedMergingRunData
  , serialiseMergingRunData
    -- * QuickCheck
  , labelMergingRunData
  , genMergingRunData
  , shrinkMergingRunData
  ) where

import           Control.Exception (bracket)
import           Control.Monad (forM)
import           Control.RefCount
import qualified Data.Vector as V
import           Database.LSMTree.Extras (showPowersOf)
import           Database.LSMTree.Extras.Generators ()
import           Database.LSMTree.Extras.RunData
import           Database.LSMTree.Internal.Index (IndexType)
import           Database.LSMTree.Internal.Lookup (ResolveSerialisedValue)
import           Database.LSMTree.Internal.MergingRun (MergingRun)
import qualified Database.LSMTree.Internal.MergingRun as MR
import           Database.LSMTree.Internal.Paths
import           Database.LSMTree.Internal.Run (RunDataCaching (..))
import qualified Database.LSMTree.Internal.Run as Run
import           Database.LSMTree.Internal.RunAcc (RunBloomFilterAlloc (..))
import           Database.LSMTree.Internal.RunNumber
import           Database.LSMTree.Internal.Serialise
import           Database.LSMTree.Internal.UniqCounter
import qualified System.FS.API as FS
import           System.FS.API (HasFS)
import           System.FS.BlockIO.API (HasBlockIO)
import           Test.QuickCheck as QC

{-------------------------------------------------------------------------------
  Flush runs
-------------------------------------------------------------------------------}

-- | Create a temporary 'Run' using 'unsafeFlushMergingRun'.
withMergingRun ::
     MR.IsMergeType t
  => HasFS IO h
  -> HasBlockIO IO h
  -> ResolveSerialisedValue
  -> IndexType
  -> FS.FsPath
  -> UniqCounter IO
  -> SerialisedMergingRunData t
  -> (Ref (MergingRun t IO h) -> IO a)
  -> IO a
withMergingRun hfs hbio resolve indexType path counter mrd = do
    bracket
      (unsafeFlushMergingRun hfs hbio resolve indexType path counter mrd)
      releaseRef

-- | Flush serialised run data to disk as if it were a write buffer.
--
-- This might leak resources if not run with asynchronous exceptions masked.
-- Use helper functions like 'withMergingRun' or 'withMergingRuns' instead.
--
-- Use of this function should be paired with a 'releaseRef'.
unsafeFlushMergingRun ::
     MR.IsMergeType t
  => HasFS IO h
  -> HasBlockIO IO h
  -> ResolveSerialisedValue
  -> IndexType
  -> FS.FsPath
  -> UniqCounter IO
  -> SerialisedMergingRunData t
  -> IO (Ref (MergingRun t IO h))
unsafeFlushMergingRun hfs hbio resolve indexType path counter = \case
    CompletedMergeData _ numRuns rd -> do
      run <- unsafeFlushRun hfs hbio indexType path counter rd
      let totalDebt = Run.size run  -- slightly hacky, generally it's larger
      mr <- MR.newCompleted numRuns totalDebt run
      releaseRef run
      return mr

    OngoingMergeData mergeType rds -> do
      runs <- fmap V.fromList $ forM rds $ \rd -> do
        unsafeFlushRun hfs hbio indexType path counter (unNonEmptyRunData rd)
      n <- incrUniqCounter counter
      let fsPaths = RunFsPaths path (RunNumber (uniqueToInt n))
      mr <- MR.new hfs hbio resolve CacheRunData (RunAllocFixed 10) indexType
              mergeType fsPaths runs
      mapM_ releaseRef runs
      return mr

{-------------------------------------------------------------------------------
  MergingRunData
-------------------------------------------------------------------------------}

-- | A data structure suitable for creating arbitrary 'MergingRun's.
--
-- Note: 'b ~ Void' should rule out blobs.
--
-- Currently, ongoing merges are always \"fresh\", i.e. there is no merge work
-- already performed.
--
-- TODO: Generate merge credits and supply them in 'unsafeFlushMergingRun',
-- similarly to how @ScheduledMergesTest@ does it.
data MergingRunData t k v b =
    CompletedMergeData t MR.NumRuns (RunData k v b)
  | OngoingMergeData t [NonEmptyRunData k v b]  -- ^ at least 2 inputs
  deriving stock (Show, Eq)

-- | See @mergeInvariant@ in the prototype.
mergingRunDataInvariant :: MergingRunData t k v b -> Either String ()
mergingRunDataInvariant = \case
    CompletedMergeData _ (MR.NumRuns n) _ ->
      assertI "completed merges are non-trivial (at least two inputs)" $
        n > 1
    OngoingMergeData _ rds -> do
      assertI "inputs to ongoing merges aren't empty" $
        all nonEmptyRunDataInvariant rds
      assertI "ongoing merges are non-trivial (at least two inputs)" $
        length rds > 1
  where
    assertI msg False = Left msg
    assertI _   True  = Right ()

mapMergingRunData ::
     Ord k'
  => (k -> k') -> (v -> v') -> (b -> b')
  -> MergingRunData t k v b -> MergingRunData t k' v' b'
mapMergingRunData f g h = \case
    CompletedMergeData t n r ->
      CompletedMergeData t n $ mapRunData f g h r
    OngoingMergeData t rs ->
      OngoingMergeData t $
        map (NonEmptyRunData . mapRunData f g h . unNonEmptyRunData) rs

type SerialisedMergingRunData t =
    MergingRunData t SerialisedKey SerialisedValue SerialisedBlob

serialiseMergingRunData ::
     (SerialiseKey k, SerialiseValue v, SerialiseValue b)
  => MergingRunData t k v b -> SerialisedMergingRunData t
serialiseMergingRunData =
    mapMergingRunData serialiseKey serialiseValue serialiseBlob

{-------------------------------------------------------------------------------
  QuickCheck
-------------------------------------------------------------------------------}

labelMergingRunData ::
     Show t => SerialisedMergingRunData t -> Property -> Property
labelMergingRunData (CompletedMergeData mt _ rd) =
      tabulate "merging run state" ["CompletedMerge"]
    . tabulate "merge type" [show mt]
    . labelRunData rd
labelMergingRunData (OngoingMergeData mt rds) =
      tabulate "merging run state" ["OngoingMerge"]
    . tabulate "merge type" [show mt]
    . tabulate "merging run inputs" [showPowersOf 2 (length rds)]
    . foldr ((.) . labelRunData . unNonEmptyRunData) id rds

instance ( Arbitrary t, Ord k, Arbitrary k, Arbitrary v, Arbitrary b
         ) => Arbitrary (MergingRunData t k v b) where
  arbitrary = genMergingRunData arbitrary arbitrary arbitrary
  shrink = shrinkMergingRunData shrink shrink shrink

genMergingRunData ::
     forall t k v b. (Arbitrary t, Ord k)
  => Gen k
  -> Gen v
  -> Gen b
  -> Gen (MergingRunData t k v b)
genMergingRunData genKey genVal genBlob =
    QC.oneof
      [ do
          mt <- arbitrary
          numRuns <- MR.NumRuns <$> QC.chooseInt (2, 32)
          rd <- genRunData genKey genVal genBlob
          pure (CompletedMergeData mt numRuns rd)
      , do
          mt <- arbitrary
          n  <- QC.chooseInt (2, 8)
          rs <- QC.vectorOf n $ QC.scale (`div` n) $
            -- scaled, so overall size is similar to a single RunData
            genNonEmptyRunData genKey genVal genBlob
          pure (OngoingMergeData mt rs)
      ]

shrinkMergingRunData ::
     Ord k
  => (k -> [k])
  -> (v -> [v])
  -> (b -> [b])
  -> MergingRunData t k v b
  -> [MergingRunData t k v b]
shrinkMergingRunData shrinkKey shrinkVal shrinkBlob = \case
    CompletedMergeData mt numRuns rd ->
      [ CompletedMergeData mt numRuns' rd'
      | (numRuns', rd') <-
          liftShrink2
            (fmap MR.NumRuns . filter (> 1) . shrink . MR.unNumRuns)
            (shrinkRunData shrinkKey shrinkVal shrinkBlob)
            (numRuns, rd)
      ]
    OngoingMergeData mt rds ->
      -- [ CompletedMergeData mt md (completeM m) ]
      [ OngoingMergeData mt rds'
      | rds' <-
          liftShrink
            (shrinkNonEmptyRunData shrinkKey shrinkVal shrinkBlob)
            rds
      , length rds' > 1
      ]
