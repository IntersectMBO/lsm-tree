module Database.LSMTree.Internal.Snapshot (
    -- * Snapshot metadata
    SnapshotMetaData (..)
  , SnapshotLabel (..)
  , SnapshotTableType (..)
    -- * Snapshot format
  , numSnapRuns
  , SnapLevels
  , SnapLevel (..)
  , SnapIncomingRun (..)
  , UnspentCredits (..)
  , SnapMergingRunState (..)
  , SpentCredits (..)
    -- * Creating snapshots
  , snapLevels
    -- * Opening snapshots
  , openLevels
  ) where

import           Control.Concurrent.Class.MonadMVar.Strict
import           Control.Concurrent.Class.MonadSTM (MonadSTM)
import           Control.Monad.Class.MonadST (MonadST)
import           Control.Monad.Class.MonadThrow (MonadMask)
import           Control.Monad.Fix (MonadFix)
import           Control.Monad.Primitive (PrimMonad)
import           Control.TempRegistry
import           Data.Primitive (readMutVar)
import           Data.Primitive.PrimVar
import           Data.Text (Text)
import qualified Data.Vector as V
import           Database.LSMTree.Internal.Config
import           Database.LSMTree.Internal.Entry
import           Database.LSMTree.Internal.Lookup (ResolveSerialisedValue)
import qualified Database.LSMTree.Internal.Merge as Merge
import           Database.LSMTree.Internal.MergeSchedule
import           Database.LSMTree.Internal.Paths (SessionRoot)
import qualified Database.LSMTree.Internal.Paths as Paths
import           Database.LSMTree.Internal.Run (Run)
import qualified Database.LSMTree.Internal.Run as Run
import           Database.LSMTree.Internal.RunNumber
import           Database.LSMTree.Internal.UniqCounter (UniqCounter,
                     incrUniqCounter, uniqueToRunNumber)
import           System.FS.API (HasFS)
import           System.FS.BlockIO.API (HasBlockIO)

{-------------------------------------------------------------------------------
  Snapshot metadata
-------------------------------------------------------------------------------}

-- | Custom text to include in a snapshot file
newtype SnapshotLabel = SnapshotLabel Text
  deriving stock (Show, Eq)

data SnapshotTableType = SnapNormalTable | SnapMonoidalTable
  deriving stock (Show, Eq)

data SnapshotMetaData = SnapshotMetaData {
    -- | Custom, user-supplied text that is included in the metadata.
    --
    -- The main use case for this field is for the user to supply textual
    -- information about the key\/value\/blob type for the table that
    -- corresponds to the snapshot. This information can then be used to
    -- dynamically check that a snapshot is opened at the correct
    -- key\/value\/blob type.
    --
    -- One could argue that the 'SnapshotName' could be used to to hold this
    -- information, but the file name of snapshot metadata is not guarded by a
    -- checksum, wherease the contents of the file are. Therefore using the
    -- 'SnapshotLabel' is safer.
    snapMetaLabel     :: !SnapshotLabel
    -- | Whether a table is normal or monoidal.
    --
    -- TODO: if we at some point decide to get rid of the normal vs. monoidal
    -- distinction, we can get rid of this field.
  , snapMetaTableType :: !SnapshotTableType
    -- | The 'TableConfig' for the snapshotted table.
    --
    -- Some of these configuration options can be overridden when a snapshot is
    -- opened: see 'TableConfigOverride'.
  , snapMetaConfig    :: !TableConfig
    -- | The shape of the LSM tree.
  , snapMetaLevels    :: !SnapLevels
  }
  deriving stock (Show, Eq)

{-------------------------------------------------------------------------------
  Levels snapshot format
-------------------------------------------------------------------------------}

numSnapRuns :: SnapLevels -> Int
numSnapRuns sl = V.sum $ V.map go1 sl
  where
    go1 (SnapLevel sir srr) = go2 sir + V.length srr
    go2 (SnapMergingRun _ _ _ _ _ smrs) = go3 smrs
    go2 (SnapSingleRun _rn)             = 1
    go3 (SnapCompletedMerge _rn)   = 1
    go3 (SnapOngoingMerge rns _ _) = V.length rns

type SnapLevels = V.Vector SnapLevel

data SnapLevel = SnapLevel {
    snapIncoming     :: !SnapIncomingRun
  , snapResidentRuns :: !(V.Vector RunNumber)
  }
  deriving stock (Show, Eq)

data SnapIncomingRun =
    SnapMergingRun !MergePolicyForLevel !NumRuns !NumEntries !UnspentCredits !MergeKnownCompleted !SnapMergingRunState
  | SnapSingleRun !RunNumber
  deriving stock (Show, Eq)

-- | The total number of unspent credits. This total is used in combination with
-- 'SpentCredits' on snapshot load to restore merging work that was lost when
-- the snapshot was created.
newtype UnspentCredits = UnspentCredits { getUnspentCredits :: Int }
  deriving stock (Show, Eq, Read)

data SnapMergingRunState =
    SnapCompletedMerge !RunNumber
  | SnapOngoingMerge !(V.Vector RunNumber) !SpentCredits !Merge.Level
  deriving stock (Show, Eq)

-- | The total number of spent credits. This total is used in combination with
-- 'UnspentCedits' on snapshot load to restore merging work that was lost when
-- the snapshot was created.
newtype SpentCredits = SpentCredits { getSpentCredits :: Int }
  deriving stock (Show, Eq, Read)

{-------------------------------------------------------------------------------
  Conversion to snapshot format
-------------------------------------------------------------------------------}

{-# SPECIALISE snapLevels :: Levels IO h -> IO SnapLevels #-}
snapLevels ::
     (PrimMonad m, MonadMVar m)
  => Levels m h
  -> m SnapLevels
snapLevels = V.mapM snapLevel

{-# SPECIALISE snapLevel :: Level IO h -> IO SnapLevel #-}
snapLevel ::
     (PrimMonad m, MonadMVar m)
  => Level m h
  -> m SnapLevel
snapLevel Level{..} = do
    sir <- snapIncomingRun incomingRun
    pure (SnapLevel sir (V.map runNumber residentRuns))

{-# SPECIALISE snapIncomingRun :: IncomingRun IO h -> IO SnapIncomingRun #-}
snapIncomingRun ::
     (PrimMonad m, MonadMVar m)
  => IncomingRun m h
  -> m SnapIncomingRun
snapIncomingRun (Single r) = pure (SnapSingleRun (runNumber r))
-- We need to know how many credits were yet unspent so we can restore merge
-- work on snapshot load. No need to snapshot the contents of totalStepsVar
-- here, since we still start counting from 0 again when loading the snapshot.
snapIncomingRun (Merging MergingRun {..}) = do
    unspentCredits <- readPrimVar (getUnspentCreditsVar mergeUnspentCredits)
    mergeCompletedCache <- readMutVar mergeKnownCompleted
    smrs <- withMVar mergeState $ \mrs -> snapMergingRunState mrs
    pure $
      SnapMergingRun
        mergePolicy
        mergeNumRuns
        mergeNumEntries
        (UnspentCredits unspentCredits)
        mergeCompletedCache
        smrs

{-# SPECIALISE snapMergingRunState :: MergingRunState IO h -> IO SnapMergingRunState #-}
snapMergingRunState ::
     PrimMonad m
  => MergingRunState m h
  -> m SnapMergingRunState
snapMergingRunState (CompletedMerge r) = pure (SnapCompletedMerge (runNumber r))
-- We need to know how many credits were spent already so we can restore merge
-- work on snapshot load.
snapMergingRunState (OngoingMerge rs (SpentCreditsVar spentCreditsVar) m) = do
    spentCredits <- readPrimVar spentCreditsVar
    pure (SnapOngoingMerge (V.map runNumber rs) (SpentCredits spentCredits) (Merge.mergeLevel m))

runNumber :: Run m h -> RunNumber
runNumber r = Paths.runNumber (Run.runRunFsPaths r)

{-------------------------------------------------------------------------------
  Opening from snapshot format
-------------------------------------------------------------------------------}

{-# SPECIALISE openLevels ::
     TempRegistry IO
  -> HasFS IO h
  -> HasBlockIO IO h
  -> TableConfig
  -> UniqCounter IO
  -> SessionRoot
  -> ResolveSerialisedValue
  -> SnapLevels
  -> IO (Levels IO h)
  #-}
openLevels ::
     forall m h. (MonadFix m, MonadMask m, MonadMVar m, MonadSTM m, MonadST m)
  => TempRegistry m
  -> HasFS m h
  -> HasBlockIO m h
  -> TableConfig
  -> UniqCounter m
  -> SessionRoot
  -> ResolveSerialisedValue
  -> SnapLevels
  -> m (Levels m h)
openLevels reg hfs hbio conf@TableConfig{..} uc sessionRoot resolve levels =
    V.iforM levels $ \i -> openLevel (LevelNo (i+1))
  where
    mkPath = Paths.RunFsPaths (Paths.activeDir sessionRoot)

    openLevel :: LevelNo -> SnapLevel -> m (Level m h)
    openLevel ln SnapLevel{..} = do
        (unspentCreditsMay, spentCreditsMay, incomingRun) <- openIncomingRun snapIncoming
        -- When a snapshot is created, merge progress is lost, so we have to
        -- redo merging work here. UnspentCredits and SpentCredits track how
        -- many credits were supplied before the snapshot was taken.
        --
        -- TODO: this use of supplyMergeCredits is leaky! If a merge completes
        -- in supplyMergeCredits, then the resulting run is not tracked in the
        -- registry, and closing the input runs is also not tracked in the
        -- registry. Note, however, that this bit of code is likely to change in
        -- #392.
        let c = maybe 0 getUnspentCredits unspentCreditsMay
              + maybe 0 getSpentCredits spentCreditsMay
        supplyMergeCredits
          (ScaledCredits c)
          (creditThresholdForLevel conf ln)
          incomingRun
        residentRuns <- V.forM snapResidentRuns $ \rn ->
          allocateTemp reg
            (Run.openFromDisk hfs hbio caching (mkPath rn))
            Run.removeReference
        pure Level{..}
      where
        caching = diskCachePolicyForLevel confDiskCachePolicy ln
        alloc = bloomFilterAllocForLevel conf ln

        openIncomingRun :: SnapIncomingRun -> m (Maybe UnspentCredits, Maybe SpentCredits, IncomingRun m h)
        openIncomingRun (SnapMergingRun mpfl nr ne unspentCredits knownCompleted smrs) = do
            (spentCreditsMay, mrs) <- openMergingRunState smrs
            (Just unspentCredits, spentCreditsMay,) . Merging <$>
              newMergingRun mpfl nr ne knownCompleted mrs
        openIncomingRun (SnapSingleRun rn) =
            (Nothing, Nothing,) . Single <$>
              allocateTemp reg
                (Run.openFromDisk hfs hbio caching (mkPath rn))
                Run.removeReference

        openMergingRunState :: SnapMergingRunState -> m (Maybe SpentCredits, MergingRunState m h)
        openMergingRunState (SnapCompletedMerge rn) =
            (Nothing,) . CompletedMerge <$>
              allocateTemp reg
                (Run.openFromDisk hfs hbio caching (mkPath rn))
                Run.removeReference
        openMergingRunState (SnapOngoingMerge rns spentCredits mergeLast) = do
            rs <- V.forM rns $ \rn ->
              allocateTemp reg
                (Run.openFromDisk hfs hbio caching ((mkPath rn)))
                Run.removeReference
            -- Initialse the variable with 0. Credits will be re-supplied later,
            -- which will ensure that this variable is updated.
            spentCreditsVar <- SpentCreditsVar <$> newPrimVar 0
            rn <- uniqueToRunNumber <$> incrUniqCounter uc
            mergeMaybe <- allocateMaybeTemp reg
              (Merge.new hfs hbio caching alloc mergeLast resolve (mkPath rn) rs)
              Merge.abort
            case mergeMaybe of
              Nothing -> error "openLevels: merges can not be empty"
              Just m  -> pure (Just spentCredits, OngoingMerge rs spentCreditsVar m)
