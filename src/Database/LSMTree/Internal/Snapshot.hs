-- TODO: remove once we properly implement snapshots
{-# OPTIONS_GHC -Wno-orphans #-}

module Database.LSMTree.Internal.Snapshot (
    -- * Snapshot format
    numSnapRuns
  , SnapLevels
  , SnapLevel (..)
  , SnapMergingRun (..)
  , SnapMergingRunState (..)
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
import           Data.Foldable (forM_, traverse_)
import           Data.Primitive.PrimVar
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
  Levels snapshot format
-------------------------------------------------------------------------------}

numSnapRuns :: SnapLevels -> Int
numSnapRuns sl = V.sum $ V.map go1 sl
  where
    go1 (SnapLevel sir srr) = go2 sir + V.length srr
    go2 (SnapMergingRun _ _ _ smrs) = go3 smrs
    go2 (SnapSingleRun _rn)         = 1
    go3 (SnapCompletedMerge _rn)   = 1
    go3 (SnapOngoingMerge rns _ _) = V.length rns

type SnapLevels = V.Vector SnapLevel

data SnapLevel = SnapLevel {
    snapIncomingRuns :: !SnapMergingRun
  , snapResidentRuns :: !(V.Vector RunNumber)
  }
  deriving stock (Show, Eq, Read)

data SnapMergingRun =
    SnapMergingRun !MergePolicyForLevel !NumRuns !AccumulatedCredits !SnapMergingRunState
  | SnapSingleRun !RunNumber
  deriving stock (Show, Eq, Read)

newtype AccumulatedCredits = AccumulatedCredits {
    unAccumulatedCredits :: Int
  }
  deriving stock (Show, Eq, Read)

data SnapMergingRunState =
    SnapCompletedMerge !RunNumber
  | SnapOngoingMerge !(V.Vector RunNumber) !NumStepsDone {- merge -} !Merge.Level
  deriving stock (Show, Eq, Read)

newtype NumStepsDone = NumStepsDone { unNumStepsDone :: Int }
  deriving stock (Show, Eq)

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
    smr <- snapMergingRun incomingRuns
    pure (SnapLevel smr (V.map runNumber residentRuns))

{-# SPECIALISE snapMergingRun :: MergingRun IO h -> IO SnapMergingRun #-}
snapMergingRun ::
     (PrimMonad m, MonadMVar m)
  => MergingRun m h
  -> m SnapMergingRun
snapMergingRun (MergingRun mpfl nr acVar mrsVar) = do
    ac <- readPrimVar acVar
    smrs <- withMVar mrsVar $ \mrs -> snapMergingRunState mrs
    pure (SnapMergingRun mpfl nr (AccumulatedCredits ac) smrs)
snapMergingRun (SingleRun r) = pure (SnapSingleRun (runNumber r))

{-# SPECIALISE snapMergingRunState :: MergingRunState IO h -> IO SnapMergingRunState #-}
snapMergingRunState ::
     PrimMonad m
  => MergingRunState m h
  -> m SnapMergingRunState
snapMergingRunState (CompletedMerge r) = pure (SnapCompletedMerge (runNumber r))
snapMergingRunState (OngoingMerge rs nsdVar m) = do
    nsd <- readPrimVar nsdVar
    pure (SnapOngoingMerge (V.map runNumber rs) (NumStepsDone nsd) (Merge.mergeLevel m))

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
        (mmmay, incomingRuns) <- openMergingRun snapIncomingRuns
        -- When a snapshot is created, merge progress is lost, so we have to
        -- redo merging work here. NumStepsDone tracks how much merging work
        -- happened before the snapshot was taken.
        --
        -- TODO: this use of supplyMergeCredits is leaky! If a merge completes
        -- in supplyMergeCredits, then the resulting run is not tracked in the
        -- registry, and closing the input runs is also not tracked in the
        -- registry. Note, however, that this bit of code is likely to change in
        -- #392.
        forM_ mmmay $ \(NumStepsDone c) -> supplyMergeCredits (Credit c) (creditThresholdForLevel conf ln) incomingRuns
        residentRuns <- V.forM snapResidentRuns $ \rn ->
          allocateTemp reg
            (Run.openFromDisk hfs hbio caching (mkPath rn))
            Run.removeReference
        pure Level{..}
      where
        caching = diskCachePolicyForLevel confDiskCachePolicy ln
        alloc = bloomFilterAllocForLevel conf ln

        openMergingRun :: SnapMergingRun -> m (Maybe NumStepsDone, MergingRun m h)
        openMergingRun (SnapMergingRun mpfl nr ac smrs) = do
            (n, mrs) <- openMergingRunState smrs
            acVar <- newPrimVar (unAccumulatedCredits ac)
            (n,) . MergingRun mpfl nr acVar <$> newMVar mrs
        openMergingRun (SnapSingleRun rn) =
            (Nothing,) . SingleRun <$>
              allocateTemp reg
                (Run.openFromDisk hfs hbio caching (mkPath rn))
                Run.removeReference

        openMergingRunState :: SnapMergingRunState -> m (Maybe NumStepsDone, MergingRunState m h)
        openMergingRunState (SnapCompletedMerge rn) =
            (Nothing,) . CompletedMerge <$>
              allocateTemp reg
                (Run.openFromDisk hfs hbio caching (mkPath rn))
                Run.removeReference
        openMergingRunState (SnapOngoingMerge rns nsd mergeLast) = do
            rs <- V.forM rns $ \rn ->
              allocateTemp reg
                (Run.openFromDisk hfs hbio caching ((mkPath rn)))
                Run.removeReference
            nsdVar <- newPrimVar $! unNumStepsDone nsd
            rn <- uniqueToRunNumber <$> incrUniqCounter uc
            mergeMaybe <- allocateTemp reg
              (Merge.new hfs hbio caching alloc mergeLast resolve (mkPath rn) rs)
              (traverse_ Merge.removeReference)
            case mergeMaybe of
              Nothing -> error "openLevels: merges can not be empty"
              Just m  -> pure (Just nsd, OngoingMerge rs nsdVar m)

{-------------------------------------------------------------------------------
  Levels
-------------------------------------------------------------------------------}

deriving stock instance Read NumRuns
deriving stock instance Read MergePolicyForLevel
deriving stock instance Read NumStepsDone
deriving newtype instance Read RunNumber
deriving stock instance Read Merge.Level

{-------------------------------------------------------------------------------
  Config
-------------------------------------------------------------------------------}

deriving stock instance Read TableConfig
deriving stock instance Read WriteBufferAlloc
deriving stock instance Read NumEntries
deriving stock instance Read SizeRatio
deriving stock instance Read MergePolicy
deriving stock instance Read BloomFilterAlloc
deriving stock instance Read FencePointerIndex
