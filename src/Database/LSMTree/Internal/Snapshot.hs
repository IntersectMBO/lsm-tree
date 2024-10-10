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
numSnapRuns a = V.sum $ V.map go1 a
  where
    go1 (SnapLevel b c) = go2 b + V.length c
    go2 (SnapMergingRun _ _ d) = go3 d
    go2 (SnapSingleRun _)      = 1
    go3 (SnapCompletedMerge _)   = 1
    go3 (SnapOngoingMerge e _ _) = V.length e

type SnapLevels = V.Vector SnapLevel

data SnapLevel = SnapLevel {
    snapIncomingRuns :: !SnapMergingRun
  , snapResidentRuns :: !(V.Vector RunNumber)
  }
  deriving stock (Show, Eq, Read)

data SnapMergingRun =
    SnapMergingRun !MergePolicyForLevel !NumRuns !SnapMergingRunState
  | SnapSingleRun !RunNumber
  deriving stock (Show, Eq, Read)

data SnapMergingRunState =
    SnapCompletedMerge !RunNumber
  | SnapOngoingMerge !(V.Vector RunNumber) !NumStepsDone {- merge -} !Merge.Level
  deriving stock (Show, Eq, Read)

{-------------------------------------------------------------------------------
  Conversion to snapshot format
-------------------------------------------------------------------------------}

snapLevels ::
     (PrimMonad m, MonadMVar m)
  => Levels m h
  -> m SnapLevels
snapLevels = V.mapM snapLevel

snapLevel ::
     (PrimMonad m, MonadMVar m)
  => Level m h
  -> m SnapLevel
snapLevel Level{..} = do
    smr <- snapMergingRun incomingRuns
    pure (SnapLevel smr (V.map runNumber residentRuns))

snapMergingRun ::
     (PrimMonad m, MonadMVar m)
  => MergingRun m h
  -> m SnapMergingRun
snapMergingRun (MergingRun mpfl nr mrsVar) = do
    smrs <- withMVar mrsVar $ \mrs -> snapMergingRunState mrs
    pure (SnapMergingRun mpfl nr smrs)
snapMergingRun (SingleRun r) = pure (SnapSingleRun (runNumber r))

snapMergingRunState ::
     PrimMonad m
  => MergingRunState m h
  -> m SnapMergingRunState
snapMergingRunState (CompletedMerge r) = pure (SnapCompletedMerge (runNumber r))
snapMergingRunState (OngoingMerge rs nsdVar m) = do
    nsd <- readPrimVar nsdVar
    pure (SnapOngoingMerge (V.map runNumber rs) nsd (Merge.mergeLevel m))

runNumber :: Run m h -> RunNumber
runNumber r = Paths.runNumber (Run.runRunFsPaths r)

{-------------------------------------------------------------------------------
  Opening from snapshot format
-------------------------------------------------------------------------------}

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
        forM_ mmmay $ \c -> supplyMergeCredits c incomingRuns
        residentRuns <- V.forM snapResidentRuns $ \rn ->
          allocateTemp reg
            (Run.openFromDisk hfs hbio caching (mkPath rn))
            Run.removeReference
        pure Level{..}
      where
        caching = diskCachePolicyForLevel confDiskCachePolicy ln
        alloc = bloomFilterAllocForLevel conf ln

        openMergingRun :: SnapMergingRun -> m (Maybe Int, MergingRun m h)
        openMergingRun (SnapMergingRun mpfl nr smrs) = do
            (n, mrs) <- openMergingRunState smrs
            (n,) . MergingRun mpfl nr <$> newMVar mrs
        openMergingRun (SnapSingleRun rn) =
            (Nothing,) . SingleRun <$>
              allocateTemp reg
                (Run.openFromDisk hfs hbio caching (mkPath rn))
                Run.removeReference

        openMergingRunState :: SnapMergingRunState -> m (Maybe Int, MergingRunState m h)
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
            nsdVar <- newPrimVar nsd
            rn <- uniqueToRunNumber <$> incrUniqCounter uc
            mergeMaybe <- allocateTemp reg
              (Merge.new hfs hbio caching alloc mergeLast resolve (mkPath rn) rs)
              (traverse_ Merge.removeReference)
            -- TODO: progress merge
            -- TODO: write test that shows a failure because we are not progressing the merge
            case mergeMaybe of
              Nothing -> error "openLevels: merges can not be empty"
              Just m  -> pure (Just (unNumStepsDone nsd), OngoingMerge rs nsdVar m)

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
