{-# LANGUAGE DataKinds #-}

-- TODO: remove once we properly implement snapshots
{-# OPTIONS_GHC -Wno-orphans #-}

module Database.LSMTree.Internal.Snapshot (
    SnapshotLabel
  , snapshot
  , open
  , deleteSnapshot
  , listSnapshots
  ) where

import           Control.Concurrent.Class.MonadMVar.Strict
import           Control.Concurrent.Class.MonadSTM (MonadSTM (..))
import qualified Control.Concurrent.Class.MonadSTM.RWVar as RW
import           Control.Monad (unless, void, when)
import           Control.Monad.Class.MonadST (MonadST (..))
import           Control.Monad.Class.MonadThrow
import           Control.Monad.Fix (MonadFix)
import           Control.Monad.Primitive
import           Control.TempRegistry
import           Control.Tracer
import           Data.Arena (newArenaManager)
import qualified Data.ByteString.Char8 as BSC
import           Data.Maybe (catMaybes)
import qualified Data.Set as Set
import qualified Data.Vector as V
import           Database.LSMTree.Internal.Config
import           Database.LSMTree.Internal.Entry
import           Database.LSMTree.Internal.Lookup (ResolveSerialisedValue)
import qualified Database.LSMTree.Internal.Merge as Merge
import           Database.LSMTree.Internal.MergeSchedule
import           Database.LSMTree.Internal.Paths (SessionRoot, SnapshotName)
import qualified Database.LSMTree.Internal.Paths as Paths
import           Database.LSMTree.Internal.Run (Run)
import qualified Database.LSMTree.Internal.Run as Run
import           Database.LSMTree.Internal.RunNumber
import           Database.LSMTree.Internal.Session
import           Database.LSMTree.Internal.TableHandle
import           Database.LSMTree.Internal.UniqCounter
import qualified Database.LSMTree.Internal.WriteBuffer as WB
import qualified Database.LSMTree.Internal.WriteBufferBlobs as WBB
import qualified System.FS.API as FS
import           System.FS.API (HasFS)
import qualified System.FS.API.Lazy as FS
import qualified System.FS.API.Strict as FS
import           System.FS.BlockIO.API (HasBlockIO)

type SnapshotLabel = String

{-# SPECIALISE snapshot ::
     ResolveSerialisedValue
  -> SnapshotName
  -> String
  -> TableHandle IO h
  -> IO Int #-}
-- |  See 'Database.LSMTree.Normal.snapshot''.
snapshot ::
     (MonadFix m, MonadMask m, MonadMVar m, MonadST m, MonadSTM m)
  => ResolveSerialisedValue
  -> SnapshotName
  -> SnapshotLabel
  -> TableHandle m h
  -> m Int
snapshot resolve snap label th = do
    traceWith (tableTracer th) $ TraceSnapshot snap
    let conf = tableConfig th
    withOpenTable th $ \thEnv -> do
      let hfs = tableHasFS thEnv
      let snapPath = Paths.snapshot (tableSessionRoot thEnv) snap
      FS.doesFileExist (tableHasFS thEnv) snapPath >>= \b ->
              when b $ throwIO (ErrSnapshotExists snap)

      -- For the temporary implementation it is okay to just flush the buffer
      -- before taking the snapshot.
      content <- modifyWithTempRegistry
                    (RW.unsafeAcquireWriteAccess (tableContent thEnv))
                    (atomically . RW.unsafeReleaseWriteAccess (tableContent thEnv))
                    $ \reg content -> do
        content' <- flushWriteBuffer
              (TraceMerge `contramap` tableTracer th)
              conf
              resolve
              hfs
              (tableHasBlockIO thEnv)
              (tableSessionRoot thEnv)
              (tableSessionUniqCounter thEnv)
              reg
              content
        pure (content', content')
      -- At this point, we've flushed the write buffer but we haven't created the
      -- snapshot file yet. If an asynchronous exception happens beyond this
      -- point, we'll take that loss, as the inner state of the table is still
      -- consistent.

      snappedLevels <- snapLevels (tableLevels content)
      let snapContents = BSC.pack $ show (label, snappedLevels, tableConfig th)

      FS.withFile
        (tableHasFS thEnv)
        snapPath
        (FS.WriteMode FS.MustBeNew) $ \h ->
          void $ FS.hPutAllStrict (tableHasFS thEnv) h snapContents

      pure $! numSnapRuns snappedLevels

{-# SPECIALISE open ::
     Session IO h
  -> SnapshotLabel
  -> TableConfigOverride
  -> SnapshotName
  -> IO (TableHandle IO h) #-}
-- |  See 'Database.LSMTree.Normal.open'.
open ::
     (MonadFix m, MonadMask m, MonadMVar m, MonadST m, MonadSTM m)
  => Session m h
  -> SnapshotLabel -- ^ Expected label
  -> TableConfigOverride -- ^ Optional config override
  -> SnapshotName
  -> m (TableHandle m h)
open sesh label override snap = do
    traceWith (sessionTracer sesh) $ TraceOpenSnapshot snap override
    withOpenSession sesh $ \seshEnv -> do
      withTempRegistry $ \reg -> do
        let hfs      = sessionHasFS seshEnv
            hbio     = sessionHasBlockIO seshEnv
            snapPath = Paths.snapshot (sessionRoot seshEnv) snap
        FS.doesFileExist hfs snapPath >>= \b ->
          unless b $ throwIO (ErrSnapshotNotExists snap)
        bs <- FS.withFile
                hfs
                snapPath
                FS.ReadMode $ \h ->
                  FS.hGetAll (sessionHasFS seshEnv) h
        let (label', snappedLevels, conf) = read $ BSC.unpack $ BSC.toStrict $ bs
        unless (label == label') $ throwIO (ErrSnapshotWrongType snap)
        let conf' = applyOverride override conf
        am <- newArenaManager
        blobpath <- Paths.tableBlobPath (sessionRoot seshEnv) <$>
                      incrUniqCounter (sessionUniqCounter seshEnv)
        tableWriteBufferBlobs
          <- allocateTemp reg
               (WBB.new hfs blobpath)
               WBB.removeReference
        tableLevels <- openLevels reg hfs hbio conf (sessionRoot seshEnv) snappedLevels
        tableCache <- mkLevelsCache reg tableLevels
        newWith reg sesh seshEnv conf' am $! TableContent {
            tableWriteBuffer = WB.empty
          , tableWriteBufferBlobs
          , tableLevels
          , tableCache
          }

{-# SPECIALISE deleteSnapshot ::
     Session IO h
  -> SnapshotName
  -> IO () #-}
-- |  See 'Database.LSMTree.Common.deleteSnapshot'.
deleteSnapshot ::
     (MonadFix m, MonadMask m, MonadSTM m)
  => Session m h
  -> SnapshotName
  -> m ()
deleteSnapshot sesh snap = do
    traceWith (sessionTracer sesh) $ TraceDeleteSnapshot snap
    withOpenSession sesh $ \seshEnv -> do
      let hfs = sessionHasFS seshEnv
          snapPath = Paths.snapshot (sessionRoot seshEnv) snap
      FS.doesFileExist hfs snapPath >>= \b ->
        unless b $ throwIO (ErrSnapshotNotExists snap)
      FS.removeFile hfs snapPath

{-# SPECIALISE listSnapshots :: Session IO h -> IO [SnapshotName] #-}
-- |  See 'Database.LSMTree.Common.listSnapshots'.
listSnapshots ::
     (MonadFix m, MonadMask m, MonadSTM m)
  => Session m h
  -> m [SnapshotName]
listSnapshots sesh = do
    traceWith (sessionTracer sesh) TraceListSnapshots
    withOpenSession sesh $ \seshEnv -> do
      let hfs = sessionHasFS seshEnv
          root = sessionRoot seshEnv
      contents <- FS.listDirectory hfs (Paths.snapshotsDir (sessionRoot seshEnv))
      snaps <- mapM (checkSnapshot hfs root) $ Set.toList contents
      pure $ catMaybes snaps
  where
    checkSnapshot hfs root s =
      case Paths.mkSnapshotName s of
        Nothing   -> pure Nothing
        Just snap -> do
          -- check that it is a file
          b <- FS.doesFileExist hfs (Paths.snapshot root snap)
          if b then pure $ Just snap
               else pure $ Nothing

{-------------------------------------------------------------------------------
  Levels snapshot format
-------------------------------------------------------------------------------}

numSnapRuns :: SnapLevels -> Int
numSnapRuns sl = V.sum $ V.map go1 sl
  where
    go1 (SnapLevel sir srr) = go2 sir + V.length srr
    go2 (SnapMergingRun smrs) = go3 smrs
    go2 (SnapSingleRun _rn)   = 1
    go3 (SnapCompletedMerge _rn) = 1
    go3 (SnapOngoingMerge rns)   = V.length rns

type SnapLevels = V.Vector SnapLevel

data SnapLevel = SnapLevel {
    snapIncomingRuns :: !SnapMergingRun
  , snapResidentRuns :: !(V.Vector RunNumber)
  }
  deriving stock (Show, Eq, Read)

data SnapMergingRun =
    SnapMergingRun !SnapMergingRunState
  | SnapSingleRun !RunNumber
  deriving stock (Show, Eq, Read)

data SnapMergingRunState =
    SnapCompletedMerge !RunNumber
  | SnapOngoingMerge !(V.Vector RunNumber) {- merge -}
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
    smr <- snapMergingRun incomingRuns
    pure (SnapLevel smr (V.map runNumber residentRuns))

{-# SPECIALISE snapMergingRun :: MergingRun IO h -> IO SnapMergingRun #-}
snapMergingRun ::
     (PrimMonad m, MonadMVar m)
  => MergingRun m h
  -> m SnapMergingRun
snapMergingRun (MergingRun mrsVar) = do
    smrs <- withMVar mrsVar $ \mrs -> snapMergingRunState mrs
    pure (SnapMergingRun smrs)
snapMergingRun (SingleRun r) = pure (SnapSingleRun (runNumber r))

{-# SPECIALISE snapMergingRunState :: MergingRunState IO h -> IO SnapMergingRunState #-}
snapMergingRunState ::
     PrimMonad m
  => MergingRunState m h
  -> m SnapMergingRunState
snapMergingRunState (CompletedMerge r) = pure (SnapCompletedMerge (runNumber r))
snapMergingRunState (OngoingMerge rs _m) = do
    pure (SnapOngoingMerge (V.map runNumber rs))

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
  -> SessionRoot
  -> SnapLevels
  -> IO (Levels IO h)
  #-}
openLevels ::
     forall m h. (MonadFix m, MonadMask m, MonadMVar m, MonadSTM m, MonadST m)
  => TempRegistry m
  -> HasFS m h
  -> HasBlockIO m h
  -> TableConfig
  -> SessionRoot
  -> SnapLevels
  -> m (Levels m h)
openLevels reg hfs hbio TableConfig{..} sessionRoot levels =
    V.iforM levels $ \i -> openLevel (LevelNo (i+1))
  where
    mkPath = Paths.RunFsPaths (Paths.activeDir sessionRoot)

    openLevel :: LevelNo -> SnapLevel -> m (Level m h)
    openLevel ln SnapLevel{..} = do
        incomingRuns <- openMergingRun snapIncomingRuns
        residentRuns <- V.forM snapResidentRuns $ \rn ->
          allocateTemp reg
            (Run.openFromDisk hfs hbio caching (mkPath rn))
            Run.removeReference
        pure Level{..}
      where
        caching = diskCachePolicyForLevel confDiskCachePolicy ln

        openMergingRun :: SnapMergingRun -> m (MergingRun m h)
        openMergingRun (SnapMergingRun smrs) = do
            mrs <- openMergingRunState smrs
            MergingRun <$> newMVar mrs
        openMergingRun (SnapSingleRun rn) =
            SingleRun <$>
              allocateTemp reg
                (Run.openFromDisk hfs hbio caching (mkPath rn))
                Run.removeReference

        openMergingRunState :: SnapMergingRunState -> m (MergingRunState m h)
        openMergingRunState (SnapCompletedMerge rn) =
            CompletedMerge <$>
              allocateTemp reg
                (Run.openFromDisk hfs hbio caching (mkPath rn))
                Run.removeReference
        openMergingRunState (SnapOngoingMerge _rns) = do
            error "openLevels: ongoing merge not yet supported"

{-------------------------------------------------------------------------------
  Levels
-------------------------------------------------------------------------------}

deriving stock instance Read MergePolicyForLevel
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
