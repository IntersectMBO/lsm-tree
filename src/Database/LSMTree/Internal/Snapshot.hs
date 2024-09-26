module Database.LSMTree.Internal.Snapshot (
    SnapshotLabel
  , snapshot
  , open
  , deleteSnapshot
  , listSnapshots
  ) where

import           Control.Concurrent.Class.MonadMVar.Strict
import           Control.Concurrent.Class.MonadSTM (MonadSTM (..))
import           Control.Concurrent.Class.MonadSTM.RWVar (RWVar)
import qualified Control.Concurrent.Class.MonadSTM.RWVar as RW
import           Control.DeepSeq
import           Control.Monad (unless, void, when)
import           Control.Monad.Class.MonadThrow
import           Control.Monad.Primitive
import           Control.TempRegistry
import           Control.Tracer
import           Data.Arena (ArenaManager, newArenaManager)
import           Data.Bifunctor (Bifunctor (..))
import qualified Data.ByteString.Char8 as BSC
import           Data.Char (isNumber)
import           Data.Foldable
import           Data.Functor.Compose (Compose (..))
import           Data.Kind
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import           Data.Maybe (catMaybes)
import qualified Data.Primitive.ByteArray as P
import           Data.Primitive.MutVar
import qualified Data.Set as Set
import           Data.Typeable
import qualified Data.Vector as V
import           Data.Word (Word64)
import           Database.LSMTree.Internal.BlobRef (WeakBlobRef (..))
import qualified Database.LSMTree.Internal.BlobRef as BlobRef
import           Database.LSMTree.Internal.Config
import           Database.LSMTree.Internal.Cursor
import           Database.LSMTree.Internal.Entry (Entry)
import qualified Database.LSMTree.Internal.Entry as Entry
import           Database.LSMTree.Internal.Lookup (ByteCountDiscrepancy,
                     ResolveSerialisedValue, lookupsIO)
import           Database.LSMTree.Internal.MergeSchedule
import           Database.LSMTree.Internal.Paths (RunFsPaths (..),
                     SessionRoot (..), SnapshotName)
import qualified Database.LSMTree.Internal.Paths as Paths
import           Database.LSMTree.Internal.Range (Range (..))
import qualified Database.LSMTree.Internal.RawBytes as RB
import           Database.LSMTree.Internal.Run (Run)
import qualified Database.LSMTree.Internal.Run as Run
import           Database.LSMTree.Internal.RunNumber
import qualified Database.LSMTree.Internal.RunReader as Reader
import           Database.LSMTree.Internal.RunReaders (OffsetKey (..))
import qualified Database.LSMTree.Internal.RunReaders as Readers
import           Database.LSMTree.Internal.Serialise (SerialisedBlob (..),
                     SerialisedKey, SerialisedValue)
import           Database.LSMTree.Internal.Session
import           Database.LSMTree.Internal.TableHandle
import           Database.LSMTree.Internal.UniqCounter
import qualified Database.LSMTree.Internal.Vector as V
import           Database.LSMTree.Internal.WriteBuffer (WriteBuffer)
import qualified Database.LSMTree.Internal.WriteBuffer as WB
import           Database.LSMTree.Internal.WriteBufferBlobs (WriteBufferBlobs)
import qualified Database.LSMTree.Internal.WriteBufferBlobs as WBB
import qualified System.FS.API as FS
import           System.FS.API (FsError, FsErrorPath (..), FsPath, Handle,
                     HasFS)
import qualified System.FS.API.Lazy as FS
import qualified System.FS.API.Strict as FS
import qualified System.FS.BlockIO.API as FS
import           System.FS.BlockIO.API (HasBlockIO)

type SnapshotLabel = String

{-# SPECIALISE snapshot :: ResolveSerialisedValue -> SnapshotName -> String -> TableHandle IO h -> IO Int #-}
-- |  See 'Database.LSMTree.Normal.snapshot''.
snapshot ::
     m ~ IO -- TODO: replace by @io-classes@ constraints for IO simulation.
  => ResolveSerialisedValue
  -> SnapshotName
  -> SnapshotLabel
  -> TableHandle m h
  -> m Int
snapshot resolve snap label th = do
    traceWith (tableTracer th) $ TraceSnapshot snap
    let conf = tableConfig th
    withOpenTable th $ \thEnv -> do
      -- For the temporary implementation it is okay to just flush the buffer
      -- before taking the snapshot.
      let hfs = tableHasFS thEnv
      content <- modifyWithTempRegistry
                    (atomically $ RW.unsafeAcquireWriteAccess (tableContent thEnv))
                    (atomically . RW.unsafeReleaseWriteAccess (tableContent thEnv))
                    $ \reg content -> do
        r <- flushWriteBuffer
              (TraceMerge `contramap` tableTracer th)
              conf
              resolve
              hfs
              (tableHasBlockIO thEnv)
              (tableSessionRoot thEnv)
              (tableSessionUniqCounter thEnv)
              reg
              content
        pure (r, r)
      -- At this point, we've flushed the write buffer but we haven't created the
      -- snapshot file yet. If an asynchronous exception happens beyond this
      -- point, we'll take that loss, as the inner state of the table is still
      -- consistent.
      runNumbers <- V.forM (tableLevels content) $ \(Level mr rs) -> do
        (,V.map (runNumber . Run.runRunFsPaths) rs) <$>
          case mr of
            SingleRun r -> pure (True, runNumber (Run.runRunFsPaths r))
            MergingRun var -> do
              readMutVar var >>= \case
                CompletedMerge r -> pure (False, runNumber (Run.runRunFsPaths r))
                OngoingMerge{}   -> error "snapshot: OngoingMerge not yet supported" -- TODO: implement
      let snapPath = Paths.snapshot (tableSessionRoot thEnv) snap
      FS.doesFileExist (tableHasFS thEnv) snapPath >>= \b ->
              when b $ throwIO (ErrSnapshotExists snap)
      FS.withFile
        (tableHasFS thEnv)
        snapPath
        (FS.WriteMode FS.MustBeNew) $ \h ->
          void $ FS.hPutAllStrict (tableHasFS thEnv) h
                      (BSC.pack $ show (label, runNumbers, tableConfig th))
      pure $! V.sum (V.map (\((_ :: (Bool, RunNumber)), rs) -> 1 + V.length rs) runNumbers)

{-# SPECIALISE open :: Session IO h -> SnapshotLabel -> TableConfigOverride -> SnapshotName -> IO (TableHandle IO h) #-}
-- |  See 'Database.LSMTree.Normal.open'.
open ::
     m ~ IO -- TODO: replace by @io-classes@ constraints for IO simulation.
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
        let (label', runNumbers, conf) =
                -- why we are using read for this?
                -- apparently this is a temporary solution, to be done properly in WP15
                read @(SnapshotLabel, V.Vector ((Bool, RunNumber), V.Vector RunNumber), TableConfig) $
                BSC.unpack $ BSC.toStrict $ bs

        let conf' = applyOverride override conf
        unless (label == label') $ throwIO (ErrSnapshotWrongType snap)
        let runPaths = V.map (bimap (second $ RunFsPaths (Paths.activeDir $ sessionRoot seshEnv))
                                    (V.map (RunFsPaths (Paths.activeDir $ sessionRoot seshEnv))))
                            runNumbers
        lvls <- openLevels reg hfs hbio (confDiskCachePolicy conf') runPaths
        am <- newArenaManager
        blobpath <- Paths.tableBlobPath (sessionRoot seshEnv) <$>
                      incrUniqCounter (sessionUniqCounter seshEnv)
        wbblobs  <- WBB.new hfs blobpath
        newWith sesh seshEnv conf' am WB.empty wbblobs lvls

{-# SPECIALISE openLevels :: TempRegistry IO -> HasFS IO h -> HasBlockIO IO h -> DiskCachePolicy -> V.Vector ((Bool, RunFsPaths), V.Vector RunFsPaths) -> IO (Levels IO (FS.Handle h)) #-}
-- | Open multiple levels.
openLevels ::
     m ~ IO -- TODO: replace by @io-classes@ constraints for IO simulation.
  => TempRegistry m
  -> HasFS m h
  -> HasBlockIO m h
  -> DiskCachePolicy
  -> V.Vector ((Bool, RunFsPaths), V.Vector RunFsPaths)
  -> m (Levels m (Handle h))
openLevels reg hfs hbio diskCachePolicy levels =
    flip V.imapMStrict levels $ \i (mrPath, rsPaths) -> do
      let ln      = LevelNo (i+1) -- level 0 is the write buffer
          caching = diskCachePolicyForLevel diskCachePolicy ln
      !r <- allocateTemp reg
              (Run.openFromDisk hfs hbio caching (snd mrPath))
              Run.removeReference
      !rs <- flip V.mapMStrict rsPaths $ \run ->
        allocateTemp reg
          (Run.openFromDisk hfs hbio caching run)
          Run.removeReference
      var <- newMutVar (CompletedMerge r)
      let !mr = if fst mrPath then SingleRun r else MergingRun var
      pure $! Level mr rs

{-# SPECIALISE deleteSnapshot :: Session IO h -> SnapshotName -> IO () #-}
-- |  See 'Database.LSMTree.Common.deleteSnapshot'.
deleteSnapshot ::
     m ~ IO -- TODO: replace by @io-classes@ constraints for IO simulation.
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
     m ~ IO -- TODO: replace by @io-classes@ constraints for IO simulation.
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
