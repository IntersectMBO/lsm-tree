module System.FS.BlockIO.Sim (
    fromHasFS
    -- * Initialisation helpers
  , simHasBlockIO
  , simHasBlockIO'
  , simErrorHasBlockIO
  , simErrorHasBlockIO'
  ) where

import           Control.Concurrent.Class.MonadMVar
import           Control.Concurrent.Class.MonadSTM.Strict
import           Control.Monad (void)
import           Control.Monad.Class.MonadThrow
import           Control.Monad.Primitive (PrimMonad)
import qualified Data.ByteString.Char8 as BS
import           System.FS.API as API
import qualified System.FS.API.Lazy as API
import qualified System.FS.API.Strict as API
import           System.FS.BlockIO.API (HasBlockIO (..), LockFileHandle (..),
                     LockMode (..))
import           System.FS.BlockIO.Serial
import           System.FS.CallStack (prettyCallStack)
import           System.FS.Sim.Error
import           System.FS.Sim.MockFS hiding (hClose, hOpen)
import           System.FS.Sim.STM

fromHasFS ::
     forall m. (MonadCatch m, MonadMVar m, PrimMonad m)
  => HasFS m HandleMock
  -> m (HasBlockIO m HandleMock)
fromHasFS hfs =
    serialHasBlockIO
      hSetNoCache
      hAdvise
      hAllocate
      (simTryLockFile hfs)
      simHSynchronise
      simSynchroniseDirectory
      (simCreateHardLink hfs)
      hfs
  where
    -- TODO: It should be possible for the implementations and simulation to
    -- throw an FsError when doing file I/O with misaligned byte arrays after
    -- hSetNoCache. Maybe they should? It might be nicest to move hSetNoCache
    -- into fs-api and fs-sim because we'd need access to the internals.
    hSetNoCache _h _b = pure ()
    hAdvise _ _ _ _ = pure ()
    hAllocate _ _ _ = pure ()

    -- Disk operations are durable by construction
    simHSynchronise _ = pure ()
    simSynchroniseDirectory _ = pure ()

-- | Lock files are reader\/writer locks.
--
-- We implement this using the content of the lock file. The content is a
-- counter, positive for readers and negative (specifically -1) for writers.
-- There can be any number of readers, but only one writer. Writers can not
-- coexist with readers.
--
-- Warning: This implementation is not robust under concurrent use (because
-- operations on files are not atomic) but should be ok for casual use. A
-- proper implementation would need to be part of the underlying 'HasFS'
-- implementations.
--
-- Warning: regular file operations on the "locked" file, like 'hOpen' or
-- 'removeFile', will still work. 'simTryLockFile' only defines how multiple
-- lock acquisitions on the same file interact, not how lock acquisition
-- interacts with other file operations.
--
simTryLockFile ::
     forall m h. MonadThrow m
  => HasFS m h
  -> FsPath
  -> LockMode
  -> m (Maybe (LockFileHandle m))
simTryLockFile hfs path lockmode =
    API.withFile hfs path (ReadWriteMode AllowExisting) $ \h -> do
      n <- readCount h
      case lockmode of
        SharedLock    | n >= 0 -> do writeCount h (n+1)
                                     mkLockFileHandle
        ExclusiveLock | n == 0 -> do writeCount h (-1)
                                     mkLockFileHandle
        _                      -> pure Nothing
  where
    mkLockFileHandle = do
      -- A lock file handle keeps open the file in read mode, such that a locked
      -- file contributes to the number of open file handles. The mock FS allows
      -- multiple readers and up to one writer to open the file concurrently.
      h <- API.hOpen hfs path ReadMode
      pure (Just (LockFileHandle { hUnlock = hUnlock h }))

    hUnlock h0 =
      API.withFile hfs path (ReadWriteMode AllowExisting) $ \h -> do
        n <- readCount h
        case lockmode of
          SharedLock    | n >  0  -> writeCount h (n-1)
          ExclusiveLock | n == -1 -> writeCount h 0
          _                       -> throwIO countCorrupt
        hClose hfs h0

    readCount :: Handle h -> m Int
    readCount h = do
      content <- BS.toStrict <$> API.hGetAllAt hfs h 0
      case reads (BS.unpack content) of
        _ | BS.null content -> pure 0
        [(n, "")]           -> pure n
        _                   -> throwIO countCorrupt

    writeCount :: Handle h -> Int -> m ()
    writeCount h n = do
      API.hSeek hfs h AbsoluteSeek 0
      _ <- API.hPutAllStrict hfs h (BS.pack (show n))
      pure ()

    countCorrupt =
      FsError {
        fsErrorType   = FsOther,
        fsErrorPath   = fsToFsErrorPathUnmounted path,
        fsErrorString = "lock file content corrupted",
        fsErrorNo     = Nothing,
        fsErrorStack  = prettyCallStack,
        fsLimitation  = False
      }

-- | @'simCreateHardLink' hfs source target@ creates a simulated hard link for
-- the @source@ path at the @target@ path.
--
-- The hard link is simulated by simply copying the source file to the target
-- path, which means that it should only be used to create hard links for files
-- that are not modified afterwards!
--
-- TODO: if we wanted to simulate proper hard links, we would have to bake the
-- feature into @fs-sim@.
simCreateHardLink :: MonadThrow m => HasFS m h -> FsPath -> FsPath -> m ()
simCreateHardLink hfs sourcePath targetPath =
    API.withFile hfs sourcePath API.ReadMode $ \sourceHandle ->
    API.withFile hfs targetPath (API.WriteMode API.MustBeNew) $ \targetHandle -> do
      -- This should /hopefully/ stream using lazy IO
      bs <- API.hGetAll hfs sourceHandle
      void $ API.hPutAll hfs targetHandle bs

{-------------------------------------------------------------------------------
  Initialisation helpers
-------------------------------------------------------------------------------}

simHasBlockIO ::
     (MonadCatch m, MonadMVar m, PrimMonad m, MonadSTM m)
  => StrictTMVar m MockFS
  -> m (HasFS m HandleMock, HasBlockIO m HandleMock)
simHasBlockIO var = do
    let hfs = simHasFS var
    hbio <- fromHasFS hfs
    pure (hfs, hbio)

simHasBlockIO' ::
     (MonadCatch m, MonadMVar m, PrimMonad m, MonadSTM m)
  => MockFS
  -> m (HasFS m HandleMock, HasBlockIO m HandleMock)
simHasBlockIO' mockFS = do
    hfs <- simHasFS' mockFS
    hbio <- fromHasFS hfs
    pure (hfs, hbio)

simErrorHasBlockIO ::
     forall m. (MonadCatch m, MonadMVar m, PrimMonad m, MonadSTM m)
  => StrictTMVar m MockFS
  -> StrictTVar m Errors
  -> m (HasFS m HandleMock, HasBlockIO m HandleMock)
simErrorHasBlockIO fsVar errorsVar = do
    let hfs = simErrorHasFS fsVar errorsVar
    hbio <- fromHasFS hfs
    pure (hfs, hbio)

simErrorHasBlockIO' ::
     (MonadCatch m, MonadMVar m, PrimMonad m, MonadSTM m)
  => MockFS
  -> Errors
  -> m (HasFS m HandleMock, HasBlockIO m HandleMock)
simErrorHasBlockIO' mockFS errs = do
    hfs <- simErrorHasFS' mockFS errs
    hbio <- fromHasFS hfs
    pure (hfs, hbio)
