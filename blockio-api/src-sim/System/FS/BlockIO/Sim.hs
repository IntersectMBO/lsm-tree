-- | Simulated instances of 'HasBlockIO' and 'HasFS'.
module System.FS.BlockIO.Sim (
    -- * Implementation details #impl#
    -- $impl

    -- * Runners
    runSimHasBlockIO
  , runSimErrorHasBlockIO
    -- * Initialisation
  , simHasBlockIO
  , simHasBlockIO'
  , simErrorHasBlockIO
  , simErrorHasBlockIO'
    -- ** Unsafe
  , unsafeFromHasFS
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
import           System.FS.BlockIO.Serial.Internal
import           System.FS.CallStack (prettyCallStack)
import           System.FS.Sim.Error
import           System.FS.Sim.MockFS hiding (hClose, hOpen)
import           System.FS.Sim.STM

{- $impl

  We include below some documentation about the effects of calling the interface
  functions on the simulated instance of the 'HasBlockIO' interface.

  [IO context]: For uniform behaviour across implementations, the simulation
    creates and stores a mocked IO context that has the open/closed behaviour
    that is specified by the interface.

  ['close']: Close the mocked context

  ['submitIO']: Submit a batch of I\/O operations using serial I\/O using a
      'HasFS'

  ['hSetNoCache']: No-op

  ['hAdvise']: No-op

  ['hAllocate']: No-op

  ['tryLockFile']: Simulate a lock by putting the lock state into the file
      contents

  ['hSynchronise']: No-op

  ['synchroniseDirectory']: No-op

  ['createHardLink']: Copy all file contents from the source path to the target
      path. Therefore, this is currently only correctly simulating hard links
      for /immutable/ files.
-}

-- | Simulate a 'HasBlockIO' using the given 'HasFS'.
--
-- === Unsafe
--
-- You will probably want to use one of the safe functions like
-- 'runSimHasBlockIO' or 'simErrorHasBlockIO' instead.
--
-- Only a simulated 'HasFS', like the 'simHasFS' and 'simErrorHasFS'
-- simulations, should be passed to 'unsafeFromHasFS'. Technically, one could
-- pass a 'HasFS' for the /real/ file system, but then the resulting
-- 'HasBlockIO' would contain a mix of simulated functions and real functions,
-- which is probably not what you want.
unsafeFromHasFS ::
     forall m. (MonadCatch m, MonadMVar m, PrimMonad m)
  => HasFS m HandleMock
  -> m (HasBlockIO m HandleMock)
unsafeFromHasFS hfs =
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
    -- throw an FsError when doing file I\/O with misaligned byte arrays after
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
  Runners
-------------------------------------------------------------------------------}

-- | @'runSimHasBlockIO' mockFS action@ runs an @action@ using a pair of
-- simulated 'HasFS' and 'HasBlockIO'.
--
-- The pair of interfaces share the same mocked file system. The initial state
-- of the mocked file system is set to @mockFs@. The final state of the mocked
-- file system is returned with the result of @action@.
--
-- If you want to have access to the current state of the mocked file system,
-- use 'simHasBlockIO' instead.
runSimHasBlockIO ::
     (MonadSTM m, PrimMonad m, MonadCatch m, MonadMVar m)
  => MockFS
  -> (HasFS m HandleMock -> HasBlockIO m HandleMock -> m a)
  -> m (a, MockFS)
runSimHasBlockIO mockFS k = do
    runSimFS mockFS $ \hfs -> do
      hbio <- unsafeFromHasFS hfs
      k hfs hbio

-- | @'runSimErrorHasBlockIO' mockFS errors action@ runs an @action@ using a
-- pair of simulated 'HasFS' and 'HasBlockIO' that allow fault injection.
--
-- The pair of interfaces share the same mocked file system. The initial state
-- of the mocked file system is set to @mockFs@. The final state of the mocked
-- file system is returned with the result of @action@.
--
-- The pair of interfaces share the same stream of errors. The initial state of
-- the stream of errors is set to @errors@. The final state of the stream of
-- errors is returned with the result of @action@.
--
-- If you want to have access to the current state of the mocked file system
-- or stream of errors, use 'simErrorHasBlockIO' instead.
runSimErrorHasBlockIO ::
     (MonadSTM m, PrimMonad m, MonadCatch m, MonadMVar m)
  => MockFS
  -> Errors
  -> (HasFS m HandleMock -> HasBlockIO m HandleMock -> m a)
  -> m (a, MockFS, Errors)
runSimErrorHasBlockIO mockFS errs k = do
    fsVar <- newTMVarIO mockFS
    errorsVar <- newTVarIO errs
    (hfs, hbio) <- simErrorHasBlockIO fsVar errorsVar
    a <- k hfs hbio
    fs' <- atomically $ takeTMVar fsVar
    errs' <- readTVarIO errorsVar
    pure (a, fs', errs')

{-------------------------------------------------------------------------------
  Initialisation
-------------------------------------------------------------------------------}

-- | @'simHasBlockIO' mockFsVar@ creates a pair of simulated 'HasFS' and
-- 'HasBlockIO'.
--
-- The pair of interfaces share the same mocked file system, which is stored in
-- @mockFsVar@. The current state of the mocked file system can be accessed by
-- the user by reading @mockFsVar@, but note that the user should not leave
-- @mockFsVar@ empty.
simHasBlockIO ::
     (MonadCatch m, MonadMVar m, PrimMonad m, MonadSTM m)
  => StrictTMVar m MockFS
  -> m (HasFS m HandleMock, HasBlockIO m HandleMock)
simHasBlockIO var = do
    let hfs = simHasFS var
    hbio <- unsafeFromHasFS hfs
    pure (hfs, hbio)

-- | @'simHasBlockIO' mockFs@ creates a pair of simulated 'HasFS' and
-- 'HasBlockIO' that allow fault injection.
--
-- The pair of interfaces share the same mocked file system. The initial state
-- of the mocked file system is set to @mockFs@.
--
-- If you want to have access to the current state of the mocked file system,
-- use 'simHasBlockIO' instead.
simHasBlockIO' ::
     (MonadCatch m, MonadMVar m, PrimMonad m, MonadSTM m)
  => MockFS
  -> m (HasFS m HandleMock, HasBlockIO m HandleMock)
simHasBlockIO' mockFS = do
    hfs <- simHasFS' mockFS
    hbio <- unsafeFromHasFS hfs
    pure (hfs, hbio)

-- | @'simErrorHasBlockIO' mockFsVar errorsVar@ creates a pair of simulated
-- 'HasFS' and 'HasBlockIO' that allow fault injection.
--
-- The pair of interfaces share the same mocked file system, which is stored in
-- @mockFsVar@. The current state of the mocked file system can be accessed by
-- the user by reading @mockFsVar@, but note that the user should not leave
-- @mockFsVar@ empty.
--
-- The pair of interfaces share the same stream of errors, which is stored in
-- @errorsVar@. The current state of the stream of errors can be accessed by the
-- user by reading @errorsVar@.
simErrorHasBlockIO ::
     forall m. (MonadCatch m, MonadMVar m, PrimMonad m, MonadSTM m)
  => StrictTMVar m MockFS
  -> StrictTVar m Errors
  -> m (HasFS m HandleMock, HasBlockIO m HandleMock)
simErrorHasBlockIO fsVar errorsVar = do
    let hfs = simErrorHasFS fsVar errorsVar
    hbio <- unsafeFromHasFS hfs
    pure (hfs, hbio)

-- | @'simErrorHasBlockIO' mockFs errors@ creates a pair of simulated 'HasFS'
-- and 'HasBlockIO' that allow fault injection.
--
-- The pair of interfaces share the same mocked file system. The initial state
-- of the mocked file system is set to @mockFs@.
--
-- The pair of interfaces share the same stream of errors. The initial state of
-- the stream of errors is set to @errors@.
--
-- If you want to have access to the current state of the mocked file system
-- or stream of errors, use 'simErrorHasBlockIO' instead.
simErrorHasBlockIO' ::
     (MonadCatch m, MonadMVar m, PrimMonad m, MonadSTM m)
  => MockFS
  -> Errors
  -> m (HasFS m HandleMock, HasBlockIO m HandleMock)
simErrorHasBlockIO' mockFS errs = do
    hfs <- simErrorHasFS' mockFS errs
    hbio <- unsafeFromHasFS hfs
    pure (hfs, hbio)
