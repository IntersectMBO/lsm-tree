{-# LANGUAGE DataKinds                  #-}
{-# LANGUAGE DerivingVia                #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE RankNTypes                 #-}
{-# LANGUAGE StandaloneDeriving         #-}
{-# LANGUAGE TypeFamilies               #-}
{-# LANGUAGE UnboxedTuples              #-}

module System.FS.BlockIO.API (
    HasBlockIO (..)
  , IOCtxParams (..)
  , defaultIOCtxParams
  , mkClosedError
  , IOOp (..)
  , ioopHandle
  , ioopFileOffset
  , ioopBuffer
  , ioopBufferOffset
  , ioopByteCount
  , IOResult (..)
    -- * Advice
  , Advice (..)
  , hAdviseAll
  , hDropCacheAll
    -- * File locks
  , GHC.LockMode (..)
  , GHC.FileLockingNotSupported (..)
  , LockFileHandle (..)
  , tryLockFileIO
    -- * Re-exports
  , ByteCount
  , FileOffset
  ) where

import           Control.DeepSeq
import           Control.Monad.Class.MonadThrow (MonadCatch (bracketOnError),
                     MonadThrow (..), bracketOnError, try)
import           Control.Monad.Primitive (PrimMonad (PrimState))
import           Data.Primitive.ByteArray (MutableByteArray)
import qualified Data.Vector as V
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Generic.Mutable as VGM
import qualified Data.Vector.Primitive as VP
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import           GHC.IO.Exception (IOErrorType (ResourceVanished))
import qualified GHC.IO.Handle.Lock as GHC
import           GHC.Stack (HasCallStack)
import qualified System.FS.API as FS
import           System.FS.API (BufferOffset, FsError (..), FsPath, Handle (..),
                     HasFS, SomeHasFS (..))
import           System.FS.IO (HandleIO)
import qualified System.IO as GHC
import           System.IO.Error (ioeSetErrorString, mkIOError)
import           System.Posix.Types (ByteCount, FileOffset)

-- | Abstract interface for submitting large batches of I\/O operations.
data HasBlockIO m h = HasBlockIO {
    -- | (Idempotent) close the interface.
    --
    -- Using 'submitIO' after 'close' should thrown an 'FsError' exception. See
    -- 'mkClosedError'.
    close    :: HasCallStack => m ()
    -- | Submit a batch of I\/O operations and wait for the result.
    --
    -- Results correspond to input 'IOOp's in a pair-wise manner, i.e., one can
    -- match 'IOOp's with 'IOResult's by indexing into both vectors at the same
    -- position.
    --
    -- If any of the I\/O operations fails, an 'FsError' exception will be thrown.
  , submitIO :: HasCallStack => V.Vector (IOOp (PrimState m) h) -> m (VU.Vector IOResult)
    -- | Set the file data caching mode for a file handle.
    --
    -- This has different effects on different distributions.
    -- * [Linux]: set the @O_DIRECT@ flag.
    -- * [MacOS]: set the @F_NOCACHE@ flag.
    -- * [Windows]: no-op.
    --
    -- TODO: subsequent reads/writes with misaligned byte arrays should fail
    -- both in simulation and real implementation.
  , hSetNoCache :: Handle h -> Bool -> m ()
    -- | Predeclare an access pattern for file data.
    --
    -- This has different effects on different distributions.
    -- * [Linux]: perform @posix_fadvise(2).
    -- * [MacOS]: no-op.
    -- * [Windows]: no-op.
  , hAdvise :: Handle h -> FileOffset -> FileOffset -> Advice -> m ()
    -- | Allocate file space.
    --
    -- This has different effects on different distributions.
    -- * [Linux]: perform @posix_fallocate(2).
    -- * [MacOS]: no-op.
    -- * [Windows]: no-op.
  , hAllocate :: Handle h -> FileOffset -> FileOffset -> m ()
    -- | Synchronize file contents with storage device.
    --
    -- Ensure that all change to the file handle's contents which exist only in
    -- memory (as buffered system cache pages) are transfered/flushed to disk.
    -- This will also (partially) update the file handle's associated metadate.
    -- On POSIX systems (including MacOS) this will likely involve call to either
    -- @fsync(2)@ or @datasync(2)@. On Windows this will likely involve a call to
    -- @flushFileBuffers@.
  , hSynchronize :: Handle h -> m ()
    -- | Try to acquire a file lock without blocking.
    --
    -- This uses different locking methods on different distributions.
    -- * [Linux]: Open file descriptor (OFD)
    -- * [MacOS]: @flock@
    -- * [Windows]: @LockFileEx@
    --
    -- This function can throw 'GHC.FileLockingNotSupported' when file locking
    -- is not supported.
    --
    -- NOTE: though it would have been nicer to allow locking /file handles/
    -- instead of /file paths/, it would make the implementation of this
    -- function in 'IO' much more complex. In particular, if we want to reuse
    -- "GHC.IO.Handle.Lock" functionality, then we have to either ...
    --
    -- 1. Convert there and back between OS-specific file desciptors and
    --   'GHC.Handle's, which is not possible on Windows without creating new
    --   file descriptors, or ...
    -- 2. Vendor all of the "GHC.IO.Handle.Lock" code and its dependencies
    --    (i.e., modules), which is a prohibitively large body of code for GHC
    --    versions before @9.0@.
    --
    -- The current interface is therefore limited, but should be sufficient for
    -- use cases where a lock file is used to guard against concurrent access by
    -- different processes. e.g., a database lock file.
    --
    -- TODO: it is /probably/ possible to provide a 'onLockFileHandle' function
    -- that allows you to use 'LockFileHandle' as a 'Handle', but only within a
    -- limited scope. That is, it has to fit the style of @withHandleToHANDLE ::
    -- Handle -> (HANDLE -> IO a) -> IO a@ from the @Win32@ package.
  , tryLockFile :: FsPath -> GHC.LockMode -> m (Maybe (LockFileHandle m))
  }

instance NFData (HasBlockIO m h) where
  rnf (HasBlockIO a b c d e f g) =
      rwhnf a `seq` rwhnf b `seq` rnf c `seq`
      rwhnf d `seq` rwhnf e `seq` rwhnf f `seq` rwhnf g

-- | Concurrency parameters for initialising a 'HasBlockIO. Can be ignored by
-- serial implementations.
data IOCtxParams = IOCtxParams {
                     ioctxBatchSizeLimit   :: !Int,
                     ioctxConcurrencyLimit :: !Int
                   }

instance NFData IOCtxParams where
  rnf (IOCtxParams x y) = rnf x `seq` rnf y

defaultIOCtxParams :: IOCtxParams
defaultIOCtxParams = IOCtxParams {
      ioctxBatchSizeLimit   = 64,
      ioctxConcurrencyLimit = 64 * 3
    }

mkClosedError :: HasCallStack => SomeHasFS m -> String -> FsError
mkClosedError (SomeHasFS hasFS) loc = FS.ioToFsError (FS.mkFsErrorPath hasFS (FS.mkFsPath [])) ioerr
  where ioerr =
          ioeSetErrorString
            (mkIOError ResourceVanished loc Nothing Nothing)
            ("HasBlockIO closed: " <> loc)


data IOOp s h =
    IOOpRead  !(Handle h) !FileOffset !(MutableByteArray s) !BufferOffset !ByteCount
  | IOOpWrite !(Handle h) !FileOffset !(MutableByteArray s) !BufferOffset !ByteCount

instance NFData (IOOp s h) where
  rnf = rwhnf

ioopHandle :: IOOp s h -> Handle h
ioopHandle (IOOpRead h _ _ _ _)  = h
ioopHandle (IOOpWrite h _ _ _ _) = h

ioopFileOffset :: IOOp s h -> FileOffset
ioopFileOffset (IOOpRead _ off _ _ _)  = off
ioopFileOffset (IOOpWrite _ off _ _ _) = off

ioopBuffer :: IOOp s h -> MutableByteArray s
ioopBuffer (IOOpRead _ _ buf _ _)  = buf
ioopBuffer (IOOpWrite _ _ buf _ _) = buf

ioopBufferOffset :: IOOp s h -> BufferOffset
ioopBufferOffset (IOOpRead _ _ _ bufOff _)  = bufOff
ioopBufferOffset (IOOpWrite _ _ _ bufOff _) = bufOff

ioopByteCount :: IOOp s h -> ByteCount
ioopByteCount (IOOpRead _ _ _ _ c)  = c
ioopByteCount (IOOpWrite _ _ _ _ c) = c

-- | Number of read/written bytes.
newtype IOResult = IOResult ByteCount
  deriving newtype VP.Prim

newtype instance VUM.MVector s IOResult = MV_IOResult (VP.MVector s IOResult)
newtype instance VU.Vector     IOResult = V_IOResult  (VP.Vector    IOResult)

deriving via (VU.UnboxViaPrim IOResult) instance VGM.MVector VU.MVector IOResult
deriving via (VU.UnboxViaPrim IOResult) instance VG.Vector   VU.Vector  IOResult

instance VUM.Unbox IOResult

-- | Basically "System.Posix.Fcntl.Advice" from the @unix@ package
data Advice =
    AdviceNormal
  | AdviceRandom
  | AdviceSequential
  | AdviceWillNeed
  | AdviceDontNeed
  | AdviceNoReuse
  deriving stock (Show, Eq)

-- | Apply 'Advice' to all bytes of a file, which is referenced by a 'Handle'.
hAdviseAll :: HasBlockIO m h -> Handle h -> Advice -> m ()
hAdviseAll hbio h advice = hAdvise hbio h 0 0 advice -- len=0 implies until the end of file

-- | Drop the full file referenced by a 'Handle' from the OS page cache, if
-- present.
hDropCacheAll :: HasBlockIO m h -> Handle h -> m ()
hDropCacheAll hbio h = hAdviseAll hbio h AdviceDontNeed

{-------------------------------------------------------------------------------
  File locks
-------------------------------------------------------------------------------}

-- | A handle to a file locked using 'tryLockFile'.
newtype LockFileHandle m = LockFileHandle {
    -- | Release a file lock acquired using 'tryLockFile'.
    hUnlock :: m ()
  }

tryLockFileIO :: HasFS IO HandleIO -> FsPath -> GHC.LockMode -> IO (Maybe (LockFileHandle IO))
tryLockFileIO hfs fsp mode = do
    fp <- FS.unsafeToFilePath hfs fsp -- shouldn't fail because we are in IO
    rethrowFsErrorIO hfs fsp $
      bracketOnError (GHC.openFile fp GHC.WriteMode) GHC.hClose $ \h -> do
        bracketOnError (GHC.hTryLock h mode) (\_ -> GHC.hUnlock h) $ \b -> do
          if b then
            pure $ Just LockFileHandle { hUnlock = rethrowFsErrorIO hfs fsp $ do
                  GHC.hUnlock h
                    `finally` GHC.hClose h
                }
          else
            pure $ Nothing

-- This is copied/adapted from System.FS.IO
rethrowFsErrorIO :: HasCallStack => HasFS IO HandleIO -> FsPath -> IO a -> IO a
rethrowFsErrorIO hfs fp action = do
    res <- try action
    case res of
      Left err -> handleError err
      Right a  -> return a
  where
    handleError :: HasCallStack => IOError -> IO a
    handleError ioErr =
      throwIO $ FS.ioToFsError (FS.mkFsErrorPath hfs fp) ioErr
