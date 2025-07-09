{-# LANGUAGE TypeFamilies  #-}
{-# LANGUAGE UnboxedTuples #-}

-- | Abstract interface, types, and utilities.
module System.FS.BlockIO.API (
    -- * HasBlockIO
    HasBlockIO (..)
  , IOOp (..)
  , ioopHandle
  , ioopFileOffset
  , ioopBuffer
  , ioopBufferOffset
  , ioopByteCount
  , IOResult (..)
    -- ** Advice
  , Advice (..)
  , hAdviseAll
  , hDropCacheAll
    -- ** File locks
  , GHC.LockMode (..)
  , GHC.FileLockingNotSupported (..)
  , LockFileHandle (..)
    -- ** Storage synchronisation
  , synchroniseFile
  , synchroniseDirectoryRecursive
    -- * Re-exports
  , ByteCount
  , FileOffset
  ) where

import           Control.DeepSeq
import           Control.Monad (forM_)
import           Control.Monad.Class.MonadThrow (MonadThrow (..))
import           Control.Monad.Primitive (PrimMonad (PrimState))
import           Data.Primitive.ByteArray (MutableByteArray)
import qualified Data.Vector as V
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Generic.Mutable as VGM
import qualified Data.Vector.Primitive as VP
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import qualified GHC.IO.Handle.Lock as GHC
import           GHC.Stack (HasCallStack)
import qualified System.FS.API as FS
import           System.FS.API (BufferOffset, FsPath, Handle (..), HasFS)
import           System.Posix.Types (ByteCount, FileOffset)
import           Text.Printf

-- | Abstract interface for submitting large batches of I\/O operations. This
-- interface is an extension of the 'HasFS' interface that is provided by the
-- @fs-api@ package.
--
-- The interface tries to specify uniform behaviour, but each implementation can
-- have subtly different effects for a variety of reasons. However, for the most
-- part the underlying implementation of an instance of the interface should not
-- affect the correctness of programs that use the interface.
--
-- For uniform behaviour across implementations, functions that create a new
-- instance of the interface should initialise an IO context. This IO context
-- may be of any shape, as long as the context has two modes: open and closed.
-- This context is only important for the 'close' and 'submitIO' functions. As
-- long as the IO context is open, 'submitIO' should perform batches of I\/O
-- operations as expected, but 'submitIO' should throw an error as soon as the
-- IO context is closed. Once the IO context is closed, it can not be re-opened
-- again. Instead, the user should create a new instance of the interface.
--
-- Note: there are a bunch of functions in the interface that have nothing to do
-- with submitting large batches of I\/O operations. In fact, only 'close' and
-- 'submitIO' are related to that. All other functions were put in this record
-- for simplicity because the authors of the library needed them and it was more
-- straightforward to add them here then to add them to @fs-api@. Still these
-- unrelated functions could and should all be moved into @fs-api@ at some point
-- in the future.
--
data HasBlockIO m h = HasBlockIO {
    -- | (Idempotent) close the IO context that is required for running
    -- 'submitIO'.
    --
    -- Using 'submitIO' after 'close' throws an 'FsError' exception.
    --
    close    :: HasCallStack => m ()

    -- | Submit a batch of I\/O operations and wait for the result.
    --
    -- Results correspond to input 'IOOp's in a pair-wise manner, i.e., one can
    -- match 'IOOp's with 'IOResult's by indexing into both vectors at the same
    -- position.
    --
    -- If any of the I\/O operations fails, an 'FsError' exception will be thrown.
    --
    -- The buffers in the 'IOOp's should be pinned memory. If any buffer is
    -- unpinned memory, an 'FsError' exception will be thrown.
    --
  , submitIO :: HasCallStack => V.Vector (IOOp (PrimState m) h) -> m (VU.Vector IOResult)

    -- TODO: once file caching is disabled, subsequent reads/writes with
    -- misaligned byte arrays should throw an error. Preferably, this should
    -- happen in both the simulation and real implementation, even if the real
    -- implementation does not support setting the file caching mode. This would
    -- make the behaviour of the file caching mode more uniform across
    -- implementations and platforms.

    -- | Set the file data caching mode for a file handle.
    --
  , hSetNoCache :: Handle h -> Bool -> m ()

    -- | Predeclare an access pattern for file data.
    --
  , hAdvise :: Handle h -> FileOffset -> FileOffset -> Advice -> m ()

    -- | Allocate file space.
    --
  , hAllocate :: Handle h -> FileOffset -> FileOffset -> m ()

    -- NOTE: though it would have been nicer to allow locking /file handles/
    -- instead of /file paths/, it would make the implementation of this
    -- function in 'IO' much more complex. In particular, if we want to reuse
    -- "GHC.IO.Handle.Lock" functionality, then we have to either ...
    --
    -- 1. Convert there and back between OS-specific file descriptors and
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

    -- | Try to acquire a file lock without blocking.
    --
    -- This function throws 'GHC.FileLockingNotSupported' when file locking is
    -- not supported.
    --
  , tryLockFile :: FsPath -> GHC.LockMode -> m (Maybe (LockFileHandle m))

    -- | Synchronise file contents with the storage device.
    --
    -- This ensures that all changes to the file handle's contents, which might
    -- exist only in memory as buffered system cache pages, are
    -- transferred/flushed to disk. This will also update the file handle's
    -- associated metadata.
    --
  , hSynchronise :: Handle h -> m ()

    -- | Synchronise a directory with the storage device.
    --
    -- This ensures that all changes to the directory, which might exist only in
    -- memory as buffered changes, are transferred/flushed to disk. This will
    -- also update the directory's associated metadata.
    --
  , synchroniseDirectory :: FsPath -> m ()

    -- | Create a hard link for an existing file at the source path and a new
    -- file at the target path.
    --
  , createHardLink :: FsPath -> FsPath -> m ()
  }

instance NFData (HasBlockIO m h) where
  rnf (HasBlockIO a b c d e f g h i) =
      rwhnf a `seq` rwhnf b `seq` rnf c `seq`
      rwhnf d `seq` rwhnf e `seq` rwhnf f `seq`
      rwhnf g `seq` rwhnf h `seq` rwhnf i

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
  deriving stock (Show, Eq)
  deriving newtype VP.Prim

newtype instance VUM.MVector s IOResult = MV_IOResult (VP.MVector s IOResult)
newtype instance VU.Vector     IOResult = V_IOResult  (VP.Vector    IOResult)

deriving via (VU.UnboxViaPrim IOResult) instance VGM.MVector VU.MVector IOResult
deriving via (VU.UnboxViaPrim IOResult) instance VG.Vector   VU.Vector  IOResult

instance VUM.Unbox IOResult

{-------------------------------------------------------------------------------
  Advice
-------------------------------------------------------------------------------}

-- | Copy of "System.Posix.Fcntl.Advice" from the @unix@ package
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
  Storage synchronisation
-------------------------------------------------------------------------------}

{-# SPECIALISE synchroniseFile :: HasFS IO h -> HasBlockIO IO h -> FsPath -> IO () #-}
-- | Synchronise a file's contents and metadata with the storage device.
synchroniseFile :: MonadThrow m => HasFS m h -> HasBlockIO m h -> FsPath -> m ()
synchroniseFile hfs hbio path =
    FS.withFile hfs path (FS.ReadWriteMode FS.MustExist) $ hSynchronise hbio

{-# SPECIALISE synchroniseDirectoryRecursive ::
     HasFS IO h
  -> HasBlockIO IO h
  -> FsPath
  -> IO ()
  #-}
-- | Synchronise a directory's contents and metadata with the storage device,
-- and recursively for all entries in the directory.
synchroniseDirectoryRecursive ::
     MonadThrow m
  => HasFS m h
  -> HasBlockIO m h
  -> FsPath
  -> m ()
synchroniseDirectoryRecursive hfs hbio path = do
    entries <- FS.listDirectory hfs path
    forM_ entries $ \entry -> do
      let path' = path FS.</> FS.mkFsPath [entry]
      isFile <- FS.doesFileExist hfs path'
      if isFile then
        synchroniseFile hfs hbio path'
      else do
        isDirectory <- FS.doesDirectoryExist hfs path'
        if isDirectory then do
          synchroniseDirectoryRecursive hfs hbio path'
          synchroniseDirectory hbio path'
        else
          error $ printf
            "listDirectoryRecursive: %s is not a file or directory"
            (show path')

{-------------------------------------------------------------------------------
  File locks
-------------------------------------------------------------------------------}

-- | A handle to a file locked using 'tryLockFile'.
newtype LockFileHandle m = LockFileHandle {
    -- | Release a file lock acquired using 'tryLockFile'.
    hUnlock :: m ()
  }
