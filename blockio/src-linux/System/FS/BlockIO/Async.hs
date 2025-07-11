module System.FS.BlockIO.Async (
    asyncHasBlockIO
  ) where

import           Control.Exception
import qualified Control.Exception as E
import           Control.Monad.Primitive
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import           Foreign.C.Error
import           GHC.IO.Exception
import           GHC.Stack
import           System.FS.API (BufferOffset (..), FsErrorPath, FsPath,
                     Handle (..), HasFS (..), ioToFsError)
import qualified System.FS.BlockIO.API as API
import           System.FS.BlockIO.API (IOOp (..), IOResult (..), LockMode,
                     ioopHandle)
import qualified System.FS.BlockIO.IO.Internal as IOI
import           System.FS.IO (HandleIO)
import           System.FS.IO.Handle
import qualified System.IO.BlockIO as I
import           System.IO.Error (ioeGetErrorType, ioeSetErrorString,
                     isResourceVanishedError)
import           System.Posix.Types

-- | IO instantiation of 'HasBlockIO', using @blockio-uring@.
asyncHasBlockIO ::
     (Handle HandleIO -> Bool -> IO ())
  -> (Handle HandleIO -> FileOffset -> FileOffset -> API.Advice -> IO ())
  -> (Handle HandleIO -> FileOffset -> FileOffset -> IO ())
  -> (FsPath -> LockMode -> IO (Maybe (API.LockFileHandle IO)))
  -> (Handle HandleIO -> IO ())
  -> (FsPath -> IO ())
  -> (FsPath -> FsPath -> IO ())
  -> HasFS IO HandleIO
  -> IOI.IOCtxParams
  -> IO (API.HasBlockIO IO HandleIO)
asyncHasBlockIO hSetNoCache hAdvise hAllocate tryLockFile hSynchronise synchroniseDirectory createHardLink hasFS ctxParams = do
  ctx <- I.initIOCtx (ctxParamsConv ctxParams)
  pure $ API.HasBlockIO {
      API.close = I.closeIOCtx ctx
    , API.submitIO = submitIO hasFS ctx
    , API.hSetNoCache
    , API.hAdvise
    , API.hAllocate
    , API.tryLockFile
    , API.hSynchronise
    , API.synchroniseDirectory
    , API.createHardLink
    }

ctxParamsConv :: IOI.IOCtxParams -> I.IOCtxParams
ctxParamsConv IOI.IOCtxParams{IOI.ioctxBatchSizeLimit, IOI.ioctxConcurrencyLimit} =
    I.IOCtxParams {
        I.ioctxBatchSizeLimit   = ioctxBatchSizeLimit
      , I.ioctxConcurrencyLimit = ioctxConcurrencyLimit
      }

submitIO ::
     HasCallStack
  => HasFS IO HandleIO
  -> I.IOCtx
  -> V.Vector (IOOp RealWorld HandleIO)
  -> IO (VU.Vector IOResult)
submitIO hasFS ioctx ioops = do
    ioops' <- mapM ioopConv ioops
    ress <- I.submitIO ioctx ioops' `catch` rethrowFsError
    hzipWithM rethrowErrno ioops ress
  where
    rethrowFsError :: IOError -> IO a
    rethrowFsError e@IOError{}
      -- Pattern matching on the error is brittle, because the structure of
      -- the exception might change between versions of @blockio-uring@.
      -- Nonetheless, it's better than nothing.
      | isResourceVanishedError e
      , ioe_location e == "IOCtx closed"
      = throwIO (IOI.mkClosedError hasFS "submitIO")
      | ioeGetErrorType e == InvalidArgument
      , ioe_location e == "MutableByteArray is unpinned"
      = throwIO (IOI.mkNotPinnedError hasFS "submitIO")
      | otherwise
      = throwIO e

    rethrowErrno ::
         HasCallStack
      => IOOp RealWorld HandleIO
      -> I.IOResult
      -> IO IOResult
    rethrowErrno ioop res = do
        case res of
          I.IOResult c -> pure (IOResult c)
          I.IOError  e -> throwAsFsError e
      where
        throwAsFsError :: HasCallStack => Errno -> IO a
        throwAsFsError errno = E.throwIO $ ioToFsError fep (fromErrno errno)

        fep :: FsErrorPath
        fep = mkFsErrorPath hasFS (handlePath (ioopHandle ioop))

        fromErrno :: Errno -> IOError
        fromErrno errno = ioeSetErrorString
                            (errnoToIOError "submitIO" errno Nothing Nothing)
                            ("submitIO failed: " <> ioopType)

        ioopType :: String
        ioopType = case ioop of
          IOOpRead{}  -> "IOOpRead"
          IOOpWrite{} -> "IOOpWrite"

ioopConv :: IOOp RealWorld HandleIO -> IO (I.IOOp RealWorld)
ioopConv (IOOpRead h off buf bufOff c) = handleFd h >>= \fd ->
    pure (I.IOOpRead  fd off buf (unBufferOffset bufOff) c)
ioopConv (IOOpWrite h off buf bufOff c) = handleFd h >>= \fd ->
    pure (I.IOOpWrite fd off buf (unBufferOffset bufOff) c)

-- This only checks whether the handle is open when we convert to an Fd. After
-- that, the handle could be closed when we're still performing blockio
-- operations.
--
-- TODO: if the handle were to have a reader/writer lock, then we could take the
-- reader lock in 'submitIO'. However, the current implementation of 'Handle'
-- only allows mutually exclusive access to the underlying file descriptor, so it
-- would require a change in @fs-api@. See [fs-sim#49].
handleFd :: Handle HandleIO -> IO Fd
handleFd h = withOpenHandle "submitIO" (handleRaw h) pure

{-# SPECIALISE hzipWithM ::
     (VUM.Unbox b, VUM.Unbox c)
  => (a -> b -> IO c)
  -> V.Vector a
  -> VU.Vector b
  -> IO (VU.Vector c)
  #-}
-- | Heterogeneous blend of `V.zipWithM` and `VU.zipWithM`
--
-- The @vector@ package does not provide functions that take distinct vector
-- containers as arguments, so we write it by hand to prevent having to convert
-- one vector type to the other.
hzipWithM ::
     forall m a b c. (PrimMonad m, VUM.Unbox b, VUM.Unbox c)
  => (a -> b -> m c)
  -> V.Vector a
  -> VU.Vector b
  -> m (VU.Vector c)
hzipWithM f v1 v2 = do
    res <- VUM.unsafeNew n
    loop res 0
  where
    !n = min (V.length v1) (VU.length v2)

    loop :: VUM.MVector (PrimState m) c -> Int -> m (VU.Vector c)
    loop !res !i
      | i == n = VU.unsafeFreeze res
      | otherwise = do
          let !x = v1 `V.unsafeIndex` i
              !y = v2 `VU.unsafeIndex` i
          !z <- f x y
          VUM.write res i z
          loop res (i+1)
