{-# LANGUAGE NamedFieldPuns #-}

module System.FS.BlockIO.Async (
    asyncHasBlockIO
  ) where

import           Control.Exception
import qualified Control.Exception as E
import           Control.Monad
import           Foreign.C.Error
import           GHC.IO.Exception
import           GHC.Stack
import           System.FS.API (BufferOffset (..), FsErrorPath, Handle (..),
                     HasFS (..), SomeHasFS (..), ioToFsError)
import qualified System.FS.BlockIO.API as API
import           System.FS.BlockIO.API (IOOp (..), IOResult (..), ioopHandle)
import           System.FS.IO (HandleIO)
import           System.FS.IO.Internal.Handle
import qualified System.IO.BlockIO as I
import           System.IO.Error (ioeSetErrorString, isResourceVanishedError)
import           System.Posix.Types

-- | IO instantiation of 'HasBlockIO', using @blockio-uring@.
asyncHasBlockIO :: HasFS IO HandleIO -> Maybe API.IOCtxParams -> IO (API.HasBlockIO IO HandleIO)
asyncHasBlockIO hasFS ctxParams = do
  ctx <- I.initIOCtx (maybe I.defaultIOCtxParams ctxParamsConv ctxParams)
  pure $ API.HasBlockIO {
      API.close = I.closeIOCtx ctx
    , API.submitIO = submitIO hasFS ctx
    }

ctxParamsConv :: API.IOCtxParams -> I.IOCtxParams
ctxParamsConv API.IOCtxParams{API.ioctxBatchSizeLimit, API.ioctxConcurrencyLimit} =
    I.IOCtxParams {
        I.ioctxBatchSizeLimit   = ioctxBatchSizeLimit
      , I.ioctxConcurrencyLimit = ioctxConcurrencyLimit
      }

submitIO ::
     HasFS IO HandleIO
  -> I.IOCtx
  -> [IOOp IO HandleIO]
  -> IO [IOResult]
submitIO hasFS ioctx ioops = do
    ioops' <- mapM ioopConv ioops
    ress <- I.submitIO ioctx ioops' `catch` rethrowClosedError
    zipWithM rethrowErrno ioops ress
  where
    rethrowClosedError :: IOError -> IO a
    rethrowClosedError e@IOError{} =
        -- Pattern matching on the error is brittle, because the structure of
        -- the exception might change between versions of @blockio-uring@.
        -- Nonetheless, it's better than nothing.
        if isResourceVanishedError e && ioe_location e == "IOCtx closed"
          then throwIO (API.mkClosedError (SomeHasFS hasFS) "submitIO")
          else throwIO e

    rethrowErrno ::
         HasCallStack
      => IOOp IO HandleIO
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

ioopConv :: IOOp IO HandleIO -> IO (I.IOOp IO)
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
-- only allows mutally exclusive access to the underlying file descriptor, so it
-- would require a change in @fs-api@. See [fs-sim#49].
handleFd :: Handle HandleIO -> IO Fd
handleFd h = withOpenHandle "submitIO" (handleRaw h) pure
