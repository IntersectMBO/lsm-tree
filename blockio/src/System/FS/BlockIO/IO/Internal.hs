{-# LANGUAGE TypeFamilies  #-}
{-# LANGUAGE UnboxedTuples #-}

module System.FS.BlockIO.IO.Internal (
    IOCtxParams (..)
  , defaultIOCtxParams
  , mkClosedError
  , tryLockFileIO
  , createHardLinkIO
  ) where

import           Control.DeepSeq (NFData (..))
import           Control.Monad.Class.MonadThrow (MonadCatch (bracketOnError),
                     MonadThrow (..), bracketOnError, try)
import           GHC.IO.Exception (IOErrorType (ResourceVanished))
import qualified GHC.IO.Handle.Lock as GHC
import           GHC.Stack (HasCallStack)
import qualified System.FS.API as FS
import           System.FS.API (FsError (..), FsPath, HasFS, SomeHasFS (..))
import           System.FS.BlockIO.API (LockFileHandle (..))
import           System.FS.IO (HandleIO)
import qualified System.IO as GHC
import           System.IO.Error (ioeSetErrorString, mkIOError)

{-------------------------------------------------------------------------------
  IO context
-------------------------------------------------------------------------------}

-- | Concurrency parameters for initialising the 'IO' context in a 'HasBlockIO'
-- instance.
--
-- [IO context parameters]: These parameters are interpreted differently based
--  on the underlying platform:
--
--  * Linux: Pass the parameters to 'initIOCtx' in the @blockio-uring@ package
--  * MacOS: Ignore the parameters
--  * Windows: Ignore the parameters
--
--  For more information about what these parameters mean and how to configure
--  them, see the @blockio-uring@ package.
data IOCtxParams = IOCtxParams {
                     ioctxBatchSizeLimit   :: !Int,
                     ioctxConcurrencyLimit :: !Int
                   }

instance NFData IOCtxParams where
  rnf (IOCtxParams x y) = rnf x `seq` rnf y

-- | Default parameters. Some manual tuning of parameters might be required to
-- achieve higher performance targets (see 'IOCtxParams').
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

{-------------------------------------------------------------------------------
  File locks
-------------------------------------------------------------------------------}

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
      Right a  -> pure a
  where
    handleError :: HasCallStack => IOError -> IO a
    handleError ioErr =
      throwIO $ FS.ioToFsError (FS.mkFsErrorPath hfs fp) ioErr

{-------------------------------------------------------------------------------
  Hard links
-------------------------------------------------------------------------------}

createHardLinkIO ::
     HasFS IO HandleIO
  -> (FilePath -> FilePath -> IO ())
  -> (FsPath -> FsPath -> IO ())
createHardLinkIO hfs f = \source target -> do
    source' <- FS.unsafeToFilePath hfs source -- shouldn't fail because we are in IO
    target' <- FS.unsafeToFilePath hfs target -- shouldn't fail because we are in IO
    f source' target'
