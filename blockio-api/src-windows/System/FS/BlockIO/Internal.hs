module System.FS.BlockIO.Internal (
    ioHasBlockIO
  ) where

import           Control.Exception (throwIO)
import           Control.Monad (unless)
import qualified System.FS.API as FS
import           System.FS.API (FsPath, Handle (..), HasFS)
import qualified System.FS.BlockIO.API as FS
import           System.FS.BlockIO.API (Advice (..), FileOffset, HasBlockIO,
                     IOCtxParams)
import qualified System.FS.BlockIO.Serial as Serial
import           System.FS.IO (HandleIO)
import qualified System.FS.IO.Handle as FS
import           System.IO.Error (doesNotExistErrorType, ioeSetErrorString,
                     mkIOError)
import qualified System.Win32.File as Windows
import qualified System.Win32.HardLink as Windows

-- | For now we use the portable serial implementation of HasBlockIO. If you
-- want to provide a proper async I/O implementation for Windows, then this is
-- where you should put it.
--
-- The recommended choice would be to use the Win32 IOCP API.
ioHasBlockIO ::
     HasFS IO HandleIO
  -> IOCtxParams
  -> IO (HasBlockIO IO HandleIO)
ioHasBlockIO hfs _params =
    Serial.serialHasBlockIO
      hSetNoCache
      hAdvise
      hAllocate
      (FS.tryLockFileIO hfs)
      hSynchronise
      (synchroniseDirectory hfs)
      (FS.createHardLinkIO hfs Windows.createHardLink)
      hfs

hSetNoCache :: Handle HandleIO -> Bool -> IO ()
hSetNoCache _h _b = pure ()

hAdvise :: Handle HandleIO -> FileOffset -> FileOffset -> Advice -> IO ()
hAdvise _h _off _len _advice = pure ()

hAllocate :: Handle HandleIO -> FileOffset -> FileOffset -> IO ()
hAllocate _h _off _len = pure ()

hSynchronise :: Handle HandleIO -> IO ()
hSynchronise h = FS.withOpenHandle "hAdvise" (handleRaw h) $ \fd ->
    Windows.flushFileBuffers fd

synchroniseDirectory :: HasFS IO HandleIO -> FsPath -> IO ()
synchroniseDirectory hfs path = do
    b <- FS.doesDirectoryExist hfs path
    unless b $
      throwIO $ FS.ioToFsError (FS.mkFsErrorPath hfs (FS.mkFsPath [])) ioerr
  where
    ioerr =
      ioeSetErrorString
        (mkIOError doesNotExistErrorType "synchroniseDirectory" Nothing Nothing)
        ("synchroniseDirectory: directory does not exist")
