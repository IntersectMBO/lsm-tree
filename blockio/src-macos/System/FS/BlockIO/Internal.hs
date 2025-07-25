module System.FS.BlockIO.Internal (
    ioHasBlockIO
  ) where

import qualified System.FS.API as FS
import           System.FS.API (FsPath, Handle (..), HasFS)
import           System.FS.BlockIO.API (Advice (..), FileOffset, HasBlockIO)
import qualified System.FS.BlockIO.IO.Internal as IOI
import qualified System.FS.BlockIO.Serial as Serial
import           System.FS.IO (HandleIO)
import qualified System.FS.IO.Handle as FS
import qualified System.Posix.Fcntl as Unix
import qualified System.Posix.Files as Unix
import qualified System.Posix.Unistd as Unix

-- | For now we use the portable serial implementation of HasBlockIO. If you
-- want to provide a proper async I\/O implementation for OSX, then this is where
-- you should put it.
--
-- The recommended choice would be to use the POSIX AIO API.
ioHasBlockIO ::
     HasFS IO HandleIO
  -> IOI.IOCtxParams
  -> IO (HasBlockIO IO HandleIO)
ioHasBlockIO hfs _params =
    Serial.serialHasBlockIO
      hSetNoCache
      hAdvise
      hAllocate
      (IOI.tryLockFileIO hfs)
      hSynchronise
      (synchroniseDirectory hfs)
      (IOI.createHardLinkIO hfs Unix.createLink)
      hfs

hSetNoCache :: Handle HandleIO -> Bool -> IO ()
hSetNoCache h b =
  FS.withOpenHandle "hSetNoCache" (handleRaw h) (flip Unix.fileSetCaching (not b))

-- TODO: it is unclear if MacOS supports @posix_fadvise(2)@, and it's hard to
-- check because there are no manual pages online. For now, it's just hardcoded
-- to be a no-op.
hAdvise :: Handle HandleIO -> FileOffset -> FileOffset -> Advice -> IO ()
hAdvise _h _off _len _advice = pure ()

hAllocate :: Handle HandleIO -> FileOffset -> FileOffset -> IO ()
hAllocate _h _off _len = pure ()

hSynchronise :: Handle HandleIO -> IO ()
hSynchronise h = FS.withOpenHandle "hSynchronise" (handleRaw h) $ \fd ->
    Unix.fileSynchronise fd

synchroniseDirectory :: HasFS IO HandleIO -> FsPath -> IO ()
synchroniseDirectory hfs path =
    FS.withFile hfs path FS.ReadMode $ hSynchronise
