module System.FS.BlockIO.Internal (
    ioHasBlockIO
  ) where

import qualified System.FS.API as FS
import           System.FS.API (FsPath, Handle (..), HasFS)
import           System.FS.BlockIO.API (Advice (..), FileOffset, HasBlockIO)
import qualified System.FS.BlockIO.Internal.Fcntl as Fcntl
import qualified System.FS.BlockIO.IO.Internal as IOI
import           System.FS.IO (HandleIO)
import qualified System.FS.IO.Handle as FS
import qualified System.Posix.Fcntl as Fcntl (Advice (..), fileAdvise,
                     fileAllocate)
import qualified System.Posix.Files as Unix
import qualified System.Posix.Unistd as Unix

import qualified System.FS.BlockIO.Serial as Serial

-- | For now we use the portable serial implementation of HasBlockIO. If you
-- want to provide a proper async I\/O implementation for FreeBSD, then this is where
-- you should put it.
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
  FS.withOpenHandle "hSetNoCache" (handleRaw h) (flip Fcntl.fileSetCaching (not b))

hAdvise :: Handle HandleIO -> FileOffset -> FileOffset -> Advice -> IO ()
hAdvise h off len advice = FS.withOpenHandle "hAdvise" (handleRaw h) $ \fd ->
    Fcntl.fileAdvise fd off len advice'
  where
    advice' = case advice of
      AdviceNormal     -> Fcntl.AdviceNormal
      AdviceRandom     -> Fcntl.AdviceRandom
      AdviceSequential -> Fcntl.AdviceSequential
      AdviceWillNeed   -> Fcntl.AdviceWillNeed
      AdviceDontNeed   -> Fcntl.AdviceDontNeed
      AdviceNoReuse    -> Fcntl.AdviceNoReuse

hAllocate :: Handle HandleIO -> FileOffset -> FileOffset -> IO ()
hAllocate h off len = FS.withOpenHandle "hAllocate" (handleRaw h) $ \fd ->
    Fcntl.fileAllocate fd off len

hSynchronise :: Handle HandleIO -> IO ()
hSynchronise h = FS.withOpenHandle "hSynchronise" (handleRaw h) $ \fd ->
    Unix.fileSynchronise fd

synchroniseDirectory :: HasFS IO HandleIO -> FsPath -> IO ()
synchroniseDirectory hfs path =
    FS.withFile hfs path FS.ReadMode $ hSynchronise
