{-# LANGUAGE CPP #-}

module System.FS.BlockIO.Internal (
    ioHasBlockIO
  ) where

import           System.FS.API (Handle (..), HasFS)
import qualified System.FS.BlockIO.API as FS
import           System.FS.BlockIO.API (Advice (..), FileOffset, HasBlockIO,
                     IOCtxParams)
import qualified System.FS.BlockIO.Serial as Serial
import           System.FS.IO (HandleIO)
import qualified System.FS.IO.Handle as FS
import qualified System.Posix.Fcntl.NoCache as Unix
import qualified System.Posix.Unistd as Unix

-- | For now we use the portable serial implementation of HasBlockIO. If you
-- want to provide a proper async I/O implementation for OSX, then this is where
-- you should put it.
--
-- The recommended choice would be to use the POSIX AIO API.
ioHasBlockIO ::
     HasFS IO HandleIO
  -> IOCtxParams
  -> IO (HasBlockIO IO HandleIO)
ioHasBlockIO hfs _params = Serial.serialHasBlockIO hSetNoCache hAdvise hAllocate hSynchronize (FS.tryLockFileIO hfs) hfs

hSetNoCache :: Handle HandleIO -> Bool -> IO ()
hSetNoCache h b =
  FS.withOpenHandle "hSetNoCache" (handleRaw h) (flip Unix.writeFcntlNoCache b)

-- | Prefer @fdatasync@ over @fsync@ when available for greater throughput.
hSynchronize :: Handle HandleIO -> IO ()
hSynchronize h =
  FS.withOpenHandle "hSynchronize" (handleRaw h)
#if HAVE_FDATASYNC
    Unix.fileSynchroniseDataOnly
#else
    Unix.fileSynchronise
#endif

-- TODO: it is unclear if MacOS supports @posix_fadvise(2)@, and it's hard to
-- check because there are no manual pages online. For now, it's just hardcoded
-- to be a no-op.
hAdvise :: Handle HandleIO -> FileOffset -> FileOffset -> Advice -> IO ()
hAdvise _h _off _len _advice = pure ()

hAllocate :: Handle HandleIO -> FileOffset -> FileOffset -> IO ()
hAllocate _h _off _len = pure ()
