module System.FS.BlockIO.Internal (
    ioHasBlockIO
  ) where

import           System.FS.API (Handle (handleRaw), HasFS)
import           System.FS.BlockIO.API (Advice (..), FileOffset, HasBlockIO,
                     IOCtxParams)
import qualified System.FS.BlockIO.Serial as Serial
import           System.FS.IO (HandleIO)
import           System.FS.IO.Handle (withOpenHandle)
import qualified System.Posix.Fcntl.NoCache as Unix

-- | For now we use the portable serial implementation of HasBlockIO. If you
-- want to provide a proper async I/O implementation for OSX, then this is where
-- you should put it.
--
-- The recommended choice would be to use the POSIX AIO API.
ioHasBlockIO ::
     HasFS IO HandleIO
  -> IOCtxParams
  -> IO (HasBlockIO IO HandleIO)
ioHasBlockIO hfs _params = Serial.serialHasBlockIO hSetNoCache hAdvise hAllocate hfs

hSetNoCache :: Handle HandleIO -> Bool -> IO ()
hSetNoCache h b =
  withOpenHandle "hSetNoCache" (handleRaw h) (flip Unix.writeFcntlNoCache b)

-- TODO: it is unclear if MacOS supports @posix_fadvise(2)@, and it's hard to
-- check because there are no manual pages online. For now, it's just hardcoded
-- to be a no-op.
hAdvise :: Handle HandleIO -> FileOffset -> FileOffset -> Advice -> IO ()
hAdvise _h _off _len _advice = pure ()

hAllocate :: Handle HandleIO -> FileOffset -> FileOffset -> IO ()
hAllocate _h _off _len = pure ()
