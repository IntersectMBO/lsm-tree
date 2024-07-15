module System.FS.BlockIO.Internal (
    ioHasBlockIO
  ) where

import           System.FS.API (Handle, HasFS)
import           System.FS.BlockIO.API (Advice (..), FileOffset, HasBlockIO,
                     IOCtxParams)
import qualified System.FS.BlockIO.Serial as Serial
import           System.FS.IO (HandleIO)

-- | For now we use the portable serial implementation of HasBlockIO. If you
-- want to provide a proper async I/O implementation for Windows, then this is
-- where you should put it.
--
-- The recommended choice would be to use the Win32 IOCP API.
ioHasBlockIO ::
     HasFS IO HandleIO
  -> IOCtxParams
  -> IO (HasBlockIO IO HandleIO)
ioHasBlockIO hfs _params = Serial.serialHasBlockIO hSetNoCache hAdvise hAllocate hfs

hSetNoCache :: Handle HandleIO -> Bool -> IO ()
hSetNoCache _h _b = pure ()

hAdvise :: Handle HandleIO -> FileOffset -> FileOffset -> Advice -> IO ()
hAdvise _h _off _len _advice = pure ()

hAllocate :: Handle HandleIO -> FileOffset -> FileOffset -> IO ()
hAllocate _h _off _len = pure ()
