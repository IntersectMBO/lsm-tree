module System.FS.BlockIO.Internal (
    ioHasBlockIO
  ) where

import           System.FS.API (HasFS)
import           System.FS.BlockIO.API (HasBlockIO, IOCtxParams)
import qualified System.FS.BlockIO.Serial as Serial
import           System.FS.IO (HandleIO)

-- | For now we use the portable serial implementation of HasBlockIO. If you
-- want to provide a proper async I/O implementation for Windows, then this is
-- where you should put it.
--
-- The recommended choice would be to use the Win32 IOCP API.
ioHasBlockIO ::
     HasFS IO HandleIO
  -> Maybe IOCtxParams
  -> IO (HasBlockIO IO HandleIO)
ioHasBlockIO hasFS _ = Serial.serialHasBlockIO hasFS
