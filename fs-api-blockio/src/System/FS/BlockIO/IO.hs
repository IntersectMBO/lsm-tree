module System.FS.BlockIO.IO (
    ioHasBlockIO
  ) where

import           System.FS.API (HasBufFS, HasFS)
import           System.FS.BlockIO.API (HasBlockIO, IOCtxParams)
import qualified System.FS.BlockIO.Internal as I
import           System.FS.IO (HandleIO)

-- | Platform-dependent IO instantiation of 'HasBlockIO'.
ioHasBlockIO ::
     HasFS IO HandleIO
  -> HasBufFS IO HandleIO
  -> Maybe IOCtxParams
  -> IO (HasBlockIO IO HandleIO)
ioHasBlockIO = I.ioHasBlockIO
