module System.FS.BlockIO.IO (
    ioHasBlockIO
  , withIOHasBlockIO
  ) where

import           Control.Exception (bracket)
import           System.FS.API (HasFS)
import           System.FS.BlockIO.API (HasBlockIO (..), IOCtxParams)
import qualified System.FS.BlockIO.Internal as I
import           System.FS.IO (HandleIO)

-- | Platform-dependent IO instantiation of 'HasBlockIO'.
ioHasBlockIO ::
     HasFS IO HandleIO
  -> IOCtxParams
  -> IO (HasBlockIO IO HandleIO)
ioHasBlockIO = I.ioHasBlockIO

withIOHasBlockIO ::
     HasFS IO HandleIO
  -> IOCtxParams
  -> (HasBlockIO IO HandleIO -> IO a)
  -> IO a
withIOHasBlockIO hfs params action =
    bracket (ioHasBlockIO hfs params) (\HasBlockIO{close} -> close) action
