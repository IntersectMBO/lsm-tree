module System.FS.BlockIO.IO (
    ioHasBlockIO
  , withIOHasBlockIO
  , IOI.IOCtxParams (..)
  , IOI.defaultIOCtxParams
  ) where

import           Control.Exception (bracket)
import           System.FS.API (HasFS)
import           System.FS.BlockIO.API (HasBlockIO (..))
import qualified System.FS.BlockIO.Internal as I
import qualified System.FS.BlockIO.IO.Internal as IOI
import           System.FS.IO (HandleIO)

-- | Platform-dependent IO instantiation of 'HasBlockIO'.
ioHasBlockIO ::
     HasFS IO HandleIO
  -> IOI.IOCtxParams
  -> IO (HasBlockIO IO HandleIO)
ioHasBlockIO = I.ioHasBlockIO

withIOHasBlockIO ::
     HasFS IO HandleIO
  -> IOI.IOCtxParams
  -> (HasBlockIO IO HandleIO -> IO a)
  -> IO a
withIOHasBlockIO hfs params action =
    bracket (ioHasBlockIO hfs params) (\HasBlockIO{close} -> close) action
