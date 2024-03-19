module System.FS.BlockIO.Internal (
    ioHasBlockIO
  ) where

import           System.FS.API (HasBufFS, HasFS)
import           System.FS.BlockIO.API (HasBlockIO, IOCtxParams)
import qualified System.FS.BlockIO.Async as I
import           System.FS.IO (HandleIO)

ioHasBlockIO ::
     HasFS IO HandleIO
  -> HasBufFS IO HandleIO
  -> Maybe IOCtxParams
  -> IO (HasBlockIO IO HandleIO)
ioHasBlockIO hfs _bhfs = I.asyncHasBlockIO hfs
