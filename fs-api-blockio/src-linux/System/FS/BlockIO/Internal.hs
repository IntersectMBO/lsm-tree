{-# LANGUAGE CPP #-}

module System.FS.BlockIO.Internal (
    ioHasBlockIO
  ) where

import           System.FS.API (HasBufFS, HasFS)
import           System.FS.BlockIO.API (HasBlockIO, IOCtxParams)
#if SERIALBLOCKIO
import qualified System.FS.BlockIO.Serial as Serial
#else
import qualified System.FS.BlockIO.Async as Async
#endif
import           System.FS.IO (HandleIO)

ioHasBlockIO ::
     HasFS IO HandleIO
  -> HasBufFS IO HandleIO
  -> Maybe IOCtxParams
  -> IO (HasBlockIO IO HandleIO)
#if SERIALBLOCKIO
ioHasBlockIO hasFS hasBufFS _ = Serial.serialHasBlockIO hasFS hasBufFS
#else
ioHasBlockIO hfs _bhfs        = Async.asyncHasBlockIO hfs
#endif
