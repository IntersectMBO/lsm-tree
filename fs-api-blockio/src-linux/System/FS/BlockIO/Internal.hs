{-# LANGUAGE CPP #-}

module System.FS.BlockIO.Internal (
    ioHasBlockIO
  ) where

import           System.FS.API (HasFS)
import           System.FS.BlockIO.API (HasBlockIO, IOCtxParams)
#if SERIALBLOCKIO
import qualified System.FS.BlockIO.Serial as Serial
#else
import qualified System.FS.BlockIO.Async as Async
#endif
import           System.FS.IO (HandleIO)

ioHasBlockIO ::
     HasFS IO HandleIO
  -> Maybe IOCtxParams
  -> IO (HasBlockIO IO HandleIO)
#if SERIALBLOCKIO
ioHasBlockIO hasFS _ = Serial.serialHasBlockIO hasFS
#else
ioHasBlockIO hfs     = Async.asyncHasBlockIO hfs
#endif
