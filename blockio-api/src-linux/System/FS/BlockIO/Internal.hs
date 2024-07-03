{-# LANGUAGE CPP #-}

module System.FS.BlockIO.Internal (
    ioHasBlockIO
  ) where

import           System.FS.API (Handle (handleRaw), HasFS)
import           System.FS.BlockIO.API (HasBlockIO, IOCtxParams)
#if SERIALBLOCKIO
import qualified System.FS.BlockIO.Serial as Serial
#else
import qualified System.FS.BlockIO.Async as Async
#endif
import           System.FS.IO (HandleIO)
import           System.FS.IO.Handle (withOpenHandle)
import qualified System.Posix.Fcntl.NoCache as Unix

ioHasBlockIO ::
     HasFS IO HandleIO
  -> IOCtxParams
  -> IO (HasBlockIO IO HandleIO)
#if SERIALBLOCKIO
ioHasBlockIO hfs _params = Serial.serialHasBlockIO hSetNoCache hfs
#else
ioHasBlockIO hfs  params = Async.asyncHasBlockIO   hSetNoCache hfs params
#endif

hSetNoCache :: Handle HandleIO -> Bool -> IO ()
hSetNoCache h b =
  withOpenHandle "hSetNoCache" (handleRaw h) (flip Unix.writeFcntlNoCache b)
