{-# LANGUAGE CPP #-}

module System.FS.BlockIO.Internal (
    ioHasBlockIO
  ) where

import           System.FS.API (Handle (handleRaw), HasFS)
import           System.FS.BlockIO.API (Advice (..), FileOffset, HasBlockIO,
                     IOCtxParams, LockMode)
#if SERIALBLOCKIO
import qualified System.FS.BlockIO.Serial as Serial
#else
import qualified System.FS.BlockIO.Async as Async
#endif
import qualified GHC.Internal.IO.Handle.Lock as GHC
import           System.FS.IO (HandleIO)
import           System.FS.IO.Handle (withOpenHandle)
import qualified System.Posix.Fcntl as Fcntl
import qualified System.Posix.Fcntl.NoCache as Unix

ioHasBlockIO ::
     HasFS IO HandleIO
  -> IOCtxParams
  -> IO (HasBlockIO IO HandleIO)
#if SERIALBLOCKIO
ioHasBlockIO hfs _params = Serial.serialHasBlockIO hSetNoCache hAdvise hAllocate hTryLock hUnlock hfs
#else
ioHasBlockIO hfs  params = Async.asyncHasBlockIO   hSetNoCache hAdvise hAllocate hTryLock hUnlock hfs params
#endif

hSetNoCache :: Handle HandleIO -> Bool -> IO ()
hSetNoCache h b =
  withOpenHandle "hSetNoCache" (handleRaw h) (flip Unix.writeFcntlNoCache b)

hAdvise :: Handle HandleIO -> FileOffset -> FileOffset -> Advice -> IO ()
hAdvise h off len advice = withOpenHandle "hAdvise" (handleRaw h) $ \fd ->
    Fcntl.fileAdvise fd off len advice'
  where
    advice' = case advice of
      AdviceNormal     -> Fcntl.AdviceNormal
      AdviceRandom     -> Fcntl.AdviceRandom
      AdviceSequential -> Fcntl.AdviceSequential
      AdviceWillNeed   -> Fcntl.AdviceWillNeed
      AdviceDontNeed   -> Fcntl.AdviceDontNeed
      AdviceNoReuse    -> Fcntl.AdviceNoReuse

hAllocate :: Handle HandleIO -> FileOffset -> FileOffset -> IO ()
hAllocate h off len = withOpenHandle "hAllocate" (handleRaw h) $ \fd ->
    Fcntl.fileAllocate fd off len

hTryLock :: Handle HandleIO -> LockMode -> IO Bool
hTryLock h mode = withOpenHandle "hTryLock" (handleRaw h) $ \fd -> do
    GHC.hTryLock fd mode

hUnlock :: Handle HandleIO -> IO ()
hUnlock h = withOpenHandle "hUnlock" (handleRaw h) $ \fd -> do
    GHC.hUnlock fd
