{-# LANGUAGE CPP #-}

module System.FS.BlockIO.Internal (
    ioHasBlockIO
  ) where

import           System.FS.API (Handle (..), HasFS)
import qualified System.FS.BlockIO.API as FS
import           System.FS.BlockIO.API (Advice (..), FileOffset, HasBlockIO,
                     IOCtxParams)
import           System.FS.IO (HandleIO)
import qualified System.FS.IO.Handle as FS
import qualified System.Posix.Fcntl as Fcntl
import qualified System.Posix.Fcntl.NoCache as Unix

#if SERIALBLOCKIO
import qualified System.FS.BlockIO.Serial as Serial
#else
import qualified System.FS.BlockIO.Async as Async
#endif

ioHasBlockIO ::
     HasFS IO HandleIO
  -> IOCtxParams
  -> IO (HasBlockIO IO HandleIO)
#if SERIALBLOCKIO
ioHasBlockIO hfs _params = Serial.serialHasBlockIO hSetNoCache hAdvise hAllocate (FS.tryLockFileIO hfs) hfs
#else
ioHasBlockIO hfs  params = Async.asyncHasBlockIO   hSetNoCache hAdvise hAllocate (FS.tryLockFileIO hfs) hfs params
#endif

hSetNoCache :: Handle HandleIO -> Bool -> IO ()
hSetNoCache h b =
  FS.withOpenHandle "hSetNoCache" (handleRaw h) (flip Unix.writeFcntlNoCache b)

hAdvise :: Handle HandleIO -> FileOffset -> FileOffset -> Advice -> IO ()
hAdvise h off len advice = FS.withOpenHandle "hAdvise" (handleRaw h) $ \fd ->
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
hAllocate h off len = FS.withOpenHandle "hAllocate" (handleRaw h) $ \fd ->
    Fcntl.fileAllocate fd off len
