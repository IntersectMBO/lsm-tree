{-# LANGUAGE CPP #-}

#include "HsUnix.h"

module System.FS.BlockIO.Internal (
    ioHasBlockIO
  ) where

import qualified System.FS.API as FS
import           System.FS.API (FsPath, Handle (..), HasFS)
import           System.FS.BlockIO.API (Advice (..), FileOffset, HasBlockIO)
import qualified System.FS.BlockIO.Internal.Fcntl as Fcntl
import qualified System.FS.BlockIO.IO.Internal as IOI
import qualified System.FS.BlockIO.Serial as Serial
import           System.FS.IO (HandleIO)
import qualified System.FS.IO.Handle as FS
import qualified System.Posix.Fcntl as Fcntl (Advice (..), fileAdvise)
#if HAVE_POSIX_FALLOCATE
import qualified System.Posix.Fcntl as Fcntl (fileAllocate)
#endif
import qualified System.Posix.Files as Unix
import qualified System.Posix.Unistd as Unix

-- | A portable, serial implementation of 'HasBlockIO' for POSIX systems that
-- do not have a dedicated implementation (Linux, MacOS, and FreeBSD do). It
-- uses only the parts of the @unix@ package that are available on the
-- platform, and degrades to no-ops where an operation is not supported.
ioHasBlockIO ::
     HasFS IO HandleIO
  -> IOI.IOCtxParams
  -> IO (HasBlockIO IO HandleIO)
ioHasBlockIO hfs _params =
    Serial.serialHasBlockIO
      hSetNoCache
      hAdvise
      hAllocate
      (IOI.tryLockFileIO hfs)
      hSynchronise
      (synchroniseDirectory hfs)
      (IOI.createHardLinkIO hfs Unix.createLink)
      hfs

hSetNoCache :: Handle HandleIO -> Bool -> IO ()
hSetNoCache h b =
  FS.withOpenHandle "hSetNoCache" (handleRaw h) (flip Fcntl.fileSetCaching (not b))

-- @posix_fadvise(2)@ where available; @unix@ makes 'fileAdvise' a no-op otherwise.
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

-- @posix_fallocate(2)@ where available. Unlike 'fileAdvise', @unix@'s
-- 'fileAllocate' throws on platforms without it, so this has to be guarded.
hAllocate :: Handle HandleIO -> FileOffset -> FileOffset -> IO ()
#if HAVE_POSIX_FALLOCATE
hAllocate h off len = FS.withOpenHandle "hAllocate" (handleRaw h) $ \fd ->
    Fcntl.fileAllocate fd off len
#else
hAllocate _h _off _len = pure ()
#endif

hSynchronise :: Handle HandleIO -> IO ()
hSynchronise h = FS.withOpenHandle "hSynchronise" (handleRaw h) $ \fd ->
    Unix.fileSynchronise fd

synchroniseDirectory :: HasFS IO HandleIO -> FsPath -> IO ()
synchroniseDirectory hfs path =
    FS.withFile hfs path FS.ReadMode $ hSynchronise
