-- | Implementations using the real file system.
--
-- The implementation of the 'HasBlockIO' interface provided in this module is
-- platform-dependent. Most importantly, on Linux, the implementation of
-- 'submitIO' is backed by @blockio-uring@: a library for asynchronous I/O. On
-- Windows and MacOS, the implementation of 'submitIO' only supports serial I/O.
module System.FS.BlockIO.IO (
    -- * Implementation details #impl#
    -- $impl

    -- * Initialisation
    ioHasBlockIO
  , withIOHasBlockIO
    -- ** Parameters
  , IOI.IOCtxParams (..)
  , IOI.defaultIOCtxParams
    -- ** Unsafe
  , unsafeFromHasFS
  , withUnsafeFromHasFS
  ) where

import           Control.Exception (bracket)
import           System.FS.API (HasFS, MountPoint)
import           System.FS.BlockIO.API (HasBlockIO (..))
import qualified System.FS.BlockIO.Internal as I
import qualified System.FS.BlockIO.IO.Internal as IOI
import           System.FS.IO (HandleIO, ioHasFS)

{- $impl

  Though the 'HasBlockIO' interface tries to capture uniform behaviour, each
  function in this implementation for the real file system can have subtly
  different effects depending on the underlying patform. For example, some
  features are not provided by some operating systems, and in some cases the
  features behave subtly differently for different operating systems. For this
  reason, we include below some documentation about the effects of calling the
  interface functions on different platforms.

  Note: if the @serialblockio@ Cabal flag is enabled, then the Linux implementation
  uses a mocked context and serial I/O for 'close' and 'submitIO', just like the
  MacOS and Windows implementations do.

  [IO context]:  When an instance of the 'HasBlockIO' interface for Linux
    systems is initialised, an @io_uring@ context is created using the
    @blockio-uring@ package and stored in the 'HasBlockIO' closure. For uniform
    behaviour, each other platform creates and stores a mocked IO context that
    has the same open/closed behaviour as an @io_uring@ context. In summary,
    each platform creates:

    * Linux: an @io_uring@ context provided by the @blockio-uring@ package
    * MacOS: a mocked context using an @MVar@
    * Windows: a mocked conext using an @MVar@

  ['close']:

    * Linux: close the @io_uring@ context through the @blockio-uring@ package
    * MacOS: close the mocked context
    * Windows: close the mocked context

  ['submitIO']: Submit a batch of I/O operations using:

    * Linux: the @submitIO@ function from the @blockio-uring@ package
    * MacOS: serial I/O using a 'HasFS'
    * Windows: serial I/O using a 'HasFS'

  ['hSetNoCache']:

    * Linux: set the @O_DIRECT@ flag
    * MacOS: set the @F_NOCACHE@ flag
    * Windows: no-op

  ['hAdvise']:

    * Linux: perform @posix_fadvise(2)@
    * MacOS: no-op
    * Windows: no-op

  ['hAllocate']:

    * Linux: perform @posix_fallocate(2)@
    * MacOS: no-op
    * Windows: no-op

  ['tryLockFile']: This uses different locking methods depending on the OS.

    * Linux: Open file descriptor (OFD)
    * MacOS: @flock@
    * Windows: @LockFileEx@

  ['hSynchronise']:

    * Linux: perform @fsync(2)@
    * MacOS: perform @fsync(2)@
    * Windows: perform @flushFileBuffers@

  ['synchroniseDirectory']:

    * Linux: perform @fsync(2)@
    * MacOS: perform @fsync(2)@
    * Windows: no-op

  ['createHardLink']:

    * Linux: perform @link@
    * MacOS: perform @link@
    * Windows: perform @CreateHardLinkW@
-}

-- | An implementation of the 'HasBlockIO' interface using the real file system.
--
-- Make sure to use 'close' the resulting 'HasBlockIO' when it is no longer
-- used. 'withUnsafeFromHasFS' does this automatically.
--
-- === Unsafe
--
-- You will probably want to use 'ioHasBlockIO' or 'withIOHasBlockIO' instead.
--
-- Only a 'HasFS' for the real file system, like 'ioHasFS', should be passed to
-- 'unsafeFromHasFS'. Technically, one could pass a 'HasFS' for a simulated file
-- system, but then the resulting 'HasBlockIO' would contain a mix of simulated
-- and real functions, which is probably not what you want.
unsafeFromHasFS ::
     HasFS IO HandleIO
  -> IOI.IOCtxParams
  -> IO (HasBlockIO IO HandleIO)
unsafeFromHasFS = I.ioHasBlockIO

-- | Perform an action using a 'HasBlockIO' instance that is only open for the
-- duration of the action.
--
-- The 'HasBlockIO' is initialised using 'unsafeFromHasFS'.
--
-- === Unsafe
--
-- You will probably want to use 'ioHasBlockIO' or 'withIOHasBlockIO' instead.
--
-- Only a 'HasFS' for the real file system, like 'ioHasFS', should be passed to
-- 'withUnsafeFromHasFS'. Technically, one could pass a 'HasFS' for a simulated
-- file system, but then the resulting 'HasBlockIO' would contain a mix of
-- simulated and real functions, which is probably not what you want.
withUnsafeFromHasFS ::
     HasFS IO HandleIO
  -> IOI.IOCtxParams
  -> (HasBlockIO IO HandleIO -> IO a)
  -> IO a
withUnsafeFromHasFS hfs params =
    bracket (unsafeFromHasFS hfs params) (\HasBlockIO{close} -> close)

-- | An implementation of the 'HasBlockIO' interface using the real file system.
--
-- Make sure to use 'close' the resulting 'HasBlockIO' when it is no longer
-- used. 'withIOHasBlockIO' does this automatically.
--
-- The 'HasFS' interface is instantiated using 'ioHasFS'.
ioHasBlockIO ::
     MountPoint
  -> IOI.IOCtxParams
  -> IO (HasFS IO HandleIO, HasBlockIO IO HandleIO)
ioHasBlockIO mount params = do
    let hfs = ioHasFS mount
    hbio <- unsafeFromHasFS hfs params
    pure (hfs, hbio)

-- | Perform an action using a 'HasFS' and a 'HasBlockIO' instance. The latter
-- is only open for the duration of the action.
--
-- The 'HasFS' and 'HasBlockIO' interfaces are initialised using 'ioHasBlockIO'.
withIOHasBlockIO ::
     MountPoint
  -> IOI.IOCtxParams
  -> (HasFS IO HandleIO -> HasBlockIO IO HandleIO -> IO a)
  -> IO a
withIOHasBlockIO mount params action =
    bracket (ioHasBlockIO mount params) (\(_, HasBlockIO{close}) -> close) (uncurry action)
