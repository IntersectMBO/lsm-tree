{-# LANGUAGE CApiFFI #-}
module System.Posix.IO.NoCache (
    OpenFileFlags (..),
    defaultFileFlags,
    openFdAt,
) where

import System.Posix.IO (OpenMode (..))
import System.Posix.Error (throwErrnoPathIfMinus1Retry)
import System.Posix.Types (Fd (..), CMode (..), FileMode)
import System.Posix.Internals (withFilePath)
import Data.Bits ((.|.))
import Foreign.C (CInt (..), CString)

#include "HsUnix.h"

-- |Correspond to some of the int flags from C's fcntl.h.
data OpenFileFlags =
 OpenFileFlags {
    append    :: Bool,           -- ^ O_APPEND
    exclusive :: Bool,           -- ^ O_EXCL, result is undefined if O_CREAT is False
                                 --
                                 -- __NOTE__: Result is undefined if 'creat' is 'Nothing'.
    noctty    :: Bool,           -- ^ O_NOCTTY
    nonBlock  :: Bool,           -- ^ O_NONBLOCK
    trunc     :: Bool,           -- ^ O_TRUNC
    nofollow  :: Bool,           -- ^ O_NOFOLLOW
                                 --
                                 -- @since 2.8.0.0
    creat     :: Maybe FileMode, -- ^ O_CREAT
                                 --
                                 -- @since 2.8.0.0
    cloexec   :: Bool,           -- ^ O_CLOEXEC
                                 --
                                 -- @since 2.8.0.0
    directory :: Bool,           -- ^ O_DIRECTORY
                                 --
                                 -- @since 2.8.0.0
    sync      :: Bool,           -- ^ O_SYNC
                                 --
                                 -- @since 2.8.0.0
    nocache   :: Bool            -- ^ O_DIRECT

 }
 deriving (Read, Show, Eq, Ord)

-- | Default values for the 'OpenFileFlags' type.
--
-- Each field of 'OpenFileFlags' is either 'False' or 'Nothing'
-- respectively.
defaultFileFlags :: OpenFileFlags
defaultFileFlags =
 OpenFileFlags {
    append    = False,
    exclusive = False,
    noctty    = False,
    nonBlock  = False,
    trunc     = False,
    nofollow  = False,
    creat     = Nothing,
    cloexec   = False,
    directory = False,
    sync      = False,
    nocache   = False
  }

-- | Open a file relative to an optional directory file descriptor.
--
-- Directory file descriptors can be used to avoid some race conditions when
-- navigating changing directory trees, or to retain access to a portion of the
-- directory tree that would otherwise become inaccessible after dropping
-- privileges.
openFdAt :: Maybe Fd -- ^ Optional directory file descriptor
         -> FilePath -- ^ Pathname to open
         -> OpenMode -- ^ Read-only, read-write or write-only
         -> OpenFileFlags -- ^ Append, exclusive, truncate, etc.
         -> IO Fd
openFdAt fdMay name how flags =
  withFilePath name $ \str ->
    throwErrnoPathIfMinus1Retry "openFdAt" name $
      openat_ fdMay str how flags


-- |Open and optionally create a file relative to an optional
-- directory file descriptor.
openat_  :: Maybe Fd -- ^ Optional directory file descriptor
         -> CString -- ^ Pathname to open
         -> OpenMode -- ^ Read-only, read-write or write-only
         -> OpenFileFlags -- ^ Append, exclusive, etc.
         -> IO Fd
openat_ fdMay str how (OpenFileFlags appendFlag exclusiveFlag nocttyFlag
                                nonBlockFlag truncateFlag nofollowFlag
                                creatFlag cloexecFlag directoryFlag
                                syncFlag nocacheFlag) =
    Fd <$> c_openat c_fd str all_flags mode_w
  where
    c_fd = maybe (#const AT_FDCWD) (\ (Fd fd) -> fd) fdMay
    all_flags  = creat .|. flags .|. open_mode

    flags =
       (if appendFlag       then (#const O_APPEND)    else 0) .|.
       (if exclusiveFlag    then (#const O_EXCL)      else 0) .|.
       (if nocttyFlag       then (#const O_NOCTTY)    else 0) .|.
       (if nonBlockFlag     then (#const O_NONBLOCK)  else 0) .|.
       (if truncateFlag     then (#const O_TRUNC)     else 0) .|.
       (if nofollowFlag     then (#const O_NOFOLLOW)  else 0) .|.
       (if cloexecFlag      then (#const O_CLOEXEC)   else 0) .|.
       (if directoryFlag    then (#const O_DIRECTORY) else 0) .|.
       (if syncFlag         then (#const O_SYNC)      else 0) .|.
       (if nocacheFlag      then (#const O_DIRECT)    else 0)

    (creat, mode_w) = case creatFlag of
                        Nothing -> (0,0)
                        Just x  -> ((#const O_CREAT), x)

    open_mode = case how of
                   ReadOnly  -> (#const O_RDONLY)
                   WriteOnly -> (#const O_WRONLY)
                   ReadWrite -> (#const O_RDWR)

foreign import capi unsafe "HsUnix.h openat"
   c_openat :: CInt -> CString -> CInt -> CMode -> IO CInt
