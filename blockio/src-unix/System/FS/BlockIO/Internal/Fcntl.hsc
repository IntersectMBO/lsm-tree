{-# LANGUAGE CPP #-}

#include "HsUnix.h"

-- | Compatibility layer for the @unix@ package to provide a @fileSetCaching@
-- function on any POSIX platform.
--
-- @unix >= 2.8.7@ defines @fileSetCaching@, but only for platforms that have
-- @O_DIRECT@ or @F_NOCACHE@; elsewhere it throws. Older @unix@ versions do not
-- define it at all. In both cases this module provides a no-op instead: the
-- portable implementation does not control caching on such platforms.
module System.FS.BlockIO.Internal.Fcntl (fileSetCaching) where

#if MIN_VERSION_unix(2,8,7) && (HAVE_O_DIRECT || HAVE_F_NOCACHE)

import System.Posix.Fcntl (fileSetCaching)

#else

import System.Posix.Types (Fd)

fileSetCaching :: Fd -> Bool -> IO ()
fileSetCaching _ _ = pure ()

#endif
