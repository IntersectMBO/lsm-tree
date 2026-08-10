{-# LANGUAGE CPP #-}

-- | Compatibility layer for the @unix@ package to provide a @fileSetCaching@ function.
--
-- @unix >= 2.8.7@ defines a @fileSetCaching@ function, but @unix < 2.8.7@ does not.
-- This module defines the function for @unix@ versions @< 2.8.7@.
-- The implementation is adapted from https://github.com/haskell/unix/blob/v2.8.8.0/System/Posix/Fcntl.hsc#L116-L182.
--
-- NOTE: in the future if we no longer support @unix@ versions @< 2.8.7@, then this module can be removed.
module System.FS.BlockIO.Internal.Fcntl (fileSetCaching) where

#if MIN_VERSION_unix(2,8,7)

import System.Posix.Fcntl (fileSetCaching)

#else

#include "HsUnix.h"
#include <fcntl.h>

import           Data.Bits
import           Foreign.C
import           System.Posix.Internals
import           System.Posix.Types (Fd (Fd))

-- | For simplification, we considered that FreeBSD HAS_O_DIRECT
fileSetCaching :: Fd -> Bool -> IO ()
fileSetCaching (Fd fd) val = do
    r <- throwErrnoIfMinus1 "fileSetCaching" (c_fcntl_read fd #{const F_GETFL})
    let r' | val       = fromIntegral r .&. complement opt_val
           | otherwise = fromIntegral r .|. opt_val
    throwErrnoIfMinus1_ "fileSetCaching" (c_fcntl_write fd #{const F_SETFL} r')
  where
    opt_val = #{const O_DIRECT}

#endif