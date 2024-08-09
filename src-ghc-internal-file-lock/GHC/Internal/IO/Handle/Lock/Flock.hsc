{-# LANGUAGE CPP #-}
{-# LANGUAGE InterruptibleFFI #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}
{-# LANGUAGE NoImplicitPrelude #-}

{-# OPTIONS_GHC -Wno-missing-export-lists #-}

-- | File locking via POSIX @flock@.
module GHC.Internal.IO.Handle.Lock.Flock where

#include "HsBaseConfig.h"

#if !HAVE_FLOCK
import GHC.Base () -- Make implicit dependency known to build system
#else

#include <sys/file.h>

import Data.Bits
import Data.Function
import Foreign.C.Error
import Foreign.C.Types
import GHC.Base
import GHC.IO.Exception

import GHC.Internal.IO.Handle.Lock.Common
import GHC.Internal.IO.Handle.Types (Handle, handleToFd)

lockImpl :: Handle -> String -> LockMode -> Bool -> IO Bool
lockImpl h ctx mode block = do
  fd <- handleToFd h
  let flags = cmode .|. (if block then 0 else #{const LOCK_NB})
  fix $ \retry -> c_flock fd flags >>= \case
    0 -> return True
    _ -> getErrno >>= \errno -> if
      | not block
      , errno == eAGAIN || errno == eACCES -> return False
      | errno == eINTR -> retry
      | otherwise -> ioException $ errnoToIOError ctx errno Nothing Nothing
  where
    cmode = case mode of
      SharedLock    -> #{const LOCK_SH}
      ExclusiveLock -> #{const LOCK_EX}

unlockImpl :: Handle -> IO ()
unlockImpl h = do
  fd <- handleToFd h
  throwErrnoIfMinus1_ "flock" $ c_flock fd #{const LOCK_UN}

foreign import ccall interruptible "flock"
  c_flock :: CInt -> CInt -> IO CInt

#endif
