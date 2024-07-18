{-# LANGUAGE CPP #-}
{-# LANGUAGE InterruptibleFFI #-}
{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE MultiWayIf #-}

{-# OPTIONS_GHC -Wno-missing-export-lists #-}

-- | File locking for Windows.
module GHC.Internal.IO.Handle.Lock.Windows where

#include "HsBaseConfig.h"

#if !defined(mingw32_HOST_OS)
import GHC.Base () -- Make implicit dependency known to build system
#else

##include <windows_cconv.h>
#include <windows.h>

import Data.Bits
import Data.Function
import GHC.IO.Handle.Windows (handleToHANDLE)
import Foreign.C.Error
import Foreign.C.Types
import Foreign.Marshal.Alloc
import Foreign.Marshal.Utils
import GHC.Base
import qualified GHC.Internal.Event.Windows as Mgr
import GHC.Event.Windows (LPOVERLAPPED, withOverlapped)
import GHC.IO.FD
import GHC.IO.Handle.FD
import GHC.IO.SubSystem
import GHC.Windows

import GHC.Internal.IO.Handle.Types (Handle, handleToFd)
import GHC.Internal.IO.Handle.Lock.Common (LockMode(..))

lockImpl :: Handle -> String -> LockMode -> Bool -> IO Bool
lockImpl = lockImplWinIO

lockImplWinIO :: Handle -> String -> LockMode -> Bool -> IO Bool
lockImplWinIO h ctx mode block = do
  wh      <- handleToHANDLE h
  fix $ \retry ->
          do retcode <- Mgr.withException ctx $
                          withOverlapped ctx wh 0 (startCB wh) completionCB
             case () of
              _ | retcode == #{const ERROR_OPERATION_ABORTED} -> retry
                | retcode == #{const ERROR_SUCCESS}           -> return True
                | retcode == #{const ERROR_LOCK_VIOLATION} && not block
                    -> return False
                | otherwise -> failWith ctx retcode
    where
      cmode = case mode of
                SharedLock    -> 0
                ExclusiveLock -> #{const LOCKFILE_EXCLUSIVE_LOCK}
      flags = if block
                 then cmode
                 else cmode .|. #{const LOCKFILE_FAIL_IMMEDIATELY}

      startCB wh lpOverlapped = do
        ret <- c_LockFileEx wh flags 0 #{const INFINITE} #{const INFINITE}
                            lpOverlapped
        return $ Mgr.CbNone ret

      completionCB err _dwBytes
        | err == #{const ERROR_SUCCESS} = Mgr.ioSuccess 0
        | otherwise                     = Mgr.ioFailed err

unlockImpl :: Handle -> IO ()
unlockImpl = unlockImplWinIO

unlockImplWinIO :: Handle -> IO ()
unlockImplWinIO h = do
  wh <- handleToHANDLE h
  _ <- Mgr.withException "unlockImpl" $
          withOverlapped "unlockImpl" wh 0 (startCB wh) completionCB
  return ()
    where
      startCB wh lpOverlapped = do
        ret <- c_UnlockFileEx wh 0 #{const INFINITE} #{const INFINITE}
                              lpOverlapped
        return $ Mgr.CbNone ret

      completionCB err _dwBytes
        | err == #{const ERROR_SUCCESS} = Mgr.ioSuccess 0
        | otherwise                     = Mgr.ioFailed err

-- https://msdn.microsoft.com/en-us/library/windows/desktop/aa365203.aspx
foreign import WINDOWS_CCONV interruptible "LockFileEx"
  c_LockFileEx :: HANDLE -> DWORD -> DWORD -> DWORD -> DWORD -> LPOVERLAPPED
               -> IO BOOL

-- https://msdn.microsoft.com/en-us/library/windows/desktop/aa365716.aspx
foreign import WINDOWS_CCONV interruptible "UnlockFileEx"
  c_UnlockFileEx :: HANDLE -> DWORD -> DWORD -> DWORD -> LPOVERLAPPED -> IO BOOL

#endif
