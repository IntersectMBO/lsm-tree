{-# LANGUAGE CPP #-}
module System.Posix.Fcntl.NoCache (
    readFcntlNoCache,
    writeFcntlNoCache,
) where

#ifdef darwin_HOST_OS
#else
import           Data.Bits (complement, (.&.), (.|.))
import           Foreign.C.Error (throwErrnoIfMinus1)
import           System.Posix.Internals (c_fcntl_read)
#endif

import           Foreign.C.Error (throwErrnoIfMinus1_)
import           System.Posix.Internals (c_fcntl_write)
import           System.Posix.Types (Fd (..))

#define _GNU_SOURCE
#include <unistd.h>
#include <fcntl.h>

readFcntlNoCache :: Fd -> IO Bool
writeFcntlNoCache :: Fd -> Bool -> IO ()

#ifdef darwin_HOST_OS

readFcntlNoCache (Fd _) = return False

writeFcntlNoCache (Fd fd) val = do
    throwErrnoIfMinus1_ "writeFcntlNoCache" (c_fcntl_write fd #{const F_NOCACHE} (if val then 1 else 0))

#else

readFcntlNoCache (Fd fd) = do
    r <- throwErrnoIfMinus1 "readFcntlNoCache" (c_fcntl_read fd #{const F_GETFL})
    return ((r .&. opt_val) /= 0)
  where
    opt_val = #{const O_DIRECT}

writeFcntlNoCache (Fd fd) val = do
    r <- throwErrnoIfMinus1 "writeFcntlNoCache" (c_fcntl_read fd #{const F_GETFL})
    let r' | val       = fromIntegral r .|. opt_val
           | otherwise = fromIntegral r .&. complement opt_val
    throwErrnoIfMinus1_ "writeFcntlNoCache" (c_fcntl_write fd #{const F_SETFL} r')
  where
    opt_val = #{const O_DIRECT}
#endif
