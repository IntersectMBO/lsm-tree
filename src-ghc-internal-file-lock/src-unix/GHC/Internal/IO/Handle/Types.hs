module GHC.Internal.IO.Handle.Types (
    Handle
  , handleToFd
  ) where

import           Foreign.C (CInt)
import           System.Posix.Types (Fd (..))

type Handle = Fd

handleToFd :: Handle -> IO CInt
handleToFd (Fd x) = pure x
