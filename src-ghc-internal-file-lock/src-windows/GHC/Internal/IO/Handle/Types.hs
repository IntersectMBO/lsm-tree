module GHC.Internal.IO.Handle.Types (
    Handle
  , handleToHANDLE
  ) where

import           System.Win32.Types

type Handle = HANDLE

handleToHANDLE :: Handle -> HANDLE
handleToHANDLE = id
