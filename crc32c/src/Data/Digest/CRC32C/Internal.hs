module Data.Digest.CRC32C.Internal where

import Foreign.C
import Foreign.Ptr

foreign import ccall "crc32c/crc32c.h crc32c_extend"
  lib_crc32c_extend :: CUInt -> Ptr CUChar -> CSize -> CUInt

foreign import ccall "crc32c/crc32c.h crc32c_value"
  lib_crc32c_value :: Ptr CUChar -> CSize -> CUInt
