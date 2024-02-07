module Data.Digest.CRC32C.Internal where

#include <crc32c/crc32c.h>

import Foreign.C
import Foreign.Ptr

lib_crc32c_extend :: CUInt -> Ptr CUChar -> CULong -> CUInt
lib_crc32c_extend = {#call pure crc32c_extend #}

lib_crc32c_value :: Ptr CUChar -> CULong -> CUInt
lib_crc32c_value  = {#call pure crc32c_value  #}