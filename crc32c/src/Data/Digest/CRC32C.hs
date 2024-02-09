module Data.Digest.CRC32C (
    crc32c
  , crc32c_update
  ) where

import           Data.ByteString (ByteString)
import           Data.ByteString.Unsafe (unsafeUseAsCStringLen)
import           Data.Word

import           Foreign
import           Foreign.C
import           Foreign.Marshal.Unsafe

crc32c :: ByteString -> Word32
crc32c bs =
  fromIntegral $
    unsafeLocalState $
      unsafeUseAsCStringLen bs $ \(p, l) ->
        lib_crc32c_value (castPtr p) (fromIntegral l)

crc32c_update :: Word32 -> ByteString -> Word32
crc32c_update hash bs =
  fromIntegral $
    unsafeLocalState $
      unsafeUseAsCStringLen bs $ \(p, l) ->
        lib_crc32c_extend (fromIntegral hash) (castPtr p) (fromIntegral l)

foreign import ccall "crc32c/crc32c.h crc32c_extend"
  lib_crc32c_extend :: CUInt -> Ptr CUChar -> CSize -> IO CUInt

foreign import ccall "crc32c/crc32c.h crc32c_value"
  lib_crc32c_value :: Ptr CUChar -> CSize -> IO CUInt
