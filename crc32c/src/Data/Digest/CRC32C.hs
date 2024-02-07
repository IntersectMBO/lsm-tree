module Data.Digest.CRC32C(
    crc32c
  , crc32c_update
  ) where

import           Data.ByteString.Internal    (ByteString (..))
import           Data.Digest.CRC32C.Internal
import           Data.Word
import           Foreign.C.Types
import           Foreign.ForeignPtr.Unsafe
import           Foreign.Ptr

crc32c :: ByteString -> Word32
crc32c (PS p o l) = fromIntegral $ lib_crc32c_value (unsafeForeignPtrToPtr p `plusPtr` o) (fromIntegral l)

crc32c_update :: Word32 -> ByteString -> Word32
crc32c_update hash (PS p o l) = fromIntegral $ lib_crc32c_extend (CUInt hash) (unsafeForeignPtrToPtr p `plusPtr` o) (fromIntegral l)

