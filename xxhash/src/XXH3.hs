{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE MagicHash    #-}
module XXH3 (
    xxh3_64bit_withSeed_bs,
    xxh3_64bit_withSeed_ba,
) where

import           Data.ByteString.Internal (ByteString (..),
                     accursedUnutterablePerformIO)
import           Data.Primitive.ByteArray (ByteArray (..))
import           Data.Word (Word64)
import           GHC.ForeignPtr (unsafeWithForeignPtr)

import           FFI

-- Note: we use unsafe FFI calls, as we expect our use case to be hashing only small data (<1kb, at most 4k).

-- | Hash 'ByteString'.
xxh3_64bit_withSeed_bs :: ByteString -> Word64 -> Word64
xxh3_64bit_withSeed_bs (BS fptr len) !salt = accursedUnutterablePerformIO $
    unsafeWithForeignPtr fptr $ \ptr -> return $! unsafe_xxh3_64bit_withSeed_ptr ptr (fromIntegral len) salt

-- | Hash (part of) 'ByteArray'.
xxh3_64bit_withSeed_ba :: ByteArray -> Int -> Int -> Word64 -> Word64
xxh3_64bit_withSeed_ba (ByteArray ba#) !off !len !salt =
    unsafe_xxh3_64bit_withSeed_ba ba# (fromIntegral off) (fromIntegral len) salt
