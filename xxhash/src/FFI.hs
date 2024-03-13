{-# LANGUAGE CApiFFI          #-}
{-# LANGUAGE MagicHash        #-}
{-# LANGUAGE UnliftedFFITypes #-}
module FFI (
    unsafe_xxh3_64bit_withSeed_ptr,
    unsafe_xxh3_64bit_withSeed_ba,
) where

import           Data.Word (Word64, Word8)
import           Foreign.C.Types (CSize (..))
import           Foreign.Ptr (Ptr)
import           GHC.Exts (ByteArray#)

foreign import capi unsafe "HsXXHash.h XXH3_64bits_withSeed"
    unsafe_xxh3_64bit_withSeed_ptr :: Ptr Word8 -> CSize -> Word64 -> Word64

foreign import capi unsafe "HsXXHash.h hs_XXH3_64bits_withSeed_offset"
    unsafe_xxh3_64bit_withSeed_ba :: ByteArray# -> CSize -> CSize -> Word64 -> Word64
