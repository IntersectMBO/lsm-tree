{-# LANGUAGE CPP       #-}
{-# LANGUAGE MagicHash #-}

module XXH3 (
    -- * One shot
    xxh3_64bit_withSeed_bs,
    xxh3_64bit_withSeed_ba,
    xxh3_64bit_withSeed_w64,
    xxh3_64bit_withSeed_w32,
    -- * Incremental
    XXH3_State,
    xxh3_64bit_createState,
    xxh3_64bit_reset_withSeed,
    xxh3_64bit_digest,
    xxh3_64bit_update_bs,
    xxh3_64bit_update_ba,
    xxh3_64bit_update_w64,
    xxh3_64bit_update_w32,
) where

import           Control.Monad (unless)
import           Control.Monad.ST (ST)
import           Control.Monad.ST.Unsafe (unsafeIOToST)
import           Data.ByteString.Internal (ByteString (..),
                     accursedUnutterablePerformIO)
import           Data.Coerce (coerce)
import qualified Data.Primitive as P
import           Data.Primitive.ByteArray (ByteArray (..))
import           Data.Word (Word32, Word64)
import           Foreign.ForeignPtr
import           GHC.Exts (MutableByteArray#)
import           GHC.ForeignPtr

import           FFI

{-# INLINE withFP #-}
withFP :: ForeignPtr a -> (P.Ptr a -> IO b) -> IO b
withFP = unsafeWithForeignPtr

-------------------------------------------------------------------------------
-- OneShot
-------------------------------------------------------------------------------

-- | Hash 'ByteString'.
xxh3_64bit_withSeed_bs :: ByteString -> Word64 -> Word64
xxh3_64bit_withSeed_bs (BS fptr len) !salt = accursedUnutterablePerformIO $
    withFP fptr $ \ptr ->
    unsafe_xxh3_64bit_withSeed_ptr ptr (fromIntegral len) salt

-- | Hash (part of) 'ByteArray'.
xxh3_64bit_withSeed_ba :: ByteArray -> Int -> Int -> Word64 -> Word64
xxh3_64bit_withSeed_ba (ByteArray ba) !off !len !salt =
    unsafe_xxh3_64bit_withSeed_ba ba (fromIntegral off) (fromIntegral len) salt

-- | Hash 'Word64'.
xxh3_64bit_withSeed_w64 :: Word64 -> Word64 -> Word64
xxh3_64bit_withSeed_w64 !x !salt =
    unsafe_xxh3_64bit_withSeed_u64 x salt

-- | Hash 'Word32'.
xxh3_64bit_withSeed_w32 :: Word32 -> Word64 -> Word64
xxh3_64bit_withSeed_w32 !x !salt =
    unsafe_xxh3_64bit_withSeed_u32 x salt

-------------------------------------------------------------------------------
-- Incremental
-------------------------------------------------------------------------------

-- | Mutable XXH3 state.
data XXH3_State s = XXH3 (MutableByteArray# s)

-- | Create 'XXH3_State'.
xxh3_64bit_createState :: forall s. ST s (XXH3_State s)
xxh3_64bit_createState = do
    -- aligned alloc, otherwise we get segfaults.
    -- see XXH3_createState implementation
    P.MutableByteArray ba <- P.newAlignedPinnedByteArray unsafe_xxh3_sizeof_state 64
    unsafeIOToST (unsafe_xxh3_initState ba)
    pure (XXH3 ba)

-- | Reset 'XXH3_State' with a seed.
xxh3_64bit_reset_withSeed :: XXH3_State s -> Word64 -> ST s ()
xxh3_64bit_reset_withSeed (XXH3 s) seed = do
    unsafeIOToST (unsafe_xxh3_64bit_reset_withSeed s seed)

-- | Return a hash value from a 'XXH3_State'.
--
-- Doesn't mutate given state, so you can update, digest and update again.
xxh3_64bit_digest :: XXH3_State s -> ST s Word64
xxh3_64bit_digest (XXH3 s) =
    unsafeIOToST (unsafe_xxh3_64bit_digest s)

-- | Update 'XXH3_State' with 'ByteString'.
xxh3_64bit_update_bs :: XXH3_State s -> ByteString -> ST s ()
xxh3_64bit_update_bs (XXH3 s) (BS fptr len) = unsafeIOToST $
    withFP fptr $ \ptr ->
    unsafe_xxh3_64bit_update_ptr s ptr (fromIntegral len)

-- | Update 'XXH3_State' with (part of) 'ByteArray'
xxh3_64bit_update_ba :: XXH3_State s -> ByteArray -> Int -> Int -> ST s ()
xxh3_64bit_update_ba (XXH3 s) (ByteArray ba) !off !len = unsafeIOToST $
    unsafe_xxh3_64bit_update_ba s ba (fromIntegral off) (fromIntegral len)

-- | Update 'XXH3_State' with 'Word64'.
xxh3_64bit_update_w64 :: XXH3_State s -> Word64 -> ST s ()
xxh3_64bit_update_w64 (XXH3 s) w64 = unsafeIOToST $
    unsafe_xxh3_64bit_update_u64 s w64

-- | Update 'XXH3_State' with 'Word32'.
xxh3_64bit_update_w32 :: XXH3_State s -> Word32 -> ST s ()
xxh3_64bit_update_w32 (XXH3 s) w32 = unsafeIOToST $
    unsafe_xxh3_64bit_update_u32 s w32
