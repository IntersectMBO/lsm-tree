{-# LANGUAGE CPP           #-}
{-# LANGUAGE MagicHash     #-}
{-# LANGUAGE UnboxedTuples #-}
-- | Minimal bit array implementation.
module Data.BloomFilter.Classic.BitArray (
    BitArray (..),
    unsafeIndex,
    prefetchIndex,
    MBitArray (..),
    new,
    unsafeSet,
    unsafeRead,
    freeze,
    unsafeFreeze,
    thaw,
    serialise,
    deserialise,
) where

import           Control.Exception (assert)
import           Control.Monad.Primitive (PrimMonad, PrimState)
import           Control.Monad.ST (ST)
import           Data.Bits
import           Data.Primitive.ByteArray
import           Data.Primitive.PrimArray
import           Data.Word (Word64, Word8)

import           GHC.Exts (Int (I#), prefetchByteArray0#)
import           GHC.ST (ST (ST))

-- | Bit vector backed up by an array of Word64
--
-- This vector's offset and length are multiples of 64
newtype BitArray = BitArray (PrimArray Word64)
  deriving stock (Eq, Show)

{-# INLINE unsafeIndex #-}
unsafeIndex :: BitArray -> Int -> Bool
unsafeIndex (BitArray arr) !i =
    assert (j >= 0 && j < sizeofPrimArray arr) $
    unsafeTestBit (indexPrimArray arr j) k
  where
    !j = unsafeShiftR i 6 -- `div` 64, bit index to Word64 index.
    !k = i .&. 63         -- `mod` 64, bit within Word64

{-# INLINE prefetchIndex #-}
prefetchIndex :: BitArray -> Int -> ST s ()
prefetchIndex (BitArray (PrimArray ba#)) !i =
    let !(I# bi#) = i `unsafeShiftR` 3 in
    ST (\s -> case prefetchByteArray0# ba# bi# s of
                s' -> (# s', () #))
    -- We only need to shiftR 3 here, not 6, because we're going from a bit
    -- offset to a byte offset for prefetch. Whereas in unsafeIndex, we go from
    -- a bit offset to a Word64 offset, so an extra shiftR 3, for 6 total.

newtype MBitArray s = MBitArray (MutablePrimArray s Word64)

-- | Will create an explicitly pinned byte array.
-- This is done because pinned byte arrays allow for more efficient
-- serialisation, but the definition of 'isByteArrayPinned' changed in GHC 9.6,
-- see <https://gitlab.haskell.org/ghc/ghc/-/issues/22255>.
--
-- TODO: remove this workaround once a solution exists, e.g. a new primop that
-- allows checking for implicit pinning.
new :: Int -> ST s (MBitArray s)
new s = do
    mba@(MutableByteArray mba#) <- newPinnedByteArray numBytes
    setByteArray mba 0 numBytes (0 :: Word8)
    return (MBitArray (MutablePrimArray mba#))
  where
    !numWords = roundUpTo64 s
    !numBytes = unsafeShiftL numWords 3 -- * 8

    -- this may overflow, but so be it (2^64 bits is a lot)
    roundUpTo64 :: Int -> Int
    roundUpTo64 i = unsafeShiftR (i + 63) 6 -- `div` 64, rounded up

serialise :: BitArray -> (ByteArray, Int, Int)
serialise bitArray =
    let ba = asByteArray bitArray
     in (ba, 0, sizeofByteArray ba)
  where
    asByteArray (BitArray (PrimArray ba#)) = ByteArray ba#

{-# INLINE deserialise #-}
-- | Do an inplace overwrite of the byte array representing the bit block.
deserialise :: PrimMonad m
            => MBitArray (PrimState m)
            -> (MutableByteArray (PrimState m) -> Int -> Int -> m ())
            -> m ()
deserialise bitArray fill = do
    let mba = asMutableByteArray bitArray
    len <- getSizeofMutableByteArray mba
    fill mba 0 len
  where
    asMutableByteArray (MBitArray (MutablePrimArray mba#)) =
      MutableByteArray mba#

unsafeSet :: MBitArray s -> Int -> ST s ()
unsafeSet (MBitArray arr) i = do
#ifdef NO_IGNORE_ASSERTS
    sz <- getSizeofMutablePrimArray arr
    assert (j >= 0 && j < sz) $ return ()
#endif
    w <- readPrimArray arr j
    writePrimArray arr j (unsafeSetBit w k)
  where
    !j = unsafeShiftR i 6 -- `div` 64
    !k = i .&. 63         -- `mod` 64

unsafeRead :: MBitArray s -> Int -> ST s Bool
unsafeRead (MBitArray arr) i = do
#ifdef NO_IGNORE_ASSERTS
    sz <- getSizeofMutablePrimArray arr
    assert (j >= 0 && j < sz) $ return ()
#endif
    w <- readPrimArray arr j
    return $! unsafeTestBit w k
  where
    !j = unsafeShiftR i 6 -- `div` 64
    !k = i .&. 63         -- `mod` 64

freeze :: MBitArray s -> ST s BitArray
freeze (MBitArray arr) = do
    len <- getSizeofMutablePrimArray arr
    BitArray <$> freezePrimArray arr 0 len

unsafeFreeze :: MBitArray s -> ST s BitArray
unsafeFreeze (MBitArray arr) =
    BitArray <$> unsafeFreezePrimArray arr

thaw :: BitArray -> ST s (MBitArray s)
thaw (BitArray arr) =
    MBitArray <$> thawPrimArray arr 0 (sizeofPrimArray arr)

{-# INLINE unsafeTestBit #-}
-- like testBit but using unsafeShiftL instead of shiftL
unsafeTestBit :: Word64 -> Int -> Bool
unsafeTestBit w k = w .&. (1 `unsafeShiftL` k) /= 0

{-# INLINE unsafeSetBit #-}
-- like setBit but using unsafeShiftL instead of shiftL
unsafeSetBit :: Word64 -> Int -> Word64
unsafeSetBit w k = w .|. (1 `unsafeShiftL` k)
