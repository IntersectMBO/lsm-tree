{-# LANGUAGE CPP           #-}
{-# LANGUAGE MagicHash     #-}
{-# LANGUAGE UnboxedTuples #-}
-- | Minimal bit vector implementation.
module Data.BloomFilter.Classic.BitVec64 (
    BitVec64 (..),
    unsafeIndex,
    prefetchIndex,
    MBitVec64 (..),
    new,
    unsafeWrite,
    unsafeRead,
    freeze,
    unsafeFreeze,
    thaw,
    unsafeRemWord64,
    serialise,
    deserialise,
) where

import           Control.Monad.ST (ST)
import           Data.Bits
import           Data.Primitive.ByteArray (ByteArray (ByteArray),
                     MutableByteArray, newPinnedByteArray, setByteArray)
import qualified Data.Vector.Primitive as VP
import qualified Data.Vector.Primitive.Mutable as VPM
import           Data.Word (Word64, Word8)

import           GHC.Exts (Int (I#), prefetchByteArray0#, uncheckedIShiftRL#,
                     (+#))
import qualified GHC.Exts
import           GHC.ST (ST (ST))
import           GHC.Word (Word64 (W64#))


-- | Bit vector backed up by an array of Word64
--
-- This vector's offset and length are multiples of 64
newtype BitVec64 = BV64 (VP.Vector Word64)
  deriving (Eq, Show)

{-# INLINE unsafeIndex #-}
unsafeIndex :: BitVec64 -> Int -> Bool
unsafeIndex (BV64 bv) i =
    unsafeTestBit (VP.unsafeIndex bv j) k
  where
    !j = unsafeShiftR i 6 -- `div` 64, bit index to Word64 index.
    !k = i .&. 63         -- `mod` 64, bit within Word64

{-# INLINE unsafeTestBit #-}
-- like testBit but using unsafeShiftL instead of shiftL
unsafeTestBit :: Word64 -> Int -> Bool
unsafeTestBit w k = w .&. (1 `unsafeShiftL` k) /= 0

{-# INLINE prefetchIndex #-}
prefetchIndex :: BitVec64 -> Int -> ST s ()
prefetchIndex (BV64 (VP.Vector (I# off#) _ (ByteArray ba#))) (I# i#) =
    ST (\s -> case prefetchByteArray0# ba# (off# +# uncheckedIShiftRL# i# 3#) s of
                s' -> (# s', () #))
    -- We only need to shiftR 3 here, not 6, because we're going from a bit
    -- offset to a byte offset for prefetch. Whereas in unsafeIndex, we go from
    -- a bit offset to a Word64 offset, so an extra shiftR 3, for 6 total.

newtype MBitVec64 s = MBV64 (VP.MVector s Word64)

-- | Will create an explicitly pinned byte array if it is larger than 1 kB.
-- This is done because pinned byte arrays allow for more efficient
-- serialisation, but the definition of 'isByteArrayPinned' changed in GHC 9.6,
-- see <https://gitlab.haskell.org/ghc/ghc/-/issues/22255>.
--
-- TODO: remove this workaround once a solution exists, e.g. a new primop that
-- allows checking for implicit pinning.
new :: Word64 -> ST s (MBitVec64 s)
new s = do
    mba <- newPinnedByteArray numBytes
    setByteArray mba 0 numBytes (0 :: Word8)
    return (MBV64 (VP.MVector 0 numWords mba))
  where
    !numWords = w2i (roundUpTo64 s)
    !numBytes = unsafeShiftL numWords 3 -- * 8

serialise :: BitVec64 -> (ByteArray, Int, Int)
serialise = asByteArray
  where
    asByteArray (BV64 (VP.Vector off len ba)) =
      (ba, off * 8, len * 8)

-- | Do an inplace overwrite of the byte array representing the bit block.
deserialise :: MBitVec64 s
            -> (MutableByteArray s -> Int -> Int -> m ())
            -> m ()
deserialise bitArray fill =
    let (ba, off, len) = asMutableByteArray bitArray
     in fill ba off len
  where
    asMutableByteArray (MBV64 (VP.MVector off len mba)) =
      (mba, off * 8, len * 8)

unsafeWrite :: MBitVec64 s -> Word64 -> Bool -> ST s ()
unsafeWrite (MBV64 mbv) i x = do
    VPM.unsafeModify mbv (\w -> if x then setBit w (w2i k) else clearBit w (w2i k)) (w2i j)
  where
    !j = unsafeShiftR i 6 -- `div` 64
    !k = i .&. 63         -- `mod` 64

unsafeRead :: MBitVec64 s -> Word64 -> ST s Bool
unsafeRead (MBV64 mbv) i = do
    !w <- VPM.unsafeRead mbv (w2i j)
    return $! testBit w (w2i k)
  where
    !j = unsafeShiftR i 6 -- `div` 64
    !k = i .&. 63         -- `mod` 64

freeze :: MBitVec64 s -> ST s BitVec64
freeze (MBV64 mbv) = BV64 <$> VP.freeze mbv

unsafeFreeze :: MBitVec64 s -> ST s BitVec64
unsafeFreeze (MBV64 mbv) = BV64 <$> VP.unsafeFreeze mbv

thaw :: BitVec64 -> ST s (MBitVec64 s)
thaw (BV64 bv) = MBV64 <$> VP.thaw bv

-- this may overflow, but so be it (1^64 bits is a lot)
roundUpTo64 :: Word64 -> Word64
roundUpTo64 i = unsafeShiftR (i + 63) 6

-- | Like 'rem' but does not check for division by 0.
unsafeRemWord64 :: Word64 -> Word64 -> Word64
#if MIN_VERSION_base(4,17,0)
unsafeRemWord64 (W64# x#) (W64# y#) = W64# (x# `GHC.Exts.remWord64#` y#)
#else
unsafeRemWord64 (W64# x#) (W64# y#) = W64# (x# `GHC.Exts.remWord#` y#)
#endif

w2i :: Word64 -> Int
w2i = fromIntegral
{-# INLINE w2i #-}
