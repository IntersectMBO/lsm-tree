{-# LANGUAGE BangPatterns #-}
module Data.BloomFilter.BitVec64 (
  BitVec64 (..),
  unsafeIndex,
  MBitVec64 (..),
  new,
  unsafeWrite,
  unsafeRead,
  freeze,
  unsafeFreeze,
  thaw,
) where

import Data.Bits
import Data.Word (Word64)
import Control.Monad.ST (ST)
import qualified Data.Vector.Primitive as P
import qualified Data.Vector.Primitive.Mutable as MP

-- | Bit vector backed up by an array of Word64
-- 
-- This vector's offset and length are multiples of 64
newtype BitVec64 = BV64 (P.Vector Word64)
  deriving (Eq, Show)

unsafeIndex :: BitVec64 -> Word64 -> Bool
unsafeIndex (BV64 bv) i = testBit (P.unsafeIndex bv (w2i j)) (w2i k)
  where
    !j = unsafeShiftR i 6 -- `div` 64
    !k = i .&. 63         -- `mod` 64

newtype MBitVec64 s = MBV64 (P.MVector s Word64)

new :: Word64 -> ST s (MBitVec64 s)
new s = MBV64 <$> MP.new (w2i (roundUpTo64 s))

unsafeWrite :: MBitVec64 s -> Word64 -> Bool -> ST s ()
unsafeWrite (MBV64 mbv) i x = do
    MP.unsafeModify mbv (\w -> if x then setBit w (w2i k) else clearBit w (w2i k)) (w2i j)
  where
    !j = unsafeShiftR i 6 -- `div` 64
    !k = i .&. 63         -- `mod` 64

unsafeRead :: MBitVec64 s -> Word64 -> ST s Bool
unsafeRead (MBV64 mbv) i = do
    !w <- MP.unsafeRead mbv (w2i j)
    return $! testBit w (w2i k)
  where
    !j = unsafeShiftR i 6 -- `div` 64
    !k = i .&. 63         -- `mod` 64

freeze :: MBitVec64 s -> ST s BitVec64
freeze (MBV64 mbv) = BV64 <$> P.freeze mbv

unsafeFreeze :: MBitVec64 s -> ST s BitVec64
unsafeFreeze (MBV64 mbv) = BV64 <$> P.unsafeFreeze mbv

thaw :: BitVec64 -> ST s (MBitVec64 s)
thaw (BV64 bv) = MBV64 <$> P.thaw bv

-- this may overflow, but so be it (1^64 bits is a lot)
roundUpTo64 :: Word64 -> Word64
roundUpTo64 i = unsafeShiftR (i + 63) 6

w2i :: Word64 -> Int
w2i = fromIntegral
{-# INLINE w2i #-}
