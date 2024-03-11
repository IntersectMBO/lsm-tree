{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE MagicHash #-}
{-# LANGUAGE UnboxedTuples #-}
module Data.BloomFilter.BitVec64 (
  BitVec64 (..),
  unsafeIndex,
  prefetchIndex,
  prefetchIndexST,
  MBitVec64 (..),
  new,
  unsafeWrite,
  unsafeRead,
  freeze,
  unsafeFreeze,
  thaw,
  remWord32,
) where

import Data.Bits
import Data.Word (Word64)
import Control.Monad.ST (ST)
import qualified Data.Vector.Primitive as P
import qualified Data.Vector.Primitive.Mutable as MP

import Data.Array.Byte
import GHC.Exts (Int(I#), (+#), realWorld#, remWord32#,
                 prefetchByteArray0#, prefetchByteArray3#, uncheckedIShiftRA#)
import GHC.ST(ST(ST))
import GHC.Word (Word32(W32#))

-- | Bit vector backed up by an array of Word64
-- 
-- This vector's offset and length are multiples of 64
newtype BitVec64 = BV64 (P.Vector Word64)
  deriving (Eq, Show)

unsafeIndex :: BitVec64 -> Int -> Bool
unsafeIndex (BV64 bv) i = unsafeTestBit (P.unsafeIndex bv j) k
  where
    !j = unsafeShiftR i 6 -- `div` 64
    !k = i .&. 63         -- `mod` 64

unsafeTestBit :: Word64 -> Int -> Bool
unsafeTestBit w k = w .&. (1 `unsafeShiftL` k) /= 0

prefetchIndex :: BitVec64 -> Int -> ()
prefetchIndex (BV64 bv) i =
    prefetchIndexPrimByteOff bv j
  where
    !j = unsafeShiftR i 3 -- `div` 8, offset in bytes not Word64s

prefetchIndexPrimByteOff :: P.Vector a -> Int -> ()
prefetchIndexPrimByteOff (P.Vector (I# off#) _ (ByteArray ba#)) (I# i#) =
    case prefetchByteArray3# ba# (off# +# i#) realWorld# of
      _ -> ()

prefetchIndexST :: BitVec64 -> Int -> ST s ()
prefetchIndexST (BV64 (P.Vector (I# off#) _ (ByteArray ba#))) (I# i#) =
    ST (\s# -> case prefetchByteArray3# ba# (off# +# (i# `uncheckedIShiftRA#` 3#)) s# of
                 s' -> (# s', () #))

remWord32 :: Word32 -> Word32 -> Word32
remWord32 (W32# x#) (W32# y#) = W32# (x# `remWord32#` y#)

newtype MBitVec64 s = MBV64 (P.MVector s Word64)

new :: Int -> ST s (MBitVec64 s)
new s = MBV64 <$> MP.new (roundUpTo64 s)

unsafeWrite :: MBitVec64 s -> Int -> Bool -> ST s ()
unsafeWrite (MBV64 mbv) i x = do
    MP.unsafeModify mbv (\w -> if x then setBit w k else clearBit w k) j
  where
    !j = unsafeShiftR i 6 -- `div` 64
    !k = i .&. 63         -- `mod` 64

unsafeRead :: MBitVec64 s -> Int -> ST s Bool
unsafeRead (MBV64 mbv) i = do
    !w <- MP.unsafeRead mbv j
    return $! testBit w k 
  where
    !j = unsafeShiftR i 6 -- `div` 64
    !k = i .&. 63         -- `mod` 64

freeze :: MBitVec64 s -> ST s BitVec64
freeze (MBV64 mbv) = BV64 <$> P.freeze mbv

unsafeFreeze :: MBitVec64 s -> ST s BitVec64
unsafeFreeze (MBV64 mbv) = BV64 <$> P.unsafeFreeze mbv

thaw :: BitVec64 -> ST s (MBitVec64 s)
thaw (BV64 bv) = MBV64 <$> P.thaw bv

roundUpTo64 :: Int -> Int
roundUpTo64 i = unsafeShiftR (i + 63) 6
