{-# LANGUAGE CPP          #-}
{-# LANGUAGE MagicHash    #-}

-- | A copy of Data.Bit.Utils from bitvec, but for Word64
module Database.LSMTree.Internal.BitUtils (
    lgWordSize,
    modWordSize,
    divWordSize,
    wordSize,
    wordsToBytes,
    nWords,
    aligned,
    alignUp,
    loMask,
    hiMask,
) where

import           Data.Bits (Bits, complement, finiteBitSize, unsafeShiftL,
                     unsafeShiftR, (.&.))
import           Data.Word (Word64)

-- | The number of bits in a 'Word'.  A handy constant to have around when defining 'Word'-based bulk operations on bit vectors.
wordSize :: Int
wordSize = 64

-- | The base 2 logarithm of 'wordSize'.
lgWordSize :: Int
lgWordSize = 6

wordSizeMask :: Int
wordSizeMask = wordSize - 1

wordSizeMaskC :: Int
wordSizeMaskC = complement wordSizeMask

divWordSize :: Bits a => a -> a
divWordSize x = unsafeShiftR x lgWordSize
{-# INLINE divWordSize #-}

modWordSize :: Int -> Int
modWordSize x = x .&. (wordSize - 1)
{-# INLINE modWordSize #-}

-- number of words needed to store n bits
nWords :: Int -> Int
nWords ns = divWordSize (ns + wordSize - 1)

wordsToBytes :: Int -> Int
wordsToBytes ns = ns `unsafeShiftL` 3

aligned :: Int -> Bool
aligned x = x .&. wordSizeMask == 0

-- round a number of bits up to the nearest multiple of word size
alignUp :: Int -> Int
alignUp x | x == x'   = x'
          | otherwise = x' + wordSize
  where x' = alignDown x

-- round a number of bits down to the nearest multiple of word size
alignDown :: Int -> Int
alignDown x = x .&. wordSizeMaskC

loMask :: Int -> Word64
loMask n = 1 `unsafeShiftL` n - 1
{-# INLINE loMask #-}

hiMask :: Int -> Word64
hiMask n = complement (1 `unsafeShiftL` n - 1)
{-# INLINE hiMask #-}
