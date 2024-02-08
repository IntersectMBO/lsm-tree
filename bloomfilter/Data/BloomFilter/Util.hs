{-# LANGUAGE BangPatterns, MagicHash, TypeOperators #-}

module Data.BloomFilter.Util
    (
      nextPowerOfTwo
    , ceil64
    ) where

import Data.Bits ((.|.), (.&.), complement, unsafeShiftR)

-- given number.
nextPowerOfTwo :: Int -> Int
{-# INLINE nextPowerOfTwo #-}
nextPowerOfTwo n =
    let a = n - 1
        b = a .|. (a `unsafeShiftR` 1)
        c = b .|. (b `unsafeShiftR` 2)
        d = c .|. (c `unsafeShiftR` 4)
        e = d .|. (d `unsafeShiftR` 8)
        f = e .|. (e `unsafeShiftR` 16)
        g = f .|. (f `unsafeShiftR` 32)  -- in case we're on a 64-bit host
        !h = g + 1
    in h

-- >>> let ceil64ref x = let y = (x `div` 64) * 64 in if x == y then y else y + 64
-- >>> and [ ceil64 i == ceil64ref i | i <- [0..200] ]
-- True
--
ceil64 :: Int -> Int
ceil64 i = (i + 63) .&. complement 0x3f
