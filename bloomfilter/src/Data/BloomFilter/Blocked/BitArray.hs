{-# LANGUAGE CPP           #-}
{-# LANGUAGE MagicHash     #-}
{-# LANGUAGE UnboxedTuples #-}
-- | Blocked bit array implementation. This uses blocks of 64 bytes, aligned
-- to 64byte boundaries to match typical cache line sizes. This means that
-- multiple accesses to the same block only require a single cache line load
-- or store.
module Data.BloomFilter.Blocked.BitArray (
    NumBlocks (..),
    bitsToBlocks,
    blocksToBits,
    BlockIx (..),
    BitIx (..),
    BitArray (..),
    unsafeIndex,
    prefetchIndex,
    MBitArray (..),
    new,
    unsafeSet,
    prefetchSet,
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

import           GHC.Exts (Int (I#), prefetchByteArray0#,
                     prefetchMutableByteArray0#)
import           GHC.ST (ST (ST))

-- | An array of blocks of bits.
--
-- Each block is 512 bits (64 bytes large), corresponding to a cache line on
-- most current architectures.
--
-- It is represented by an array of 'Word64'. This array is aligned to 64 bytes
-- so that multiple accesses within a single block will use only one cache line.
--
newtype BitArray = BitArray (PrimArray Word64)
  deriving stock (Eq, Show)

-- | Blocks are 512 bits, 64 bytes.
newtype NumBlocks = NumBlocks Int
  deriving stock Eq

-- | The number of 512-bit blocks for the given number of bits. This rounds
-- up to the nearest multiple of 512.
bitsToBlocks :: Int -> NumBlocks
bitsToBlocks n = NumBlocks ((n+511) `div` 512)  -- rounded up

blocksToBits :: NumBlocks -> Int
blocksToBits (NumBlocks n) = n * 512

newtype BlockIx = BlockIx Word
newtype BitIx   = BitIx   Int

{-# INLINE unsafeIndex #-}
unsafeIndex :: BitArray -> BlockIx -> BitIx -> Bool
unsafeIndex (BitArray arr) blockIx blockBitIx =
    assert (wordIx >= 0 && wordIx < sizeofPrimArray arr) $
    indexPrimArray arr wordIx `unsafeTestBit` wordBitIx
  where
    (wordIx, wordBitIx) = wordAndBitIndex blockIx blockBitIx

{-# INLINE prefetchIndex #-}
prefetchIndex :: BitArray -> BlockIx -> ST s ()
prefetchIndex (BitArray (PrimArray ba#)) (BlockIx blockIx) =
    -- For reading, we want to prefetch such that we do least disturbance of
    -- the caches. We will typically not keep this cache line longer than one
    -- use of elemHashes which does several memory reads of the same cache line.
    let !i@(I# i#) = fromIntegral blockIx `shiftL` 6 in
    -- blockIx * 64 to go from block index to the byte offset of the beginning
    -- of the block. This offset is in bytes, not words.

    assert (i >= 0 && i < sizeofByteArray (ByteArray ba#) - 63) $

    -- In prefetchByteArray0, the 0 refers to a "non temporal" load, which is
    -- a hint that the value will be used soon, and then not used again (soon).
    -- So the caches can evict the value as soon as they like.
    ST (\s -> case prefetchByteArray0# ba# i# s of
                s' -> (# s', () #))

newtype MBitArray s = MBitArray (MutablePrimArray s Word64)

-- | We create an explicitly pinned byte array, aligned to 64 bytes.
--
new :: NumBlocks -> ST s (MBitArray s)
new (NumBlocks numBlocks) = do
    mba@(MutableByteArray mba#) <- newAlignedPinnedByteArray numBytes 64
    setByteArray mba 0 numBytes (0 :: Word8)
    return (MBitArray (MutablePrimArray mba#))
  where
    !numBytes = numBlocks * 64

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

unsafeSet :: MBitArray s -> BlockIx -> BitIx -> ST s ()
unsafeSet (MBitArray arr) blockIx blockBitIx = do
#ifdef NO_IGNORE_ASSERTS
    sz <- getSizeofMutablePrimArray arr
    assert (wordIx >= 0 && wordIx < sz) $ return ()
#endif
    w <- readPrimArray arr wordIx
    writePrimArray arr wordIx (unsafeSetBit w wordBitIx)
  where
    (wordIx, wordBitIx) = wordAndBitIndex blockIx blockBitIx

{-# INLINE prefetchSet #-}
prefetchSet :: MBitArray s -> BlockIx -> ST s ()
prefetchSet (MBitArray (MutablePrimArray mba#)) (BlockIx blockIx) = do
    -- For setting, we will do several writes to the same cache line, but all
    -- immediately after each other, after which we will not need the value in
    -- the cache again (for a long time). So as with prefetchIndex we want to
    -- disturbe the caches the least, and so we use prefetchMutableByteArray0.
    let !(I# i#) = fromIntegral blockIx `shiftL` 6
    -- blockIx * 64 to go from block index to the byte offset of the beginning
    -- of the block. This offset is in bytes, not words.

#ifdef NO_IGNORE_ASSERTS
    sz <- getSizeofMutableByteArray (MutableByteArray mba#)
    assert (let i = I# i# in i >= 0 && i < sz-63) $ return ()
#endif

    -- In prefetchMutableByteArray0, the 0 refers to a "non temporal" load,
    -- which is a hint that the value will be used soon, and then not used
    -- again (soon). So the caches can evict the value as soon as they like.
    ST (\s -> case prefetchMutableByteArray0# mba# i# s of
                s' -> (# s', () #))

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

{-# INLINE wordAndBitIndex #-}
-- | Given the index of the 512 bit block, and the index of the bit within the
-- block, compute the index of the word in the array, and index of the bit
-- within the word.
--
wordAndBitIndex :: BlockIx -> BitIx -> (Int, Int)
wordAndBitIndex (BlockIx blockIx) (BitIx blockBitIx) =
    assert (blockBitIx < 512) $
    (wordIx, wordBitIx)
  where
    -- Select the Word64 in the underlying array based on the block index
    -- and the bit index.
    -- * There are 8 Word64s in each 64byte block.
    -- * Use 3 bits (bits 6..8) to select the Word64 within the block
    wordIx    = fromIntegral blockIx `shiftL` 3 -- * 8
              + (blockBitIx `shiftR` 6) .&. 7 -- `div` 64, `mod` 8

    -- Bits 0..5 of blockBitIx select the bit within Word64
    wordBitIx = blockBitIx .&. 63 -- `mod` 64

{-# INLINE unsafeTestBit #-}
-- like testBit but using unsafeShiftL instead of shiftL
unsafeTestBit :: Word64 -> Int -> Bool
unsafeTestBit w k = w .&. (1 `unsafeShiftL` k) /= 0

{-# INLINE unsafeSetBit #-}
-- like setBit but using unsafeShiftL instead of shiftL
unsafeSetBit :: Word64 -> Int -> Word64
unsafeSetBit w k = w .|. (1 `unsafeShiftL` k)
