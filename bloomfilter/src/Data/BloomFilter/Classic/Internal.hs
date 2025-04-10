{-# OPTIONS_HADDOCK not-home #-}
-- | This module defines the 'Bloom' and 'MBloom' types and all the functions
-- that need direct knowledge of and access to the representation. This forms
-- the trusted base.
module Data.BloomFilter.Classic.Internal (
    -- * Mutable Bloom filters
    MBloom,
    new,
    insertHashes,

    -- * Immutable Bloom filters
    Bloom,
    bloomInvariant,
    size,
    elemHashes,

    -- * Conversion
    serialise,
    deserialise,
    freeze,
    unsafeFreeze,
    thaw,
) where

import           Control.Exception (assert)
import           Control.DeepSeq (NFData (..))
import           Control.Monad.Primitive (PrimState)
import           Control.Monad.ST (ST)
import           Data.Bits
import qualified Data.BloomFilter.Classic.BitVec64 as V
import           Data.Kind (Type)
import           Data.Primitive.ByteArray (ByteArray, MutableByteArray,
                     sizeofByteArray)
import qualified Data.Vector.Primitive as P
import           Data.Word (Word64)

import           Data.BloomFilter.Classic.Calc
import           Data.BloomFilter.Hash

-------------------------------------------------------------------------------
-- Mutable Bloom filters
--

type MBloom :: Type -> Type -> Type
-- | A mutable Bloom filter, for use within the 'ST' monad.
data MBloom s a = MBloom {
      mbNumBits   :: {-# UNPACK #-} !Int  -- ^ non-zero
    , mbNumHashes :: {-# UNPACK #-} !Int
    , mbBitArray  :: {-# UNPACK #-} !(V.MBitVec64 s)
    }
type role MBloom nominal nominal

instance Show (MBloom s a) where
    show mb = "MBloom { " ++ show (mbNumBits mb) ++ " bits } "

-- | Create a new mutable Bloom filter.
--
-- The size is ceiled at $2^48$. Tell us if you need bigger bloom filters.
--
new :: BloomSize -> ST s (MBloom s a)
new BloomSize { sizeBits, sizeHashes = mbNumHashes } = do
    let !mbNumBits = max 1 (min 0x1_0000_0000_0000 sizeBits)
    mbBitArray <- V.new (fromIntegral mbNumBits)
    pure MBloom {
      mbNumBits,
      mbNumHashes,
      mbBitArray
    }

insertHashes :: MBloom s a -> CheapHashes a -> ST s ()
insertHashes MBloom { mbNumBits = m, mbNumHashes = k, mbBitArray = v } !h =
    go 0
  where
    go !i | i >= k = return ()
          | otherwise = let !idx = evalHashes h i `rem` fromIntegral m
                        in V.unsafeWrite v idx True >> go (i + 1)

-- | Modify the filter's bit array. The callback is expected to read (exactly)
-- the given number of bytes into the given byte array buffer.
--
deserialise :: MBloom (PrimState m) a
            -> (MutableByteArray (PrimState m) -> Int -> Int -> m ())
            -> m ()
deserialise MBloom {mbBitArray} fill =
    V.deserialise mbBitArray fill


-------------------------------------------------------------------------------
-- Immutable Bloom filters
--

type Bloom :: Type -> Type
-- | An immutable Bloom filter.
data Bloom a = Bloom {
      numBits   :: {-# UNPACK #-} !Int  -- ^ non-zero
    , numHashes :: {-# UNPACK #-} !Int
    , bitArray  :: {-# UNPACK #-} !V.BitVec64
    }
type role Bloom nominal

bloomInvariant :: Bloom a -> Bool
bloomInvariant Bloom { numBits = s, bitArray = V.BV64 (P.Vector off len ba) } =
       s > 0
    && s <= 2^(48 :: Int)
    && off >= 0
    && ceilDiv64 s == fromIntegral len
    && (off + len) * 8 <= sizeofByteArray ba
  where
    ceilDiv64 x = unsafeShiftR (x + 63) 6

instance Eq (Bloom a) where
    -- We support arbitrary sized bitvectors,
    -- therefore an equality is a bit involved:
    -- we need to be careful when comparing the last bits of bitArray.
    (==) Bloom { numBits = n,  numHashes = k,  bitArray = V.BV64 v  }
         Bloom { numBits = n', numHashes = k', bitArray = V.BV64 v' } =
        k == k' &&
        n == n' &&
        P.take w v == P.take w v' && -- compare full words
        if l == 0 then True else unsafeShiftL x s == unsafeShiftL x' s -- compare last words
      where
        !w = fromIntegral (unsafeShiftR n 6) :: Int  -- n `div` 64
        !l = fromIntegral (n .&. 63) :: Int          -- n `mod` 64
        !s = 64 - l

        -- last words
        x = P.unsafeIndex v w
        x' = P.unsafeIndex v' w

instance Show (Bloom a) where
    show mb = "Bloom { " ++ show (numBits mb) ++ " bits } "

instance NFData (Bloom a) where
    rnf !_ = ()

-- | Return the size of the Bloom filter.
size :: Bloom a -> BloomSize
size Bloom { numBits, numHashes } =
    BloomSize {
      sizeBits   = numBits,
      sizeHashes = numHashes
    }

-- | Query an immutable Bloom filter for membership using already constructed 'Hashes' value.
elemHashes :: CheapHashes a -> Bloom a -> Bool
elemHashes !ch Bloom { numBits, numHashes, bitArray } =
    go 0
  where
    go :: Int -> Bool
    go !i | i >= numHashes
          = True
    go !i = let idx' :: Word64
                !idx' = evalHashes ch i in
            let idx :: Int
                !idx = fromIntegral (idx' `V.unsafeRemWord64` fromIntegral numBits) in
            -- While the idx' can cover the full Word64 range,
            -- after taking the remainder, it now must fit in
            -- and Int because it's less than the filter size.
            if V.unsafeIndex bitArray idx
              then go (i + 1)
              else False

serialise :: Bloom a -> (BloomSize, ByteArray, Int, Int)
serialise b@Bloom{bitArray} =
    (size b, ba, off, len)
  where
    (ba, off, len) = V.serialise bitArray


-------------------------------------------------------------------------------
-- Conversions between mutable and immutable Bloom filters
--

-- | Create an immutable Bloom filter from a mutable one.  The mutable
-- filter may be modified afterwards.
freeze :: MBloom s a -> ST s (Bloom a)
freeze MBloom { mbNumBits, mbNumHashes, mbBitArray } = do
    bitArray <- V.freeze mbBitArray
    let !bf = Bloom {
                numBits   = mbNumBits,
                numHashes = mbNumHashes,
                bitArray
              }
    assert (bloomInvariant bf) $ pure bf

-- | Create an immutable Bloom filter from a mutable one without copying. The
-- mutable filter /must not/ be modified afterwards. For a safer creation
-- interface, use 'freeze' or 'create'.
unsafeFreeze :: MBloom s a -> ST s (Bloom a)
unsafeFreeze MBloom { mbNumBits, mbNumHashes, mbBitArray } = do
    bitArray <- V.unsafeFreeze mbBitArray
    let !bf = Bloom {
                numBits   = mbNumBits,
                numHashes = mbNumHashes,
                bitArray
              }
    assert (bloomInvariant bf) $ pure bf

-- | Copy an immutable Bloom filter to create a mutable one.  There is
-- no non-copying equivalent.
thaw :: Bloom a -> ST s (MBloom s a)
thaw Bloom { numBits, numHashes, bitArray } = do
    mbBitArray <- V.thaw bitArray
    pure MBloom {
      mbNumBits   = numBits,
      mbNumHashes = numHashes,
      mbBitArray
    }
