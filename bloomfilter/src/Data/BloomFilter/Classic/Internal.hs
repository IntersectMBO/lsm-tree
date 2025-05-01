{-# OPTIONS_HADDOCK not-home #-}
-- | This module exports 'Bloom'' definition.
module Data.BloomFilter.Classic.Internal (
    -- * Mutable Bloom filters
    MBloom (..),
    new,
    insertHashes,
    readHashes,

    -- * Immutable Bloom filters
    Bloom (..),
    bloomInvariant,

    -- * Conversion
    deserialise,
) where

import           Control.DeepSeq (NFData (..))
import           Control.Monad.Primitive (PrimState)
import           Control.Monad.ST (ST)
import           Data.Bits
import           Data.Kind (Type)
import           Data.Primitive.ByteArray
import qualified Data.Vector.Primitive as VP

import qualified Data.BloomFilter.Classic.BitVec64 as V
import           Data.BloomFilter.Classic.Calc (BloomSize (..))
import           Data.BloomFilter.Hash (CheapHashes, evalHashes)

import           Prelude hiding (read)

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

instance NFData (MBloom s a) where
    rnf !_ = ()

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

readHashes :: forall s a. CheapHashes a -> MBloom s a -> ST s Bool
readHashes !ch MBloom { mbNumBits = m, mbNumHashes = k, mbBitArray = v } =
    go 0
  where
    go :: Int -> ST s Bool
    go !i | i >= k    = return True
          | otherwise = do let !idx' = evalHashes ch i
                           let !idx = idx' `rem` fromIntegral m
                           b <- V.unsafeRead v idx
                           if b
                           then go (i + 1)
                           else return False

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
data Bloom a = Bloom {
      numBits   :: {-# UNPACK #-} !Int  -- ^ non-zero
    , numHashes :: {-# UNPACK #-} !Int
    , bitArray  :: {-# UNPACK #-} !V.BitVec64
    }
  deriving Eq
type role Bloom nominal

bloomInvariant :: Bloom a -> Bool
bloomInvariant Bloom { numBits = s, bitArray = V.BV64 (VP.Vector off len ba) } =
       s > 0
    && s <= 2^(48 :: Int)
    && off >= 0
    && ceilDiv64 s == fromIntegral len
    && (off + len) * 8 <= sizeofByteArray ba
  where
    ceilDiv64 x = unsafeShiftR (x + 63) 6

instance Show (Bloom a) where
    show mb = "Bloom { " ++ show (numBits mb) ++ " bits } "

instance NFData (Bloom a) where
    rnf !_ = ()
