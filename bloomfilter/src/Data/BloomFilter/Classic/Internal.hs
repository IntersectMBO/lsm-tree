{-# LANGUAGE CPP           #-}
{-# LANGUAGE MagicHash     #-}
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
import           Control.Monad.Primitive (PrimMonad, PrimState)
import           Control.Monad.ST (ST)
import           Data.Bits
import           Data.Kind (Type)
import           Data.Primitive.ByteArray
import           Data.Primitive.PrimArray
import           Data.Word (Word64)

#if MIN_VERSION_base(4,17,0)
import           GHC.Exts (remWord64#)
#else
import           GHC.Exts (remWord#)
#endif
import           GHC.Word (Word64 (W64#))

import           Data.BloomFilter.Classic.BitArray (BitArray, MBitArray)
import qualified Data.BloomFilter.Classic.BitArray as BitArray
import           Data.BloomFilter.Classic.Calc
import           Data.BloomFilter.Hash

-- | The version of the format used by 'serialise' and 'deserialise'. The
-- format number will change when there is an incompatible change in the
-- library, such that deserialising and using the filter will not work.
-- This can include more than just changes to the serialised format, for
-- example changes to hash functions or how the hash is mapped to bits.
--
-- Note that the format produced does not include this version. Version
-- checking is the responsibility of the user of the library.
--
formatVersion :: Int
formatVersion = 0

-------------------------------------------------------------------------------
-- Mutable Bloom filters
--

type MBloom :: Type -> Type -> Type
-- | A mutable Bloom filter, for use within the 'ST' monad.
data MBloom s a = MBloom {
      mbNumBits   :: {-# UNPACK #-} !Int  -- ^ non-zero
    , mbNumHashes :: {-# UNPACK #-} !Int
    , mbBitArray  :: {-# UNPACK #-} !(MBitArray s)
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
new BloomSize { sizeBits, sizeHashes } = do
    let !mbNumBits = max 1 (min 0x1_0000_0000_0000 sizeBits)
    mbBitArray <- BitArray.new mbNumBits
    pure MBloom {
      mbNumBits,
      mbNumHashes = max 1 sizeHashes,
      mbBitArray
    }

insertHashes :: MBloom s a -> CheapHashes a -> ST s ()
insertHashes MBloom { mbNumBits = m, mbNumHashes = k, mbBitArray = a } !ch =
    go 0
  where
    go !i | i >= k = return ()
    go !i = let idx' :: Word64
                !idx' = evalHashes ch i in
            let idx :: Int
                !idx = fromIntegral (idx' `unsafeRemWord64` fromIntegral m) in
            -- While the idx' can cover the full Word64 range,
            -- after taking the remainder, it now must fit in
            -- and Int because it's less than the filter size.
            BitArray.unsafeSet a idx >> go (i + 1)

-- | Overwrite the filter's bit array. Use 'new' to create a filter of the
-- expected size and then use this function to fill in the bit data.
--
-- The callback is expected to read (exactly) the given number of bytes into
-- the given byte array buffer.
--
-- See also 'formatVersion' for compatibility advice.
--
deserialise :: PrimMonad m
            => MBloom (PrimState m) a
            -> (MutableByteArray (PrimState m) -> Int -> Int -> m ())
            -> m ()
deserialise MBloom {mbBitArray} fill =
    BitArray.deserialise mbBitArray fill


-------------------------------------------------------------------------------
-- Immutable Bloom filters
--

type Bloom :: Type -> Type
-- | An immutable Bloom filter.
data Bloom a = Bloom {
      numBits   :: {-# UNPACK #-} !Int  -- ^ non-zero
    , numHashes :: {-# UNPACK #-} !Int
    , bitArray  :: {-# UNPACK #-} !BitArray
    }
  deriving Eq
type role Bloom nominal

bloomInvariant :: Bloom a -> Bool
bloomInvariant Bloom { numBits, bitArray = BitArray.BitArray pa } =
       numBits > 0
    && numBits <= 2^(48 :: Int)
    && ceilDiv64 numBits == sizeofPrimArray pa
  where
    ceilDiv64 x = unsafeShiftR (x + 63) 6

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
                !idx = fromIntegral (idx' `unsafeRemWord64` fromIntegral numBits) in
            -- While the idx' can cover the full Word64 range,
            -- after taking the remainder, it now must fit in
            -- and Int because it's less than the filter size.
            if BitArray.unsafeIndex bitArray idx
              then go (i + 1)
              else False

-- | Serialise the bloom filter to a 'BloomSize' (which is needed to
-- deserialise) and a 'ByteArray' along with the offset and length containing
-- the filter's bit data.
--
-- See also 'formatVersion' for compatibility advice.
--
serialise :: Bloom a -> (BloomSize, ByteArray, Int, Int)
serialise b@Bloom{bitArray} =
    (size b, ba, off, len)
  where
    (ba, off, len) = BitArray.serialise bitArray


-------------------------------------------------------------------------------
-- Conversions between mutable and immutable Bloom filters
--

-- | Create an immutable Bloom filter from a mutable one.  The mutable
-- filter may be modified afterwards.
freeze :: MBloom s a -> ST s (Bloom a)
freeze MBloom { mbNumBits, mbNumHashes, mbBitArray } = do
    bitArray <- BitArray.freeze mbBitArray
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
    bitArray <- BitArray.unsafeFreeze mbBitArray
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
    mbBitArray <- BitArray.thaw bitArray
    pure MBloom {
      mbNumBits   = numBits,
      mbNumHashes = numHashes,
      mbBitArray
    }


-------------------------------------------------------------------------------
-- Low level utils
--

-- | Like 'rem' but does not check for division by 0.
unsafeRemWord64 :: Word64 -> Word64 -> Word64
#if MIN_VERSION_base(4,17,0)
unsafeRemWord64 (W64# x#) (W64# y#) = W64# (x# `remWord64#` y#)
#else
unsafeRemWord64 (W64# x#) (W64# y#) = W64# (x# `remWord#` y#)
#endif
