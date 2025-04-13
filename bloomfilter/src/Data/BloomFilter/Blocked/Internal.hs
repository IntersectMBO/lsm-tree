{-# LANGUAGE CPP           #-}
{-# LANGUAGE MagicHash     #-}
{-# LANGUAGE UnboxedTuples #-}
{-# OPTIONS_HADDOCK not-home #-}
-- | This module defines the 'Bloom' and 'MBloom' types and all the functions
-- that need direct knowledge of and access to the representation. This forms
-- the trusted base.
module Data.BloomFilter.Blocked.Internal (
    -- * Mutable Bloom filters
    MBloom,
    new,
    insertHashes,
    prepareHash,
    insertHash,

    -- * Immutable Bloom filters
    Bloom,
    bloomInvariant,
    size,
    elemHashes,

    -- * Conversion
    freeze,
    unsafeFreeze,
    thaw,

    -- * (De)Serialisation
    formatVersion,
    serialise,
    deserialise,
  ) where

import           Control.Exception (assert)
import           Control.DeepSeq (NFData (..))
import           Control.Monad.Primitive (PrimMonad, PrimState)
import           Control.Monad.ST (ST)
import           Data.Bits
import           Data.Kind (Type)
import           Data.Primitive.ByteArray
import           Data.Primitive.PrimArray
import           Data.Word (Word32, Word64)

import           Data.BloomFilter.Blocked.BitArray (BitArray, MBitArray,
                     bitsToBlocks, blocksToBits)
import qualified Data.BloomFilter.Blocked.BitArray as BitArray
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
formatVersion = 1000

-------------------------------------------------------------------------------
-- Mutable Bloom filters
--

type MBloom :: Type -> Type -> Type
-- | A mutable Bloom filter, for use within the 'ST' monad.
data MBloom s a = MBloom {
      mbNumBlocks :: {-# UNPACK #-} !Int  -- ^ non-zero
    , mbNumHashes :: {-# UNPACK #-} !Int
    , mbBitArray  :: {-# UNPACK #-} !(MBitArray s)
    }
type role MBloom nominal nominal

instance Show (MBloom s a) where
    show mb = "MBloom { " ++ show numBits ++ " bits } "
      where
        numBits = blocksToBits (mbNumBlocks mb)

instance NFData (MBloom s a) where
    rnf !_ = ()

-- | Create a new mutable Bloom filter.
--
-- The maximum size is $2^41$ bits (256 Gbytes). Tell us if you need bigger
-- bloom filters.
--
new :: BloomSize -> ST s (MBloom s a)
new BloomSize { sizeBits, sizeHashes } = do
    let numBlocks :: Int
        numBlocks = max 1 (bitsToBlocks sizeBits)
                .&. 0xffff_ffff
    mbBitArray <- BitArray.new numBlocks
    pure MBloom {
      mbNumBlocks = numBlocks,
      mbNumHashes = max 1 sizeHashes,
      mbBitArray
    }

{-# NOINLINE insertHashes #-}
insertHashes :: forall s a. MBloom s a -> Hash -> ST s ()
insertHashes MBloom { mbNumBlocks, mbNumHashes, mbBitArray } !h0 =
    go h2 mbNumHashes
  where
    (!h1, !h2) = splitHash h0

    blockIx :: Word32
    !blockIx = assert (mbNumBlocks > 0) $
               reduceRange32 h1 (fromIntegral mbNumBlocks)

    go :: Word32 -> Int -> ST s ()
    go !_ 0  = return ()
    go !h !i = do
      let blockBitIx :: Int
          blockBitIx = topNBits h 9
      assert (blockIx >= 0 && fromIntegral blockIx < mbNumBlocks) $
        BitArray.unsafeSet mbBitArray blockIx blockBitIx
      go (nextCheapHash32 h) (i-1)

prepareHash :: forall s a. MBloom s a -> Hash -> ST s Word32
prepareHash MBloom { mbNumBlocks, mbBitArray } !h0 = do
    BitArray.prefetchSet mbBitArray blockIx
    return blockIx
  where
    (!h1, _) = splitHash h0

    blockIx :: Word32
    !blockIx = assert (mbNumBlocks > 0) $
               reduceRange32 h1 (fromIntegral mbNumBlocks)

insertHash :: forall s a. MBloom s a -> Hash -> Word32 -> ST s ()
insertHash MBloom { mbNumHashes, mbBitArray } !h0 !blockIx =
    go h2 mbNumHashes
  where
    (_, !h2) = splitHash h0

    go :: Word32 -> Int -> ST s ()
    go !_ 0  = return ()
    go !h !i = do
      let blockBitIx :: Int
          blockBitIx = topNBits h 9
      BitArray.unsafeSet mbBitArray blockIx blockBitIx
      go (nextCheapHash32 h) (i-1)

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
      numBlocks :: {-# UNPACK #-} !Int  -- ^ non-zero
    , numHashes :: {-# UNPACK #-} !Int
    , bitArray  :: {-# UNPACK #-} !BitArray
    }
  deriving Eq
type role Bloom nominal

bloomInvariant :: Bloom a -> Bool
bloomInvariant Bloom { numBlocks, bitArray = BitArray.BitArray pa } =
    fromIntegral numBlocks * 8 == sizeofPrimArray pa

instance Show (Bloom a) where
    show mb = "Bloom { " ++ show numBits ++ " bits } "
      where
        numBits = blocksToBits (fromIntegral (numBlocks mb))

instance NFData (Bloom a) where
    rnf !_ = ()

-- | Return the size of the Bloom filter.
size :: Bloom a -> BloomSize
size Bloom { numBlocks, numHashes } =
    BloomSize {
      sizeBits   = blocksToBits numBlocks,
      sizeHashes = numHashes
    }

-- | Query an immutable Bloom filter for membership using already constructed
-- 'Hash' value.
elemHashes :: Bloom a -> Hash -> Bool
elemHashes Bloom { numBlocks, numHashes, bitArray } !h0 =
    go h2 numHashes
  where
    (!h1, !h2) = splitHash h0

    blockIx :: Word32
    !blockIx = assert (numBlocks > 0) $
               reduceRange32 h1 (fromIntegral numBlocks)

    go :: Word32 -> Int -> Bool
    go !_ 0 = True
    go !h !i
      | let blockBitIx :: Int
            blockBitIx = topNBits h 9
      , assert (blockIx >= 0) $
        assert (fromIntegral blockIx < numBlocks) $
        BitArray.unsafeIndex bitArray blockIx blockBitIx
      = go (nextCheapHash32 h) (i-1)

      | otherwise = False

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
freeze MBloom { mbNumBlocks, mbNumHashes, mbBitArray } = do
    bitArray <- BitArray.freeze mbBitArray
    let !bf = Bloom {
                numBlocks = mbNumBlocks,
                numHashes = mbNumHashes,
                bitArray
              }
    assert (bloomInvariant bf) $ pure bf

-- | Create an immutable Bloom filter from a mutable one without copying. The
-- mutable filter /must not/ be modified afterwards. For a safer creation
-- interface, use 'freeze' or 'create'.
unsafeFreeze :: MBloom s a -> ST s (Bloom a)
unsafeFreeze MBloom { mbNumBlocks, mbNumHashes, mbBitArray } = do
    bitArray <- BitArray.unsafeFreeze mbBitArray
    let !bf = Bloom {
                numBlocks = mbNumBlocks,
                numHashes = mbNumHashes,
                bitArray
              }
    assert (bloomInvariant bf) $ pure bf

-- | Copy an immutable Bloom filter to create a mutable one.  There is
-- no non-copying equivalent.
thaw :: Bloom a -> ST s (MBloom s a)
thaw Bloom { numBlocks, numHashes, bitArray } = do
    mbBitArray <- BitArray.thaw bitArray
    pure MBloom {
      mbNumBlocks = numBlocks,
      mbNumHashes = numHashes,
      mbBitArray
    }


-------------------------------------------------------------------------------
-- Low level utils
--

{-# INLINE reduceRange32 #-}
-- | Given a word sampled uniformly from the full 'Word32' range, such as a
-- hash, reduce it fairly to a value in the range @[0,n)@.
--
-- See <https://lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/>
--
reduceRange32 :: Word32 -- ^ Sample from 0..2^32-1
              -> Word32 -- ^ upper bound of range [0,n)
              -> Word32 -- ^ result within range
reduceRange32 x n =
    assert (n > 0) $
    let w :: Word64
        w = fromIntegral x * fromIntegral n
     in fromIntegral (w `shiftR` 32)

{-# INLINE splitHash #-}
-- | Split a 64-bit hash into a pair of 32-bit hashes
splitHash :: Hash -> (Word32, Word32)
splitHash w =
    (fromIntegral (w `shiftR` 32), fromIntegral w)

{-# INLINE nextCheapHash32 #-}
-- | Compute a short series of hash values, by multiplying by the golden ratio
-- (as a fraction of a 32bit value). Take the top few bits only each time.
nextCheapHash32 :: Word32 -> Word32
nextCheapHash32 h = h * 0x9e3779b9

{-# INLINE topNBits #-}
topNBits :: Word32 -> Int -> Int
topNBits w n = fromIntegral w `shiftR` (32-n)
