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

    -- * Immutable Bloom filters
    Bloom,
    bloomInvariant,
    size,

    -- * Hash-based operations
    Hashes,
    hashes,
    insertHashes,
    prefetchInsert,
    elemHashes,
    prefetchElem,

    -- * Conversion
    freeze,
    unsafeFreeze,
    thaw,

    -- * (De)Serialisation
    formatVersion,
    serialise,
    deserialise,
  ) where

import           Control.DeepSeq (NFData (..))
import           Control.Exception (assert)
import           Control.Monad.Primitive (PrimMonad, PrimState)
import           Control.Monad.ST (ST)
import           Data.Bits
import           Data.Kind (Type)
import           Data.Primitive.ByteArray
import           Data.Primitive.PrimArray
import           Data.Primitive.Types (Prim (..))

import           Data.BloomFilter.Blocked.BitArray (BitArray, BitIx (..),
                     BlockIx (..), MBitArray, NumBlocks (..), bitsToBlocks,
                     blocksToBits)
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
-- The library guarantes that the format version value for the classic
-- ("Data.BloomFilter.Classic") and blocked ("Data.BloomFilter.Blocked")
-- implementation will not overlap with each other or any previous value used
-- by either implementation. So switching between the two implementations will
-- always be detectable and unambigious.
--
-- History:
--
-- * Version 1000: original blocked implementation
--
formatVersion :: Int
formatVersion = 1000

-------------------------------------------------------------------------------
-- Mutable Bloom filters
--

type MBloom :: Type -> Type -> Type
-- | A mutable Bloom filter, for use within the 'ST' monad.
data MBloom s a = MBloom {
      mbNumBlocks :: {-# UNPACK #-} !NumBlocks  -- ^ non-zero
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
-- The reason for the current limit of $2^41$ bits is that this corresponds to
-- 2^32 blocks, each of size 64 bytes (512 bits). The reason for the current
-- limit of 2^32 blocks is that for efficiency we use a single 64bit hash per
-- element, and split that into a pair of 32bit hashes which are used for
-- probing the filter. To go bigger would need a pair of hashes.
--
new :: BloomSize -> ST s (MBloom s a)
new BloomSize { sizeBits, sizeHashes } = do
    let numBlocks = bitsToBlocks (max 1 (min 0x200_0000_0000 sizeBits))
    mbBitArray <- BitArray.new numBlocks
    pure MBloom {
      mbNumBlocks = numBlocks,
      mbNumHashes = max 1 sizeHashes,
      mbBitArray
    }

{-# NOINLINE insertHashes #-}
insertHashes :: forall s a. MBloom s a -> Hashes a -> ST s ()
insertHashes MBloom { mbNumBlocks, mbNumHashes, mbBitArray } !h =
    go g0 mbNumHashes
  where
    blockIx :: BlockIx
    (!blockIx, !g0) = blockIxAndBitGen h mbNumBlocks

    go :: BitIxGen -> Int -> ST s ()
    go !_ 0  = return ()
    go !g !i = do
      let blockBitIx :: BitIx
          (!blockBitIx, !g') = genBitIndex g
      assert (let BlockIx    b = blockIx
                  NumBlocks nb = mbNumBlocks
               in b >= 0 && b < fromIntegral nb) $
        BitArray.unsafeSet mbBitArray blockIx blockBitIx
      go g' (i-1)

prefetchInsert :: MBloom s a -> Hashes a -> ST s ()
prefetchInsert MBloom { mbNumBlocks, mbBitArray } !h =
    BitArray.prefetchSet mbBitArray blockIx
  where
    blockIx :: BlockIx
    (!blockIx, _) = blockIxAndBitGen h mbNumBlocks

-- | Overwrite the filter's bit array. Use 'new' to create a filter of the
-- expected size and then use this function to fill in the bit data.
--
-- The callback is expected to write (exactly) the given number of bytes into
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
      numBlocks :: {-# UNPACK #-} !NumBlocks  -- ^ non-zero
    , numHashes :: {-# UNPACK #-} !Int
    , bitArray  :: {-# UNPACK #-} !BitArray
    }
  deriving Eq
type role Bloom nominal

bloomInvariant :: Bloom a -> Bool
bloomInvariant Bloom {
                 numBlocks = NumBlocks nb,
                 numHashes,
                 bitArray  = BitArray.BitArray pa
               } =
    nb * 8 == sizeofPrimArray pa
 && numHashes > 0

instance Show (Bloom a) where
    show mb = "Bloom { " ++ show numBits ++ " bits } "
      where
        numBits = blocksToBits (numBlocks mb)

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
elemHashes :: Bloom a -> Hashes a -> Bool
elemHashes Bloom { numBlocks, numHashes, bitArray } !h =
    go g0 numHashes
  where
    blockIx :: BlockIx
    (!blockIx, !g0) = blockIxAndBitGen h numBlocks

    go :: BitIxGen -> Int -> Bool
    go !_ 0 = True
    go !g !i
      | let blockBitIx :: BitIx
            (!blockBitIx, !g') = genBitIndex g
      , assert (let BlockIx    b = blockIx
                    NumBlocks nb = numBlocks
                 in b >= 0 && b < fromIntegral nb) $
        BitArray.unsafeIndex bitArray blockIx blockBitIx
      = go g' (i-1)

      | otherwise = False

prefetchElem :: Bloom a -> Hashes a -> ST s ()
prefetchElem Bloom { numBlocks, bitArray } !h =
    BitArray.prefetchIndex bitArray blockIx
  where
    blockIx :: BlockIx
    (!blockIx, _) = blockIxAndBitGen h numBlocks

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
reduceRange32 :: Word -- ^ Sample from 0..2^32-1
              -> Word -- ^ upper bound of range [0,n)
              -> Word -- ^ result within range
reduceRange32 x n =
    assert (n > 0) $
    let w :: Word
        w = x * n
     in w `shiftR` 32

-------------------------------------------------------------------------------
-- Hashes
--

-- | A small family of hashes, for probing bits in a (blocked) bloom filter.
--
newtype Hashes a = Hashes Hash
  deriving stock Show
  deriving newtype Prim
type role Hashes nominal

{-# INLINE hashes #-}
hashes :: Hashable a => a -> Hashes a
hashes = Hashes . hash64

{-# INLINE blockIxAndBitGen #-}
-- | The scheme for turning 'Hashes' into block and bit indexes is as follows:
-- the high 32bits of the 64bit hash select the block of bits, while the low
-- 32bits are used with a simpler PRNG to produce a sequence of probe points
-- within the selected 512bit block.
--
blockIxAndBitGen :: Hashes a -> NumBlocks -> (BlockIx, BitIxGen)
blockIxAndBitGen (Hashes w64) (NumBlocks numBlocks) =
    assert (numBlocks > 0) $
    (blockIx, bitGen)
  where
    blockIx = BlockIx (high32 `reduceRange32` fromIntegral numBlocks)
    bitGen  = BitIxGen low32

    high32, low32 :: Word
    high32 = fromIntegral (w64 `shiftR` 32)
    low32  = fromIntegral w64 .&. 0xffff_ffff

newtype BitIxGen = BitIxGen Word

{-# INLINE genBitIndex #-}
-- | Generate the next in a short sequence of pseudo-random 9-bit values. This
-- is used for selecting the probe bit within the 512 bit block.
--
-- This simple generator works by multiplying a 32bit value by the golden ratio
-- (as a fraction of a 32bit value). This is only suitable for short sequences
-- using the top few bits each time.
genBitIndex :: BitIxGen -> (BitIx, BitIxGen)
genBitIndex (BitIxGen h) =
    (BitIx i, BitIxGen h')
  where
    i  :: Int
    i  = fromIntegral (h `shiftR` (32-9)) -- top 9 bits

    h' :: Word
    h' = (h * 0x9e37_79b9) .&. 0xffff_ffff -- keep least significant 32 bits
