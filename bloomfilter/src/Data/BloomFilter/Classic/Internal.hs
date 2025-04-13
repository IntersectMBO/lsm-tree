{-# LANGUAGE CPP           #-}
{-# LANGUAGE MagicHash     #-}
{-# LANGUAGE UnboxedTuples #-}
{-# OPTIONS_HADDOCK not-home #-}
-- | This module defines the 'Bloom' and 'MBloom' types and all the functions
-- that need direct knowledge of and access to the representation. This forms
-- the trusted base.
module Data.BloomFilter.Classic.Internal (
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
    elemHashes,
    readHashes,

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
import           Data.Word (Word64)

import           GHC.Exts (Int (I#), Int#, int2Word#, timesWord2#,
                     uncheckedIShiftL#, word2Int#, (+#))
import qualified GHC.Exts as Exts
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
-- The library guarantes that the format version value for the classic
-- ("Data.BloomFilter.Classic") and blocked ("Data.BloomFilter.Blocked")
-- implementation will not overlap with each other or any previous value used
-- by either implementation. So switching between the two implementations will
-- always be detectable and unambigious.
--
-- History:
--
-- * Version 0: original
--
-- * Version 1: changed range reduction (of hash to bit index) from remainder
--   to method based on multiplication.
--
formatVersion :: Int
formatVersion = 1

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

insertHashes :: MBloom s a -> Hashes a -> ST s ()
insertHashes MBloom { mbNumBits, mbNumHashes, mbBitArray } !h =
    go 0
  where
    go !i | i >= mbNumHashes = return ()
    go !i = do
      let probe :: Word64
          probe = evalHashes h i
          index :: Int
          index = reduceRange64 probe mbNumBits
      BitArray.unsafeSet mbBitArray index
      go (i + 1)

readHashes :: forall s a. MBloom s a -> Hashes a -> ST s Bool
readHashes MBloom { mbNumBits, mbNumHashes, mbBitArray } !h =
    go 0
  where
    go :: Int -> ST s Bool
    go !i | i >= mbNumHashes = return True
    go !i = do
      let probe :: Word64
          probe = evalHashes h i
          index :: Int
          index = reduceRange64 probe mbNumBits
      b <- BitArray.unsafeRead mbBitArray index
      if b then go (i + 1)
           else return False

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
      numBits   :: {-# UNPACK #-} !Int  -- ^ non-zero
    , numHashes :: {-# UNPACK #-} !Int
    , bitArray  :: {-# UNPACK #-} !BitArray
    }
  deriving Eq
type role Bloom nominal

bloomInvariant :: Bloom a -> Bool
bloomInvariant Bloom { numBits, numHashes, bitArray = BitArray.BitArray pa } =
       numBits > 0
    && numBits <= 2^(48 :: Int)
    && ceilDiv64 numBits == sizeofPrimArray pa
    && numHashes > 0
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

-- | Query an immutable Bloom filter for membership using already constructed
-- 'Hashes' value.
elemHashes :: Bloom a -> Hashes a -> Bool
elemHashes Bloom { numBits, numHashes, bitArray } !h =
    go 0
  where
    go :: Int -> Bool
    go !i | i >= numHashes = True
    go !i =
      let probe :: Word64
          probe = evalHashes h i
          index :: Int
          index = reduceRange64 probe numBits
       in if BitArray.unsafeIndex bitArray index
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

-- | Given a word sampled uniformly from the full 'Word64' range, such as a
-- hash, reduce it fairly to a value in the range @[0,n)@.
--
-- See <https://lemire.me/blog/2016/06/27/a-fast-alternative-to-the-modulo-reduction/>
--
{-# INLINE reduceRange64 #-}
reduceRange64 :: Word64 -- ^ Sample from 0..2^64-1
              -> Int -- ^ upper bound of range [0,n)
              -> Int -- ^ result within range
reduceRange64 (W64# x) (I# n) =
    -- Note that we use widening multiplication of two 64bit numbers, with a
    -- 128bit result. GHC provides a primop which returns the 128bit result as
    -- a pair of 64bit words. There are (as of 2025) no high level wrappers in
    -- the base or primitive packages, so we use the primops directly.
    case timesWord2# (word64ToWordShim# x) (int2Word# n) of
      (# high, _low #) -> I# (word2Int# high)
    -- Note that while x can cover the full Word64 range, since the result is
    -- less than n, and since n was an Int then the result fits an Int too.

{-# INLINE word64ToWordShim# #-}

#if MIN_VERSION_base(4,17,0)
word64ToWordShim# :: Exts.Word64# -> Exts.Word#
word64ToWordShim# = Exts.word64ToWord#
#else
word64ToWordShim# :: Exts.Word# -> Exts.Word#
word64ToWordShim# x# = x#
#endif

-------------------------------------------------------------------------------
-- Hashes
--

-- | A pair of hashes used for a double hashing scheme.
--
-- See 'evalHashes'.
data Hashes a = Hashes !Hash !Hash
  deriving Show
type role Hashes nominal

instance Prim (Hashes a) where
    sizeOfType# _ = 16#
    alignmentOfType# _ = 8#

    indexByteArray# ba i = Hashes
        (indexByteArray# ba (indexLo i))
        (indexByteArray# ba (indexHi i))
    readByteArray# ba i s1 =
        case readByteArray# ba (indexLo i) s1 of { (# s2, lo #) ->
        case readByteArray# ba (indexHi i) s2 of { (# s3, hi #) ->
        (# s3, Hashes lo hi #)
        }}
    writeByteArray# ba i (Hashes lo hi) s =
        writeByteArray# ba (indexHi i) hi (writeByteArray# ba (indexLo i) lo s)

    indexOffAddr# ba i = Hashes
        (indexOffAddr# ba (indexLo i))
        (indexOffAddr# ba (indexHi i))
    readOffAddr# ba i s1 =
        case readOffAddr# ba (indexLo i) s1 of { (# s2, lo #) ->
        case readOffAddr# ba (indexHi i) s2 of { (# s3, hi #) ->
        (# s3, Hashes lo hi #)
        }}
    writeOffAddr# ba i (Hashes lo hi) s =
        writeOffAddr# ba (indexHi i) hi (writeOffAddr# ba (indexLo i) lo s)

indexLo :: Int# -> Int#
indexLo i = uncheckedIShiftL# i 1#

indexHi :: Int# -> Int#
indexHi i = uncheckedIShiftL# i 1# +# 1#

{- Note [Original Hashes]

Compute a list of 32-bit hashes relatively cheaply.  The value to
hash is inspected at most twice, regardless of the number of hashes
requested.

We use a variant of Kirsch and Mitzenmacher's technique from \"Less
Hashing, Same Performance: Building a Better Bloom Filter\",
<http://www.eecs.harvard.edu/~kirsch/pubs/bbbf/esa06.pdf>.

Where Kirsch and Mitzenmacher multiply the second hash by a
coefficient, we shift right by the coefficient.  This offers better
performance (as a shift is much cheaper than a multiply), and the
low order bits of the final hash stay well mixed.

-}

{- Note: [Hashes]

On the first glance the 'evalHashes' scheme seems dubious.

Firstly, it's original performance motivation is dubious.

> multiply the second hash by a coefficient

While the scheme double hashing scheme is presented in
theoretical analysis as

    g(i) = a + i * b

In practice it's implemented in a loop which looks like

    g[0] = a
    for (i = 1; i < k; i++) {
        a += b;
        g[i] = a;
    }

I.e. with just an addition.

Secondly there is no analysis anywhere about the
'evalHashes' scheme.

Peter Dillinger's thesis (Adaptive Approximate State Storage)
discusses various fast hashing schemes (section 6.5),
mentioning why ordinary "double hashing" is weak scheme.

Issue 1: when second hash value is bad, e.g. not coprime with bloom filters size in bits,
we can get repetitions (worst case 0, or m/2).

Issue 2: in bloom filter scenario, whether we do a + i * b or h0 - i * b' (with b' = -b)
as we probe all indices (as set) doesn't matter, not sequentially (like in hash table).
So we lose one bit entropy.

Issue 3: the scheme is prone to partial overlap.
Two values with the same second hash value could overlap on many indices.

Then Dillinger discusses various schemes which solve this issue.

The Hashes scheme seems to avoid these cuprits.
This is probably because it uses most of the bits of the second hash, even in m = 2^n scenarios.
(normal double hashing and enhances double hashing don't use the high bits or original hash then).
TL;DR Hashes seems to work well in practice.

For the record: RocksDB uses an own scheme as well,
where first hash is used to pick a cache line, and second one to generate probes inside it.
https://github.com/facebook/rocksdb/blob/096fb9b67d19a9a180e7c906b4a0cdb2b2d0c1f6/util/bloom_impl.h

-}

-- | Evalute 'Hashes' family.
--
-- \[
-- g_i = h_0 + \left\lfloor h_1 / 2^i \right\rfloor
-- \]
--
evalHashes :: Hashes a -> Int -> Hash
evalHashes (Hashes h1 h2) i = h1 + (h2 `unsafeShiftR` i)

-- | Create 'Hashes' structure.
--
-- It's simply hashes the value twice using seed 0 and 1.
hashes :: Hashable a => a -> Hashes a
hashes v = Hashes (hashSalt64 0 v) (hashSalt64 1 v)
{-# INLINE hashes #-}
