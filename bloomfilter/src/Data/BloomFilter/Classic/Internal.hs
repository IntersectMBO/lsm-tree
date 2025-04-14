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
    serialise,
    deserialise,
    freeze,
    unsafeFreeze,
    thaw,
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

#if MIN_VERSION_base(4,17,0)
import           GHC.Exts (remWord64#)
#else
import           GHC.Exts (remWord#)
#endif
import           GHC.Exts (Int#, uncheckedIShiftL#, (+#))
import           GHC.Word (Word64 (W64#))

import           Data.BloomFilter.Classic.BitArray (BitArray, MBitArray)
import qualified Data.BloomFilter.Classic.BitArray as BitArray
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
          index = fromIntegral (probe `unsafeRemWord64` fromIntegral mbNumBits)
      -- While the probe point can cover the full Word64 range, after range
      -- reduction it is less then the filter size and thus must fit in an Int.
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
          index = fromIntegral (probe `unsafeRemWord64` fromIntegral mbNumBits)
      b <- BitArray.unsafeRead mbBitArray index
      if b then go (i + 1)
           else return False

-- | Modify the filter's bit array. The callback is expected to read (exactly)
-- the given number of bytes into the given byte array buffer.
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
          index = fromIntegral (probe `unsafeRemWord64` fromIntegral numBits)
          -- While the probe point can cover the full Word64 range, after range
          -- reduction it is less then the filter size and must fit in an Int.
       in if BitArray.unsafeIndex bitArray index
            then go (i + 1)
            else False

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
