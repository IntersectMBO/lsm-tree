{-# LANGUAGE MagicHash     #-}
{-# LANGUAGE UnboxedTuples #-}
-- |
--
-- Fast hashing of Haskell values.
-- The hash used is XXH3 64bit.
--
module Data.BloomFilter.Hash (
    -- * Basic hash functionality
    Hash,
    Hashable(..),
    hash64,
    hashByteArray,
    -- * Incremental hashing
    Incremental (..),
    HashState,
    incrementalHash,
    -- * Compute a family of hash values
    CheapHashes (..),
    evalHashes,
    makeHashes,
) where

import           Control.Monad (forM_)
import           Control.Monad.ST (ST, runST)
import           Data.Bits (unsafeShiftR)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
import           Data.Char (ord)
import qualified Data.Primitive.ByteArray as P
import           Data.Primitive.Types (Prim (..))
import           Data.Word (Word32, Word64)
import           GHC.Exts (Int#, uncheckedIShiftL#, (+#))
import qualified XXH3

-- | A hash value is 64 bits wide.
type Hash = Word64

-------------------------------------------------------------------------------
-- One shot hashing
-------------------------------------------------------------------------------

-- | The class of types that can be converted to a hash value.
--
-- The instances are meant to be stable, the hash values can be persisted.
--
class Hashable a where
    -- | Compute a 64-bit hash of a value.
    hashSalt64 ::
           Word64  -- ^ seed
        -> a       -- ^ value to hash
        -> Word64

-- | Compute a 64-bit hash.
hash64 :: Hashable a => a -> Word64
hash64 = hashSalt64 0

instance Hashable () where
    hashSalt64 salt _ = salt

instance Hashable Char where
    -- Char's ordinal value should fit into Word32
    hashSalt64 salt c = hashSalt64 salt (fromIntegral (ord c) :: Word32)

instance Hashable BS.ByteString where
    hashSalt64 salt bs = XXH3.xxh3_64bit_withSeed_bs bs salt

instance Hashable LBS.ByteString where
    hashSalt64 salt lbs =
        incrementalHash salt $ \s ->
        forM_ (LBS.toChunks lbs) $ \bs ->
        update s bs

instance Hashable P.ByteArray where
    hashSalt64 salt ba = XXH3.xxh3_64bit_withSeed_ba ba 0 (P.sizeofByteArray ba) salt

instance Hashable Word64 where
    hashSalt64 salt w = XXH3.xxh3_64bit_withSeed_w64 w salt

instance Hashable Word32 where
    hashSalt64 salt w = XXH3.xxh3_64bit_withSeed_w32 w salt

{- Note [Tree hashing]

We recursively hash inductive types (instead e.g. just serially hashing
their fields). Why?

So ("", "x") and ("x", "") or [[],[],[""]], [[],[""],[]] and [[""],[],[]]
have different hash values!

Another approach would be to have injective serialisation,
but then 'Incremental BS.ByteString' instance (e.g.) would need to serialise
the length, so we'd need third class for "pieces", keeping 'Incremental'
just adding bytes to the state (without any extras).

-}

instance Hashable a => Hashable [a] where
    hashSalt64 salt xs = incrementalHash salt $ \s -> forM_ xs $ \x ->
        update s (hash64 x)

instance (Hashable a, Hashable b) => Hashable (a, b) where
    hashSalt64 salt (x, y) = incrementalHash salt $ \s -> do
        update s (hash64 x)
        update s (hash64 y)

-- | Hash a (part of) 'P.ByteArray'.
hashByteArray :: P.ByteArray -> Int -> Int -> Word64 -> Word64
hashByteArray = XXH3.xxh3_64bit_withSeed_ba

-------------------------------------------------------------------------------
-- Incremental hashing
-------------------------------------------------------------------------------

-- | Hash state for incremental hashing
newtype HashState s = HashState (XXH3.XXH3_State s)

-- | The class of types that can be incrementally hashed.
class Incremental a where
    update :: HashState s -> a -> ST s ()

instance Incremental BS.ByteString where
    update (HashState s) = XXH3.xxh3_64bit_update_bs s

instance Incremental Word32 where
    update (HashState s) = XXH3.xxh3_64bit_update_w32 s

instance Incremental Word64 where
    update (HashState s) = XXH3.xxh3_64bit_update_w64 s

instance Incremental Char where
    update s c = update s (fromIntegral (ord c) :: Word32)

-- | Calculate incrementally constructed hash.
incrementalHash :: Word64 -> (forall s. HashState s -> ST s ()) -> Word64
incrementalHash seed f = runST $ do
    s <- XXH3.xxh3_64bit_createState
    XXH3.xxh3_64bit_reset_withSeed s seed
    f (HashState s)
    XXH3.xxh3_64bit_digest s

-------------------------------------------------------------------------------
-- CheapHashes
-------------------------------------------------------------------------------

-- | A pair of hashes used for a double hashing scheme.
--
-- See 'evalCheapHashes'.
data CheapHashes a = CheapHashes !Hash !Hash
  deriving Show
type role CheapHashes nominal

instance Prim (CheapHashes a) where
    sizeOfType# _ = 16#
    alignmentOfType# _ = 8#

    indexByteArray# ba i = CheapHashes
        (indexByteArray# ba (indexLo i))
        (indexByteArray# ba (indexHi i))
    readByteArray# ba i s1 =
        case readByteArray# ba (indexLo i) s1 of { (# s2, lo #) ->
        case readByteArray# ba (indexHi i) s2 of { (# s3, hi #) ->
        (# s3, CheapHashes lo hi #)
        }}
    writeByteArray# ba i (CheapHashes lo hi) s =
        writeByteArray# ba (indexHi i) hi (writeByteArray# ba (indexLo i) lo s)

    indexOffAddr# ba i = CheapHashes
        (indexOffAddr# ba (indexLo i))
        (indexOffAddr# ba (indexHi i))
    readOffAddr# ba i s1 =
        case readOffAddr# ba (indexLo i) s1 of { (# s2, lo #) ->
        case readOffAddr# ba (indexHi i) s2 of { (# s3, hi #) ->
        (# s3, CheapHashes lo hi #)
        }}
    writeOffAddr# ba i (CheapHashes lo hi) s =
        writeOffAddr# ba (indexHi i) hi (writeOffAddr# ba (indexLo i) lo s)

indexLo :: Int# -> Int#
indexLo i = uncheckedIShiftL# i 1#

indexHi :: Int# -> Int#
indexHi i = uncheckedIShiftL# i 1# +# 1#

{- Note [Original CheapHashes]

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

{- Note: [CheapHashes]

On the first glance the 'evalCheapHashes' scheme seems dubious.

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
'evalCheapHashes' scheme.

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

The CheapHashes scheme seems to avoid these cuprits.
This is probably because it uses most of the bits of the second hash, even in m = 2^n scenarios.
(normal double hashing and enhances double hashing don't use the high bits or original hash then).
TL;DR CheapHashes seems to work well in practice.

For the record: RocksDB uses an own scheme as well,
where first hash is used to pick a cache line, and second one to generate probes inside it.
https://github.com/facebook/rocksdb/blob/096fb9b67d19a9a180e7c906b4a0cdb2b2d0c1f6/util/bloom_impl.h

-}

-- | Evaluate 'CheapHashes' family.
--
-- \[
-- g_i = h_0 + \left\lfloor h_1 / 2^i \right\rfloor
-- \]
--
evalHashes :: CheapHashes a -> Int -> Hash
evalHashes (CheapHashes h1 h2) i = h1 + (h2 `unsafeShiftR` i)

-- | Create 'CheapHashes' structure.
--
-- It's simply hashes the value twice using seed 0 and 1.
makeHashes :: Hashable a => a -> CheapHashes a
makeHashes v = CheapHashes (hashSalt64 0 v) (hashSalt64 1 v)
{-# SPECIALISE makeHashes :: BS.ByteString -> CheapHashes BS.ByteString #-}
{-# INLINEABLE makeHashes #-}
