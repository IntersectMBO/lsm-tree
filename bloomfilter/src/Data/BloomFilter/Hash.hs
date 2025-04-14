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
) where

import           Control.Monad (forM_)
import           Control.Monad.ST (ST, runST)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
import           Data.Char (ord)
import qualified Data.Primitive.ByteArray as P
import           Data.Word (Word32, Word64)
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
