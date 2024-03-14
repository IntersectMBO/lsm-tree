{-# LANGUAGE BangPatterns, CPP, CApiFFI, ForeignFunctionInterface,
    TypeOperators, RoleAnnotations, MagicHash, UnliftedFFITypes #-}
{-# LANGUAGE InstanceSigs #-}

-- |
-- Module: Data.BloomFilter.Hash
-- Copyright: Bryan O'Sullivan
-- License: BSD3
--
-- Maintainer: Bryan O'Sullivan <bos@serpentine.com>
-- Stability: unstable
-- Portability: portable
--
-- Fast hashing of Haskell values.  The hash functions used are Bob
-- Jenkins's public domain functions, which combine high performance
-- with excellent mixing properties.  For more details, see
-- <http://burtleburtle.net/bob/hash/>.
--
-- In addition to the usual "one input, one output" hash functions,
-- this module provides multi-output hash functions, suitable for use
-- in applications that need multiple hashes, such as Bloom filtering.

module Data.BloomFilter.Hash
    (
      Hash
    -- * Basic hash functionality
    , Hashable(..)
    , hash64
    -- * Compute a family of hash values
    , CheapHashes (..)
    , cheapHashes
    , evalCheapHashes
    , makeCheapHashes
    -- * Hash functions
    , hashByteArray
    ) where

import Data.Array.Byte (ByteArray (..))
import Data.Bits (unsafeShiftR)
import Data.Word (Word64)
import XXH3 (xxh3_64bit_withSeed_ba, xxh3_64bit_withSeed_bs)
import qualified Data.ByteString as SB
import qualified Data.Primitive as P

-- | A hash value is 64 bits wide.
type Hash = Word64

class Hashable a where
    -- | Compute a 64-bit hash of a value.  The first salt value
    -- perturbs the first element of the result, and the second salt
    -- perturbs the second.
    hashSalt64
        :: Word64               -- ^ salt
        -> a           -- ^ value to hash
        -> Word64

-- | Compute a 64-bit hash.
hash64 :: Hashable a => a -> Word64
hash64 = hashSalt64 0

-- | Compute a list of 32-bit hashes relatively cheaply.  The value to
-- hash is inspected at most twice, regardless of the number of hashes
-- requested.
--
-- We use a variant of Kirsch and Mitzenmacher's technique from \"Less
-- Hashing, Same Performance: Building a Better Bloom Filter\",
-- <http://www.eecs.harvard.edu/~kirsch/pubs/bbbf/esa06.pdf>.
--
-- Where Kirsch and Mitzenmacher multiply the second hash by a
-- coefficient, we shift right by the coefficient.  This offers better
-- performance (as a shift is much cheaper than a multiply), and the
-- low order bits of the final hash stay well mixed.
data CheapHashes a = CheapHashes !Hash !Hash
  deriving Show
type role CheapHashes nominal

evalCheapHashes :: CheapHashes a -> Int -> Hash
evalCheapHashes (CheapHashes h1 h2) i = h1 + (h2 `unsafeShiftR` i)

makeCheapHashes :: Hashable a => a -> CheapHashes a
{-# SPECIALIZE makeCheapHashes :: SB.ByteString -> CheapHashes SB.ByteString #-}
makeCheapHashes v = CheapHashes (fromIntegral (hashSalt64 0 v)) (fromIntegral (hashSalt64 1 v))

cheapHashes :: Hashable a => Int -- ^ number of hashes to compute
            -> a                 -- ^ value to hash
            -> [Hash]
{-# SPECIALIZE cheapHashes :: Int -> SB.ByteString -> [Hash] #-}
cheapHashes k v = go 0
    where !ch = makeCheapHashes v

          go :: Int -> [Hash]
          go !i | i == k = []
                | otherwise = evalCheapHashes ch i : go (i + 1)

instance Hashable SB.ByteString where
    hashSalt64 salt bs = xxh3_64bit_withSeed_bs bs salt

-- This instance is for tests only
instance Hashable Word64 where
    hashSalt64 salt w = case P.primArrayFromList [w] of
        P.PrimArray ba# -> xxh3_64bit_withSeed_ba (ByteArray ba#) 0 8 salt

hashByteArray :: ByteArray -> Int -> Int -> Word64 -> Word64
hashByteArray = xxh3_64bit_withSeed_ba
