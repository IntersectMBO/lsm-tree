-- |
-- A fast, space efficient Bloom filter implementation.  A Bloom
-- filter is a set-like data structure that provides a probabilistic
-- membership test.
--
-- * Queries do not give false negatives.  When an element is added to
--   a filter, a subsequent membership test will definitely return
--   'True'.
--
-- * False positives /are/ possible.  If an element has not been added
--   to a filter, a membership test /may/ nevertheless indicate that
--   the element is present.
--
-- This module provides low-level control.  For an easier to use
-- interface, see the "Data.BloomFilter.Easy" module.

module Data.BloomFilter.Mutable (
    -- * Overview
    -- $overview

    -- ** Ease of use
    -- $ease

    -- ** Performance
    -- $performance

    -- * Types
    Hash,
    MBloom,
    MBloom',
    CheapHashes,
    RealHashes,
    -- * Mutable Bloom filters

    -- ** Creation
    new,

    -- ** Accessors
    length,
    elem,

    -- ** Mutation
    insert,
) where

import           Control.Monad (liftM)
import           Control.Monad.ST (ST)
import           Data.BloomFilter.Hash (CheapHashes, Hash, Hashable,
                     Hashes (..), RealHashes)
import           Data.BloomFilter.Mutable.Internal
import           Data.Word (Word64)

import qualified Data.BloomFilter.BitVec64 as V

import           Prelude hiding (elem, length)

-- | Mutable Bloom filter using CheapHashes hashing scheme.
type MBloom s = MBloom' s CheapHashes

-- | Create a new mutable Bloom filter.
--
-- The size is ceiled at $2^48$. Tell us if you need bigger bloom filters.
--
new :: Int                    -- ^ number of hash functions to use
    -> Word64                 -- ^ number of bits in filter
    -> ST s (MBloom' s h a)
new hash numBits = MBloom hash numBits' `liftM` V.new numBits'
  where numBits' | numBits == 0                = 1
                 | numBits >= 0xffff_ffff_ffff = 0x1_0000_0000_0000
                 | otherwise                   = numBits

-- | Insert a value into a mutable Bloom filter.  Afterwards, a
-- membership query for the same value is guaranteed to return @True@.
insert :: (Hashes h, Hashable a) => MBloom' s h a -> a -> ST s ()
insert !mb !x = insertHashes mb (makeHashes x)

insertHashes :: Hashes h => MBloom' s h a -> h a -> ST s ()
insertHashes (MBloom k m v) !h = go 0
  where
    go !i | i >= k = return ()
          | otherwise = let !idx = evalHashes h i `rem` m
                        in V.unsafeWrite v idx True >> go (i + 1)

-- | Query a mutable Bloom filter for membership.  If the value is
-- present, return @True@.  If the value is not present, there is
-- /still/ some possibility that @True@ will be returned.
elem :: (Hashes h, Hashable a) => a -> MBloom' s h a -> ST s Bool
elem elt mb = elemHashes (makeHashes elt) mb

elemHashes :: forall h s a. Hashes h => h a -> MBloom' s h a -> ST s Bool
elemHashes !ch (MBloom k m v) = go 0 where
    go :: Int -> ST s Bool
    go !i | i >= k    = return True
          | otherwise = do let !idx' = evalHashes ch i
                           let !idx = idx' `rem` m
                           b <- V.unsafeRead v idx
                           if b
                           then go (i + 1)
                           else return False

-- | Return the size of a mutable Bloom filter, in bits.
length :: MBloom' s h a -> Word64
length = size

-- $overview
--
-- Each of the functions for creating Bloom filters accepts two parameters:
--
-- * The number of bits that should be used for the filter.  Note that
--   a filter is fixed in size; it cannot be resized after creation.
--
-- * A number of hash functions, /k/, to be used for the filter.
--
-- By choosing these parameters with care, it is possible to tune for
-- a particular false positive rate.
-- The 'Data.BloomFilter.Easy.suggestSizing' function in
-- the "Data.BloomFilter.Easy" module calculates useful estimates for
-- these parameters.

-- $ease
--
-- This module provides both mutable interfaces for creating and
-- querying a Bloom filter.  It is most useful as a low-level way to
-- manage a Bloom filter with a custom set of characteristics.

-- $performance
--
-- The implementation has been carefully tuned for high performance
-- and low space consumption.
