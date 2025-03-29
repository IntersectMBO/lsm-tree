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
    MBloom (..),
    CheapHashes,
    -- * Mutable Bloom filters

    -- ** Creation
    BloomSize (..),
    new,

    -- ** Accessors
    size,
    elem,

    -- ** Mutation
    insert,
) where

import           Control.Monad.ST (ST)
import           Data.Kind (Type)

import qualified Data.BloomFilter.BitVec64 as V
import           Data.BloomFilter.Calc (BloomSize (..))
import           Data.BloomFilter.Hash (CheapHashes, Hash, Hashable, evalHashes,
                     makeHashes)

import           Prelude hiding (elem)

type MBloom :: Type -> Type -> Type
-- | A mutable Bloom filter, for use within the 'ST' monad.
data MBloom s a = MBloom {
      numBits   :: {-# UNPACK #-} !Int  -- ^ non-zero
    , numHashes :: {-# UNPACK #-} !Int
    , bitArray  :: {-# UNPACK #-} !(V.MBitVec64 s)
    }
type role MBloom nominal nominal

instance Show (MBloom s a) where
    show mb = "MBloom { " ++ show (numBits mb) ++ " bits } "

-- | Create a new mutable Bloom filter.
--
-- The size is ceiled at $2^48$. Tell us if you need bigger bloom filters.
--
new :: BloomSize -> ST s (MBloom s a)
new BloomSize { bloomNumBits = numBits, bloomNumHashes } = do
    bitArray <- V.new (fromIntegral numBits')
    pure MBloom {
      numBits   = numBits',
      numHashes = bloomNumHashes,
      bitArray
    }
  where numBits' | numBits == 0                = 1
                 | numBits >= 0xffff_ffff_ffff = 0x1_0000_0000_0000
                 | otherwise                   = numBits

-- | Insert a value into a mutable Bloom filter.  Afterwards, a
-- membership query for the same value is guaranteed to return @True@.
insert :: Hashable a => MBloom s a -> a -> ST s ()
insert !mb !x = insertHashes mb (makeHashes x)

insertHashes :: MBloom s a -> CheapHashes a -> ST s ()
insertHashes MBloom { numBits = m, numHashes = k, bitArray = v } !h =
    go 0
  where
    go !i | i >= k = return ()
          | otherwise = let !idx = evalHashes h i `rem` fromIntegral m
                        in V.unsafeWrite v idx True >> go (i + 1)

-- | Query a mutable Bloom filter for membership.  If the value is
-- present, return @True@.  If the value is not present, there is
-- /still/ some possibility that @True@ will be returned.
elem :: Hashable a => a -> MBloom s a -> ST s Bool
elem elt mb = elemHashes (makeHashes elt) mb

elemHashes :: forall s a. CheapHashes a -> MBloom s a -> ST s Bool
elemHashes !ch MBloom { numBits = m, numHashes = k, bitArray = v } =
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
-- | Return the size of the Bloom filter.
size :: MBloom s a -> BloomSize
size MBloom { numBits, numHashes } =
    BloomSize {
      bloomNumBits   = numBits,
      bloomNumHashes = numHashes
    }

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
