{-# LANGUAGE BangPatterns, CPP, Rank2Types,
    TypeOperators,FlexibleContexts #-}

-- |
-- Module: Data.BloomFilter.Mutable
-- Copyright: Bryan O'Sullivan
-- License: BSD3
--
-- Maintainer: Bryan O'Sullivan <bos@serpentine.com>
-- Stability: unstable
-- Portability: portable
--
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

module Data.BloomFilter.Mutable
    (
    -- * Overview
    -- $overview

    -- ** Ease of use
    -- $ease

    -- ** Performance
    -- $performance

    -- * Types
      Hash
    , MBloom
    -- * Mutable Bloom filters

    -- ** Creation
    , new

    -- ** Accessors
    , length
    , elem

    -- ** Mutation
    , insert

    -- * The underlying representation
    -- | If you serialize the raw bit arrays below to disk, do not
    -- expect them to be portable to systems with different
    -- conventions for endianness or word size.

    -- | The raw bit array used by the mutable 'MBloom' type.
    , bitArray
    ) where

#include "MachDeps.h"

import Control.Monad (liftM, forM_)
import Control.Monad.ST (ST)
import Data.BloomFilter.Util (ceil64)
import Data.BloomFilter.Mutable.Internal
import Data.BloomFilter.Hash (Hashable)

import qualified Data.BloomFilter.BitVec64 as V

import Prelude hiding (elem, length, notElem,
                       (/), (*), div, divMod, mod)

-- | Create a new mutable Bloom filter.  For efficiency, the number of
-- bits used may be larger than the number requested.  It is always
-- rounded up to the nearest higher power of two, but will be clamped
-- at a maximum of 4 gigabits, since hashes are 32 bits in size.
new :: Int                    -- ^ number of hash functions to use
    -> Int                    -- ^ number of bits in filter
    -> ST s (MBloom s a)
new hash numBits = MB hash numBits' `liftM` V.new numBits'
  where numBits' | numBits < 64 = 64
                 | numBits > maxHash = maxHash
                 | otherwise = ceil64 numBits
              
maxHash :: Int
#if WORD_SIZE_IN_BITS == 64
maxHash = 4294967296
#else
maxHash = 1073741824
#endif

-- | Insert a value into a mutable Bloom filter.  Afterwards, a
-- membership query for the same value is guaranteed to return @True@.
insert :: Hashable a => MBloom s a -> a -> ST s ()
insert mb elt = do
  let mu = bitArray mb
  forM_ (hashes mb elt) $ \idx' -> do
      let !idx = fromIntegral idx' `rem` size mb :: Int
      V.unsafeWrite mu idx True

-- | Query a mutable Bloom filter for membership.  If the value is
-- present, return @True@.  If the value is not present, there is
-- /still/ some possibility that @True@ will be returned.
elem :: Hashable a => a -> MBloom s a -> ST s Bool
elem elt mb = loop (hashes mb elt)
  where mu = bitArray mb
        loop (idx':wbs) = do
          let !idx = fromIntegral idx' `rem` size mb :: Int
          b <- V.unsafeRead mu idx
          case b of
              False -> return False
              True  -> loop wbs
        loop _ = return True

-- bitsInHash :: Int
-- bitsInHash = sizeOf (undefined :: Hash) `shiftL` 3

-- | Return the size of a mutable Bloom filter, in bits.
length :: MBloom s a -> Int
length = size

-- $overview
--
-- Each of the functions for creating Bloom filters accepts two parameters:
--
-- * The number of bits that should be used for the filter.  Note that
--   a filter is fixed in size; it cannot be resized after creation.
--
-- * A function that accepts a value, and should return a fixed-size
--   list of hashes of that value.  To keep the false positive rate
--   low, the hashes computes should, as far as possible, be
--   independent.
--
-- By choosing these parameters with care, it is possible to tune for
-- a particular false positive rate.  The @suggestSizing@ function in
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
--
-- For efficiency, the number of bits requested when creating a Bloom
-- filter is rounded up to the nearest power of two.  This lets the
-- implementation use bitwise operations internally, instead of much
-- more expensive multiplication, division, and modulus operations.
