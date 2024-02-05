{-# LANGUAGE BangPatterns, Rank2Types, ScopedTypeVariables, TypeOperators, RoleAnnotations #-}

-- |
-- Module: Data.BloomFilter
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

module Data.BloomFilter
    (
    -- * Overview
    -- $overview

    -- ** Ease of use
    -- $ease

    -- ** Performance
    -- $performance

    -- * Types
      Hash
    , Bloom
    , MBloom

    -- * Immutable Bloom filters

    -- ** Conversion
    , freeze
    , thaw
    , unsafeFreeze

    -- ** Creation
    , unfold

    , fromList
    , empty
    , singleton

    -- ** Accessors
    , length
    , elem
    , notElem

    -- ** Modification
    , insert
    , insertList

    -- * The underlying representation
    -- | If you serialize the raw bit arrays below to disk, do not
    -- expect them to be portable to systems with different
    -- conventions for endianness or word size.

    -- | The raw bit array used by the immutable 'Bloom' type.
    , bitArray
    ) where

import Control.Monad (liftM, forM_)
import Control.Monad.ST (ST, runST)
import Control.DeepSeq (NFData(..))
import Data.Bits ((.&.), unsafeShiftL)
import qualified Data.BloomFilter.Mutable as MB
import qualified Data.BloomFilter.Mutable.Internal as MB
import Data.BloomFilter.Mutable.Internal (Hash, MBloom)
import Data.BloomFilter.Hash (Hashable)
import qualified Data.BloomFilter.Hash as Hash

import Prelude hiding (elem, length, notElem,
                       (/), (*), div, divMod, mod, rem)

import Data.Bit (Bit (..))
import qualified Data.Vector.Unboxed as UV

-- | An immutable Bloom filter, suitable for querying from pure code.
data Bloom a = B {
      hashesN :: {-# UNPACK #-} !Int
    , shift :: {-# UNPACK #-} !Int
    , mask :: {-# UNPACK #-} !Int
    , bitArray :: {-# UNPACK #-} !(UV.Vector Bit)
    }
type role Bloom nominal

hashes :: Hash.Hashable a => Bloom a -> a -> [Hash.Hash]
hashes ub = Hash.cheapHashes (hashesN ub)
{-# INLINE hashes #-}

instance Show (Bloom a) where
    show ub = "Bloom { " ++ show ((1::Int) `unsafeShiftL` shift ub) ++ " bits } "

instance NFData (Bloom a) where
    rnf !_ = ()

-- | Create an immutable Bloom filter, using the given setup function
-- which executes in the 'ST' monad.
--
-- Example:
--
-- @
--import "Data.BloomFilter.Hash" (cheapHashes)
--
--filter = create (cheapHashes 3) 1024 $ \mf -> do
--           insertMB mf \"foo\"
--           insertMB mf \"bar\"
-- @
--
-- Note that the result of the setup function is not used.
create :: Int        -- ^ number of hash functions to use
        -> Int                  -- ^ number of bits in filter
        -> (forall s. (MBloom s a -> ST s ()))  -- ^ setup function
        -> Bloom a
{-# INLINE create #-}
create hash numBits body = runST $ do
  mb <- MB.new hash numBits
  body mb
  unsafeFreeze mb

-- | Create an immutable Bloom filter from a mutable one.  The mutable
-- filter may be modified afterwards.
freeze :: MBloom s a -> ST s (Bloom a)
freeze mb = B (MB.hashesN mb) (MB.shift mb) (MB.mask mb) `liftM`
            UV.freeze (MB.bitArray mb)

-- | Create an immutable Bloom filter from a mutable one.  The mutable
-- filter /must not/ be modified afterwards, or a runtime crash may
-- occur.  For a safer creation interface, use 'freeze' or 'create'.
unsafeFreeze :: MBloom s a -> ST s (Bloom a)
unsafeFreeze mb = B (MB.hashesN mb) (MB.shift mb) (MB.mask mb) `liftM`
                    UV.unsafeFreeze (MB.bitArray mb)

-- | Copy an immutable Bloom filter to create a mutable one.  There is
-- no non-copying equivalent.
thaw :: Bloom a -> ST s (MBloom s a)
thaw ub = MB.MB (hashesN ub) (shift ub) (mask ub) `liftM` UV.thaw (bitArray ub)

-- | Create an empty Bloom filter.
--
-- This function is subject to fusion with 'insert'
-- and 'insertList'.
empty :: Int                    -- ^ number of hash functions to use
       -> Int                   -- ^ number of bits in filter
       -> Bloom a
{-# INLINE [1] empty #-}
empty hash numBits = create hash numBits (\_ -> return ())

-- | Create a Bloom filter with a single element.
--
-- This function is subject to fusion with 'insert'
-- and 'insertList'.
singleton :: Hashable a
           => Int               -- ^ number of hash functions to use
           -> Int               -- ^ number of bits in filter
           -> a                 -- ^ element to insert
           -> Bloom a
{-# INLINE [1] singleton #-}
singleton hash numBits elt = create hash numBits (\mb -> MB.insert mb elt)

-- | Query an immutable Bloom filter for membership.  If the value is
-- present, return @True@.  If the value is not present, there is
-- /still/ some possibility that @True@ will be returned.
elem :: Hashable a => a -> Bloom a -> Bool
elem elt ub = all test (hashes ub elt)
  where test idx' =
          let !idx = fromIntegral idx' .&. mask ub :: Int
          in unBit (bitArray ub UV.! idx)

modify :: (forall s. (MBloom s a -> ST s z))  -- ^ mutation function (result is discarded)
        -> Bloom a
        -> Bloom a
{-# INLINE modify #-}
modify body ub = runST $ do
  mb <- thaw ub
  _ <- body mb
  unsafeFreeze mb

-- | Create a new Bloom filter from an existing one, with the given
-- member added.
--
-- This function may be expensive, as it is likely to cause the
-- underlying bit array to be copied.
--
-- Repeated applications of this function with itself are subject to
-- fusion.
insert :: Hashable a => a -> Bloom a -> Bloom a
{-# NOINLINE insert #-}
insert elt = modify (flip MB.insert elt)

-- | Create a new Bloom filter from an existing one, with the given
-- members added.
--
-- This function may be expensive, as it is likely to cause the
-- underlying bit array to be copied.
--
-- Repeated applications of this function with itself are subject to
-- fusion.
insertList :: Hashable a => [a] -> Bloom a -> Bloom a
{-# NOINLINE insertList #-}
insertList elts = modify $ \mb -> mapM_ (MB.insert mb) elts

{-# RULES "Bloom insert . insert" forall a b u.
    insert b (insert a u) = insertList [a,b] u
  #-}

{-# RULES "Bloom insertList . insert" forall x xs u.
    insertList xs (insert x u) = insertList (x:xs) u
  #-}

{-# RULES "Bloom insert . insertList" forall x xs u.
    insert x (insertList xs u) = insertList (x:xs) u
  #-}

{-# RULES "Bloom insertList . insertList" forall xs ys u.
    insertList xs (insertList ys u) = insertList (xs++ys) u
  #-}

{-# RULES "Bloom insertList . empty" forall h n xs.
    insertList xs (empty h n) = fromList h n xs
  #-}

{-# RULES "Bloom insertList . singleton" forall h n x xs.
    insertList xs (singleton h n x) = fromList h n (x:xs)
  #-}

-- | Query an immutable Bloom filter for non-membership.  If the value
-- /is/ present, return @False@.  If the value is not present, there
-- is /still/ some possibility that @False@ will be returned.
notElem :: Hashable a => a -> Bloom a -> Bool
notElem elt ub = any test (hashes ub elt)
  where test idx' =
          let !idx = fromIntegral idx' .&. mask ub :: Int
          in not (unBit (bitArray ub UV.! idx))

-- | Return the size of an immutable Bloom filter, in bits.
length :: Bloom a -> Int
length = unsafeShiftL 1 . shift

-- | Build an immutable Bloom filter from a seed value.  The seeding
-- function populates the filter as follows.
--
--   * If it returns 'Nothing', it is finished producing values to
--     insert into the filter.
--
--   * If it returns @'Just' (a,b)@, @a@ is added to the filter and
--     @b@ is used as a new seed.
unfold :: forall a b. Hashable a
        => Int                       -- ^ number of hash functions to use
        -> Int                       -- ^ number of bits in filter
        -> (b -> Maybe (a, b))       -- ^ seeding function
        -> b                         -- ^ initial seed
        -> Bloom a
{-# INLINE unfold #-}
unfold hs numBits f k = create hs numBits (loop k)
  where loop :: forall s. b -> MBloom s a -> ST s ()
        loop j mb = case f j of
                      Just (a, j') -> MB.insert mb a >> loop j' mb
                      _ -> return ()

-- | Create an immutable Bloom filter, populating it from a list of
-- values.
--
-- Here is an example that uses the @cheapHashes@ function from the
-- "Data.BloomFilter.Hash" module to create a hash function that
-- returns three hashes.
--
-- @
--import "Data.BloomFilter.Hash" (cheapHashes)
--
--filt = fromList 3 1024 [\"foo\", \"bar\", \"quux\"]
-- @
fromList :: Hashable a
          => Int                -- ^ number of hash functions to use
          -> Int                -- ^ number of bits in filter
          -> [a]                -- ^ values to populate with
          -> Bloom a
{-# INLINE [1] fromList #-}
fromList hs numBits list = create hs numBits $ forM_ list . MB.insert

{-# RULES "Bloom insertList . fromList" forall h n xs ys.
    insertList xs (fromList h n ys) = fromList h n (xs ++ ys)
  #-}

{-
-- This is a simpler definition, but GHC doesn't inline the unfold
-- sensibly.

fromList hashes numBits = unfold hashes numBits convert
  where convert (x:xs) = Just (x, xs)
        convert _      = Nothing
-}

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
-- This module provides immutable interfaces for working with a
-- query-only Bloom filter, and for converting to and from mutable
-- Bloom filters.
--
-- For a higher-level interface that is easy to use, see the
-- 'Data.BloomFilter.Easy' module.

-- $performance
--
-- The implementation has been carefully tuned for high performance
-- and low space consumption.
--
-- For efficiency, the number of bits requested when creating a Bloom
-- filter is rounded up to the nearest power of two.  This lets the
-- implementation use bitwise operations internally, instead of much
-- more expensive multiplication, division, and modulus operations.
