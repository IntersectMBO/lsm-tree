{-# LANGUAGE BangPatterns             #-}
{-# LANGUAGE Rank2Types               #-}
{-# LANGUAGE RoleAnnotations          #-}
{-# LANGUAGE ScopedTypeVariables      #-}
{-# LANGUAGE StandaloneKindSignatures #-}
{-# LANGUAGE TypeApplications         #-}
{-# LANGUAGE TypeOperators            #-}
-- |
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

module Data.BloomFilter (
    -- * Overview
    -- $overview

    -- ** Ease of use
    -- $ease

    -- ** Performance
    -- $performance

    -- ** Differences from bloomfilter package
    -- $differences

    -- * Types
    Hash,
    Bloom,
    MBloom,
    Bloom',
    MBloom',
    CheapHashes,
    RealHashes,

    -- * Immutable Bloom filters

    -- ** Conversion
    freeze,
    thaw,
    unsafeFreeze,

    -- ** Creation
    unfold,

    fromList,
    empty,
    singleton,

    -- ** Accessors
    length,
    elem,
    elemHashes,
    notElem,
) where

import           Control.Monad (forM_, liftM)
import           Control.Monad.ST (ST, runST)
import           Data.BloomFilter.Hash (CheapHashes, Hash, Hashable,
                     Hashes (..), RealHashes)
import           Data.BloomFilter.Internal (Bloom' (..))
import           Data.BloomFilter.Mutable (MBloom, MBloom', insert, new)
import qualified Data.BloomFilter.Mutable.Internal as MB
import           Data.Word (Word64)

import           Prelude hiding (elem, length, notElem)

import qualified Data.BloomFilter.BitVec64 as V

-- | Bloom filter using 'CheapHashes' hashing scheme.
type Bloom = Bloom' CheapHashes

-- | Create an immutable Bloom filter, using the given setup function
-- which executes in the 'ST' monad.
--
-- Example:
--
-- @
-- TODO
--import "Data.BloomFilter.Hash" (cheapHashes)
--
--filter = create (cheapHashes 3) 1024 $ \mf -> do
--           insertMB mf \"foo\"
--           insertMB mf \"bar\"
-- @
--
-- Note that the result of the setup function is not used.
create :: Int        -- ^ number of hash functions to use
        -> Word64                 -- ^ number of bits in filter
        -> (forall s. (MBloom' s h a -> ST s ()))  -- ^ setup function
        -> Bloom' h a
{-# INLINE create #-}
create hash numBits body = runST $ do
    mb <- new hash numBits
    body mb
    unsafeFreeze mb

-- | Create an immutable Bloom filter from a mutable one.  The mutable
-- filter may be modified afterwards.
freeze :: MBloom' s h a -> ST s (Bloom' h a)
freeze mb = Bloom (MB.hashesN mb) (MB.size mb) `liftM`
            V.freeze (MB.bitArray mb)

-- | Create an immutable Bloom filter from a mutable one.  The mutable
-- filter /must not/ be modified afterwards, or a runtime crash may
-- occur.  For a safer creation interface, use 'freeze' or 'create'.
unsafeFreeze :: MBloom' s h a -> ST s (Bloom' h a)
unsafeFreeze mb = Bloom (MB.hashesN mb) (MB.size mb) `liftM`
                    V.unsafeFreeze (MB.bitArray mb)

-- | Copy an immutable Bloom filter to create a mutable one.  There is
-- no non-copying equivalent.
thaw :: Bloom' h a -> ST s (MBloom' s h a)
thaw ub = MB.MBloom (hashesN ub) (size ub) `liftM` V.thaw (bitArray ub)

-- | Create an empty Bloom filter.
empty :: Int                    -- ^ number of hash functions to use
      -> Word64                 -- ^ number of bits in filter
      -> Bloom' h a
{-# INLINE [1] empty #-}
empty hash numBits = create hash numBits (\_ -> return ())

-- | Create a Bloom filter with a single element.
singleton :: (Hashes h, Hashable a)
          => Int               -- ^ number of hash functions to use
          -> Word64            -- ^ number of bits in filter
          -> a                 -- ^ element to insert
          -> Bloom' h a
singleton hash numBits elt = create hash numBits (\mb -> insert mb elt)

-- | Query an immutable Bloom filter for membership.  If the value is
-- present, return @True@.  If the value is not present, there is
-- /still/ some possibility that @True@ will be returned.
elem :: (Hashes h, Hashable a) => a -> Bloom' h a -> Bool
elem elt ub = elemHashes (makeHashes elt) ub
{-# SPECIALIZE elem :: Hashable a => a -> Bloom a -> Bool #-}

-- | Query an immutable Bloom filter for membership using already constructed 'Hashes' value.
elemHashes :: Hashes h => h a -> Bloom' h a -> Bool
elemHashes !ch !ub = go 0 where
    go :: Int -> Bool
    go !i | i >= hashesN ub = True
          | otherwise       = let !idx' = evalHashes ch i in
                              let !idx = idx' `rem` size ub in
                              if V.unsafeIndex (bitArray ub) idx
                              then go (i + 1)
                              else False
{-# SPECIALIZE elemHashes :: CheapHashes a -> Bloom a -> Bool #-}

-- | Query an immutable Bloom filter for non-membership.  If the value
-- /is/ present, return @False@.  If the value is not present, there
-- is /still/ some possibility that @False@ will be returned.
notElem :: (Hashes h, Hashable a) => a -> Bloom' h a -> Bool
notElem elt ub = notElemHashes (makeHashes elt) ub

-- | Query an immutable Bloom filter for non-membership using already constructed 'Hashes' value.
notElemHashes :: Hashes h => h a -> Bloom' h a -> Bool
notElemHashes !ch !ub = not (elemHashes ch ub)

-- | Return the size of an immutable Bloom filter, in bits.
length :: Bloom' h a -> Word64
length = size

-- | Build an immutable Bloom filter from a seed value.  The seeding
-- function populates the filter as follows.
--
--   * If it returns 'Nothing', it is finished producing values to
--     insert into the filter.
--
--   * If it returns @'Just' (a,b)@, @a@ is added to the filter and
--     @b@ is used as a new seed.
unfold :: forall a b h. (Hashes h, Hashable a)
       => Int                       -- ^ number of hash functions to use
       -> Word64                    -- ^ number of bits in filter
       -> (b -> Maybe (a, b))       -- ^ seeding function
       -> b                         -- ^ initial seed
       -> Bloom' h a
{-# INLINE unfold #-}
unfold hs numBits f k = create hs numBits (loop k)
  where loop :: forall s. b -> MBloom' s h a -> ST s ()
        loop j mb = case f j of
                      Just (a, j') -> insert mb a >> loop j' mb
                      _            -> return ()

-- | Create an immutable Bloom filter, populating it from a list of
-- values.
--
-- Here is an example that uses the @cheapHashes@ function from the
-- "Data.BloomFilter.Hash" module to create a hash function that
-- returns three hashes.
--
-- @
-- filt = fromList 3 1024 [\"foo\", \"bar\", \"quux\"]
-- @
fromList :: (Hashes h, Hashable a)
         => Int                -- ^ number of hash functions to use
         -> Word64             -- ^ number of bits in filter
         -> [a]                -- ^ values to populate with
         -> Bloom' h a
fromList hs numBits list = create hs numBits $ forM_ list . insert

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
-- This module provides immutable interfaces for working with a
-- query-only Bloom filter, and for converting to and from mutable
-- Bloom filters.
--
-- For a higher-level interface that is easy to use, see the
-- "Data.BloomFilter.Easy" module.

-- $performance
--
-- The implementation has been carefully tuned for high performance
-- and low space consumption.

-- $differences
--
-- This package is (almost entirely rewritten) fork of
-- [bloomfilter](https://hackage.haskell.org/package/bloomfilter) package.
--
-- The main differences are
--
-- * This packages support bloomfilters of arbitrary sizes
--   (not limited to powers of two). Also sizes over 2^32 are supported.
--
-- * The 'Bloom' and 'MBloom' types are parametrised over 'Hashes' variable,
--   instead of having a @a -> ['Hash']@ typed field.
--   This separation allows clean de/serialization of Bloom filters in this
--   package, as the hashing scheme is a static.
--
-- * [XXH3 hash](https://xxhash.com/) is used instead of Jenkins'
--   lookup3.
