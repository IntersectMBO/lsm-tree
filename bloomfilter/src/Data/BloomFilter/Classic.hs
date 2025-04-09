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
-- interface, see the "Data.BloomFilter.Classic.Easy" module.

module Data.BloomFilter.Classic (
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
    CheapHashes,
    BloomSize (..),

    -- * Immutable Bloom filters

    -- ** Conversion
    freeze,
    thaw,
    unsafeFreeze,

    -- ** Creation
    create,
    unfold,
    fromList,
    deserialise,

    -- ** Accessors
    size,
    elem,
    elemHashes,
    notElem,
    serialise,
) where

import           Control.Exception (assert)
import           Control.Monad (forM_)
import           Control.Monad.ST (ST, runST)
import           Control.Monad.Primitive (PrimMonad, PrimState, RealWorld,
                     stToPrim)
import           Data.Primitive.ByteArray (ByteArray, MutableByteArray)
import           Data.Word (Word64)

import qualified Data.BloomFilter.Classic.BitVec64 as V
import           Data.BloomFilter.Classic.Internal (Bloom (..), bloomInvariant)
import           Data.BloomFilter.Classic.Mutable (BloomSize (..), MBloom)
import qualified Data.BloomFilter.Classic.Mutable as MB
import           Data.BloomFilter.Hash (CheapHashes, Hash, Hashable, evalHashes,
                     makeHashes)

import           Prelude hiding (elem, notElem)

-- | Create an immutable Bloom filter, using the given setup function
-- which executes in the 'ST' monad.
--
-- Example:
--
-- @
--filter = create (BloomSize 1024 3) $ \mf -> do
--           insert mf \"foo\"
--           insert mf \"bar\"
-- @
--
-- Note that the result of the setup function is not used.
create :: BloomSize
       -> (forall s. (MBloom s a -> ST s ()))  -- ^ setup function
       -> Bloom a
{-# INLINE create #-}
create bloomsize body =
    runST $ do
      mb <- MB.new bloomsize
      body mb
      unsafeFreeze mb

-- | Create an immutable Bloom filter from a mutable one.  The mutable
-- filter may be modified afterwards.
freeze :: MBloom s a -> ST s (Bloom a)
freeze MB.MBloom { numBits, numHashes, bitArray } = do
    bitArray' <- V.freeze bitArray
    let !bf = Bloom {
                numHashes,
                numBits,
                bitArray = bitArray'
              }
    assert (bloomInvariant bf) $ pure bf

-- | Create an immutable Bloom filter from a mutable one.  The mutable
-- filter /must not/ be modified afterwards, or a runtime crash may
-- occur.  For a safer creation interface, use 'freeze' or 'create'.
unsafeFreeze :: MBloom s a -> ST s (Bloom a)
unsafeFreeze MB.MBloom { numBits, numHashes, bitArray } = do
    bitArray' <- V.unsafeFreeze bitArray
    let !bf = Bloom {
                numHashes,
                numBits,
                bitArray = bitArray'
              }
    assert (bloomInvariant bf) $ pure bf

-- | Copy an immutable Bloom filter to create a mutable one.  There is
-- no non-copying equivalent.
thaw :: Bloom a -> ST s (MBloom s a)
thaw Bloom { numBits, numHashes, bitArray } = do
    bitArray' <- V.thaw bitArray
    pure MB.MBloom {
      numBits,
      numHashes,
      bitArray = bitArray'
    }

-- | Query an immutable Bloom filter for membership.  If the value is
-- present, return @True@.  If the value is not present, there is
-- /still/ some possibility that @True@ will be returned.
elem :: Hashable a => a -> Bloom a -> Bool
elem elt ub = elemHashes (makeHashes elt) ub

-- | Query an immutable Bloom filter for membership using already constructed 'Hashes' value.
elemHashes :: CheapHashes a -> Bloom a -> Bool
elemHashes !ch Bloom { numBits, numHashes, bitArray } =
    go 0
  where
    go :: Int -> Bool
    go !i | i >= numHashes
          = True
    go !i = let idx' :: Word64
                !idx' = evalHashes ch i in
            let idx :: Int
                !idx = fromIntegral (idx' `V.unsafeRemWord64` fromIntegral numBits) in
            -- While the idx' can cover the full Word64 range,
            -- after taking the remainder, it now must fit in
            -- and Int because it's less than the filter size.
            if V.unsafeIndex bitArray idx
              then go (i + 1)
              else False

-- | Query an immutable Bloom filter for non-membership.  If the value
-- /is/ present, return @False@.  If the value is not present, there
-- is /still/ some possibility that @False@ will be returned.
notElem :: Hashable a => a -> Bloom a -> Bool
notElem elt ub = notElemHashes (makeHashes elt) ub

-- | Query an immutable Bloom filter for non-membership using already constructed 'Hashes' value.
notElemHashes :: CheapHashes a -> Bloom a -> Bool
notElemHashes !ch !ub = not (elemHashes ch ub)

-- | Return the size of the Bloom filter.
size :: Bloom a -> BloomSize
size Bloom { numBits, numHashes } =
    BloomSize {
      sizeBits   = numBits,
      sizeHashes = numHashes
    }

-- | Build an immutable Bloom filter from a seed value.  The seeding
-- function populates the filter as follows.
--
--   * If it returns 'Nothing', it is finished producing values to
--     insert into the filter.
--
--   * If it returns @'Just' (a,b)@, @a@ is added to the filter and
--     @b@ is used as a new seed.
unfold :: forall a b.
          Hashable a
       => BloomSize
       -> (b -> Maybe (a, b))       -- ^ seeding function
       -> b                         -- ^ initial seed
       -> Bloom a
{-# INLINE unfold #-}
unfold bloomsize f k = create bloomsize (loop k)
  where loop :: forall s. b -> MBloom s a -> ST s ()
        loop j mb = case f j of
                      Just (a, j') -> MB.insert mb a >> loop j' mb
                      _            -> return ()

-- | Create an immutable Bloom filter, populating it from a list of
-- values.
--
-- Here is an example that uses the @cheapHashes@ function from the
-- "Data.BloomFilter.Classic.Hash" module to create a hash function that
-- returns three hashes.
--
-- @
-- filt = fromList 3 1024 [\"foo\", \"bar\", \"quux\"]
-- @
fromList :: Hashable a
         => BloomSize
         -> [a]                -- ^ values to populate with
         -> Bloom a
fromList bloomsize list = create bloomsize $ forM_ list . MB.insert

serialise :: Bloom a -> (BloomSize, ByteArray, Int, Int)
serialise b@Bloom{bitArray} =
    (size b, ba, off, len)
  where
    (ba, off, len) = V.serialise bitArray

{-# SPECIALISE deserialise :: BloomSize
                           -> (MutableByteArray RealWorld -> Int -> Int -> IO ())
                           -> IO (Bloom a) #-}
deserialise :: PrimMonad m
            => BloomSize
            -> (MutableByteArray (PrimState m) -> Int -> Int -> m ())
            -> m (Bloom a)
deserialise bloomsize fill = do
    mbloom <- stToPrim $ MB.new bloomsize
    MB.deserialise mbloom fill
    stToPrim $ unsafeFreeze mbloom

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
-- The 'Data.BloomFilter.Classic.Easy.suggestSizing' function in
-- the "Data.BloomFilter.Classic.Easy" module calculates useful estimates for
-- these parameters.

-- $ease
--
-- This module provides immutable interfaces for working with a
-- query-only Bloom filter, and for converting to and from mutable
-- Bloom filters.
--
-- For a higher-level interface that is easy to use, see the
-- "Data.BloomFilter.Classic.Easy" module.

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
