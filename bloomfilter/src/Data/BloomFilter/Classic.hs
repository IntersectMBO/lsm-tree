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

    -- ** Example: a spell checker
    -- $example

    -- ** Differences from bloomfilter package
    -- $differences

    -- * Types
    Hash,
    CheapHashes,

    -- * Immutable Bloom filters
    Bloom,

    -- ** Creation
    create,
    unfold,
    fromList,
    deserialise,

    -- ** Sizes
    NumEntries,
    BloomSize (..),
    FPR,
    sizeForFPR,
    BitsPerEntry,
    sizeForBits,
    sizeForPolicy,
    BloomPolicy (..),
    policyFPR,
    policyForFPR,
    policyForBits,

    -- ** Accessors
    size,
    elem,
    elemHashes,
    notElem,
    serialise,

    -- * Mutable Bloom filters
    MBloom,
    new,
    insert,

    -- ** Conversion
    freeze,
    thaw,
    unsafeFreeze,
) where

import           Control.Exception (assert)
import           Control.Monad.Primitive (PrimMonad, PrimState, RealWorld,
                     stToPrim)
import           Control.Monad.ST (ST, runST)
import           Data.Primitive.ByteArray (ByteArray, MutableByteArray)
import           Data.Word (Word64)

import qualified Data.BloomFilter.Classic.BitVec64 as V
import           Data.BloomFilter.Classic.Calc
import           Data.BloomFilter.Classic.Internal (Bloom (..), bloomInvariant)
import           Data.BloomFilter.Classic.Mutable (MBloom (..), insert, new)
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
--filter = create (sizeForBits 16 2) $ \mf -> do
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
      mb <- new bloomsize
      body mb
      unsafeFreeze mb

-- | Create an immutable Bloom filter from a mutable one.  The mutable
-- filter may be modified afterwards.
freeze :: MBloom s a -> ST s (Bloom a)
freeze MBloom { mbNumBits, mbNumHashes, mbBitArray } = do
    bitArray <- V.freeze mbBitArray
    let !bf = Bloom {
                numBits   = mbNumBits,
                numHashes = mbNumHashes,
                bitArray
              }
    assert (bloomInvariant bf) $ pure bf

-- | Create an immutable Bloom filter from a mutable one.  The mutable
-- filter /must not/ be modified afterwards, or a runtime crash may
-- occur.  For a safer creation interface, use 'freeze' or 'create'.
unsafeFreeze :: MBloom s a -> ST s (Bloom a)
unsafeFreeze MBloom { mbNumBits, mbNumHashes, mbBitArray } = do
    bitArray <- V.unsafeFreeze mbBitArray
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
    mbBitArray <- V.thaw bitArray
    pure MBloom {
      mbNumBits   = numBits,
      mbNumHashes = numHashes,
      mbBitArray
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
                      Just (a, j') -> insert mb a >> loop j' mb
                      _            -> return ()


{-# INLINEABLE fromList #-}
-- | Create a Bloom filter, populating it from a sequence of values.
--
-- For example
--
-- @
-- filt = fromList (policyForBits 10) [\"foo\", \"bar\", \"quux\"]
-- @
fromList :: (Foldable t, Hashable a)
         => BloomPolicy
         -> t a -- ^ values to populate with
         -> Bloom a
fromList policy xs =
    create bsize (\b -> mapM_ (insert b) xs)
  where
    bsize = sizeForPolicy policy (length xs)

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
    mbloom <- stToPrim $ new bloomsize
    MB.deserialise mbloom fill
    stToPrim $ unsafeFreeze mbloom

-- $overview
--
-- Each of the functions for creating Bloom filters accepts a 'BloomSize'. The
-- size determines the number of bits that should be used for the filter. Note
-- that a filter is fixed in size; it cannot be resized after creation.
--
-- The size can be specified by asking for a target false positive rate (FPR)
-- or a number of bits per element, and the number of elements in the filter.
-- For example:
--
-- * @'sizeForFPR' 1e-3 10_000@ for a Bloom filter sized for 10,000 elements
--   with a false positive rate of 1 in 1000
--
-- * @'sizeForBits' 10 10_000@ for a Bloom filter sized for 10,000 elements
--   with 10 bits per element
--
-- Depending on the application it may be more important to target a fixed
-- amount of memory to use, or target a specific FPR.
--
-- As a very rough guide for filter sizes, here are a range of FPRs and bits
-- per element:
--
-- * FPR of 1e-1 requires approximately 4.8 bits per element
-- * FPR of 1e-2 requires approximately 9.6 bits per element
-- * FPR of 1e-3 requires approximately 14.4 bits per element
-- * FPR of 1e-4 requires approximately 19.2 bits per element
-- * FPR of 1e-5 requires approximately 24.0 bits per element
--

-- $example
--
-- This example reads a dictionary file containing one word per line,
-- constructs a Bloom filter with a 1% false positive rate, and
-- spellchecks its standard input.  Like the Unix @spell@ command, it
-- prints each word that it does not recognize.
--
-- @
-- import Data.Maybe (mapMaybe)
-- import qualified Data.BloomFilter as B
--
-- main = do
--   filt \<- B.fromList (B.policyForFPR 0.01) . words \<$> readFile "\/usr\/share\/dict\/words"
--   let check word | B.elem word filt  = Nothing
--                  | otherwise         = Just word
--   interact (unlines . mapMaybe check . lines)
-- @

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
