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

module Data.BloomFilter.Blocked (
    -- * Types
    Hash,
    Hashable,

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
    notElem,
    (?),
    serialise,

    -- * Mutable Bloom filters
    MBloom,
    new,
    insert,

    -- ** Conversion
    freeze,
    thaw,
    unsafeFreeze,

    -- * Low level variants
    Hashes,
    hashes,
    insertHashes,
    elemHashes,
    -- ** Prefetching
    prefetchInsert,
    prefetchElem,
) where

import           Control.Monad.Primitive (PrimMonad, PrimState, RealWorld,
                     stToPrim)
import           Control.Monad.ST (ST, runST)
import           Data.Primitive.ByteArray (MutableByteArray)

import           Data.BloomFilter.Blocked.Calc
import           Data.BloomFilter.Blocked.Internal hiding (deserialise)
import qualified Data.BloomFilter.Blocked.Internal as Internal
import           Data.BloomFilter.Hash

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

{-# INLINEABLE insert #-}
-- | Insert a value into a mutable Bloom filter.  Afterwards, a
-- membership query for the same value is guaranteed to return @True@.
insert :: Hashable a => MBloom s a -> a -> ST s ()
insert = \ !mb !x -> insertHashes mb (hashes x)

{-# INLINE elem #-}
-- | Query an immutable Bloom filter for membership.  If the value is
-- present, return @True@.  If the value is not present, there is
-- /still/ some possibility that @True@ will be returned.
elem :: Hashable a => a -> Bloom a -> Bool
elem = \ !x !b -> elemHashes b (hashes x)

-- | Same as 'elem' but with the opposite argument order:
--
-- > x `elem` bfilter
--
-- versus
--
-- > bfilter ? x
--
(?) :: Hashable a => Bloom a -> a -> Bool
(?) = flip elem

{-# INLINE notElem #-}
-- | Query an immutable Bloom filter for non-membership.  If the value
-- /is/ present, return @False@.  If the value is not present, there
-- is /still/ some possibility that @False@ will be returned.
notElem :: Hashable a => a -> Bloom a -> Bool
notElem = \x b -> not (x `elem` b)

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
unfold bloomsize f k =
    create bloomsize body
  where
    body :: forall s. MBloom s a -> ST s ()
    body mb = loop k
      where
        loop :: b -> ST s ()
        loop !j = case f j of
                    Nothing      -> return ()
                    Just (a, j') -> insert mb a >> loop j'

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

{-# SPECIALISE deserialise :: BloomSize
                           -> (MutableByteArray RealWorld -> Int -> Int -> IO ())
                           -> IO (Bloom a) #-}
deserialise :: PrimMonad m
            => BloomSize
            -> (MutableByteArray (PrimState m) -> Int -> Int -> m ())
            -> m (Bloom a)
deserialise bloomsize fill = do
    mbloom <- stToPrim $ new bloomsize
    Internal.deserialise mbloom fill
    stToPrim $ unsafeFreeze mbloom

