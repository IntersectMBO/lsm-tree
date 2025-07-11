-- | A fast, space efficient Bloom filter implementation. A Bloom filter is a
-- set-like data structure that provides a probabilistic membership test.
--
-- * Queries do not give false negatives. When an element is added to a filter,
--   a subsequent membership test will definitely return 'True'.
--
-- * False positives /are/ possible. If an element has not been added to a
--   filter, a membership test /may/ nevertheless indicate that the element is
--   present.
--
module Data.BloomFilter.Classic (
    -- * Overview
    -- $overview

    -- * Types
    Hash,
    Salt,
    Hashable,

    -- * Immutable Bloom filters
    Bloom,

    -- ** Creation
    create,
    unfold,
    fromList,

    -- ** (De)Serialisation
    formatVersion,
    serialise,
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

    -- * Mutable Bloom filters
    MBloom,
    new,
    maxSizeBits,
    insert,
    read,

    -- ** Conversion
    freeze,
    thaw,
    unsafeFreeze,

    -- * Low level variants
    Hashes,
    hashesWithSalt,
    insertHashes,
    elemHashes,
    readHashes,
) where

import           Control.Monad.Primitive (PrimMonad, PrimState, RealWorld,
                     stToPrim)
import           Control.Monad.ST (ST, runST)
import           Data.Primitive.ByteArray (MutableByteArray)

import           Data.BloomFilter.Classic.Calc
import           Data.BloomFilter.Classic.Internal hiding (deserialise)
import qualified Data.BloomFilter.Classic.Internal as Internal
import           Data.BloomFilter.Hash

import           Prelude hiding (elem, notElem, read)

-- $setup
--
-- >>> import Text.Printf

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
-- >>> fmap (printf "%0.1f" . policyBits . policyForFPR) [1e-1, 1e-2, 1e-3, 1e-4, 1e-5] :: [String]
-- ["4.8","9.6","14.4","19.2","24.0"]

-- | Create an immutable Bloom filter, using the given setup function
-- which executes in the 'ST' monad.
--
-- Example:
--
-- >>> :{
-- filter = create (sizeForBits 16 2) 4 $ \mf -> do
--  insert mf "foo"
--  insert mf "bar"
-- :}
--
-- Note that the result of the setup function is not used.
create :: BloomSize
       -> Salt
       -> (forall s. (MBloom s a -> ST s ()))  -- ^ setup function
       -> Bloom a
{-# INLINE create #-}
create bloomsize bloomsalt body =
    runST $ do
      mb <- new bloomsize bloomsalt
      body mb
      unsafeFreeze mb

-- | Insert a value into a mutable Bloom filter.  Afterwards, a
-- membership query for the same value is guaranteed to return @True@.
insert :: Hashable a => MBloom s a -> a -> ST s ()
insert !mb !x = insertHashes mb (hashesWithSalt (mbHashSalt mb) x)

-- | Query an immutable Bloom filter for membership.  If the value is
-- present, return @True@.  If the value is not present, there is
-- /still/ some possibility that @True@ will be returned.
elem :: Hashable a => a -> Bloom a -> Bool
elem = \ !x !b -> elemHashes b (hashesWithSalt (hashSalt b) x)

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

-- | Query an immutable Bloom filter for non-membership.  If the value
-- /is/ present, return @False@.  If the value is not present, there
-- is /still/ some possibility that @False@ will be returned.
notElem :: Hashable a => a -> Bloom a -> Bool
notElem = \ x b -> not (x `elem` b)

-- | Query a mutable Bloom filter for membership.  If the value is
-- present, return @True@.  If the value is not present, there is
-- /still/ some possibility that @True@ will be returned.
read :: Hashable a => MBloom s a -> a -> ST s Bool
read !mb !x = readHashes mb (hashesWithSalt (mbHashSalt mb) x)

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
       -> Salt
       -> (b -> Maybe (a, b))       -- ^ seeding function
       -> b                         -- ^ initial seed
       -> Bloom a
{-# INLINE unfold #-}
unfold bloomsize bloomsalt f k =
    create bloomsize bloomsalt body
  where
    body :: forall s. MBloom s a -> ST s ()
    body mb = loop k
      where
        loop :: b -> ST s ()
        loop !j = case f j of
                    Nothing      -> pure ()
                    Just (a, j') -> insert mb a >> loop j'

{-# INLINEABLE fromList #-}
-- | Create a Bloom filter, populating it from a sequence of values.
--
-- For example
--
-- @
-- filt = fromList (policyForBits 10) 4 [\"foo\", \"bar\", \"quux\"]
-- @
fromList :: (Foldable t, Hashable a)
         => BloomPolicy
         -> Salt
         -> t a -- ^ values to populate with
         -> Bloom a
fromList policy bsalt xs =
    create bsize bsalt (\b -> mapM_ (insert b) xs)
  where
    bsize = sizeForPolicy policy (length xs)

{-# SPECIALISE deserialise ::
     BloomSize
  -> Salt
  -> (MutableByteArray RealWorld -> Int -> Int -> IO ())
  -> IO (Bloom a) #-}
deserialise :: PrimMonad m
            => BloomSize
            -> Salt
            -> (MutableByteArray (PrimState m) -> Int -> Int -> m ())
            -> m (Bloom a)
deserialise bloomsalt bloomsize fill = do
    mbloom <- stToPrim $ new bloomsalt bloomsize
    Internal.deserialise mbloom fill
    stToPrim $ unsafeFreeze mbloom
