-- | A fast, space efficient Bloom filter implementation.  A Bloom filter is a
-- set-like data structure that provides a probabilistic membership test.
--
-- * Queries do not give false negatives. When an element is added to a filter,
--   a subsequent membership test will definitely return 'True'.
--
-- * False positives /are/ possible. If an element has not been added to a
--   filter, a membership test /may/ nevertheless indicate that the element is
--   present.
--
module Data.BloomFilter.Blocked (
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
    insertMany,
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
    -- ** Prefetching
    prefetchInsert,
    prefetchElem,
) where

import           Control.Monad.Primitive (PrimMonad, PrimState, RealWorld,
                     stToPrim)
import           Control.Monad.ST (ST, runST)
import           Data.Bits ((.&.))
import           Data.Primitive.ByteArray (MutableByteArray)
import qualified Data.Primitive.PrimArray as P

import           Data.BloomFilter.Blocked.Calc (BitsPerEntry, BloomPolicy (..),
                     BloomSize (..), FPR, NumEntries, policyFPR, policyForBits,
                     policyForFPR, sizeForBits, sizeForFPR, sizeForPolicy)
import           Data.BloomFilter.Blocked.Internal hiding (deserialise)
import qualified Data.BloomFilter.Blocked.Internal as Internal
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
-- * FPR of 1e-2 requires approximately 9.8 bits per element
-- * FPR of 1e-3 requires approximately 15.8 bits per element
-- * FPR of 1e-4 requires approximately 22.6 bits per element
-- * FPR of 1e-5 requires approximately 30.2 bits per element
--
-- >>> fmap (printf "%0.1f" . policyBits . policyForFPR) [1e-1, 1e-2, 1e-3, 1e-4, 1e-5] :: [String]
-- ["4.8","9.8","15.8","22.6","30.2"]

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

{-# INLINEABLE insert #-}
-- | Insert a value into a mutable Bloom filter.  Afterwards, a
-- membership query for the same value is guaranteed to return @True@.
insert :: Hashable a => MBloom s a -> a -> ST s ()
insert = \ !mb !x -> insertHashes mb (hashesWithSalt (mbHashSalt mb) x)

{-# INLINE elem #-}
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

{-# INLINE notElem #-}
-- | Query an immutable Bloom filter for non-membership.  If the value
-- /is/ present, return @False@.  If the value is not present, there
-- is /still/ some possibility that @False@ will be returned.
notElem :: Hashable a => a -> Bloom a -> Bool
notElem = \x b -> not (x `elem` b)

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
-- filter = fromList (policyForBits 10) 4 [\"foo\", \"bar\", \"quux\"]
-- @
fromList :: (Foldable t, Hashable a)
         => BloomPolicy
         -> Salt
         -> t a -- ^ values to populate with
         -> Bloom a
fromList policy bloomsalt xs =
    create bsize bloomsalt (\b -> mapM_ (insert b) xs)
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
deserialise bloomsize bloomsalt fill = do
    mbloom <- stToPrim $ new bloomsize bloomsalt
    Internal.deserialise mbloom fill
    stToPrim $ unsafeFreeze mbloom


-----------------------------------------------------------
-- Bulk insert
--

{-# INLINABLE insertMany #-}
-- | A bulk insert of many elements.
--
-- This is somewhat faster than repeated insertion using 'insert'. It uses
-- memory prefetching to improve the utilisation of memory bandwidth. This has
-- greatest benefit for large filters (that do not fit in L3 cache) and for
-- inserting many elements, e.g. > 10.
--
-- To get best performance, you probably want to specialise this function to
-- the 'Hashable' instance and to the lookup action. It is marked @INLINABLE@
-- to help with this.
--
insertMany ::
     forall a s.
     Hashable a
  => MBloom s a
  -> (Int -> ST s a) -- ^ Action to lookup elements, indexed @0..n-1@
  -> Int             -- ^ @n@, number of elements to insert
  -> ST s ()
insertMany bloom key n =
    P.newPrimArray 0x10 >>= body
  where
    -- The general strategy is to use a rolling buffer @buf@ (of size 16). At
    -- the write end of the buffer, we prepare the probe locations and prefetch
    -- the corresponding cache line. At the read end, we do the hash insert.
    -- By having a prefetch distance of 15 between the write and read ends, we
    -- can have up to 15 memory reads in flight at once, thus improving
    -- utilisation of the memory bandwidth.
    body :: P.MutablePrimArray s (Hashes a) -> ST s ()
    body !buf = prepareProbes 0 0
      where
        -- Start by filling the buffer as far as we can, either to the end of
        -- the buffer or until we run out of elements.
        prepareProbes :: Int -> Int -> ST s ()
        prepareProbes !i !i_w
          | i_w < 0x0f && i < n = do
              k <- key i
              let !kh = hashesWithSalt (mbHashSalt bloom) k
              prefetchInsert bloom kh
              P.writePrimArray buf i_w kh
              prepareProbes (i+1) (i_w+1)

          | n > 0     = insertProbe 0 0 i_w
          | otherwise = pure ()

        -- Read from the read end of the buffer and do the inserts.
        insertProbe :: Int -> Int -> Int -> ST s ()
        insertProbe !i !i_r !i_w = do
            kh <- P.readPrimArray buf i_r
            insertHashes bloom kh
            nextProbe i i_r i_w

        -- Move on to the next entry.
        nextProbe :: Int -> Int -> Int -> ST s ()
        nextProbe !i !i_r !i_w
          -- If there are elements left, we prepare them and add them at the
          -- write end of the buffer, before inserting the next element
          -- (from the read end of the buffer).
          | i < n = do
              k <- key i
              let !kh = hashesWithSalt (mbHashSalt bloom) k
              prefetchInsert bloom kh
              P.writePrimArray buf i_w kh
              insertProbe
                (i+1)
                ((i_r + 1) .&. 0x0f)
                ((i_w + 1) .&. 0x0f)

          -- Or if there's no more elements to add to the buffer, but the
          -- buffer is still non-empty, we just loop draining the buffer.
          | ((i_r + 1) .&. 0x0f) /= i_w =
              insertProbe
                i
                ((i_r + 1) .&. 0x0f)
                i_w

          -- When the buffer is empty, we're done.
          | otherwise = pure ()
