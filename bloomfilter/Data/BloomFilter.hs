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
    , Bloom (..)
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
    , elemCheapHashes
    , elemCheapHashes'
    , notElem
    , elemMany
    , elemMany'
    , elemMany''
    , elemMany'''

    -- ** Modification
    , insert
    , insertList

    -- * The underlying representation
    -- | If you serialize the raw bit arrays below to disk, do not
    -- expect them to be portable to systems with different
    -- conventions for endianness or word size.
    ) where

import Control.Monad (liftM, forM_)
import Control.Monad.ST (ST, runST)
import Control.DeepSeq (NFData(..))
import GHC.Conc.Sync (pseq)
import Data.Word (Word8)
import qualified Data.BloomFilter.Mutable as MB
import qualified Data.BloomFilter.Mutable.Internal as MB
import Data.BloomFilter.Mutable.Internal (Hash, MBloom)
import Data.BloomFilter.Hash (Hashable, CheapHashes, evalCheapHashes, makeCheapHashes)
import qualified Data.BloomFilter.Hash as Hash
import qualified Data.Vector as Vec
import qualified Data.Vector.Unboxed as UVec
import qualified Data.Vector.Unboxed.Mutable as MUVec

import qualified Data.Primitive.Array as P
import qualified Data.Primitive.PrimArray as P

import Prelude hiding (elem, length, notElem,
                       (/), (*), div, divMod, mod)

import qualified Data.BloomFilter.BitVec64 as V
import GHC.Base (remInt)

-- | An immutable Bloom filter, suitable for querying from pure code.
data Bloom a = B {
      hashesN  :: {-# UNPACK #-} !Int
    , size     :: {-# UNPACK #-} !Int   -- ^ Size in bits. This is a multiple of 64
    , bitArray :: {-# UNPACK #-} !V.BitVec64
    }
  deriving (Eq, Show)
type role Bloom nominal

hashes :: Hash.Hashable a => Bloom a -> a -> [Hash.Hash]
hashes ub = Hash.cheapHashes (hashesN ub)
{-# INLINE hashes #-}


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
freeze mb = B (MB.hashesN mb) (MB.size mb) `liftM`
            V.freeze (MB.bitArray mb)

-- | Create an immutable Bloom filter from a mutable one.  The mutable
-- filter /must not/ be modified afterwards, or a runtime crash may
-- occur.  For a safer creation interface, use 'freeze' or 'create'.
unsafeFreeze :: MBloom s a -> ST s (Bloom a)
unsafeFreeze mb = B (MB.hashesN mb) (MB.size mb) `liftM`
                    V.unsafeFreeze (MB.bitArray mb)

-- | Copy an immutable Bloom filter to create a mutable one.  There is
-- no non-copying equivalent.
thaw :: Bloom a -> ST s (MBloom s a)
thaw ub = MB.MB (hashesN ub) (size ub) `liftM` V.thaw (bitArray ub)

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
elem elt ub = elemCheapHashes (makeCheapHashes elt) ub

elemCheapHashes :: CheapHashes a -> Bloom a -> Bool
elemCheapHashes ch ub = go 0 where
  go :: Int -> Bool
  go !i | i >= hashesN ub = True
        | otherwise       = let !idx = fromIntegral (evalCheapHashes ch i)`rem` size ub :: Int
                            in if V.unsafeIndex (bitArray ub) idx
                               then go (i + 1)
                               else False

elemCheapHashes' :: CheapHashes a -> Bloom a -> Bool
elemCheapHashes' =
    \ch ub ->
      let !idx0 = fromIntegral (evalCheapHashes ch 0) `remInt` size ub
          !idx1 = fromIntegral (evalCheapHashes ch 1) `remInt` size ub
       in go ch ub 0 idx0 idx1
  where
    go !ch !ub !hn idx idx' =
      if V.unsafeIndex (bitArray ub) idx
        then if hn+1 >= hashesN ub
               then True
               else let !idx'' = fromIntegral (evalCheapHashes ch (hn+2)) `remInt` size ub
                     in go ch ub (hn+1) idx' idx''
        else False
      

-- | Query several Bloom filters for membership of a single key. The result
-- is equivalent to @map (elem k) bs@ but may be faster when used with many
-- filters by taking advantage of shared calculations.
--
elemMany :: Hashable a => a -> Vec.Vector (Bloom a) -> UVec.Vector Bool
elemMany !elt =
    \ubs -> let !ch = makeCheapHashes elt
--             in Vec.convert (Vec.map (elemCheapHashes ch) ubs)
             in prefetch ch ubs (Vec.length ubs - 1)
         `pseq` Vec.convert (Vec.map (elemCheapHashes ch) ubs)
  where
    prefetch _ch _ubs 0 = ()
    prefetch ch ubs n =
      let ub = ubs Vec.! n
          idx0, idx1 :: Int
          !idx0 = fromIntegral (evalCheapHashes ch 0) `rem` size ub
          !idx1 = fromIntegral (evalCheapHashes ch 1) `rem` size ub
       in       V.prefetchIndex (bitArray ub) idx0
          `seq` V.prefetchIndex (bitArray ub) idx1
          `seq` prefetch ch ubs (n-1)

elemMany' :: forall a.
              Hashable a
           => a
           -> Vec.Vector (Bloom a)
           -> UVec.Vector Bool
elemMany' = \ !elt !bv ->
    runST $ do
      let n = Vec.length bv
      biv <- MUVec.generate n id -- fill biv with [0..]
      rs  <- MUVec.new n
      let !ch = makeCheapHashes elt
          !hn = hashesN (bv Vec.! 0)
      go ch bv biv rs hn 0 n
      UVec.unsafeFreeze rs
  where
    go :: forall s.
          CheapHashes a
       -> Vec.Vector (Bloom a)
       -> MUVec.MVector s Int
       -> MUVec.MVector s Bool
       -> Int
       -> Int
       -> Int
       -> ST s ()
    go !ch !bv !biv !rs !hn !i !n
      | i == n = if hn == 0
                   then setRemainderTrue biv rs 0 n
                   else go ch bv biv rs (hn-1) 0 n

      | otherwise = do
          j <- MUVec.read biv i
          let !b     = bv Vec.! j
              !idx   = fromIntegral (evalCheapHashes ch 0) `rem` size b
              !probe = V.unsafeIndex (bitArray b) idx
          if probe
            then go ch bv biv rs hn (i+1) n
            else do
              -- write False to results array
              MUVec.write rs j False
              -- remove entry from remaining array
              let !n' = n - 1
              MUVec.read biv n' >>= MUVec.write biv i
              go ch bv biv rs hn i (n-1)

    setRemainderTrue ::
          MUVec.MVector s Int
       -> MUVec.MVector s Bool
       -> Int
       -> Int
       -> ST s ()
    setRemainderTrue !biv !rs !i !n
      | i == n    = return ()
      | otherwise = do
          j <- MUVec.read biv i
          MUVec.write rs j True
          setRemainderTrue biv rs (i+1) n


elemMany'' :: forall a.
              Hashable a
           => a
           -> P.Array (Bloom a)
           -> P.PrimArray Word8
elemMany'' = \ !elt !bv ->
    runST $ do
      let n = P.sizeofArray bv
      biv <- P.newPrimArray n
      sequence_ [ P.writePrimArray biv i i | i <- [0..n-1]]
      rs  <- P.newPrimArray n
      let !ch = makeCheapHashes elt
          !hn = hashesN (P.indexArray bv 0)
      go ch bv biv rs hn 0 n
      P.unsafeFreezePrimArray rs
  where
    go :: forall s.
          CheapHashes a
       -> P.Array (Bloom a)
       -> P.MutablePrimArray s Int
       -> P.MutablePrimArray s Word8
       -> Int
       -> Int
       -> Int
       -> ST s ()
    go !ch !bv !biv !rs !hn !i !n
      | i == n = if hn == 0
                   then setRemainderTrue biv rs 0 n
                   else go ch bv biv rs (hn-1) 0 n

      | otherwise = do
          j <- P.readPrimArray biv i
          let !b     = P.indexArray bv j
              !idx   = fromIntegral (evalCheapHashes ch 0) `rem` size b
              !probe = V.unsafeIndex (bitArray b) idx
          if probe
            then go ch bv biv rs hn (i+1) n
            else do
              -- write False to results array
              P.writePrimArray rs j 0
              -- remove entry from remaining array
              let !n' = n - 1
              P.readPrimArray biv n' >>= P.writePrimArray biv i
              go ch bv biv rs hn i (n-1)

    setRemainderTrue ::
          P.MutablePrimArray s Int
       -> P.MutablePrimArray s Word8
       -> Int
       -> Int
       -> ST s ()
    setRemainderTrue !biv !rs !i !n
      | i == n    = return ()
      | otherwise = do
          j <- P.readPrimArray biv i
          P.writePrimArray rs j 1
          setRemainderTrue biv rs (i+1) n
      
elemMany''' :: forall a.
                Hashable a
             => a
             -> P.Array (Bloom a)
             -> P.PrimArray Word8
elemMany''' = \ !elt !bv ->
    runST $ do
      let n = P.sizeofArray bv
      biv <- P.newPrimArray n
      sequence_ [ P.writePrimArray biv i i | i <- [0..n-1]]
      pp  <- P.newPrimArray n
      rs  <- P.newPrimArray n
      let !ch = makeCheapHashes elt

      prologue        ch bv    biv pp   n  0
      n' <- probeLoop ch bv rs biv pp 0 n  0
      epilogue              rs biv      n' 0

      P.unsafeFreezePrimArray rs
  where
    -- Set up biv, the current subset of filters,
    -- and fill in the initial probe points
    prologue ch bv biv pp n i 
      | i == n    = return ()
      | otherwise = do
          let !b     = P.indexArray bv i
              !idx   = fromIntegral (evalCheapHashes ch 0 `V.remWord32` fromIntegral (size b))
--              !idx   = fromIntegral (evalCheapHashes ch 0) `remInt` size b
          P.writePrimArray biv i i
          P.writePrimArray pp  i idx
          V.prefetchIndexST (bitArray b) idx
          prologue ch bv biv pp n (i+1)

    probeLoop :: forall s.
          CheapHashes a
       -> P.Array (Bloom a)          -- the actual filters, immutable
       -> P.MutablePrimArray s Word8 -- results array
       -> P.MutablePrimArray s Int   -- the current subset of filters
       -> P.MutablePrimArray s Int   -- probe points for current filters
       -> Int                        -- current hash number (increasing)
       -> Int                        -- size of current filter subset
       -> Int                        -- index into current filter subset
       -> ST s Int
    probeLoop !ch !bv !rs !biv !pp !hn !n !i
      | i == n = if hn == hashesN (P.indexArray bv 0)
                   then return n
                   else probeLoop ch bv rs biv pp (hn+1) n 0

      | otherwise = do
          idx <- P.readPrimArray pp i
          j   <- P.readPrimArray biv i
          let !b = P.indexArray bv j
          -- now probe the filter location (which was calculated and prefetched on the previous iteration or prologue)
          if V.unsafeIndex (bitArray b) idx
            then do
              -- fill in and prefetch the next probe point for the next iteration
              let !idx' = fromIntegral (evalCheapHashes ch (hn+1) `V.remWord32` fromIntegral (size b))
--              let !idx' = fromIntegral (evalCheapHashes ch (hn+1)) `remInt` size b
              V.prefetchIndexST (bitArray b) idx'
              P.writePrimArray pp i idx'
              probeLoop ch bv rs biv pp hn n (i+1)
            else do
              -- write False to results array
              P.writePrimArray rs j 0
              -- remove entry from remaining array
              let !n' = n - 1
              P.readPrimArray biv n' >>= P.writePrimArray biv i
              probeLoop ch bv rs biv pp hn (n-1) i

    -- Set any remaining results indexes to be True
    epilogue ::
          P.MutablePrimArray s Word8
       -> P.MutablePrimArray s Int
       -> Int
       -> Int
       -> ST s ()
    epilogue !rs !biv !n !i
      | i == n    = return ()
      | otherwise = do
          j <- P.readPrimArray biv i
          P.writePrimArray rs j 1
          epilogue rs biv n (i+1)

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
          let !idx = fromIntegral idx' `rem` size ub :: Int
          in not (V.unsafeIndex (bitArray ub) idx)

-- | Return the size of an immutable Bloom filter, in bits.
length :: Bloom a -> Int
length = size

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
