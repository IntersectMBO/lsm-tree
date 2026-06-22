{-# LANGUAGE CPP             #-}
{-# LANGUAGE MagicHash       #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE UnboxedTuples   #-}
{-# OPTIONS_HADDOCK not-home #-}

module Database.LSMTree.Internal.BloomFilter (
  -- * Types
  Bloom.Bloom,
  Bloom.MBloom,
  Bloom.Salt,

  -- * Bulk query
  bloomQueries,
  RunIxKeyIx(RunIxKeyIx),
  RunIx, KeyIx,
) where

import           Data.Bits
import qualified Data.Primitive as P
import qualified Data.Vector as V
import qualified Data.Vector.Primitive as VP
import           Data.Word (Word32)

import           Control.Exception (assert)

import           Control.Monad.ST (ST, runST)

import           Data.BloomFilter.Blocked (Bloom)
import qualified Data.BloomFilter.Blocked as Bloom
import           Database.LSMTree.Internal.Serialise (SerialisedKey)
import qualified Database.LSMTree.Internal.Vector as P

#ifdef HAVE_STRICT_ARRAY
import qualified Database.LSMTree.Internal.StrictArray as P
#endif

import           Prelude hiding (filter)

-- Bulk query
-----------------------------------------------------------

type KeyIx = Int
type RunIx = Int

-- | A 'RunIxKeyIx' is a (compact) pair of a 'RunIx' and a 'KeyIx'.
--
-- We represent it as a 32bit word, using:
--
-- * 16 bits for the run\/filter index (MSB)
-- * 16 bits for the key index (LSB)
--
newtype RunIxKeyIx = MkRunIxKeyIx Word32
  deriving stock Eq
  deriving newtype P.Prim

pattern RunIxKeyIx :: RunIx -> KeyIx -> RunIxKeyIx
pattern RunIxKeyIx r k <- (unpackRunIxKeyIx -> (r, k))
  where
    RunIxKeyIx r k = packRunIxKeyIx r k
{-# INLINE RunIxKeyIx #-}
{-# COMPLETE RunIxKeyIx #-}

packRunIxKeyIx :: Int -> Int -> RunIxKeyIx
packRunIxKeyIx r k =
    assert (r >= 0 && r <= 0xffff
         && k >= 0 && k <= 0xffff) $
    MkRunIxKeyIx $
      (fromIntegral :: Word -> Word32) $
        (fromIntegral r `unsafeShiftL` 16)
     .|. fromIntegral k
{-# INLINE packRunIxKeyIx #-}

unpackRunIxKeyIx :: RunIxKeyIx -> (Int, Int)
unpackRunIxKeyIx (MkRunIxKeyIx c) =
    ( fromIntegral (c `unsafeShiftR` 16)
    , fromIntegral (c .&. 0xffff)
    )
{-# INLINE unpackRunIxKeyIx #-}

instance Show RunIxKeyIx where
  showsPrec _ (RunIxKeyIx r k) =
    showString "RunIxKeyIx " . showsPrec 11 r
              . showChar ' ' . showsPrec 11 k

type ResIx = Int -- Result index

{-# NOINLINE bloomQueries #-}
-- | Perform a batch of bloom queries. The result is a tuple of indexes into the
-- vector of runs and vector of keys respectively. The order of keys and
-- runs\/filters in the input is maintained in the output. This implementation
-- produces results in key-major order.
--
-- The result vector can be of variable length. The initial estimate is 2x the
-- number of keys but this is grown if needed (using a doubling strategy).
--
bloomQueries ::
     Bloom.Salt
  -> V.Vector (Bloom SerialisedKey)
  -> V.Vector SerialisedKey
  -> VP.Vector RunIxKeyIx
bloomQueries !_salt !filters !keys
  | V.null filters || V.null keys = VP.empty
bloomQueries !salt !filters !keys =
    runST (bloomQueries_loop1 filters' keyhashes)
  where
    filters'  = toFiltersArray filters
    keyhashes = P.generatePrimArray (V.length keys) $ \i ->
                  Bloom.hashesWithSalt salt (V.unsafeIndex keys i)

-- loop over all keys
bloomQueries_loop1 ::
     BloomFilters
  -> P.PrimArray (Bloom.Hashes SerialisedKey)
  -> ST s (VP.Vector RunIxKeyIx)
bloomQueries_loop1 !filters !keyhashes = do
    res <- P.newPrimArray (P.sizeofPrimArray keyhashes * 2)
    (res', resix') <- go res 0 0
    parr <- P.unsafeFreezePrimArray =<< P.resizeMutablePrimArray res' resix'
    pure $! P.primArrayToPrimVector parr
  where
    go !res !resix !kix
      | kix == P.sizeofPrimArray keyhashes = pure (res, resix)
      | otherwise = do
        let !keyhash = P.indexPrimArray keyhashes kix
        bloomQueries_loop2_prefetch filters keyhash 0
        (res', resix') <- bloomQueries_loop2 filters keyhash kix res resix 0
        go res' resix' (kix+1)

-- loop over all filters
bloomQueries_loop2 ::
     BloomFilters
  -> Bloom.Hashes SerialisedKey
  -> KeyIx
  -> P.MutablePrimArray s RunIxKeyIx
  -> ResIx
  -> RunIx
  -> ST s (P.MutablePrimArray s RunIxKeyIx, ResIx)
bloomQueries_loop2 !filters !keyhash !kix = go
  where
    go res2 resix2 rix
      | rix == lengthFiltersArray filters = pure (res2, resix2)
      | let !filter = indexFiltersArray filters rix
      , Bloom.elemHashes filter keyhash = do
          P.writePrimArray res2 resix2 (RunIxKeyIx rix kix)
          ressz2 <- P.getSizeofMutablePrimArray res2
          res2'  <- if resix2+1 < ressz2
                     then pure res2
                     else P.resizeMutablePrimArray res2 (ressz2 * 2)
          go res2' (resix2+1) (rix+1)

      | otherwise =
          go res2 resix2 (rix+1)

bloomQueries_loop2_prefetch ::
     BloomFilters
  -> Bloom.Hashes SerialisedKey
  -> RunIx
  -> ST s ()
bloomQueries_loop2_prefetch !filters !keyhash = go
  where
    go !rix
      | rix == lengthFiltersArray filters = pure ()
      | otherwise  = do
          let !filter = indexFiltersArray filters rix
          Bloom.prefetchElem filter keyhash
          go (rix+1)

type BloomFilters =
#ifdef HAVE_STRICT_ARRAY
  P.StrictArray (Bloom SerialisedKey)
#else
  V.Vector (Bloom SerialisedKey)
#endif

{-# INLINE toFiltersArray #-}
toFiltersArray :: V.Vector (Bloom SerialisedKey) -> BloomFilters

{-# INLINE indexFiltersArray #-}
indexFiltersArray :: BloomFilters -> Int -> Bloom SerialisedKey

{-# INLINE lengthFiltersArray #-}
lengthFiltersArray :: BloomFilters -> Int

#ifdef HAVE_STRICT_ARRAY
toFiltersArray     = P.vectorToStrictArray
indexFiltersArray  = P.indexStrictArray
lengthFiltersArray = P.sizeofStrictArray
#else
toFiltersArray     = id
indexFiltersArray  = V.unsafeIndex
lengthFiltersArray = V.length
#endif
