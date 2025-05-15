{-# LANGUAGE MagicHash       #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE UnboxedTuples   #-}
{-# OPTIONS_HADDOCK not-home #-}

-- | An implementation of batched bloom filter query, optimised using memory
-- prefetch.
module Database.LSMTree.Internal.BloomFilterQuery1 (
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
    , fromIntegral (c .&. 0xfff)
    )
{-# INLINE unpackRunIxKeyIx #-}

instance Show RunIxKeyIx where
  showsPrec _ (RunIxKeyIx r k) =
    showString "RunIxKeyIx " . showsPrec 11 r
              . showChar ' ' . showsPrec 11 k

type ResIx = Int -- Result index

-- | Perform a batch of bloom queries. The result is a tuple of indexes into the
-- vector of runs and vector of keys respectively. The order of keys and
-- runs\/filters in the input is maintained in the output. This implementation
-- produces results in key-major order.
--
-- The result vector can be of variable length. The initial estimate is 2x the
-- number of keys but this is grown if needed (using a doubling strategy).
--
bloomQueries ::
     V.Vector (Bloom SerialisedKey)
  -> V.Vector SerialisedKey
  -> VP.Vector RunIxKeyIx
bloomQueries !filters !keys | V.null filters || V.null keys = VP.empty
bloomQueries !filters !keys =
    runST $ do
      res  <- P.newPrimArray (ksN * 2)
      res' <- loop1 res 0 0
      parr <- P.unsafeFreezePrimArray res'
      pure $! P.primArrayToPrimVector parr
  where
    !rsN = V.length filters
    !ksN = V.length keys
    !keyhashes = P.generatePrimArray (V.length keys) $ \i ->
                   Bloom.hashes (V.unsafeIndex keys i)

    -- loop over all filters
    loop1 ::
         P.MutablePrimArray s RunIxKeyIx
      -> ResIx
      -> RunIx
      -> ST s (P.MutablePrimArray s RunIxKeyIx)
    loop1 !res !resix !rix | rix == rsN = P.resizeMutablePrimArray res resix
    loop1 !res !resix !rix = do
        loop2_prefetch 0
        (res', resix') <- loop2 res resix 0
        loop1 res' resix' (rix+1)
      where
        !filter = V.unsafeIndex filters rix

        -- loop over all keys
        loop2 ::
             P.MutablePrimArray s RunIxKeyIx
          -> ResIx
          -> KeyIx
          -> ST s (P.MutablePrimArray s RunIxKeyIx, ResIx)
        loop2 !res2 !resix2 !kix
          | kix == ksN = pure (res2, resix2)
          | let !keyhash = P.indexPrimArray keyhashes kix
          , Bloom.elemHashes filter keyhash = do
              P.writePrimArray res2 resix2 (RunIxKeyIx rix kix)
              ressz2 <- P.getSizeofMutablePrimArray res2
              res2'  <- if resix2+1 < ressz2
                         then return res2
                         else P.resizeMutablePrimArray res2 (ressz2 * 2)
              loop2 res2' (resix2+1) (kix+1)

          | otherwise =
              loop2 res2 resix2 (kix+1)

        loop2_prefetch :: KeyIx -> ST s ()
        loop2_prefetch !kix
          | kix == ksN = pure ()
          | otherwise  = do
              let !keyhash = P.indexPrimArray keyhashes kix
              Bloom.prefetchElem filter keyhash
              loop2_prefetch (kix+1)
