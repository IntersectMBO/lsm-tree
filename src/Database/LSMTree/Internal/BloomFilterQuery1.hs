{-# LANGUAGE CPP             #-}
{-# LANGUAGE MagicHash       #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE UnboxedTuples   #-}
{-# OPTIONS_HADDOCK not-home #-}

module Database.LSMTree.Internal.BloomFilterQuery1 (
  bloomQueries,
  RunIxKeyIx(RunIxKeyIx),
  RunIx, KeyIx,
) where

import           Data.Bits
import qualified Data.Primitive as P
import qualified Data.Vector as V
import qualified Data.Vector.Primitive as VP
import qualified Data.Vector.Primitive.Mutable as VPM
import           Data.Word (Word32)

import           Control.Exception (assert)
import           Control.Monad.ST (ST)

import           Data.BloomFilter (Bloom)
import qualified Data.BloomFilter as Bloom
import qualified Data.BloomFilter.Hash as Bloom

import           Database.LSMTree.Internal.Serialise (SerialisedKey)


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
-- vector of runs and vector of keys respectively.
--
-- The result vector can be of variable length. The initial estimate is 2x the
-- number of keys but this is grown if needed (using a doubling strategy).
--
bloomQueries ::
     V.Vector (Bloom SerialisedKey)
  -> V.Vector SerialisedKey
  -> VP.Vector RunIxKeyIx
bloomQueries !blooms !ks
  | rsN == 0 || ksN == 0 = VP.empty
  | otherwise            = VP.create $ do
      res <- VPM.unsafeNew (V.length ks * 2)
      loop1 res 0 0
  where
    !rsN = V.length blooms
    !ksN = V.length ks

    hs :: VP.Vector (Bloom.CheapHashes SerialisedKey)
    !hs  = VP.generate ksN $ \i -> Bloom.makeHashes (V.unsafeIndex ks i)

    -- Loop over all run indexes
    loop1 ::
         VPM.MVector s RunIxKeyIx
      -> ResIx
      -> RunIx
      -> ST s (VPM.MVector s RunIxKeyIx)
    loop1 !res1 !resix1 !rix
      | rix == rsN = pure $ VPM.slice 0 resix1 res1
      | otherwise
      = do
          (res1', resix1') <- loop2 res1 resix1 0 (blooms `V.unsafeIndex` rix)
          loop1 res1' resix1' (rix+1)
      where
        -- Loop over all key indexes
        loop2 ::
             VPM.MVector s RunIxKeyIx
          -> ResIx
          -> KeyIx
          -> Bloom SerialisedKey
          -> ST s (VPM.MVector s RunIxKeyIx, ResIx)
        loop2 !res2 !resix2 !kix !b
          | kix == ksN = pure (res2, resix2)
          | let !h = hs `VP.unsafeIndex` kix
          , Bloom.elemHashes h b = do
              -- Double the vector if we've reached the end.
              -- Note unsafeGrow takes the number to grow by, not the new size.
              res2' <- if resix2 == VPM.length res2
                        then VPM.unsafeGrow res2 (VPM.length res2)
                        else pure res2
              VPM.unsafeWrite res2' resix2 (RunIxKeyIx rix kix)
              loop2 res2' (resix2+1) (kix+1) b
          | otherwise = loop2 res2 resix2 (kix+1) b
