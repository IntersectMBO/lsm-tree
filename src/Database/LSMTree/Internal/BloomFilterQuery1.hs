module Database.LSMTree.Internal.BloomFilterQuery1 (
  bloomQueries,
  bloomQueriesDefault,
  RunIx, KeyIx,
) where

import           Data.BloomFilter (Bloom)
import qualified Data.BloomFilter as Bloom
import qualified Data.BloomFilter.Hash as Bloom
import qualified Data.Vector as V
import qualified Data.Vector.Primitive as VP
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import           Data.Word (Word32)

import           Control.Monad.ST (ST)

import           Database.LSMTree.Internal.Serialise (SerialisedKey)


-- Bulk query
-----------------------------------------------------------

type KeyIx = Int
type RunIx = Int

-- | 'bloomQueries' with a default result vector size of @V.length ks * 2@.
--
-- TODO: tune the starting estimate based on the expected true- and
-- false-positives.
bloomQueriesDefault ::
     V.Vector (Bloom SerialisedKey)
  -> V.Vector SerialisedKey
  -> VU.Vector (RunIx, KeyIx)
bloomQueriesDefault blooms ks = bloomQueries blooms ks (fromIntegral $ V.length ks * 2)

type ResIx = Int -- Result index

-- | Perform a batch of bloom queries. The result is a tuple of indexes into the
-- vector of runs and vector of keys respectively.
--
-- The result vector can be of variable length. An estimate should be provided,
-- and the vector is grown if needed.
--
-- TODO: we consider it likely that we could implement a optimised, batched
-- version of bloom filter queries, which would largely replace this function.
bloomQueries ::
     V.Vector (Bloom SerialisedKey)
  -> V.Vector SerialisedKey
  -> Word32
  -> VU.Vector (RunIx, KeyIx)
bloomQueries !blooms !ks !resN
  | rsN == 0 || ksN == 0 = VU.empty
  | otherwise            = VU.create $ do
      res <- VUM.unsafeNew (fromIntegral resN)
      loop1 res 0 0
  where
    !rsN = V.length blooms
    !ksN = V.length ks

    hs :: VP.Vector (Bloom.CheapHashes SerialisedKey)
    !hs  = VP.generate ksN $ \i -> Bloom.makeCheapHashes (V.unsafeIndex ks i)

    -- Loop over all run indexes
    loop1 ::
         VUM.MVector s (RunIx, KeyIx)
      -> ResIx
      -> RunIx
      -> ST s (VUM.MVector s (RunIx, KeyIx))
    loop1 !res1 !resix1 !rix
      | rix == rsN = pure $ VUM.slice 0 resix1 res1
      | otherwise
      = do
          (res1', resix1') <- loop2 res1 resix1 0 (blooms `V.unsafeIndex` rix)
          loop1 res1' resix1' (rix+1)
      where
        -- Loop over all key indexes
        loop2 ::
             VUM.MVector s (RunIx, KeyIx)
          -> ResIx
          -> KeyIx
          -> Bloom SerialisedKey
          -> ST s (VUM.MVector s (RunIx, KeyIx), ResIx)
        loop2 !res2 !resix2 !kix !b
          | kix == ksN = pure (res2, resix2)
          | let !h = hs `VP.unsafeIndex` kix
          , Bloom.elemHashes h b = do
              -- Grows the vector if we've reached the end.
              --
              -- TODO: tune how much much we grow the vector each time based on
              -- the expected true- and false-positives.
              res2' <- if resix2 == VUM.length res2
                        then VUM.unsafeGrow res2 ksN
                        else pure res2
              VUM.unsafeWrite res2' resix2 (rix, kix)
              loop2 res2' (resix2+1) (kix+1) b
          | otherwise = loop2 res2 resix2 (kix+1) b
