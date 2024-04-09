{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE DeriveFunctor       #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE ScopedTypeVariables #-}

module Database.LSMTree.Lookup (
    -- * Lookup preparation
    RunLookupView (..)
  , RunIx
  , KeyIx
  , prepLookups
  , bloomQueries
  , bloomQueriesDefault
  , indexSearches
  ) where

import           Control.Monad.Primitive
import           Control.Monad.ST.Strict
import           Data.Primitive.ByteArray
import qualified Data.Vector as V
import qualified Data.Vector.Mutable as VM
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import           Data.Word (Word32)
import           Database.LSMTree.Run.BloomFilter (Bloom)
import qualified Database.LSMTree.Run.BloomFilter as Bloom
import           Database.LSMTree.Run.Index.Compact (CompactIndex,
                     PageSpan (..))
import qualified Database.LSMTree.Run.Index.Compact as Index
import           Database.LSMTree.Serialise
import           System.FS.API (Handle)
import           System.FS.BlockIO.API

-- | View of a 'Run' with only the essentials for lookups.
data RunLookupView h = RunLookupView {
    rlvKOpsHandle :: !h
  , rlvBloom      :: !(Bloom SerialisedKey)
  , rlvIndex      :: !CompactIndex
  }
  deriving Functor

{-# SPECIALIZE prepLookups ::
         V.Vector (RunLookupView (Handle h))
      -> V.Vector SerialisedKey
      -> ST s (VU.Vector (RunIx, KeyIx), V.Vector (IOOp (ST s) h)) #-}
{-# SPECIALIZE prepLookups ::
         V.Vector (RunLookupView (Handle h))
      -> V.Vector SerialisedKey
      -> IO (VU.Vector (RunIx, KeyIx), V.Vector (IOOp IO h)) #-}
-- | Prepare disk lookups by doing bloom filter queries, index searches and
-- creating 'IOOp's. The result is a vector of 'IOOp's and a vector of indexes,
-- both of which are the same length. The indexes record the run and key
-- associated with each 'IOOp'.
prepLookups ::
     PrimMonad m
  => V.Vector (RunLookupView (Handle h))
  -> V.Vector SerialisedKey
  -> m (VU.Vector (RunIx, KeyIx), V.Vector (IOOp m h))
prepLookups rs ks = do
  let rkixs = bloomQueriesDefault rs ks
  ioops <- indexSearches rs ks rkixs
  pure (rkixs, ioops)

type KeyIx = Int
type RunIx = Int
type ResIx = Int -- Result index

-- | 'bloomQueries' with a default result vector size of @V.length ks * 2@.
--
-- The result vector can be of variable length, so we use a generous
-- estimate here, and we grow the vector if needed. TODO: tune the
-- starting estimate based on the expected true- and false-positives.
bloomQueriesDefault ::
     V.Vector (RunLookupView h)
  -> V.Vector SerialisedKey
  -> VU.Vector (RunIx, KeyIx)
bloomQueriesDefault rs ks = bloomQueries rs ks (fromIntegral $ V.length ks * 2)

-- | Perform a batch of bloom queries. The result is a tuple of indexes into the
-- vector of runs and vector of keys respectively.
--
-- Note: the result vector should be ephemeral, so do not retain it.
--
-- TODO: we consider it likely that we could implement a optimised, batched
-- version of bloom filter queries, which would largely replace this function.
bloomQueries ::
     V.Vector (RunLookupView h)
  -> V.Vector SerialisedKey
  -> Word32
  -> VU.Vector (RunIx, KeyIx)
bloomQueries !rs !ks !resN
  | rsN == 0 || ksN == 0 = VU.empty
  | otherwise            = VU.create $ do
      res <- VUM.unsafeNew (fromIntegral resN)
      loop1 res 0 0
  where
    !rsN = V.length rs
    !ksN = V.length ks

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
          (res1', resix1') <- loop2 res1 resix1 0 (rlvBloom $ rs `V.unsafeIndex` rix)
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
          | let !k = ks `V.unsafeIndex` kix
          , Bloom.elem k b = do
              -- Grows the vector if we've reached the end. TODO: tune how
              -- much much we grow the vector each time based on the
              -- expected true- and false-positives.
              res2' <- if resix2 == VUM.length res2
                        then VUM.unsafeGrow res2 ksN
                        else pure res2
              VUM.unsafeWrite res2' resix2 (rix, kix)
              loop2 res2' (resix2+1) (kix+1) b
          | otherwise = loop2 res2 resix2 (kix+1) b

{-# SPECIALIZE indexSearches ::
         V.Vector (RunLookupView (Handle h))
      -> V.Vector SerialisedKey
      -> VU.Vector (RunIx, KeyIx)
      -> ST s (V.Vector (IOOp (ST s) h)) #-}
{-# SPECIALIZE indexSearches ::
         V.Vector (RunLookupView (Handle h))
      -> V.Vector SerialisedKey
      -> VU.Vector (RunIx, KeyIx)
      -> IO (V.Vector (IOOp IO h)) #-}
-- | Perform a batch of fence pointer index searches, and create an 'IOOp' for
-- each search result. The resulting vector has the same length as the
-- @VU.Vector (RunIx, KeyIx)@ argument, because index searching always returns a
-- positive search result.
indexSearches ::
     forall m h. PrimMonad m
  => V.Vector (RunLookupView (Handle h))
  -> V.Vector SerialisedKey
  -> VU.Vector (RunIx, KeyIx) -- ^ Result of 'bloomQueries'
  -> m (V.Vector (IOOp m h))
indexSearches !rs !ks !rkixs = do
    -- The result vector has exactly the same length as @rkixs@.
    res <- VM.unsafeNew n
    loop res 0
    V.unsafeFreeze res
  where
    !n = VU.length rkixs

    -- Loop over all indexes in @rkixs@
    loop :: VM.MVector (PrimState m) (IOOp m h) -> Int -> m ()
    loop !res !i
      | i == n = pure ()
      | otherwise = do
          let (!rix, !kix) = rkixs `VU.unsafeIndex` i
              !r     = rs `V.unsafeIndex` rix
              !k     = ks `V.unsafeIndex` kix
              !pspan = Index.search k (rlvIndex r)
              !size  = Index.pageSpanSize pspan
          -- The current allocation strategy is to allocate a new pinned
          -- byte array for each 'IOOp'. One optimisation we are planning to
          -- do is to use a cache of re-usable buffers, in which case we
          -- decrease the GC load. TODO: re-usable buffers.
          !buf <- newPinnedByteArray (size * 4096)
          let !ioop = IOOpRead
                        (rlvKOpsHandle r)
                        (fromIntegral $ Index.unPageNo (pageSpanStart pspan) * 4096)
                        buf
                        0
                        (fromIntegral $ size * 4096)
          VM.unsafeWrite res i $! ioop
          loop res (i+1)
