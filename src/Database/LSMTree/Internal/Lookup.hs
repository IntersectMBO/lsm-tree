{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE CPP                 #-}
{-# LANGUAGE DeriveAnyClass      #-}
{-# LANGUAGE DeriveFunctor       #-}
{-# LANGUAGE DerivingStrategies  #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE NamedFieldPuns      #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}

module Database.LSMTree.Internal.Lookup (
    ResolveSerialisedValue
  , ByteCountDiscrepancy (..)
  , lookupsIO
    -- * Internal: exposed for tests and benchmarks
  , RunIx
  , KeyIx
  , RunIxKeyIx(..)
  , prepLookups
  , bloomQueries
  , indexSearches
  , intraPageLookups
  ) where

import           Data.Arena (Arena, ArenaManager, allocateFromArena, withArena)
import           Data.Bifunctor
import           Data.BloomFilter (Bloom)
import           Data.Primitive.ByteArray
import qualified Data.Vector as V
import qualified Data.Vector.Mutable as VM
import qualified Data.Vector.Primitive as VP
import qualified Data.Vector.Unboxed as VU

import           Control.Exception (Exception, assert)
import           Control.Monad
import           Control.Monad.Class.MonadST as Class
import           Control.Monad.Class.MonadThrow (MonadThrow (..))
import           Control.Monad.Primitive
import           Control.Monad.ST.Strict

import           Database.LSMTree.Internal.BlobRef (WeakBlobRef (..))
import           Database.LSMTree.Internal.Entry
import           Database.LSMTree.Internal.IndexCompact (IndexCompact,
                     PageSpan (..))
import qualified Database.LSMTree.Internal.IndexCompact as Index
import           Database.LSMTree.Internal.RawBytes (RawBytes (..))
import qualified Database.LSMTree.Internal.RawBytes as RB
import           Database.LSMTree.Internal.RawPage
import           Database.LSMTree.Internal.Run (Run, mkBlobRefForRun)
import           Database.LSMTree.Internal.Serialise
import qualified Database.LSMTree.Internal.Vector as V
import qualified Database.LSMTree.Internal.WriteBuffer as WB
import qualified Database.LSMTree.Internal.WriteBufferBlobs as WBB
import           System.FS.API (BufferOffset (..), Handle)
import           System.FS.BlockIO.API

#ifdef BLOOM_QUERY_FAST
import           Database.LSMTree.Internal.BloomFilterQuery2 (RunIxKeyIx (..),
                     bloomQueries)
#else
import           Database.LSMTree.Internal.BloomFilterQuery1 (RunIxKeyIx (..),
                     bloomQueries)
#endif

-- | Prepare disk lookups by doing bloom filter queries, index searches and
-- creating 'IOOp's. The result is a vector of 'IOOp's and a vector of indexes,
-- both of which are the same length. The indexes record the run and key
-- associated with each 'IOOp'.
prepLookups ::
     Arena s
  -> V.Vector (Bloom SerialisedKey)
  -> V.Vector IndexCompact
  -> V.Vector (Handle h)
  -> V.Vector SerialisedKey
  -> ST s (VP.Vector RunIxKeyIx, V.Vector (IOOp s h))
prepLookups arena blooms indexes kopsFiles ks = do
  let !rkixs = bloomQueries blooms ks
  !ioops <- indexSearches arena indexes kopsFiles ks rkixs
  pure (rkixs, ioops)

type KeyIx = Int
type RunIx = Int

-- | Perform a batch of fence pointer index searches, and create an 'IOOp' for
-- each search result. The resulting vector has the same length as the
-- @VP.Vector RunIxKeyIx@ argument, because index searching always returns a
-- positive search result.
indexSearches
  :: Arena s
  -> V.Vector IndexCompact
  -> V.Vector (Handle h)
  -> V.Vector SerialisedKey
  -> VP.Vector RunIxKeyIx -- ^ Result of 'bloomQueries'
  -> ST s (V.Vector (IOOp s h))
indexSearches !arena !indexes !kopsFiles !ks !rkixs = V.generateM n $ \i -> do
    let (RunIxKeyIx !rix !kix)
                     = rkixs `VP.unsafeIndex` i
        !c           = indexes `V.unsafeIndex` rix
        !h           = kopsFiles `V.unsafeIndex` rix
        !k           = ks `V.unsafeIndex` kix
        !pspan       = Index.search k c
        !size        = Index.pageSpanSize pspan
    -- The current allocation strategy is to allocate a new pinned
    -- byte array for each 'IOOp'. One optimisation we are planning to
    -- do is to use a cache of re-usable buffers, in which case we
    -- decrease the GC load. TODO: re-usable buffers.
    (!off, !buf) <- allocateFromArena arena (Index.getNumPages size * 4096) 4096
    pure $! IOOpRead
              h
              (fromIntegral $ Index.unPageNo (pageSpanStart pspan) * 4096)
              buf
              (fromIntegral off)
              (Index.getNumPages size * 4096)
  where
    !n = VP.length rkixs

{-
  Note [Batched lookups, buffer strategy and restrictions]
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

  As a first implementation, we use a simple buffering strategy for batched
  lookups: allocate fresh buffers, and GC them once they are no longer used. In
  a later phase of development, we will look into more elaborate buffer
  strategies, for example using a cache of page buffers, which reduces the
  number of blocks of memory that we allocate/free.

  When we implement a reusable buffer strategy, we will have to take extra care
  to copy bytes from raw pages when necessary. 'rawPageLookup' slices serialised
  values from pages without copying bytes. If the serialised value survives
  after the reusable buffer is returned to the cache, accessing that serialised
  value's bytes has undefined behaviour __unless__ we ensure that we copy from
  the raw page instead of or in addition to slicing.

  There are currently no alignment constraints on the buffers, but there will be
  in the future. The plan is to optimise file access by having key\/operation
  files opened with the @O_DIRECT@ flag, which requires buffers to be aligned to
  the filesystem block size (typically 4096 bytes). We expect to benefit from
  this flag in the UTxO use case, because UTxO keys are uniformly distributed
  hashes, which means that there is little to no spatial locality when
  performing lookups, and so we can skip looking at the page cache.
-}

-- | Value resolve function: what to do when resolving two @Mupdate@s
type ResolveSerialisedValue = SerialisedValue -> SerialisedValue -> SerialisedValue

-- | An 'IOOp' read/wrote fewer or more bytes than expected
data ByteCountDiscrepancy = ByteCountDiscrepancy {
    expected :: !ByteCount
  , actual   :: !ByteCount
  }
  deriving stock (Show, Eq)
  deriving anyclass (Exception)

{-# SPECIALIZE lookupsIO ::
       HasBlockIO IO h
    -> ArenaManager RealWorld
    -> ResolveSerialisedValue
    -> WB.WriteBuffer
    -> WBB.WriteBufferBlobs IO h
    -> V.Vector (Run IO h)
    -> V.Vector (Bloom SerialisedKey)
    -> V.Vector IndexCompact
    -> V.Vector (Handle h)
    -> V.Vector SerialisedKey
    -> IO (V.Vector (Maybe (Entry SerialisedValue (WeakBlobRef IO (Handle h)))))
  #-}
-- | Batched lookups in I\/O.
--
-- See Note [Batched lookups, buffer strategy and restrictions]
--
-- PRECONDITION: the vectors of bloom filters, indexes and file handles
-- should pointwise match with the vectors of runs.
lookupsIO ::
     forall m h. (MonadThrow m, MonadST m)
  => HasBlockIO m h
  -> ArenaManager (PrimState m)
  -> ResolveSerialisedValue
  -> WB.WriteBuffer
  -> WBB.WriteBufferBlobs m h
  -> V.Vector (Run m h) -- ^ Runs @rs@
  -> V.Vector (Bloom SerialisedKey) -- ^ The bloom filters inside @rs@
  -> V.Vector IndexCompact -- ^ The indexes inside @rs@
  -> V.Vector (Handle h) -- ^ The file handles to the key\/value files inside @rs@
  -> V.Vector SerialisedKey
  -> m (V.Vector (Maybe (Entry SerialisedValue (WeakBlobRef m (Handle h)))))
lookupsIO !hbio !mgr !resolveV !wb !wbblobs !rs !blooms !indexes !kopsFiles !ks =
    assert precondition $
    withArena mgr $ \arena -> do
      (rkixs, ioops) <- Class.stToIO $ prepLookups arena blooms indexes kopsFiles ks
      ioress <- submitIO hbio ioops
      intraPageLookups resolveV wb wbblobs rs ks rkixs ioops ioress
  where
    -- we check only that the lengths match, because checking the contents is
    -- too expensive.
    precondition =
      assert (V.length rs == V.length blooms) $
      assert (V.length rs == V.length indexes) $
      assert (V.length rs == V.length kopsFiles) $
      True

{-# SPECIALIZE intraPageLookups ::
       ResolveSerialisedValue
    -> WB.WriteBuffer
    -> WBB.WriteBufferBlobs IO h
    -> V.Vector (Run IO h)
    -> V.Vector SerialisedKey
    -> VP.Vector RunIxKeyIx
    -> V.Vector (IOOp RealWorld h)
    -> VU.Vector IOResult
    -> IO (V.Vector (Maybe (Entry SerialisedValue (WeakBlobRef IO (Handle h)))))
  #-}
-- | Intra-page lookups, and combining lookup results from multiple runs and
-- the write buffer.
--
-- This function assumes that @rkixs@ is ordered such that newer runs are
-- handled first. The order matters for resolving cases where we find the same
-- key in multiple runs.
--
intraPageLookups ::
     forall m h. (PrimMonad m, MonadThrow m)
  => ResolveSerialisedValue
  -> WB.WriteBuffer
  -> WBB.WriteBufferBlobs m h
  -> V.Vector (Run m h)
  -> V.Vector SerialisedKey
  -> VP.Vector RunIxKeyIx
  -> V.Vector (IOOp (PrimState m) h)
  -> VU.Vector IOResult
  -> m (V.Vector (Maybe (Entry SerialisedValue (WeakBlobRef m (Handle h)))))
intraPageLookups !resolveV !wb !wbblobs !rs !ks !rkixs !ioops !ioress = do
    -- We accumulate results into the 'res' vector. When there are several
    -- lookup hits for the same key then we combine the results. The combining
    -- operator is associative but not commutative, so we must do this in the
    -- right order. We start with the write buffer lookup results and then go
    -- through the run lookup results in rkixs, which must be ordered by run.
    --
    -- TODO: reassess the representation of the result vector to try to reduce
    -- intermediate allocations. For example use a less convenient
    -- representation with several vectors (e.g. separate blob info) and
    -- convert to the final convenient representation in a single pass near
    -- the surface API so that all the conversions can be done in one pass
    -- without intermediate allocations.
    --
    res <- VM.generateM (V.length ks) $ \ki ->
             case WB.lookup wb (V.unsafeIndex ks ki) of
               Nothing -> pure Nothing
               Just e  -> pure $! Just $!
                            fmap (WeakBlobRef . WBB.mkBlobRef wbblobs) e
                -- TODO:  ^^ we should be able to avoid this allocation by
                -- combining the conversion with other later conversions.
    loop res 0
    V.unsafeFreeze res
  where
    !n = V.length ioops

    loop ::
         VM.MVector (PrimState m)
                    (Maybe (Entry SerialisedValue (WeakBlobRef m (Handle h))))
      -> Int
      -> m ()
    loop !res !ioopix
      | ioopix == n =  pure ()
      | otherwise = do
          let ioop = ioops `V.unsafeIndex` ioopix
              iores = ioress `VU.unsafeIndex` ioopix
          checkIOResult ioop iores
          let (RunIxKeyIx !rix !kix) = rkixs `VP.unsafeIndex` ioopix
              r = rs `V.unsafeIndex` rix
              k = ks `V.unsafeIndex` kix
          buf <- unsafeFreezeByteArray (ioopBuffer ioop)
          let boff = unBufferOffset $ ioopBufferOffset ioop
          case rawPageLookup (makeRawPage buf boff) k of
            LookupEntryNotPresent -> pure ()
            -- Laziness ensures that we only compute the forcing of the value in
            -- the entry when the result is needed.
            LookupEntry e         -> do
                let e' = bimap copySerialisedValue
                               (WeakBlobRef . mkBlobRefForRun r) e
                -- TODO: ^^ we should be able to avoid this allocation by
                -- combining the conversion with other later conversions.
                V.unsafeInsertWithMStrict res (combine resolveV) kix e'
            -- Laziness ensures that we only compute the appending of the prefix
            -- and suffix when the result is needed. We do not use 'force' here,
            -- since appending already creates a new primary vector.
            LookupEntryOverflow e m -> do
                let v' (SerialisedValue v) = SerialisedValue $ v <>
                      RawBytes (V.mkPrimVector
                                  (unBufferOffset (ioopBufferOffset ioop) + 4096)
                                  (fromIntegral m)
                                  buf)
                    e' = bimap v' (WeakBlobRef . mkBlobRefForRun r) e
                V.unsafeInsertWithMStrict res (combine resolveV) kix e'
          loop res (ioopix + 1)

    -- Check that the IOOp was performed succesfully, and that it wrote/read
    -- exactly as many bytes as we expected. If not, then the buffer won't
    -- contain the correct number of disk-page bytes, so we throw an exception.
    checkIOResult :: IOOp (PrimState m) h -> IOResult -> m ()
    checkIOResult ioop (IOResult m) =
        unless (expected == m) $
          throwIO ByteCountDiscrepancy {expected, actual = m}
      where expected = ioopByteCount ioop

    -- Force a serialised value to not retain any memory by copying the
    -- underlying raw bytes.
    copySerialisedValue :: SerialisedValue -> SerialisedValue
    copySerialisedValue (SerialisedValue rb) =
        SerialisedValue (RB.copy rb)
