{-# LANGUAGE CPP #-}
{-# OPTIONS_HADDOCK not-home #-}

module Database.LSMTree.Internal.Lookup (
    LookupAcc
  , lookupsIOWithWriteBuffer
  , lookupsIO
    -- * Errors
  , TableCorruptedError (..)
    -- * Internal: exposed for tests and benchmarks
  , RunIx
  , KeyIx
  , RunIxKeyIx(..)
  , prepLookups
  , bloomQueries
  , indexSearches
  , intraPageLookupsWithWriteBuffer
  , intraPageLookupsOn
  ) where

import           Data.Bifunctor
import           Data.Primitive.ByteArray
import qualified Data.Vector as V
import qualified Data.Vector.Mutable as VM
import qualified Data.Vector.Primitive as VP
import qualified Data.Vector.Unboxed as VU
import           Database.LSMTree.Internal.Arena (Arena, ArenaManager,
                     allocateFromArena, withArena)

import           Control.Exception (assert)
import           Control.Monad
import           Control.Monad.Class.MonadST as ST
import           Control.Monad.Class.MonadThrow (Exception, MonadThrow (..))
import           Control.Monad.Primitive
import           Control.Monad.ST.Strict
import           Control.RefCount

import           Database.LSMTree.Internal.BlobRef (WeakBlobRef (..))
import           Database.LSMTree.Internal.BloomFilter (Bloom, RunIxKeyIx (..),
                     bloomQueries)
import qualified Database.LSMTree.Internal.BloomFilter as Bloom
import           Database.LSMTree.Internal.Entry
import           Database.LSMTree.Internal.Index (Index)
import qualified Database.LSMTree.Internal.Index as Index (search)
import           Database.LSMTree.Internal.Page (PageSpan (..), getNumPages,
                     pageSpanSize, unPageNo)
import           Database.LSMTree.Internal.RawBytes (RawBytes (..))
import qualified Database.LSMTree.Internal.RawBytes as RB
import           Database.LSMTree.Internal.RawPage
import           Database.LSMTree.Internal.Run (Run)
import qualified Database.LSMTree.Internal.Run as Run
import           Database.LSMTree.Internal.Serialise
import qualified Database.LSMTree.Internal.Vector as V
import qualified Database.LSMTree.Internal.WriteBuffer as WB
import qualified Database.LSMTree.Internal.WriteBufferBlobs as WBB
import           System.FS.API (BufferOffset (..), Handle)
import           System.FS.BlockIO.API

-- | Prepare disk lookups by doing bloom filter queries, index searches and
-- creating 'IOOp's. The result is a vector of 'IOOp's and a vector of indexes,
-- both of which are the same length. The indexes record the run and key
-- associated with each 'IOOp'.
prepLookups ::
     Arena s
  -> Bloom.Salt
  -> V.Vector (Bloom SerialisedKey)
  -> V.Vector Index
  -> V.Vector (Handle h)
  -> V.Vector SerialisedKey
  -> ST s (VP.Vector RunIxKeyIx, V.Vector (IOOp s h))
prepLookups arena salt blooms indexes kopsFiles ks = do
  let !rkixs = bloomQueries salt blooms ks
  !ioops <- indexSearches arena indexes kopsFiles ks rkixs
  pure (rkixs, ioops)

type KeyIx = Int
type RunIx = Int

-- | Perform a batch of fence pointer index searches, and create an 'IOOp' for
-- each search result. The resulting vector has the same length as the
-- @VP.Vector RunIxKeyIx@ argument, because index searching always returns a
-- positive search result.
indexSearches ::
     Arena s
  -> V.Vector Index
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
        !size        = pageSpanSize pspan
    -- Acquire a reusable buffer
    (!off, !buf) <- allocateFromArena arena (getNumPages size * 4096) 4096
    pure $! IOOpRead
              h
              (fromIntegral $ unPageNo (pageSpanStart pspan) * 4096)
              buf
              (fromIntegral off)
              (getNumPages size * 4096)
  where
    !n = VP.length rkixs

type LookupAcc m h = V.Vector (Maybe (Entry SerialisedValue (WeakBlobRef m h)))

{-# SPECIALISE lookupsIOWithWriteBuffer ::
       HasBlockIO IO h
    -> ArenaManager RealWorld
    -> ResolveSerialisedValue
    -> Bloom.Salt
    -> WB.WriteBuffer
    -> Ref (WBB.WriteBufferBlobs IO h)
    -> V.Vector (Ref (Run IO h))
    -> V.Vector (Bloom SerialisedKey)
    -> V.Vector Index
    -> V.Vector (Handle h)
    -> V.Vector SerialisedKey
    -> IO (LookupAcc IO h)
  #-}
-- | Like 'lookupsIO', but takes a write buffer into account.
lookupsIOWithWriteBuffer ::
     forall m h. (MonadThrow m, MonadST m)
  => HasBlockIO m h
  -> ArenaManager (PrimState m)
  -> ResolveSerialisedValue
  -> Bloom.Salt
  -> WB.WriteBuffer
  -> Ref (WBB.WriteBufferBlobs m h)
  -> V.Vector (Ref (Run m h)) -- ^ Runs @rs@
  -> V.Vector (Bloom SerialisedKey) -- ^ The bloom filters inside @rs@
  -> V.Vector Index -- ^ The indexes inside @rs@
  -> V.Vector (Handle h) -- ^ The file handles to the key\/value files inside @rs@
  -> V.Vector SerialisedKey
  -> m (LookupAcc m h)
lookupsIOWithWriteBuffer !hbio !mgr !resolveV !salt !wb !wbblobs !rs !blooms !indexes !kopsFiles !ks =
    assert precondition $
    withArena mgr $ \arena -> do
      (rkixs, ioops) <- ST.stToIO $ prepLookups arena salt blooms indexes kopsFiles ks
      ioress <- submitIO hbio ioops
      intraPageLookupsWithWriteBuffer resolveV wb wbblobs rs ks rkixs ioops ioress
  where
    -- we check only that the lengths match, because checking the contents is
    -- too expensive.
    precondition =
      assert (V.length rs == V.length blooms) $
      assert (V.length rs == V.length indexes) $
      assert (V.length rs == V.length kopsFiles) $
      True

{-# SPECIALISE lookupsIO ::
       HasBlockIO IO h
    -> ArenaManager RealWorld
    -> ResolveSerialisedValue
    -> Bloom.Salt
    -> V.Vector (Ref (Run IO h))
    -> V.Vector (Bloom SerialisedKey)
    -> V.Vector Index
    -> V.Vector (Handle h)
    -> V.Vector SerialisedKey
    -> IO (LookupAcc IO h)
  #-}
-- | Batched lookups in I\/O.
--
-- PRECONDITION: the vectors of bloom filters, indexes and file handles
-- should pointwise match with the vectors of runs.
lookupsIO ::
     forall m h. (MonadThrow m, MonadST m)
  => HasBlockIO m h
  -> ArenaManager (PrimState m)
  -> ResolveSerialisedValue
  -> Bloom.Salt
  -> V.Vector (Ref (Run m h)) -- ^ Runs @rs@
  -> V.Vector (Bloom SerialisedKey) -- ^ The bloom filters inside @rs@
  -> V.Vector Index -- ^ The indexes inside @rs@
  -> V.Vector (Handle h) -- ^ The file handles to the key\/value files inside @rs@
  -> V.Vector SerialisedKey
  -> m (LookupAcc m h)
lookupsIO !hbio !mgr !resolveV !salt !rs !blooms !indexes !kopsFiles !ks =
    assert precondition $
    withArena mgr $ \arena -> do
      (rkixs, ioops) <- ST.stToIO $ prepLookups arena salt blooms indexes kopsFiles ks
      ioress <- submitIO hbio ioops
      intraPageLookupsOn resolveV (V.map (const Nothing) ks) rs ks rkixs ioops ioress
  where
    -- we check only that the lengths match, because checking the contents is
    -- too expensive.
    precondition =
      assert (V.length rs == V.length blooms) $
      assert (V.length rs == V.length indexes) $
      assert (V.length rs == V.length kopsFiles) $
      True

{-# SPECIALISE intraPageLookupsWithWriteBuffer ::
       ResolveSerialisedValue
    -> WB.WriteBuffer
    -> Ref (WBB.WriteBufferBlobs IO h)
    -> V.Vector (Ref (Run IO h))
    -> V.Vector SerialisedKey
    -> VP.Vector RunIxKeyIx
    -> V.Vector (IOOp RealWorld h)
    -> VU.Vector IOResult
    -> IO (LookupAcc IO h)
  #-}
-- | Like 'intraPageLookupsOn', but uses the write buffer as the initial
-- accumulator.
--
intraPageLookupsWithWriteBuffer ::
     forall m h. (PrimMonad m, MonadThrow m)
  => ResolveSerialisedValue
  -> WB.WriteBuffer
  -> Ref (WBB.WriteBufferBlobs m h)
  -> V.Vector (Ref (Run m h))
  -> V.Vector SerialisedKey
  -> VP.Vector RunIxKeyIx
  -> V.Vector (IOOp (PrimState m) h)
  -> VU.Vector IOResult
  -> m (LookupAcc m h)
intraPageLookupsWithWriteBuffer !resolveV !wb !wbblobs !rs !ks !rkixs !ioops !ioress = do
    -- The most recent values are in the write buffer, so we use it to
    -- initialise the accumulator.
    acc0 <-
      V.generateM (V.length ks) $ \ki ->
        case WB.lookup wb (V.unsafeIndex ks ki) of
          Nothing -> pure Nothing
          Just e  -> pure $! Just $! fmap (WBB.mkWeakBlobRef wbblobs) e
          -- TODO:  ^^ we should be able to avoid this allocation by
          -- combining the conversion with other later conversions.
    intraPageLookupsOn resolveV acc0 rs ks rkixs ioops ioress

-- | The table data is corrupted.
data TableCorruptedError
    = ErrLookupByteCountDiscrepancy
        -- | Expected byte count.
        !ByteCount
        -- | Actual byte count.
        !ByteCount
    deriving stock (Show, Eq)
    deriving anyclass (Exception)

{-# SPECIALISE intraPageLookupsOn ::
       ResolveSerialisedValue
    -> LookupAcc IO h
    -> V.Vector (Ref (Run IO h))
    -> V.Vector SerialisedKey
    -> VP.Vector RunIxKeyIx
    -> V.Vector (IOOp RealWorld h)
    -> VU.Vector IOResult
    -> IO (LookupAcc IO h)
  #-}
-- | Intra-page lookups, and combining lookup results from multiple runs and
-- a potential initial accumulator (e.g. from the write buffer).
--
-- This function assumes that @rkixs@ is ordered such that newer runs are
-- handled first. The order matters for resolving cases where we find the same
-- key in multiple runs.
--
intraPageLookupsOn ::
     forall m h. (PrimMonad m, MonadThrow m)
  => ResolveSerialisedValue
  -> LookupAcc m h  -- initial acc
  -> V.Vector (Ref (Run m h))
  -> V.Vector SerialisedKey
  -> VP.Vector RunIxKeyIx
  -> V.Vector (IOOp (PrimState m) h)
  -> VU.Vector IOResult
  -> m (LookupAcc m h)
intraPageLookupsOn !resolveV !acc0 !rs !ks !rkixs !ioops !ioress =
    assert (V.length acc0 == V.length ks) $ do
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
    res <- V.unsafeThaw acc0
    loop res 0
    V.unsafeFreeze res
  where
    !n = V.length ioops

    loop ::
         VM.MVector (PrimState m)
                    (Maybe (Entry SerialisedValue (WeakBlobRef m h)))
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
                let e' = bimap copySerialisedValue (Run.mkWeakBlobRef r) e
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
                    e' = bimap v' (Run.mkWeakBlobRef r) e
                V.unsafeInsertWithMStrict res (combine resolveV) kix e'
          loop res (ioopix + 1)

    -- Check that the IOOp was performed successfully, and that it wrote/read
    -- exactly as many bytes as we expected. If not, then the buffer won't
    -- contain the correct number of disk-page bytes, so we throw an exception.
    checkIOResult :: IOOp (PrimState m) h -> IOResult -> m ()
    checkIOResult ioop (IOResult m) =
        unless (expected == m) . throwIO $
          ErrLookupByteCountDiscrepancy expected m
      where expected = ioopByteCount ioop

    -- Force a serialised value to not retain any memory by copying the
    -- underlying raw bytes.
    copySerialisedValue :: SerialisedValue -> SerialisedValue
    copySerialisedValue (SerialisedValue rb) =
        SerialisedValue (RB.copy rb)
