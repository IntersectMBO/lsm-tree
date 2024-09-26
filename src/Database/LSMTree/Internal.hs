module Database.LSMTree.Internal (
    -- * Existentials
    Session' (..)
  , NormalTable (..)
  , NormalCursor (..)
  , MonoidalTable (..)
  , MonoidalCursor (..)
    -- * Exceptions
  , LSMTreeError (..)
    -- * Tracing
  , LSMTreeTrace (..)
  , TableTrace (..)
    -- * Session
  , Session (..)
  , SessionState (..)
  , SessionEnv (..)
    -- ** Implementation of public API
  , withSession
  , openSession
  , closeSession
    -- * Table handle
  , TableHandle (..)
  , TableHandleState (..)
  , TableHandleEnv (..)
  , withOpenTable
    -- ** Implementation of public API
  , ResolveSerialisedValue
  , withTable
  , new
  , close
  , lookups
  , rangeLookup
  , updates
  , retrieveBlobs
    -- ** Cursor API
  , Cursor (..)
  , CursorState (..)
  , CursorEnv (..)
  , OffsetKey (..)
  , withCursor
  , newCursor
  , closeCursor
  , readCursor
  , readCursorWhile
    -- * Snapshots
  , SnapshotLabel
  , snapshot
  , open
  , deleteSnapshot
  , listSnapshots
    -- * Mutiple writable table handles
  , duplicate
  ) where

import           Control.Concurrent.Class.MonadMVar.Strict
import           Control.Concurrent.Class.MonadSTM (MonadSTM (..))
import           Control.Concurrent.Class.MonadSTM.RWVar (RWVar)
import qualified Control.Concurrent.Class.MonadSTM.RWVar as RW
import           Control.DeepSeq
import           Control.Monad (unless, void, when)
import           Control.Monad.Class.MonadThrow
import           Control.Monad.Primitive
import           Control.TempRegistry
import           Control.Tracer
import           Data.Arena (ArenaManager, newArenaManager)
import           Data.Bifunctor (Bifunctor (..))
import qualified Data.ByteString.Char8 as BSC
import           Data.Char (isNumber)
import           Data.Foldable
import           Data.Functor.Compose (Compose (..))
import           Data.Kind
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import           Data.Maybe (catMaybes)
import qualified Data.Primitive.ByteArray as P
import           Data.Primitive.MutVar
import qualified Data.Set as Set
import           Data.Typeable
import qualified Data.Vector as V
import           Data.Word (Word64)
import           Database.LSMTree.Internal.BlobRef (WeakBlobRef (..))
import qualified Database.LSMTree.Internal.BlobRef as BlobRef
import           Database.LSMTree.Internal.Config
import           Database.LSMTree.Internal.Cursor
import           Database.LSMTree.Internal.Entry (Entry)
import qualified Database.LSMTree.Internal.Entry as Entry
import           Database.LSMTree.Internal.Lookup (ByteCountDiscrepancy,
                     ResolveSerialisedValue, lookupsIO)
import           Database.LSMTree.Internal.MergeSchedule
import           Database.LSMTree.Internal.Paths (RunFsPaths (..),
                     SessionRoot (..), SnapshotName)
import qualified Database.LSMTree.Internal.Paths as Paths
import           Database.LSMTree.Internal.Range (Range (..))
import qualified Database.LSMTree.Internal.RawBytes as RB
import           Database.LSMTree.Internal.Run (Run)
import qualified Database.LSMTree.Internal.Run as Run
import           Database.LSMTree.Internal.RunNumber
import qualified Database.LSMTree.Internal.RunReader as Reader
import           Database.LSMTree.Internal.RunReaders (OffsetKey (..))
import qualified Database.LSMTree.Internal.RunReaders as Readers
import           Database.LSMTree.Internal.Serialise (SerialisedBlob (..),
                     SerialisedKey, SerialisedValue)
import           Database.LSMTree.Internal.Session
import           Database.LSMTree.Internal.Snapshot
import           Database.LSMTree.Internal.TableHandle
import           Database.LSMTree.Internal.UniqCounter
import qualified Database.LSMTree.Internal.Vector as V
import           Database.LSMTree.Internal.WriteBuffer (WriteBuffer)
import qualified Database.LSMTree.Internal.WriteBuffer as WB
import           Database.LSMTree.Internal.WriteBufferBlobs (WriteBufferBlobs)
import qualified Database.LSMTree.Internal.WriteBufferBlobs as WBB
import qualified System.FS.API as FS
import           System.FS.API (FsError, FsErrorPath (..), FsPath, Handle,
                     HasFS)
import qualified System.FS.API.Lazy as FS
import qualified System.FS.API.Strict as FS
import qualified System.FS.BlockIO.API as FS
import           System.FS.BlockIO.API (HasBlockIO)

{-------------------------------------------------------------------------------
  Existentials
-------------------------------------------------------------------------------}

type Session' :: (Type -> Type) -> Type
data Session' m = forall h. Typeable h => Session' !(Session m h)

instance NFData (Session' m) where
  rnf (Session' s) = rnf s

type NormalTable :: (Type -> Type) -> Type -> Type -> Type -> Type
data NormalTable m k v b = forall h. Typeable h =>
    NormalTable !(TableHandle m h)

instance NFData (NormalTable m k v b) where
  rnf (NormalTable th) = rnf th

type NormalCursor :: (Type -> Type) -> Type -> Type -> Type -> Type
data NormalCursor m k v blob = forall h. Typeable h =>
    NormalCursor !(Cursor m h)

instance NFData (NormalCursor m k v b) where
  rnf (NormalCursor c) = rnf c

type MonoidalTable :: (Type -> Type) -> Type -> Type -> Type
data MonoidalTable m k v = forall h. Typeable h =>
    MonoidalTable !(TableHandle m h)

instance NFData (MonoidalTable m k v) where
  rnf (MonoidalTable th) = rnf th

type MonoidalCursor :: (Type -> Type) -> Type -> Type -> Type
data MonoidalCursor m k v = forall h. Typeable h =>
    MonoidalCursor !(Cursor m h)

instance NFData (MonoidalCursor m k v) where
  rnf (MonoidalCursor c) = rnf c

{-------------------------------------------------------------------------------
  Table handle
-------------------------------------------------------------------------------}

{-# SPECIALISE lookups :: ResolveSerialisedValue -> V.Vector SerialisedKey -> TableHandle IO h -> (Maybe (Entry SerialisedValue (WeakBlobRef IO (Handle h))) -> lookupResult) -> IO (V.Vector lookupResult) #-}
-- | See 'Database.LSMTree.Normal.lookups'.
lookups ::
     m ~ IO -- TODO: replace by @io-classes@ constraints for IO simulation.
  => ResolveSerialisedValue
  -> V.Vector SerialisedKey
  -> TableHandle m h
  -> (Maybe (Entry SerialisedValue (WeakBlobRef m (Handle h))) -> lookupResult)
     -- ^ How to map from an entry to a lookup result.
  -> m (V.Vector lookupResult)
lookups resolve ks th fromEntry = do
    traceWith (tableTracer th) $ TraceLookups (V.length ks)
    withOpenTable th $ \thEnv -> do
      let arenaManager = tableHandleArenaManager th
      RW.withReadAccess (tableContent thEnv) $ \tableContent -> do
        let !cache = tableCache tableContent
        ioRes <-
          lookupsIO
            (tableHasBlockIO thEnv)
            arenaManager
            resolve
            (cachedRuns cache)
            (cachedFilters cache)
            (cachedIndexes cache)
            (cachedKOpsFiles cache)
            ks
        --TODO: this bit is all a bit of a mess, not well factored
        --TODO: incorporate write buffer lookups into the lookupsIO
        --TODO: reduce allocations involved with converting BlobSpan to BlobRef
        -- and Entry to the lookup result. Try one single conversion rather
        -- than multiple steps that each allocate.
        let !wb = tableWriteBuffer tableContent
            !wbblobs = tableWriteBufferBlobs tableContent
        toBlobRef <- WBB.mkBlobRef wbblobs
        let wbLookup = fmap (fmap (WeakBlobRef . toBlobRef))
                     . WB.lookup wb
        pure $!
          V.zipWithStrict
            (\k1 e2 -> fromEntry $ Entry.combineMaybe resolve (wbLookup k1) e2)
            ks ioRes

{-# SPECIALISE rangeLookup :: ResolveSerialisedValue -> Range SerialisedKey -> TableHandle IO h -> (SerialisedKey -> SerialisedValue -> Maybe (WeakBlobRef IO (Handle h)) -> res) -> IO (V.Vector res) #-}
-- | See 'Database.LSMTree.Normal.rangeLookup'.
rangeLookup ::
     m ~ IO -- TODO: replace by @io-classes@ constraints for IO simulation.
  => ResolveSerialisedValue
  -> Range SerialisedKey
  -> TableHandle m h
  -> (SerialisedKey -> SerialisedValue -> Maybe (WeakBlobRef m (Handle h)) -> res)
     -- ^ How to map to a query result, different for normal/monoidal
  -> m (V.Vector res)
rangeLookup resolve range th fromEntry = do
    traceWith (tableTracer th) $ TraceRangeLookup range
    case range of
      FromToExcluding lb ub ->
        withCursor (OffsetKey lb) th $ \cursor ->
          go cursor (< ub) []
      FromToIncluding lb ub ->
        withCursor (OffsetKey lb) th $ \cursor ->
          go cursor (<= ub) []
  where
    -- TODO: tune!
    -- Also, such a high number means that many tests never cover the case
    -- of having multiple chunks. Expose through the public API as config?
    chunkSize = 500

    go cursor isInUpperBound !chunks = do
      chunk <- readCursorWhile resolve isInUpperBound chunkSize cursor fromEntry
      let !n = V.length chunk
      if n >= chunkSize
        then go cursor isInUpperBound (chunk : chunks)
             -- This requires an extra copy. If we had a size hint, we could
             -- directly write everything into the result vector.
             -- TODO(optimise): revisit
        else return (V.concat (reverse (V.slice 0 n chunk : chunks)))

{-# SPECIALISE updates :: ResolveSerialisedValue -> V.Vector (SerialisedKey, Entry SerialisedValue SerialisedBlob) -> TableHandle IO h -> IO () #-}
-- | See 'Database.LSMTree.Normal.updates'.
--
-- Does not enforce that mupsert and blobs should not occur in the same table.
updates ::
     m ~ IO -- TODO: replace by @io-classes@ constraints for IO simulation.
  => ResolveSerialisedValue
  -> V.Vector (SerialisedKey, Entry SerialisedValue SerialisedBlob)
  -> TableHandle m h
  -> m ()
updates resolve es th = do
    traceWith (tableTracer th) $ TraceUpdates (V.length es)
    let conf = tableConfig th
    withOpenTable th $ \thEnv -> do
      let hfs = tableHasFS thEnv
      modifyWithTempRegistry_
        (atomically $ RW.unsafeAcquireWriteAccess (tableContent thEnv))
        (atomically . RW.unsafeReleaseWriteAccess (tableContent thEnv)) $ \reg -> do
          updatesWithInterleavedFlushes
            (TraceMerge `contramap` tableTracer th)
            conf
            resolve
            hfs
            (tableHasBlockIO thEnv)
            (tableSessionRoot thEnv)
            (tableSessionUniqCounter thEnv)
            es
            reg

{-------------------------------------------------------------------------------
  Blobs
-------------------------------------------------------------------------------}

retrieveBlobs ::
     m ~ IO  -- TODO: replace by @io-classes@ constraints for IO simulation.
  => Session m h
  -> V.Vector (WeakBlobRef m (FS.Handle h))
  -> m (V.Vector SerialisedBlob)
retrieveBlobs sesh wrefs =
    withOpenSession sesh $ \seshEnv ->
      handle (\(BlobRef.WeakBlobRefInvalid i) -> throwIO (ErrBlobRefInvalid i)) $
      BlobRef.withWeakBlobRefs wrefs $ \refs -> do

        -- Prepare the IOOps:
        -- We use a single large memory buffer, with appropriate offsets within
        -- the buffer.
        let bufSize :: Int
            !bufSize = V.sum (V.map BlobRef.blobRefSpanSize refs)

            {-# INLINE bufOffs #-}
            bufOffs :: V.Vector Int
            bufOffs = V.scanl (+) 0 (V.map BlobRef.blobRefSpanSize refs)
        buf <- P.newPinnedByteArray bufSize
        let ioops = V.zipWith (BlobRef.readBlobIOOp buf) bufOffs refs
            hbio  = sessionHasBlockIO seshEnv

        -- Submit the IOOps all in one go:
        _ <- FS.submitIO hbio ioops
        -- We do not need to inspect the results because IO errors are
        -- thrown as exceptions, and the result is just the read length
        -- which is already known. Short reads can't happen here.

        -- Construct the SerialisedBlobs results:
        -- This is just the different offsets within the shared buffer.
        ba <- P.unsafeFreezeByteArray buf
        pure $! V.zipWith
                  (\off len -> SerialisedBlob (RB.fromByteArray off len ba))
                  bufOffs
                  (V.map BlobRef.blobRefSpanSize refs)
