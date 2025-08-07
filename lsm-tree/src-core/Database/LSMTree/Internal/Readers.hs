{-# OPTIONS_HADDOCK not-home #-}

-- | Multiple inputs (write buffers, runs) that are being read incrementally.
module Database.LSMTree.Internal.Readers (
    Readers (..)
  , OffsetKey (..)
  , ReaderSource (..)
  , ReadersMergeType (..)
  , new
  , close
  , peekKey
  , HasMore (..)
  , pop
  , dropWhileKey
    -- * Internals
  , Reader (..)
  , ReaderNumber (..)
  , ReadCtx (..)
  ) where

import           Control.Monad (zipWithM)
import           Control.Monad.Class.MonadST (MonadST)
import           Control.Monad.Class.MonadSTM (MonadSTM (..))
import           Control.Monad.Class.MonadThrow (MonadMask)
import           Control.Monad.Primitive
import           Control.RefCount
import           Data.Function (on)
import           Data.Functor ((<&>))
import           Data.List.NonEmpty (nonEmpty)
import qualified Data.Map.Strict as Map
import           Data.Maybe (catMaybes)
import           Data.Primitive.MutVar
import           Data.Traversable (for)
import           Database.LSMTree.Internal.BlobRef (BlobSpan, RawBlobRef)
import           Database.LSMTree.Internal.Entry (Entry (..))
import qualified Database.LSMTree.Internal.Entry as Entry
import           Database.LSMTree.Internal.Index.CompactAcc (SMaybe (..),
                     smaybe)
import           Database.LSMTree.Internal.Run (Run)
import           Database.LSMTree.Internal.RunReader (OffsetKey (..),
                     RunReader (..))
import qualified Database.LSMTree.Internal.RunReader as RunReader
import           Database.LSMTree.Internal.Serialise
import qualified Database.LSMTree.Internal.WriteBuffer as WB
import qualified Database.LSMTree.Internal.WriteBufferBlobs as WB
import qualified KMerge.Heap as Heap
import qualified System.FS.API as FS

-- | A collection of runs and write buffers being read from, yielding elements
-- in order. More precisely, that means first ordered by their key, then by the
-- input run they came from. This is important for resolving multiple entries
-- with the same key into one.
--
-- Construct with 'new', then keep calling 'pop'.
-- If aborting early, remember to call 'close'!
--
-- Creating a 'Readers' does not retain a reference to the input 'Run's or the
-- 'WriteBufferBlobs', but does retain an independent reference on their blob
-- files. It is not necessary to separately retain the 'Run's or the
-- 'WriteBufferBlobs' for correct use of the 'Readers'. There is one important
-- caveat however: to preserve the validity of 'BlobRef's then it is necessary
-- to separately retain a reference to the 'Run' or its 'BlobFile' to preserve
-- the validity of 'BlobRefs'.
--
-- TODO: do this more nicely by changing 'Reader' to preserve the 'BlobFile'
-- ref until it is explicitly closed, and also retain the 'BlobFile' from the
-- WBB and release all of these 'BlobFiles' once the 'Readers' is itself closed.
--
data Readers m h = Readers {
      readersHeap :: !(Heap.MutableHeap (PrimState m) (ReadCtx m h))
      -- | Since there is always one reader outside of the heap, we need to
      -- store it separately. This also contains the next k\/op to yield, unless
      -- all readers are drained, i.e. both:
      -- 1. the reader inside the 'ReadCtx' is empty
      -- 2. the heap is empty
    , readersNext :: !(MutVar (PrimState m) (ReadCtx m h))
    }

newtype ReaderNumber = ReaderNumber Int
  deriving stock (Eq, Ord)

-- | Each heap element needs some more context than just the reader.
-- E.g. the 'Eq' instance we need to be able to access the first key to be read
-- in a pure way.
--
-- TODO(optimisation): We allocate this record for each k/op. This might be
-- avoidable, see ideas below.
data ReadCtx m h = ReadCtx {
      -- We could avoid this using a more specialised mutable heap with separate
      -- arrays for keys and values (or even each of their components).
      -- Using an 'STRef' could avoid reallocating the record for every entry,
      -- but that might not be straightforward to integrate with the heap.
      readCtxHeadKey   :: !SerialisedKey
    , readCtxHeadEntry :: !(RunReader.Entry m h)
      -- We could get rid of this by making 'LoserTree' stable (for which there
      -- is a prototype already).
      -- Alternatively, if we decide to have an invariant that the number in
      -- 'RunFsPaths' is always higher for newer runs, then we could use that
      -- in the 'Ord' instance.
    , readCtxNumber    :: !ReaderNumber
    , readCtxReader    :: !(Reader m h)
    }

instance Eq (ReadCtx m h) where
  (==) = (==) `on` (\r -> (readCtxHeadKey r, readCtxNumber r))

-- | Makes sure we resolve entries in the right order.
instance Ord (ReadCtx m h) where
  compare = compare `on` (\r -> (readCtxHeadKey r, readCtxNumber r))

-- | An individual reader must be able to produce a sequence of pairs of
-- 'SerialisedKey' and 'RunReader.Entry', with ordered und unique keys.
--
-- TODO: This is slightly inelegant. This module could work generally for
-- anything that can produce elements, but currently is very specific to having
-- write buffer and run readers. Also, for run merging, no write buffer is
-- involved, but we still need to branch on this sum type.
-- A more general version is possible, but despite SPECIALISE-ing everything
-- showed ~100 bytes of extra allocations per entry that is read (which might be
-- avoidable with some tinkering).
data Reader m h =
    -- | The list allows to incrementally read from the write buffer without
    -- having to find the next entry in the Map again (requiring key
    -- comparisons) or having to copy out all entries.
    --
    -- TODO: more efficient representation? benchmark!
    ReadBuffer  !(MutVar (PrimState m) [KOp m h])
  | ReadRun     !(RunReader m h)
    -- | Recursively read from another reader. This requires keeping track of
    -- its 'HasMore' status, since we should not try to read another entry from
    -- it once it is drained.
    --
    -- We represent the recursive reader and 'HasMore' status together as a
    -- 'Maybe' 'Readers'. The reason is subtle: once a 'Readers' becomes drained
    -- it is immediately closed, after which the structure should not be used
    -- anymore or you'd be using resources after they have been closed already.
    --
    -- TODO: maybe it's a slightly more ergonomic alternative to no close the
    -- 'Readers' automatically.
  | ReadReaders !ReadersMergeType !(SMaybe (Readers m h))

type KOp m h = (SerialisedKey, Entry SerialisedValue (RawBlobRef m h))

data ReaderSource m h =
    FromWriteBuffer !WB.WriteBuffer !(Ref (WB.WriteBufferBlobs m h))
  | FromRun         !(Ref (Run m h))
    -- | Recursive case, allowing to build a tree of readers for a merging tree.
  | FromReaders     !ReadersMergeType ![ReaderSource m h]

{-# SPECIALISE new ::
     ResolveSerialisedValue
  -> OffsetKey
  -> [ReaderSource IO h]
  -> IO (Maybe (Readers IO h)) #-}
new :: forall m h.
     (MonadMask m, MonadST m, MonadSTM m)
  => ResolveSerialisedValue
  -> OffsetKey
  -> [ReaderSource m h]
  -> m (Maybe (Readers m h))
new resolve !offsetKey sources = do
    readers <- zipWithM (fromSource . ReaderNumber) [1..] sources
    for (nonEmpty (catMaybes readers)) $ \xs -> do
      (readersHeap, readCtx) <- Heap.newMutableHeap xs
      readersNext <- newMutVar readCtx
      pure Readers {..}
  where
    fromSource :: ReaderNumber -> ReaderSource m h -> m (Maybe (ReadCtx m h))
    fromSource n src =
        case src of
          FromWriteBuffer wb wbblobs -> do
            rs <- fromWB wb wbblobs
            nextReadCtx resolve n rs
          FromRun r -> do
            rs <- ReadRun <$> RunReader.new offsetKey r
            nextReadCtx resolve n rs
          FromReaders mergeType nestedSources -> do
            new resolve offsetKey nestedSources >>= \case
              Nothing -> pure Nothing
              Just rs -> nextReadCtx resolve n (ReadReaders mergeType (SJust rs))

    fromWB :: WB.WriteBuffer -> Ref (WB.WriteBufferBlobs m h) -> m (Reader m h)
    fromWB wb wbblobs = do
        let kops = Map.toList $ filterWB $ WB.toMap wb
        ReadBuffer <$> newMutVar (map convertBlobs kops)
      where
        -- TODO: this conversion involves quite a lot of allocation
        convertBlobs :: (k, Entry v BlobSpan) -> (k, Entry v (RawBlobRef m h))
        convertBlobs = fmap (fmap (WB.mkRawBlobRef wbblobs))

        filterWB = case offsetKey of
            NoOffsetKey -> id
            OffsetKey k -> Map.dropWhileAntitone (< k)

{-# SPECIALISE close :: Readers IO (FS.Handle h) -> IO () #-}
-- | Clean up the resources held by the readers.
--
-- Only call this function when aborting before all readers have been drained!
close ::
     (MonadMask m, MonadSTM m, PrimMonad m)
  => Readers m h
  -> m ()
close Readers {..} = do
    ReadCtx {readCtxReader} <- readMutVar readersNext
    closeReader readCtxReader
    closeHeap
  where
    closeReader = \case
        ReadBuffer _             -> pure ()
        ReadRun r                -> RunReader.close r
        ReadReaders _ readersMay -> smaybe (pure ()) close readersMay
    closeHeap =
        Heap.extract readersHeap >>= \case
          Nothing -> pure ()
          Just ReadCtx {readCtxReader} -> do
            closeReader readCtxReader
            closeHeap

{-# SPECIALISE peekKey :: Readers IO h -> IO SerialisedKey #-}
-- | Return the smallest key present in the readers, without consuming any
-- entries.
peekKey ::
     PrimMonad m
  => Readers m h
  -> m SerialisedKey
peekKey Readers {..} = do
    readCtxHeadKey <$> readMutVar readersNext

-- | Once a function returned 'Drained', do not use the 'Readers' any more!
data HasMore = HasMore | Drained
  deriving stock (Eq, Show)

{-# SPECIALISE pop ::
     ResolveSerialisedValue
  -> Readers IO h
  -> IO (SerialisedKey, RunReader.Entry IO h, HasMore) #-}
-- | Remove the entry with the smallest key and return it. If there are multiple
-- entries with that key, it removes the one from the source that came first
-- in list supplied to 'new'. No resolution of multiple entries takes place.
pop ::
     (MonadMask m, MonadSTM m, MonadST m)
  => ResolveSerialisedValue
  -> Readers m h
  -> m (SerialisedKey, RunReader.Entry m h, HasMore)
pop resolve r@Readers {..} = do
    ReadCtx {..} <- readMutVar readersNext
    hasMore <- dropOne resolve r readCtxNumber readCtxReader
    pure (readCtxHeadKey, readCtxHeadEntry, hasMore)

-- TODO: avoid duplication with Merge.TreeMergeType?
data ReadersMergeType = MergeLevel | MergeUnion
  deriving stock (Eq, Show)

{-# SPECIALISE popResolved ::
     ResolveSerialisedValue
  -> ReadersMergeType
  -> Readers IO h
  -> IO (SerialisedKey, RunReader.Entry IO h, HasMore) #-}
-- | Produces an entry with the smallest key, resolving all input entries if
-- there are multiple. Therefore, the next call to 'peekKey' will return a
-- larger key than the one returned here.
--
-- General notes on the code below:
-- * It is quite similar to the one in Internal.Cursor and Internal.Merge. Maybe
--   we can avoid some duplication.
-- * Any function that doesn't take a 'hasMore' argument assumes that the
--   readers have not been drained yet, so we must check before calling them.
-- * There is probably opportunity for optimisations.
--
-- TODO: use this function in Internal.Cursor? Measure performance impact.
popResolved ::
     forall h m.
     (MonadMask m, MonadST m, MonadSTM m)
  => ResolveSerialisedValue
  -> ReadersMergeType
  -> Readers m h
  -> m (SerialisedKey, RunReader.Entry m h, HasMore)
popResolved resolve mergeType readers = readEntry
  where
    readEntry :: m (SerialisedKey, RunReader.Entry m h, HasMore)
    readEntry = do
        (key, entry, hasMore) <- pop resolve readers
        case hasMore of
          Drained -> do
            pure (key, entry, Drained)
          HasMore -> do
            case mergeType of
              MergeLevel -> handleLevel key (RunReader.toFullEntry entry)
              MergeUnion -> handleUnion key (RunReader.toFullEntry entry)

    handleUnion :: SerialisedKey
                -> Entry SerialisedValue (RawBlobRef m h)
                -> m (SerialisedKey, RunReader.Entry m h, HasMore)
    handleUnion key entry = do
        nextKey <- peekKey readers
        if nextKey /= key
          then
            -- No more entries for same key, done.
            pure (key, RunReader.Entry entry, HasMore)
          else do
            (_, nextEntry, hasMore) <- pop resolve readers
            let resolved = Entry.combineUnion resolve entry
                             (RunReader.toFullEntry nextEntry)
            case hasMore of
              HasMore -> handleUnion key resolved
              Drained -> pure (key, RunReader.Entry resolved, Drained)

    handleLevel :: SerialisedKey
                -> Entry SerialisedValue (RawBlobRef m h)
                -> m (SerialisedKey, RunReader.Entry m h, HasMore)
    handleLevel key entry =
        case entry of
          Upsert v ->
            handleMupdate key v
          _ -> do
            -- Anything but Upsert supersedes all previous entries of
            -- the same key, so we can simply drop them and are done.
            hasMore' <- dropRemaining key
            pure (key, RunReader.Entry entry, hasMore')

    -- Resolve a 'Mupsert' value with the other entries of the same key.
    handleMupdate :: SerialisedKey
                  -> SerialisedValue
                  -> m (SerialisedKey, RunReader.Entry m h, HasMore)
    handleMupdate key v = do
        nextKey <- peekKey readers
        if nextKey /= key
          then
            -- No more entries for same key, done.
            pure (key, RunReader.Entry (Upsert v), HasMore)
          else do
            (_, nextEntry, hasMore) <- pop resolve readers
            let resolved = Entry.combine resolve (Upsert v)
                             (RunReader.toFullEntry nextEntry)
            case hasMore of
              HasMore -> handleLevel key resolved
              Drained -> pure (key, RunReader.Entry resolved, Drained)

    dropRemaining :: SerialisedKey -> m HasMore
    dropRemaining key = do
        (_, hasMore) <- dropWhileKey resolve readers key
        pure hasMore

{-# SPECIALISE dropWhileKey ::
     ResolveSerialisedValue
  -> Readers IO h
  -> SerialisedKey
  -> IO (Int, HasMore) #-}
-- | Drop all entries with a key that is smaller or equal to the supplied one.
dropWhileKey ::
     (MonadMask m, MonadSTM m, MonadST m)
  => ResolveSerialisedValue
  -> Readers m h
  -> SerialisedKey
  -> m (Int, HasMore)  -- ^ How many were dropped?
dropWhileKey resolve Readers {..} key = do
    cur <- readMutVar readersNext
    if readCtxHeadKey cur <= key
      then go 0 cur
      else pure (0, HasMore)  -- nothing to do
  where
    -- invariant: @readCtxHeadKey <= key@
    go !n ReadCtx {readCtxNumber, readCtxReader} = do
        mNext <- nextReadCtx resolve readCtxNumber readCtxReader >>= \case
          Nothing  -> Heap.extract readersHeap
          Just ctx -> Just <$> Heap.replaceRoot readersHeap ctx
        let !n' = n + 1
        case mNext of
          Nothing -> do
            pure (n', Drained)
          Just next -> do
            -- hasMore
            if readCtxHeadKey next <= key
              then
                go n' next
              else do
                writeMutVar readersNext next
                pure (n', HasMore)

{-# SPECIALISE dropOne ::
     ResolveSerialisedValue
  -> Readers IO h
  -> ReaderNumber
  -> Reader IO h
  -> IO HasMore #-}
dropOne ::
     (MonadMask m, MonadSTM m, MonadST m)
  => ResolveSerialisedValue
  -> Readers m h
  -> ReaderNumber
  -> Reader m h
  -> m HasMore
dropOne resolve Readers {..} number reader = do
    mNext <- nextReadCtx resolve number reader >>= \case
      Nothing  -> Heap.extract readersHeap
      Just ctx -> Just <$> Heap.replaceRoot readersHeap ctx
    case mNext of
      Nothing ->
        pure Drained
      Just next -> do
        writeMutVar readersNext next
        pure HasMore

{-# SPECIALISE nextReadCtx ::
     ResolveSerialisedValue
  -> ReaderNumber
  -> Reader IO h
  -> IO (Maybe (ReadCtx IO h)) #-}
nextReadCtx ::
     (MonadMask m, MonadSTM m, MonadST m)
  => ResolveSerialisedValue
  -> ReaderNumber
  -> Reader m h
  -> m (Maybe (ReadCtx m h))
nextReadCtx resolve readCtxNumber readCtxReader =
    case readCtxReader of
      ReadBuffer r -> atomicModifyMutVar r $ \case
        [] ->
          ([], Nothing)
        ((readCtxHeadKey, e) : rest) ->
          let readCtxHeadEntry = RunReader.Entry e
          in  (rest, Just ReadCtx {..})
      ReadRun r -> RunReader.next r <&> \case
        RunReader.Empty ->
          Nothing
        RunReader.ReadEntry readCtxHeadKey readCtxHeadEntry ->
          Just ReadCtx {..}
      ReadReaders mergeType readersMay -> case readersMay of
        SNothing ->
          pure Nothing
        SJust readers -> do
          (readCtxHeadKey, readCtxHeadEntry, hasMore) <-
            popResolved resolve mergeType readers
          let readersMay' = case hasMore of
                Drained -> SNothing
                HasMore -> SJust readers
          pure $ Just ReadCtx {
              -- TODO: reduce allocations?
              readCtxReader = ReadReaders mergeType readersMay'
            , ..
          }
