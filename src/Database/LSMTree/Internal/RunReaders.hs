module Database.LSMTree.Internal.RunReaders (
    Readers (..)
  , OffsetKey (..)
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
import qualified Data.Vector as V
import           Database.LSMTree.Internal.BlobRef (RawBlobRef)
import           Database.LSMTree.Internal.Entry (Entry (..))
import           Database.LSMTree.Internal.Run (Run)
import           Database.LSMTree.Internal.RunReader (OffsetKey (..),
                     RunReader (..))
import qualified Database.LSMTree.Internal.RunReader as Reader
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
-- Creating a 'RunReaders' does not increase the runs' reference count, so make
-- sure they remain open while using the 'RunReaders'.
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
    , readCtxHeadEntry :: !(Reader.Entry m h)
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

-- TODO: This is slightly inelegant. This module could work generally for
-- anything that can produce elements, but currently is very specific to having
-- write buffer and run readers. Also, for run merging, no write buffer is
-- involved, but we still need to branch on this sum type.
-- A more general version is possible, but despite SPECIALISE-ing everything
-- showed ~100 bytes of extra allocations per entry that is read (which might be
-- avoidable with some tinkering).
data Reader m h =
    ReadRun    !(RunReader m h)
    -- The list allows to incrementally read from the write buffer without
    -- having to find the next entry in the Map again (requiring key
    -- comparisons) or having to copy out all entries.
    -- TODO: more efficient representation? benchmark!
  | ReadBuffer !(MutVar (PrimState m) [KOp m h])

type KOp m h = (SerialisedKey, Entry SerialisedValue (RawBlobRef m h))

{-# SPECIALISE new ::
     OffsetKey
  -> Maybe (WB.WriteBuffer, Ref (WB.WriteBufferBlobs IO h))
  -> V.Vector (Ref (Run IO h))
  -> IO (Maybe (Readers IO h)) #-}
new :: forall m h.
     (MonadMask m, MonadST m, MonadSTM m)
  => OffsetKey
  -> Maybe (WB.WriteBuffer, Ref (WB.WriteBufferBlobs m h))
  -> V.Vector (Ref (Run m h))
  -> m (Maybe (Readers m h))
new !offsetKey wbs runs = do
    wBuffer <- maybe (pure Nothing) (uncurry fromWB) wbs
    readers <- zipWithM (fromRun . ReaderNumber) [1..] (V.toList runs)
    let contexts = nonEmpty . catMaybes $ wBuffer : readers
    for contexts $ \xs -> do
      (readersHeap, readCtx) <- Heap.newMutableHeap xs
      readersNext <- newMutVar readCtx
      return Readers {..}
  where
    fromWB :: WB.WriteBuffer
           -> Ref (WB.WriteBufferBlobs m h)
           -> m (Maybe (ReadCtx m h))
    fromWB wb wbblobs = do
        --TODO: this BlobSpan to BlobRef conversion involves quite a lot of allocation
        kops <- newMutVar $ map (fmap (fmap (WB.mkRawBlobRef wbblobs))) $
                  Map.toList $ filterWB $ WB.toMap wb
        nextReadCtx (ReaderNumber 0) (ReadBuffer kops)
      where
        filterWB = case offsetKey of
            NoOffsetKey -> id
            OffsetKey k -> Map.dropWhileAntitone (< k)

    fromRun :: ReaderNumber -> Ref (Run m h) -> m (Maybe (ReadCtx m h))
    fromRun n run = do
        reader <- Reader.new offsetKey run
        nextReadCtx n (ReadRun reader)

{-# SPECIALISE close ::
     Readers IO (FS.Handle h)
  -> IO () #-}
-- | Only call when aborting before all readers have been drained.
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
        ReadRun r    -> Reader.close r
        ReadBuffer _ -> pure ()
    closeHeap =
        Heap.extract readersHeap >>= \case
          Nothing -> return ()
          Just ReadCtx {readCtxReader} -> do
            closeReader readCtxReader
            closeHeap

{-# SPECIALISE peekKey ::
     Readers IO h
  -> IO SerialisedKey #-}
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
    Readers IO h
  -> IO (SerialisedKey, Reader.Entry IO h, HasMore) #-}
pop ::
     (MonadMask m, MonadSTM m, MonadST m)
  => Readers m h
  -> m (SerialisedKey, Reader.Entry m h, HasMore)
pop r@Readers {..} = do
    ReadCtx {..} <- readMutVar readersNext
    hasMore <- dropOne r readCtxNumber readCtxReader
    return (readCtxHeadKey, readCtxHeadEntry, hasMore)

{-# SPECIALISE dropWhileKey ::
     Readers IO h
  -> SerialisedKey
  -> IO (Int, HasMore) #-}
dropWhileKey ::
     (MonadMask m, MonadSTM m, MonadST m)
  => Readers m h
  -> SerialisedKey
  -> m (Int, HasMore)  -- ^ How many were dropped?
dropWhileKey Readers {..} key = do
    cur <- readMutVar readersNext
    if readCtxHeadKey cur == key
      then go 0 cur
      else return (0, HasMore)  -- nothing to do
  where
    -- invariant: @readCtxHeadKey == key@
    go !n ReadCtx {readCtxNumber, readCtxReader} = do
        mNext <- nextReadCtx readCtxNumber readCtxReader >>= \case
          Nothing  -> Heap.extract readersHeap
          Just ctx -> Just <$> Heap.replaceRoot readersHeap ctx
        let !n' = n + 1
        case mNext of
          Nothing -> do
            return (n', Drained)
          Just next -> do
            -- hasMore
            if readCtxHeadKey next == key
              then
                go n' next
              else do
                writeMutVar readersNext next
                return (n', HasMore)

{-# SPECIALISE dropOne ::
     Readers IO h
  -> ReaderNumber
  -> Reader IO h
  -> IO HasMore #-}
dropOne ::
     (MonadMask m, MonadSTM m, MonadST m)
  => Readers m h
  -> ReaderNumber
  -> Reader m h
  -> m HasMore
dropOne Readers {..} number reader = do
    mNext <- nextReadCtx number reader >>= \case
      Nothing  -> Heap.extract readersHeap
      Just ctx -> Just <$> Heap.replaceRoot readersHeap ctx
    case mNext of
      Nothing ->
        return Drained
      Just next -> do
        writeMutVar readersNext next
        return HasMore

{-# SPECIALISE nextReadCtx ::
     ReaderNumber
  -> Reader IO h
  -> IO (Maybe (ReadCtx IO h)) #-}
nextReadCtx ::
     (MonadMask m, MonadSTM m, MonadST m)
  => ReaderNumber
  -> Reader m h
  -> m (Maybe (ReadCtx m h))
nextReadCtx readCtxNumber readCtxReader =
    case readCtxReader of
      ReadRun r -> Reader.next r <&> \case
        Reader.Empty ->
          Nothing
        Reader.ReadEntry readCtxHeadKey readCtxHeadEntry ->
          Just ReadCtx {..}
      ReadBuffer r -> atomicModifyMutVar r $ \case
        [] ->
          ([], Nothing)
        ((readCtxHeadKey, e) : rest) ->
          let readCtxHeadEntry = Reader.Entry e
          in  (rest, Just ReadCtx {..})
