module Database.LSMTree.Internal.RunReaders (
    Readers (..)
  , new
  , newAtOffset
  , newAtOffsetMaybe
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
import           Control.Monad.Primitive
import           Data.Function (on)
import           Data.Functor ((<&>))
import           Data.List.NonEmpty (nonEmpty)
import qualified Data.Map.Strict as Map
import           Data.Maybe (catMaybes)
import           Data.Primitive.MutVar
import           Data.Traversable (for)
import           Database.LSMTree.Internal.BlobRef (BlobRef)
import           Database.LSMTree.Internal.Entry (Entry (..))
import           Database.LSMTree.Internal.Run (Run)
import           Database.LSMTree.Internal.RunReader (RunReader)
import qualified Database.LSMTree.Internal.RunReader as Reader
import           Database.LSMTree.Internal.Serialise
import qualified Database.LSMTree.Internal.WriteBuffer as WB
import qualified KMerge.Heap as Heap
import qualified System.FS.API as FS
import           System.FS.API (HasFS)
import           System.FS.BlockIO.API (HasBlockIO)

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
data Readers m fhandle = Readers {
      readersHeap :: !(Heap.MutableHeap (PrimState m) (ReadCtx m fhandle))
      -- | Since there is always one reader outside of the heap, we need to
      -- store it separately. This also contains the next k\/op to yield, unless
      -- all readers are drained, i.e. both:
      -- 1. the reader inside the 'ReadCtx' is empty
      -- 2. the heap is empty
    , readersNext :: !(MutVar (PrimState m) (ReadCtx m fhandle))
    }

newtype ReaderNumber = ReaderNumber Int
  deriving stock (Eq, Ord)

-- | Each heap element needs some more context than just the reader.
-- E.g. the 'Eq' instance we need to be able to access the first key to be read
-- in a pure way.
--
-- TODO(optimisation): We allocate this record for each k/op. This might be
-- avoidable, see ideas below.
data ReadCtx m fhandle = ReadCtx {
      -- We could avoid this using a more specialised mutable heap with separate
      -- arrays for keys and values (or even each of their components).
      -- Using an 'STRef' could avoid reallocating the record for every entry,
      -- but that might not be straightforward to integrate with the heap.
      readCtxHeadKey   :: !SerialisedKey
    , readCtxHeadEntry :: !(Reader.Entry m fhandle)
      -- We could get rid of this by making 'LoserTree' stable (for which there
      -- is a prototype already).
      -- Alternatively, if we decide to have an invariant that the number in
      -- 'RunFsPaths' is always higher for newer runs, then we could use that
      -- in the 'Ord' instance.
    , readCtxNumber    :: !ReaderNumber
    , readCtxReader    :: !(Reader m fhandle)
    }

instance Eq (ReadCtx m fhandle) where
  (==) = (==) `on` (\r -> (readCtxHeadKey r, readCtxNumber r))

-- | Makes sure we resolve entries in the right order.
instance Ord (ReadCtx m fhandle) where
  compare = compare `on` (\r -> (readCtxHeadKey r, readCtxNumber r))

-- TODO: This is slightly inelegant. This module could work generally for
-- anything that can produce elements, but currently is very specific to having
-- write buffer and run readers. Also, for run merging, no write buffer is
-- involved, but we still need to branch on this sum type.
-- A more general version is possible, but despite SPECIALISE-ing everything
-- showed ~100 bytes of extra allocations per entry that is read (which might be
-- avoidable with some tinkering).
data Reader m fhandle =
    ReadRun    !(RunReader m fhandle)
    -- The list allows to incrementally read from the write buffer without
    -- having to find the next entry in the Map again (requiring key
    -- comparisons) or having to copy out all entries.
    -- TODO: more efficient representation? benchmark!
  | ReadBuffer !(MutVar (PrimState m) [KOp m fhandle])

type KOp m fhandle = (SerialisedKey, Entry SerialisedValue (BlobRef m fhandle))

-- | On equal keys, elements from runs earlier in the list are yielded first.
-- This means that the list of runs should be sorted from new to old.
new :: forall h .
     HasFS IO h
  -> HasBlockIO IO h
  -> Maybe WB.WriteBuffer
  -> [Run IO (FS.Handle h)]
  -> IO (Maybe (Readers IO (FS.Handle h)))
new fs hbio = newAtOffsetMaybe fs hbio Nothing

-- | On equal keys, elements from runs earlier in the list are yielded first.
-- This means that the list of runs should be sorted from new to old.
newAtOffset :: forall h .
     HasFS IO h
  -> HasBlockIO IO h
  -> SerialisedKey  -- ^ offset
  -> Maybe WB.WriteBuffer
  -> [Run IO (FS.Handle h)]
  -> IO (Maybe (Readers IO (FS.Handle h)))
newAtOffset fs hbio offset = newAtOffsetMaybe fs hbio (Just offset)

newAtOffsetMaybe :: forall h .
     HasFS IO h
  -> HasBlockIO IO h
  -> Maybe SerialisedKey  -- ^ offset
  -> Maybe WB.WriteBuffer
  -> [Run IO (FS.Handle h)]
  -> IO (Maybe (Readers IO (FS.Handle h)))
newAtOffsetMaybe fs hbio mOffset wbs runs = do
    wBuffer <- maybe (pure Nothing) fromWB wbs
    readers <- zipWithM (fromRun . ReaderNumber) [1..] runs
    let contexts = nonEmpty . catMaybes $ wBuffer : readers
    for contexts $ \xs -> do
      (readersHeap, readCtx) <- Heap.newMutableHeap xs
      readersNext <- newMutVar readCtx
      return Readers {..}
  where
    fromWB :: WB.WriteBuffer -> IO (Maybe (ReadCtx IO (FS.Handle h)))
    fromWB wb = do
        kops <-
          newMutVar $ map (fmap errOnBlob) $ Map.toList $
            maybe id (\o -> Map.dropWhileAntitone (< o)) mOffset $
              WB.toMap wb
        nextReadCtx fs hbio (ReaderNumber 0) (ReadBuffer kops)

    fromRun :: ReaderNumber -> Run IO (FS.Handle h) -> IO (Maybe (ReadCtx IO (FS.Handle h)))
    fromRun n run = nextReadCtx fs hbio n . ReadRun =<< Reader.new fs hbio mOffset run

-- | TODO: remove once blob references are implemented
errOnBlob :: Entry SerialisedValue SerialisedBlob -> Entry SerialisedValue (BlobRef m h)
errOnBlob (Insert v)           = Insert v
errOnBlob (InsertWithBlob _ b) = error $ "RunReaders: blob references not supported: " ++ show b
errOnBlob (Mupdate v)          = Mupdate v
errOnBlob Delete               = Delete

-- | Only call when aborting before all readers have been drained.
close ::
     HasFS IO h
  -> HasBlockIO IO h
  -> Readers IO (FS.Handle h)
  -> IO ()
close fs hbio Readers {..} = do
    ReadCtx {readCtxReader} <- readMutVar readersNext
    closeReader readCtxReader
    closeHeap
  where
    closeReader = \case
        ReadRun r    -> Reader.close fs hbio r
        ReadBuffer _ -> pure ()
    closeHeap =
        Heap.extract readersHeap >>= \case
          Nothing -> return ()
          Just ReadCtx {readCtxReader} -> do
            closeReader readCtxReader
            closeHeap

peekKey ::
     Readers IO (FS.Handle h)
  -> IO SerialisedKey
peekKey Readers {..} = do
    readCtxHeadKey <$> readMutVar readersNext

-- | Once a function returned 'Drained', do not use the 'Readers' any more!
data HasMore = HasMore | Drained
  deriving stock (Eq, Show)

pop ::
     HasFS IO h
  -> HasBlockIO IO h
  -> Readers IO (FS.Handle h)
  -> IO (SerialisedKey, Reader.Entry IO (FS.Handle h), HasMore)
pop fs hbio r@Readers {..} = do
    ReadCtx {..} <- readMutVar readersNext
    hasMore <- dropOne fs hbio r readCtxNumber readCtxReader
    return (readCtxHeadKey, readCtxHeadEntry, hasMore)

dropWhileKey ::
     HasFS IO h
  -> HasBlockIO IO h
  -> Readers IO (FS.Handle h)
  -> SerialisedKey
  -> IO (Int, HasMore)  -- ^ How many were dropped?
dropWhileKey fs hbio Readers {..} key = do
    cur <- readMutVar readersNext
    if readCtxHeadKey cur == key
      then go 0 cur
      else return (0, HasMore)  -- nothing to do
  where
    -- invariant: @readCtxHeadKey == key@
    go !n ReadCtx {readCtxNumber, readCtxReader} = do
        mNext <- nextReadCtx fs hbio readCtxNumber readCtxReader >>= \case
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


dropOne ::
     HasFS IO h
  -> HasBlockIO IO h
  -> Readers IO (FS.Handle h)
  -> ReaderNumber
  -> Reader IO (FS.Handle h)
  -> IO HasMore
dropOne fs hbio Readers {..} number reader = do
    mNext <- nextReadCtx fs hbio number reader >>= \case
      Nothing  -> Heap.extract readersHeap
      Just ctx -> Just <$> Heap.replaceRoot readersHeap ctx
    case mNext of
      Nothing ->
        return Drained
      Just next -> do
        writeMutVar readersNext next
        return HasMore

nextReadCtx ::
     HasFS IO h
  -> HasBlockIO IO h
  -> ReaderNumber
  -> Reader IO (FS.Handle h)
  -> IO (Maybe (ReadCtx IO (FS.Handle h)))
nextReadCtx fs hbio readCtxNumber readCtxReader =
    case readCtxReader of
      ReadRun r -> Reader.next fs hbio r <&> \case
        Reader.Empty ->
          Nothing
        Reader.ReadEntry readCtxHeadKey readCtxHeadEntry ->
          Just ReadCtx {..}
      ReadBuffer r -> atomicModifyMutVar r $ \case
        [] ->
          ([], Nothing)
        ((readCtxHeadKey, e) : rest) ->
          let readCtxHeadEntry = Reader.Entry e
          in (rest, Just ReadCtx {..})
