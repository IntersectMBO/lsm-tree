module Database.LSMTree.Internal.RunReaders (
    Readers (..)
  , new
  , close
  , peekKey
  , HasMore (..)
  , pop
  , dropWhileKey
  ) where

import           Control.Monad (zipWithM)
import           Control.Monad.Primitive (RealWorld)
import           Data.Function (on)
import           Data.IORef
import           Data.Maybe (catMaybes)
import           Data.Traversable (for)
import           Database.LSMTree.Internal.Run (Run)
import           Database.LSMTree.Internal.RunReader (RunReader)
import qualified Database.LSMTree.Internal.RunReader as Reader
import           Database.LSMTree.Internal.Serialise
import qualified KMerge.Heap as Heap
import qualified System.FS.API as FS
import           System.FS.API (HasFS)

-- | Abstraction for the collection of 'RunReader', yielding elements in order.
-- More precisely, that means first ordered by their key, then by the input
-- run they came from. This is important for resolving multiple entries with the
-- same key into one.
--
-- Construct with 'new', then keep calling 'pop'.
-- If aborting early, remember to call 'close'!
--
-- Creating a 'RunReaders' does not increase the runs' reference count, so make
-- sure they remain open while using the 'RunReaders'.
data Readers fhandle = Readers {
      readersHeap :: !(Heap.MutableHeap RealWorld (ReadCtx fhandle))
      -- | Since there is always one reader outside of the heap, we need to
      -- store it separately. This also contains the next k\/op to yield, unless
      -- all readers are drained, i.e. both:
      -- 1. the reader inside the 'ReadCtx' is empty
      -- 2. the heap is empty
    , readersNext :: !(IORef (ReadCtx fhandle))
    }

newtype ReaderNumber = ReaderNumber Int
  deriving stock (Eq, Ord)

-- | Each heap element needs some more context than just the reader.
-- E.g. the 'Eq' instance we need to be able to access the first key to be read
-- in a pure way.
--
-- TODO(optimisation): We allocate this record for each k/op. This might be
-- avoidable, see ideas below.
data ReadCtx fhandle = ReadCtx {
      -- We could avoid this using a more specialised mutable heap with separate
      -- arrays for keys and values (or even each of their components).
      -- Using an 'STRef' could avoid reallocating the record for every entry,
      -- but that might not be straightforward to integrate with the heap.
      readCtxHeadKey   :: !SerialisedKey
    , readCtxHeadEntry :: !(Reader.Entry fhandle)
      -- We could get rid of this by making 'LoserTree' stable (for which there
      -- is a prototype already).
      -- Alternatively, if we decide to have an invariant that the number in
      -- 'RunFsPaths' is always higher for newer runs, then we could use that
      -- in the 'Ord' instance.
    , readCtxNumber    :: !ReaderNumber
    , readCtxReader    :: !(RunReader fhandle)
    }

instance Eq (ReadCtx fhandle) where
  (==) = (==) `on` (\r -> (readCtxHeadKey r, readCtxNumber r))

-- | Makes sure we resolve entries in the right order.
instance Ord (ReadCtx fhandle) where
  compare = compare `on` (\r -> (readCtxHeadKey r, readCtxNumber r))

-- | On equal keys, elements from runs earlier in the list are yielded first.
-- This means that the list of runs should be sorted from new to old.
new ::
     HasFS IO h
  -> [Run (FS.Handle h)]
  -> IO (Maybe (Readers (FS.Handle h)))
new fs runs = do
    readers <- zipWithM (fromRun . ReaderNumber) [1..] runs
    (readersHeap, firstReadCtx) <- Heap.newMutableHeap (catMaybes readers)
    for firstReadCtx $ \readCtx -> do
      readersNext <- newIORef readCtx
      return Readers {..}
  where
    fromRun n run = nextReadCtx fs n =<< Reader.new fs run

-- | Only call when aborting before all readers have been drained.
close ::
     HasFS IO h
  -> Readers (FS.Handle h)
  -> IO ()
close fs Readers {..} = do
    ReadCtx {readCtxReader} <- readIORef readersNext
    Reader.close fs readCtxReader
    closeHeap
  where
    closeHeap =
        Heap.extract readersHeap >>= \case
          Nothing -> return ()
          Just ReadCtx {readCtxReader} -> do
            Reader.close fs readCtxReader
            closeHeap

peekKey ::
     Readers (FS.Handle h)
  -> IO SerialisedKey
peekKey Readers {..} = do
    readCtxHeadKey <$> readIORef readersNext

-- | Once a function returned 'Drained', do not use the 'Readers' any more!
data HasMore = HasMore | Drained
  deriving stock (Eq, Show)

pop ::
     HasFS IO h
  -> Readers (FS.Handle h)
  -> IO (SerialisedKey, Reader.Entry (FS.Handle h), HasMore)
pop fs r@Readers {..} = do
    ReadCtx {..} <- readIORef readersNext
    hasMore <- dropOne fs r readCtxNumber readCtxReader
    return (readCtxHeadKey, readCtxHeadEntry, hasMore)

dropWhileKey ::
     HasFS IO h
  -> Readers (FS.Handle h)
  -> SerialisedKey
  -> IO (Int, HasMore)  -- ^ How many were dropped?
dropWhileKey fs Readers {..} key = do
    cur <- readIORef readersNext
    if readCtxHeadKey cur == key
      then go 0 cur
      else return (0, HasMore)  -- nothing to do
  where
    -- invariant: @readCtxHeadKey == key@
    go !n ReadCtx {readCtxNumber, readCtxReader} = do
        mNext <- nextReadCtx fs readCtxNumber readCtxReader >>= \case
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
                writeIORef readersNext next
                return (n', HasMore)


dropOne ::
     HasFS IO h
  -> Readers (FS.Handle h)
  -> ReaderNumber
  -> RunReader (FS.Handle h)
  -> IO HasMore
dropOne fs Readers {..} number reader = do
    mNext <- nextReadCtx fs number reader >>= \case
      Nothing  -> Heap.extract readersHeap
      Just ctx -> Just <$> Heap.replaceRoot readersHeap ctx
    case mNext of
      Nothing ->
        return Drained
      Just next -> do
        writeIORef readersNext next
        return HasMore

nextReadCtx ::
     HasFS IO h
  -> ReaderNumber
  -> RunReader (FS.Handle h)
  -> IO (Maybe (ReadCtx (FS.Handle h)))
nextReadCtx fs readCtxNumber readCtxReader = do
    res <- Reader.next fs readCtxReader
    case res of
      Reader.Empty -> do
        return Nothing
      Reader.ReadEntry readCtxHeadKey readCtxHeadEntry ->
        return (Just ReadCtx {..})
