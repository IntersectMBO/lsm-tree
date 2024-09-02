module Database.LSMTree.Internal.RunReaders (
    Readers (..)
  , ReaderNumber (..)
  , ReadCtx (..)
  , new
  , newAtOffset
  , close
  , peekKey
  , HasMore (..)
  , pop
  , dropWhileKey
  ) where

import           Control.Monad (zipWithM)
import           Control.Monad.Primitive
import           Data.Function (on)
import           Data.List.NonEmpty (nonEmpty)
import           Data.Maybe (catMaybes)
import           Data.Primitive.MutVar
import           Data.Traversable (for)
import           Database.LSMTree.Internal.Run (Run)
import           Database.LSMTree.Internal.RunReader (RunReader)
import qualified Database.LSMTree.Internal.RunReader as Reader
import           Database.LSMTree.Internal.Serialise
import qualified KMerge.Heap as Heap
import qualified System.FS.API as FS
import           System.FS.API (HasFS)
import           System.FS.BlockIO.API (HasBlockIO)

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
data Readers s fhandle = Readers {
      readersHeap :: !(Heap.MutableHeap s (ReadCtx fhandle))
      -- | Since there is always one reader outside of the heap, we need to
      -- store it separately. This also contains the next k\/op to yield, unless
      -- all readers are drained, i.e. both:
      -- 1. the reader inside the 'ReadCtx' is empty
      -- 2. the heap is empty
    , readersNext :: !(MutVar s (ReadCtx fhandle))
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
    , readCtxHeadEntry :: !(Reader.Entry IO fhandle)
      -- We could get rid of this by making 'LoserTree' stable (for which there
      -- is a prototype already).
      -- Alternatively, if we decide to have an invariant that the number in
      -- 'RunFsPaths' is always higher for newer runs, then we could use that
      -- in the 'Ord' instance.
    , readCtxNumber    :: !ReaderNumber
    , readCtxReader    :: !(RunReader IO fhandle)
    }

instance Eq (ReadCtx fhandle) where
  (==) = (==) `on` (\r -> (readCtxHeadKey r, readCtxNumber r))

-- | Makes sure we resolve entries in the right order.
instance Ord (ReadCtx fhandle) where
  compare = compare `on` (\r -> (readCtxHeadKey r, readCtxNumber r))

-- | On equal keys, elements from runs earlier in the list are yielded first.
-- This means that the list of runs should be sorted from new to old.
new :: forall h .
     HasFS IO h
  -> HasBlockIO IO h
  -> [Run IO (FS.Handle h)]
  -> IO (Maybe (Readers RealWorld (FS.Handle h)))
new = newAtOffsetMaybe Nothing

-- | On equal keys, elements from runs earlier in the list are yielded first.
-- This means that the list of runs should be sorted from new to old.
-- TODO: merge with 'new'?
newAtOffset :: forall h .
     HasFS IO h
  -> HasBlockIO IO h
  -> SerialisedKey  -- ^ offset
  -> [Run IO (FS.Handle h)]
  -> IO (Maybe (Readers RealWorld (FS.Handle h)))
newAtOffset fs hbio offset = newAtOffsetMaybe (Just offset) fs hbio

newAtOffsetMaybe :: forall h .
     Maybe SerialisedKey  -- ^ offset
  -> HasFS IO h
  -> HasBlockIO IO h
  -> [Run IO (FS.Handle h)]
  -> IO (Maybe (Readers RealWorld (FS.Handle h)))
newAtOffsetMaybe  offsetMay fs hbio runs = do
    readers <- zipWithM (fromRun . ReaderNumber) [1..] runs
    for (nonEmpty (catMaybes readers)) $ \xs -> do
      (readersHeap, readCtx) <- Heap.newMutableHeap xs
      readersNext <- newMutVar readCtx
      return Readers {..}
  where
    fromRun :: ReaderNumber -> Run IO (FS.Handle h) -> IO (Maybe (ReadCtx (FS.Handle h)))
    fromRun n run = nextReadCtx fs hbio n =<< Reader.new fs hbio offsetMay run

-- | Only call when aborting before all readers have been drained.
close ::
     HasFS IO h
  -> HasBlockIO IO h
  -> Readers RealWorld (FS.Handle h)
  -> IO ()
close fs hbio Readers {..} = do
    ReadCtx {readCtxReader} <- readMutVar readersNext
    Reader.close fs hbio readCtxReader
    closeHeap
  where
    closeHeap =
        Heap.extract readersHeap >>= \case
          Nothing -> return ()
          Just ReadCtx {readCtxReader} -> do
            Reader.close fs hbio readCtxReader
            closeHeap

peekKey ::
     Readers RealWorld (FS.Handle h)
  -> IO SerialisedKey
peekKey Readers {..} = do
    readCtxHeadKey <$> readMutVar readersNext

-- | Once a function returned 'Drained', do not use the 'Readers' any more!
data HasMore = HasMore | Drained
  deriving stock (Eq, Show)

pop ::
     HasFS IO h
  -> HasBlockIO IO h
  -> Readers RealWorld (FS.Handle h)
  -> IO (SerialisedKey, Reader.Entry IO (FS.Handle h), HasMore)
pop fs hbio r@Readers {..} = do
    ReadCtx {..} <- readMutVar readersNext
    hasMore <- dropOne fs hbio r readCtxNumber readCtxReader
    return (readCtxHeadKey, readCtxHeadEntry, hasMore)

dropWhileKey ::
     HasFS IO h
  -> HasBlockIO IO h
  -> Readers RealWorld (FS.Handle h)
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
  -> Readers RealWorld (FS.Handle h)
  -> ReaderNumber
  -> RunReader IO (FS.Handle h)
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
  -> RunReader IO (FS.Handle h)
  -> IO (Maybe (ReadCtx (FS.Handle h)))
nextReadCtx fs hbio readCtxNumber readCtxReader = do
    res <- Reader.next fs hbio readCtxReader
    case res of
      Reader.Empty -> do
        return Nothing
      Reader.ReadEntry readCtxHeadKey readCtxHeadEntry ->
        return (Just ReadCtx {..})
