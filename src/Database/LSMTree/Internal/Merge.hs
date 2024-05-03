{-# LANGUAGE BangPatterns     #-}
{-# LANGUAGE LambdaCase       #-}
{-# LANGUAGE NamedFieldPuns   #-}
{-# LANGUAGE RecordWildCards  #-}
{-# LANGUAGE TypeApplications #-}

module Database.LSMTree.Internal.Merge (
    Merge (..)
  , Level (..)
  , Mappend
  , new
  , StepResult (..)
  , steps
  , close
  ) where

import           Control.Exception (assert)
import           Control.Monad (when, zipWithM)
import           Control.Monad.Primitive (RealWorld)
import           Data.Bifunctor (first)
import           Data.Coerce (coerce)
import           Data.Function (on)
import           Data.IORef
import           Data.Maybe (catMaybes)
import           Data.Traversable (for)
import           Data.Word (Word32)
import           Database.LSMTree.Internal.BitMath
import           Database.LSMTree.Internal.BlobRef (BlobRef (..))
import           Database.LSMTree.Internal.Entry
import           Database.LSMTree.Internal.PageAcc (entryWouldFitInPage)
import qualified Database.LSMTree.Internal.RawBytes as RB
import           Database.LSMTree.Internal.RawOverflowPage (RawOverflowPage,
                     rawOverflowPageRawBytes)
import           Database.LSMTree.Internal.RawPage
import           Database.LSMTree.Internal.Run (Run)
import qualified Database.LSMTree.Internal.Run as Run
import           Database.LSMTree.Internal.RunBuilder (RunBuilder)
import qualified Database.LSMTree.Internal.RunBuilder as Builder
import           Database.LSMTree.Internal.RunReader (RunReader)
import qualified Database.LSMTree.Internal.RunReader as Reader
import           Database.LSMTree.Internal.Serialise
import qualified KMerge.Heap as Heap
import qualified System.FS.API as FS
import           System.FS.API (HasFS)

-- | An in-progress incremental k-way merge of 'Run's.
--
-- Each step reads exactly one entry and potentially writes one entry.
--
-- TODO: Try the other approach:
-- Always produce one entry (which could use multiple inputs of the same key).
--
-- TODO: Reference counting will have to be done somewhere, either here or in
-- the layer above.
data Merge fhandle = Merge {
      mergeLevel        :: !Level
    , mergeMappend      :: !Mappend
    , mergeReaders      :: {-# UNPACK #-} !(Readers fhandle)
    , mergeBuilder      :: !(RunBuilder fhandle)
      -- | The key most recently read from 'mergeReaders'.
    , mergeCurrentKey   :: !(IORef SerialisedKey)
      -- | The entry most recently read from 'mergeReaders', which potentially
      -- still has to be resolved with the next ones (if their keys match).
    , mergeCurrentEntry :: !(IORef (PrefixEntry fhandle))
    }

data Level = MidLevel | LastLevel
  deriving (Eq, Show)

type Mappend = SerialisedValue -> SerialisedValue -> SerialisedValue

-- | Invariant: 'Mupdate' entries are always 'FullEntry'.
data PrefixEntry fhandle
    -- | The value is fully in memory, but can still be large, e.g. when it was
    -- created by a 'Mupdate' operation.
  = FullEntry !(Entry SerialisedValue (BlobRef (Run fhandle)))
    -- | The 'Entry' contains just a prefix of the value, the rest is in the
    -- (non-zero) overflow pages. This entry can only come from the input run.
  | OverflowEntry
      !(Entry SerialisedValue (BlobRef (Run fhandle)))
      !RawPage
      !Word32
      ![RawOverflowPage]

prefixEntryInvariant :: PrefixEntry fhandle -> Bool
prefixEntryInvariant FullEntry{} = True
prefixEntryInvariant (OverflowEntry e page lenSuffix overflowPages)
  | Mupdate _ <- e = False
  | otherwise =
       not (null overflowPages)
    && rawPageOverflowPages page == ceilDivPageSize (fromIntegral lenSuffix)
    && rawPageOverflowPages page == length overflowPages

toFullEntry :: PrefixEntry fhandle -> Entry SerialisedValue (BlobRef (Run fhandle))
toFullEntry (FullEntry e) =
    e
toFullEntry (OverflowEntry e _ lenSuffix overflowPages) =
    first appendSuffix e
  where
    appendSuffix (SerialisedValue prefix) =
      SerialisedValue $ RB.take (RB.size prefix + fromIntegral lenSuffix) $
        mconcat (prefix : map rawOverflowPageRawBytes overflowPages)

-- | Returns 'Nothing' if no input 'Run' contains any entries.
-- The list of runs should be sorted from new to old.
new ::
     HasFS IO h
  -> Level
  -> Mappend
  -> Run.RunFsPaths
  -> [Run (FS.Handle h)]
  -> IO (Maybe (Merge (FS.Handle h)))
new fs mergeLevel mergeMappend targetPaths runs = do
    mreaders <- newReaders fs runs
    for mreaders $ \(mergeReaders, firstKey, firstEntry) -> do
      mergeCurrentKey <- newIORef firstKey
      mergeCurrentEntry <- newIORef firstEntry

      -- calculate upper bounds based on input runs
      let numEntries = coerce (sum @[] @Int) (map Run.runNumEntries runs)
      let numPages = sum (map Run.sizeInPages runs)
      mergeBuilder <- Builder.new fs targetPaths numEntries numPages

      return Merge {..}

data StepResult fhandle = MergeInProgress | MergeComplete !(Run fhandle)

-- | Do a fixed number of steps of merging. Each step reads a single entry, then
-- either resolves it with the previous one or writes the previous one out to
-- the run being created.
--
-- The resulting run has a reference count of 1.
--
-- Do not call again after 'MergeComplete' has been returned!
steps ::
     HasFS IO h
  -> Merge (FS.Handle h)
  -> Int
  -> IO (StepResult (FS.Handle h))
steps fs Merge {..} = \numSteps -> do
    curKey <- readIORef mergeCurrentKey
    curEntry <- readIORef mergeCurrentEntry
    go curKey curEntry numSteps
  where
    go !curKey !curEntry n
      | n <= 0 = do
          writeIORef mergeCurrentKey curKey
          writeIORef mergeCurrentEntry curEntry
          return MergeInProgress
    go !curKey !curEntry !n =
      getFromReaders fs mergeReaders >>= \case
        Just (newKey, newEntry) | newKey == curKey -> do
          -- still same key: don't write yet, resolve!
          let resolvedEntry = resolveEntries mergeMappend curEntry newEntry
          go curKey resolvedEntry (n-1)
        Just (newKey, newEntry) -> do
          -- new key: write old entry!
          writeEntry fs mergeLevel mergeBuilder curKey curEntry
          go newKey newEntry (n-1)
        Nothing -> do
          -- finished reading: write last entry!
          writeEntry fs mergeLevel mergeBuilder curKey curEntry
          run <- Run.fromMutable fs (Run.RefCount 1) mergeBuilder
          -- All Readers have been drained, the builder finalised.
          -- No further cleanup required.
          return (MergeComplete run)

resolveEntries :: Mappend -> PrefixEntry h -> PrefixEntry h -> PrefixEntry h
resolveEntries mrg e1 e2 =
    assert (prefixEntryInvariant e1) $
    assert (prefixEntryInvariant e2) $
    case e1 of
      FullEntry (Mupdate v1) ->
        -- for monoidal resolution, we need the full second value to be present
        FullEntry (combine mrg (Mupdate v1) (toFullEntry e2))
      _ ->
        -- otherwise, just take the first one (OverflowEntry can't be Mupdate!)
        e1

-- | This function should be called when discarding a 'Merge' before it
-- was done (i.e. returned 'MergeComplete'). This removes the incomplete files
-- created for the new run so far and avoids leaking file handles.
--
-- Once it has been called, do not use the 'Merge' any more!
close ::
     HasFS IO h
  -> Merge (FS.Handle h)
  -> IO ()
close fs Merge {..} = do
    Builder.close fs mergeBuilder
    closeReaders fs mergeReaders

{-------------------------------------------------------------------------------
  Read
-------------------------------------------------------------------------------}

-- | Abstraction for the collection of 'RunReader', yielding elements in order.
-- More precisely, that means first ordered by their key, then by the input
-- run they came from. This is important for resolving multiple entries with the
-- same key into one.
--
-- Construct with 'newReaders', then keep calling 'getFromReaders'.
-- If aborting early, remember to call 'closeReaders'!
data Readers fhandle = Readers {
      readersHeap     :: !(Heap.MutableHeap RealWorld (ReadCtx fhandle))
      -- | Since there is always one reader outside of the heap, we need to
      -- store it separately.
    , readersLastRead :: !(IORef (RunReader fhandle, ReaderNumber))
    }

newtype ReaderNumber = ReaderNumber Int
  deriving (Eq, Ord)

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
    , readCtxHeadEntry :: !(PrefixEntry fhandle)
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
newReaders ::
     HasFS IO h
  -> [Run (FS.Handle h)]
  -> IO (Maybe (Readers (FS.Handle h), SerialisedKey, PrefixEntry (FS.Handle h)))
newReaders fs runs = do
    readers <- zipWithM (fromRun . ReaderNumber) [1..] runs
    (readersHeap, firstReadCtx) <- Heap.newMutableHeap (catMaybes readers)
    for firstReadCtx $ \ReadCtx {..} -> do
      readersLastRead <- newIORef (readCtxReader, readCtxNumber)
      return (Readers {..}, readCtxHeadKey, readCtxHeadEntry)
  where
    fromRun n run = nextReadCtx fs n =<< Reader.new fs run

getFromReaders ::
     HasFS IO h
  -> Readers (FS.Handle h)
  -> IO (Maybe (SerialisedKey, PrefixEntry (FS.Handle h)))
getFromReaders fs Readers {..} = do
    (reader, n) <- readIORef readersLastRead
    mNext <- nextReadCtx fs n reader >>= \case
      Nothing  -> Heap.extract readersHeap
      Just ctx -> Just <$> Heap.replaceRoot readersHeap ctx
    for mNext $ \ReadCtx {..} -> do
      writeIORef readersLastRead (readCtxReader, readCtxNumber)
      return (readCtxHeadKey, readCtxHeadEntry)

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
      Reader.ReadSmallEntry readCtxHeadKey entry -> do
        -- Note that this small entry could be the only one on the page. We
        -- only care about it being small, not single-entry, since it could
        -- still end up sharing a page with other entries in the merged run.
        -- TODO(optimise): This currently doesn't fully exploit the case where
        -- there is a single page small entry on the page which again ends up
        -- as the only entry of a page. In that case we could have copied the
        -- RawPage (but we find out too late to easily exploit it).
        let readCtxHeadEntry = FullEntry entry
        return (Just ReadCtx {..})
      Reader.ReadLargeEntry readCtxHeadKey entry rawPage lenSuffix overflowPages -> do
        let overflowEntry = OverflowEntry entry rawPage lenSuffix overflowPages
        let readCtxHeadEntry = case entry of
              Mupdate _ -> FullEntry (toFullEntry overflowEntry)
              _         -> overflowEntry
        return (Just ReadCtx {..})

-- | Only call when aborting before all readers have been drained.
closeReaders ::
     HasFS IO h
  -> Readers (FS.Handle h)
  -> IO ()
closeReaders fs Readers {..} = do
    (reader, _) <- readIORef readersLastRead
    Reader.close fs reader
    go
  where
    go = Heap.extract readersHeap >>= \case
          Nothing -> return ()
          Just ReadCtx {readCtxReader} -> do
            Reader.close fs readCtxReader
            go

{-------------------------------------------------------------------------------
  Write
-------------------------------------------------------------------------------}

writeEntry ::
     HasFS IO h
  -> Level
  -> RunBuilder (FS.Handle h)
  -> SerialisedKey
  -> PrefixEntry (FS.Handle h)
  -> IO ()
writeEntry fs level builder key = \case
    FullEntry entry -> do
      -- simply add entry
      when (shouldWriteEntry entry) $
        Builder.addKeyOp fs builder key =<< traverse resolveBlobRef entry
    e@(OverflowEntry entryPrefix rawPage _ overflowPages) ->
      assert (entryWouldFitInPage key (fmap blobRefSpan entryPrefix)) $
      assert (prefixEntryInvariant e) $
      assert (shouldWriteEntry entryPrefix) $  -- Delete can't overflow
        if null entryPrefix  -- has blob?
        then do
          -- no blob, directly copy all pages as they are
          Builder.addLargeSerialisedKeyOp fs builder key rawPage overflowPages
        else do
          -- has blob, we can't just copy the first page, fall back
          -- we simply append the overflow pages to the value
          Builder.addKeyOp fs builder key
            =<< traverse resolveBlobRef (toFullEntry e)
          -- TODO(optimise): This copies the overflow pages unnecessarily.
          -- We could extend the RunBuilder API to allow to either:
          -- 1. write an Entry (containing the value prefix) + [RawOverflowPage]
          -- 2. write a RawPage + SerialisedBlob + [RawOverflowPage], rewriting
          --      the raw page's blob offset (slightly faster, but a bit hacky)
  where
    -- we could also turn Mupdate into Insert, but no need to complicate things
    shouldWriteEntry = case level of
        MidLevel -> const True
        LastLevel -> \case
          Delete -> False
          _      -> True

    resolveBlobRef (BlobRef run blobSpan) =
      Run.readBlob fs run blobSpan
