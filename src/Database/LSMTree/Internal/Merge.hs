-- | The 'Merge' type and its functions are not intended for concurrent use.
-- Concurrent access should therefore be sequentialised using a suitable
-- concurrency primitive, such as an 'MVar'.
module Database.LSMTree.Internal.Merge (
    Merge (..)
  , Level (..)
  , Mappend
  , new
  , close
  , complete
  , stepsToCompletion
  , stepsToCompletionCounted
  , StepResult (..)
  , steps
  ) where

import           Control.Exception (assert)
import           Control.Monad (when)
import           Control.Monad.Primitive (PrimState, RealWorld)
import           Control.RefCount (RefCount (..))
import           Data.Coerce (coerce)
import           Data.Primitive.MutVar
import           Data.Traversable (for)
import qualified Data.Vector as V
import           Database.LSMTree.Internal.BlobRef (BlobRef)
import           Database.LSMTree.Internal.Entry
import           Database.LSMTree.Internal.Run (Run, RunDataCaching)
import qualified Database.LSMTree.Internal.Run as Run
import           Database.LSMTree.Internal.RunAcc (RunBloomFilterAlloc (..))
import           Database.LSMTree.Internal.RunBuilder (RunBuilder)
import qualified Database.LSMTree.Internal.RunBuilder as Builder
import qualified Database.LSMTree.Internal.RunReader as Reader
import           Database.LSMTree.Internal.RunReaders (Readers)
import qualified Database.LSMTree.Internal.RunReaders as Readers
import           Database.LSMTree.Internal.Serialise
import qualified System.FS.API as FS
import           System.FS.API (HasFS)
import           System.FS.BlockIO.API (HasBlockIO)

-- | An in-progress incremental k-way merge of 'Run's.
--
-- Since we always resolve all entries of the same key in one go, there is no
-- need to store incompletely-resolved entries.
--
-- TODO: Reference counting will have to be done somewhere, either here or in
-- the layer above.
data Merge m h = Merge {
      mergeLevel          :: !Level
    , mergeMappend        :: !Mappend
    , mergeReaders        :: {-# UNPACK #-} !(Readers m (FS.Handle h))
    , mergeBuilder        :: !(RunBuilder (PrimState m) (FS.Handle h))
      -- | The caching policy to use for the Run in the 'MergeComplete'.
    , mergeCaching        :: !RunDataCaching
      -- | The result of the latest call to 'steps'. This is used to determine
      -- whether a merge can be 'complete'd.
    , mergeLastStepResult :: !(MutVar (PrimState m) StepResult)
    , mergeHasFS          :: !(HasFS m h)
    , mergeHasBlockIO     :: !(HasBlockIO m h)
    }

data Level = MidLevel | LastLevel
  deriving stock (Eq, Show)

type Mappend = SerialisedValue -> SerialisedValue -> SerialisedValue

-- | Returns 'Nothing' if no input 'Run' contains any entries.
-- The list of runs should be sorted from new to old.
new ::
     HasFS IO h
  -> HasBlockIO IO h
  -> RunDataCaching
  -> RunBloomFilterAlloc
  -> Level
  -> Mappend
  -> Run.RunFsPaths
  -> V.Vector (Run IO (FS.Handle h))
  -> IO (Maybe (Merge IO h))
new fs hbio mergeCaching alloc mergeLevel mergeMappend targetPaths runs = do
    -- no offset, no write buffer
    mreaders <- Readers.new fs hbio Readers.NoOffsetKey Nothing runs
    for mreaders $ \mergeReaders -> do
      -- calculate upper bounds based on input runs
      let numEntries = coerce (sum @V.Vector @Int) (fmap Run.runNumEntries runs)
      mergeBuilder <- Builder.new fs targetPaths numEntries alloc
      mergeLastStepResult <- newMutVar $! MergeInProgress
      return Merge {
          mergeHasFS = fs
        , mergeHasBlockIO = hbio
        , ..
        }


-- | This function should be called when discarding a 'Merge' before it
-- was done (i.e. returned 'MergeComplete'). This removes the incomplete files
-- created for the new run so far and avoids leaking file handles.
--
-- Once it has been called, do not use the 'Merge' any more!
close :: Merge IO h -> IO ()
close Merge {..} = do
    Builder.close mergeHasFS mergeBuilder
    Readers.close mergeHasFS mergeHasBlockIO mergeReaders

-- | Complete a 'Merge', returning a new 'Run' as the result of merging the
-- input runs. This function will /not/ do any merging work if there is any
-- remaining. That is, if not enough 'steps' were performed to exhaust the input
-- 'Readers', this function will throw an error.
--
-- Note: this function creates new 'Run' resources, so it is recommended to run
-- this function with async exceptions masked. Otherwise, these resources can
-- leak.
complete :: Merge IO h -> IO (Run IO (FS.Handle h))
complete Merge{..} = do
    readMutVar mergeLastStepResult >>= \case
      MergeInProgress -> error "complete: Merge is not yet completed!"
      MergeComplete -> do
        Run.fromMutable mergeHasFS mergeHasBlockIO mergeCaching
                        (RefCount 1) mergeBuilder

-- | Like 'steps', but calling 'complete' once the merge is finished.
--
-- Note: run with async exceptions masked. See 'complete'.
stepsToCompletion :: Merge IO h -> Int -> IO (Run IO (FS.Handle h))
stepsToCompletion m stepBatchSize = go
  where
    go = do
      steps m stepBatchSize >>= \case
        (_, MergeInProgress) -> go
        (_, MergeComplete)   -> complete m

-- | Like 'steps', but calling 'complete' once the merge is finished.
--
-- Note: run with async exceptions masked. See 'complete'.
stepsToCompletionCounted :: Merge IO h -> Int -> IO (Int, Run IO (FS.Handle h))
stepsToCompletionCounted m stepBatchSize = go 0
  where
    go !stepsSum = do
      steps m stepBatchSize >>= \case
        (n, MergeInProgress) -> go (stepsSum + n)
        (n, MergeComplete)   -> let !stepsSum' = stepsSum + n
                                in (stepsSum',) <$> complete m

data StepResult = MergeInProgress | MergeComplete

stepsInvariant :: Int -> (Int, StepResult) -> Bool
stepsInvariant requestedSteps = \case
    (n, MergeInProgress) -> n >= requestedSteps
    _                    -> True

-- | Do at least a given number of steps of merging. Each step reads a single
-- entry, then either resolves the previous entry with the new one or writes it
-- out to the run being created. Since we always finish resolving a key we
-- started, we might do slightly more work than requested.
--
-- Returns the number of input entries read, which is guaranteed to be at least
-- as many as requested (unless the merge is complete).
steps ::
     Merge IO h
  -> Int  -- ^ How many input entries to consume (at least)
  -> IO (Int, StepResult)
steps Merge {..} requestedSteps = assertStepsInvariant <$> do
    -- TODO: ideally, we would not check whether the merge was already done on
    -- every call to @steps@. It is important for correctness, however, that we
    -- do not call @steps@ on a merge when it was already done. It is not yet
    -- clear whether our (upcoming) implementation of scheduled merges is going
    -- to satisfy this precondition when it calls @steps@, so for now we do the
    -- check.
    readMutVar mergeLastStepResult >>= \case
      MergeComplete   -> pure (0, MergeComplete)
      MergeInProgress -> go 0
  where
    assertStepsInvariant res = assert (stepsInvariant requestedSteps res) res

    fs = mergeHasFS
    hbio = mergeHasBlockIO

    go !n
      | n >= requestedSteps =
          return (n, MergeInProgress)
      | otherwise = do
          (key, entry, hasMore) <- Readers.pop fs hbio mergeReaders
          case hasMore of
            Readers.HasMore ->
              handleEntry (n + 1) key entry
            Readers.Drained -> do
              -- no future entries, no previous entry to resolve, just write!
              writeReaderEntry fs mergeLevel mergeBuilder key entry
              writeMutVar mergeLastStepResult $! MergeComplete
              pure (n + 1, MergeComplete)

    handleEntry !n !key (Reader.Entry (Mupdate v)) =
        -- resolve small mupsert vals with the following entries of the same key
        handleMupdate n key v
    handleEntry !n !key (Reader.EntryOverflow (Mupdate v) _ len overflowPages) =
        -- resolve large mupsert vals with following entries of the same key
        handleMupdate n key (Reader.appendOverflow len overflowPages v)
    handleEntry !n !key entry = do
        -- otherwise, we can just drop all following entries of same key
        writeReaderEntry fs mergeLevel mergeBuilder key entry
        dropRemaining n key

    -- the value is from a mupsert, complete (not just a prefix)
    handleMupdate !n !key !v = do
        nextKey <- Readers.peekKey mergeReaders
        if nextKey /= key
          then do
            -- resolved all entries for this key, write it
            writeSerialisedEntry fs mergeLevel mergeBuilder key (Mupdate v)
            go n
          else do
            (_, nextEntry, hasMore) <- Readers.pop fs hbio mergeReaders
            -- for resolution, we need the full second value to be present
            let resolved = combine mergeMappend
                             (Mupdate v)
                             (Reader.toFullEntry nextEntry)
            case hasMore of
              Readers.HasMore -> case resolved of
                Mupdate v' ->
                  -- still a mupsert, keep resolving
                  handleMupdate (n + 1) key v'
                _ -> do
                  -- done with this key, now the remaining entries are obsolete
                  writeSerialisedEntry fs mergeLevel mergeBuilder key resolved
                  dropRemaining (n + 1) key
              Readers.Drained -> do
                writeSerialisedEntry fs mergeLevel mergeBuilder key resolved
                writeMutVar mergeLastStepResult $! MergeComplete
                pure (n + 1, MergeComplete)

    dropRemaining !n !key = do
        (dropped, hasMore) <- Readers.dropWhileKey fs hbio mergeReaders key
        case hasMore of
          Readers.HasMore -> go (n + dropped)
          Readers.Drained -> do
            writeMutVar mergeLastStepResult $! MergeComplete
            pure (n + dropped, MergeComplete)


writeReaderEntry ::
     HasFS IO h
  -> Level
  -> RunBuilder RealWorld (FS.Handle h)
  -> SerialisedKey
  -> Reader.Entry IO (FS.Handle h)
  -> IO ()
writeReaderEntry fs level builder key (Reader.Entry entryFull) =
      -- Small entry.
      -- Note that this small entry could be the only one on the page. We only
      -- care about it being small, not single-entry, since it could still end
      -- up sharing a page with other entries in the merged run.
      -- TODO(optimise): This doesn't fully exploit the case where there is a
      -- single page small entry on the page which again ends up as the only
      -- entry of a page (which would for example happen a lot if most entries
      -- have 2k-4k bytes). In that case we could have copied the RawPage
      -- (but we find out too late to easily exploit it).
      writeSerialisedEntry fs level builder key entryFull
writeReaderEntry fs level builder key entry@(Reader.EntryOverflow prefix page _ overflowPages)
  | InsertWithBlob {} <- prefix =
      assert (shouldWriteEntry level prefix) $ do -- large, can't be delete
        -- has blob, we can't just copy the first page, fall back
        -- we simply append the overflow pages to the value
        Builder.addKeyOp fs builder key (Reader.toFullEntry entry)
        -- TODO(optimise): This copies the overflow pages unnecessarily.
        -- We could extend the RunBuilder API to allow to either:
        -- 1. write an Entry (containing the value prefix) + [RawOverflowPage]
        -- 2. write a RawPage + SerialisedBlob + [RawOverflowPage], rewriting
        --      the raw page's blob offset (slightly faster, but a bit hacky)
  | otherwise =
      assert (shouldWriteEntry level prefix) $  -- large, can't be delete
        -- no blob, directly copy all pages as they are
        Builder.addLargeSerialisedKeyOp fs builder key page overflowPages

writeSerialisedEntry ::
     HasFS IO h
  -> Level
  -> RunBuilder RealWorld (FS.Handle h)
  -> SerialisedKey
  -> Entry SerialisedValue (BlobRef IO (FS.Handle h))
  -> IO ()
writeSerialisedEntry fs level builder key entry =
    when (shouldWriteEntry level entry) $
      Builder.addKeyOp fs builder key entry

-- One the last level we could also turn Mupdate into Insert,
-- but no need to complicate things.
shouldWriteEntry :: Level -> Entry v b -> Bool
shouldWriteEntry level = \case
    Delete -> level == MidLevel
    _      -> True
