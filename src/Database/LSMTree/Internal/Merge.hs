-- | The 'Merge' type and its functions are not intended for concurrent use.
-- Concurrent access should therefore be sequentialised using a suitable
-- concurrency primitive, such as an 'MVar'.
module Database.LSMTree.Internal.Merge (
    Merge (..)
  , Level (..)
  , Mappend
  , MergeState (..)
  , new
  , addReference
  , removeReference
  , removeReferenceN
  , readRefCount
  , complete
  , stepsToCompletion
  , stepsToCompletionCounted
  , StepResult (..)
  , steps
  ) where

import           Control.Exception (assert)
import           Control.Monad (when)
import           Control.Monad.Class.MonadST (MonadST)
import           Control.Monad.Class.MonadSTM (MonadSTM (..))
import           Control.Monad.Class.MonadThrow (MonadCatch, MonadMask (..),
                     MonadThrow (..))
import           Control.Monad.Fix (MonadFix)
import           Control.Monad.Primitive (PrimMonad, PrimState, RealWorld)
import           Control.RefCount (RefCount (..), RefCounter)
import qualified Control.RefCount as RC
import           Data.Coerce (coerce)
import           Data.Primitive.MutVar
import           Data.Traversable (for)
import qualified Data.Vector as V
import           Data.Word
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
import           GHC.Stack (HasCallStack)
import qualified System.FS.API as FS
import           System.FS.API (HasFS)
import           System.FS.BlockIO.API (HasBlockIO)

-- | An in-progress incremental k-way merge of 'Run's.
--
-- Since we always resolve all entries of the same key in one go, there is no
-- need to store incompletely-resolved entries.
data Merge m h = Merge {
      mergeLevel      :: !Level
    , mergeMappend    :: !Mappend
    , mergeReaders    :: {-# UNPACK #-} !(Readers m h)
    , mergeBuilder    :: !(RunBuilder m h)
      -- | The caching policy to use for the Run in the 'MergeComplete'.
    , mergeCaching    :: !RunDataCaching
      -- | The result of the latest call to 'steps'. This is used to determine
      -- whether a merge can be 'complete'd.
    , mergeState      :: !(MutVar (PrimState m) MergeState)
    , mergeRefCounter :: !(RefCounter m)
    , mergeHasFS      :: !(HasFS m h)
    , mergeHasBlockIO :: !(HasBlockIO m h)
    }

-- | The current state of the merge.
data MergeState =
    -- | There is still merging work to be done
    Merging
    -- | There is no more merging work to be done, but the merge still has to be
    -- completed to yield a new run.
  | MergingDone
    -- | A run was yielded as the result of a merge. The merge is implicitly
    -- closed.
  | Completed
    -- | The merge was closed before it was completed.
  | Closed

data Level = MidLevel | LastLevel
  deriving stock (Eq, Show)

type Mappend = SerialisedValue -> SerialisedValue -> SerialisedValue

{-# SPECIALISE new ::
     HasFS IO h
  -> HasBlockIO IO h
  -> RunDataCaching
  -> RunBloomFilterAlloc
  -> Level
  -> Mappend
  -> Run.RunFsPaths
  -> V.Vector (Run IO h)
  -> IO (Maybe (Merge IO h)) #-}
-- | Returns 'Nothing' if no input 'Run' contains any entries.
-- The list of runs should be sorted from new to old.
new ::
     (MonadCatch m, MonadSTM m, MonadST m, MonadFix m)
  => HasFS m h
  -> HasBlockIO m h
  -> RunDataCaching
  -> RunBloomFilterAlloc
  -> Level
  -> Mappend
  -> Run.RunFsPaths
  -> V.Vector (Run m h)
  -> m (Maybe (Merge m h))
new fs hbio mergeCaching alloc mergeLevel mergeMappend targetPaths runs = do
    -- no offset, no write buffer
    mreaders <- Readers.new Readers.NoOffsetKey Nothing runs
    for mreaders $ \mergeReaders -> do
      -- calculate upper bounds based on input runs
      let numEntries = coerce (sum @V.Vector @Int) (fmap Run.runNumEntries runs)
      mergeBuilder <- Builder.new fs hbio targetPaths numEntries alloc
      mergeState <- newMutVar $! Merging
      mergeRefCounter <-
        RC.mkRefCounter1 (Just $! finaliser mergeState mergeBuilder mergeReaders)
      return Merge {
          mergeHasFS = fs
        , mergeHasBlockIO = hbio
        , ..
        }

{-# SPECIALISE addReference :: Merge IO h -> IO () #-}
addReference :: (HasCallStack, PrimMonad m) => Merge m h -> m ()
addReference Merge{..} = RC.addReference mergeRefCounter

{-# SPECIALISE removeReference :: Merge IO h -> IO () #-}
removeReference :: (HasCallStack, PrimMonad m, MonadMask m) => Merge m h -> m ()
removeReference Merge{..} = RC.removeReference mergeRefCounter

{-# SPECIALISE removeReferenceN :: Merge IO h -> Word64 -> IO () #-}
removeReferenceN :: (HasCallStack, PrimMonad m, MonadMask m) => Merge m h -> Word64 -> m ()
removeReferenceN r = RC.removeReferenceN (mergeRefCounter r)

{-# SPECIALISE readRefCount :: Merge IO h -> IO RefCount #-}
readRefCount :: PrimMonad m => Merge m h -> m RefCount
readRefCount Merge{..} = RC.readRefCount mergeRefCounter

{-# SPECIALISE finaliser ::
     MutVar RealWorld MergeState
  -> RunBuilder IO h
  -> Readers IO h
  -> IO () #-}
-- | Closes the underlying builder and readers.
--
-- This function is idempotent. Technically, this is not necessary because the
-- finaliser is going to run only once, but it is a nice property for
-- @close@-like functions to be idempotent.
finaliser ::
     (MonadFix m, MonadSTM m, MonadST m)
  => MutVar (PrimState m) MergeState
  -> RunBuilder m h
  -> Readers m h
  -> m ()
finaliser var b rs = do
    st <- readMutVar var
    let shouldClose = case st of
          Merging     -> True
          MergingDone -> True
          Completed   -> False
          Closed      -> False
    when shouldClose $ do
        Builder.close b
        Readers.close rs
        writeMutVar var $! Closed

{-# SPECIALISE complete ::
     Merge IO h
  -> IO (Run IO h) #-}
-- | Complete a 'Merge', returning a new 'Run' as the result of merging the
-- input runs.
--
-- The resulting run has the same reference count as the input 'Merge'. The
-- 'Merge' does not have to be closed afterwards, since it is closed implicitly
-- by 'complete'.
--
-- This function will /not/ do any merging work if there is any remaining. That
-- is, if not enough 'steps' were performed to exhaust the input 'Readers', this
-- function will throw an error.
--
-- Returns an error if the merge was not yet done, if it was already completed
-- before, or if it was already closed.
--
-- Note: this function creates new 'Run' resources, so it is recommended to run
-- this function with async exceptions masked. Otherwise, these resources can
-- leak.
complete ::
     (MonadFix m, MonadSTM m, MonadST m, MonadThrow m)
  => Merge m h
  -> m (Run m h)
complete Merge{..} = do
    readMutVar mergeState >>= \case
      Merging -> error "complete: Merge is not done"
      MergingDone -> do
        -- Since access to a merge /should/ be sequentialised, we can assume
        -- that the ref count has not changed between this read and the use of
        -- fromMutable.
        --
        -- TODO: alternatively, the mergeRefCounter could be reused as the
        -- reference counter for the output run.
        n <- RC.readRefCount mergeRefCounter
        r <- Run.fromMutable mergeCaching n mergeBuilder
        writeMutVar mergeState $! Completed
        pure r
      Completed -> error "complete: Merge is already completed"
      Closed -> error "complete: Merge is closed"

{-# SPECIALISE stepsToCompletion ::
     Merge IO h
  -> Int
  -> IO (Run IO h) #-}
-- | Like 'steps', but calling 'complete' once the merge is finished.
--
-- Note: run with async exceptions masked. See 'complete'.
stepsToCompletion ::
      (MonadCatch m, MonadFix m, MonadSTM m, MonadST m)
   => Merge m h
   -> Int
   -> m (Run m h)
stepsToCompletion m stepBatchSize = go
  where
    go = do
      steps m stepBatchSize >>= \case
        (_, MergeInProgress) -> go
        (_, MergeComplete)   -> complete m

{-# SPECIALISE stepsToCompletionCounted ::
     Merge IO h
  -> Int
  -> IO (Int, Run IO h) #-}
-- | Like 'steps', but calling 'complete' once the merge is finished.
--
-- Note: run with async exceptions masked. See 'complete'.
stepsToCompletionCounted ::
     (MonadCatch m, MonadFix m, MonadSTM m, MonadST m)
  => Merge m h
  -> Int
  -> m (Int, Run m h)
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

{-# SPECIALISE steps ::
     Merge IO h
  -> Int
  -> IO (Int, StepResult) #-}
-- | Do at least a given number of steps of merging. Each step reads a single
-- entry, then either resolves the previous entry with the new one or writes it
-- out to the run being created. Since we always finish resolving a key we
-- started, we might do slightly more work than requested.
--
-- Returns the number of input entries read, which is guaranteed to be at least
-- as many as requested (unless the merge is complete).
--
-- Returns an error if the merge was already completed or closed.
steps ::
     forall m h.
     (MonadCatch m, MonadSTM m, MonadST m)
  => Merge m h
  -> Int  -- ^ How many input entries to consume (at least)
  -> m (Int, StepResult)
steps Merge {..} requestedSteps = assertStepsInvariant <$> do
    -- TODO: ideally, we would not check whether the merge was already done on
    -- every call to @steps@. It is important for correctness, however, that we
    -- do not call @steps@ on a merge when it was already done. It is not yet
    -- clear whether our (upcoming) implementation of scheduled merges is going
    -- to satisfy this precondition when it calls @steps@, so for now we do the
    -- check.
    readMutVar mergeState >>= \case
      Merging -> go 0
      MergingDone -> pure (0, MergeComplete)
      Completed -> error "steps: Merge is completed"
      Closed -> error "steps: Merge is closed"
  where
    assertStepsInvariant res = assert (stepsInvariant requestedSteps res) res

    go :: Int -> m (Int, StepResult)
    go !n
      | n >= requestedSteps =
          return (n, MergeInProgress)
      | otherwise = do
          (key, entry, hasMore) <- Readers.pop mergeReaders
          case hasMore of
            Readers.HasMore ->
              handleEntry (n + 1) key entry
            Readers.Drained -> do
              -- no future entries, no previous entry to resolve, just write!
              writeReaderEntry mergeLevel mergeBuilder key entry
              writeMutVar mergeState $! MergingDone
              pure (n + 1, MergeComplete)

    handleEntry !n !key (Reader.Entry (Mupdate v)) =
        -- resolve small mupsert vals with the following entries of the same key
        handleMupdate n key v
    handleEntry !n !key (Reader.EntryOverflow (Mupdate v) _ len overflowPages) =
        -- resolve large mupsert vals with following entries of the same key
        handleMupdate n key (Reader.appendOverflow len overflowPages v)
    handleEntry !n !key entry = do
        -- otherwise, we can just drop all following entries of same key
        writeReaderEntry mergeLevel mergeBuilder key entry
        dropRemaining n key

    -- the value is from a mupsert, complete (not just a prefix)
    handleMupdate !n !key !v = do
        nextKey <- Readers.peekKey mergeReaders
        if nextKey /= key
          then do
            -- resolved all entries for this key, write it
            writeSerialisedEntry mergeLevel mergeBuilder key (Mupdate v)
            go n
          else do
            (_, nextEntry, hasMore) <- Readers.pop mergeReaders
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
                  writeSerialisedEntry mergeLevel mergeBuilder key resolved
                  dropRemaining (n + 1) key
              Readers.Drained -> do
                writeSerialisedEntry mergeLevel mergeBuilder key resolved
                writeMutVar mergeState $! MergingDone
                pure (n + 1, MergeComplete)

    dropRemaining !n !key = do
        (dropped, hasMore) <- Readers.dropWhileKey mergeReaders key
        case hasMore of
          Readers.HasMore -> go (n + dropped)
          Readers.Drained -> do
            writeMutVar mergeState $! MergingDone
            pure (n + dropped, MergeComplete)

{-# SPECIALISE writeReaderEntry ::
     Level
  -> RunBuilder IO h
  -> SerialisedKey
  -> Reader.Entry IO (FS.Handle h)
  -> IO () #-}
writeReaderEntry ::
     (MonadSTM m, MonadST m, MonadThrow m)
  => Level
  -> RunBuilder m h
  -> SerialisedKey
  -> Reader.Entry m (FS.Handle h)
  -> m ()
writeReaderEntry level builder key (Reader.Entry entryFull) =
      -- Small entry.
      -- Note that this small entry could be the only one on the page. We only
      -- care about it being small, not single-entry, since it could still end
      -- up sharing a page with other entries in the merged run.
      -- TODO(optimise): This doesn't fully exploit the case where there is a
      -- single page small entry on the page which again ends up as the only
      -- entry of a page (which would for example happen a lot if most entries
      -- have 2k-4k bytes). In that case we could have copied the RawPage
      -- (but we find out too late to easily exploit it).
      writeSerialisedEntry level builder key entryFull
writeReaderEntry level builder key entry@(Reader.EntryOverflow prefix page _ overflowPages)
  | InsertWithBlob {} <- prefix =
      assert (shouldWriteEntry level prefix) $ do -- large, can't be delete
        -- has blob, we can't just copy the first page, fall back
        -- we simply append the overflow pages to the value
        Builder.addKeyOp builder key (Reader.toFullEntry entry)
        -- TODO(optimise): This copies the overflow pages unnecessarily.
        -- We could extend the RunBuilder API to allow to either:
        -- 1. write an Entry (containing the value prefix) + [RawOverflowPage]
        -- 2. write a RawPage + SerialisedBlob + [RawOverflowPage], rewriting
        --      the raw page's blob offset (slightly faster, but a bit hacky)
  | otherwise =
      assert (shouldWriteEntry level prefix) $  -- large, can't be delete
        -- no blob, directly copy all pages as they are
        Builder.addLargeSerialisedKeyOp builder key page overflowPages

{-# SPECIALISE writeSerialisedEntry ::
     Level
  -> RunBuilder IO h
  -> SerialisedKey
  -> Entry SerialisedValue (BlobRef IO (FS.Handle h))
  -> IO () #-}
writeSerialisedEntry ::
     (MonadSTM m, MonadST m, MonadThrow m)
  => Level
  -> RunBuilder m h
  -> SerialisedKey
  -> Entry SerialisedValue (BlobRef m (FS.Handle h))
  -> m ()
writeSerialisedEntry level builder key entry =
    when (shouldWriteEntry level entry) $
      Builder.addKeyOp builder key entry

-- One the last level we could also turn Mupdate into Insert,
-- but no need to complicate things.
shouldWriteEntry :: Level -> Entry v b -> Bool
shouldWriteEntry level = \case
    Delete -> level == MidLevel
    _      -> True
