{-# OPTIONS_HADDOCK not-home #-}

-- | The 'Merge' type and its functions are not intended for concurrent use.
-- Concurrent access should therefore be sequentialised using a suitable
-- concurrency primitive, such as an 'MVar'.
--
module Database.LSMTree.Internal.Merge (
    Merge (..)
  , MergeType (..)
  , IsMergeType (..)
  , LevelMergeType (..)
  , TreeMergeType (..)
  , MergeState (..)
  , RunParams (..)
  , new
  , abort
  , complete
  , stepsToCompletion
  , stepsToCompletionCounted
  , StepResult (..)
  , steps
  , mergeRunParams
  ) where

import           Control.DeepSeq (NFData (..))
import           Control.Exception (assert)
import           Control.Monad (when)
import           Control.Monad.Class.MonadST (MonadST)
import           Control.Monad.Class.MonadSTM (MonadSTM (..))
import           Control.Monad.Class.MonadThrow (MonadMask, MonadThrow)
import           Control.Monad.Primitive (PrimState)
import           Control.RefCount
import           Data.Primitive.MutVar
import           Data.Traversable (for)
import qualified Data.Vector as V
import           Database.LSMTree.Internal.BlobRef (RawBlobRef)
import           Database.LSMTree.Internal.Entry
import           Database.LSMTree.Internal.Readers (Readers)
import qualified Database.LSMTree.Internal.Readers as Readers
import           Database.LSMTree.Internal.Run (Run)
import qualified Database.LSMTree.Internal.Run as Run
import           Database.LSMTree.Internal.RunBuilder (RunBuilder, RunParams)
import qualified Database.LSMTree.Internal.RunBuilder as Builder
import qualified Database.LSMTree.Internal.RunReader as Reader
import           Database.LSMTree.Internal.Serialise
import qualified System.FS.API as FS
import           System.FS.API (HasFS)
import           System.FS.BlockIO.API (HasBlockIO)

-- | An in-progress incremental k-way merge of 'Run's.
--
-- Since we always resolve all entries of the same key in one go, there is no
-- need to store incompletely-resolved entries.
data Merge t m h = Merge {
      mergeType        :: !t
      -- | We also store @isLastLevel mergeType@ and @isUnion mergeType@ here to
      -- avoid recomputing them it again and again for each call to 'steps',
      -- which would also add an 'IsMergeType' constraint to large parts of the
      -- interface.
    , mergeIsLastLevel :: !Bool
    , mergeIsUnion     :: !Bool
    , mergeResolve     :: !ResolveSerialisedValue
    , mergeReaders     :: {-# UNPACK #-} !(Readers m h)
    , mergeBuilder     :: !(RunBuilder m h)
      -- | The result of the latest call to 'steps'. This is used to determine
      -- whether a merge can be 'complete'd.
    , mergeState       :: !(MutVar (PrimState m) MergeState)
    , mergeHasFS       :: !(HasFS m h)
    , mergeHasBlockIO  :: !(HasBlockIO m h)
    }

mergeRunParams :: Merge t m h -> RunParams
mergeRunParams = Builder.runBuilderParams . mergeBuilder

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

-- | Merges can either exist on a level of the LSM, or be a union merge of two
-- tables.
class IsMergeType t where
  -- | A last level merge behaves differently from a mid-level merge: last level
  -- merges can actually remove delete operations, whereas mid-level merges must
  -- preserve them.
  isLastLevel :: t -> Bool
  -- | Union merges follow the semantics of @Data.Map.unionWith (<>)@. Since the
  -- input runs are semantically treated like @Data.Map@s, deletes are ignored
  -- and inserts act like mupserts, so they need to be merged monoidally using
  -- 'resolveValue'.
  isUnion :: t -> Bool

-- | Fully general merge type, mainly useful for testing.
data MergeType = MergeTypeMidLevel | MergeTypeLastLevel | MergeTypeUnion
  deriving stock (Eq, Show)

instance NFData MergeType where
  rnf MergeTypeMidLevel  = ()
  rnf MergeTypeLastLevel = ()
  rnf MergeTypeUnion     = ()

instance IsMergeType MergeType where
  isLastLevel = \case
      MergeTypeMidLevel  -> False
      MergeTypeLastLevel -> True
      MergeTypeUnion     -> True
  isUnion = \case
      MergeTypeMidLevel  -> False
      MergeTypeLastLevel -> False
      MergeTypeUnion     -> True

-- | Different types of merges created as part of a regular (non-union) level.
--
-- A last level merge behaves differently from a mid-level merge: last level
-- merges can actually remove delete operations, whereas mid-level merges must
-- preserve them. This is orthogonal to the 'MergePolicy'.
data LevelMergeType = MergeMidLevel | MergeLastLevel
  deriving stock (Eq, Show)

instance NFData LevelMergeType where
  rnf MergeMidLevel  = ()
  rnf MergeLastLevel = ()

instance IsMergeType LevelMergeType where
  isLastLevel = \case
      MergeMidLevel  -> False
      MergeLastLevel -> True
  isUnion = const False

-- | Different types of merges created as part of the merging tree.
data TreeMergeType = MergeLevel | MergeUnion
  deriving stock (Eq, Show)

instance NFData TreeMergeType where
  rnf MergeLevel = ()
  rnf MergeUnion = ()

instance IsMergeType TreeMergeType where
  isLastLevel = const True
  isUnion = \case
      MergeLevel -> False
      MergeUnion -> True

{-# SPECIALISE new ::
     IsMergeType t
  => HasFS IO h
  -> HasBlockIO IO h
  -> RunParams
  -> t
  -> ResolveSerialisedValue
  -> Run.RunFsPaths
  -> V.Vector (Ref (Run IO h))
  -> IO (Maybe (Merge t IO h)) #-}
-- | Returns 'Nothing' if no input 'Run' contains any entries.
-- The list of runs should be sorted from new to old.
new ::
     (IsMergeType t, MonadMask m, MonadSTM m, MonadST m)
  => HasFS m h
  -> HasBlockIO m h
  -> RunParams
  -> t
  -> ResolveSerialisedValue
  -> Run.RunFsPaths
  -> V.Vector (Ref (Run m h))
  -> m (Maybe (Merge t m h))
new hfs hbio runParams mergeType mergeResolve targetPaths runs = do
    let sources = Readers.FromRun <$> V.toList runs
    mreaders <- Readers.new mergeResolve Readers.NoOffsetKey sources
    -- TODO: Exception safety! If Readers.new fails after already creating some
    -- run readers, or Builder.new fails, the run readers will stay open,
    -- holding handles of the input runs' files.
    for mreaders $ \mergeReaders -> do
      -- calculate upper bounds based on input runs
      let numEntries = V.foldMap' Run.size runs
      mergeBuilder <- Builder.new hfs hbio runParams targetPaths numEntries
      mergeState <- newMutVar $! Merging
      return Merge {
          mergeIsLastLevel = isLastLevel mergeType
        , mergeIsUnion = isUnion mergeType
        , mergeHasFS = hfs
        , mergeHasBlockIO = hbio
        , ..
        }

{-# SPECIALISE abort :: Merge t IO (FS.Handle h) -> IO () #-}
-- | This function should be called when discarding a 'Merge' before it
-- was done (i.e. returned 'MergeComplete'). This removes the incomplete files
-- created for the new run so far and avoids leaking file handles.
--
-- Once it has been called, do not use the 'Merge' any more!
abort :: (MonadMask m, MonadSTM m, MonadST m) => Merge t m h -> m ()
abort Merge {..} = do
    readMutVar mergeState >>= \case
      Merging -> do
        Readers.close mergeReaders
        Builder.close mergeBuilder
      MergingDone -> do
        -- the readers are already drained, therefore closed
        Builder.close mergeBuilder
      Completed ->
        assert False $ pure ()
      Closed ->
        assert False $ pure ()
    writeMutVar mergeState $! Closed

{-# SPECIALISE complete ::
     Merge t IO h
  -> IO (Ref (Run IO h)) #-}
-- | Complete a 'Merge', returning a new 'Run' as the result of merging the
-- input runs.
--
-- All resources held by the merge are released, so do not use the it any more!
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
-- leak. And it must eventually be released with 'releaseRef'.
--
complete ::
     (MonadSTM m, MonadST m, MonadMask m)
  => Merge t m h
  -> m (Ref (Run m h))
complete Merge{..} = do
    readMutVar mergeState >>= \case
      Merging -> error "complete: Merge is not done"
      MergingDone -> do
        -- the readers are already drained, therefore closed
        r <- Run.fromBuilder mergeBuilder
        writeMutVar mergeState $! Completed
        pure r
      Completed -> error "complete: Merge is already completed"
      Closed -> error "complete: Merge is closed"

{-# SPECIALISE stepsToCompletion ::
     Merge t IO h
  -> Int
  -> IO (Ref (Run IO h)) #-}
-- | Like 'steps', but calling 'complete' once the merge is finished.
--
-- Note: run with async exceptions masked. See 'complete'.
stepsToCompletion ::
      (MonadMask m, MonadSTM m, MonadST m)
   => Merge t m h
   -> Int
   -> m (Ref (Run m h))
stepsToCompletion m stepBatchSize = go
  where
    go = do
      steps m stepBatchSize >>= \case
        (_, MergeInProgress) -> go
        (_, MergeDone)       -> complete m

{-# SPECIALISE stepsToCompletionCounted ::
     Merge t IO h
  -> Int
  -> IO (Int, Ref (Run IO h)) #-}
-- | Like 'steps', but calling 'complete' once the merge is finished.
--
-- Note: run with async exceptions masked. See 'complete'.
stepsToCompletionCounted ::
     (MonadMask m, MonadSTM m, MonadST m)
  => Merge t m h
  -> Int
  -> m (Int, Ref (Run m h))
stepsToCompletionCounted m stepBatchSize = go 0
  where
    go !stepsSum = do
      steps m stepBatchSize >>= \case
        (n, MergeInProgress) -> go (stepsSum + n)
        (n, MergeDone)       -> let !stepsSum' = stepsSum + n
                                in (stepsSum',) <$> complete m

data StepResult = MergeInProgress | MergeDone
  deriving stock Eq

stepsInvariant :: Int -> (Int, StepResult) -> Bool
stepsInvariant requestedSteps = \case
    (n, MergeInProgress) -> n >= requestedSteps
    _                    -> True

{-# SPECIALISE steps ::
     Merge t IO h
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
     (MonadMask m, MonadSTM m, MonadST m)
  => Merge t m h
  -> Int  -- ^ How many input entries to consume (at least)
  -> m (Int, StepResult)
steps m@Merge {..} requestedSteps = assertStepsInvariant <$> do
    -- TODO: ideally, we would not check whether the merge was already done on
    -- every call to @steps@. It is important for correctness, however, that we
    -- do not call @steps@ on a merge when it was already done. It is not yet
    -- clear whether our (upcoming) implementation of scheduled merges is going
    -- to satisfy this precondition when it calls @steps@, so for now we do the
    -- check.
    readMutVar mergeState >>= \case
      Merging     -> if mergeIsUnion then doStepsUnion m requestedSteps
                                     else doStepsLevel m requestedSteps
      MergingDone -> pure (0, MergeDone)
      Completed   -> error "steps: Merge is completed"
      Closed      -> error "steps: Merge is closed"
  where
    assertStepsInvariant res = assert (stepsInvariant requestedSteps res) res

{-# SPECIALISE doStepsLevel ::
     Merge t IO h
  -> Int
  -> IO (Int, StepResult) #-}
doStepsLevel ::
     (MonadMask m, MonadSTM m, MonadST m)
  => Merge t m h
  -> Int  -- ^ How many input entries to consume (at least)
  -> m (Int, StepResult)
doStepsLevel m@Merge {..} requestedSteps = go 0
  where
    go !n
      | n >= requestedSteps =
          return (n, MergeInProgress)
      | otherwise = do
          (key, entry, hasMore) <- Readers.pop mergeResolve mergeReaders
          case hasMore of
            Readers.HasMore ->
              handleEntry (n + 1) key entry
            Readers.Drained -> do
              -- no future entries, no previous entry to resolve, just write!
              writeReaderEntry m key entry
              writeMutVar mergeState $! MergingDone
              pure (n + 1, MergeDone)

    handleEntry !n !key (Reader.Entry (Mupdate v)) =
        -- resolve small mupsert vals with the following entries of the same key
        handleMupdate n key v
    handleEntry !n !key (Reader.EntryOverflow (Mupdate v) _ len overflowPages) =
        -- resolve large mupsert vals with following entries of the same key
        handleMupdate n key (Reader.appendOverflow len overflowPages v)
    handleEntry !n !key entry = do
        -- otherwise, we can just drop all following entries of same key
        writeReaderEntry m key entry
        dropRemaining n key

    -- the value is from a mupsert, complete (not just a prefix)
    handleMupdate !n !key !v = do
        nextKey <- Readers.peekKey mergeReaders
        if nextKey /= key
          then do
            -- resolved all entries for this key, write it
            writeSerialisedEntry m key (Mupdate v)
            go n
          else do
            (_, nextEntry, hasMore) <- Readers.pop mergeResolve mergeReaders
            -- for resolution, we need the full second value to be present
            let resolved = combine mergeResolve
                             (Mupdate v)
                             (Reader.toFullEntry nextEntry)
            case hasMore of
              Readers.HasMore -> case resolved of
                Mupdate v' ->
                  -- still a mupsert, keep resolving
                  handleMupdate (n + 1) key v'
                _ -> do
                  -- done with this key, now the remaining entries are obsolete
                  writeSerialisedEntry m key resolved
                  dropRemaining (n + 1) key
              Readers.Drained -> do
                writeSerialisedEntry m key resolved
                writeMutVar mergeState $! MergingDone
                pure (n + 1, MergeDone)

    dropRemaining !n !key = do
        (dropped, hasMore) <- Readers.dropWhileKey mergeResolve mergeReaders key
        case hasMore of
          Readers.HasMore -> go (n + dropped)
          Readers.Drained -> do
            writeMutVar mergeState $! MergingDone
            pure (n + dropped, MergeDone)

{-# SPECIALISE doStepsUnion ::
     Merge t IO h
  -> Int
  -> IO (Int, StepResult) #-}
doStepsUnion ::
     (MonadMask m, MonadSTM m, MonadST m)
  => Merge t m h
  -> Int  -- ^ How many input entries to consume (at least)
  -> m (Int, StepResult)
doStepsUnion m@Merge {..} requestedSteps = go 0
  where
    go !n
      | n >= requestedSteps =
          return (n, MergeInProgress)
      | otherwise = do
          (key, entry, hasMore) <- Readers.pop mergeResolve mergeReaders
          handleEntry (n + 1) key entry hasMore

    -- Similar to 'handleMupdate' in 'stepsLevel', but here we have to combine
    -- all entries monoidally, so there are no obsolete/overwritten entries
    -- that we could skip.
    --
    -- TODO(optimisation): If mergeResolve is const, we could skip all remaining
    -- entries for the key. Unfortunately, we can't inspect the function. This
    -- would require encoding it as something like `Const | Resolve (_ -> _ ->
    -- _)`.
    handleEntry !n !key !entry Readers.Drained = do
        -- no future entries, no previous entry to resolve, just write!
        writeReaderEntry m key entry
        writeMutVar mergeState $! MergingDone
        pure (n, MergeDone)

    handleEntry !n !key !entry Readers.HasMore = do
        nextKey <- Readers.peekKey mergeReaders
        if nextKey /= key
          then do
            -- resolved all entries for this key, write it
            writeReaderEntry m key entry
            go n
          else do
            (_, nextEntry, hasMore) <- Readers.pop mergeResolve mergeReaders
            -- for resolution, we need the full second value to be present
            let resolved = combineUnion mergeResolve
                             (Reader.toFullEntry entry)
                             (Reader.toFullEntry nextEntry)
            handleEntry (n + 1) key (Reader.Entry resolved) hasMore

{-# INLINE writeReaderEntry #-}
writeReaderEntry ::
     (MonadSTM m, MonadST m, MonadThrow m)
  => Merge t m h
  -> SerialisedKey
  -> Reader.Entry m h
  -> m ()
writeReaderEntry m key (Reader.Entry entryFull) =
      -- Small entry.
      -- Note that this small entry could be the only one on the page. We only
      -- care about it being small, not single-entry, since it could still end
      -- up sharing a page with other entries in the merged run.
      -- TODO(optimise): This doesn't fully exploit the case where there is a
      -- single page small entry on the page which again ends up as the only
      -- entry of a page (which would for example happen a lot if most entries
      -- have 2k-4k bytes). In that case we could have copied the RawPage
      -- (but we find out too late to easily exploit it).
      writeSerialisedEntry m key entryFull
writeReaderEntry m key entry@(Reader.EntryOverflow prefix page _ overflowPages)
  | InsertWithBlob {} <- prefix =
      assert (shouldWriteEntry m prefix) $ do -- large, can't be delete
        -- has blob, we can't just copy the first page, fall back
        -- we simply append the overflow pages to the value
        Builder.addKeyOp (mergeBuilder m) key (Reader.toFullEntry entry)
        -- TODO(optimise): This copies the overflow pages unnecessarily.
        -- We could extend the RunBuilder API to allow to either:
        -- 1. write an Entry (containing the value prefix) + [RawOverflowPage]
        -- 2. write a RawPage + SerialisedBlob + [RawOverflowPage], rewriting
        --      the raw page's blob offset (slightly faster, but a bit hacky)
  | otherwise =
      assert (shouldWriteEntry m prefix) $  -- large, can't be delete
        -- no blob, directly copy all pages as they are
        Builder.addLargeSerialisedKeyOp (mergeBuilder m) key page overflowPages

{-# INLINE writeSerialisedEntry #-}
writeSerialisedEntry ::
     (MonadSTM m, MonadST m, MonadThrow m)
  => Merge t m h
  -> SerialisedKey
  -> Entry SerialisedValue (RawBlobRef m h)
  -> m ()
writeSerialisedEntry m key entry =
    when (shouldWriteEntry m entry) $
      Builder.addKeyOp (mergeBuilder m) key entry

-- On the last level we could also turn Mupdate into Insert, but no need to
-- complicate things.
shouldWriteEntry :: Merge t m h -> Entry v b -> Bool
shouldWriteEntry m Delete = not (mergeIsLastLevel m)
shouldWriteEntry _ _      = True
