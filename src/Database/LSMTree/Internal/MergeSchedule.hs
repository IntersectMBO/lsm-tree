{-# LANGUAGE CPP       #-}
{-# LANGUAGE DataKinds #-}

-- TODO: establish that this implementation matches up with the ScheduledMerges
-- prototype. See lsm-tree#445.
module Database.LSMTree.Internal.MergeSchedule (
    -- * Traces
    AtLevel (..)
  , MergeTrace (..)
    -- * Table content
  , TableContent (..)
  , addReferenceTableContent
  , removeReferenceTableContent
    -- * Levels cache
  , LevelsCache (..)
  , mkLevelsCache
    -- * Levels, runs and ongoing merges
  , Levels
  , Level (..)
  , MergingRun (..)
  , NumRuns (..)
  , MergingRunState (..)
    -- * Flushes and scheduled merges
  , updatesWithInterleavedFlushes
  , flushWriteBuffer
    -- * Exported for cabal-docspec
  , MergePolicyForLevel (..)
  , maxRunSize
    -- * Credits
  , supplyCredits
  , ScaledCredits (..)
  , supplyMergeCredits
  ) where

import           Control.Concurrent.Class.MonadMVar.Strict
import           Control.Monad (when)
import           Control.Monad.Class.MonadST (MonadST)
import           Control.Monad.Class.MonadSTM (MonadSTM (..))
import           Control.Monad.Class.MonadThrow (MonadCatch, MonadMask,
                     MonadThrow (..))
import           Control.Monad.Fix (MonadFix)
import           Control.Monad.Primitive
import           Control.RefCount (RefCount (..))
import           Control.TempRegistry
import           Control.Tracer
import           Data.BloomFilter (Bloom)
import           Data.Primitive.PrimVar
import qualified Data.Vector as V
import           Database.LSMTree.Internal.Assertions (assert,
                     fromIntegralChecked)
import           Database.LSMTree.Internal.Config
import           Database.LSMTree.Internal.Entry (Entry, NumEntries (..),
                     unNumEntries)
import           Database.LSMTree.Internal.IndexCompact (IndexCompact)
import           Database.LSMTree.Internal.Lookup (ResolveSerialisedValue)
import           Database.LSMTree.Internal.Merge (Merge, StepResult (..))
import qualified Database.LSMTree.Internal.Merge as Merge
import           Database.LSMTree.Internal.Paths (RunFsPaths (..),
                     SessionRoot (..))
import qualified Database.LSMTree.Internal.Paths as Paths
import           Database.LSMTree.Internal.Run (Run, RunDataCaching (..))
import qualified Database.LSMTree.Internal.Run as Run
import           Database.LSMTree.Internal.RunAcc (RunBloomFilterAlloc (..))
import           Database.LSMTree.Internal.RunNumber
import           Database.LSMTree.Internal.Serialise (SerialisedBlob,
                     SerialisedKey, SerialisedValue)
import           Database.LSMTree.Internal.UniqCounter
import           Database.LSMTree.Internal.Vector (mapStrict)
import           Database.LSMTree.Internal.WriteBuffer (WriteBuffer)
import qualified Database.LSMTree.Internal.WriteBuffer as WB
import           Database.LSMTree.Internal.WriteBufferBlobs (WriteBufferBlobs)
import qualified Database.LSMTree.Internal.WriteBufferBlobs as WBB
import qualified System.FS.API as FS
import           System.FS.API (HasFS)
import           System.FS.BlockIO.API (HasBlockIO)

{-------------------------------------------------------------------------------
  Traces
-------------------------------------------------------------------------------}

data AtLevel a = AtLevel LevelNo a
  deriving stock Show

data MergeTrace =
    TraceFlushWriteBuffer
      NumEntries -- ^ Size of the write buffer
      RunNumber
      RunDataCaching
      RunBloomFilterAlloc
  | TraceAddLevel
  | TraceAddRun
      RunNumber -- ^ newly added run
      (V.Vector RunNumber) -- ^ resident runs
  | TraceNewMerge
      (V.Vector NumEntries) -- ^ Sizes of input runs
      RunNumber
      RunDataCaching
      RunBloomFilterAlloc
      MergePolicyForLevel
      Merge.Level
  | TraceCompletedMerge
      NumEntries -- ^ Size of output run
      RunNumber
  | TraceExpectCompletedMerge
      RunNumber
  | TraceNewMergeSingleRun
      NumEntries -- ^ Size of run
      RunNumber
  | TraceExpectCompletedMergeSingleRun
      RunNumber
  deriving stock Show

{-------------------------------------------------------------------------------
  Table content
-------------------------------------------------------------------------------}

data TableContent m h = TableContent {
    --TODO: probably less allocation to make this a MutVar
    tableWriteBuffer      :: !WriteBuffer
    -- | The blob storage for entries in the write buffer
  , tableWriteBufferBlobs :: !(WriteBufferBlobs m h)
    -- | A hierarchy of levels. The vector indexes double as level numbers.
  , tableLevels           :: !(Levels m h)
    -- | Cache of flattened 'levels'.
  , tableCache            :: !(LevelsCache m h)
  }

{-# SPECIALISE addReferenceTableContent :: TempRegistry IO -> TableContent IO h -> IO () #-}
addReferenceTableContent ::
     (PrimMonad m, MonadMask m, MonadMVar m)
  => TempRegistry m
  -> TableContent m h
  -> m ()
addReferenceTableContent reg (TableContent _wb wbb levels cache) = do
    allocateTemp reg (WBB.addReference wbb) (\_ -> WBB.removeReference wbb)
    addReferenceLevels reg levels
    addReferenceLevelsCache reg cache

{-# SPECIALISE removeReferenceTableContent :: TempRegistry IO -> TableContent IO h -> IO () #-}
removeReferenceTableContent ::
     (PrimMonad m, MonadMask m, MonadMVar m)
  => TempRegistry m
  -> TableContent m h
  -> m ()
removeReferenceTableContent reg (TableContent _wb wbb levels cache) = do
    freeTemp reg (WBB.removeReference wbb)
    removeReferenceLevels reg levels
    removeReferenceLevelsCache reg cache

{-------------------------------------------------------------------------------
  Levels cache
-------------------------------------------------------------------------------}

-- | Flattend cache of the runs that referenced by a table.
--
-- This cache includes a vector of runs, but also vectors of the runs broken
-- down into components, like bloom filters, fence pointer indexes and file
-- handles. This allows for quick access in the lookup code. Recomputing this
-- cache should be relatively rare.
--
-- Caches take reference counts for its runs on construction, and they release
-- references when the cache is invalidated. This is done so that incremental
-- merges can remove references for their input runs when a merge completes,
-- without closing runs that might be in use for other operations such as
-- lookups. This does mean that a cache can keep runs open for longer than
-- necessary, so caches should be rebuilt using, e.g., 'rebuildCache', in a
-- timely manner.
data LevelsCache m h = LevelsCache_ {
    cachedRuns      :: !(V.Vector (Run m h))
  , cachedFilters   :: !(V.Vector (Bloom SerialisedKey))
  , cachedIndexes   :: !(V.Vector IndexCompact)
  , cachedKOpsFiles :: !(V.Vector (FS.Handle h))
  }

{-# SPECIALISE mkLevelsCache ::
     TempRegistry IO
  -> Levels IO h
  -> IO (LevelsCache IO h) #-}
-- | Flatten the argument 'Level's into a single vector of runs, and use that to
-- populate the 'LevelsCache'. The cache will take a reference for each of the
-- runs that end up in the cache.
mkLevelsCache ::
     (PrimMonad m, MonadMVar m, MonadMask m)
  => TempRegistry m
  -> Levels m h
  -> m (LevelsCache m h)
mkLevelsCache reg lvls = do
  rs <- forRunM lvls $ \r -> allocateTemp reg (Run.addReference r) (\_ -> Run.removeReference r) >> pure r
  pure $! LevelsCache_ {
      cachedRuns      = rs
    , cachedFilters   = mapStrict Run.runFilter rs
    , cachedIndexes   = mapStrict Run.runIndex rs
    , cachedKOpsFiles = mapStrict Run.runKOpsFile rs
    }

{-# SPECIALISE rebuildCache ::
     TempRegistry IO
  -> LevelsCache IO h
  -> Levels IO h
  -> IO (LevelsCache IO h) #-}
-- | Remove references to runs in the old cache, and create a new cache with
-- fresh references taken for the runs in the new levels.
--
-- TODO: caches are currently only rebuilt in flushWriteBuffer. If an
-- OngoingMerge is completed, then tables will only rebuild the cache, and
-- therefore release "old" runs, when a flush is initiated. This is sub-optimal,
-- and there are at least two solutions, but it is unclear which is faster or
-- more convenient.
--
-- * Get rid of the cache entirely, and have each batch of lookups take
--   references for runs in the levels structure.
--
-- * Keep the cache feature, but force a rebuild every once in a while, e.g.,
--   once in every 100 lookups.
--
-- TODO: rebuilding the cache can invalidate blob references if the cache was
-- holding the last reference to a run. This is not really a problem of just the
-- caching approach, but allowing merges to finish early. We should come up with
-- a solution to keep blob references valid until the next /update/ comes along.
-- Lookups should no invalidate blob erferences.
rebuildCache ::
     (PrimMonad m, MonadMVar m, MonadMask m)
  => TempRegistry m
  -> LevelsCache m h -- ^ old cache
  -> Levels m h -- ^ new levels
  -> m (LevelsCache m h) -- ^ new cache
rebuildCache reg oldCache newLevels = do
    removeReferenceLevelsCache reg oldCache
    mkLevelsCache reg newLevels

{-# SPECIALISE addReferenceLevelsCache ::
     TempRegistry IO
  -> LevelsCache IO h
  -> IO () #-}
addReferenceLevelsCache ::
     (PrimMonad m, MonadMask m, MonadMVar m)
  => TempRegistry m
  -> LevelsCache m h
  -> m ()
addReferenceLevelsCache reg cache =
    V.forM_ (cachedRuns cache) $ \r ->
      allocateTemp reg
        (Run.addReference r)
        (\_ -> Run.removeReference r)

{-# SPECIALISE removeReferenceLevelsCache ::
     TempRegistry IO
  -> LevelsCache IO h
  -> IO () #-}
removeReferenceLevelsCache ::
     (PrimMonad m, MonadMVar m, MonadMask m)
  => TempRegistry m
  -> LevelsCache m h
  -> m ()
removeReferenceLevelsCache reg cache =
    V.forM_ (cachedRuns cache) $ \r -> freeTemp reg (Run.removeReference r)

{-------------------------------------------------------------------------------
  Levels, runs and ongoing merges
-------------------------------------------------------------------------------}

type Levels m h = V.Vector (Level m h)

-- | Runs in order from newer to older
data Level m h = Level {
    incomingRuns :: !(MergingRun m h)
  , residentRuns :: !(V.Vector (Run m h))
  }

-- | A merging run is either a single run, or some ongoing merge.
data MergingRun m h =
    -- | A merging of multiple runs.
    MergingRun !MergePolicyForLevel !NumRuns !(StrictMVar m (MergingRunState m h))
    -- | The result of merging a single run, is a single run.
  | SingleRun !(Run m h)

newtype NumRuns = NumRuns { unNumRuns :: Int }
  deriving stock (Show, Eq)

data MergingRunState m h =
    CompletedMerge
      !(Run m h)
      -- ^ Output run
  | OngoingMerge
      !(V.Vector (Run m h))
      -- ^ Input runs
      !(PrimVar (PrimState m) Int)
      -- ^ The total number of performed merging steps.
      !(PrimVar (PrimState m) Int)
      -- ^ The total number of supplied credits.
      !(Merge m h)

{-# SPECIALISE addReferenceLevels :: TempRegistry IO -> Levels IO h -> IO () #-}
addReferenceLevels ::
     (PrimMonad m, MonadMVar m, MonadMask m)
  => TempRegistry m
  -> Levels m h
  -> m ()
addReferenceLevels reg levels =
    forRunAndMergeM_ levels
      (\r -> allocateTemp reg (Run.addReference r) (\_ -> Run.removeReference r))
      (\m -> allocateTemp reg (Merge.addReference m) (\_ -> Merge.removeReference m))

{-# SPECIALISE addReferenceLevels :: TempRegistry IO -> Levels IO h -> IO () #-}
removeReferenceLevels ::
     (PrimMonad m, MonadMVar m, MonadMask m)
  => TempRegistry m
  -> Levels m h
  -> m ()
removeReferenceLevels reg levels =
    forRunAndMergeM_ levels
      (\r -> freeTemp reg (Run.removeReference r))
      (\m -> freeTemp reg (Merge.removeReference m))

{-# SPECIALISE forRunAndMergeM_ ::
     Levels IO h
  -> (Run IO h -> IO ())
  -> (Merge IO h -> IO ())
  -> IO () #-}
forRunAndMergeM_ ::
     MonadMVar m
  => Levels m h
  -> (Run m h -> m ())
  -> (Merge m h -> m ())
  -> m ()
forRunAndMergeM_ lvls k1 k2 = V.forM_ lvls $ \(Level mr rs) -> do
    case mr of
      SingleRun r    -> k1 r
      MergingRun _ _ var -> withMVar var $ \case
        CompletedMerge r -> k1 r
        OngoingMerge irs _ _ m -> V.mapM_ k1 irs >> k2 m
    V.mapM_ k1 rs

{-# SPECIALISE foldRunM ::
     (b -> Run IO h -> IO b)
  -> b
  -> Levels IO h
  -> IO b #-}
foldRunM ::
     MonadMVar m
  => (b -> Run m h -> m b)
  -> b
  -> Levels m h
  -> m b
foldRunM f x lvls = flip (flip V.foldM x) lvls $ \y (Level mr rs) -> do
    z <- case mr of
      SingleRun r -> f y r
      MergingRun _ _ var -> withMVar var $ \case
        CompletedMerge r -> f y r
        OngoingMerge irs _ _ _m -> V.foldM f y irs
    V.foldM f z rs

{-# SPECIALISE forRunM ::
     Levels IO h
  -> (Run IO h -> IO a)
  -> IO (V.Vector a) #-}
-- TODO: this is not terribly performant, but it is also not sure if we are
-- going to need this in the end. We might get rid of the LevelsCache, and we
-- currently only use forRunM in mkLevelsCache.
forRunM ::
     MonadMVar m
  => Levels m h
  -> (Run m h -> m a)
  -> m (V.Vector a)
forRunM lvls k = do
    V.reverse . V.fromList <$> foldRunM (\acc r -> k r >>= \x -> pure (x : acc)) [] lvls

{-------------------------------------------------------------------------------
  Flushes and scheduled merges
-------------------------------------------------------------------------------}

{-# SPECIALISE updatesWithInterleavedFlushes ::
     Tracer IO (AtLevel MergeTrace)
  -> TableConfig
  -> ResolveSerialisedValue
  -> HasFS IO h
  -> HasBlockIO IO h
  -> SessionRoot
  -> UniqCounter IO
  -> V.Vector (SerialisedKey, Entry SerialisedValue SerialisedBlob)
  -> TempRegistry IO
  -> TableContent IO h
  -> IO (TableContent IO h) #-}
-- | A single batch of updates can fill up the write buffer multiple times. We
-- flush the write buffer each time it fills up before trying to fill it up
-- again.
--
-- TODO: in practice the size of a batch will be much smaller than the maximum
-- size of the write buffer, so we should optimise for the case that small
-- batches are inserted. Ideas:
--
-- * we can allow a range of sizes to flush to disk rather than just the max size
--
-- * could do a map bulk merge rather than sequential insert, on the prefix of
--   the batch that's guaranteed to fit
--
-- * or flush the existing buffer if we would expect the next batch to cause the
--   buffer to become too large
--
-- TODO: we could also optimise for the case where the write buffer is small
-- compared to the size of the batch, but it is less critical. In particular, in
-- case the write buffer is empty, or if it fills up multiple times for a single
-- batch of updates, we might be able to skip adding entries to the write buffer
-- for a large part. When the write buffer is empty, we can sort and deduplicate
-- the vector of updates directly, slice it up into properly sized sub-vectors,
-- and write those to disk. Of course, any remainder that did not fit into a
-- whole run should then end up in a fresh write buffer.
updatesWithInterleavedFlushes ::
     forall m h.
     (MonadFix m, MonadMask m, MonadMVar m, MonadSTM m, MonadST m)
  => Tracer m (AtLevel MergeTrace)
  -> TableConfig
  -> ResolveSerialisedValue
  -> HasFS m h
  -> HasBlockIO m h
  -> SessionRoot
  -> UniqCounter m
  -> V.Vector (SerialisedKey, Entry SerialisedValue SerialisedBlob)
  -> TempRegistry m
  -> TableContent m h
  -> m (TableContent m h)
updatesWithInterleavedFlushes tr conf resolve hfs hbio root uc es reg tc = do
    let wb = tableWriteBuffer tc
        wbblobs = tableWriteBufferBlobs tc
    (wb', es') <- addWriteBufferEntries hfs resolve wbblobs maxn wb es
    -- Supply credits before flushing, so that we complete merges in time. The
    -- number of supplied credits is based on the size increase of the write
    -- buffer, not the the number of processed entries @length es' - length es@.
    let numAdded = unNumEntries (WB.numEntries wb') - unNumEntries (WB.numEntries wb)
    supplyCredits numAdded (tableLevels tc)
    let tc' = tc { tableWriteBuffer = wb' }
    if WB.numEntries wb' < maxn then do
      pure $! tc'
    -- If the write buffer did reach capacity, then we flush.
    else do
      tc'' <- flushWriteBuffer tr conf resolve hfs hbio root uc reg tc'
      -- In the fortunate case where we have already performed all the updates,
      -- return,
      if V.null es' then
        pure $! tc''
      -- otherwise, keep going
      else
        updatesWithInterleavedFlushes tr conf resolve hfs hbio root uc es' reg tc''
  where
    AllocNumEntries maxn = confWriteBufferAlloc conf

{-# SPECIALISE addWriteBufferEntries ::
     HasFS IO h
  -> ResolveSerialisedValue
  -> WriteBufferBlobs IO h
  -> NumEntries
  -> WriteBuffer
  -> V.Vector (SerialisedKey, Entry SerialisedValue SerialisedBlob)
  -> IO (WriteBuffer, V.Vector (SerialisedKey, Entry SerialisedValue SerialisedBlob)) #-}
-- | Add entries to the write buffer up until a certain write buffer size @n@.
--
-- NOTE: if the write buffer is larger @n@ already, this is a no-op.
addWriteBufferEntries ::
     (MonadSTM m, MonadThrow m, PrimMonad m)
  => HasFS m h
  -> ResolveSerialisedValue
  -> WriteBufferBlobs m h
  -> NumEntries
  -> WriteBuffer
  -> V.Vector (SerialisedKey, Entry SerialisedValue SerialisedBlob)
  -> m (WriteBuffer, V.Vector (SerialisedKey, Entry SerialisedValue SerialisedBlob))
addWriteBufferEntries hfs f wbblobs maxn =
    \wb es ->
      (\ r@(wb', es') ->
          -- never exceed the write buffer capacity
          assert (WB.numEntries wb' <= maxn) $
          -- If the new write buffer has not reached capacity yet, then it must
          -- be the case that we have performed all the updates.
          assert ((WB.numEntries wb'  < maxn && V.null es')
               || (WB.numEntries wb' == maxn)) $
          r)
      <$> go wb es
  where
    --TODO: exception safety for async exceptions or I/O errors from writing blobs
    go !wb !es
      | WB.numEntries wb >= maxn = pure (wb, es)

      | Just ((k, e), es') <- V.uncons es = do
          e' <- traverse (WBB.addBlob hfs wbblobs) e
          go (WB.addEntry f k e' wb) es'

      | otherwise = pure (wb, es)


{-# SPECIALISE flushWriteBuffer ::
     Tracer IO (AtLevel MergeTrace)
  -> TableConfig
  -> ResolveSerialisedValue
  -> HasFS IO h
  -> HasBlockIO IO h
  -> SessionRoot
  -> UniqCounter IO
  -> TempRegistry IO
  -> TableContent IO h
  -> IO (TableContent IO h) #-}
-- | Flush the write buffer to disk, regardless of whether it is full or not.
--
-- The returned table content contains an updated set of levels, where the write
-- buffer is inserted into level 1.
flushWriteBuffer ::
     (MonadFix m, MonadMask m, MonadMVar m, MonadST m, MonadSTM m)
  => Tracer m (AtLevel MergeTrace)
  -> TableConfig
  -> ResolveSerialisedValue
  -> HasFS m h
  -> HasBlockIO m h
  -> SessionRoot
  -> UniqCounter m
  -> TempRegistry m
  -> TableContent m h
  -> m (TableContent m h)
flushWriteBuffer tr conf@TableConfig{confDiskCachePolicy}
                 resolve hfs hbio root uc reg tc
  | WB.null (tableWriteBuffer tc) = pure tc
  | otherwise = do
    !n <- incrUniqCounter uc
    let !size  = WB.numEntries (tableWriteBuffer tc)
        !l     = LevelNo 1
        !cache = diskCachePolicyForLevel confDiskCachePolicy l
        !alloc = bloomFilterAllocForLevel conf l
        !path  = Paths.runPath root (uniqueToRunNumber n)
    traceWith tr $ AtLevel l $ TraceFlushWriteBuffer size (runNumber path) cache alloc
    r <- allocateTemp reg
            (Run.fromWriteBuffer hfs hbio
              cache
              alloc
              path
              (tableWriteBuffer tc)
              (tableWriteBufferBlobs tc))
            Run.removeReference
    freeTemp reg (WBB.removeReference (tableWriteBufferBlobs tc))
    wbblobs' <- allocateTemp reg (WBB.new hfs (Paths.tableBlobPath root n)) WBB.removeReference
    levels' <- addRunToLevels tr conf resolve hfs hbio root uc r reg (tableLevels tc)
    tableCache' <- rebuildCache reg (tableCache tc) levels'
    pure $! TableContent {
        tableWriteBuffer = WB.empty
      , tableWriteBufferBlobs = wbblobs'
      , tableLevels = levels'
      , tableCache = tableCache'
      }

{- TODO: re-enable
-- | Note that the invariants rely on the fact that levelling is only used on
-- the last level.
--
-- NOTE: @_levelsInvariant@ is based on the @ScheduledMerges.invariant@
-- prototype. See @ScheduledMerges.invariant@ for documentation about the merge
-- algorithm.
_levelsInvariant :: forall m h. TableConfig -> Levels m h -> ST (PrimState m) Bool
_levelsInvariant conf levels =
    go (LevelNo 1) levels >>= \ !_ -> pure True
  where
    sr = confSizeRatio conf
    wba = confWriteBufferAlloc conf

    go :: LevelNo -> Levels m h -> ST (PrimState m) ()
    go !_ (V.uncons -> Nothing) = pure ()

    go !ln (V.uncons -> Just (Level mr rs, ls)) = do
      mrs <- case mr of
               SingleRun r    -> pure $ CompletedMerge r
               MergingRun var -> readMutVar var
      assert (length rs < sizeRatioInt sr) $ pure ()
      assert (expectedRunLengths ln rs ls) $ pure ()
      assert (expectedMergingRunLengths ln mr mrs ls) $ pure ()
      go (succ ln) ls

    -- All runs within a level "proper" (as opposed to the incoming runs
    -- being merged) should be of the correct size for the level.
    expectedRunLengths ln rs ls = do
      case mergePolicyForLevel (confMergePolicy conf) ln ls of
        -- Levels using levelling have only one run, and that single run is
        -- (almost) always involved in an ongoing merge. Thus there are no
        -- other "normal" runs. The exception is when a levelling run becomes
        -- too large and is promoted, in that case initially there's no merge,
        -- but it is still represented as a 'MergingRun', using 'SingleRun'.
        LevelLevelling -> assert (V.null rs) True
        LevelTiering   -> V.all (\r -> assert (fits LevelTiering r ln) True) rs

    -- Incoming runs being merged also need to be of the right size, but the
    -- conditions are more complicated.
    expectedMergingRunLengths ln mr mrs ls =
      case mergePolicyForLevel (confMergePolicy conf) ln ls of
        LevelLevelling ->
          case (mr, mrs) of
            -- A single incoming run (which thus didn't need merging) must be
            -- of the expected size range already
            (SingleRun r, CompletedMerge{}) -> assert (fits LevelLevelling r ln) True
                        -- A completed merge for levelling can be of almost any size at all!
            -- It can be smaller, due to deletions in the last level. But it
            -- can't be bigger than would fit into the next level.
            (_, CompletedMerge r) -> assert (fitsUB LevelLevelling r (succ ln)) True
        LevelTiering ->
          case (mr, mrs, mergeLastForLevel ls) of
            -- A single incoming run (which thus didn't need merging) must be
            -- of the expected size already
            (SingleRun r, CompletedMerge{}, _) -> assert (fits LevelTiering r ln) True

            -- A completed last level run can be of almost any smaller size due
            -- to deletions, but it can't be bigger than the next level down.
            -- Note that tiering on the last level only occurs when there is
            -- a single level only.
            (_, CompletedMerge r, Merge.LastLevel) ->
                assert (ln == LevelNo 1) $
                assert (fitsUB LevelTiering r (succ ln)) $
                True

            -- A completed mid level run is usually of the size for the
            -- level it is entering, but can also be one smaller (in which case
            -- it'll be held back and merged again).
            (_, CompletedMerge r, Merge.MidLevel) ->
                assert (fitsUB LevelTiering r ln || fitsUB LevelTiering r (succ ln)) True

    -- Check that a run fits in the current level
    fits policy r ln = fitsLB policy r ln && fitsUB policy r ln
    -- Check that a run is too large for previous levels
    fitsLB policy r ln = maxRunSize sr wba policy (pred ln) < Run.runNumEntries r
    -- Check that a run is too small for next levels
    fitsUB policy r ln = Run.runNumEntries r <= maxRunSize sr wba policy ln
-}

{-# SPECIALISE addRunToLevels ::
     Tracer IO (AtLevel MergeTrace)
  -> TableConfig
  -> ResolveSerialisedValue
  -> HasFS IO h
  -> HasBlockIO IO h
  -> SessionRoot
  -> UniqCounter IO
  -> Run IO h
  -> TempRegistry IO
  -> Levels IO h
  -> IO (Levels IO h) #-}
-- | Add a run to the levels, and propagate merges.
--
-- NOTE: @go@ is based on the @ScheduledMerges.increment@ prototype. See @ScheduledMerges.increment@
-- for documentation about the merge algorithm.
addRunToLevels ::
     forall m h.
     (MonadFix m, MonadMask m, MonadMVar m, MonadST m, MonadSTM m)
  => Tracer m (AtLevel MergeTrace)
  -> TableConfig
  -> ResolveSerialisedValue
  -> HasFS m h
  -> HasBlockIO m h
  -> SessionRoot
  -> UniqCounter m
  -> Run m h
  -> TempRegistry m
  -> Levels m h
  -> m (Levels m h)
addRunToLevels tr conf@TableConfig{..} resolve hfs hbio root uc r0 reg levels = do
    ls' <- go (LevelNo 1) (V.singleton r0) levels
{- TODO: re-enable
#ifdef NO_IGNORE_ASSERTS
    void $ stToIO $ _levelsInvariant conf ls'
#endif
-}
    return ls'
  where
    -- NOTE: @go@ is based on the @increment@ function from the
    -- @ScheduledMerges@ prototype.
    go !ln rs (V.uncons -> Nothing) = do
        traceWith tr $ AtLevel ln TraceAddLevel
        -- Make a new level
        let policyForLevel = mergePolicyForLevel confMergePolicy ln V.empty
        mr <- newMerge policyForLevel Merge.LastLevel ln rs
        return $ V.singleton $ Level mr V.empty
    go !ln rs' (V.uncons -> Just (Level mr rs, ls)) = do
        r <- expectCompletedMerge ln mr
        case mergePolicyForLevel confMergePolicy ln ls of
          -- If r is still too small for this level then keep it and merge again
          -- with the incoming runs.
          LevelTiering | runSize r <= maxRunSize' conf LevelTiering (pred ln) -> do
            let mergelast = mergeLastForLevel ls
            mr' <- newMerge LevelTiering mergelast ln (rs' `V.snoc` r)
            pure $! Level mr' rs `V.cons` ls
          -- This tiering level is now full. We take the completed merged run
          -- (the previous incoming runs), plus all the other runs on this level
          -- as a bundle and move them down to the level below. We start a merge
          -- for the new incoming runs. This level is otherwise empty.
          LevelTiering | levelIsFull confSizeRatio rs -> do
            mr' <- newMerge LevelTiering Merge.MidLevel ln rs'
            ls' <- go (succ ln) (r `V.cons` rs) ls
            pure $! Level mr' V.empty `V.cons` ls'
          -- This tiering level is not yet full. We move the completed merged run
          -- into the level proper, and start the new merge for the incoming runs.
          LevelTiering -> do
            let mergelast = mergeLastForLevel ls
            mr' <- newMerge LevelTiering mergelast ln rs'
            traceWith tr $ AtLevel ln
                         $ TraceAddRun
                            (runNumber $ Run.runRunFsPaths r)
                            (V.map (runNumber . Run.runRunFsPaths) rs)
            pure $! Level mr' (r `V.cons` rs) `V.cons` ls
          -- The final level is using levelling. If the existing completed merge
          -- run is too large for this level, we promote the run to the next
          -- level and start merging the incoming runs into this (otherwise
          -- empty) level .
          LevelLevelling | runSize r > maxRunSize' conf LevelLevelling ln -> do
            assert (V.null rs && V.null ls) $ pure ()
            mr' <- newMerge LevelTiering Merge.MidLevel ln rs'
            ls' <- go (succ ln) (V.singleton r) V.empty
            pure $! Level mr' V.empty `V.cons` ls'
          -- Otherwise we start merging the incoming runs into the run.
          LevelLevelling -> do
            assert (V.null rs && V.null ls) $ pure ()
            mr' <- newMerge LevelLevelling Merge.LastLevel ln (rs' `V.snoc` r)
            pure $! Level mr' V.empty `V.cons` V.empty

    expectCompletedMerge :: LevelNo -> MergingRun m h -> m (Run m h)
    expectCompletedMerge ln (SingleRun r) = do
      traceWith tr $ AtLevel ln $ TraceExpectCompletedMergeSingleRun (runNumber $ Run.runRunFsPaths r)
      pure r
    expectCompletedMerge ln (MergingRun _ _ var) = do
      r <- withMVar var $ \case
        CompletedMerge r -> pure r
        OngoingMerge{} -> do
          -- If the algorithm finds an ongoing merge here, then it is a bug in
          -- our merge sceduling algorithm. As such, we throw a pure error.
          error "expectCompletedMerge: expected a completed merge, but found an ongoing merge"
      traceWith tr $ AtLevel ln $ TraceExpectCompletedMerge (runNumber $ Run.runRunFsPaths r)
      pure r

    newMerge :: MergePolicyForLevel
             -> Merge.Level
             -> LevelNo
             -> V.Vector (Run m h)
             -> m (MergingRun m h)
    newMerge mergepolicy mergelast ln rs
      | Just (r, rest) <- V.uncons rs
      , V.null rest = do
          traceWith tr $ AtLevel ln $ TraceNewMergeSingleRun (Run.runNumEntries r) (runNumber $ Run.runRunFsPaths r)
          pure (SingleRun r)
      | otherwise = do
        assert (let l = V.length rs in l >= 2 && l <= 5) $ pure ()
        !n <- incrUniqCounter uc
        let !caching = diskCachePolicyForLevel confDiskCachePolicy ln
            !alloc = bloomFilterAllocForLevel conf ln
            !runPaths = Paths.runPath root (uniqueToRunNumber n)
        traceWith tr $ AtLevel ln $ TraceNewMerge (V.map Run.runNumEntries rs) (runNumber runPaths) caching alloc mergepolicy mergelast
        case confMergeSchedule of
          OneShot -> do
            r <- allocateTemp reg
                  (mergeRuns resolve hfs hbio caching alloc runPaths mergelast rs)
                  Run.removeReference
            traceWith tr $ AtLevel ln $ TraceCompletedMerge (Run.runNumEntries r) (runNumber $ Run.runRunFsPaths r)
            V.mapM_ (freeTemp reg . Run.removeReference) rs
            var <- newMVar $! CompletedMerge r
            pure $! MergingRun mergepolicy (NumRuns $ V.length rs) var
          Incremental -> do
            mergeMaybe <- allocateMaybeTemp reg
              (Merge.new hfs hbio caching alloc mergelast resolve runPaths rs)
              Merge.removeReference
            case mergeMaybe of
              Nothing -> error "newMerge: merges can not be empty"
              Just m -> do
                totalStepsVar <- newPrimVar $! 0
                totalCreditsVar <- newPrimVar $! 0
                var <- newMVar $! OngoingMerge rs totalStepsVar totalCreditsVar m
                pure $! MergingRun mergepolicy (NumRuns $ V.length rs) var

data MergePolicyForLevel = LevelTiering | LevelLevelling
  deriving stock (Show, Eq)

mergePolicyForLevel :: MergePolicy -> LevelNo -> Levels m h -> MergePolicyForLevel
mergePolicyForLevel MergePolicyLazyLevelling (LevelNo n) nextLevels
  | n == 1
  , V.null nextLevels
  = LevelTiering    -- always use tiering on first level
  | V.null nextLevels = LevelLevelling  -- levelling on last level
  | otherwise         = LevelTiering

runSize :: Run m h -> NumEntries
runSize run = Run.runNumEntries run

-- $setup
-- >>> import Database.LSMTree.Internal.Entry
-- >>> import Database.LSMTree.Internal.Config

-- | Compute the maximum size of a run for a given level.
--
-- The @size@ of a tiering run at each level is allowed to be
-- @bufferSize*sizeRatio^(level-1) < size <= bufferSize*sizeRatio^level@.
--
-- >>> unNumEntries . maxRunSize Four (AllocNumEntries (NumEntries 2)) LevelTiering . LevelNo <$> [0, 1, 2, 3, 4]
-- [0,2,8,32,128]
--
-- The @size@ of a levelling run at each level is allowed to be
-- @bufferSize*sizeRatio^(level-1) < size <= bufferSize*sizeRatio^(level+1)@. A
-- levelling run can take take up a whole level, so the maximum size of a run is
-- @sizeRatio*@ larger than the maximum size of a tiering run on the same level.
--
-- >>> unNumEntries . maxRunSize Four (AllocNumEntries (NumEntries 2)) LevelLevelling . LevelNo <$> [0, 1, 2, 3, 4]
-- [0,8,32,128,512]
maxRunSize ::
     SizeRatio
  -> WriteBufferAlloc
  -> MergePolicyForLevel
  -> LevelNo
  -> NumEntries
maxRunSize (sizeRatioInt -> sizeRatio) (AllocNumEntries (NumEntries bufferSize))
           policy (LevelNo ln) =
    NumEntries $ case policy of
      LevelLevelling -> runSizeTiering * sizeRatio
      LevelTiering   -> runSizeTiering
  where
    runSizeTiering
      | ln < 0 = error "maxRunSize: non-positive level number"
      | ln == 0 = 0
      | otherwise = bufferSize * sizeRatio ^ (pred ln)

maxRunSize' :: TableConfig -> MergePolicyForLevel -> LevelNo -> NumEntries
maxRunSize' config policy ln =
    maxRunSize (confSizeRatio config) (confWriteBufferAlloc config) policy ln

mergeLastForLevel :: Levels m h -> Merge.Level
mergeLastForLevel levels
 | V.null levels = Merge.LastLevel
 | otherwise     = Merge.MidLevel

levelIsFull :: SizeRatio -> V.Vector (Run m h) -> Bool
levelIsFull sr rs = V.length rs + 1 >= (sizeRatioInt sr)

{-# SPECIALISE mergeRuns :: ResolveSerialisedValue -> HasFS IO h -> HasBlockIO IO h -> RunDataCaching -> RunBloomFilterAlloc -> RunFsPaths -> Merge.Level -> V.Vector (Run IO h) -> IO (Run IO h) #-}
mergeRuns ::
     (MonadCatch m, MonadFix m, MonadST m, MonadSTM m)
  => ResolveSerialisedValue
  -> HasFS m h
  -> HasBlockIO m h
  -> RunDataCaching
  -> RunBloomFilterAlloc
  -> RunFsPaths
  -> Merge.Level
  -> V.Vector (Run m h)
  -> m (Run m h)
mergeRuns resolve hfs hbio caching alloc runPaths mergeLevel runs = do
    Merge.new hfs hbio caching alloc mergeLevel resolve runPaths runs >>= \case
      Nothing -> error "mergeRuns: no inputs"
      Just m -> Merge.stepsToCompletion m 1024

{-------------------------------------------------------------------------------
  Credits
-------------------------------------------------------------------------------}

type Credit = Int

{-# SPECIALISE supplyCredits ::
     Credit
  -> Levels IO h
  -> IO ()
  #-}
supplyCredits ::
     (MonadSTM m, MonadST m, MonadMVar m, MonadMask m, MonadFix m)
  => Credit
  -> Levels m h
  -> m ()
supplyCredits c levels =
    V.iforM_ levels $ \_i (Level mr _rs) ->
    -- let !ln = i + 1 in
    let !c' = scaleCreditsForMerge mr c in
    supplyMergeCredits c' mr

-- | 'Credit's scaled based on the merge requirements for merging runs. See
-- 'scaleCreditsForMerge'.
newtype ScaledCredits = ScaledCredits Int

-- | Scale a number of credits to a number of merge steps to be performed, based
-- on the merging run.
--
-- Initially, 1 update supplies 1 credit. However, since merging runs have
-- different numbers of input runs/entries, we may have to a more or less
-- merging work than 1 merge step for each credit.
scaleCreditsForMerge :: MergingRun m h -> Credit -> ScaledCredits
-- A single run is a trivially completed merge, so it requires no credits.
scaleCreditsForMerge SingleRun{} _ = ScaledCredits 0
-- A levelling merge has 1 input run and one resident run, which is (up to) 4x
-- bigger than the others. It needs to be completed before another run comes in.
--
-- TODO: this is currently assuming a naive worst case, where the resident run
-- is as large as it can be for the current level. We probably have enough
-- information available here to lower the worst-case upper bound by looking at
-- the sizes of the input runs. As as result, merge work would/could be more
-- evenly distributed over time when the resident run is smaller than the worst
-- case.
scaleCreditsForMerge (MergingRun LevelLevelling _ _) c =
    ScaledCredits (c * (1 + 4))
-- A tiering merge has 5 runs at most (one could be held back to merged again)
-- and must be completed before the level is full (once 4 more runs come in).
scaleCreditsForMerge (MergingRun LevelTiering (NumRuns n) _) c =
    ScaledCredits ((c * n + 3) `div` 4)
    -- same as division rounding up: ceiling (c * n / 4)

{-# SPECIALISE supplyMergeCredits :: ScaledCredits -> MergingRun IO h -> IO () #-}
-- TODO: implement doing merge werk in batches, instead of always taking the
-- MVar. The thresholds for doing merge work should be different for each level,
-- maybe co-prime?
supplyMergeCredits ::
     (MonadSTM m, MonadST m, MonadMVar m, MonadMask m, MonadFix m)
  => ScaledCredits
  -> MergingRun m h
  -> m ()
supplyMergeCredits _ SingleRun{} = pure ()
supplyMergeCredits (ScaledCredits c) (MergingRun _ _ var) = do
    mergeIsDone <- withMVar var $ \case
      CompletedMerge{} -> pure False
      (OngoingMerge _rs totalStepsVar totalCreditsVar m) -> do
        totalSteps <- readPrimVar totalStepsVar
        totalCredits <- readPrimVar totalCreditsVar

        -- If we previously performed too many merge steps, then we perform
        -- fewer now.
        let stepsToDo = max 0 (totalCredits + c - totalSteps)
        -- Merge.steps guarantees that stepsDone >= stepsToDo /unless/ the merge
        -- was just now finished.
        (stepsDone, stepResult) <- Merge.steps m stepsToDo
        assert (case stepResult of
                  MergeInProgress -> stepsDone >= stepsToDo
                  MergeDone       -> True
               ) $ pure ()

        -- This should be the only point at which we write to these variables.
        --
        -- It is guaranteed that totalSteps' >= totaltCredits' /unless/ the
        -- merge was just now finished.
        let totalSteps' = totalSteps + stepsDone
        let totalCredits' = totalCredits + c
        -- If an exception happens between the next two writes, then only
        -- totalCreditsVar will be outdated, which is okay because we will
        -- resupply credits. It also means we can maintain that @readPrimVar
        -- totalStepsVar >= readPrimVar totalCreditsVar@, /unless/ the merge was
        -- just now finished.
        writePrimVar totalStepsVar $! totalSteps + stepsDone
        writePrimVar totalCreditsVar $! totalCredits + c
        assert (case stepResult of
                  MergeInProgress -> totalSteps' >= totalCredits'
                  MergeDone       -> True
               ) $ pure ()

        pure $ stepResult == MergeDone
    when mergeIsDone $
      modifyMVarMasked_ var $ \case
        mr@CompletedMerge{} -> pure $! mr
        (OngoingMerge rs _totalStepsVar _totalCreditsVar m) -> do
          -- TODO: we'll likely move away from this style of reference counting,
          -- so this code will change in the future.
          RefCount n <- Merge.readRefCount m
          let !n' = fromIntegralChecked n
          V.forM_ rs $ \r -> Run.removeReferenceN r n'
          r <- Merge.complete m
          Merge.removeReferenceN m n'
          pure $! CompletedMerge r
