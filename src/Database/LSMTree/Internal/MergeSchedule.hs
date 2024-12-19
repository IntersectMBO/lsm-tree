-- TODO: establish that this implementation matches up with the ScheduledMerges
-- prototype. See lsm-tree#445.
module Database.LSMTree.Internal.MergeSchedule (
    -- * Traces
    AtLevel (..)
  , MergeTrace (..)
    -- * Table content
  , TableContent (..)
  , duplicateTableContent
  , releaseTableContent
    -- * Levels cache
  , LevelsCache (..)
  , mkLevelsCache
    -- * Levels, runs and ongoing merges
  , Levels
  , Level (..)
  , IncomingRun (..)
    -- * Flushes and scheduled merges
  , updatesWithInterleavedFlushes
  , flushWriteBuffer
    -- * Exported for cabal-docspec
  , MergePolicyForLevel (..)
  , maxRunSize
    -- * Credits
  , Credits (..)
  , supplyCredits
  , creditThresholdForLevel
  ) where

import           Control.Concurrent.Class.MonadMVar.Strict
import           Control.Monad.Class.MonadST (MonadST)
import           Control.Monad.Class.MonadSTM (MonadSTM (..))
import           Control.Monad.Class.MonadThrow (MonadMask, MonadThrow (..))
import           Control.Monad.Primitive
import           Control.RefCount
import           Control.TempRegistry
import           Control.Tracer
import           Data.BloomFilter (Bloom)
import           Data.Foldable (fold)
import qualified Data.Vector as V
import           Database.LSMTree.Internal.Assertions (assert)
import           Database.LSMTree.Internal.Config
import           Database.LSMTree.Internal.Entry (Entry, NumEntries (..),
                     unNumEntries)
import           Database.LSMTree.Internal.Index.Compact (IndexCompact)
import           Database.LSMTree.Internal.Lookup (ResolveSerialisedValue)
import qualified Database.LSMTree.Internal.Merge as Merge
import           Database.LSMTree.Internal.MergingRun (MergePolicyForLevel (..),
                     MergingRun, NumRuns (..))
import qualified Database.LSMTree.Internal.MergingRun as MR
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
  | TraceCompletedMerge  -- TODO: currently not traced for Incremental merges
      NumEntries -- ^ Size of output run
      RunNumber
    -- | This is traced at the latest point the merge could complete.
  | TraceExpectCompletedMerge
      RunNumber
  | TraceNewMergeSingleRun
      NumEntries -- ^ Size of run
      RunNumber
  deriving stock Show

{-------------------------------------------------------------------------------
  Table content
-------------------------------------------------------------------------------}

data TableContent m h = TableContent {
    --TODO: probably less allocation to make this a MutVar
    tableWriteBuffer      :: !WriteBuffer
    -- | The blob storage for entries in the write buffer
  , tableWriteBufferBlobs :: !(Ref (WriteBufferBlobs m h))
    -- | A hierarchy of levels. The vector indexes double as level numbers.
  , tableLevels           :: !(Levels m h)
    -- | Cache of flattened 'levels'.
  , tableCache            :: !(LevelsCache m h)
  }

{-# SPECIALISE duplicateTableContent :: TempRegistry IO -> TableContent IO h -> IO (TableContent IO h) #-}
duplicateTableContent ::
     (PrimMonad m, MonadMask m, MonadMVar m)
  => TempRegistry m
  -> TableContent m h
  -> m (TableContent m h)
duplicateTableContent reg (TableContent wb wbb levels cache) = do
    wbb'    <- allocateTemp reg (dupRef wbb) releaseRef
    levels' <- duplicateLevels reg levels
    cache'  <- duplicateLevelsCache reg cache
    return $! TableContent wb wbb' levels' cache'

{-# SPECIALISE releaseTableContent :: TempRegistry IO -> TableContent IO h -> IO () #-}
releaseTableContent ::
     (PrimMonad m, MonadMask m, MonadMVar m)
  => TempRegistry m
  -> TableContent m h
  -> m ()
releaseTableContent reg (TableContent _wb wbb levels cache) = do
    freeTemp reg (releaseRef wbb)
    releaseLevels reg levels
    releaseLevelsCache reg cache

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
-- Caches keep references to its runs on construction, and they release each
-- reference when the cache is invalidated. This is done so that incremental
-- merges can remove references for their input runs when a merge completes,
-- without closing runs that might be in use for other operations such as
-- lookups. This does mean that a cache can keep runs open for longer than
-- necessary, so caches should be rebuilt using, e.g., 'rebuildCache', in a
-- timely manner.
data LevelsCache m h = LevelsCache_ {
    cachedRuns      :: !(V.Vector (Ref (Run m h)))
  , cachedFilters   :: !(V.Vector (Bloom SerialisedKey))
  , cachedIndexes   :: !(V.Vector IndexCompact)
  , cachedKOpsFiles :: !(V.Vector (FS.Handle h))
  }

{-# SPECIALISE mkLevelsCache ::
     TempRegistry IO
  -> Levels IO h
  -> IO (LevelsCache IO h) #-}
-- | Flatten the argument 'Level's into a single vector of runs, including all
-- runs that are inputs to an ongoing merge. Use that to populate the
-- 'LevelsCache'. The cache will take a reference for each of its runs.
mkLevelsCache ::
     forall m h. (PrimMonad m, MonadMVar m, MonadMask m)
  => TempRegistry m
  -> Levels m h
  -> m (LevelsCache m h)
mkLevelsCache reg lvls = do
    rs <- foldRunAndMergeM
      (fmap V.singleton . dupRun)
      (\mr -> allocateTemp reg (MR.duplicateRuns mr) (V.mapM_ releaseRef))
      lvls
    pure $! LevelsCache_ {
        cachedRuns      = rs
      , cachedFilters   = mapStrict (\(DeRef r) -> Run.runFilter   r) rs
      , cachedIndexes   = mapStrict (\(DeRef r) -> Run.runIndex    r) rs
      , cachedKOpsFiles = mapStrict (\(DeRef r) -> Run.runKOpsFile r) rs
      }
  where
    dupRun r = allocateTemp reg (dupRef r) releaseRef

    -- TODO: this is not terribly performant, but it is also not sure if we are
    -- going to need this in the end. We might get rid of the LevelsCache.
    foldRunAndMergeM ::
         Monoid a
      => (Ref (Run m h) -> m a)
      -> (Ref (MergingRun m h) -> m a)
      -> Levels m h
      -> m a
    foldRunAndMergeM k1 k2 ls =
        fmap fold $ V.forM ls $ \(Level ir rs) -> do
          incoming <- case ir of
            Single     r -> k1 r
            Merging _ mr -> k2 mr
          (incoming <>) . fold <$> V.forM rs k1

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
    releaseLevelsCache reg oldCache
    mkLevelsCache reg newLevels

{-# SPECIALISE duplicateLevelsCache ::
     TempRegistry IO
  -> LevelsCache IO h
  -> IO (LevelsCache IO h) #-}
duplicateLevelsCache ::
     (PrimMonad m, MonadMask m, MonadMVar m)
  => TempRegistry m
  -> LevelsCache m h
  -> m (LevelsCache m h)
duplicateLevelsCache reg cache = do
    rs' <- V.forM (cachedRuns cache) $ \r ->
             allocateTemp reg (dupRef r) releaseRef
    return cache { cachedRuns = rs' }

{-# SPECIALISE releaseLevelsCache ::
     TempRegistry IO
  -> LevelsCache IO h
  -> IO () #-}
releaseLevelsCache ::
     (PrimMonad m, MonadMVar m, MonadMask m)
  => TempRegistry m
  -> LevelsCache m h
  -> m ()
releaseLevelsCache reg cache =
    V.forM_ (cachedRuns cache) $ \r ->
      freeTemp reg (releaseRef r)

{-------------------------------------------------------------------------------
  Levels, runs and ongoing merges
-------------------------------------------------------------------------------}

type Levels m h = V.Vector (Level m h)

-- | Runs in order from newer to older
data Level m h = Level {
    incomingRun  :: !(IncomingRun m h)
  , residentRuns :: !(V.Vector (Ref (Run m h)))
  }

-- | An incoming run is either a single run, or a merge.
data IncomingRun m h =
       Single  !(Ref (Run m h))
     | Merging !MergePolicyForLevel !(Ref (MergingRun m h))

mergePolicyForLevel :: MergePolicy -> LevelNo -> Levels m h -> MergePolicyForLevel
mergePolicyForLevel MergePolicyLazyLevelling (LevelNo n) nextLevels
  | n == 1
  , V.null nextLevels
  = LevelTiering    -- always use tiering on first level
  | V.null nextLevels = LevelLevelling  -- levelling on last level
  | otherwise         = LevelTiering

{-# SPECIALISE duplicateLevels :: TempRegistry IO -> Levels IO h -> IO (Levels IO h) #-}
duplicateLevels ::
     (PrimMonad m, MonadMVar m, MonadMask m)
  => TempRegistry m
  -> Levels m h
  -> m (Levels m h)
duplicateLevels reg levels =
    V.forM levels $ \Level {incomingRun, residentRuns} -> do
      incomingRun'  <- duplicateIncomingRun reg incomingRun
      residentRuns' <- V.forM residentRuns $ \r ->
                         allocateTemp reg (dupRef r) releaseRef
      return $! Level {
        incomingRun  = incomingRun',
        residentRuns = residentRuns'
      }

{-# SPECIALISE releaseLevels :: TempRegistry IO -> Levels IO h -> IO () #-}
releaseLevels ::
     (PrimMonad m, MonadMVar m, MonadMask m)
  => TempRegistry m
  -> Levels m h
  -> m ()
releaseLevels reg levels =
    V.forM_ levels $ \Level {incomingRun, residentRuns} -> do
      releaseIncomingRun reg incomingRun
      V.mapM_ (freeTemp reg . releaseRef) residentRuns

{-# SPECIALISE duplicateIncomingRun :: TempRegistry IO -> IncomingRun IO h -> IO (IncomingRun IO h) #-}
duplicateIncomingRun ::
     (PrimMonad m, MonadMask m, MonadMVar m)
  => TempRegistry m
  -> IncomingRun m h
  -> m (IncomingRun m h)
duplicateIncomingRun reg (Single r) =
    Single <$> allocateTemp reg (dupRef r) releaseRef

duplicateIncomingRun reg (Merging mp mr) =
    Merging mp <$> allocateTemp reg (dupRef mr) releaseRef

{-# SPECIALISE releaseIncomingRun :: TempRegistry IO -> IncomingRun IO h -> IO () #-}
releaseIncomingRun ::
     (PrimMonad m, MonadMask m, MonadMVar m)
  => TempRegistry m
  -> IncomingRun m h -> m ()
releaseIncomingRun reg (Single r)     = freeTemp reg (releaseRef r)
releaseIncomingRun reg (Merging _ mr) = freeTemp reg (releaseRef mr)

{-# SPECIALISE iforLevelM_ :: Levels IO h -> (LevelNo -> Level IO h -> IO ()) -> IO () #-}
iforLevelM_ :: Monad m => Levels m h -> (LevelNo -> Level m h -> m ()) -> m ()
iforLevelM_ lvls k = V.iforM_ lvls $ \i lvl -> k (LevelNo (i + 1)) lvl

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
     (MonadMask m, MonadMVar m, MonadSTM m, MonadST m)
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
    supplyCredits conf (Credits numAdded) (tableLevels tc)
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
  -> Ref (WriteBufferBlobs IO h)
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
  -> Ref (WriteBufferBlobs m h)
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
     (MonadMask m, MonadMVar m, MonadST m, MonadSTM m)
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
            releaseRef
    freeTemp reg (releaseRef (tableWriteBufferBlobs tc))
    wbblobs' <- allocateTemp reg (WBB.new hfs (Paths.tableBlobPath root n))
                                 releaseRef
    levels' <- addRunToLevels tr conf resolve hfs hbio root uc r reg (tableLevels tc)
    tableCache' <- rebuildCache reg (tableCache tc) levels'
    pure $! TableContent {
        tableWriteBuffer = WB.empty
      , tableWriteBufferBlobs = wbblobs'
      , tableLevels = levels'
      , tableCache = tableCache'
      }

{-# SPECIALISE addRunToLevels ::
     Tracer IO (AtLevel MergeTrace)
  -> TableConfig
  -> ResolveSerialisedValue
  -> HasFS IO h
  -> HasBlockIO IO h
  -> SessionRoot
  -> UniqCounter IO
  -> Ref (Run IO h)
  -> TempRegistry IO
  -> Levels IO h
  -> IO (Levels IO h) #-}
-- | Add a run to the levels, and propagate merges.
--
-- NOTE: @go@ is based on the @ScheduledMerges.increment@ prototype.
-- See @ScheduledMerges.increment@ for documentation about the merge algorithm.
addRunToLevels ::
     forall m h.
     (MonadMask m, MonadMVar m, MonadST m, MonadSTM m)
  => Tracer m (AtLevel MergeTrace)
  -> TableConfig
  -> ResolveSerialisedValue
  -> HasFS m h
  -> HasBlockIO m h
  -> SessionRoot
  -> UniqCounter m
  -> Ref (Run m h)
  -> TempRegistry m
  -> Levels m h
  -> m (Levels m h)
addRunToLevels tr conf@TableConfig{..} resolve hfs hbio root uc r0 reg levels = do
    go (LevelNo 1) (V.singleton r0) levels
  where
    -- NOTE: @go@ is based on the @increment@ function from the
    -- @ScheduledMerges@ prototype.
    --
    -- Releases the vector of runs.
    go ::
         LevelNo
      -> V.Vector (Ref (Run m h))
      -> V.Vector (Level m h )
      -> m (V.Vector (Level m h))
    go !ln rs (V.uncons -> Nothing) = do
        traceWith tr $ AtLevel ln TraceAddLevel
        -- Make a new level
        let policyForLevel = mergePolicyForLevel confMergePolicy ln V.empty
        ir <- newMerge policyForLevel Merge.LastLevel ln rs
        return $! V.singleton $ Level ir V.empty
    go !ln rs' (V.uncons -> Just (Level ir rs, ls)) = do
        r <- expectCompletedMerge ln ir
        case mergePolicyForLevel confMergePolicy ln ls of
          -- If r is still too small for this level then keep it and merge again
          -- with the incoming runs.
          LevelTiering | Run.size r <= maxRunSize' conf LevelTiering (pred ln) -> do
            let mergelast = mergeLastForLevel ls
            ir' <- newMerge LevelTiering mergelast ln (rs' `V.snoc` r)
            pure $! Level ir' rs `V.cons` ls
          -- This tiering level is now full. We take the completed merged run
          -- (the previous incoming runs), plus all the other runs on this level
          -- as a bundle and move them down to the level below. We start a merge
          -- for the new incoming runs. This level is otherwise empty.
          LevelTiering | levelIsFull confSizeRatio rs -> do
            ir' <- newMerge LevelTiering Merge.MidLevel ln rs'
            ls' <- go (succ ln) (r `V.cons` rs) ls
            pure $! Level ir' V.empty `V.cons` ls'
          -- This tiering level is not yet full. We move the completed merged run
          -- into the level proper, and start the new merge for the incoming runs.
          LevelTiering -> do
            let mergelast = mergeLastForLevel ls
            ir' <- newMerge LevelTiering mergelast ln rs'
            traceWith tr $ AtLevel ln
                         $ TraceAddRun
                            (Run.runFsPathsNumber r)
                            (V.map Run.runFsPathsNumber rs)
            pure $! Level ir' (r `V.cons` rs) `V.cons` ls
          -- The final level is using levelling. If the existing completed merge
          -- run is too large for this level, we promote the run to the next
          -- level and start merging the incoming runs into this (otherwise
          -- empty) level .
          LevelLevelling | Run.size r > maxRunSize' conf LevelLevelling ln -> do
            assert (V.null rs && V.null ls) $ pure ()
            ir' <- newMerge LevelTiering Merge.MidLevel ln rs'
            ls' <- go (succ ln) (V.singleton r) V.empty
            pure $! Level ir' V.empty `V.cons` ls'
          -- Otherwise we start merging the incoming runs into the run.
          LevelLevelling -> do
            assert (V.null rs && V.null ls) $ pure ()
            ir' <- newMerge LevelLevelling Merge.LastLevel ln (rs' `V.snoc` r)
            pure $! Level ir' V.empty `V.cons` V.empty

    -- Releases the incoming run.
    expectCompletedMerge :: LevelNo -> IncomingRun m h -> m (Ref (Run m h))
    expectCompletedMerge ln ir = do
      r <- case ir of
        Single r     -> pure r
        Merging _ mr -> do
          r <- allocateTemp reg (MR.expectCompleted mr) releaseRef
          freeTemp reg (releaseRef mr)
          pure r
      traceWith tr $ AtLevel ln $
        TraceExpectCompletedMerge (Run.runFsPathsNumber r)
      pure r

    -- Releases the runs.
    newMerge :: MergePolicyForLevel
             -> Merge.Level
             -> LevelNo
             -> V.Vector (Ref (Run m h))
             -> m (IncomingRun m h)
    newMerge mergePolicy mergeLevel ln rs
      | Just (r, rest) <- V.uncons rs
      , V.null rest = do
          traceWith tr $ AtLevel ln $
            TraceNewMergeSingleRun (Run.size r)
                                   (Run.runFsPathsNumber r)
          -- We create a fresh reference and release the original one.
          -- This will also make it easier to trace back where it was allocated.
          ir <- Single <$> allocateTemp reg (dupRef r) releaseRef
          freeTemp reg (releaseRef r)
          pure ir

      | otherwise = do
        assert (let l = V.length rs in l >= 2 && l <= 5) $ pure ()
        !n <- incrUniqCounter uc
        let !caching = diskCachePolicyForLevel confDiskCachePolicy ln
            !alloc = bloomFilterAllocForLevel conf ln
            !runPaths = Paths.runPath root (uniqueToRunNumber n)
        traceWith tr $ AtLevel ln $
          TraceNewMerge (V.map Run.size rs) (runNumber runPaths) caching alloc mergePolicy mergeLevel
        -- The runs will end up inside the merging run, with fresh references.
        -- The original references can be released (but only on the happy path).
        mr <- allocateTemp reg
          (MR.new hfs hbio resolve caching alloc mergeLevel runPaths rs)
          releaseRef
        V.forM_ rs $ \r -> freeTemp reg (releaseRef r)
        case confMergeSchedule of
          Incremental -> pure ()
          OneShot -> do
            let !required = MR.Credits (unNumEntries (V.foldMap' Run.size rs))
            let !thresh = creditThresholdForLevel conf ln
            MR.supplyCredits required thresh mr
            -- This ensures the merge is really completed. However, we don't
            -- release the merge yet and only briefly inspect the resulting run.
            bracket (MR.expectCompleted mr) releaseRef $ \r ->
              traceWith tr $ AtLevel ln $
                TraceCompletedMerge (Run.size r) (Run.runFsPathsNumber r)

        return (Merging mergePolicy mr)

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

levelIsFull :: SizeRatio -> V.Vector run -> Bool
levelIsFull sr rs = V.length rs + 1 >= (sizeRatioInt sr)

{-------------------------------------------------------------------------------
  Credits
-------------------------------------------------------------------------------}

{-
  Note [Credits]
~~~~~~~~~~~~~~

  With scheduled merges, each update (e.g., insert) on a table contributes to
  the progression of ongoing merges in the levels structure. This ensures that
  merges are finished in time before a new merge has to be started. The points
  in the evolution of the levels structure where new merges are started are
  known: a flush of a full write buffer will create a new run on the first
  level, and after sufficient flushes (e.g., 4) we will start at least one new
  merge on the second level. This may cascade down to lower levels depending on
  how full the levels are. As such, we have a well-defined measure to determine
  when merges should be finished: it only depends on the maximum size of the
  write buffer!

  The simplest solution to making sure merges are done in time is to step them
  to completion immediately when started. This does not, however, spread out
  work over time nicely. Instead, we schedule merge work based on how many
  updates are made on the table, taking care to ensure that the merge is
  finished /just/ in time before the next flush comes around, and not too early.

  TODO: we can still spread out work more evenly over time. We are finishing
  some merges too early, for example. See 'creditsForMerge'.

  The progression is tracked using merge credits, where each single update
  contributes a single credit to each ongoing merge. This is equivalent to
  saying we contribute a credit to each level, since each level contains
  precisely one ongoing merge. Contributing a credit does not, however,
  translate directly to doing one /unit/ of merging work:

  * The amount of work to do for one credit is adjusted depending on the type of
    merge we are doing. Last-level merges, for example, can have larger inputs,
    and therefore we have to do a little more work for each credit. As such, we
    /scale/ credits for the specific type of merge.

  * Unspent credits are accumulated until they go over a threshold, after which
    a batch of merge work will be performed. Configuring this threshold should
    allow to achieve a nice balance between spreading out I/O and achieving good
    (concurrent) performance.

  Merging runs can be shared across tables, which means that multiple threads
  can contribute to the same merge concurrently.
-}

-- | Merge credits that get supplied to a table's levels.
newtype Credits = Credits Int

{-# SPECIALISE supplyCredits ::
     TableConfig
  -> Credits
  -> Levels IO h
  -> IO ()
  #-}
-- | Supply the given amount of credits to each merge in the levels structure.
-- This /may/ cause some merges to progress.
supplyCredits ::
     (MonadSTM m, MonadST m, MonadMVar m, MonadMask m)
  => TableConfig
  -> Credits
  -> Levels m h
  -> m ()
supplyCredits conf c levels =
    iforLevelM_ levels $ \ln (Level ir _rs) ->
      case ir of
        Single{}   -> pure ()
        Merging mp mr -> do
          let !c' = scaleCreditsForMerge mp mr c
          let !thresh = creditThresholdForLevel conf ln
          MR.supplyCredits c' thresh mr

-- | Scale a number of credits to a number of merge steps to be performed, based
-- on the merging run.
--
-- Initially, 1 update supplies 1 credit. However, since merging runs have
-- different numbers of input runs/entries, we may have to a more or less
-- merging work than 1 merge step for each credit.
scaleCreditsForMerge :: MergePolicyForLevel -> Ref (MergingRun m h) -> Credits -> MR.Credits
-- A single run is a trivially completed merge, so it requires no credits.
scaleCreditsForMerge LevelTiering _ (Credits c) =
    -- A tiering merge has 5 runs at most (one could be held back to merged
    -- again) and must be completed before the level is full (once 4 more
    -- runs come in).
    MR.Credits (c * (1 + 4))

scaleCreditsForMerge LevelLevelling (DeRef mr) (Credits c) =
    -- A levelling merge has 1 input run and one resident run, which is (up
    -- to) 4x bigger than the others. It needs to be completed before
    -- another run comes in.
    -- TODO: this is currently assuming a naive worst case, where the
    -- resident run is as large as it can be for the current level. We
    -- probably have enough information available here to lower the
    -- worst-case upper bound by looking at the sizes of the input runs.
    -- As as result, merge work would/could be more evenly distributed over
    -- time when the resident run is smaller than the worst case.
    let NumRuns n = MR.mergeNumRuns mr
       -- same as division rounding up: ceiling (c * n / 4)
    in MR.Credits ((c * n + 3) `div` 4)

-- TODO: the thresholds for doing merge work should be different for each level,
-- maybe co-prime?
creditThresholdForLevel :: TableConfig -> LevelNo -> MR.CreditThreshold
creditThresholdForLevel conf (LevelNo _i) =
    let AllocNumEntries (NumEntries x) = confWriteBufferAlloc conf
    in  MR.CreditThreshold x
