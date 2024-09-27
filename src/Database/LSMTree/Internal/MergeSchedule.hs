{-# LANGUAGE CPP       #-}
{-# LANGUAGE DataKinds #-}

module Database.LSMTree.Internal.MergeSchedule (
    -- * Traces
    AtLevel (..)
  , MergeTrace (..)
    -- * Table content
  , TableContent (..)
    -- * Levels cache
  , LevelsCache (..)
  , mkLevelsCache
    -- * Levels, runs and ongoing merges
  , Levels
  , Level (..)
  , MergingRun (..)
  , MergingRunState (..)
  , forRunM_
    -- * Flushes and scheduled merges
  , updatesWithInterleavedFlushes
  , flushWriteBuffer
    -- * Exported for cabal-docspec
  , MergePolicyForLevel (..)
  , maxRunSize
  ) where

import           Control.Monad.Primitive
import           Control.TempRegistry
import           Control.Tracer
import           Data.BloomFilter (Bloom)
import           Data.Primitive.MutVar
import qualified Data.Vector as V
import           Database.LSMTree.Internal.Assertions (assert)
import           Database.LSMTree.Internal.Config
import           Database.LSMTree.Internal.Entry (Entry, NumEntries (..))
import           Database.LSMTree.Internal.IndexCompact (IndexCompact)
import           Database.LSMTree.Internal.Lookup (ResolveSerialisedValue)
import           Database.LSMTree.Internal.Merge (Merge)
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
import           System.FS.API (Handle, HasFS)
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
  , tableLevels           :: !(Levels m (Handle h))
    -- | Cache of flattened 'levels'.
    --
    -- INVARIANT: when 'level's is modified, this cache should be updated as
    -- well, for example using 'mkLevelsCache'.
  , tableCache            :: !(LevelsCache m (Handle h))
  }

{-------------------------------------------------------------------------------
  Levels cache
-------------------------------------------------------------------------------}

-- | Flattend cache of the runs that referenced by a table handle.
--
-- This cache includes a vector of runs, but also vectors of the runs broken
-- down into components, like bloom filters, fence pointer indexes and file
-- handles. This allows for quick access in the lookup code. Recomputing this
-- cache should be relatively rare.
--
-- Use 'mkLevelsCache' to ensure that there are no mismatches between the vector
-- of runs and the vectors of run components.
data LevelsCache m h = LevelsCache_ {
    cachedRuns      :: !(V.Vector (Run m h))
  , cachedFilters   :: !(V.Vector (Bloom SerialisedKey))
  , cachedIndexes   :: !(V.Vector IndexCompact)
  , cachedKOpsFiles :: !(V.Vector h)
  }

{-# SPECIALISE mkLevelsCache :: Levels IO h -> IO (LevelsCache IO h) #-}
-- | Flatten the argument 'Level's into a single vector of runs, and use that to
-- populate the 'LevelsCache'.
mkLevelsCache :: PrimMonad m => Levels m h -> m (LevelsCache m h)
mkLevelsCache lvls = do
  rs <- forRunM lvls pure
  pure $! LevelsCache_ {
      cachedRuns      = rs
    , cachedFilters   = mapStrict Run.runFilter rs
    , cachedIndexes   = mapStrict Run.runIndex rs
    , cachedKOpsFiles = mapStrict Run.runKOpsFile rs
    }

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
    -- TODO: replace the MutVar by a different type of mutable location when
    -- implementing scheduled merges
    MergingRun !(MutVar (PrimState m) (MergingRunState m h))
  | SingleRun !(Run m h)

data MergingRunState m h =
    CompletedMerge !(Run m h)
  | OngoingMerge !(V.Vector (Run m h)) !(Merge m h)

{-# SPECIALISE forRunM_ :: Levels IO h -> (Run IO h -> IO ()) -> IO () #-}
forRunM_ :: PrimMonad m => Levels m h -> (Run m h -> m ()) -> m ()
forRunM_ lvls k = V.forM_ lvls $ \(Level mr rs) -> do
    case mr of
      SingleRun r    -> k r
      MergingRun var -> readMutVar var >>= \case
        CompletedMerge r -> k r
        OngoingMerge irs _m -> V.mapM_ k irs
    V.mapM_ k rs


{-# SPECIALISE foldRunM :: (b -> Run IO h -> IO b) -> b -> Levels IO h -> IO b #-}
foldRunM :: PrimMonad m => (b -> Run m h -> m b) -> b -> Levels m h -> m b
foldRunM f x lvls = flip (flip V.foldM x) lvls $ \y (Level mr rs) -> do
    z <- case mr of
      SingleRun r -> f y r
      MergingRun var -> readMutVar var >>= \case
        CompletedMerge r -> f y r
        OngoingMerge irs _m -> V.foldM f y irs
    V.foldM f z rs

{-# SPECIALISE forRunM :: Levels IO h -> (Run IO h -> IO a) -> IO (V.Vector a) #-}
-- TODO: this is not terribly performant, but it is also not sure if we are
-- going to need this in the end. We might get rid of the LevelsCache, and we
-- currently only use forRunM in mkLevelsCache.
forRunM :: PrimMonad m => Levels m h -> (Run m h -> m a) -> m (V.Vector a)
forRunM lvls k = do
    V.reverse . V.fromList <$> foldRunM (\acc r -> k r >>= \x -> pure (x : acc)) [] lvls

{-------------------------------------------------------------------------------
  Flushes and scheduled merges
-------------------------------------------------------------------------------}

{-# SPECIALISE updatesWithInterleavedFlushes :: Tracer IO (AtLevel MergeTrace) -> TableConfig -> ResolveSerialisedValue -> HasFS IO h -> HasBlockIO IO h -> SessionRoot -> UniqCounter IO -> V.Vector (SerialisedKey, Entry SerialisedValue SerialisedBlob) -> TempRegistry IO -> TableContent IO h -> IO (TableContent IO h) #-}
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
     forall m h. m ~ IO -- TODO: replace by @io-classes@ constraints for IO simulation.
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

-- | Add entries to the write buffer up until a certain write buffer size @n@.
--
-- NOTE: if the write buffer is larger @n@ already, this is a no-op.
addWriteBufferEntries ::
     HasFS IO h
  -> ResolveSerialisedValue
  -> WriteBufferBlobs IO h
  -> NumEntries
  -> WriteBuffer
  -> V.Vector (SerialisedKey, Entry SerialisedValue SerialisedBlob)
  -> IO (WriteBuffer, V.Vector (SerialisedKey, Entry SerialisedValue SerialisedBlob))
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


{-# SPECIALISE flushWriteBuffer :: Tracer IO (AtLevel MergeTrace) -> TableConfig -> ResolveSerialisedValue -> HasFS IO h -> HasBlockIO IO h -> SessionRoot -> UniqCounter IO -> TempRegistry IO -> TableContent IO h -> IO (TableContent IO h) #-}
-- | Flush the write buffer to disk, regardless of whether it is full or not.
--
-- The returned table content contains an updated set of levels, where the write
-- buffer is inserted into level 1.
flushWriteBuffer ::
     m ~ IO -- TODO: replace by @io-classes@ constraints for IO simulation.
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
    WBB.removeReference (tableWriteBufferBlobs tc)
    wbblobs' <- WBB.new hfs (Paths.tableBlobPath root n)
    levels' <- addRunToLevels tr conf resolve hfs hbio root uc r reg (tableLevels tc)
    cache' <- mkLevelsCache levels'
    pure $! TableContent {
        tableWriteBuffer = WB.empty
      , tableWriteBufferBlobs = wbblobs'
      , tableLevels = levels'
      , tableCache = cache'
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

{-# SPECIALISE addRunToLevels :: Tracer IO (AtLevel MergeTrace) -> TableConfig -> ResolveSerialisedValue -> HasFS IO h -> HasBlockIO IO h -> SessionRoot -> UniqCounter IO -> Run IO (Handle h) -> TempRegistry IO -> Levels IO (Handle h) -> IO (Levels IO (Handle h)) #-}
-- | Add a run to the levels, and propagate merges.
--
-- NOTE: @go@ is based on the @ScheduledMerges.increment@ prototype. See @ScheduledMerges.increment@
-- for documentation about the merge algorithm.
addRunToLevels ::
     forall m h. m ~ IO -- TODO: replace by @io-classes@ constraints for IO simulation.
  => Tracer m (AtLevel MergeTrace)
  -> TableConfig
  -> ResolveSerialisedValue
  -> HasFS m h
  -> HasBlockIO m h
  -> SessionRoot
  -> UniqCounter m
  -> Run m (Handle h)
  -> TempRegistry m
  -> Levels m (Handle h)
  -> m (Levels m (Handle h))
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
        -- TODO: until we have proper scheduling, the merging run is actually
        -- always stepped to completion immediately, so we can see it is just a
        -- single run.
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

    expectCompletedMerge :: LevelNo -> MergingRun m (Handle h) -> m (Run m (Handle h))
    expectCompletedMerge ln (SingleRun r) = do
      traceWith tr $ AtLevel ln $ TraceExpectCompletedMergeSingleRun (runNumber $ Run.runRunFsPaths r)
      pure r
    expectCompletedMerge ln (MergingRun var) = do
      readMutVar var >>= \case
        CompletedMerge r -> do
          traceWith tr $ AtLevel ln $ TraceExpectCompletedMerge (runNumber $ Run.runRunFsPaths r)
          pure r
        OngoingMerge _rs _m -> error "expectCompletedMerge: OngoingMerge not yet supported" -- TODO: implement.

    -- TODO: Until we implement proper scheduling, this does not only start a
    -- merge, but it also steps it to completion.
    newMerge :: MergePolicyForLevel
             -> Merge.Level
             -> LevelNo
             -> V.Vector (Run m (Handle h))
             -> m (MergingRun m (Handle h))
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
            var <- newMutVar (CompletedMerge r)
            pure $! MergingRun var
          Incremental -> error "newMerge: Incremental is not yet supported" -- TODO: implement

data MergePolicyForLevel = LevelTiering | LevelLevelling
  deriving stock Show

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
maxRunSize :: SizeRatio
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

{-# SPECIALISE mergeRuns :: ResolveSerialisedValue -> HasFS IO h -> HasBlockIO IO h -> RunDataCaching -> RunBloomFilterAlloc -> RunFsPaths -> Merge.Level -> V.Vector (Run IO (Handle h)) -> IO (Run IO (Handle h)) #-}
mergeRuns ::
     m ~ IO
  => ResolveSerialisedValue
  -> HasFS m h
  -> HasBlockIO m h
  -> RunDataCaching
  -> RunBloomFilterAlloc
  -> RunFsPaths
  -> Merge.Level
  -> V.Vector (Run m (Handle h))
  -> m (Run m (Handle h))
mergeRuns resolve hfs hbio caching alloc runPaths mergeLevel runs = do
    Merge.new hfs hbio caching alloc mergeLevel resolve runPaths runs >>= \case
      Nothing -> error "mergeRuns: no inputs"
      Just merge -> go merge
  where
    go m =
      Merge.steps m 1024 >>= \case
        (_, Merge.MergeInProgress)   -> go m
        (_, Merge.MergeComplete run) -> return run
