{-# LANGUAGE CPP          #-}
{-# LANGUAGE DataKinds    #-}
{-# LANGUAGE TypeFamilies #-}

{- HLINT ignore "Use when" -}

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
  , MergingRun (..)
  , newMergingRun
  , NumRuns (..)
  , UnspentCreditsVar (..)
  , MergingRunState (..)
  , TotalStepsVar (..)
  , SpentCreditsVar (..)
  , MergeKnownCompleted (..)
    -- * Flushes and scheduled merges
  , updatesWithInterleavedFlushes
  , flushWriteBuffer
    -- * Exported for cabal-docspec
  , MergePolicyForLevel (..)
  , maxRunSize
    -- * Credits
  , Credit (..)
  , supplyCredits
  , ScaledCredits (..)
  , supplyMergeCredits
  , CreditThreshold (..)
  , creditThresholdForLevel
  ) where

import           Control.Concurrent.Class.MonadMVar.Strict
import           Control.Monad (void, when, (<$!>))
import           Control.Monad.Class.MonadST (MonadST)
import           Control.Monad.Class.MonadSTM (MonadSTM (..))
import           Control.Monad.Class.MonadThrow (MonadCatch (bracketOnError),
                     MonadMask, MonadThrow (..))
import           Control.Monad.Primitive
import           Control.RefCount
import           Control.TempRegistry
import           Control.Tracer
import           Data.BloomFilter (Bloom)
import           Data.Foldable (fold)
import           Data.Primitive.MutVar
import           Data.Primitive.PrimVar
import qualified Data.Vector as V
import           Database.LSMTree.Internal.Assertions (assert)
import           Database.LSMTree.Internal.Config
import           Database.LSMTree.Internal.Entry (Entry, NumEntries (..),
                     unNumEntries)
import           Database.LSMTree.Internal.Index.Compact (IndexCompact)
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
    return $ TableContent wb wbb' levels' cache'

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
      (duplicateMergingRunRuns reg)
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
            Single r   -> k1 r
            Merging mr -> k2 mr
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
     | Merging !(Ref (MergingRun m h))

-- | A merging of multiple runs.
--
-- TODO: Move to a separate module.
data MergingRun m h = MergingRun {
      mergePolicy         :: !MergePolicyForLevel
    , mergeNumRuns        :: !NumRuns
      -- | Sum of number of entries in the input runs
    , mergeNumEntries     :: !NumEntries
      -- | The number of currently /unspent/ credits
    , mergeUnspentCredits :: !(UnspentCreditsVar (PrimState m))
      -- | The total number of performed merging steps.
    , mergeStepsPerformed :: !(TotalStepsVar (PrimState m))
      -- | A variable that caches knowledge about whether the merge has been
      -- completed. If 'MergeKnownCompleted', then we are sure the merge has been
      -- completed, otherwise if 'MergeMaybeCompleted' we have to check the
      -- 'MergingRunState'.
    , mergeKnownCompleted :: !(MutVar (PrimState m) MergeKnownCompleted)
    , mergeState          :: !(StrictMVar m (MergingRunState m h))
    , mergeRefCounter     :: !(RefCounter m)
    }

instance RefCounted (MergingRun m h) where
    type FinaliserM (MergingRun m h) = m
    getRefCounter = mergeRefCounter

{-# SPECIALISE newMergingRun ::
     MergePolicyForLevel
  -> NumRuns
  -> NumEntries
  -> MergeKnownCompleted
  -> MergingRunState IO h
  -> IO (Ref (MergingRun IO h))
  #-}
-- | This allows constructing ill-formed MergingRuns, but the flexibility is
-- needed for creating a merging run that is already Completed, as well as
-- opening a merging run from a snapshot.
--
-- TODO: instead create a Single run when OneShot merging?
--
-- TODO: do not store MergeKnownCompleted in snapshot? It's redundant.
--
-- TODO: slightly different API for opening from snapshot?
newMergingRun ::
     (MonadMVar m, MonadMask m, MonadSTM m, MonadST m)
  => MergePolicyForLevel
  -> NumRuns
  -> NumEntries
  -> MergeKnownCompleted
  -> MergingRunState m h
  -> m (Ref (MergingRun m h))
newMergingRun mergePolicy mergeNumRuns mergeNumEntries knownCompleted state = do
    mergeUnspentCredits <- UnspentCreditsVar <$> newPrimVar 0
    mergeStepsPerformed <- TotalStepsVar <$> newPrimVar 0
    case state of
      OngoingMerge{}   -> assert (knownCompleted == MergeMaybeCompleted) (pure ())
      CompletedMerge{} -> pure ()
    mergeKnownCompleted <- newMutVar knownCompleted
    mergeState <- newMVar $! state
    newRef (finalise mergeState) $ \mergeRefCounter ->
      MergingRun {
        mergePolicy
      , mergeNumRuns
      , mergeNumEntries
      , mergeUnspentCredits
      , mergeStepsPerformed
      , mergeKnownCompleted
      , mergeState
      , mergeRefCounter
      }
  where
    finalise var = withMVar var $ \case
        CompletedMerge r ->
          releaseRef r
        OngoingMerge rs _ m -> do
          V.forM_ rs releaseRef
          Merge.abort m

-- | Create references to the runs that should be queried for lookups.
-- In particular, if the merge is not complete, these are the input runs.
duplicateMergingRunRuns ::
     (PrimMonad m, MonadMVar m, MonadMask m)
  => TempRegistry m
  -> Ref (MergingRun m h)
  -> m (V.Vector (Ref (Run m h)))
duplicateMergingRunRuns reg (DeRef mr) =
    -- We take the references while holding the MVar to make sure the MergingRun
    -- does not get completed concurrently before we are done.
    withMVar (mergeState mr) $ \case
      CompletedMerge r    -> V.singleton <$> dupRun r
      OngoingMerge rs _ _ -> V.mapM dupRun rs
  where
    dupRun r = allocateTemp reg (dupRef r) releaseRef

data MergePolicyForLevel = LevelTiering | LevelLevelling
  deriving stock (Show, Eq)

mergePolicyForLevel :: MergePolicy -> LevelNo -> Levels m h -> MergePolicyForLevel
mergePolicyForLevel MergePolicyLazyLevelling (LevelNo n) nextLevels
  | n == 1
  , V.null nextLevels
  = LevelTiering    -- always use tiering on first level
  | V.null nextLevels = LevelLevelling  -- levelling on last level
  | otherwise         = LevelTiering

newtype NumRuns = NumRuns { unNumRuns :: Int }
  deriving stock (Show, Eq)

newtype UnspentCreditsVar s = UnspentCreditsVar { getUnspentCreditsVar :: PrimVar s Int }

data MergingRunState m h =
    CompletedMerge
      !(Ref (Run m h))
      -- ^ Output run
  | OngoingMerge
      !(V.Vector (Ref (Run m h)))
      -- ^ Input runs
      !(SpentCreditsVar (PrimState m))
      -- ^ The total number of spent credits.
      !(Merge m h)

newtype TotalStepsVar s = TotalStepsVar { getTotalStepsVar ::  PrimVar s Int  }

newtype SpentCreditsVar s = SpentCreditsVar { getSpentCreditsVar :: PrimVar s Int }

data MergeKnownCompleted = MergeKnownCompleted | MergeMaybeCompleted
  deriving stock (Show, Eq, Read)

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
      return Level {
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

duplicateIncomingRun reg (Merging mr) =
    Merging <$> allocateTemp reg (dupRef mr) releaseRef

{-# SPECIALISE releaseIncomingRun :: TempRegistry IO -> IncomingRun IO h -> IO () #-}
releaseIncomingRun ::
     (PrimMonad m, MonadMask m, MonadMVar m)
  => TempRegistry m
  -> IncomingRun m h -> m ()
releaseIncomingRun reg (Single r)   = freeTemp reg (releaseRef r)
releaseIncomingRun reg (Merging mr) = freeTemp reg (releaseRef mr)

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
    supplyCredits conf (Credit numAdded) (tableLevels tc)
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
    fitsLB policy r ln = maxRunSize sr wba policy (pred ln) < Run.size r
    -- Check that a run is too small for next levels
    fitsUB policy r ln = Run.size r <= maxRunSize sr wba policy ln
-}

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
-- NOTE: @go@ is based on the @ScheduledMerges.increment@ prototype. See @ScheduledMerges.increment@
-- for documentation about the merge algorithm.
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
        ir <- newMerge policyForLevel Merge.LastLevel ln rs
        return $ V.singleton $ Level ir V.empty
    go !ln rs' (V.uncons -> Just (Level ir rs, ls)) = do
        r <- expectCompletedMergeTraced ln ir
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

    expectCompletedMergeTraced :: LevelNo -> IncomingRun m h
                               -> m (Ref (Run m h))
    expectCompletedMergeTraced ln ir = do
      r <- expectCompletedMerge reg ir
      traceWith tr $ AtLevel ln $
        TraceExpectCompletedMerge (Run.runFsPathsNumber r)
      pure r

    -- TODO: refactor, pull to top level?
    newMerge :: MergePolicyForLevel
             -> Merge.Level
             -> LevelNo
             -> V.Vector (Ref (Run m h))
             -> m (IncomingRun m h)
    newMerge mergePolicy mergelast ln rs
      | Just (r, rest) <- V.uncons rs
      , V.null rest = do
          traceWith tr $ AtLevel ln $
            TraceNewMergeSingleRun (Run.size r)
                                   (Run.runFsPathsNumber r)
          pure (Single r)
      | otherwise = do
        assert (let l = V.length rs in l >= 2 && l <= 5) $ pure ()
        !n <- incrUniqCounter uc
        let !caching = diskCachePolicyForLevel confDiskCachePolicy ln
            !alloc = bloomFilterAllocForLevel conf ln
            !runPaths = Paths.runPath root (uniqueToRunNumber n)
        traceWith tr $ AtLevel ln $
          TraceNewMerge (V.map Run.size rs) (runNumber runPaths) caching alloc mergePolicy mergelast
        let numInputRuns = NumRuns $ V.length rs
        let numInputEntries = V.foldMap' Run.size rs
        case confMergeSchedule of
          OneShot -> do
            r <- allocateTemp reg
                  (mergeRuns resolve hfs hbio caching alloc runPaths mergelast rs)
                  releaseRef
            traceWith tr $ AtLevel ln $
              TraceCompletedMerge (Run.size r)
                                  (Run.runFsPathsNumber r)
            V.mapM_ (freeTemp reg . releaseRef) rs
            Merging <$!> newMergingRun mergePolicy numInputRuns numInputEntries MergeKnownCompleted (CompletedMerge r)

          Incremental -> do
            mergeMaybe <- allocateMaybeTemp reg
              (Merge.new hfs hbio caching alloc mergelast resolve runPaths rs)
              Merge.abort
            case mergeMaybe of
              Nothing -> error "newMerge: merges can not be empty"
              Just m -> do
                spentCreditsVar <- SpentCreditsVar <$> newPrimVar 0
                Merging <$!> newMergingRun mergePolicy numInputRuns numInputEntries MergeMaybeCompleted (OngoingMerge rs spentCreditsVar m)

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

{-# SPECIALISE mergeRuns :: ResolveSerialisedValue -> HasFS IO h -> HasBlockIO IO h -> RunDataCaching -> RunBloomFilterAlloc -> RunFsPaths -> Merge.Level -> V.Vector (Ref (Run IO h)) -> IO (Ref (Run IO h)) #-}
mergeRuns ::
     (MonadMask m, MonadST m, MonadSTM m)
  => ResolveSerialisedValue
  -> HasFS m h
  -> HasBlockIO m h
  -> RunDataCaching
  -> RunBloomFilterAlloc
  -> RunFsPaths
  -> Merge.Level
  -> V.Vector (Ref (Run m h))
  -> m (Ref (Run m h))
mergeRuns resolve hfs hbio caching alloc runPaths mergeLevel runs = do
    Merge.new hfs hbio caching alloc mergeLevel resolve runPaths runs >>= \case
      Nothing -> error "mergeRuns: no inputs"
      Just m -> Merge.stepsToCompletion m 1024

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

  As mentioned, merge work is done in batches based on accumulated, unspent
  credits and a threshold value. Moreover, merging runs can be shared across
  tables, which means that multiple threads can contribute to the same merge
  concurrently. The design to contribute credits to the same merging run is
  largely lock-free. The design ensures consistency of the unspent credits and
  the merge state, while allowing threads to progress without waiting on other
  threads.

  First, scaled credits are added atomically to a PrimVar that holds the current
  total of unspent credits. If this addition exceeded the threshold, then
  credits are atomically subtracted from the PrimVar to get it below the
  threshold. The number of subtracted credits is then the number of merge steps
  that will be performed. While doing the merging work, a (more expensive) MVar
  lock is taken to ensure that the merging work itself is performed only
  sequentially. If at some point, doing the merge work resulted in the merge
  being done, then the merge is converted into a new run.

  In the presence of async exceptions, we offer a weaker guarantee regarding
  consistency of the accumulated, unspent credits and the merge state: a merge
  /may/ progress more than the number of credits that were taken. If an async
  exception happens at some point during merging work, then we put back all the
  credits we took beforehand. This makes the implementation simple, and merges
  will still finish in time. It would be bad if we did not put back credits,
  because then a merge might not finish in time, which will mess up the shape of
  the levels tree.

  The implementation also tracks the total of spent credits, and the number of
  perfomed merge steps. These are the use cases:

  * The total of spent credits + the total of unspent credits is used by the
    snapshot feature to restore merge work on snapshot load that was lost during
    snapshot creation.

  * For simplicity, merges are allowed to do more steps than requested. However,
    it does mean that once we do more steps next time a batch of work is done,
    then we should account for the surplus of steps performed by the previous
    batch. The total of spent credits + the number of performed merge steps is
    used to compute this surplus, and adjust for it.

    TODO: we should reconsider at some later point in time whether this surplus
    adjustment is necessary. It does not make a difference for correctness, but it
    does mean we get a slightly better distribution of work over time. For
    sensible batch sizes and workloads without many duplicate keys, it probably
    won't make much of a difference. However, without this calculation the surplus
    can accumulate over time, so if we're really pedantic about work distribution
    then this is the way to go

  Async exceptions are allowed to mess up the consistency between the the merge
  state, the merge steps performed variable, and the spent credits variable.
  There is an important invariant that we maintain, even in the presence of
  async exceptions: @merge steps actually performed >= recorded merge steps
  performed >= recorded spent credits@. TODO: and this makes it correct (?).
-}

newtype Credit = Credit Int

{-# SPECIALISE supplyCredits ::
     TableConfig
  -> Credit
  -> Levels IO h
  -> IO ()
  #-}
-- | Supply the given amount of credits to each merge in the levels structure.
-- This /may/ cause some merges to progress.
supplyCredits ::
     (MonadSTM m, MonadST m, MonadMVar m, MonadMask m)
  => TableConfig
  -> Credit
  -> Levels m h
  -> m ()
supplyCredits conf c levels =
    iforLevelM_ levels $ \ln (Level ir _rs) ->
      let !c' = scaleCreditsForMerge ir c in
      let !creditsThresh = creditThresholdForLevel conf ln in
      supplyMergeCredits c' creditsThresh ir

-- | 'Credit's scaled based on the merge requirements for merging runs. See
-- 'scaleCreditsForMerge'.
newtype ScaledCredits = ScaledCredits Int

-- | Scale a number of credits to a number of merge steps to be performed, based
-- on the merging run.
--
-- Initially, 1 update supplies 1 credit. However, since merging runs have
-- different numbers of input runs/entries, we may have to a more or less
-- merging work than 1 merge step for each credit.
scaleCreditsForMerge :: IncomingRun m h -> Credit -> ScaledCredits
-- A single run is a trivially completed merge, so it requires no credits.
scaleCreditsForMerge (Single _) _ = ScaledCredits 0
scaleCreditsForMerge (Merging (DeRef MergingRun {..})) (Credit c) =
    case mergePolicy of
      LevelTiering ->
        -- A tiering merge has 5 runs at most (one could be held back to merged
        -- again) and must be completed before the level is full (once 4 more
        -- runs come in).
        ScaledCredits (c * (1 + 4))
      LevelLevelling ->
        -- A levelling merge has 1 input run and one resident run, which is (up
        -- to) 4x bigger than the others. It needs to be completed before
        -- another run comes in.
        -- TODO: this is currently assuming a naive worst case, where the
        -- resident run is as large as it can be for the current level. We
        -- probably have enough information available here to lower the
        -- worst-case upper bound by looking at the sizes of the input runs.
        -- As as result, merge work would/could be more evenly distributed over
        -- time when the resident run is smaller than the worst case.
        let NumRuns n = mergeNumRuns
           -- same as division rounding up: ceiling (c * n / 4)
        in ScaledCredits ((c * n + 3) `div` 4)

{-# SPECIALISE supplyMergeCredits :: ScaledCredits -> CreditThreshold -> IncomingRun IO h -> IO () #-}
-- | Supply the given amount of credits to a merging run. This /may/ cause an
-- ongoing merge to progress.
supplyMergeCredits ::
     forall m h. (MonadSTM m, MonadST m, MonadMVar m, MonadMask m)
  => ScaledCredits
  -> CreditThreshold
  -> IncomingRun m h
  -> m ()
supplyMergeCredits _ _ Single{} = pure ()
supplyMergeCredits (ScaledCredits c) creditsThresh
                   (Merging (DeRef MergingRun {..})) = do
    mergeCompleted <- readMutVar mergeKnownCompleted

    -- The merge is already finished
    if mergeCompleted == MergeKnownCompleted then
      pure ()
    else do
      -- unspentCredits' is our /estimate/ of what the new total of unspent credits is.
      Credit unspentCredits' <- addUnspentCredits mergeUnspentCredits (Credit c)
      totalSteps <- readPrimVar (getTotalStepsVar mergeStepsPerformed)

      -- We can finish the merge immediately
      if totalSteps + unspentCredits' >= unNumEntries mergeNumEntries then do
        isMergeDone <-
          bracketOnError (takeAllUnspentCredits mergeUnspentCredits)
                         (putBackUnspentCredits mergeUnspentCredits)
                         (stepMerge mergeState mergeStepsPerformed)
        when isMergeDone $ completeMerge mergeState mergeKnownCompleted
      -- We can do some merging work without finishing the merge immediately
      else if unspentCredits' >= getCreditThreshold creditsThresh then do
        isMergeDone <-
          -- Try to take some unspent credits. The number of taken credits is the
          -- number of merging steps we will try to do.
          --
          -- If an error happens during the body, then we put back as many credits
          -- as we took, even if the merge has progressed. See Note [Credits] why
          -- this is okay.
          bracketOnError
            (tryTakeUnspentCredits mergeUnspentCredits creditsThresh (Credit unspentCredits'))
            (mapM_ (putBackUnspentCredits mergeUnspentCredits)) $ \case
              Nothing -> pure False
              Just c' -> stepMerge mergeState mergeStepsPerformed c'

        -- If we just finished the merge, then we convert the output of the merge
        -- into a new run. i.e., we complete the merge.
        --
        -- If an async exception happens before we get to perform the
        -- completion, then that is fine. The next supplyMergeCredits will
        -- complete the merge.
        when isMergeDone $ completeMerge mergeState mergeKnownCompleted
      -- Just accumulate unspent credits, because we are not over the threshold yet
      else
        pure ()

{-# SPECIALISE addUnspentCredits ::
     UnspentCreditsVar RealWorld
  -> Credit
  -> IO Credit #-}
-- | Add credits to unspent credits. Returns the /estimate/ of what the new
-- total of unspent credits is. The /actual/ total might have been changed again
-- by a different thread.
addUnspentCredits ::
     PrimMonad m
  => UnspentCreditsVar (PrimState m)
  -> Credit
  -> m Credit
addUnspentCredits (UnspentCreditsVar !var) (Credit c) = Credit . (c+) <$> fetchAddInt var c

{-# SPECIALISE tryTakeUnspentCredits ::
     UnspentCreditsVar RealWorld
  -> CreditThreshold
  -> Credit
  -> IO (Maybe Credit) #-}
-- | In a CAS-loop, subtract credits from the unspent credits to get it below
-- the threshold again. If succesful, return Just that many credits, or Nothing
-- otherwise.
--
-- The number of taken credits is a multiple of creditsThresh, so that the
-- amount of merging work that we do each time is relatively uniform.
--
-- Nothing can be returned if the variable has already gone below the threshold,
-- which may happen if another thread is concurrently doing the same loop on
-- 'mergeUnspentCredits'.
tryTakeUnspentCredits ::
     PrimMonad m
  => UnspentCreditsVar (PrimState m)
  -> CreditThreshold
  -> Credit
  -> m (Maybe Credit)
tryTakeUnspentCredits
    unspentCreditsVar@(UnspentCreditsVar !var)
    thresh@(CreditThreshold !creditsThresh)
    (Credit !prev)
  | prev < creditsThresh = pure Nothing
  | otherwise = do
      -- numThresholds is guaranteed to be >= 1
      let !numThresholds = prev `div` creditsThresh
          !creditsToTake = numThresholds * creditsThresh
          !new = prev - creditsToTake
      assert (new < creditsThresh) $ pure ()
      prev' <- casInt var prev new
      if prev' == prev then
        pure (Just (Credit creditsToTake))
      else
        tryTakeUnspentCredits unspentCreditsVar thresh (Credit prev')

{-# SPECIALISE putBackUnspentCredits :: UnspentCreditsVar RealWorld -> Credit -> IO () #-}
putBackUnspentCredits ::
     PrimMonad m
  => UnspentCreditsVar (PrimState m)
  -> Credit
  -> m ()
putBackUnspentCredits (UnspentCreditsVar !var) (Credit !x) = void $ fetchAddInt var x

{-# SPECIALISE takeAllUnspentCredits :: UnspentCreditsVar RealWorld -> IO Credit #-}
-- | In a CAS-loop, subtract all unspent credits and return them.
takeAllUnspentCredits ::
     PrimMonad m
  => UnspentCreditsVar (PrimState m)
  -> m Credit
takeAllUnspentCredits (UnspentCreditsVar !unspentCreditsVar) = do
    prev <- readPrimVar unspentCreditsVar
    casLoop prev
  where
    casLoop !prev = do
      prev' <- casInt unspentCreditsVar prev 0
      if prev' == prev then
        pure (Credit prev)
      else
        casLoop prev'

{-# SPECIALISE stepMerge :: StrictMVar IO (MergingRunState IO h) -> TotalStepsVar RealWorld -> Credit -> IO Bool #-}
stepMerge ::
     (MonadMVar m, MonadMask m, MonadSTM m, MonadST m)
  => StrictMVar m (MergingRunState m h)
  -> TotalStepsVar (PrimState m)
  -> Credit
  -> m Bool
stepMerge mergeVar (TotalStepsVar totalStepsVar) (Credit c) =
    withMVar mergeVar $ \case
      CompletedMerge{} -> pure False
      (OngoingMerge
          _rs
          (SpentCreditsVar spentCreditsVar)
          m) -> do
        totalSteps <- readPrimVar totalStepsVar
        spentCredits <- readPrimVar spentCreditsVar

        -- If we previously performed too many merge steps, then we
        -- perform fewer now.
        let stepsToDo = max 0 (spentCredits + c - totalSteps)
        -- Merge.steps guarantees that stepsDone >= stepsToDo /unless/
        -- the merge was just now finished.
        (stepsDone, stepResult) <- Merge.steps m stepsToDo
        assert (case stepResult of
                  MergeInProgress -> stepsDone >= stepsToDo
                  MergeDone       -> True
                ) $ pure ()

        -- This should be the only point at which we write to these
        -- variables.
        --
        -- It is guaranteed that totalSteps' >= spentCredits' /unless/
        -- the merge was just now finished.
        let totalSteps' = totalSteps + stepsDone
        let spentCredits' = spentCredits + c
        -- It is guaranteed that @readPrimVar totalStepsVar >=
        -- readPrimVar spentCreditsVar@, /unless/ the merge was just now
        -- finished.
        writePrimVar totalStepsVar $! totalSteps'
        writePrimVar spentCreditsVar $! spentCredits'
        assert (case stepResult of
                  MergeInProgress -> totalSteps' >= spentCredits'
                  MergeDone       -> True
              ) $ pure ()

        pure $ stepResult == MergeDone

{-# SPECIALISE completeMerge ::
     StrictMVar IO (MergingRunState IO h)
  -> MutVar RealWorld MergeKnownCompleted
  -> IO () #-}
-- | Convert an 'OngoingMerge' to a 'CompletedMerge'.
completeMerge ::
     (MonadSTM m, MonadST m, MonadMVar m, MonadMask m)
  => StrictMVar m (MergingRunState m h)
  -> MutVar (PrimState m) MergeKnownCompleted
  -> m ()
completeMerge mergeVar mergeKnownCompletedVar = do
    modifyMVarMasked_ mergeVar $ \case
      mrs@CompletedMerge{} -> pure $! mrs
      (OngoingMerge rs _spentCreditsVar m) -> do
        -- first try to complete the merge before performing other side effects,
        -- in case the completion fails
        r <- Merge.complete m
        V.forM_ rs releaseRef
        -- Cache the knowledge that we completed the merge
        writeMutVar mergeKnownCompletedVar MergeKnownCompleted
        pure $! CompletedMerge r

{-# SPECIALISE expectCompletedMerge :: TempRegistry IO -> IncomingRun IO h -> IO (Ref (Run IO h)) #-}
expectCompletedMerge ::
     (MonadMVar m, MonadSTM m, MonadST m, MonadMask m)
  => TempRegistry m -> IncomingRun m h -> m (Ref (Run m h))
expectCompletedMerge _ (Single r) = pure r
expectCompletedMerge reg (Merging (mr@(DeRef MergingRun {..}))) = do
    knownCompleted <- readMutVar mergeKnownCompleted
    -- The merge is not guaranteed to be complete, so we do the remaining steps
    when (knownCompleted == MergeMaybeCompleted) $ do
      totalSteps <- readPrimVar (getTotalStepsVar mergeStepsPerformed)
      isMergeDone <- stepMerge mergeState mergeStepsPerformed (Credit (unNumEntries mergeNumEntries - totalSteps))
      when isMergeDone $ completeMerge mergeState mergeKnownCompleted
      -- TODO: can we think of a check to see if we did not do too much work here?
    r <- withMVar mergeState $ \case
      CompletedMerge r -> pure r
      OngoingMerge{} -> do
        -- If the algorithm finds an ongoing merge here, then it is a bug in
        -- our merge sceduling algorithm. As such, we throw a pure error.
        error "expectCompletedMerge: expected a completed merge, but found an ongoing merge"
    -- return a fresh reference to the run
    r' <- allocateTemp reg (dupRef r) releaseRef
    freeTemp reg (releaseRef mr)
    pure r'

newtype CreditThreshold = CreditThreshold { getCreditThreshold :: Int }

-- TODO: the thresholds for doing merge work should be different for each level,
-- maybe co-prime?
creditThresholdForLevel :: TableConfig -> LevelNo -> CreditThreshold
creditThresholdForLevel conf (LevelNo _i) =
    let AllocNumEntries (NumEntries x) = confWriteBufferAlloc conf
    in  CreditThreshold x
