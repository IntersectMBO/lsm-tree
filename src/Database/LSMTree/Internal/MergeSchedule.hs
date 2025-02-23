{-# LANGUAGE CPP           #-}

#if !(MIN_VERSION_GLASGOW_HASKELL(9,0,0,0))
-- Fix for ghc 8.10.x with deriving newtype Prim
{-# LANGUAGE DataKinds     #-}
#endif

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
  , MergePolicyForLevel (..)
  , newIncomingSingleRun
  , newIncomingCompletedMergingRun
  , newIncomingMergingRun
  , supplyCreditsIncomingRun
  , snapshotIncomingRun
    -- * Union level
  , UnionLevel (..)
    -- * Flushes and scheduled merges
  , updatesWithInterleavedFlushes
  , flushWriteBuffer
    -- * Exported for cabal-docspec
  , maxRunSize
    -- * Credits
  , MergeDebt (..)
  , MergeCredits (..)
  , supplyCredits
  , creditThresholdForLevel
  , NominalDebt (..)
  , NominalCredits (..)
    -- * Exported for testing
  , addWriteBufferEntries
  ) where

import           Control.ActionRegistry
import           Control.Concurrent.Class.MonadMVar.Strict
import           Control.DeepSeq (NFData (..))
import           Control.Monad.Class.MonadST (MonadST)
import           Control.Monad.Class.MonadSTM (MonadSTM (..))
import           Control.Monad.Class.MonadThrow (MonadMask, MonadThrow (..))
import           Control.Monad.Primitive
import           Control.RefCount
import           Control.Tracer
import           Data.BloomFilter (Bloom)
import           Data.Foldable (fold)
import           Data.Primitive (Prim)
import           Data.Primitive.PrimVar
import qualified Data.Vector as V
import           Database.LSMTree.Internal.Assertions (assert)
import           Database.LSMTree.Internal.Config
import           Database.LSMTree.Internal.Entry (Entry, NumEntries (..),
                     unNumEntries)
import           Database.LSMTree.Internal.Index (Index)
import           Database.LSMTree.Internal.Lookup (ResolveSerialisedValue)
import           Database.LSMTree.Internal.MergingRun (MergeCredits (..),
                     MergeDebt (..), MergingRun, NumRuns (..))
import qualified Database.LSMTree.Internal.MergingRun as MR
import           Database.LSMTree.Internal.MergingTree (MergingTree)
import           Database.LSMTree.Internal.Paths (ActiveDir, RunFsPaths (..),
                     SessionRoot (..))
import qualified Database.LSMTree.Internal.Paths as Paths
import           Database.LSMTree.Internal.Run (Run, RunDataCaching (..))
import qualified Database.LSMTree.Internal.Run as Run
import           Database.LSMTree.Internal.RunAcc (RunBloomFilterAlloc (..))
import           Database.LSMTree.Internal.RunNumber
import           Database.LSMTree.Internal.Serialise (SerialisedBlob,
                     SerialisedKey, SerialisedValue)
import           Database.LSMTree.Internal.UniqCounter
import           Database.LSMTree.Internal.Vector (forMStrict, mapStrict)
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
      MR.LevelMergeType
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

-- | The levels of the table, from most to least recently inserted.
--
-- Concurrency: read-only operations are allowed to be concurrent with each
-- other, but update operations must not be concurrent with each other or read
-- operations. For example, inspecting the levels cache can be done
-- concurrently, but 'updatesWithInterleavedFlushes' must be serialised.
--
data TableContent m h = TableContent {
    -- | The in-memory level 0 of the table
    --
    -- TODO: probably less allocation to make this a MutVar
    tableWriteBuffer      :: !WriteBuffer
    -- | The blob storage for entries in the write buffer
  , tableWriteBufferBlobs :: !(Ref (WriteBufferBlobs m h))
    -- | A hierarchy of \"regular\" on-disk levels numbered 1 and up. Note that
    -- vector index @n@ refers to level @n+1@.
  , tableLevels           :: !(Levels m h)
    -- | Cache of flattened regular 'levels'.
  , tableCache            :: !(LevelsCache m h)
    -- | An optional final union level, not included in the table cache.
  , tableUnionLevel       :: !(UnionLevel m h)
  }

{-# SPECIALISE duplicateTableContent :: ActionRegistry IO -> TableContent IO h -> IO (TableContent IO h) #-}
duplicateTableContent ::
     (PrimMonad m, MonadMask m)
  => ActionRegistry m
  -> TableContent m h
  -> m (TableContent m h)
duplicateTableContent reg (TableContent wb wbb levels cache ul) = do
    wbb'    <- withRollback reg (dupRef wbb) releaseRef
    levels' <- duplicateLevels reg levels
    cache'  <- duplicateLevelsCache reg cache
    ul'     <- duplicateUnionLevel reg ul
    return $! TableContent wb wbb' levels' cache' ul'

{-# SPECIALISE releaseTableContent :: ActionRegistry IO -> TableContent IO h -> IO () #-}
releaseTableContent ::
     (PrimMonad m, MonadMask m)
  => ActionRegistry m
  -> TableContent m h
  -> m ()
releaseTableContent reg (TableContent _wb wbb levels cache ul) = do
    delayedCommit reg (releaseRef wbb)
    releaseLevels reg levels
    releaseLevelsCache reg cache
    releaseUnionLevel reg ul

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
  , cachedIndexes   :: !(V.Vector Index)
  , cachedKOpsFiles :: !(V.Vector (FS.Handle h))
  }

{-# SPECIALISE mkLevelsCache ::
     ActionRegistry IO
  -> Levels IO h
  -> IO (LevelsCache IO h) #-}
-- | Flatten the argument 'Level's into a single vector of runs, including all
-- runs that are inputs to an ongoing merge. Use that to populate the
-- 'LevelsCache'. The cache will take a reference for each of its runs.
mkLevelsCache ::
     forall m h. (PrimMonad m, MonadMVar m, MonadMask m)
  => ActionRegistry m
  -> Levels m h
  -> m (LevelsCache m h)
mkLevelsCache reg lvls = do
    rs <- foldRunAndMergeM
      (fmap V.singleton . dupRun)
      (\mr -> withRollback reg (MR.duplicateRuns mr) (V.mapM_ releaseRef))
      lvls
    pure $! LevelsCache_ {
        cachedRuns      = rs
      , cachedFilters   = mapStrict (\(DeRef r) -> Run.runFilter   r) rs
      , cachedIndexes   = mapStrict (\(DeRef r) -> Run.runIndex    r) rs
      , cachedKOpsFiles = mapStrict (\(DeRef r) -> Run.runKOpsFile r) rs
      }
  where
    dupRun r = withRollback reg (dupRef r) releaseRef

    -- TODO: this is not terribly performant, but it is also not sure if we are
    -- going to need this in the end. We might get rid of the LevelsCache.
    foldRunAndMergeM ::
         Monoid a
      => (Ref (Run m h) -> m a)
      -> (Ref (MergingRun MR.LevelMergeType m h) -> m a)
      -> Levels m h
      -> m a
    foldRunAndMergeM k1 k2 ls =
        fmap fold $ forMStrict ls $ \(Level ir rs) -> do
          incoming <- case ir of
            Single         r -> k1 r
            Merging _ _ _ mr -> k2 mr
          (incoming <>) . fold <$> V.forM rs k1

{-# SPECIALISE rebuildCache ::
     ActionRegistry IO
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
  => ActionRegistry m
  -> LevelsCache m h -- ^ old cache
  -> Levels m h -- ^ new levels
  -> m (LevelsCache m h) -- ^ new cache
rebuildCache reg oldCache newLevels = do
    releaseLevelsCache reg oldCache
    mkLevelsCache reg newLevels

{-# SPECIALISE duplicateLevelsCache ::
     ActionRegistry IO
  -> LevelsCache IO h
  -> IO (LevelsCache IO h) #-}
duplicateLevelsCache ::
     (PrimMonad m, MonadMask m)
  => ActionRegistry m
  -> LevelsCache m h
  -> m (LevelsCache m h)
duplicateLevelsCache reg cache = do
    rs' <- forMStrict (cachedRuns cache) $ \r ->
             withRollback reg (dupRef r) releaseRef
    return cache { cachedRuns = rs' }

{-# SPECIALISE releaseLevelsCache ::
     ActionRegistry IO
  -> LevelsCache IO h
  -> IO () #-}
releaseLevelsCache ::
     (PrimMonad m, MonadMask m)
  => ActionRegistry m
  -> LevelsCache m h
  -> m ()
releaseLevelsCache reg cache =
    V.forM_ (cachedRuns cache) $ \r ->
      delayedCommit reg (releaseRef r)

{-------------------------------------------------------------------------------
  Levels
-------------------------------------------------------------------------------}

type Levels m h = V.Vector (Level m h)

-- | A level is a sequence of resident runs at this level, prefixed by an
-- incoming run, which is usually multiple runs that are being merged. Once
-- completed, the resulting run will become a resident run at this level.
data Level m h = Level {
    incomingRun  :: !(IncomingRun m h)
  , residentRuns :: !(V.Vector (Ref (Run m h)))
  }

{-# SPECIALISE duplicateLevels :: ActionRegistry IO -> Levels IO h -> IO (Levels IO h) #-}
duplicateLevels ::
     (PrimMonad m, MonadMask m)
  => ActionRegistry m
  -> Levels m h
  -> m (Levels m h)
duplicateLevels reg levels =
    forMStrict levels $ \Level {incomingRun, residentRuns} -> do
      incomingRun'  <- duplicateIncomingRun reg incomingRun
      residentRuns' <- forMStrict residentRuns $ \r ->
                         withRollback reg (dupRef r) releaseRef
      return $! Level {
        incomingRun  = incomingRun',
        residentRuns = residentRuns'
      }

{-# SPECIALISE releaseLevels :: ActionRegistry IO -> Levels IO h -> IO () #-}
releaseLevels ::
     (PrimMonad m, MonadMask m)
  => ActionRegistry m
  -> Levels m h
  -> m ()
releaseLevels reg levels =
    V.forM_ levels $ \Level {incomingRun, residentRuns} -> do
      releaseIncomingRun reg incomingRun
      V.mapM_ (delayedCommit reg . releaseRef) residentRuns

{-# SPECIALISE iforLevelM_ :: Levels IO h -> (LevelNo -> Level IO h -> IO ()) -> IO () #-}
iforLevelM_ :: Monad m => Levels m h -> (LevelNo -> Level m h -> m ()) -> m ()
iforLevelM_ lvls k = V.iforM_ lvls $ \i lvl -> k (LevelNo (i + 1)) lvl

{-------------------------------------------------------------------------------
  Incoming runs
-------------------------------------------------------------------------------}

-- | An incoming run is either a single run, or a merge.
data IncomingRun m h =
       Single  !(Ref (Run m h))
     | Merging !MergePolicyForLevel
               !NominalDebt
               !(PrimVar (PrimState m) NominalCredits)
               !(Ref (MergingRun MR.LevelMergeType m h))

data MergePolicyForLevel = LevelTiering | LevelLevelling
  deriving stock (Show, Eq)

instance NFData MergePolicyForLevel where
  rnf LevelTiering   = ()
  rnf LevelLevelling = ()

-- | Total merge debt to complete the merge in an incoming run.
--
-- This corresponds to the number (worst case, minimum number) of update
-- operatons inserted into the table, before we will expect the merge to
-- complete.
newtype NominalDebt = NominalDebt Int
  deriving stock Eq

-- | Merge credits that get supplied to a table's levels.
--
-- This corresponds to the number of update operatons inserted into the table.
newtype NominalCredits = NominalCredits Int
  deriving stock Eq
  deriving newtype (Prim, NFData)

nominalDebtAsCredits :: NominalDebt -> NominalCredits
nominalDebtAsCredits (NominalDebt c) = NominalCredits c

{-# SPECIALISE duplicateIncomingRun :: ActionRegistry IO -> IncomingRun IO h -> IO (IncomingRun IO h) #-}
duplicateIncomingRun ::
     (PrimMonad m, MonadMask m)
  => ActionRegistry m
  -> IncomingRun m h
  -> m (IncomingRun m h)
duplicateIncomingRun reg (Single r) =
    Single <$> withRollback reg (dupRef r) releaseRef

duplicateIncomingRun reg (Merging mp md mcv mr) =
    Merging mp md <$> (newPrimVar =<< readPrimVar mcv)
                  <*> withRollback reg (dupRef mr) releaseRef

{-# SPECIALISE releaseIncomingRun :: ActionRegistry IO -> IncomingRun IO h -> IO () #-}
releaseIncomingRun ::
     (PrimMonad m, MonadMask m)
  => ActionRegistry m
  -> IncomingRun m h -> m ()
releaseIncomingRun reg (Single         r) = delayedCommit reg (releaseRef r)
releaseIncomingRun reg (Merging _ _ _ mr) = delayedCommit reg (releaseRef mr)

{-# SPECIALISE newIncomingSingleRun ::
     Tracer IO (AtLevel MergeTrace)
  -> LevelNo
  -> Ref (Run IO h)
  -> IO (IncomingRun IO h) #-}
newIncomingSingleRun ::
     Monad m
  => Tracer m (AtLevel MergeTrace)
  -> LevelNo
  -> Ref (Run m h)
  -> m (IncomingRun m h)
newIncomingSingleRun tr ln r = do
    traceWith tr $ AtLevel ln $
      TraceNewMergeSingleRun (Run.size r) (Run.runFsPathsNumber r)
    return (Single r)

{-# SPECIALISE newIncomingCompletedMergingRun ::
     Tracer IO (AtLevel MergeTrace)
  -> TableConfig
  -> ActionRegistry IO
  -> LevelNo
  -> MergePolicyForLevel
  -> NumRuns
  -> MergeDebt
  -> Ref (Run IO h)
  -> IO (IncomingRun IO h) #-}
newIncomingCompletedMergingRun ::
    (MonadMask m, MonadMVar m, MonadSTM m, MonadST m)
  => Tracer m (AtLevel MergeTrace)
  -> TableConfig
  -> ActionRegistry m
  -> LevelNo
  -> MergePolicyForLevel
  -> NumRuns
  -> MergeDebt
  -> Ref (Run m h)
  -> m (IncomingRun m h)
newIncomingCompletedMergingRun tr conf reg ln mergePolicy nr mergeDebt r = do
    traceWith tr $ AtLevel ln $
      TraceNewMergeSingleRun (Run.size r) (Run.runFsPathsNumber r)
    mr <- withRollback reg (MR.newCompleted nr mergeDebt r) releaseRef
    let nominalDebt    = nominalDebtForLevel conf ln
        nominalCredits = nominalDebtAsCredits nominalDebt
    nominalCreditsVar <- newPrimVar nominalCredits
    return (Merging mergePolicy nominalDebt nominalCreditsVar mr)

{-# SPECIALISE newIncomingMergingRun ::
     Tracer IO (AtLevel MergeTrace)
  -> HasFS IO h
  -> HasBlockIO IO h
  -> ActiveDir
  -> UniqCounter IO
  -> TableConfig
  -> ResolveSerialisedValue
  -> ActionRegistry IO
  -> MergePolicyForLevel
  -> MR.LevelMergeType
  -> LevelNo
  -> V.Vector (Ref (Run IO h))
  -> IO (IncomingRun IO h) #-}
newIncomingMergingRun ::
     (MonadMask m, MonadMVar m, MonadSTM m, MonadST m)
  => Tracer m (AtLevel MergeTrace)
  -> HasFS m h
  -> HasBlockIO m h
  -> ActiveDir
  -> UniqCounter m
  -> TableConfig
  -> ResolveSerialisedValue
  -> ActionRegistry m
  -> MergePolicyForLevel
  -> MR.LevelMergeType
  -> LevelNo
  -> V.Vector (Ref (Run m h))
  -> m (IncomingRun m h)
newIncomingMergingRun tr hfs hbio activeDir uc
                      conf@TableConfig {
                             confDiskCachePolicy,
                             confFencePointerIndex
                           }
                      resolve reg
                      mergePolicy mergeType ln rs = do
    !rn <- uniqueToRunNumber <$> incrUniqCounter uc
    let !caching   = diskCachePolicyForLevel confDiskCachePolicy ln
        !alloc     = bloomFilterAllocForLevel conf ln
        !indexType = indexTypeForRun confFencePointerIndex
        !runPaths  = Paths.runPath activeDir rn
    traceWith tr $ AtLevel ln $
      TraceNewMerge (V.map Run.size rs) (runNumber runPaths)
                    caching alloc mergePolicy mergeType
    mr <- withRollback reg
            (MR.new hfs hbio resolve caching
                    alloc indexType mergeType
                    runPaths rs)
            releaseRef
    let nominalDebt    = nominalDebtForLevel conf ln
        nominalCredits = NominalCredits 0
    nominalCreditsVar <- newPrimVar nominalCredits
    assert (MR.totalMergeDebt mr <= maxMergeDebt conf mergePolicy ln) $
      return (Merging mergePolicy nominalDebt nominalCreditsVar mr)

{-# SPECIALISE supplyCreditsIncomingRun ::
     TableConfig
  -> LevelNo
  -> IncomingRun IO h
  -> NominalCredits
  -> IO () #-}
-- | Supply a given number of nominal credits to the merge in an incoming run.
-- This is a relative addition of credits, not a new absolute total value.
supplyCreditsIncomingRun ::
     (MonadSTM m, MonadST m, MonadMVar m, MonadMask m)
  => TableConfig
  -> LevelNo
  -> IncomingRun m h
  -> NominalCredits
  -> m ()
supplyCreditsIncomingRun _ _ (Single _r) _ = return ()
supplyCreditsIncomingRun conf ln (Merging _ nominalDebt nominalCreditsVar mr)
                         deposit = do
    (_nominalCredits,
     nominalCredits') <- depositNominalCredits nominalDebt nominalCreditsVar
                                               deposit
    let !mergeDebt     = MR.totalMergeDebt mr
        !mergeCredits' = scaleNominalToMergeCredit nominalDebt mergeDebt
                                                   nominalCredits'
        !thresh = creditThresholdForLevel conf ln
    (_suppliedCredits,
     _suppliedCredits') <- MR.supplyCreditsAbsolute mr thresh mergeCredits'
    --TODO: consider tracing supply of credits
    return ()

-- | Deposit nominal credits in the local credits var, ensuring the total
-- credits does not exceed the total debt.
--
-- Depositing /could/ leave the credit higher than the total debt. It is not
-- avoided by construction. The scenario is this: when a completed merge is
-- underfull, we combine it with the incoming run, so it means we have one run
-- fewer on the level then we'd normally have. This means that the level
-- becomes full at a later time, so more time passes before we call
-- 'MR.expectCompleted' on any levels further down the tree. This means we keep
-- supplying nominal credits to levels further down past the point their
-- nominal debt is paid off. So the solution here is just to drop any nominal
-- credits that are in excess of the nominal debt.
--
-- This is /not/ itself thread safe. All 'TableContent' update operations are
-- expected to be serialised by the caller. See concurrency comments for
-- 'TableContent' for detail.
depositNominalCredits ::
     PrimMonad m
  => NominalDebt
  -> PrimVar (PrimState m) NominalCredits
  -> NominalCredits
  -> m (NominalCredits, NominalCredits)
depositNominalCredits (NominalDebt nominalDebt)
                      nominalCreditsVar
                      (NominalCredits deposit) = do
    NominalCredits before <- readPrimVar nominalCreditsVar
    let !after = NominalCredits (min (before + deposit) nominalDebt)
    writePrimVar nominalCreditsVar after
    return (NominalCredits before, after)

scaleNominalToMergeCredit ::
     NominalDebt
  -> MergeDebt
  -> NominalCredits
  -> MergeCredits
scaleNominalToMergeCredit (NominalDebt             nominalDebt)
                          (MergeDebt (MergeCredits mergeDebt))
                          (NominalCredits          nominalCredits) =
    -- The specification is:
    let mergeCredits_spec = floor $ toRational nominalCredits
                                  * toRational mergeDebt
                                  / toRational nominalDebt
     in assert (nominalDebt > 0) $
        MergeCredits mergeCredits_spec

{-# SPECIALISE immediatelyCompleteIncomingRun ::
     Tracer IO (AtLevel MergeTrace)
  -> TableConfig
  -> LevelNo
  -> IncomingRun IO h
  -> IO () #-}
-- | Supply enough credits to complete the merge now.
immediatelyCompleteIncomingRun ::
     (MonadSTM m, MonadST m, MonadMVar m, MonadMask m)
  => Tracer m (AtLevel MergeTrace)
  -> TableConfig
  -> LevelNo
  -> IncomingRun m h
  -> m ()
immediatelyCompleteIncomingRun tr conf ln ir =
    case ir of
      Single{} -> return ()
      Merging _ (NominalDebt nominalDebt) nominalCreditsVar mr -> do

        NominalCredits nominalCredits <- readPrimVar nominalCreditsVar
        let !deposit = NominalCredits (nominalDebt - nominalCredits)
        supplyCreditsIncomingRun conf ln ir deposit

        -- This ensures the merge is really completed. However, we don't
        -- release the merge yet and only briefly inspect the resulting run.
        bracket (MR.expectCompleted mr) releaseRef $ \r ->
          traceWith tr $ AtLevel ln $
            TraceCompletedMerge (Run.size r) (Run.runFsPathsNumber r)

{-# SPECIALISE snapshotIncomingRun ::
     IncomingRun IO h
  -> IO (Either (Ref (Run IO h))
                (MergePolicyForLevel,
                 NumRuns,
                 NominalDebt,
                 NominalCredits,
                 MergeDebt,
                 MergeCredits,
                 MR.MergingRunState MR.LevelMergeType IO h)) #-}
snapshotIncomingRun ::
     (PrimMonad m, MonadMVar m)
  => IncomingRun m h
  -> m (Either (Ref (Run m h))
               (MergePolicyForLevel,
                NumRuns,
                NominalDebt,
                NominalCredits,
                MergeDebt,
                MergeCredits,
                MR.MergingRunState MR.LevelMergeType m h))
snapshotIncomingRun (Single r) = pure (Left r)
snapshotIncomingRun (Merging mergePolicy nominalDebt nominalCreditsVar mr) = do
    (numRuns, mergeDebt, mergeCredit, state) <- MR.snapshot mr
    nominalCredits <- readPrimVar nominalCreditsVar
    pure (Right (mergePolicy, numRuns,
                 nominalDebt, nominalCredits,
                 mergeDebt, mergeCredit,
                 state))

{-------------------------------------------------------------------------------
  Union level
-------------------------------------------------------------------------------}

-- | An additional optional last level, created as a result of
-- 'Database.LSMTree.Monoidal.union'. It can not only contain an ongoing merge
-- of multiple runs, but a nested tree of merges.
--
-- TODO: So far, this is
-- * never created
-- * not stored in snapshots
-- * not loaded from snapshots
-- * ignored in lookups
-- * never made merge progress on (by supplying credits to it)
-- * never merged into the regular levels
data UnionLevel m h =
    NoUnion
  | Union !(Ref (MergingTree m h))

{-# SPECIALISE duplicateUnionLevel ::
     ActionRegistry IO
  -> UnionLevel IO h
  -> IO (UnionLevel IO h) #-}
duplicateUnionLevel ::
     (PrimMonad m, MonadMask m)
  => ActionRegistry m
  -> UnionLevel m h
  -> m (UnionLevel m h)
duplicateUnionLevel reg ul =
    case ul of
      NoUnion    -> return ul
      Union tree -> Union <$> withRollback reg (dupRef tree) releaseRef

{-# SPECIALISE releaseUnionLevel ::
     ActionRegistry IO
  -> UnionLevel IO h
  -> IO () #-}
releaseUnionLevel ::
     (PrimMonad m, MonadMask m)
  => ActionRegistry m
  -> UnionLevel m h
  -> m ()
releaseUnionLevel _   NoUnion      = return ()
releaseUnionLevel reg (Union tree) = delayedCommit reg (releaseRef tree)

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
  -> ActionRegistry IO
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
  -> ActionRegistry m
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
    supplyCredits conf (NominalCredits numAdded) (tableLevels tc)
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
  -> ActionRegistry IO
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
  -> ActionRegistry m
  -> TableContent m h
  -> m (TableContent m h)
flushWriteBuffer tr conf@TableConfig{confFencePointerIndex, confDiskCachePolicy}
                 resolve hfs hbio root uc reg tc
  | WB.null (tableWriteBuffer tc) = pure tc
  | otherwise = do
    !n <- incrUniqCounter uc
    let !size      = WB.numEntries (tableWriteBuffer tc)
        !ln        = LevelNo 1
        !cache     = diskCachePolicyForLevel confDiskCachePolicy ln
        !alloc     = bloomFilterAllocForLevel conf ln
        !indexType = indexTypeForRun confFencePointerIndex
        !path      = Paths.runPath (Paths.activeDir root) (uniqueToRunNumber n)
    traceWith tr $ AtLevel ln $
      TraceFlushWriteBuffer size (runNumber path) cache alloc
    r <- withRollback reg
            (Run.fromWriteBuffer hfs hbio
              cache
              alloc
              indexType
              path
              (tableWriteBuffer tc)
              (tableWriteBufferBlobs tc))
            releaseRef
    delayedCommit reg (releaseRef (tableWriteBufferBlobs tc))
    wbblobs' <- withRollback reg (WBB.new hfs (Paths.tableBlobPath root n))
                                 releaseRef
    levels' <- addRunToLevels tr conf resolve hfs hbio root uc r reg
                 (tableLevels tc)
                 (tableUnionLevel tc)
    tableCache' <- rebuildCache reg (tableCache tc) levels'
    pure $! TableContent {
        tableWriteBuffer = WB.empty
      , tableWriteBufferBlobs = wbblobs'
      , tableLevels = levels'
      , tableCache = tableCache'
        -- TODO: move into regular levels if merge completed and size fits
      , tableUnionLevel = tableUnionLevel tc
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
  -> ActionRegistry IO
  -> Levels IO h
  -> UnionLevel IO h
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
  -> ActionRegistry m
  -> Levels m h
  -> UnionLevel m h
  -> m (Levels m h)
addRunToLevels tr conf@TableConfig{..} resolve hfs hbio root uc r0 reg levels ul = do
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
        let policyForLevel = mergePolicyForLevel confMergePolicy ln V.empty ul
        ir <- newMerge policyForLevel MR.MergeLastLevel ln rs
        return $! V.singleton $ Level ir V.empty
    go !ln rs' (V.uncons -> Just (Level ir rs, ls)) = do
        r <- expectCompletedMerge ln ir
        case mergePolicyForLevel confMergePolicy ln ls ul of
          -- If r is still too small for this level then keep it and merge again
          -- with the incoming runs.
          LevelTiering | Run.size r <= maxRunSize' conf LevelTiering (pred ln) -> do
            let mergeType = mergeTypeForLevel ls ul
            ir' <- newMerge LevelTiering mergeType ln (rs' `V.snoc` r)
            pure $! Level ir' rs `V.cons` ls
          -- This tiering level is now full. We take the completed merged run
          -- (the previous incoming runs), plus all the other runs on this level
          -- as a bundle and move them down to the level below. We start a merge
          -- for the new incoming runs. This level is otherwise empty.
          LevelTiering | levelIsFull confSizeRatio rs -> do
            ir' <- newMerge LevelTiering MR.MergeMidLevel ln rs'
            ls' <- go (succ ln) (r `V.cons` rs) ls
            pure $! Level ir' V.empty `V.cons` ls'
          -- This tiering level is not yet full. We move the completed merged run
          -- into the level proper, and start the new merge for the incoming runs.
          LevelTiering -> do
            let mergeType = mergeTypeForLevel ls ul
            ir' <- newMerge LevelTiering mergeType ln rs'
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
            ir' <- newMerge LevelTiering MR.MergeMidLevel ln rs'
            ls' <- go (succ ln) (V.singleton r) V.empty
            pure $! Level ir' V.empty `V.cons` ls'
          -- Otherwise we start merging the incoming runs into the run.
          LevelLevelling -> do
            assert (V.null rs && V.null ls) $ pure ()
            ir' <- newMerge LevelLevelling MR.MergeLastLevel ln (rs' `V.snoc` r)
            pure $! Level ir' V.empty `V.cons` V.empty

    -- Releases the incoming run.
    expectCompletedMerge :: LevelNo -> IncomingRun m h -> m (Ref (Run m h))
    expectCompletedMerge ln ir = do
      r <- case ir of
        Single         r -> pure r
        Merging _ _ _ mr -> do
          r <- withRollback reg (MR.expectCompleted mr) releaseRef
          delayedCommit reg (releaseRef mr)
          pure r
      traceWith tr $ AtLevel ln $
        TraceExpectCompletedMerge (Run.runFsPathsNumber r)
      pure r

    -- Releases the runs.
    newMerge :: MergePolicyForLevel
             -> MR.LevelMergeType
             -> LevelNo
             -> V.Vector (Ref (Run m h))
             -> m (IncomingRun m h)
    newMerge mergePolicy mergeType ln rs
      | Just (r, rest) <- V.uncons rs
      , V.null rest = do
          -- We create a fresh reference and release the original one.
          -- This will also make it easier to trace back where it was allocated.
          r' <- withRollback reg (dupRef r) releaseRef
          ir <- newIncomingSingleRun tr ln r'
          delayedCommit reg (releaseRef r)
          pure ir

      | otherwise = assert (let l = V.length rs in l >= 2 && l <= 5) $ do
          ir <- newIncomingMergingRun tr hfs hbio (Paths.activeDir root) uc
                                      conf resolve reg
                                      mergePolicy mergeType ln rs
          -- The runs will end up inside the merging run, with fresh references.
          -- The original references can be released (but only on the happy path).
          V.forM_ rs $ \r -> delayedCommit reg (releaseRef r)
          case confMergeSchedule of
            Incremental -> pure ()
            OneShot     -> immediatelyCompleteIncomingRun tr conf ln ir
          return ir

-- | We use levelling on the last level, unless that is also the first level.
mergePolicyForLevel ::
     MergePolicy
  -> LevelNo
  -> Levels m h
  -> UnionLevel m h
  -> MergePolicyForLevel
mergePolicyForLevel MergePolicyLazyLevelling (LevelNo n) nextLevels unionLevel
  | n == 1
  = LevelTiering    -- always use tiering on first level

  | V.null nextLevels
  , NoUnion <- unionLevel
  = LevelLevelling  -- levelling on last level

  | otherwise
  = LevelTiering

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
maxRunSize _ _ _ (LevelNo ln)
  | ln < 0  = error "maxRunSize: non-positive level number"
  | ln == 0 = NumEntries 0

maxRunSize sizeRatio (AllocNumEntries bufferSize) LevelTiering ln =
    NumEntries $ maxRunSizeTiering (sizeRatioInt sizeRatio) bufferSize ln

maxRunSize sizeRatio (AllocNumEntries bufferSize) LevelLevelling ln =
    NumEntries $ maxRunSizeLevelling (sizeRatioInt sizeRatio) bufferSize ln

maxRunSizeTiering, maxRunSizeLevelling :: Int -> NumEntries -> LevelNo -> Int
maxRunSizeTiering sizeRatio (NumEntries bufferSize) (LevelNo ln) =
    bufferSize * sizeRatio ^ pred ln

maxRunSizeLevelling sizeRatio bufferSize ln =
    maxRunSizeTiering sizeRatio bufferSize (succ ln)

maxRunSize' :: TableConfig -> MergePolicyForLevel -> LevelNo -> NumEntries
maxRunSize' config policy ln =
    maxRunSize (confSizeRatio config) (confWriteBufferAlloc config) policy ln

-- | If there are no further levels provided, this level is the last one.
-- However, if a 'Union' is present, it acts as another (last) level.
mergeTypeForLevel :: Levels m h -> UnionLevel m h -> MR.LevelMergeType
mergeTypeForLevel levels unionLevel
  | V.null levels, NoUnion <- unionLevel = MR.MergeLastLevel
  | otherwise                            = MR.MergeMidLevel

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

{-# SPECIALISE supplyCredits ::
     TableConfig
  -> NominalCredits
  -> Levels IO h
  -> IO ()
  #-}
-- | Supply the given amount of credits to each merge in the levels structure.
-- This /may/ cause some merges to progress.
supplyCredits ::
     (MonadSTM m, MonadST m, MonadMVar m, MonadMask m)
  => TableConfig
  -> NominalCredits
  -> Levels m h
  -> m ()
supplyCredits conf deposit levels =
    iforLevelM_ levels $ \ln (Level ir _rs) ->
      supplyCreditsIncomingRun conf ln ir deposit

maxMergeDebt :: TableConfig -> MergePolicyForLevel -> LevelNo -> MergeDebt
maxMergeDebt TableConfig {
               confWriteBufferAlloc = AllocNumEntries bufferSize,
               confSizeRatio
             } mergePolicy ln =
    let !sizeRatio = sizeRatioInt confSizeRatio in
    case mergePolicy of
      LevelLevelling ->
        MergeDebt . MergeCredits $
          sizeRatio * maxRunSizeTiering sizeRatio bufferSize (pred ln)
                    + maxRunSizeLevelling sizeRatio bufferSize ln

      LevelTiering   ->
        MergeDebt . MergeCredits $
          maxRuns * maxRunSizeTiering sizeRatio bufferSize (pred ln)
        where
          -- We can hold back underfull runs, so sometimes the are n+1 runs,
          -- rather than the typical n at a tiering level (n = LSM size ratio).
          maxRuns = sizeRatio + 1

-- | The nominal debt equals the minimum of credits we will supply before we
-- expect the merge to complete. This is the same as the number of updates
-- in a run that gets moved to this level.
nominalDebtForLevel :: TableConfig -> LevelNo -> NominalDebt
nominalDebtForLevel TableConfig {
                      confWriteBufferAlloc = AllocNumEntries !bufferSize,
                      confSizeRatio
                    } ln =
    NominalDebt (maxRunSizeTiering (sizeRatioInt confSizeRatio) bufferSize ln)

-- TODO: the thresholds for doing merge work should be different for each level,
-- maybe co-prime?
creditThresholdForLevel :: TableConfig -> LevelNo -> MR.CreditThreshold
creditThresholdForLevel conf (LevelNo _i) =
    let AllocNumEntries (NumEntries x) = confWriteBufferAlloc conf
    in  MR.CreditThreshold (MR.UnspentCredits (MergeCredits x))
