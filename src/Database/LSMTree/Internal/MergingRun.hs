{-# LANGUAGE TypeFamilies #-}

{- HLINT ignore "Use when" -}

-- | An incremental merge of multiple runs.
module Database.LSMTree.Internal.MergingRun (
    MergingRun (..)
  , new
  , newCompleted
  , duplicateRuns
  , supplyCredits
  , expectCompleted
    -- * Useful types
  , Credits (..)
  , CreditThreshold (..)
  , NumRuns (..)
  , MergePolicyForLevel (..)
    -- * Internal state
  , UnspentCreditsVar (..)
  , TotalStepsVar (..)
  , MergingRunState (..)
  , SpentCreditsVar (..)
  , MergeKnownCompleted (..)
  ) where

import           Control.Concurrent.Class.MonadMVar.Strict
import           Control.DeepSeq (NFData (..))
import           Control.Monad (void, when)
import           Control.Monad.Class.MonadST (MonadST)
import           Control.Monad.Class.MonadSTM (MonadSTM (..))
import           Control.Monad.Class.MonadThrow (MonadCatch (bracketOnError),
                     MonadMask)
import           Control.Monad.Primitive
import           Control.RefCount
import           Control.TempRegistry
import           Data.Maybe (fromMaybe)
import           Data.Primitive.MutVar
import           Data.Primitive.PrimVar
import qualified Data.Vector as V
import           Database.LSMTree.Internal.Assertions (assert)
import           Database.LSMTree.Internal.Entry (NumEntries (..), unNumEntries)
import           Database.LSMTree.Internal.Lookup (ResolveSerialisedValue)
import           Database.LSMTree.Internal.Merge (Merge, StepResult (..))
import qualified Database.LSMTree.Internal.Merge as Merge
import           Database.LSMTree.Internal.Paths (RunFsPaths (..))
import           Database.LSMTree.Internal.Run (Run)
import qualified Database.LSMTree.Internal.Run as Run
import           Database.LSMTree.Internal.RunAcc (RunBloomFilterAlloc)
import           System.FS.API (HasFS)
import           System.FS.BlockIO.API (HasBlockIO)

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
      -- completed. If 'MergeKnownCompleted', then we are sure the merge has
      -- been completed, otherwise if 'MergeMaybeCompleted' we have to check the
      -- 'MergingRunState'.
    , mergeKnownCompleted :: !(MutVar (PrimState m) MergeKnownCompleted)
    , mergeState          :: !(StrictMVar m (MergingRunState m h))
    , mergeRefCounter     :: !(RefCounter m)
    }

instance RefCounted m (MergingRun m h) where
    getRefCounter = mergeRefCounter

data MergePolicyForLevel = LevelTiering | LevelLevelling
  deriving stock (Show, Eq)

instance NFData MergePolicyForLevel where
  rnf LevelTiering   = ()
  rnf LevelLevelling = ()

newtype NumRuns = NumRuns { unNumRuns :: Int }
  deriving stock (Show, Eq)
  deriving newtype NFData

newtype UnspentCreditsVar s = UnspentCreditsVar {
    getUnspentCreditsVar :: PrimVar s Int
  }

newtype TotalStepsVar s = TotalStepsVar {
    getTotalStepsVar ::  PrimVar s Int
  }

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

newtype SpentCreditsVar s = SpentCreditsVar {
    getSpentCreditsVar :: PrimVar s Int
  }

data MergeKnownCompleted = MergeKnownCompleted | MergeMaybeCompleted
  deriving stock (Show, Eq, Read)

instance NFData MergeKnownCompleted where
  rnf MergeKnownCompleted = ()
  rnf MergeMaybeCompleted = ()

{-# SPECIALISE new ::
     HasFS IO h
  -> HasBlockIO IO h
  -> ResolveSerialisedValue
  -> Run.RunDataCaching
  -> RunBloomFilterAlloc
  -> Merge.Level
  -> MergePolicyForLevel
  -> RunFsPaths
  -> V.Vector (Ref (Run IO h))
  -> IO (Ref (MergingRun IO h)) #-}
-- | Create a new merging run, returning a reference to it that must ultimately
-- be released via 'releaseRef'.
--
-- Duplicates the supplied references to the runs.
--
-- This function should be run with asynchronous exceptions masked to prevent
-- failing after internal resources have already been created.
new ::
     (MonadMVar m, MonadMask m, MonadSTM m, MonadST m)
  => HasFS m h
  -> HasBlockIO m h
  -> ResolveSerialisedValue
  -> Run.RunDataCaching
  -> RunBloomFilterAlloc
  -> Merge.Level
  -> MergePolicyForLevel
  -> RunFsPaths
  -> V.Vector (Ref (Run m h))
  -> m (Ref (MergingRun m h))
new hfs hbio resolve caching alloc mergeLevel mergePolicy runPaths inputRuns =
    -- If creating the Merge fails, we must release the references again.
    withTempRegistry $ \reg -> do
      runs <- V.mapM (\r -> allocateTemp reg (dupRef r) releaseRef) inputRuns
      merge <- fromMaybe (error "newMerge: merges can not be empty")
        <$> Merge.new hfs hbio caching alloc mergeLevel resolve runPaths runs
      let numInputRuns = NumRuns $ V.length runs
      let numInputEntries = V.foldMap' Run.size runs
      spentCreditsVar <- SpentCreditsVar <$> newPrimVar 0
      unsafeNew mergePolicy numInputRuns numInputEntries MergeMaybeCompleted $
        OngoingMerge runs spentCreditsVar merge

{-# SPECIALISE newCompleted ::
     MergePolicyForLevel
  -> NumRuns
  -> NumEntries
  -> Ref (Run IO h)
  -> IO (Ref (MergingRun IO h)) #-}
-- | Create a merging run that is already in the completed state, returning a
-- reference that must ultimately be released via 'releaseRef'.
--
-- Duplicates the supplied reference to the run.
--
-- This function should be run with asynchronous exceptions masked to prevent
-- failing after internal resources have already been created.
newCompleted ::
     (MonadMVar m, MonadMask m, MonadSTM m, MonadST m)
  => MergePolicyForLevel
  -> NumRuns
  -> NumEntries
  -> Ref (Run m h)
  -> m (Ref (MergingRun m h))
newCompleted mergePolicy numInputRuns numInputEntries inputRun = do
    bracketOnError (dupRef inputRun) releaseRef $ \run ->
      unsafeNew mergePolicy numInputRuns numInputEntries MergeKnownCompleted $
        CompletedMerge run

{-# INLINE unsafeNew #-}
unsafeNew ::
     (MonadMVar m, MonadMask m, MonadSTM m, MonadST m)
  => MergePolicyForLevel
  -> NumRuns
  -> NumEntries
  -> MergeKnownCompleted
  -> MergingRunState m h
  -> m (Ref (MergingRun m h))
unsafeNew mergePolicy mergeNumRuns mergeNumEntries knownCompleted state = do
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
{-# SPECIALISE duplicateRuns :: Ref (MergingRun IO h) -> IO (V.Vector (Ref (Run IO h))) #-}
duplicateRuns ::
     (PrimMonad m, MonadMVar m, MonadMask m)
  => Ref (MergingRun m h)
  -> m (V.Vector (Ref (Run m h)))
duplicateRuns (DeRef mr) =
    -- We take the references while holding the MVar to make sure the MergingRun
    -- does not get completed concurrently before we are done.
    withMVar (mergeState mr) $ \case
      CompletedMerge r    -> V.singleton <$> dupRef r
      OngoingMerge rs _ _ -> withTempRegistry $ \reg ->
        V.mapM (\r -> allocateTemp reg (dupRef r) releaseRef) rs

{-------------------------------------------------------------------------------
  Credits
-------------------------------------------------------------------------------}

{-
  Note [Merge Batching]
~~~~~~~~~~~~~~

  Merge work is done in batches based on accumulated, unspent credits and a
  threshold value. Moreover, merging runs can be shared across tables, which
  means that multiple threads can contribute to the same merge concurrently.
  The design to contribute credits to the same merging run is largely lock-free.
  It ensures consistency of the unspent credits and the merge state, while
  allowing threads to progress without waiting on other threads.

  First, credits are added atomically to a PrimVar that holds the current total
  of unspent credits. If this addition exceeded the threshold, then credits are
  atomically subtracted from the PrimVar to get it below the threshold. The
  number of subtracted credits is then the number of merge steps that will be
  performed. While doing the merging work, a (more expensive) MVar lock is taken
  to ensure that the merging work itself is performed only sequentially. If at
  some point, doing the merge work resulted in the merge being done, then the
  merge is converted into a new run.

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
    adjustment is necessary. It does not make a difference for correctness, but
    it does mean we get a slightly better distribution of work over time. For
    sensible batch sizes and workloads without many duplicate keys, it probably
    won't make much of a difference. However, without this calculation the
    surplus can accumulate over time, so if we're really pedantic about work
    distribution then this is the way to go.

  Async exceptions are allowed to mess up the consistency between the the merge
  state, the merge steps performed variable, and the spent credits variable.
  There is an important invariant that we maintain, even in the presence of
  async exceptions: @merge steps actually performed >= recorded merge steps
  performed >= recorded spent credits@. TODO: and this makes it correct (?).
-}

newtype Credits = Credits Int

-- | Credits are accumulated until they go over the 'CreditThreshold', after
-- which a batch of merge work will be performed. Configuring this threshold
-- should allow to achieve a nice balance between spreading out I/O and
-- achieving good (concurrent) performance.
newtype CreditThreshold = CreditThreshold { getCreditThreshold :: Int }

{-# SPECIALISE supplyCredits ::
     Credits
  -> CreditThreshold
  -> Ref (MergingRun IO h)
  -> IO () #-}
-- | Supply the given amount of credits to a merging run. This /may/ cause an
-- ongoing merge to progress.
supplyCredits ::
     forall m h. (MonadSTM m, MonadST m, MonadMVar m, MonadMask m)
  => Credits
  -> CreditThreshold
  -> Ref (MergingRun m h)
  -> m ()
supplyCredits (Credits c) creditsThresh (DeRef MergingRun {..}) = do
    mergeCompleted <- readMutVar mergeKnownCompleted

    -- The merge is already finished
    if mergeCompleted == MergeKnownCompleted then
      pure ()
    else do
      -- unspentCredits' is our /estimate/ of what the new total of unspent
      -- credits is.
      Credits unspentCredits' <- addUnspentCredits mergeUnspentCredits (Credits c)
      totalSteps <- readPrimVar (getTotalStepsVar mergeStepsPerformed)

      if totalSteps + unspentCredits' >= unNumEntries mergeNumEntries then do
        -- We can finish the merge immediately
        isMergeDone <-
          bracketOnError (takeAllUnspentCredits mergeUnspentCredits)
                         (putBackUnspentCredits mergeUnspentCredits)
                         (stepMerge mergeState mergeStepsPerformed)
        when isMergeDone $ completeMerge mergeState mergeKnownCompleted
      else if unspentCredits' >= getCreditThreshold creditsThresh then do
        -- We can do some merging work without finishing the merge immediately
        isMergeDone <-
          -- Try to take some unspent credits. The number of taken credits is
          -- the number of merging steps we will try to do.
          --
          -- If an error happens during the body, then we put back as many
          -- credits as we took, even if the merge has progressed. See Note
          -- [Merge Batching] to see why this is okay.
          bracketOnError
            (tryTakeUnspentCredits mergeUnspentCredits creditsThresh (Credits unspentCredits'))
            (mapM_ (putBackUnspentCredits mergeUnspentCredits)) $ \case
              Nothing -> pure False
              Just c' -> stepMerge mergeState mergeStepsPerformed c'

        -- If we just finished the merge, then we convert the output of the
        -- merge into a new run. i.e., we complete the merge.
        --
        -- If an async exception happens before we get to perform the
        -- completion, then that is fine. The next supplyCredits will
        -- complete the merge.
        when isMergeDone $ completeMerge mergeState mergeKnownCompleted
      else
        -- Just accumulate credits, because we are not over the threshold yet
        pure ()

{-# SPECIALISE addUnspentCredits ::
     UnspentCreditsVar RealWorld
  -> Credits
  -> IO Credits #-}
-- | Add credits to unspent credits. Returns the /estimate/ of what the new
-- total of unspent credits is. The /actual/ total might have been changed again
-- by a different thread.
addUnspentCredits ::
     PrimMonad m
  => UnspentCreditsVar (PrimState m)
  -> Credits
  -> m Credits
addUnspentCredits (UnspentCreditsVar !var) (Credits c) =
    Credits . (c+) <$> fetchAddInt var c

{-# SPECIALISE tryTakeUnspentCredits ::
     UnspentCreditsVar RealWorld
  -> CreditThreshold
  -> Credits
  -> IO (Maybe Credits) #-}
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
  -> Credits
  -> m (Maybe Credits)
tryTakeUnspentCredits
    unspentCreditsVar@(UnspentCreditsVar !var)
    thresh@(CreditThreshold !creditsThresh)
    (Credits !before)
  | before < creditsThresh = pure Nothing
  | otherwise = do
      -- numThresholds is guaranteed to be >= 1
      let !numThresholds = before `div` creditsThresh
          !creditsToTake = numThresholds * creditsThresh
          !after = before - creditsToTake
      assert (after < creditsThresh) $ pure ()
      before' <- casInt var before after
      if before' == before then
        pure (Just (Credits creditsToTake))
      else
        tryTakeUnspentCredits unspentCreditsVar thresh (Credits before')

{-# SPECIALISE putBackUnspentCredits ::
     UnspentCreditsVar RealWorld
  -> Credits
  -> IO () #-}
putBackUnspentCredits ::
     PrimMonad m
  => UnspentCreditsVar (PrimState m)
  -> Credits
  -> m ()
putBackUnspentCredits (UnspentCreditsVar !var) (Credits !x) =
    void $ fetchAddInt var x

{-# SPECIALISE takeAllUnspentCredits ::
     UnspentCreditsVar RealWorld
  -> IO Credits #-}
-- | In a CAS-loop, subtract all unspent credits and return them.
takeAllUnspentCredits ::
     PrimMonad m
  => UnspentCreditsVar (PrimState m)
  -> m Credits
takeAllUnspentCredits (UnspentCreditsVar !unspentCreditsVar) = do
    prev <- readPrimVar unspentCreditsVar
    casLoop prev
  where
    casLoop !prev = do
      prev' <- casInt unspentCreditsVar prev 0
      if prev' == prev then
        pure (Credits prev)
      else
        casLoop prev'

{-# SPECIALISE stepMerge ::
     StrictMVar IO (MergingRunState IO h)
  -> TotalStepsVar RealWorld
  -> Credits
  -> IO Bool #-}
stepMerge ::
     (MonadMVar m, MonadMask m, MonadSTM m, MonadST m)
  => StrictMVar m (MergingRunState m h)
  -> TotalStepsVar (PrimState m)
  -> Credits
  -> m Bool
stepMerge mergeVar (TotalStepsVar totalStepsVar) (Credits c) =
    withMVar mergeVar $ \case
      CompletedMerge{} -> pure False
      (OngoingMerge _rs (SpentCreditsVar spentCreditsVar) m) -> do
        totalSteps <- readPrimVar totalStepsVar
        spentCredits <- readPrimVar spentCreditsVar

        -- If we previously performed too many merge steps, then we perform
        -- fewer now.
        let stepsToDo = max 0 (spentCredits + c - totalSteps)
        -- Merge.steps guarantees that @stepsDone >= stepsToDo@ /unless/ the
        -- merge was just now finished.
        (stepsDone, stepResult) <- Merge.steps m stepsToDo
        assert (case stepResult of
                  MergeInProgress -> stepsDone >= stepsToDo
                  MergeDone       -> True
                ) $ pure ()

        -- This should be the only point at which we write to these variables.
        --
        -- It is guaranteed that @totalSteps' >= spentCredits'@ /unless/ the
        -- merge was just now finished.
        let totalSteps' = totalSteps + stepsDone
        let spentCredits' = spentCredits + c
        -- It is guaranteed that
        -- @readPrimVar totalStepsVar >= readPrimVar spentCreditsVar@,
        -- /unless/ the merge was just now finished.
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

{-# SPECIALISE expectCompleted ::
     Ref (MergingRun IO h)
  -> IO (Ref (Run IO h)) #-}
-- | This does /not/ release the reference, but allocates a new reference for
-- the returned run, which must be released at some point.
expectCompleted ::
     (MonadMVar m, MonadSTM m, MonadST m, MonadMask m)
  => Ref (MergingRun m h) -> m (Ref (Run m h))
expectCompleted (DeRef MergingRun {..}) = do
    knownCompleted <- readMutVar mergeKnownCompleted
    -- The merge is not guaranteed to be complete, so we do the remaining steps
    when (knownCompleted == MergeMaybeCompleted) $ do
      totalSteps <- readPrimVar (getTotalStepsVar mergeStepsPerformed)
      let !credits = Credits (unNumEntries mergeNumEntries - totalSteps)
      isMergeDone <- stepMerge mergeState mergeStepsPerformed credits
      when isMergeDone $ completeMerge mergeState mergeKnownCompleted
      -- TODO: can we think of a check to see if we did not do too much work
      -- here?
    withMVar mergeState $ \case
      CompletedMerge r -> dupRef r  -- return a fresh reference to the run
      OngoingMerge{} -> do
        -- If the algorithm finds an ongoing merge here, then it is a bug in
        -- our merge sceduling algorithm. As such, we throw a pure error.
        error "expectCompleted: expected a completed merge, but found an ongoing merge"
