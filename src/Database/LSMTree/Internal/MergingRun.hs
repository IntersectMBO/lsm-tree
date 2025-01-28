{-# LANGUAGE CPP             #-}
{-# LANGUAGE MultiWayIf      #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE TypeFamilies    #-}

{- HLINT ignore "Use when" -}

-- | An incremental merge of multiple runs.
module Database.LSMTree.Internal.MergingRun (
    -- * Merging run
    MergingRun (..)
  , NumRuns (..)
  , new
  , newCompleted
  , duplicateRuns
  , supplyCredits
  , expectCompleted

    -- * Credit tracking
    -- $credittracking
  , Credits (..)
  , CreditThreshold (..)
  , SuppliedCredits (..)
  , atomicReadSuppliedCredits

  -- * Concurrency
  -- $concurrency

    -- * Internal state
  , MergingRunState (..)
  , MergeKnownCompleted (..)
  , CreditsVar (..)
  ) where

import           Control.ActionRegistry
import           Control.Concurrent.Class.MonadMVar.Strict
import           Control.DeepSeq (NFData (..))
import           Control.Monad (when)
import           Control.Monad.Class.MonadST (MonadST)
import           Control.Monad.Class.MonadSTM (MonadSTM (..))
import           Control.Monad.Class.MonadThrow (MonadCatch (bracketOnError),
                     MonadMask)
import           Control.Monad.Primitive
import           Control.RefCount
import           Data.Bits
import           Data.Maybe (fromMaybe)
import           Data.Primitive.MutVar
import           Data.Primitive.PrimVar
import qualified Data.Vector as V
import           Database.LSMTree.Internal.Assertions (assert)
import           Database.LSMTree.Internal.Entry (NumEntries (..))
import           Database.LSMTree.Internal.Lookup (ResolveSerialisedValue)
import           Database.LSMTree.Internal.Merge (Merge, MergeType (..),
                     StepResult (..))
import qualified Database.LSMTree.Internal.Merge as Merge
import           Database.LSMTree.Internal.Paths (RunFsPaths (..))
import           Database.LSMTree.Internal.Run (Run)
import qualified Database.LSMTree.Internal.Run as Run
import           Database.LSMTree.Internal.RunAcc (RunBloomFilterAlloc)
import           System.FS.API (HasFS)
import           System.FS.BlockIO.API (HasBlockIO)

data MergingRun m h = MergingRun {
      mergeNumRuns        :: !NumRuns
      -- | Sum of number of entries in the input runs.
      -- This also corresponds to the total merging debt.
    , mergeNumEntries     :: !NumEntries

      -- See $credittracking

      -- | A pair of counters for tracking supplied credits:
      --
      -- 1. The supplied credits that have been spent on merging steps plus the
      --    supplied credits that are in the process of being spent.
      -- 2. The supplied credits that have not been spent and are available to
      --    spend.
      --
      -- The counters are always read & modified together atomically.
      --
    , mergeCreditsVar     :: !(CreditsVar (PrimState m))

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

newtype NumRuns = NumRuns { unNumRuns :: Int }
  deriving stock (Show, Eq)
  deriving newtype NFData

data MergingRunState m h =
    CompletedMerge
      !(Ref (Run m h))
      -- ^ Output run
  | OngoingMerge
      !(V.Vector (Ref (Run m h)))
      -- ^ Input runs
      !(Merge m h)

data MergeKnownCompleted = MergeKnownCompleted | MergeMaybeCompleted
  deriving stock Eq

instance NFData MergeKnownCompleted where
  rnf MergeKnownCompleted = ()
  rnf MergeMaybeCompleted = ()

{-# SPECIALISE new ::
     HasFS IO h
  -> HasBlockIO IO h
  -> ResolveSerialisedValue
  -> Run.RunDataCaching
  -> RunBloomFilterAlloc
  -> MergeType
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
  -> MergeType
  -> RunFsPaths
  -> V.Vector (Ref (Run m h))
  -> m (Ref (MergingRun m h))
new hfs hbio resolve caching alloc mergeType runPaths inputRuns =
    -- If creating the Merge fails, we must release the references again.
    withActionRegistry $ \reg -> do
      runs <- V.mapM (\r -> withRollback reg (dupRef r) releaseRef) inputRuns
      merge <- fromMaybe (error "newMerge: merges can not be empty")
        <$> Merge.new hfs hbio caching alloc mergeType resolve runPaths runs
      let numInputRuns = NumRuns $ V.length runs
      let numInputEntries = V.foldMap' Run.size runs
      unsafeNew numInputRuns numInputEntries MergeMaybeCompleted $
        OngoingMerge runs merge

{-# SPECIALISE newCompleted ::
     NumRuns
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
  => NumRuns
  -> NumEntries
  -> Ref (Run m h)
  -> m (Ref (MergingRun m h))
newCompleted numInputRuns numInputEntries inputRun = do
    bracketOnError (dupRef inputRun) releaseRef $ \run ->
      unsafeNew numInputRuns numInputEntries MergeKnownCompleted $
        CompletedMerge run

{-# INLINE unsafeNew #-}
unsafeNew ::
     (MonadMVar m, MonadMask m, MonadSTM m, MonadST m)
  => NumRuns
  -> NumEntries
  -> MergeKnownCompleted
  -> MergingRunState m h
  -> m (Ref (MergingRun m h))
unsafeNew mergeNumRuns mergeNumEntries knownCompleted state = do
    mergeCreditsVar <- CreditsVar <$> newPrimVar 0
    case state of
      OngoingMerge{}   -> assert (knownCompleted == MergeMaybeCompleted) (pure ())
      CompletedMerge{} -> pure ()
    mergeKnownCompleted <- newMutVar knownCompleted
    mergeState <- newMVar $! state
    newRef (finalise mergeState) $ \mergeRefCounter ->
      MergingRun {
        mergeNumRuns
      , mergeNumEntries
      , mergeCreditsVar
      , mergeKnownCompleted
      , mergeState
      , mergeRefCounter
      }
  where
    finalise var = withMVar var $ \case
        CompletedMerge r ->
          releaseRef r
        OngoingMerge rs m -> do
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
      CompletedMerge r  -> V.singleton <$> dupRef r
      OngoingMerge rs _ -> withActionRegistry $ \reg ->
        V.mapM (\r -> withRollback reg (dupRef r) releaseRef) rs

{-------------------------------------------------------------------------------
  Credits
-------------------------------------------------------------------------------}

{- $credittracking

The credits concept we use here comes from amortised analysis of data
structures (see the Bankers Method from Okasaki). Though here we use it not as
an analysis method but within the code itself for tracking the state of the
scheduled (i.e. incremental) merge.

In the prototype things are relatively simple: we simulate performing merge
work in batches (based on a threshold) and the credit tracking reflects this by
tracking unspent credits (and the debt corresponding to the remaining merge
work to do).

The implementation is similar but somewhat more complex. We also accumulate
unspent credits until they reach a threshold at which point we do a batch of
merging work. Unlike the prototype, the implementation tracks both credits
spent credits as yet unspent. We will elaborate on why and how below.

In the prototype, the credits spent equals the merge steps performed. The
same holds in the real implementation, but making it so is more complicated.
When we spend credits on merging work, the number of steps we perform is not
guaranteed to be the same as the credits supplied. For example we may ask to do
100 credits of merging work, but the merge code (for perfectly sensible
efficiency reasons) will decide to do 102 units of merging work. The rule is
that we may do (slightly) more work than the credits supplied but not less.
To account for this we spend more credits, corresponding to the excess merging
work performed. We spend them by borrowing them from the unspent credits, which
may leave the unspent credits with a negative balance.

Furthermore, the real implementation has to cope with concurrency: multiple
threads sharing the same 'MergingRun' and calling 'supplyCredits' concurrently.
The credit accounting thus needs to define the state of the credits while
merging work is in progress by some thread. The approach we take is to define
spent credits to include those that are in the process of being spent, leaving
unspent credits as those that are available for a thread to spend on merging
work.

Thus we track two things:

 * credits spent ('SpentCredits'): credits supplied that have been or are in
   the process of being spent on performing merging steps; and

 * credits unspent ('UnspentCredits'): credits supplied that are not yet spent
   and are thus available to spend.

The credits supplied is the sum of the credits spent and unspent.

The credits spent and the steps performed (or in the process of being
performed) will typically be equal. They are not guaranteed to be equal in the
presence of exceptions (synchronous or asynchronous). In this case we offer a
weaker guarantee: : a merge /may/ progress more steps than the number of
credits that were spent. If an exception happens at some point during merging
work, we will \"unspend\" all the credits we intended to spend, but we will not
revert all merging steps that we already successfully performed before the
exception. Thus we may do more merging steps than the credits we accounted as
spent. This makes the implementation simple, and merges will still finish in
time. It would be bad if we did not put back credits, because then a merge
might not finish in time, which will mess up the shape of the levels tree.
-}

newtype Credits = Credits Int
  deriving stock (Eq, Ord)
  deriving newtype (Num, Real, Enum, Integral)

{-# INLINE numEntriesToTotalDebt #-}
-- | The total debt of the merging run is exactly the sum total number of
-- entries across all the input runs to be merged.
--
numEntriesToTotalDebt :: NumEntries -> Credits
numEntriesToTotalDebt (NumEntries n) = Credits n

-- | Unspent credits are accumulated until they go over the 'CreditThreshold',
-- after which a batch of merge work will be performed. Configuring this
-- threshold should allow to achieve a nice balance between spreading out
-- I\/O and achieving good (concurrent) performance.
--
-- Note that ideally the batch size for different LSM levels should be
-- co-prime so that merge work at different levels is not synchronised.
--
newtype CreditThreshold = CreditThreshold Credits

-- | The supplied credits is simply the sum of all the credits that have been
-- (successfully) supplied to a merging run via 'supplyCredits'.
--
-- The supplied credits is also the sum of the 'SpentCredits' and
-- 'UnspentCredits'.
--
-- The supplied credits increases monotonically, even in the presence of
-- (a)synchronous exceptions.
--
newtype SuppliedCredits = SuppliedCredits Credits

-- | The spent credits are supplied credits that have been spent on performing
-- merging steps plus the supplied credits that are in the process of being
-- spent (by some thread calling 'supplyCredits').
--
newtype SpentCredits = SpentCredits Credits

-- | 40 bit unsigned number
instance Bounded SpentCredits where
    minBound = SpentCredits 0
    maxBound = SpentCredits (Credits (1 `unsafeShiftL` 40 - 1))

-- | The unspent credits are supplied credits that have not yet been spent on
-- performing merging steps and are available to spend.
--
-- Note: unspent credits may be negative! This can occur when more merge
-- steps were performed than there were credits to cover. In this case the
-- credits are borrowed from the unspent credits, which may result in the
-- current unspent credits being negative for a time.
--
newtype UnspentCredits = UnspentCredits Credits

-- | 24 bit signed number
instance Bounded UnspentCredits where
    minBound = UnspentCredits (Credits ((-1) `unsafeShiftL` 23))
    maxBound = UnspentCredits (Credits (  1  `unsafeShiftL` 23 - 1))

-- | This holds the pair of the 'SpentCredits' and the 'UnspentCredits'. All
-- operations on this pair are atomic.
--
-- The model to think about is a @TVar (SpentCredits, UnspentCredits)@ but the
-- physical representation is a single mutable unboxed 64bit signed @Int@,
-- using 40 bits for the spent credits and 24 for the unspent credits. The
-- spent credits are unsigned, while the unspent credits are signed, so 40 bits
-- and 23+1 bits respectively. This imposes a limit of just over 1 trillion for
-- the spent credits and thus run size, and 8.3 million for the unspent credits
-- (23 + sign bit).
--
-- If these limits ever become restrictive, then the implementation could be
-- changed to use a TVar or a double-word CAS (DWCAS, i.e. 128bit).
--
newtype CreditsVar s = CreditsVar (PrimVar s Int)

pattern CreditsPair :: SpentCredits -> UnspentCredits -> Int
pattern CreditsPair sc uc <- (unpackCreditsPair -> (sc, uc))
  where
    CreditsPair sc uc = packCreditsPair sc uc
#if MIN_VERSION_GLASGOW_HASKELL(9,2,0,0)
{-# INLINE CreditsPair #-}
#endif
{-# COMPLETE CreditsPair #-}

-- TODO: test pack/unpack round trip with the minBound & maxBounds

{-# INLINE packCreditsPair #-}
packCreditsPair :: SpentCredits -> UnspentCredits -> Int
packCreditsPair (SpentCredits (Credits sc)) (UnspentCredits (Credits uc)) =
      sc `unsafeShiftL` 24
  .|. (uc .&. 0xffffff)

{-# INLINE unpackCreditsPair #-}
unpackCreditsPair :: Int -> (SpentCredits, UnspentCredits)
unpackCreditsPair cp =
    -- we use unsigned shift for spent, and sign extending shift for unspent
    ( SpentCredits   (Credits (w2i (i2w cp `unsafeShiftR` 24)))
    , UnspentCredits (Credits ((cp `unsafeShiftL` 40) `unsafeShiftR` 40))
    )
  where
    i2w :: Int -> Word
    w2i :: Word -> Int
    i2w = fromIntegral
    w2i = fromIntegral

{-------------------------------------------------------------------------------
  Credit transactions
-------------------------------------------------------------------------------}

{- $concurrency

Merging runs can be shared across tables, which means that multiple threads can
contribute to the same merge concurrently. The design to contribute credits to
the same merging run is largely lock-free. It ensures consistency of the
unspent credits and the merge state, while allowing threads to progress without
waiting on other threads.

The entry point for merging is 'supplyCredits'. This may be called by
concurrent threads that share the same merging run. No locks are held
initially.

The main lock we will discuss is the 'mergeState' 'StrictMVar', and we will
refer to it as the merge lock.

We get the easy things out of the way first: the 'mergeKnownCompleted'
variable is purely an optimisation. It starts out as 'MergeMaybeCompleted'
and is only ever modified once to 'MergeKnownCompleted'. It is modified with
the merge lock held, but read without the lock. It does not matter if a thread
reads a stale value of 'MergeMaybeCompleted'. We can analyse the remainder of
the algorithm as if we were always in the 'MergeMaybeCompleted' state.

Variable access and locks:

* 'CreditsVar' contains the pair of the current 'SpentCredits' and
  'UnspentCredits'. Is only operated upon using transactions (atomic CAS),
  and most of these transactions are done without the merge lock held.
  The two constituent components can increase and decrease, but the total
  supplied credits (sum of spent and unspent) can only increase.

* 'MergeState' contains the state of the merge itself. It is protected by the
  merge lock.

First, we do a moderately complex transaction 'atomicDepositAndSpendCredits',
which does the following:

 * Deposit credits to the unspent pot, while guaranteeing that the total
   supplied credits does not exceed the total debt for the merging run.
 * Compute any leftover credits (that would have exceeded the total debt).
 * Compute the credits to spend on performing merge steps, depending on which
   of three cases we are in:

    1. we have supplied enough credits to complete the merge;
    2. not case 1, but enough unspent credits have accumulated to do a batch of
       merge work;
    3. not case 1 or 2, not enough credits to do any merge work.

  * Update the spent and unspent pots
  * Return the credits to spend now and any leftover credits.

If there are now credits to spend, then we attempt to perform that number of
merging steps. While doing the merging work, the (more expensive) merge lock is
taken to ensure that the merging work itself is performed only sequentially.

Note that it is not guaranteed that the merge gets completed, even if the
credits supplied has reached the total debt. It may be interrupted during the
merge (by an async exception). This does not matter because the merge will be
completed in 'expectCompleted'. Completing early is an optimisation.

If an exception occurs during the merge then the credits that were in the
process of being spent are transferred back from the spent to the unspent pot
using 'atomicSpendCredits' (with a negative amount). It is this case that
implies that the spent credits may not increase monotonically, even though the
supplied credits do increase monotonically.

Once performing merge steps is done, if it turns out that excess merge steps
were performed then we must do a further accounting transaction:
'atomicSpendCredits' to spend the excess credits. This is done without respect
to the balance of the unspent credits, which may result in the unspent credit
balance becoming negative. This is ok, and will result in more credits having
to be supplied next time before reaching the credit batch threshold. The
unspent credits can not be negative by the time the merge is complete because
the performing of merge steps cannot do excess steps when it reaches the end of
the merge.

-}

{-# INLINE atomicReadCredits #-}
atomicReadCredits ::
     PrimMonad m
  => CreditsVar (PrimState m)
  -> m (SpentCredits, UnspentCredits)
atomicReadCredits (CreditsVar v) =
    unpackCreditsPair <$> atomicReadInt v

{-# INLINE atomicReadSuppliedCredits #-}
atomicReadSuppliedCredits ::
     PrimMonad m
  => CreditsVar (PrimState m)
  -> m SuppliedCredits
atomicReadSuppliedCredits (CreditsVar v) = do
    cp <- atomicReadInt v
    let !supplied =
           case cp of
             CreditsPair (SpentCredits   spent)
                         (UnspentCredits unspent) -> spent + unspent
    return (SuppliedCredits supplied)

{-# INLINE atomicModifyInt #-}
-- | Atomically modify a single mutable integer variable, using a CAS loop.
atomicModifyInt ::
     PrimMonad m
  => PrimVar (PrimState m) Int
  -> (Int -> (Int, a))
  -> m a
atomicModifyInt var f =
    readPrimVar var >>= casLoop
  where
    casLoop !before = do
      let (!after, !result) = f before
      before' <- casInt var before after
      if before' == before
        then return result
        else casLoop before'

{-# SPECIALISE atomicDepositAndSpendCredits ::
     CreditsVar RealWorld
  -> Credits
  -> CreditThreshold
  -> Credits
  -> IO (Credits, Credits) #-}
-- | Atomically: add to the unspent credits pot, subject to the supplied
-- credits not exceeding the total debt. Return the new spent and unspent
-- credits, plus any leftover credits in excess of the total debt.
--
-- This is the only operation that changes the total supplied credits, and in
-- a non-decreasing way. Hence overall the supplied credits is monotonically
-- non-decreasing.
--
atomicDepositAndSpendCredits ::
     PrimMonad m
  => CreditsVar (PrimState m)
  -> Credits -- ^ total debt
  -> CreditThreshold
  -> Credits -- ^ to deposit
  -> m (Credits, Credits) -- ^ (spendCredits, leftoverCredits)
atomicDepositAndSpendCredits (CreditsVar !var) !totalDebt
                             (CreditThreshold !batchThreshold) !credits =
    assert (credits >= 0) $
    atomicModifyInt var $ \(CreditsPair !spent !unspent) ->
      let (supplied', unspent', leftover) = depositCredits spent unspent
          (spend, spent'', unspent'')

            -- 1. supplied enough credits to complete the merge;
            | supplied' == totalDebt
            = spendAllCredits   spent unspent'

            -- 2. not case 1, but enough unspent credits have accumulated to do
            -- a batch of merge work;
            | (\(UnspentCredits x)->x) unspent' >= batchThreshold
            = spendBatchCredits spent unspent'

            -- 3. not case 1 or 2, not enough credits to do any merge work.
            | otherwise
            = (0, spent, unspent')

       in txResultFor spent'' unspent'' spend leftover
  where
    txResultFor (SpentCredits spent) (UnspentCredits unspent) spend leftover =
      let !after = CreditsPair (SpentCredits spent) (UnspentCredits unspent)
          result = (spend, leftover)

       in assert (spent + unspent <= totalDebt) $
          assert (leftover >= 0 && leftover <= credits) $
          (after, result)

    depositCredits (SpentCredits !spent) (UnspentCredits !unspent) =
      let !supplied  = spent + unspent
          !leftover  = max 0 (supplied + credits - totalDebt)
          !deposit   = credits - leftover
          !unspent'  = unspent + deposit
          !supplied' = spent + unspent'
       in assert (unspent'  >= unspent) $
          assert (deposit   >= 0) $
          assert (leftover  >= 0) $
          (supplied', UnspentCredits unspent', leftover)

    spendBatchCredits (SpentCredits !spent) (UnspentCredits !unspent) =
      -- numBatches may be zero, in which case the result will be zero
      let !nBatches = unspent `div` batchThreshold
          !spend    = nBatches * batchThreshold
          !spent'   = spent   + spend
          !unspent' = unspent - spend
       in assert (spend >= 0) $
          assert (unspent' < batchThreshold) $
          assert (spent' + unspent' == spent + unspent) $
          (spend, SpentCredits spent', UnspentCredits unspent')

    spendAllCredits (SpentCredits !spent) (UnspentCredits !unspent) =
      let spend    = unspent
          spent'   = spent + spend
          unspent' = 0
       in assert (spent' + unspent' == spent + unspent) $
          (spend, SpentCredits spent', UnspentCredits unspent')


{-# SPECIALISE atomicSpendCredits ::
     CreditsVar RealWorld
  -> Credits
  -> IO () #-}
-- | Atomically: transfer the given number of credits from the unspent pot to
-- the spent pot. The new unspent credits balance may be negative.
--
-- The amount to spend can also be negative to reverse a spending transaction.
--
-- The total supplied credits is unchanged.
--
atomicSpendCredits ::
     PrimMonad m
  => CreditsVar (PrimState m)
  -> Credits -- ^ Can be positive to spend, or negative to unspend.
  -> m ()
atomicSpendCredits (CreditsVar var) spend =
    atomicModifyInt var $ \(CreditsPair (SpentCredits   !spent)
                                        (UnspentCredits !unspent)) ->
      let spent'   = spent   + spend
          unspent' = unspent - spend
          after    = CreditsPair (SpentCredits   spent')
                                 (UnspentCredits unspent')
       in assert (spent' + unspent' == spent + unspent) $
          (after, ())

{-------------------------------------------------------------------------------
  The main algorithms
-------------------------------------------------------------------------------}

{-# SPECIALISE supplyCredits ::
     Ref (MergingRun IO h)
  -> CreditThreshold
  -> Credits
  -> IO () #-}
-- | Supply the given amount of credits to a merging run. This /may/ cause an
-- ongoing merge to progress.
supplyCredits ::
     forall m h. (MonadSTM m, MonadST m, MonadMVar m, MonadMask m)
  => Ref (MergingRun m h)
  -> CreditThreshold
  -> Credits
  -> m ()
supplyCredits (DeRef MergingRun {
                 mergeKnownCompleted,
                 mergeNumEntries,
                 mergeCreditsVar,
                 mergeState
               })
              !creditBatchThreshold !credits =
    assert (credits >= 0) $ do
    mergeCompleted <- readMutVar mergeKnownCompleted
    case mergeCompleted of
      MergeKnownCompleted -> pure ()
      MergeMaybeCompleted ->
        bracketOnError
          -- Atomically add credits to the unspent credits (but not allowing
          -- 'suppliedCredits' to exceed the total debt), determine which case
          -- we're in and thus how many credits we should try to spend now on
          -- performing merge steps. Return the credits to spend now and any
          -- leftover credits that would exceed the debt limit.
          (atomicDepositAndSpendCredits
            mergeCreditsVar
            (numEntriesToTotalDebt mergeNumEntries)
            creditBatchThreshold credits)

          -- If an exception occurs while merging (sync or async) then we
          -- reverse the spending of the credits (but not the deposit).
          (\(spendCredits, _leftoverCredits) ->
            atomicSpendCredits mergeCreditsVar (-spendCredits))

          (\(spendCredits, _leftoverCredits) ->
            when (spendCredits > 0) $ do
              weFinishedMerge <-
                performMergeSteps mergeState mergeCreditsVar spendCredits

              -- If an async exception happens before we get to perform the
              -- completion, then that is fine. The next supplyCredits will
              -- complete the merge.
              when weFinishedMerge $
                completeMerge mergeState mergeKnownCompleted)

{-# SPECIALISE performMergeSteps ::
     StrictMVar IO (MergingRunState IO h)
  -> CreditsVar RealWorld
  -> Credits
  -> IO Bool #-}
performMergeSteps ::
     (MonadMVar m, MonadMask m, MonadSTM m, MonadST m)
  => StrictMVar m (MergingRunState m h)
  -> CreditsVar (PrimState m)
  -> Credits
  -> m Bool
performMergeSteps mergeVar creditsVar (Credits credits) =
    withMVar mergeVar $ \case
      CompletedMerge{}   -> pure False
      OngoingMerge _rs m -> do
        -- We have dealt with the case of credits <= 0 above,
        -- so here we know credits is positive
        let stepsToDo = credits
        (stepsDone, stepResult) <- Merge.steps m stepsToDo
        assert (stepResult == MergeDone || stepsDone >= stepsToDo) (pure ())
        -- Merge.steps guarantees that @stepsDone >= stepsToDo@ /unless/ the
        -- merge was just now finished and excess credit was supplied.
        -- The latter is possible. As noted elsewhere, exceptions can result in
        -- us having done more merge steps than we accounted for with spent
        -- credits, hence it is possible when getting to the end of the merge
        -- for us to try to do more steps than there are steps possible to do.

        -- If excess merging steps were done then we must account for that.
        -- We do so by borrowing the excess from the unspent credits pot and
        -- spending them, i.e. doing a transfer from unspent to spent. This
        -- may result in the unspent credits pot becoming negative.
        let stepsExcess = Credits (stepsDone - stepsToDo)
        when (stepsExcess > 0) $
          atomicSpendCredits creditsVar stepsExcess

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
      (OngoingMerge rs m) -> do
        -- first try to complete the merge before performing other side effects,
        -- in case the completion fails
        --TODO: Run.fromMutable claims not to be exception safe
        -- may need to use uninteruptible mask
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
      (SpentCredits   spentCredits,
       UnspentCredits unspentCredits) <- atomicReadCredits mergeCreditsVar
      let !totalDebt       = numEntriesToTotalDebt mergeNumEntries
          !suppliedCredits = spentCredits + unspentCredits
          !credits         = assert (suppliedCredits <= totalDebt) $
                             totalDebt - spentCredits
      --TODO: the following ought to be true and be the right answer:
      --  !credits         = assert (suppliedCredits == totalDebt) $
      --                     unspentCredits

      --TODO: what about exception safety: check if it is ok to be interrupted
      -- between performMergeSteps and completeMerge here, and above.
      weFinishedMerge <- performMergeSteps mergeState mergeCreditsVar credits
      when weFinishedMerge $ completeMerge mergeState mergeKnownCompleted
      -- TODO: can we think of a check to see if we did not do too much work
      -- here? <-- assert (suppliedCredits == totalDebt) ought to do it!
      -- A related question is if we finished the merge too early, could have
      -- spread out the work better.
    withMVar mergeState $ \case
      CompletedMerge r -> dupRef r  -- return a fresh reference to the run
      OngoingMerge{} -> do
        -- If the algorithm finds an ongoing merge here, then it is a bug in
        -- our merge sceduling algorithm. As such, we throw a pure error.
        error "expectCompleted: expected a completed merge, but found an ongoing merge"
