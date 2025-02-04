{-# LANGUAGE CPP             #-}
{-# LANGUAGE MultiWayIf      #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE TypeFamilies    #-}

{- HLINT ignore "Use when" -}

-- | An incremental merge of multiple runs.
module Database.LSMTree.Internal.MergingRun (
    -- * Merging run
    MergingRun
  , NumRuns (..)
  , new
  , newCompleted
  , duplicateRuns
  , supplyCredits
  , expectCompleted
  , snapshot
  , numRuns

    -- * Credit tracking
    -- $credittracking
  , Credits (..)
  , CreditThreshold (..)
  , SuppliedCredits (..)

  -- * Concurrency
  -- $concurrency

    -- * Internal state
  , pattern MergingRun
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

-- | Take a snapshot of the state of a merging run.
snapshot ::
     (PrimMonad m, MonadMVar m)
  => Ref (MergingRun m h)
  -> m (MergingRunState m h,
        SuppliedCredits,
        NumRuns,
        NumEntries)
snapshot (DeRef MergingRun {..}) = do
    state <- readMVar mergeState
    (SpentCredits   spent,
     UnspentCredits unspent) <- atomicReadCredits mergeCreditsVar
    let supplied = SuppliedCredits (spent + unspent)
    return (state, supplied, mergeNumRuns, mergeNumEntries)

numRuns :: Ref (MergingRun m h) -> NumRuns
numRuns (DeRef MergingRun {mergeNumRuns}) = mergeNumRuns

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
performed) will typically be equal. They are not guaranteed to be equal
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
-- Note that due to asynchronous exceptions this counter can decrease as well
-- as increase. This can happen if a thread calling 'supplyCredits' is
-- interrupted after the credits have been added to the supplied credits but
-- before the corresponding merging work has been completed, in which case the
-- supplied credits will be removed again.
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
#if MIN_VERSION_GLASGOW_HASKELL(9,0,0,0)
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

The main lock will will discuss is the 'mergeState' 'StrictMVar', and we will
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

First, we use 'atomicDepositCredits' to deposit credits (atomically) to the
unspent pot in the 'CreditsVar'. This is guaranteed to never cause the supplied
credits to exceed the total debt for the merging run, and so there may be some
leftover credits. This returns the snapshot of the new spent, unspent (and thus
supplied) credits as a result of the deposit.

We now test to see if we are in one of three cases:

1. definitely close enough to the end of the merge to complete it;

2. not case 1, but enough unspent credits have accumulated to do a batch of
   merge work;

3. not case 1 or 2, not enough credits to do any merge work.

To test if we are in case 1, we check if the supplied credits has reached the
total merge debt (the number of input merge entries). This test of \"have we
supplied enough credits to pay off the debt" is monotonic: the debt is fixed
and the credits supplied increases monotonically. We then do another credit
accounting transaction: `atomicSpendAllCredits` that transfers all credits in
the unspent pot to the spent pot. This may transfer zero credits if another
thread got there first. The amount transferred is returned and we then attempt
to perform that number of merging steps. While doing the merging work, the
(more expensive) merge lock is taken to ensure that the merging work itself is
performed only sequentially. Note that it is not guaranteed that the merge gets
completed, neither by this thread nor another. While one thread will succeed in
transferring the credits, it may be interrupted during the merge (by an async
exception) and have to \"unspend\" the credits again (i.e. transfer them back).
This does not matter because the merge will be completed in 'expectCompleted'.
Completing early is an optimisation.

For case 2, we test if the (snapshot of the) unspent credits are over the batch
threshold. If so we do another accounting transaction: 'atomicSpendBatchCredits'
that attempts to transfer one or more batches of unspent credits (a multiple of
the batch size) to the spent pot, and returning the credits transferred. If
multiple concurrent threads are doing the same then only one will succeed in
transferring credits, while the others will find they transferred zero. The
unsuccessful threads will do no work, while the successful one will take the
merge lock and try to perform the number of merge steps corresponding to the
credits transferred.

For case 3 there is nothing further to do.

In case 1 and 2, if an exception occurs then the credits that were in the
process of being spent are transferred back from the spent to the unspent pot
using 'atomicUnspendCredits'. This is what means the spent credits does not
increase monotonically, even though the supplied credits do increase
monotonically.

In case 1 and 2, once performing merge steps is done, if it turns out that
excess merge steps were performed then we must do a further accounting
transaction: 'atomicSpendGivenCredits' to spend the excess credits. This is
done without respect to the balance of the unspent credits, which may result in
the unspent credit balance becoming negative. This is ok, and will result in
more credits having to be supplied next time before reaching the credit batch
threshold. The unspent credits can not be negative by the time the merge is
complete because the performing of merge steps cannot do excess steps when it
reaches the end of the merge.

-}

{-# INLINE atomicReadCredits #-}
atomicReadCredits ::
     PrimMonad m
  => CreditsVar (PrimState m)
  -> m (SpentCredits, UnspentCredits)
atomicReadCredits (CreditsVar v) =
    unpackCreditsPair <$> atomicReadInt v

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

{-# SPECIALISE atomicDepositCredits ::
     CreditsVar RealWorld
  -> Credits
  -> Credits
  -> IO (SuppliedCredits, SpentCredits, UnspentCredits, Credits) #-}
-- | Atomically: add to the unspent credits pot, subject to the supplied
-- credits not exceeding the total debt. Return the new spent and unspent
-- credits, plus any leftover credits in excess of the total debt.
--
-- This is the only operation that changes the total supplied credits, and in
-- a non-decreasing way. Hence overall the supplied credits is monotonically
-- non-decreasing.
--
atomicDepositCredits ::
     PrimMonad m
  => CreditsVar (PrimState m)
  -> Credits -- ^ total debt
  -> Credits -- ^ to deposit
  -> m (SuppliedCredits, SpentCredits, UnspentCredits, Credits)
atomicDepositCredits (CreditsVar var) totalDebt credits =
    assert (credits >= 0) $
    atomicModifyInt var $ \(CreditsPair (SpentCredits   !spent)
                                        (UnspentCredits !unspent)) ->
      let !supplied  = spent + unspent
          !leftover  = max 0 (supplied + credits - totalDebt)
          !deposit   = credits - leftover
          !unspent'  = unspent + deposit
          !after     = CreditsPair (SpentCredits   spent)
                                   (UnspentCredits unspent')
          !supplied' = spent + unspent'
          result     = (SuppliedCredits supplied',
                        SpentCredits    spent,
                        UnspentCredits  unspent',
                                        leftover)
       in assert (supplied' >= supplied) $
          assert (unspent'  >= unspent) $
          assert (deposit   >= 0) $
          assert (leftover  >= 0) $
          (after, result)

{-# SPECIALISE atomicSpendBatchCredits ::
     CreditsVar RealWorld
  -> CreditThreshold
  -> IO Credits #-}
-- | Atomically: transfer credits from the unspent pot into the spent pot, and
-- return the number transferred.
--
-- The number transferred will be a non-negative multiple of the credit
-- threshold. This can be zero if the current number of unspent credits is less
-- than the threshold. The unspent credits afterwards will be less than the
-- threshold.
--
-- The total supplied credits is unchanged.
--
atomicSpendBatchCredits ::
     PrimMonad m
  => CreditsVar (PrimState m)
  -> CreditThreshold
  -> m Credits
atomicSpendBatchCredits (CreditsVar var) (CreditThreshold batchThreshold) =
    atomicModifyInt var $ \(CreditsPair (SpentCredits   !spent)
                                        (UnspentCredits !unspent)) ->
      -- numBatches may be zero, in which case the result will be zero
      let !nBatches = unspent `div` batchThreshold
          !spend    = nBatches * batchThreshold
          !spent'   = spent   + spend
          !unspent' = unspent - spend
          !after    = CreditsPair (SpentCredits   spent')
                                  (UnspentCredits unspent')
       in assert (spend >= 0) $
          assert (unspent' < batchThreshold) $
          assert (spent' + unspent' == spent + unspent) $
          (after, spend)

{-# SPECIALISE atomicSpendAllCredits ::
     CreditsVar RealWorld
  -> IO Credits #-}
-- | Atomically: transfer all unspent credits into the spent pot, and return
-- the number transferred (which may be zero, or negative).
--
-- The total supplied credits is unchanged.
--
atomicSpendAllCredits ::
     PrimMonad m
  => CreditsVar (PrimState m)
  -> m Credits
atomicSpendAllCredits (CreditsVar var) =
    atomicModifyInt var $ \(CreditsPair (SpentCredits   !spent)
                                        (UnspentCredits !unspent)) ->
      let spent'   = spent + unspent
          unspent' = 0
          after    = CreditsPair (SpentCredits   spent')
                                 (UnspentCredits unspent')
       in assert (spent' + unspent' == spent + unspent) $
          (after, unspent)

{-# SPECIALISE atomicSpendGivenCredits ::
     CreditsVar RealWorld
  -> Credits
  -> IO () #-}
-- | Atomically: transfer the given number of credits from the unspent pot to
-- the spent pot. The new unspent credits balance may be negative.
--
-- The total supplied credits is unchanged.
--
atomicSpendGivenCredits ::
     PrimMonad m
  => CreditsVar (PrimState m)
  -> Credits
  -> m ()
atomicSpendGivenCredits (CreditsVar var) spend =
    assert (spend >= 0) $
    atomicModifyInt var $ \(CreditsPair (SpentCredits   !spent)
                                        (UnspentCredits !unspent)) ->
      let spent'   = spent   + spend
          unspent' = unspent - spend
          after    = CreditsPair (SpentCredits   spent')
                                 (UnspentCredits unspent')
       in assert (spent' + unspent' == spent + unspent) $
          (after, ())

{-# SPECIALISE atomicUnspendCredits ::
     CreditsVar RealWorld
  -> Credits
  -> IO () #-}
-- | Atomically: transfer the given number of credits from the spent pot to
-- the unspent pot.
--
-- The total supplied credits is unchanged.
--
atomicUnspendCredits ::
     PrimMonad m
  => CreditsVar (PrimState m)
  -> Credits
  -> m ()
atomicUnspendCredits (CreditsVar var) unspend =
    assert (unspend >= 0) $
    atomicModifyInt var $ \(CreditsPair (SpentCredits   !spent)
                                        (UnspentCredits !unspent)) ->
      let spent'   = spent   - unspend
          unspent' = unspent + unspend
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
  -> IO Credits #-}
-- | Supply the given amount of credits to a merging run. This /may/ cause an
-- ongoing merge to progress.
supplyCredits ::
     forall m h. (MonadSTM m, MonadST m, MonadMVar m, MonadMask m)
  => Ref (MergingRun m h)
  -> CreditThreshold
  -> Credits
  -> m Credits
supplyCredits (DeRef mrun@MergingRun {mergeKnownCompleted})
              !creditBatchThreshold !credits = do
    mergeCompleted <- readMutVar mergeKnownCompleted
    case mergeCompleted of
      MergeKnownCompleted -> pure credits
      MergeMaybeCompleted -> supplyCredits' mrun creditBatchThreshold credits

{-# SPECIALISE supplyCredits' ::
     MergingRun IO h
  -> CreditThreshold
  -> Credits
  -> IO Credits #-}
supplyCredits' ::
     forall m h. (MonadSTM m, MonadST m, MonadMVar m, MonadMask m)
  => MergingRun m h
  -> CreditThreshold
  -> Credits
  -> m Credits
supplyCredits' MergingRun {
                 mergeNumEntries,
                 mergeCreditsVar,
                 mergeState,
                 mergeKnownCompleted
               }
               creditBatchThreshold@(CreditThreshold !batchThreshold)
               !credits =
    assert (credits >= 0) $ do
    -- Atomically add credits to the unspent credits (but not allowing
    -- 'suppliedCredits' to exceed the total debt), and return a consistent
    -- snapshot of the new supplied, spent and unspent credits, and any
    -- leftover credits that would exceed the debt limit.
    let totalDebt = numEntriesToTotalDebt mergeNumEntries
    (SuppliedCredits suppliedCredits, -- sum of spent and unspent
     SpentCredits    spentCredits,
     UnspentCredits  unspentCredits,
                     leftoverCredits)
      <- atomicDepositCredits mergeCreditsVar totalDebt credits

    assert (suppliedCredits <= totalDebt &&
            suppliedCredits == spentCredits + unspentCredits &&
            leftoverCredits >= 0 && leftoverCredits <= credits)
           (return ())

    if
      | suppliedCredits == totalDebt -> do
        -- If the credit tracking says we've supplied enough credits (to meet
        -- the total merge debt) then we can (try to) do the work to finish
        -- the merge immediately.
        --
        -- We try to transfer all the remaining unspent credits into the spent
        -- pot and then perform the remaining merge steps. We may race with
        -- another thread trying to do the same, so we may end up transferring
        -- zero credits and thus not be the thread to do the final merge
        -- steps. But if it is us that finishes the merge steps then we also
        -- complete the merge (to release resources).
        --
        -- If an exception occurs (sync or async) then we reverse the transfer
        -- of the unspent credits.
        weFinishedMerge <-
          bracketOnError
            (atomicSpendAllCredits mergeCreditsVar)
            (atomicUnspendCredits mergeCreditsVar)
            (performMergeSteps mergeState mergeCreditsVar)
        -- If an async exception happens before we get to perform the
        -- completion, then that is fine. The next supplyCredits will
        -- complete the merge.
        when weFinishedMerge $ completeMerge mergeState mergeKnownCompleted
        -- We reliably know the credits we supplied that were left over.
        return leftoverCredits

      | unspentCredits >= batchThreshold -> do
        -- If the unspent credits have reached the threshold then we will try
        -- to perform a batch of merging work.
        --
        -- We try to transfer a batch of credits from the unspent to spent pot
        -- and then spend the transferred amount on merging work. If we race
        -- with another thread trying to do the same then we may end up
        -- transferring zero credits, in which case we do no merging work.
        --
        -- If an exception occurs while merging (sync or async) then we
        -- reverse the transfer of the unspent credits.
        weFinishedMerge <-
          bracketOnError
            (atomicSpendBatchCredits mergeCreditsVar creditBatchThreshold)
            (atomicUnspendCredits mergeCreditsVar)
            (performMergeSteps mergeState mergeCreditsVar)
        -- If an async exception happens before we get to perform the
        -- completion, then that is fine. The next supplyCredits will
        -- complete the merge.
        when weFinishedMerge $ completeMerge mergeState mergeKnownCompleted
        -- We didn't finish, so can be no leftover credits
        assert (leftoverCredits == 0) $ pure (Credits 0)

      -- Otherwise just accumulate credits (which we did already above),
      -- because we are not over the threshold yet.
      | otherwise ->
        -- We didn't finish, so can be no leftover credits
        assert (leftoverCredits == 0) $ pure (Credits 0)

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
performMergeSteps _ _ (Credits credits) | credits <= 0 =
    -- If we raced with another thread then we may find we have no work to do,
    -- in which case we do not need to wait on the MVar, but can just carry on.
    pure False

performMergeSteps mergeVar creditsVar (Credits credits) =
    withMVar mergeVar $ \case
      CompletedMerge{}   -> pure False
      OngoingMerge _rs m -> do
        -- We have dealt with the case of credits <= 0 above,
        -- so here we know credits is positive
        let stepsToDo = credits
        (stepsDone, stepResult) <- Merge.steps m stepsToDo
        assert (stepsDone >= stepsToDo) (pure ())
        -- Merge.steps guarantees that @stepsDone >= stepsToDo@ /unless/ the
        -- merge was just now finished and excess credit was supplied. But
        -- elsewhere we guarantee that we do not supply credit in excess of
        -- the total debt, so the inequality should hold here always.

        -- If excess merging steps were done then we must account for that.
        -- We do so by borrowing the excess from the unspent credits pot and
        -- spending them, i.e. doing a transfer from unspent to spent. This
        -- may result in the unspent credits pot becoming negative.
        let stepsExcess = Credits (stepsDone - stepsToDo)
        when (stepsExcess > 0) $
          atomicSpendGivenCredits creditsVar stepsExcess

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
--TODO: the following ought to be true and the right answer:
--        !credits         = assert (suppliedCredits == totalDebt) $
--                           unspentCredits
      -- what about exception safety
      weFinishedMerge <- performMergeSteps mergeState mergeCreditsVar credits
      when weFinishedMerge $ completeMerge mergeState mergeKnownCompleted
      -- TODO: can we think of a check to see if we did not do too much work
      -- here?
    withMVar mergeState $ \case
      CompletedMerge r -> dupRef r  -- return a fresh reference to the run
      OngoingMerge{} -> do
        -- If the algorithm finds an ongoing merge here, then it is a bug in
        -- our merge sceduling algorithm. As such, we throw a pure error.
        error "expectCompleted: expected a completed merge, but found an ongoing merge"
