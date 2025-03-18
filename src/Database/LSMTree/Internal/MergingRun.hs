{-# LANGUAGE CPP             #-}
{-# LANGUAGE PatternSynonyms #-}

-- | An incremental merge of multiple runs.
module Database.LSMTree.Internal.MergingRun (
    -- * Merging run
    MergingRun
  , NumRuns (..)
  , RunParams (..)
  , new
  , newCompleted
  , duplicateRuns
  , supplyCreditsRelative
  , supplyCreditsAbsolute
  , expectCompleted
  , snapshot
  , numRuns
  , totalMergeDebt
  , mergeType

    -- * Merge types
  , IsMergeType (..)
  , LevelMergeType (..)
  , TreeMergeType (..)

    -- * Credit tracking
    -- $credittracking
  , MergeDebt (..)
  , numEntriesToMergeDebt
  , MergeCredits (..)
  , CreditThreshold (..)
  , SpentCredits (..)
  , UnspentCredits (..)

  -- * Concurrency
  -- $concurrency

    -- * Internal state
  , pattern MergingRun
  , mergeState
  , MergingRunState (..)
  , MergeKnownCompleted (..)
  , CreditsVar (..)
  , pattern CreditsPair
  ) where

import           Control.ActionRegistry
import           Control.Concurrent.Class.MonadMVar.Strict
import           Control.DeepSeq (NFData (..))
import           Control.Exception (ErrorCall (..))
import           Control.Monad (when)
import           Control.Monad.Class.MonadST (MonadST)
import           Control.Monad.Class.MonadSTM (MonadSTM (..))
import           Control.Monad.Class.MonadThrow (MonadCatch (bracketOnError),
                     MonadMask, MonadThrow (throwIO))
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
import           Database.LSMTree.Internal.Merge (IsMergeType (..),
                     LevelMergeType (..), Merge, RunParams (..),
                     StepResult (..), TreeMergeType (..))
import qualified Database.LSMTree.Internal.Merge as Merge
import           Database.LSMTree.Internal.Paths (RunFsPaths (..))
import           Database.LSMTree.Internal.Run (Run)
import qualified Database.LSMTree.Internal.Run as Run
import           System.FS.API (HasFS)
import           System.FS.BlockIO.API (HasBlockIO)

data MergingRun t m h = MergingRun {
      mergeNumRuns        :: !NumRuns

      -- | The total merge debt.
      --
      -- This corresponds to the sum of the number of entries in the input runs.
    , mergeDebt           :: !MergeDebt

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
    , mergeState          :: !(StrictMVar m (MergingRunState t m h))
    , mergeRefCounter     :: !(RefCounter m)
    }

instance RefCounted m (MergingRun t m h) where
    getRefCounter = mergeRefCounter

newtype NumRuns = NumRuns { unNumRuns :: Int }
  deriving stock (Show, Eq)
  deriving newtype NFData

data MergingRunState t m h =
    CompletedMerge
      !(Ref (Run m h))
      -- ^ Output run
  | OngoingMerge
      !(V.Vector (Ref (Run m h)))
      -- ^ Input runs
      !(Merge t m h)

data MergeKnownCompleted = MergeKnownCompleted | MergeMaybeCompleted
  deriving stock Eq

instance NFData MergeKnownCompleted where
  rnf MergeKnownCompleted = ()
  rnf MergeMaybeCompleted = ()

{-# SPECIALISE new ::
     Merge.IsMergeType t
  => HasFS IO h
  -> HasBlockIO IO h
  -> ResolveSerialisedValue
  -> RunParams
  -> t
  -> RunFsPaths
  -> V.Vector (Ref (Run IO h))
  -> IO (Ref (MergingRun t IO h)) #-}
-- | Create a new merging run, returning a reference to it that must ultimately
-- be released via 'releaseRef'.
--
-- Duplicates the supplied references to the runs.
--
-- This function should be run with asynchronous exceptions masked to prevent
-- failing after internal resources have already been created.
new ::
     (Merge.IsMergeType t, MonadMVar m, MonadMask m, MonadSTM m, MonadST m)
  => HasFS m h
  -> HasBlockIO m h
  -> ResolveSerialisedValue
  -> RunParams
  -> t
  -> RunFsPaths
  -> V.Vector (Ref (Run m h))
  -> m (Ref (MergingRun t m h))
new hfs hbio resolve runParams ty runPaths inputRuns =
    assert (V.length inputRuns > 0) $ do
    -- there can be empty runs, which we don't want to include in the merge
    let nonEmptyRuns = V.filter (\r -> Run.size r > NumEntries 0) inputRuns
    -- If creating the Merge fails, we must release the references again.
    withActionRegistry $ \reg -> do
      let dupRun r = withRollback reg (dupRef r) releaseRef
      case V.length nonEmptyRuns of
        0 -> do
          r <- dupRun (V.head inputRuns)
          unsafeNew
            (NumRuns (V.length inputRuns))  -- TODO: is this sensible?
            (MergeDebt 0)
            (SpentCredits 0)
            MergeKnownCompleted
            (CompletedMerge r)
        1 -> do
          r <- dupRun (V.head nonEmptyRuns)
          unsafeNew
            (NumRuns (V.length inputRuns))  -- TODO: is this sensible?
            (MergeDebt 0)
            (SpentCredits 0)
            MergeKnownCompleted
            (CompletedMerge r)
        _ -> do
          mergeRuns <- V.mapM dupRun nonEmptyRuns
          merge <- fromMaybe (error "newMerge: merges can not be empty")
            <$> Merge.new hfs hbio runParams ty resolve runPaths mergeRuns
          unsafeNew
            (NumRuns (V.length inputRuns))  -- TODO: is this sensible?
            (numEntriesToMergeDebt (V.foldMap' Run.size mergeRuns))
            (SpentCredits 0)
            MergeMaybeCompleted
            (OngoingMerge mergeRuns merge)

{-# SPECIALISE newCompleted ::
     NumRuns
  -> MergeDebt
  -> Ref (Run IO h)
  -> IO (Ref (MergingRun t IO h)) #-}
-- | Create a merging run that is already in the completed state, returning a
-- reference that must ultimately be released via 'releaseRef'.
--
-- Duplicates the supplied reference to the run.
--
-- This function should be run with asynchronous exceptions masked to prevent
-- failing after internal resources have already been created.
newCompleted ::
     (MonadMVar m, MonadMask m, MonadSTM m, MonadST m)
  => NumRuns   -- ^ Since there are no longer any input runs, we need to be
               -- told how many there were.
  -> MergeDebt -- ^ Since there are no longer any input runs, we need to be
               -- told what the merge debt was.
  -> Ref (Run m h)
  -> m (Ref (MergingRun t m h))
newCompleted numInputRuns mergeDebt inputRun = do
    bracketOnError (dupRef inputRun) releaseRef $ \run ->
      unsafeNew
        numInputRuns
        mergeDebt
        (SpentCredits (mergeDebtAsCredits mergeDebt)) -- since it is completed
        MergeKnownCompleted
        (CompletedMerge run)

{-# INLINE unsafeNew #-}
unsafeNew ::
     (MonadMVar m, MonadMask m, MonadSTM m, MonadST m)
  => NumRuns
  -> MergeDebt
  -> SpentCredits
  -> MergeKnownCompleted
  -> MergingRunState t m h
  -> m (Ref (MergingRun t m h))
unsafeNew _ (MergeDebt mergeDebt) _ _ _
  | SpentCredits mergeDebt > maxBound
  = throwIO (ErrorCall "MergingRun.new: run size exceeds maximum of 2^40")

unsafeNew mergeNumRuns mergeDebt (SpentCredits spentCredits)
          knownCompleted state = do
    let !credits = CreditsPair (SpentCredits spentCredits) (UnspentCredits 0)
    mergeCreditsVar <- CreditsVar <$> newPrimVar credits
    mergeKnownCompleted <- newMutVar knownCompleted
    mergeState <- newMVar $! state
    newRef (finalise mergeState) $ \mergeRefCounter ->
      MergingRun {
        mergeNumRuns
      , mergeDebt
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
          -- The RunReaders in the Merge keep their own file handles to the
          -- run kopsFile open. We must close these handles *before* we release
          -- the runs themselves, which will close and delete the files.
          -- Otherwise we would be removing files that still have open handles
          -- (which does not work on Windows, and is caught by the MockFS).
          Merge.abort m
          V.forM_ rs releaseRef

-- | Create references to the runs that should be queried for lookups.
-- In particular, if the merge is not complete, these are the input runs.
{-# SPECIALISE duplicateRuns ::
     Ref (MergingRun t IO h) -> IO (V.Vector (Ref (Run IO h))) #-}
duplicateRuns ::
     (PrimMonad m, MonadMVar m, MonadMask m)
  => Ref (MergingRun t m h)
  -> m (V.Vector (Ref (Run m h)))
duplicateRuns (DeRef mr) =
    -- We take the references while holding the MVar to make sure the MergingRun
    -- does not get completed concurrently before we are done.
    withMVar (mergeState mr) $ \case
      CompletedMerge r  -> V.singleton <$> dupRef r
      OngoingMerge rs _ -> withActionRegistry $ \reg ->
        V.mapM (\r -> withRollback reg (dupRef r) releaseRef) rs

-- | Take a snapshot of the state of a merging run.
--
-- TODO: this is not concurrency safe! The inputs runs to the merging run could
-- be released concurrently by another thread that completes the merge, while
-- the snapshot is taking place. The solution is for snapshot here to duplicate
-- the runs it returns _while_ holding the mergeState MVar (to exclude threads
-- that might concurrently complete the merge). And then the caller of course
-- must be updated to release the extra references.
--
{-# SPECIALISE snapshot ::
     Ref (MergingRun t IO h)
  -> IO (NumRuns,
         MergeDebt,
         MergeCredits,
         MergingRunState t IO h) #-}
snapshot ::
     (PrimMonad m, MonadMVar m)
  => Ref (MergingRun t m h)
  -> m (NumRuns,
        MergeDebt,
        MergeCredits,
        MergingRunState t m h)
snapshot (DeRef MergingRun {..}) = do
    state <- readMVar mergeState
    (SpentCredits   spent,
     UnspentCredits unspent) <- atomicReadCredits mergeCreditsVar
    let supplied = spent + unspent
    return (mergeNumRuns, mergeDebt, supplied, state)

numRuns :: Ref (MergingRun t m h) -> NumRuns
numRuns (DeRef MergingRun {mergeNumRuns}) = mergeNumRuns

totalMergeDebt :: Ref (MergingRun t m h) -> MergeDebt
totalMergeDebt (DeRef MergingRun {mergeDebt}) = mergeDebt

{-# INLINE mergeType #-}
mergeType :: MonadMVar m => Ref (MergingRun t m h) -> m (Maybe t)
mergeType (DeRef mr) = do
    s <- readMVar (mergeState mr)
    return $ case s of
      CompletedMerge _ -> Nothing
      OngoingMerge _ m -> Just (Merge.mergeType m)

{-------------------------------------------------------------------------------
  Credits
-------------------------------------------------------------------------------}

{- $credittracking

The credits and debt concept we use here comes from amortised analysis of data
structures (see the Bankers Method from Okasaki). Though here we use it not as
an analysis method but within the code itself for tracking the state of the
scheduled (i.e. incremental) merge.

There are two notions of credits (and corresponding debt) in this LSM
implementation: nominal credits and merge credits. The merging run deals
exclusively with merge credits. See 'IncomingRun' for nominal credits.

A single merge credit corresponds with a merge step performed. Merge steps are
measured by the number of entries in the input runs that are consumed. We
measure the merge in terms of inputs, not outputs, because the number of inputs
is known beforehand, whereas the number of outputs is not. The total merge debt
is therefore defined to be the sum of the number of entries across the input
runs to the merge. Once the merge credits spent equals the merge debt then the
merge is (or rather must be) complete.

In both the prototype and implementation we accumulate unspent credits until
they reach a threshold at which point we do a batch of merging work. We track
both credits spent and credits as yet unspent.

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
spent credit to include credits that are in the process of being spent,
leaving unspent credit as credits that are available for a thread to spend on
merging work.

Thus we track three things:

 * spent credits ('SpentCredits'): credits supplied that have been or are in
   the process of being spent on performing merging steps;

 * unspent credits ('UnspentCredits'): credits supplied that are not yet spent
   and are thus available to spend; and

 * merge debt ('MergeDebt'): the sum of the sizes of the input runs, and thus
   the total merge credits that have to be spent for the merge to be complete.

And define a derived measure:

 * supplied credits: the sum of the spent and unspent credits. This is
   therefore also the sum of all the credits that have been (successfully)
   supplied to a merging run via 'supplyCredits'.

   The supplied credits increases monotonically, even in the presence of
   (a)synchronous exceptions.

   We guarantee that the supplied credits never exceeds the total debt.

When the supplied credits equals the merge debt then we may not have actually
completed the merge (since that requires spending the credits) but we have the
potential to complete the merge whenever needed without supplying any more
credits.

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

newtype MergeCredits = MergeCredits Int
  deriving stock (Eq, Ord)
  deriving newtype (Num, Real, Enum, Integral, NFData)

newtype MergeDebt = MergeDebt MergeCredits
  deriving stock (Eq, Ord)
  deriving newtype (NFData)

mergeDebtAsCredits :: MergeDebt -> MergeCredits
mergeDebtAsCredits (MergeDebt c) = c

{-# INLINE numEntriesToMergeDebt #-}
-- | The total debt of the merging run is exactly the sum total number of
-- entries across all the input runs to be merged.
--
numEntriesToMergeDebt :: NumEntries -> MergeDebt
numEntriesToMergeDebt (NumEntries n) = MergeDebt (MergeCredits n)

-- | Unspent credits are accumulated until they go over the 'CreditThreshold',
-- after which a batch of merge work will be performed. Configuring this
-- threshold should allow to achieve a nice balance between spreading out
-- I\/O and achieving good (concurrent) performance.
--
-- Note that ideally the batch size for different LSM levels should be
-- co-prime so that merge work at different levels is not synchronised.
--
newtype CreditThreshold = CreditThreshold UnspentCredits

-- | The spent credits are supplied credits that have been spent on performing
-- merging steps plus the supplied credits that are in the process of being
-- spent (by some thread calling 'supplyCredits').
--
newtype SpentCredits = SpentCredits MergeCredits
  deriving newtype (Eq, Ord)

-- | 40 bit unsigned number
instance Bounded SpentCredits where
    minBound = SpentCredits 0
    maxBound = SpentCredits (MergeCredits (1 `unsafeShiftL` 40 - 1))

-- | The unspent credits are supplied credits that have not yet been spent on
-- performing merging steps and are available to spend.
--
-- Note: unspent credits may be negative! This can occur when more merge
-- steps were performed than there were credits to cover. In this case the
-- credits are borrowed from the unspent credits, which may result in the
-- current unspent credits being negative for a time.
--
newtype UnspentCredits = UnspentCredits MergeCredits
  deriving newtype (Eq, Ord)

-- | 24 bit signed number
instance Bounded UnspentCredits where
    minBound = UnspentCredits (MergeCredits ((-1) `unsafeShiftL` 23))
    maxBound = UnspentCredits (MergeCredits (  1  `unsafeShiftL` 23 - 1))

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

{-# INLINE packCreditsPair #-}
packCreditsPair :: SpentCredits -> UnspentCredits -> Int
packCreditsPair spent@(SpentCredits (MergeCredits sc))
                unspent@(UnspentCredits (MergeCredits uc)) =
      assert (spent   >= minBound && spent   <= maxBound) $
      assert (unspent >= minBound && unspent <= maxBound) $

      sc `unsafeShiftL` 24
  .|. (uc .&. 0xffffff)

{-# INLINE unpackCreditsPair #-}
unpackCreditsPair :: Int -> (SpentCredits, UnspentCredits)
unpackCreditsPair cp =
    -- we use unsigned shift for spent, and sign extending shift for unspent
    ( SpentCredits   (MergeCredits (w2i (i2w cp `unsafeShiftR` 24)))
    , UnspentCredits (MergeCredits ((cp `unsafeShiftL` 40) `unsafeShiftR` 40))
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

The credits to supply can be specified as either an absolute or relative value.
That is, we can ask that the number of supplied credits be set to a value N, or
we can specify an additional N credits.
increasing so in the absolute case, there is no change if the requested new
supplied credit value is less than the current value. Supplying credits from
the levels (via incoming runs) uses absolute credits, while supplying credits
from merging trees using relative credits.

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

-- | Credits supplied using a relative value or an absolute value.
data SupplyMergeCredits = SupplyMergeCredits
                            !SupplyRelativeOrAbsolute
                            !MergeCredits
-- Note this is deliberately represented as a product type, not a sum type, to
-- get better unboxing in function args.

-- | Should we supply credits using a relative value or an absolute value.
data SupplyRelativeOrAbsolute = SupplyRelative | SupplyAbsolute

{-# SPECIALISE atomicDepositAndSpendCredits ::
     CreditsVar RealWorld
  -> MergeDebt
  -> CreditThreshold
  -> SupplyMergeCredits
  -> IO (MergeCredits, MergeCredits, MergeCredits, MergeCredits) #-}
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
  -> MergeDebt -- ^ total debt
  -> CreditThreshold
  -> SupplyMergeCredits
  -> m (MergeCredits, MergeCredits, MergeCredits, MergeCredits)
     -- ^ (suppliedBefore, suppliedAfter, spendCredits, leftoverCredits)
atomicDepositAndSpendCredits (CreditsVar !var) (MergeDebt !totalDebt)
                             (CreditThreshold !batchThreshold)
                             (SupplyMergeCredits !supplyRelOrAbs !credits) =
    assert (credits >= 0) $
    atomicModifyInt var $ \(CreditsPair !spent !unspent) ->
      let (supplied, supplied', unspent', leftover)
            = depositCredits spent unspent supplyRelOrAbs credits

          (spend, spent'', unspent'')
            -- 1. supplied enough credits to complete the merge;
            | supplied' == totalDebt
            = spendAllCredits   spent unspent'

            -- 2. not case 1, but enough unspent credits have accumulated to do
            -- a batch of merge work;
            | unspent' >= batchThreshold
            = spendBatchCredits spent unspent' batchThreshold

            -- 3. not case 1 or 2, not enough credits to do any merge work.
            | otherwise
            = (0, spent, unspent')

       in txResultFor supplied spent'' unspent'' spend leftover
  where
    txResultFor !supplied (SpentCredits !spent) (UnspentCredits !unspent)
                !spend !leftover =
      let !after     = CreditsPair (SpentCredits spent) (UnspentCredits unspent)
          !supplied' = spent + unspent
          !result    = (supplied, supplied', spend, leftover)

       in assert (supplied  <= supplied') $
          assert (supplied' <= totalDebt) $
          (after, result)

    depositCredits (SpentCredits !spent) (UnspentCredits !unspent)
                   SupplyRelative !deposit =
      let !supplied  = spent + unspent
          !leftover  = max 0 (supplied + deposit - totalDebt)
          !deposit'  = deposit - leftover
          !unspent'  = unspent + deposit'
          !supplied' = spent + unspent'
       in assert (unspent' >= unspent) $
          assert (deposit' >= 0) $
          assert (leftover >= 0 && leftover <= deposit) $
          (supplied, supplied', UnspentCredits unspent', leftover)

    depositCredits (SpentCredits !spent) (UnspentCredits !unspent)
                   SupplyAbsolute !targetSupplied =
      let !supplied  = spent + unspent
          !supplied' = min totalDebt (max supplied targetSupplied)
          !deposit   = supplied' - supplied
          !leftover  = 0 -- meaningless concept for absolute case
          !unspent'  = unspent + deposit
       in assert (unspent'  >= unspent) $
          assert (supplied' == spent + unspent') $
          assert (deposit   >= 0) $
          (supplied, supplied', UnspentCredits unspent', leftover)

    spendBatchCredits (SpentCredits !spent) (UnspentCredits !unspent)
                      (UnspentCredits !unspentBatchThreshold) =
      -- numBatches may be zero, in which case the result will be zero
      let !nBatches = unspent `div` unspentBatchThreshold
          !spend    = nBatches * unspentBatchThreshold
          !spent'   = spent   + spend
          !unspent' = unspent - spend
       in assert (spend >= 0) $
          assert (unspent' < unspentBatchThreshold) $
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
  -> MergeCredits
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
  -> MergeCredits -- ^ Can be positive to spend, or negative to unspend.
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

{-# INLINE supplyCreditsRelative #-}
-- | Supply the given amount of credits to a merging run. This /may/ cause an
-- ongoing merge to progress.
--
-- The credits are given in relative terms: as an addition to the current
-- supplied credits. See 'supplyCreditsAbsolute' to set the supplied credits
-- to an absolute value.
--
-- The result is:
--
--  1. The (absolute value of the) supplied credits beforehand.
--  2. The (absolute value of the) supplied credits afterwards.
--  3. The number of credits left over. This will be non-zero if the credits
--     supplied would take the total supplied credits over the total merge debt.
--
supplyCreditsRelative ::
     forall t m h. (MonadSTM m, MonadST m, MonadMVar m, MonadMask m)
  => Ref (MergingRun t m h)
  -> CreditThreshold
  -> MergeCredits
  -> m (MergeCredits, MergeCredits, MergeCredits)
       -- ^ (suppliedCredits, suppliedCredits', leftoverCredits)
supplyCreditsRelative mr th c = do
    r@(_suppliedCredits, suppliedCredits', leftoverCredits)
      <- supplyCredits mr th (SupplyMergeCredits SupplyRelative c)

    assert (suppliedCredits' == mergeDebtAsCredits (totalMergeDebt mr)
            || leftoverCredits == 0) $
      pure r

{-# INLINE supplyCreditsAbsolute #-}
-- | Set the supplied credits to the given value, unless the current value is
-- already greater. This /may/ cause an ongoing merge to progress.
--
-- The credits are given in absolute terms: as the new value for the current
-- supplied credits. See 'supplyCreditsRelative' to set the supplied credits
-- as a relative addition to the current value.
--
-- The given credit value must be no greater than the 'totalMergeDebt'.
--
-- The result is the new value of the total supplied credits, which may be more
-- than the specified value if the current value was already greater than the
-- specified value.
--
-- The result is:
--
--  1. The (absolute value of the) supplied credits beforehand.
--  2. The (absolute value of the) supplied credits afterwards. This will be
--     equal to the given value or to the supplied credits beforehand,
--     whichever is the greater.
--
supplyCreditsAbsolute ::
     forall t m h. (MonadSTM m, MonadST m, MonadMVar m, MonadMask m)
  => Ref (MergingRun t m h)
  -> CreditThreshold
  -> MergeCredits
  -> m (MergeCredits, MergeCredits)
       -- ^ (suppliedCredits, suppliedCredits')
supplyCreditsAbsolute mr th c =
    assert (0 <= c && c <= mergeDebtAsCredits (totalMergeDebt mr)) $ do
    (suppliedCredits, suppliedCredits', _leftoverCredits)
      <- supplyCredits mr th (SupplyMergeCredits SupplyAbsolute c)
    assert (suppliedCredits' == max c suppliedCredits) $
      pure (suppliedCredits, suppliedCredits')

{-# SPECIALISE supplyCredits ::
     Ref (MergingRun t IO h)
  -> CreditThreshold
  -> SupplyMergeCredits
  -> IO (MergeCredits, MergeCredits, MergeCredits) #-}
-- | Supply the given amount of credits to a merging run. This /may/ cause an
-- ongoing merge to progress.
supplyCredits ::
     forall t m h. (MonadSTM m, MonadST m, MonadMVar m, MonadMask m)
  => Ref (MergingRun t m h)
  -> CreditThreshold
  -> SupplyMergeCredits
  -> m (MergeCredits, MergeCredits, MergeCredits)
       -- ^ (suppliedCredits, suppliedCredits', leftoverCredits)
supplyCredits (DeRef MergingRun {
                 mergeKnownCompleted,
                 mergeDebt,
                 mergeCreditsVar,
                 mergeState
               })
              !creditBatchThreshold
              (SupplyMergeCredits !supplyRelOrAbs !credits) =
    assert (credits >= 0) $ do
    mergeCompleted <- readMutVar mergeKnownCompleted
    case mergeCompleted of
      MergeKnownCompleted ->
        let suppliedCredits  = mergeDebtAsCredits mergeDebt -- we're completed!
            suppliedCredits' = suppliedCredits -- we can't supply more now
            leftoverCredits  = credits -- but meaningless for SupplyAbsolute
         in pure (suppliedCredits, suppliedCredits', leftoverCredits)
      MergeMaybeCompleted ->
        bracketOnError
          -- Atomically add credits to the unspent credits (but not allowing
          -- supplied credits to exceed the total debt), determine which case
          -- we're in and thus how many credits we should try to spend now on
          -- performing merge steps. Return the credits to spend now and any
          -- leftover credits that would exceed the debt limit.
          (atomicDepositAndSpendCredits
            mergeCreditsVar mergeDebt
            creditBatchThreshold
            (SupplyMergeCredits supplyRelOrAbs credits))

          -- If an exception occurs while merging (sync or async) then we
          -- reverse the spending of the credits (but not the deposit).
          (\(_, _, spendCredits, _) ->
            atomicSpendCredits mergeCreditsVar (-spendCredits))

          (\(suppliedCredits, suppliedCredits',
             spendCredits, leftoverCredits) -> do
            when (spendCredits > 0) $ do
              weFinishedMerge <-
                performMergeSteps mergeState mergeCreditsVar spendCredits

              -- If an async exception happens before we get to perform the
              -- completion, then that is fine. The next supplyCredits will
              -- complete the merge.
              when weFinishedMerge $
                completeMerge mergeState mergeKnownCompleted

            assert   (               0 <= suppliedCredits) $
              assert (suppliedCredits  <= suppliedCredits') $
              assert (suppliedCredits' <= mergeDebtAsCredits mergeDebt) $
              return (suppliedCredits, suppliedCredits', leftoverCredits))

{-# SPECIALISE performMergeSteps ::
     StrictMVar IO (MergingRunState t IO h)
  -> CreditsVar RealWorld
  -> MergeCredits
  -> IO Bool #-}
performMergeSteps ::
     (MonadMVar m, MonadMask m, MonadSTM m, MonadST m)
  => StrictMVar m (MergingRunState t m h)
  -> CreditsVar (PrimState m)
  -> MergeCredits
  -> m Bool
performMergeSteps mergeVar creditsVar credits =
    assert (credits >= 0) $
    withMVar mergeVar $ \case
      CompletedMerge{}   -> pure False
      OngoingMerge _rs m -> do
        let MergeCredits stepsToDo = credits
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
        let stepsExcess = MergeCredits (stepsDone - stepsToDo)
        when (stepsExcess > 0) $
          atomicSpendCredits creditsVar stepsExcess

        pure $ stepResult == MergeDone

{-# SPECIALISE completeMerge ::
     StrictMVar IO (MergingRunState t IO h)
  -> MutVar RealWorld MergeKnownCompleted
  -> IO () #-}
-- | Convert an 'OngoingMerge' to a 'CompletedMerge'.
completeMerge ::
     (MonadSTM m, MonadST m, MonadMVar m, MonadMask m)
  => StrictMVar m (MergingRunState t m h)
  -> MutVar (PrimState m) MergeKnownCompleted
  -> m ()
completeMerge mergeVar mergeKnownCompletedVar = do
    modifyMVarMasked_ mergeVar $ \case
      mrs@CompletedMerge{} -> pure $! mrs
      (OngoingMerge rs m) -> do
        -- first try to complete the merge before performing other side effects,
        -- in case the completion fails
        --TODO: Run.fromMutable (used in Merge.complete) claims not to be
        -- exception safe so we should probably be using the resource registry
        -- and test for exception safety.
        r <- Merge.complete m
        V.forM_ rs releaseRef
        -- Cache the knowledge that we completed the merge
        writeMutVar mergeKnownCompletedVar MergeKnownCompleted
        pure $! CompletedMerge r

{-# SPECIALISE expectCompleted ::
     Ref (MergingRun t IO h)
  -> IO (Ref (Run IO h)) #-}
-- | This does /not/ release the reference, but allocates a new reference for
-- the returned run, which must be released at some point.
expectCompleted ::
     (MonadMVar m, MonadSTM m, MonadST m, MonadMask m)
  => Ref (MergingRun t m h) -> m (Ref (Run m h))
expectCompleted (DeRef MergingRun {..}) = do
    knownCompleted <- readMutVar mergeKnownCompleted
    -- The merge is not guaranteed to be complete, so we do the remaining steps
    when (knownCompleted == MergeMaybeCompleted) $ do
      (SpentCredits   spentCredits,
       UnspentCredits unspentCredits) <- atomicReadCredits mergeCreditsVar
      let suppliedCredits = spentCredits + unspentCredits
          !credits        = assert (MergeDebt suppliedCredits == mergeDebt) $
                            assert (unspentCredits >= 0) $
                            unspentCredits

      weFinishedMerge <- performMergeSteps mergeState mergeCreditsVar credits
      -- If an async exception happens before we get to perform the
      -- completion, then that is fine. The next 'expectCompleted' will
      -- complete the merge.
      when weFinishedMerge $ completeMerge mergeState mergeKnownCompleted
    withMVar mergeState $ \case
      CompletedMerge r -> dupRef r  -- return a fresh reference to the run
      OngoingMerge{} -> do
        -- If the algorithm finds an ongoing merge here, then it is a bug in
        -- our merge sceduling algorithm. As such, we throw a pure error.
        error "expectCompleted: expected a completed merge, but found an ongoing merge"
