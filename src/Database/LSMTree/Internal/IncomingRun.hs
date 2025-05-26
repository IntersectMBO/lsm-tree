{-# LANGUAGE CPP           #-}
{-# LANGUAGE MagicHash     #-}
{-# LANGUAGE UnboxedTuples #-}
{-# OPTIONS_HADDOCK not-home #-}

module Database.LSMTree.Internal.IncomingRun (
    IncomingRun (..)
  , MergePolicyForLevel (..)
  , duplicateIncomingRun
  , releaseIncomingRun
  , newIncomingSingleRun
  , newIncomingMergingRun
  , snapshotIncomingRun

    -- * Credits and credit tracking
    -- $credittracking
  , NominalDebt (..)
  , NominalCredits (..)
  , nominalDebtAsCredits
  , supplyCreditsIncomingRun
  , immediatelyCompleteIncomingRun
  ) where

import           Control.Concurrent.Class.MonadMVar.Strict
import           Control.DeepSeq (NFData (..))
import           Control.Monad.Class.MonadST (MonadST)
import           Control.Monad.Class.MonadSTM (MonadSTM (..))
import           Control.Monad.Class.MonadThrow (MonadMask, MonadThrow (..))
import           Control.Monad.Primitive
import           Control.RefCount
import           Data.Primitive (Prim)
import           Data.Primitive.PrimVar
import           Database.LSMTree.Internal.Assertions (assert)
import           Database.LSMTree.Internal.Config
import           Database.LSMTree.Internal.MergingRun (MergeCredits (..),
                     MergeDebt (..), MergingRun)
import qualified Database.LSMTree.Internal.MergingRun as MR
import           Database.LSMTree.Internal.Run (Run)

import           GHC.Exts (Word (W#), quotRemWord2#, timesWord2#)

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

{-# SPECIALISE duplicateIncomingRun :: IncomingRun IO h -> IO (IncomingRun IO h) #-}
duplicateIncomingRun ::
     (PrimMonad m, MonadMask m)
  => IncomingRun m h
  -> m (IncomingRun m h)
duplicateIncomingRun (Single r) =
    Single <$> dupRef r

duplicateIncomingRun (Merging mp md mcv mr) =
    Merging mp md <$> (newPrimVar =<< readPrimVar mcv)
                  <*> dupRef mr

{-# SPECIALISE releaseIncomingRun :: IncomingRun IO h -> IO () #-}
releaseIncomingRun ::
     (PrimMonad m, MonadMask m)
  => IncomingRun m h -> m ()
releaseIncomingRun (Single         r) = releaseRef r
releaseIncomingRun (Merging _ _ _ mr) = releaseRef mr

{-# INLINE newIncomingSingleRun #-}
newIncomingSingleRun ::
     (PrimMonad m, MonadThrow m)
  => Ref (Run m h)
  -> m (IncomingRun m h)
newIncomingSingleRun r = Single <$> dupRef r

{-# INLINE newIncomingMergingRun #-}
newIncomingMergingRun ::
     (PrimMonad m, MonadThrow m)
  => MergePolicyForLevel
  -> NominalDebt
  -> Ref (MergingRun MR.LevelMergeType m h)
  -> m (IncomingRun m h)
newIncomingMergingRun mergePolicy nominalDebt mr = do
    nominalCreditsVar <- newPrimVar (NominalCredits 0)
    Merging mergePolicy nominalDebt nominalCreditsVar <$> dupRef mr

{-# SPECIALISE snapshotIncomingRun ::
     IncomingRun IO h
  -> IO (Either (Ref (Run IO h))
                (MergePolicyForLevel,
                 NominalDebt,
                 NominalCredits,
                 Ref (MergingRun MR.LevelMergeType IO h))) #-}
snapshotIncomingRun ::
     PrimMonad m
  => IncomingRun m h
  -> m (Either (Ref (Run m h))
               (MergePolicyForLevel,
                NominalDebt,
                NominalCredits,
                Ref (MergingRun MR.LevelMergeType m h)))
snapshotIncomingRun (Single r) = pure (Left r)
snapshotIncomingRun (Merging mergePolicy nominalDebt nominalCreditsVar mr) = do
    nominalCredits <- readPrimVar nominalCreditsVar
    pure (Right (mergePolicy, nominalDebt, nominalCredits, mr))

{-------------------------------------------------------------------------------
  Credits
-------------------------------------------------------------------------------}

{- $credittracking

With scheduled merges, each update (e.g., insert) on a table contributes to the
progression of ongoing merges in the levels structure. This ensures that merges
are finished in time before a new merge has to be started. The points in the
evolution of the levels structure where new merges are started are known: a
flush of a full write buffer will create a new run on the first level, and
after sufficient flushes (e.g., 4) we will start at least one new merge on the
second level. This may cascade down to lower levels depending on how full the
levels are. As such, we have a well-defined measure to determine when merges
should be finished: it only depends on the maximum size of the write buffer!

The simplest solution to making sure merges are done in time is to step them to
completion immediately when started. This does not, however, spread out work
over time nicely. Instead, we schedule merge work based on how many updates are
made on the table, taking care to ensure that the merge is finished /just/ in
time before the next flush comes around, and not too early.

The progression is tracked using nominal credits. Each individual update
contributes a single credit to each level, since each level contains precisely
one ongoing merge. Contributing a credit does not, however, translate directly
to performing one /unit/ of merging work:

* The amount of work to do for one credit is adjusted depending on the actual
  size of the merge we are doing. Last-level merges, for example, can have
  larger inputs, and therefore we have to do a little more work for each
  credit. Or input runs involved in a merge can be less than maximal size for
  the level, and so there may be less merging work to do. As such, we /scale/
  'NominalCredits' to 'MergeCredits', and then supply the 'MergeCredits' to
  the 'MergingRun'.

* Supplying 'MergeCredits' to a 'MergingRun' does not necessarily directly
  translate into performing merging work. Merge credits are accumulated until
  they go over a threshold, after which a batch of merge work will be performed.
  Configuring this threshold should allow a good balance between spreading out
  I\/O and achieving good (concurrent) performance.

Merging runs can be shared across tables, which means that multiple threads
can contribute to the same merge concurrently. Incoming runs however are /not/
shared between tables. As such the tracking of 'NominalCredits' does not need
to use any concurrency precautions.
-}

-- | Total merge debt to complete the merge in an incoming run.
--
-- This corresponds to the number (worst case, minimum number) of update
-- operations inserted into the table, before we will expect the merge to
-- complete.
newtype NominalDebt = NominalDebt Int
  deriving stock Eq
  deriving newtype (NFData)

-- | Merge credits that get supplied to a table's levels.
--
-- This corresponds to the number of update operations inserted into the table.
newtype NominalCredits = NominalCredits Int
  deriving stock Eq
  deriving newtype (Prim, NFData)

nominalDebtAsCredits :: NominalDebt -> NominalCredits
nominalDebtAsCredits (NominalDebt c) = NominalCredits c

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
supplyCreditsIncomingRun _ _ (Single _r) _ = pure ()
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
    pure ()
    --TODO: currently each supplying credits action results in contributing
    -- credits to the underlying merge, but this need not be the case. We
    -- _could_ do threshold based batching at the level of the IncomingRun.
    -- The IncomingRun does not need to worry about concurrency, so does not
    -- pay the cost of atomic operations on the counters. Then when we
    -- accumulate a batch we could supply that to the MergingRun (which must
    -- use atomic operations for its counters). We could potentially simplify
    -- MergingRun by dispensing with batching for the MergeCredits counters.

-- TODO: the thresholds for doing merge work should be different for each level,
-- maybe co-prime?
creditThresholdForLevel :: TableConfig -> LevelNo -> MR.CreditThreshold
creditThresholdForLevel conf (LevelNo _i) =
    let AllocNumEntries x = confWriteBufferAlloc conf
    in  MR.CreditThreshold (MR.UnspentCredits (MergeCredits x))

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
{-# SPECIALISE depositNominalCredits ::
     NominalDebt
  -> PrimVar RealWorld NominalCredits
  -> NominalCredits
  -> IO (NominalCredits, NominalCredits) #-}
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
    pure (NominalCredits before, after)

-- | Linearly scale a nominal credit (between 0 and the nominal debt) into an
-- equivalent merge credit (between 0 and the total merge debt).
--
-- Crucially, @100% nominal credit ~~ 100% merge credit@, so when we pay off
-- the nominal debt, we also exactly pay off the merge debt. That is:
--
-- > scaleNominalToMergeCredit nominalDebt mergeDebt nominalDebt == mergeDebt
--
-- (modulo some newtype conversions)
--
scaleNominalToMergeCredit ::
     NominalDebt
  -> MergeDebt
  -> NominalCredits
  -> MergeCredits
scaleNominalToMergeCredit (NominalDebt             nominalDebt)
                          (MergeDebt (MergeCredits mergeDebt))
                          (NominalCredits          nominalCredits) =
    -- The scaling involves an operation: (a * b) `div` c
    -- but where potentially the variables a,b,c may be bigger than a 32bit
    -- integer can hold. This would be the case for runs that have more than
    -- 4 billion entries.
    --
    -- (This is assuming 64bit Int, the problem would be even worse for 32bit
    -- systems. The solution here would also work for 32bit systems, allowing
    -- up to, 2^31, 2 billion entries per run.)
    --
    -- To work correctly in this case we need higher range for the intermediate
    -- result a*b which could be bigger than 64bits can hold. A correct
    -- implementation can use Rational, but a fast implementation should use
    -- only integer operations. This is relevant because this is on the fast
    -- path for small insertions into the table that often do no merging work
    -- and just update credit counters.

    -- The fast implementation uses integer operations that produce a 128bit
    -- intermediate result for the a*b result, and use a 128bit numerator in
    -- the division operation (but 64bit denominator). These are known as
    -- "widening multiplication" and "narrowing division". GHC has direct
    -- support for these operations as primops: timesWord2# and quotRemWord2#,
    -- but they are not exposed through any high level API shipped with GHC.

    -- The specification using Rational is:
    let mergeCredits_spec = floor $ toRational nominalCredits
                                  * toRational mergeDebt
                                  / toRational nominalDebt
    -- Note that it doesn't matter if we use floor or ceiling here.
    -- Rounding errors will not compound because we sum nominal debt and
    -- convert absolute nominal to absolute merging credit. We don't
    -- convert each deposit and sum all the rounding errors.
    -- When nominalCredits == nominalDebt then the result is exact anyway
    -- (being mergeDebt) so the rounding mode makes no difference when we
    -- get to the end of the merge. Using floor makes things simpler for
    -- the fast integer implementation below, so we take that as the spec.

        -- If the nominalCredits is between 0 and nominalDebt then it's
        -- guaranteed that the mergeCredit is between 0 and mergeDebt.
        -- The mergeDebt fits in an Int, therefore the result does too.
        -- Therefore the undefined behaviour case of timesDivABC_fast is
        -- avoided and the w2i cannot overflow.
        mergeCredits_fast = w2i $ timesDivABC_fast (i2w nominalCredits)
                                                   (i2w mergeDebt)
                                                   (i2w nominalDebt)
     in assert (0 < nominalDebt) $
        assert (0 <= nominalCredits && nominalCredits <= nominalDebt) $
        assert (mergeCredits_spec == mergeCredits_fast) $
        MergeCredits mergeCredits_fast
  where
    {-# INLINE i2w #-}
    {-# INLINE w2i #-}
    i2w :: Int -> Word
    w2i :: Word -> Int
    i2w = fromIntegral
    w2i = fromIntegral

-- | Compute @(a * b) `div` c@ for unsigned integers for the full range of
-- 64bit unsigned integers, provided that @a <= c@ and thus the result will
-- fit in 64bits.
--
-- The @a * b@ intermediate result is computed using 128bit precision.
--
-- Note: the behaviour is undefined if the result will not fit in 64bits.
-- It will probably result in immediate termination with SIGFPE.
--
timesDivABC_fast :: Word -> Word -> Word -> Word
timesDivABC_fast (W# a) (W# b) (W# c) =
    case timesWord2# a b of
      (# ph, pl #) ->
            case quotRemWord2# ph pl c of
              (# q, _r #) -> W# q

{-# SPECIALISE immediatelyCompleteIncomingRun ::
     TableConfig
  -> LevelNo
  -> IncomingRun IO h
  -> IO (Ref (Run IO h)) #-}
-- | Supply enough credits to complete the merge now.
immediatelyCompleteIncomingRun ::
     (MonadSTM m, MonadST m, MonadMVar m, MonadMask m)
  => TableConfig
  -> LevelNo
  -> IncomingRun m h
  -> m (Ref (Run m h))
immediatelyCompleteIncomingRun conf ln ir =
    case ir of
      Single r -> dupRef r
      Merging _ (NominalDebt nominalDebt) nominalCreditsVar mr -> do

        NominalCredits nominalCredits <- readPrimVar nominalCreditsVar
        let !deposit = NominalCredits (nominalDebt - nominalCredits)
        supplyCreditsIncomingRun conf ln ir deposit

        -- This ensures the merge is really completed. However, we don't
        -- release the merge yet, but we do return a new reference to the run.
        MR.expectCompleted mr
