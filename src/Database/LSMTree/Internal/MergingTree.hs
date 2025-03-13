{-# LANGUAGE PatternSynonyms #-}

-- | An incremental merge of multiple runs, preserving a bracketing structure.
module Database.LSMTree.Internal.MergingTree (
    -- $mergingtrees
    MergingTree (..)
  , PreExistingRun (..)
  , newCompletedMerge
  , newOngoingMerge
  , newPendingLevelMerge
  , newPendingUnionMerge
  , isStructurallyEmpty
  , remainingMergeDebt
    -- * Internal state
  , MergingTreeState (..)
  , PendingMerge (..)
  ) where

import           Control.Concurrent.Class.MonadMVar.Strict
import           Control.Monad ((<$!>))
import           Control.Monad.Class.MonadThrow (MonadMask)
import           Control.Monad.Primitive
import           Control.RefCount
import           Data.Foldable (traverse_)
import           Data.Vector (Vector)
import qualified Data.Vector as V
import           Database.LSMTree.Internal.Entry (NumEntries (..))
import           Database.LSMTree.Internal.MergingRun (MergeDebt (..),
                     MergingRun)
import qualified Database.LSMTree.Internal.MergingRun as MR
import           Database.LSMTree.Internal.Run (Run)
import qualified Database.LSMTree.Internal.Run as Run

-- $mergingtrees Semantically, tables are key-value stores like Haskell's
-- @Map@. Table unions then behave like @Map.unionWith (<>)@. If one of the
-- input tables contains a value at a particular key, the result will also
-- contain it. If multiple tables share that key, the values will be combined
-- monoidally.
--
-- Looking at the implementation, tables are not just key-value pairs, but
-- consist of runs. If each table was just a single run, unioning would involve
-- a run merge similar to the one used for compaction (when a level is full),
-- but with a different merge type 'MR.MergeUnion' that differs semantically:
-- Here, runs don't represent updates (overwriting each other), but they each
-- represent the full state of a table. There is no distinction between no
-- entry and a 'Delete', between an 'Insert' and a 'Mupsert'.
--
-- To union two tables, we can therefore first merge down each table into a
-- single run (using regular level merges) and then union merge these.
--
-- However, we want to spread out the work required and perform these merges
-- incrementally. At first, we only create a new table that is empty except for
-- a data structure 'MergingTree', representing the merges that need to be
-- done. The usual operations can then be performed on the table while the
-- merge is in progress: Inserts go into the table as usual, not affecting its
-- last level ('UnionLevel'), lookups need to consider the tree (requiring some
-- complexity and runtime overhead), further unions incorporate the in-progress
-- tree into the resulting one, which also shares future merging work.
--
-- It seems necessary to represent the suspended merges using a tree. Other
-- approaches don't allow for full sharing of the incremental work (e.g.
-- because they effectively \"re-bracket\" nested unions). It also seems
-- necessary to first merge each input table into a single run, as there is no
-- practical distributive property between level and union merges.


-- | A \"merging tree\" is a mutable representation of an incremental
-- tree-shaped nested merge. This allows to represent union merges of entire
-- tables, each of which itself first need to be merged to become a single run.
--
-- Trees have to support arbitrarily deep nesting, since each input to 'union'
-- might already contain an in-progress merging tree (which then becomes shared
-- between multiple tables).
--
data MergingTree m h = MergingTree {
      mergeState      :: !(StrictMVar m (MergingTreeState m h))
    , mergeRefCounter :: !(RefCounter m)
    }

instance RefCounted m (MergingTree m h) where
    getRefCounter = mergeRefCounter

data MergingTreeState m h =
    CompletedTreeMerge
      !(Ref (Run m h))
      -- ^ Output run

    -- | Reuses MergingRun to allow sharing existing merges.
  | OngoingTreeMerge
      !(Ref (MergingRun MR.TreeMergeType m h))

  | PendingTreeMerge
      !(PendingMerge m h)

-- | A merge that is waiting for its inputs to complete.
data PendingMerge m h =
    -- | The collection of inputs is the entire contents of a table,
    -- i.e. its (merging) runs and finally a union merge (if that table
    -- already contained a union).
    PendingLevelMerge
      !(Vector (PreExistingRun m h))
      !(Maybe (Ref (MergingTree m h)))

    -- | Each input is the entire content of a table (as a merging tree).
  | PendingUnionMerge
      !(Vector (Ref (MergingTree m h)))

pendingContent ::
     PendingMerge m h
  -> ( MR.TreeMergeType
     , Vector (PreExistingRun m h)
     , Vector (Ref (MergingTree m h))
     )
pendingContent = \case
    PendingLevelMerge prs t -> (MR.MergeLevel, prs, maybe V.empty V.singleton t)
    PendingUnionMerge    ts -> (MR.MergeUnion, V.empty, ts)

{-# COMPLETE PendingMerge #-}
pattern PendingMerge ::
     MR.TreeMergeType
  -> Vector (PreExistingRun m h)
  -> Vector (Ref (MergingTree m h))
  -> PendingMerge m h
pattern PendingMerge mt prs ts <- (pendingContent -> (mt, prs, ts))

data PreExistingRun m h =
    PreExistingRun        !(Ref (Run m h))
  | PreExistingMergingRun !(Ref (MergingRun MR.LevelMergeType m h))

{-# SPECIALISE newCompletedMerge ::
     Ref (Run IO h)
  -> IO (Ref (MergingTree IO h)) #-}
newCompletedMerge ::
     (MonadMVar m, PrimMonad m, MonadMask m)
  => Ref (Run m h)
  -> m (Ref (MergingTree m h))
newCompletedMerge run = mkMergingTree . CompletedTreeMerge =<< dupRef run

{-# SPECIALISE newOngoingMerge ::
     Ref (MergingRun MR.TreeMergeType IO h)
  -> IO (Ref (MergingTree IO h)) #-}
-- | Create a new 'MergingTree' representing the merge of an ongoing run.
-- The usage of this function is primarily to facilitate the reloading of an
-- ongoing merge from a persistent snapshot.
newOngoingMerge ::
     (MonadMVar m, PrimMonad m, MonadMask m)
  => Ref (MergingRun MR.TreeMergeType m h)
  -> m (Ref (MergingTree m h))
newOngoingMerge mr = mkMergingTree . OngoingTreeMerge =<< dupRef mr

{-# SPECIALISE newPendingLevelMerge ::
     [PreExistingRun IO h]
  -> Maybe (Ref (MergingTree IO h))
  -> IO (Ref (MergingTree IO h)) #-}
-- | Create a new 'MergingTree' representing the merge of a sequence of
-- pre-existing runs (completed or ongoing, plus a optional final tree).
-- This is for merging the entire contents of a table down to a single run
-- (while sharing existing ongoing merges).
--
-- Shape: if the list of runs is empty and the optional input tree is
-- structurally empty, the result will also be structurally empty. See
-- 'isStructurallyEmpty'.
--
-- Resource tracking:
-- * This allocates a new 'Ref' which the caller is responsible for releasing
--   eventually.
-- * The ownership of all input 'Ref's remains with the caller. This action
--   will create duplicate references, not adopt the given ones.
--
-- ASYNC: this should be called with asynchronous exceptions masked because it
-- allocates\/creates resources.
newPendingLevelMerge ::
     forall m h.
     (MonadMVar m, MonadMask m, PrimMonad m)
  => [PreExistingRun m h]
  -> Maybe (Ref (MergingTree m h))
  -> m (Ref (MergingTree m h))
newPendingLevelMerge [] (Just t) = dupRef t
newPendingLevelMerge [PreExistingRun r] Nothing = do
    -- No need to create a pending merge here.
    --
    -- We could do something similar for PreExistingMergingRun, but it's:
    -- * complicated, because of the LevelMergeType\/TreeMergeType mismatch.
    -- * unneeded, since that case should never occur. If there is only a
    --   single entry in the list, there can only be one level in the input
    --   table. At level 1 there are no merging runs, so it must be a
    --   PreExistingRun.
    r' <- dupRef r
    -- There are no interruption points here, and thus provided async
    -- exceptions are masked then there can be no async exceptions here at all.
    mkMergingTree (CompletedTreeMerge r')

newPendingLevelMerge prs mmt = do
    -- isStructurallyEmpty is an interruption point, and can receive async
    -- exceptions even when masked. So we use it first, *before* allocating
    -- new references.
    mmt' <- dupMaybeMergingTree mmt
    prs' <- traverse dupPreExistingRun (V.fromList prs)
    mkMergingTree (PendingTreeMerge (PendingLevelMerge prs' mmt'))
  where
    dupPreExistingRun (PreExistingRun r) =
      PreExistingRun <$!> dupRef r
    dupPreExistingRun (PreExistingMergingRun mr) =
      PreExistingMergingRun <$!> dupRef mr

    dupMaybeMergingTree :: Maybe (Ref (MergingTree m h))
                        -> m (Maybe (Ref (MergingTree m h)))
    dupMaybeMergingTree Nothing   = return Nothing
    dupMaybeMergingTree (Just mt) = do
      isempty <- isStructurallyEmpty mt
      if isempty
        then return Nothing
        else Just <$!> dupRef mt

{-# SPECIALISE newPendingUnionMerge ::
     [Ref (MergingTree IO h)]
  -> IO (Ref (MergingTree IO h)) #-}
-- | Create a new 'MergingTree' representing the union of one or more merging
-- trees. This is for unioning the content of multiple tables (represented
-- themselves as merging trees).
--
-- Shape: if all of the input trees are structurally empty, the result will
-- also be structurally empty. See 'isStructurallyEmpty'.
--
-- Resource tracking:
-- * This allocates a new 'Ref' which the caller is responsible for releasing
--   eventually.
-- * The ownership of all input 'Ref's remains with the caller. This action
--   will create duplicate references, not adopt the given ones.
--
-- ASYNC: this should be called with asynchronous exceptions masked because it
-- allocates\/creates resources.
newPendingUnionMerge ::
     (MonadMVar m, MonadMask m, PrimMonad m)
  => [Ref (MergingTree m h)]
  -> m (Ref (MergingTree m h))
newPendingUnionMerge mts = do
    mts' <- V.filterM (fmap not . isStructurallyEmpty) (V.fromList mts)
    -- isStructurallyEmpty is interruptible even with async exceptions masked,
    -- but we use it before allocating new references.
    mts'' <- V.mapM dupRef mts'
    case V.uncons mts'' of
      Just (mt, x) | V.null x
        -> return mt
      _ -> mkMergingTree (PendingTreeMerge (PendingUnionMerge mts''))

{-# SPECIALISE isStructurallyEmpty :: Ref (MergingTree IO h) -> IO Bool #-}
-- | Test if a 'MergingTree' is \"obviously\" empty by virtue of its structure.
-- This is not the same as being empty due to a pending or ongoing merge
-- happening to produce an empty run.
--
isStructurallyEmpty :: MonadMVar m => Ref (MergingTree m h) -> m Bool
isStructurallyEmpty (DeRef MergingTree {mergeState}) =
    isEmpty <$> readMVar mergeState
  where
    isEmpty (PendingTreeMerge (PendingLevelMerge prs Nothing)) = V.null prs
    isEmpty (PendingTreeMerge (PendingUnionMerge mts))         = V.null mts
    isEmpty _                                                  = False
    -- It may also turn out to be useful to consider CompletedTreeMerge with
    -- a zero length runs as empty.

{-# SPECIALISE mkMergingTree ::
     MergingTreeState IO h
  -> IO (Ref (MergingTree IO h)) #-}
-- | Constructor helper.
--
-- This adopts the references in the MergingTreeState, so callers should
-- duplicate first. This is not the normal pattern, but this is an internal
-- helper only.
--
mkMergingTree ::
     (MonadMVar m, PrimMonad m, MonadMask m)
  => MergingTreeState m h
  -> m (Ref (MergingTree m h))
mkMergingTree mergeTreeState = do
    mergeState <- newMVar mergeTreeState
    newRef (finalise mergeState) $ \mergeRefCounter ->
      MergingTree {
        mergeState
      , mergeRefCounter
      }

{-# SPECIALISE finalise :: StrictMVar IO (MergingTreeState IO h) -> IO () #-}
finalise :: (MonadMVar m, PrimMonad m, MonadMask m)
         => StrictMVar m (MergingTreeState m h) -> m ()
finalise mergeState = releaseMTS =<< readMVar mergeState
  where
    releaseMTS (CompletedTreeMerge r) = releaseRef r
    releaseMTS (OngoingTreeMerge  mr) = releaseRef mr
    releaseMTS (PendingTreeMerge ptm) =
      case ptm of
        PendingUnionMerge mts        -> traverse_ releaseRef mts
        PendingLevelMerge prs mmt    -> traverse_ releasePER prs
                                     >> traverse_ releaseRef mmt

    releasePER (PreExistingRun         r) = releaseRef r
    releasePER (PreExistingMergingRun mr) = releaseRef mr

{-# SPECIALISE remainingMergeDebt ::
     Ref (MergingTree IO h) -> IO (MergeDebt, NumEntries) #-}
-- | Calculate an upper bound on the merge credits required to complete the
-- merge, i.e. turn the tree into a 'CompletedTreeMerge'. For the recursive
-- calculation, we also return an upper bound on the size of the resulting run.
remainingMergeDebt ::
     (MonadMVar m, PrimMonad m)
  => Ref (MergingTree m h) -> m (MergeDebt, NumEntries)
remainingMergeDebt (DeRef mt) = do
    readMVar (mergeState mt) >>= \case
      CompletedTreeMerge r -> return (MergeDebt 0, Run.size r)
      OngoingTreeMerge mr  -> addDebtOne <$> MR.remainingMergeDebt mr
      PendingTreeMerge ptm -> addDebtOne <$> remainingMergeDebtPendingMerge ptm
  where
    -- An ongoing merge should never have 0 debt, even if the 'MergingRun' in it
    -- says it is completed. We still need to update it to 'CompletedTreeMerge'.
    -- Similarly, a pending merge needs some work to complete it, even if all
    -- its inputs are empty.
    --
    -- Note that we can't use @max 1@, as this would violate the property that
    -- supplying N credits reduces the remaining debt by at least N.
    addDebtOne (MergeDebt !debt, !size) = (MergeDebt (debt + 1), size)

{-# SPECIALISE remainingMergeDebtPendingMerge ::
     PendingMerge IO h -> IO (MergeDebt, NumEntries) #-}
remainingMergeDebtPendingMerge ::
     (MonadMVar m, PrimMonad m)
  => PendingMerge m h -> m (MergeDebt, NumEntries)
remainingMergeDebtPendingMerge (PendingMerge _ prs mts) = do
    -- TODO: optimise to reduce allocations
    debtsPre <- traverse remainingMergeDebtPreExistingRun prs
    debtsChild <- traverse remainingMergeDebt mts
    return (debtOfNestedMerge (debtsPre <> debtsChild))
  where
    remainingMergeDebtPreExistingRun = \case
        PreExistingRun        r  -> return (MergeDebt 0, Run.size r)
        PreExistingMergingRun mr -> MR.remainingMergeDebt mr

debtOfNestedMerge :: Vector (MergeDebt, NumEntries) -> (MergeDebt, NumEntries)
debtOfNestedMerge debts =
    -- complete all children, then one merge of them all (so debt is their size)
    (MergeDebt (c + MR.MergeCredits n), NumEntries n)
  where
    (MergeDebt c, NumEntries n) = foldl' add (MergeDebt 0, NumEntries 0) debts

    add (MergeDebt !d1, NumEntries !n1) (MergeDebt !d2, NumEntries !n2) =
        (MergeDebt (d1 + d2), NumEntries (n1 + n2))
