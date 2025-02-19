-- | An incremental merge of multiple runs, preserving a bracketing structure.
module Database.LSMTree.Internal.MergingTree (
    -- $mergingtrees
    MergingTree (..)
  , newPendingLevelMerge
  , PreExistingRun (..)
  , newPendingUnionMerge
  , isStructurallyEmpty
    -- * Internal state
  , MergingTreeState (..)
  , PendingMerge (..)
  ) where

import           Control.Concurrent.Class.MonadMVar.Strict
import           Control.Monad (filterM)
import           Control.Monad.Class.MonadThrow (MonadMask)
import           Control.Monad.Primitive
import           Control.RefCount
import           Data.Foldable (traverse_)
import           Database.LSMTree.Internal.MergingRun (MergingRun)
import qualified Database.LSMTree.Internal.MergingRun as MR
import           Database.LSMTree.Internal.Run (Run)

-- $mergingtrees Semantically, tables are key-value stores like Haskell's
-- @Map@. Table unions then behave like @Map.unionWith (<>)@. If one of the
-- input tables contains a value at a particular key, the result will also
-- contain it. If multiple tables share that key, the values will be combined
-- monoidally.
--
-- Looking at the implementation, tables are not just key-value pairs, but
-- consist of runs. If each table was just a single run, unioning would involve
-- a run merge similar to the one used for compaction (when a level is full),
-- but with a different merge type 'MergeUnion' that differs semantically:
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
      ![PreExistingRun m h]
      !(Maybe (Ref (MergingTree m h)))

    -- | Each input is the entire content of a table (as a merging tree).
  | PendingUnionMerge
      ![Ref (MergingTree m h)]

data PreExistingRun m h =
    PreExistingRun        !(Ref (Run m h))
  | PreExistingMergingRun !(Ref (MergingRun MR.LevelMergeType m h))

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
newPendingLevelMerge prs mmt = do
    -- There are no interruption points here, and thus provided async exceptions
    -- are masked then there can be no async exceptions here at all.
    mergeTreeState <- case (prs, mmt) of
      ([PreExistingRun r], Nothing) ->
        -- No need to create a pending merge here.
        --
        -- We could do something similar for PreExistingMergingRun, but it's:
        -- * complicated, because of the LevelMergeType/TreeMergeType mismatch.
        -- * unneeded, since that case should never occur. If there is only a
        --   single entry in the list, there can only be one level in the input
        --   table. At level 1 there are no merging runs, so it must be a
        --   PreExistingRun.
        CompletedTreeMerge <$> dupRef r

      _ -> PendingTreeMerge <$>
            (PendingLevelMerge <$> traverse dupPreExistingRun prs
                               <*> dupMaybeMergingTree mmt)

    newMergeTree mergeTreeState
  where
    dupPreExistingRun (PreExistingRun r) =
      PreExistingRun <$> dupRef r
    dupPreExistingRun (PreExistingMergingRun mr) =
      PreExistingMergingRun <$> dupRef mr

    dupMaybeMergingTree :: Maybe (Ref (MergingTree m h))
                        -> m (Maybe (Ref (MergingTree m h)))
    dupMaybeMergingTree Nothing   = return Nothing
    dupMaybeMergingTree (Just mt) = do
      isempty <- isStructurallyEmpty mt
      if isempty
        then return Nothing
        else Just <$> dupRef mt

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
    mts' <- mapM dupRef =<< filterM (fmap not . isStructurallyEmpty) mts
    case mts' of
      [mt] -> return mt
      _    -> newMergeTree (PendingTreeMerge (PendingUnionMerge mts'))

-- | Test if a 'MergingTree' is \"obviously\" empty by virtue of its structure.
-- This is not the same as being empty due to a pending or ongoing merge
-- happening to produce an empty run.
--
isStructurallyEmpty :: MonadMVar m => Ref (MergingTree m h) -> m Bool
isStructurallyEmpty (DeRef MergingTree {mergeState}) =
    isEmpty <$> readMVar mergeState
  where
    isEmpty (PendingTreeMerge (PendingLevelMerge [] Nothing)) = True
    isEmpty (PendingTreeMerge (PendingUnionMerge []))         = True
    isEmpty _                                                 = False
    -- It may also turn out to be useful to consider CompletedTreeMerge with
    -- a zero length runs as empty.

-- | Constructor helper.
newMergeTree ::
     (MonadMVar m, PrimMonad m, MonadMask m)
  => MergingTreeState m h
  -> m (Ref (MergingTree m h))
newMergeTree mergeTreeState = do
    mergeState <- newMVar mergeTreeState
    newRef (finalise mergeState) $ \mergeRefCounter ->
      MergingTree {
        mergeState
      , mergeRefCounter
      }

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

