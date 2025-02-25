module Database.LSMTree.Internal.MergingTree.Lookup (
    LookupTree (..)
  , buildLookupTree
  , releaseLookupTree
  , foldLookupTree
  ) where

import           Control.ActionRegistry
import           Control.Concurrent.Class.MonadMVar.Strict
import           Control.Exception (assert)
import           Control.Monad.Class.MonadAsync (Async, MonadAsync)
import qualified Control.Monad.Class.MonadAsync as Async
import           Control.Monad.Class.MonadThrow (MonadMask)
import           Control.Monad.Primitive
import           Control.RefCount
import           Data.Foldable (traverse_)
import qualified Data.Vector as V
import qualified Database.LSMTree.Internal.Entry as Entry
import           Database.LSMTree.Internal.Lookup (LookupAcc,
                     ResolveSerialisedValue)
import qualified Database.LSMTree.Internal.Merge as Merge
import           Database.LSMTree.Internal.MergingRun (TreeMergeType (..))
import qualified Database.LSMTree.Internal.MergingRun as MR
import qualified Database.LSMTree.Internal.MergingTree as MT
import           Database.LSMTree.Internal.Run (Run)

data LookupTree a =
    LookupBatch a
  | LookupNode TreeMergeType [LookupTree a]  -- TODO: use Vector? NonEmpty?
  deriving stock (Foldable, Functor, Traversable)

foldLookupTree ::
     MonadAsync m
  => ResolveSerialisedValue
  -> LookupTree (Async m (LookupAcc m h))
  -> m (LookupAcc m h)
foldLookupTree resolve = \case
    LookupBatch batch ->
      Async.wait batch
    LookupNode mt children ->
      mergeLookupAcc resolve mt <$> traverse (foldLookupTree resolve) children

-- TODO: do more efficiently on mutable vectors?
mergeLookupAcc ::
     ResolveSerialisedValue
  -> MR.TreeMergeType
  -> [LookupAcc m h]
  -> LookupAcc m h
mergeLookupAcc resolve mt = foldl1 (V.zipWith updateEntry)
  where
    updateEntry Nothing    old        = old
    updateEntry new        Nothing    = new
    updateEntry (Just new) (Just old) = Just (combine new old)

    combine = case mt of
        MR.MergeLevel -> Entry.combine resolve
        MR.MergeUnion -> Entry.combineUnion resolve

-- | Create a 'LookupTree', duplicating the references to all the tree's runs.
--
-- Assumes that the merging tree is not 'MT.isStructurallyEmpty'.
--
-- TODO: SPECIALISE
-- TODO: use API of MergingRun etc., not internal state directly
buildLookupTree ::
     (PrimMonad m, MonadMVar m, MonadMask m)
  => ActionRegistry m
  -> Ref (MT.MergingTree m h)
  -> m (LookupTree (V.Vector (Ref (Run m h))))
buildLookupTree reg = go
  where
    go (DeRef mt) =
        -- we make sure the state is not updated while we look at it, so no runs
        -- get dropped before we duplicated the reference.
        withMVar (MT.mergeState mt) $ \case
          MT.CompletedTreeMerge r ->
            LookupBatch . V.singleton <$> dupRun r
          MT.OngoingTreeMerge (DeRef mr) ->
            withMVar (MR.mergeState mr) $ \case
              MR.CompletedMerge r ->
                LookupBatch . V.singleton <$> dupRun r
              MR.OngoingMerge inputs merge ->
                case Merge.mergeType merge of
                  MR.MergeLevel ->
                    LookupBatch <$> traverse dupRun inputs  -- combine
                  MR.MergeUnion ->
                    fmap (LookupNode Merge.MergeUnion) $
                      fmap (map (LookupBatch . V.singleton)) $
                        traverse dupRun (V.toList inputs)
          MT.PendingTreeMerge (MT.PendingLevelMerge prs tree) -> do
            preExisting <- do
              -- combine
              runs <- V.concat . V.toList <$> traverse flattenPreExistingRun prs
              LookupBatch <$> traverse dupRun runs
            case tree of
              Nothing -> return preExisting
              Just t -> do
                childTree <- go t
                return $ LookupNode MR.MergeLevel [preExisting, childTree]
          MT.PendingTreeMerge (MT.PendingUnionMerge trees) ->
            assert (not (null trees)) $  -- not isStructurallyEmpty
              LookupNode MR.MergeUnion <$> traverse go (V.toList trees)

    dupRun r = withRollback reg (dupRef r) releaseRef

releaseLookupTree ::
     (PrimMonad m, MonadMask m)
  => ActionRegistry m
  -> LookupTree (V.Vector (Ref (Run m h)))
  -> m ()
releaseLookupTree reg = traverse_ (traverse_ (delayedCommit reg . releaseRef))

flattenPreExistingRun ::
     MonadMVar m
  => MT.PreExistingRun m h
  -> m (V.Vector (Ref (Run m h)))
flattenPreExistingRun (MT.PreExistingRun r)         = return $ V.singleton r
flattenPreExistingRun (MT.PreExistingMergingRun mr) = flattenMergingRun mr

flattenMergingRun ::
     MonadMVar m
  => Ref (MR.MergingRun MR.LevelMergeType m h)
  -> m (V.Vector (Ref (Run m h)))
flattenMergingRun (DeRef mr) =
    withMVar (MR.mergeState mr) $ \case
      MR.CompletedMerge r  -> return $ V.singleton r
      MR.OngoingMerge rs _ -> return rs
