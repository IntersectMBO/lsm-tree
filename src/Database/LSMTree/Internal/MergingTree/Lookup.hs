module Database.LSMTree.Internal.MergingTree.Lookup (
    LookupTree (..)
  , mapMStrict
  , mkLookupNode
  , buildLookupTree
  , foldLookupTree
  ) where

import           Control.ActionRegistry
import           Control.Concurrent.Class.MonadMVar.Strict
import           Control.Exception (assert)
import           Control.Monad
import           Control.Monad.Class.MonadAsync (Async, MonadAsync)
import qualified Control.Monad.Class.MonadAsync as Async
import           Control.Monad.Class.MonadThrow (MonadMask)
import           Control.Monad.Primitive
import           Control.RefCount
import qualified Data.Vector as V
import qualified Database.LSMTree.Internal.Entry as Entry
import           Database.LSMTree.Internal.Lookup (LookupAcc,
                     ResolveSerialisedValue)
import qualified Database.LSMTree.Internal.MergingRun as MR
import qualified Database.LSMTree.Internal.MergingTree as MT
import           Database.LSMTree.Internal.Run (Run)
import qualified Database.LSMTree.Internal.Vector as V

-- | A simplified representation of the shape of a 'MT.MergingTree'.
data LookupTree a =
    LookupBatch !a
    -- | Use 'mkLookupNode' to construct this.
  | LookupNode !MR.TreeMergeType !(V.Vector (LookupTree a)) -- ^ length 2 or more
  deriving stock (Foldable)

-- | Deriving 'Traversable' leads to functions that are not strict in the
-- elements of the vector of children. This function avoids that issue.
mapMStrict :: Monad m => (a -> m b) -> LookupTree a -> m (LookupTree b)
mapMStrict f = \case
  LookupBatch a  -> LookupBatch <$!> f a
  LookupNode t v -> LookupNode t <$!> V.mapMStrict (mapMStrict f) v

-- | Asserts that the vector is non-empty. Collapses singleton nodes.
mkLookupNode :: MR.TreeMergeType -> V.Vector (LookupTree a) -> LookupTree a
mkLookupNode ty ts
  | assert (not (null ts)) (V.length ts == 1) = V.head ts
  | otherwise                                 = LookupNode ty ts

-- | Combine a tree of accs into a single one, using the 'MR.TreeMergeType' of
-- each node.
{-# SPECIALISE foldLookupTree ::
     ResolveSerialisedValue
  -> LookupTree (Async IO (LookupAcc IO h))
  -> IO (LookupAcc IO h) #-}
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

-- | Requires multiple inputs, all of the same length.
--
-- TODO: do more efficiently on mutable vectors?
mergeLookupAcc ::
     ResolveSerialisedValue
  -> MR.TreeMergeType
  -> V.Vector (LookupAcc m h)
  -> LookupAcc m h
mergeLookupAcc resolve mt accs =
    assert (V.length accs > 1) $
    assert (V.all ((== V.length (V.head accs)) . V.length) accs) $
      foldl1 (V.zipWith updateEntry) accs
  where
    updateEntry Nothing    old        = old
    updateEntry new        Nothing    = new
    updateEntry (Just new) (Just old) = Just (combine new old)

    combine = case mt of
        MR.MergeLevel -> Entry.combine resolve
        MR.MergeUnion -> Entry.combineUnion resolve

-- | Create a 'LookupTree' of batches of runs, e.g. to do lookups on. The
-- entries within each batch are to be combined using 'MR.MergeLevel'.
--
-- Assumes that the merging tree is not 'MT.isStructurallyEmpty'.
--
-- This function duplicates the references to all the tree's runs. These
-- references later need to be released.
{-# SPECIALISE buildLookupTree ::
     ActionRegistry IO
  -> Ref (MT.MergingTree IO h)
  -> IO (LookupTree (V.Vector (Ref (Run IO h)))) #-}
buildLookupTree ::
     (PrimMonad m, MonadMVar m, MonadMask m)
  => ActionRegistry m
  -> Ref (MT.MergingTree m h)
  -> m (LookupTree (V.Vector (Ref (Run m h))))
buildLookupTree reg (DeRef mt) =
    -- we make sure the state is not updated while we look at it, so no runs get
    -- dropped before we duplicated the reference.
    withMVar (MT.mergeState mt) $ \case
      MT.CompletedTreeMerge r ->
        LookupBatch . V.singleton <$!> dupRun r
      MT.OngoingTreeMerge mr -> do
        !rs <- withRollback reg (MR.duplicateRuns mr) (V.mapM_ releaseRef)
        ty <- MR.mergeType mr
        return $ case ty of
          Nothing            -> LookupBatch rs  -- just one run
          Just MR.MergeLevel -> LookupBatch rs  -- combine runs
          Just MR.MergeUnion -> mkLookupNode MR.MergeUnion  -- separate
                                  (LookupBatch . V.singleton <$!> rs)
      MT.PendingTreeMerge (MT.PendingLevelMerge prs Nothing) -> do
        LookupBatch . V.concatMap id <$!>  -- combine runs
          V.mapMStrict duplicatePreExistingRun prs
      MT.PendingTreeMerge (MT.PendingLevelMerge prs (Just tree)) -> do
        !child <- buildLookupTree reg tree
        if V.null prs
          then return child
          else do
            preExisting <- do
              LookupBatch . V.concatMap id <$!>  -- combine runs
                V.mapMStrict duplicatePreExistingRun prs
            return $ mkLookupNode MR.MergeLevel $ V.fromList [preExisting, child]
      MT.PendingTreeMerge (MT.PendingUnionMerge trees) ->
        mkLookupNode MR.MergeUnion <$!> V.mapMStrict (buildLookupTree reg) trees
  where
    dupRun r = withRollback reg (dupRef r) releaseRef

    duplicatePreExistingRun (MT.PreExistingRun r) =
        V.singleton <$!> dupRun r
    duplicatePreExistingRun (MT.PreExistingMergingRun mr) =
        withRollback reg (MR.duplicateRuns mr) (V.mapM_ releaseRef)
