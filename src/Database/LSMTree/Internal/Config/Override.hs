-- Definitions for override table config options.
module Database.LSMTree.Internal.Config.Override (
    -- $override-policy

    -- * Override disk cache policy
    OverrideDiskCachePolicy (..)
  , overrideDiskCachePolicy
  ) where

import qualified Data.Vector as V
import           Database.LSMTree.Internal.Config
import           Database.LSMTree.Internal.MergeSchedule (MergePolicyForLevel,
                     NominalCredits, NominalDebt)
import           Database.LSMTree.Internal.MergingRun (LevelMergeType,
                     MergeCredits, MergeDebt, TreeMergeType)
import           Database.LSMTree.Internal.Run
import           Database.LSMTree.Internal.RunAcc (RunBloomFilterAlloc)
import           Database.LSMTree.Internal.RunNumber (RunNumber)
import           Database.LSMTree.Internal.Snapshot

-- $override-policy
--
-- === Limitations
--
-- Overriding config options should in many cases be possible as long as there
-- is a mitigation strategy to ensure that a change in options does not cause
-- the database state of a table to become inconsistent. But what does this
-- strategy look like? And if we allow a config option to be overridden on a
-- live table (one that a user has access to), how should we apply these new
-- options to shared data like merging runs? Moreover, would we answer these
-- questions differently for each type of config option?
--
-- For now, it seems to be the most straightforward to limit the config options
-- we allow to be overridden, and that we only change the config options
-- offline. That is, we override the config option just before opening a
-- snapshot from disk. At that point, there is no sharing because the table is
-- not live yet, which simplifies how changing a config option is handled.
--
-- Another complicating factor is that we have thought about the possibility of
-- restoring sharing of ongoing merges between live tables and newly opened
-- snapshots. At that point, we run into the same challenges again... But for
-- now, changing only the disk cache policy offline should work fine.

{-------------------------------------------------------------------------------
  Override disk cache policy
-------------------------------------------------------------------------------}

-- | Override the 'DiskCachePolicy'
data OverrideDiskCachePolicy =
    OverrideDiskCachePolicy DiskCachePolicy
  | NoOverrideDiskCachePolicy
  deriving stock (Show, Eq)

-- | Override the disk cache policy that is stored in snapshot metadata.
--
-- Tables opened from the new 'SnapshotMetaData' will use the new value for the
-- disk cache policy.
overrideDiskCachePolicy :: OverrideDiskCachePolicy -> SnapshotMetaData -> SnapshotMetaData
overrideDiskCachePolicy (OverrideDiskCachePolicy dcp) = override dcp
overrideDiskCachePolicy NoOverrideDiskCachePolicy     = id

-- | This class is only here so that we can recursively call 'override' on all
-- fields of a datatype, instead of having to invent a new name for each type
-- that the function is called on such as 'overrideTableConfig',
-- 'overrideSnapshotRun', etc.
class Override o a where
  override :: o -> a -> a

-- NOTE: the instances below explicitly pattern match on the types of
-- constructor fields. This makes the code more verbose, but it also makes the
-- code a little more future proof. It should help us not to forget to update
-- the instances when new fields are added or existing fields change. In
-- particular, if anything changes about the constructor or its fields (and
-- their types), then we will see a compiler error, and then we are forced to
-- look at the code and make adjustments.

instance Override DiskCachePolicy SnapshotMetaData where
  override dcp
    (SnapshotMetaData (sl :: SnapshotLabel)(stt :: SnapshotTableType)
      (tc :: TableConfig) (rn :: RunNumber)
      (sls :: (SnapLevels SnapshotRun))
      (smt :: (Maybe (SnapMergingTree SnapshotRun))))
    = SnapshotMetaData sl stt (override dcp tc) rn (override dcp sls) $
        let rdc = diskCachePolicyForLevel dcp UnionLevel
        in  fmap (override rdc) smt

instance Override DiskCachePolicy TableConfig where
  override dcp
    (TableConfig (cmp :: MergePolicy) (csz :: SizeRatio)
      (cwba :: WriteBufferAlloc) (cbfa :: BloomFilterAlloc)
      (cfit :: FencePointerIndexType) (_dcp :: DiskCachePolicy)
      (cfs :: MergeSchedule))
    = TableConfig cmp csz cwba cbfa cfit dcp cfs

instance Override DiskCachePolicy (SnapLevels SnapshotRun) where
  override dcp (SnapLevels (vec :: V.Vector (SnapLevel SnapshotRun))) =
      SnapLevels $ V.imap go vec
    where
      go (LevelNo . (+1) -> ln) (x :: SnapLevel SnapshotRun) =
          let rdc = diskCachePolicyForLevel dcp (RegularLevel ln)
          in  override rdc x

instance Override RunDataCaching (SnapLevel SnapshotRun) where
  override rdc
    (SnapLevel (sir :: SnapIncomingRun SnapshotRun) (srs :: V.Vector SnapshotRun))
    = SnapLevel (override rdc sir) (V.map (override rdc) srs)

instance Override RunDataCaching (SnapIncomingRun SnapshotRun) where
  override rdc = \case
      SnapIncomingSingleRun (sr :: SnapshotRun) ->
        SnapIncomingSingleRun $ override rdc sr
      SnapIncomingMergingRun
        (mpfl :: MergePolicyForLevel) (nd :: NominalDebt)
        (nc :: NominalCredits) (smr :: SnapMergingRun LevelMergeType SnapshotRun) ->
          SnapIncomingMergingRun mpfl nd nc (override rdc smr)

instance Override RunDataCaching (SnapMergingRun t SnapshotRun) where
  override rdc = \case
      SnapCompletedMerge (md :: MergeDebt) (sr :: SnapshotRun) ->
        SnapCompletedMerge md (override rdc sr)
      SnapOngoingMerge
        (rp :: RunParams) (mc :: MergeCredits)
        (srs :: V.Vector SnapshotRun) (t :: t) ->
          SnapOngoingMerge (override rdc rp) mc (V.map (override rdc) srs) t

instance Override RunDataCaching RunParams where
  override rdc
    (RunParams (_rdc :: RunDataCaching) (rbfa :: RunBloomFilterAlloc) (it :: IndexType))
    = RunParams rdc rbfa it

instance Override RunDataCaching SnapshotRun where
  override rdc
    (SnapshotRun (rn :: RunNumber) (_rdc :: RunDataCaching) (it ::IndexType))
    = SnapshotRun rn rdc it

instance Override RunDataCaching (SnapMergingTree SnapshotRun) where
  override rdc (SnapMergingTree (smts ::  SnapMergingTreeState SnapshotRun))
    = SnapMergingTree (override rdc smts)

instance Override RunDataCaching (SnapMergingTreeState SnapshotRun) where
  override rdc = \case
      SnapCompletedTreeMerge (sr :: SnapshotRun) ->
        SnapCompletedTreeMerge (override rdc sr)
      SnapPendingTreeMerge (spm :: SnapPendingMerge SnapshotRun) ->
        SnapPendingTreeMerge (override rdc spm)
      SnapOngoingTreeMerge (smr :: SnapMergingRun TreeMergeType SnapshotRun) ->
        SnapOngoingTreeMerge (override rdc smr)

instance Override RunDataCaching (SnapPendingMerge SnapshotRun) where
  override rdc = \case
      SnapPendingLevelMerge
        (spers :: [SnapPreExistingRun SnapshotRun])
        (msmt :: Maybe (SnapMergingTree SnapshotRun)) ->
          SnapPendingLevelMerge (fmap (override rdc) spers) (fmap (override rdc) msmt)
      SnapPendingUnionMerge (smts :: [SnapMergingTree SnapshotRun]) ->
        SnapPendingUnionMerge (fmap (override rdc) smts)

instance Override RunDataCaching (SnapPreExistingRun SnapshotRun) where
  override rdc = \case
      SnapPreExistingRun (sr :: SnapshotRun) -> SnapPreExistingRun (override rdc sr)
      SnapPreExistingMergingRun (smr :: SnapMergingRun LevelMergeType SnapshotRun) ->
        SnapPreExistingMergingRun (override rdc smr)
