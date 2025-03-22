-- Definitions for override table config options.
module Database.LSMTree.Internal.Config.Override (
    -- $override-policy

    -- * Override disk cache policy
    overrideDiskCachePolicy
  ) where

import qualified Data.Vector as V
import           Database.LSMTree.Internal.Config
import           Database.LSMTree.Internal.Run
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

-- | Override the disk cache policy that is stored in snapshot metadata.
--
-- Tables opened from the new 'SnapshotMetaData' will use the new value for the
-- disk cache policy.
overrideDiskCachePolicy :: DiskCachePolicy -> SnapshotMetaData -> SnapshotMetaData
overrideDiskCachePolicy = override

-- | This class is only here so that we can recursively call 'override' on all
-- fields of a datatype, instead of having to invent a new name for each type
-- that the function is called on such as 'overrideTableConfig',
-- 'overrideSnapshotRun', etc.
class Override o a where
  override :: o -> a -> a

instance Override DiskCachePolicy SnapshotMetaData where
  override dcp SnapshotMetaData{..} = SnapshotMetaData {
        snapMetaConfig = override dcp snapMetaConfig
      , snapMetaLevels = override dcp snapMetaLevels
      , snapMergingTree =
          let rdc = diskCachePolicyForLevel dcp UnionLevel
          in  override rdc snapMergingTree
      , ..
      }

instance Override DiskCachePolicy TableConfig where
  override dcp conf = conf {
        confDiskCachePolicy = dcp
      }

instance Override DiskCachePolicy (SnapLevels SnapshotRun) where
  override dcp (SnapLevels vec) = SnapLevels $ V.imap go vec
    where
      go (LevelNo . (+1) -> ln) x =
          let rdc = diskCachePolicyForLevel dcp (RegularLevel ln)
          in  override rdc x

instance Override RunDataCaching (SnapLevel SnapshotRun) where
  override rdc SnapLevel{..} =  SnapLevel {
        snapIncoming = override rdc snapIncoming
      , snapResidentRuns = override rdc snapResidentRuns
      }

instance Override RunDataCaching (SnapIncomingRun SnapshotRun) where
  override rdc = \case
      SnapIncomingSingleRun sr -> SnapIncomingSingleRun $ override rdc sr
      SnapIncomingMergingRun mpfl nd nc smr ->
        SnapIncomingMergingRun mpfl nd nc (override rdc smr)

instance Override o a => Override o (V.Vector a) where
  override o vec = V.map (override o) vec

instance Override RunDataCaching (SnapMergingRun t SnapshotRun) where
  override rdc = \case
      SnapCompletedMerge md sr ->
        SnapCompletedMerge md (override rdc sr)
      SnapOngoingMerge rp mc srs t ->
        SnapOngoingMerge (override rdc rp) mc (override rdc srs) t

instance Override RunDataCaching RunParams where
  override rdc RunParams{..} = RunParams {
        runParamCaching = rdc
      , ..
      }

instance Override RunDataCaching SnapshotRun where
  override rdc SnapshotRun{..} = SnapshotRun {
        snapRunCaching = rdc
      , ..
      }

instance Override o a => Override o (Maybe a) where
  override o (Just x) = Just (override o x)
  override _ Nothing  = Nothing

instance Override RunDataCaching (SnapMergingTree SnapshotRun) where
  override rdc (SnapMergingTree smts) = SnapMergingTree (override rdc smts)

instance Override RunDataCaching (SnapMergingTreeState SnapshotRun) where
  override rdc = \case
      SnapCompletedTreeMerge sr -> SnapCompletedTreeMerge (override rdc sr)
      SnapPendingTreeMerge spm -> SnapPendingTreeMerge (override rdc spm)
      SnapOngoingTreeMerge smr -> SnapOngoingTreeMerge (override rdc smr)

instance Override RunDataCaching (SnapPendingMerge SnapshotRun) where
  override rdc = \case
      SnapPendingLevelMerge spers msmt ->
        SnapPendingLevelMerge (map (override rdc) spers) (override rdc msmt)
      SnapPendingUnionMerge smts ->
        SnapPendingUnionMerge (map (override rdc) smts)

instance Override RunDataCaching (SnapPreExistingRun SnapshotRun) where
  override rdc = \case
      SnapPreExistingRun sr -> SnapPreExistingRun (override rdc sr)
      SnapPreExistingMergingRun smr -> SnapPreExistingMergingRun (override rdc smr)
