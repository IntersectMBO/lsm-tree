module Database.LSMTree.Internal.Snapshot (
    -- * Snapshot metadata
    SnapshotMetaData (..)
  , SnapshotLabel (..)
  , SnapshotTableType (..)
    -- * Levels snapshot format
  , SnapLevels (..)
  , SnapLevel (..)
  , SnapIncomingRun (..)
  , UnspentCredits (..)
  , SnapMergingRunState (..)
  , SpentCredits (..)
    -- * Conversion to levels snapshot format
  , toSnapLevels
    -- * Runs
  , snapshotRuns
  , openRuns
    -- * Opening from levels snapshot format
  , fromSnapLevels
    -- * Hard links
  , HardLinkDurable (..)
  , hardLinkRunFiles
  ) where

import           Control.Concurrent.Class.MonadMVar.Strict
import           Control.Concurrent.Class.MonadSTM (MonadSTM)
import           Control.DeepSeq (NFData (..))
import           Control.Monad (when)
import           Control.Monad.Class.MonadST (MonadST)
import           Control.Monad.Class.MonadThrow (MonadMask)
import           Control.Monad.Primitive (PrimMonad)
import           Control.RefCount
import           Control.TempRegistry
import           Data.Foldable (sequenceA_)
import           Data.Primitive.PrimVar
import           Data.Text (Text)
import           Data.Traversable (for)
import qualified Data.Vector as V
import           Database.LSMTree.Internal.Config
import           Database.LSMTree.Internal.Entry
import           Database.LSMTree.Internal.Lookup (ResolveSerialisedValue)
import qualified Database.LSMTree.Internal.Merge as Merge
import           Database.LSMTree.Internal.MergeSchedule
import           Database.LSMTree.Internal.MergingRun (NumRuns (..))
import qualified Database.LSMTree.Internal.MergingRun as MR
import           Database.LSMTree.Internal.Paths (ActiveDir (..),
                     NamedSnapshotDir (..), RunFsPaths (..), pathsForRunFiles,
                     runChecksumsPath)
import           Database.LSMTree.Internal.Run (Run)
import qualified Database.LSMTree.Internal.Run as Run
import           Database.LSMTree.Internal.RunNumber
import           Database.LSMTree.Internal.UniqCounter (UniqCounter,
                     incrUniqCounter, uniqueToRunNumber)
import qualified System.FS.API as FS
import           System.FS.API (HasFS)
import qualified System.FS.BlockIO.API as FS
import           System.FS.BlockIO.API (HasBlockIO)

{-------------------------------------------------------------------------------
  Snapshot metadata
-------------------------------------------------------------------------------}

-- | Custom, user-supplied text that is included in the metadata.
--
-- The main use case for a 'SnapshotLabel' is for the user to supply textual
-- information about the key\/value\/blob type for the table that corresponds to
-- the snapshot. This information is used to dynamically check that a snapshot
-- is opened at the correct key\/value\/blob type.
newtype SnapshotLabel = SnapshotLabel Text
  deriving stock (Show, Eq)
  deriving newtype NFData

-- TODO: revisit if we need three table types.
data SnapshotTableType = SnapNormalTable | SnapMonoidalTable | SnapFullTable
  deriving stock (Show, Eq)

instance NFData SnapshotTableType where
  rnf SnapNormalTable   = ()
  rnf SnapMonoidalTable = ()
  rnf SnapFullTable     = ()

data SnapshotMetaData = SnapshotMetaData {
    -- | See 'SnapshotLabel'.
    --
    -- One could argue that the 'SnapshotName' could be used to to hold this
    -- type information, but the file name of snapshot metadata is not guarded
    -- by a checksum, wherease the contents of the file are. Therefore using the
    -- 'SnapshotLabel' is safer.
    snapMetaLabel     :: !SnapshotLabel
    -- | Whether a table is normal or monoidal.
    --
    -- TODO: if we at some point decide to get rid of the normal vs. monoidal
    -- distinction, we can get rid of this field.
  , snapMetaTableType :: !SnapshotTableType
    -- | The 'TableConfig' for the snapshotted table.
    --
    -- Some of these configuration options can be overridden when a snapshot is
    -- opened: see 'TableConfigOverride'.
  , snapMetaConfig    :: !TableConfig
    -- | The shape of the LSM tree.
  , snapMetaLevels    :: !(SnapLevels RunNumber)
  }
  deriving stock (Show, Eq)

instance NFData SnapshotMetaData where
  rnf (SnapshotMetaData a b c d) = rnf a `seq` rnf b `seq` rnf c `seq` rnf d

{-------------------------------------------------------------------------------
  Levels snapshot format
-------------------------------------------------------------------------------}

newtype SnapLevels r = SnapLevels { getSnapLevels :: V.Vector (SnapLevel r) }
  deriving stock (Show, Eq, Functor, Foldable, Traversable)
  deriving newtype NFData

data SnapLevel r = SnapLevel {
    snapIncoming     :: !(SnapIncomingRun r)
  , snapResidentRuns :: !(V.Vector r)
  }
  deriving stock (Show, Eq, Functor, Foldable, Traversable)

instance NFData r => NFData (SnapLevel r) where
  rnf (SnapLevel a b) = rnf a `seq` rnf b

data SnapIncomingRun r =
    SnapMergingRun !MergePolicyForLevel !NumRuns !NumEntries !UnspentCredits !(SnapMergingRunState r)
  | SnapSingleRun !r
  deriving stock (Show, Eq, Functor, Foldable, Traversable)

instance NFData r => NFData (SnapIncomingRun r) where
  rnf (SnapMergingRun a b c d e) =
      rnf a `seq` rnf b `seq` rnf c `seq` rnf d `seq` rnf e
  rnf (SnapSingleRun a) = rnf a

-- | The total number of unspent credits. This total is used in combination with
-- 'SpentCredits' on snapshot load to restore merging work that was lost when
-- the snapshot was created.
newtype UnspentCredits = UnspentCredits { getUnspentCredits :: Int }
  deriving stock (Show, Eq, Read)
  deriving newtype NFData

data SnapMergingRunState r =
    SnapCompletedMerge !r
  | SnapOngoingMerge !(V.Vector r) !SpentCredits !Merge.Level
  deriving stock (Show, Eq, Functor, Foldable, Traversable)

instance NFData r => NFData (SnapMergingRunState r) where
  rnf (SnapCompletedMerge a)   = rnf a
  rnf (SnapOngoingMerge a b c) = rnf a `seq` rnf b `seq` rnf c

-- | The total number of spent credits. This total is used in combination with
-- 'UnspentCedits' on snapshot load to restore merging work that was lost when
-- the snapshot was created.
newtype SpentCredits = SpentCredits { getSpentCredits :: Int }
  deriving stock (Show, Eq, Read)
  deriving newtype NFData

{-------------------------------------------------------------------------------
  Conversion to levels snapshot format
-------------------------------------------------------------------------------}

{-# SPECIALISE toSnapLevels :: Levels IO h -> IO (SnapLevels (Ref (Run IO h))) #-}
toSnapLevels ::
     (PrimMonad m, MonadMVar m)
  => Levels m h
  -> m (SnapLevels (Ref (Run m h)))
toSnapLevels levels = SnapLevels <$> V.mapM toSnapLevel levels

{-# SPECIALISE toSnapLevel :: Level IO h -> IO (SnapLevel (Ref (Run IO h))) #-}
toSnapLevel ::
     (PrimMonad m, MonadMVar m)
  => Level m h
  -> m (SnapLevel (Ref (Run m h)))
toSnapLevel Level{..} = do
    sir <- toSnapIncomingRun incomingRun
    pure (SnapLevel sir residentRuns)

{-# SPECIALISE toSnapIncomingRun :: IncomingRun IO h -> IO (SnapIncomingRun (Ref (Run IO h))) #-}
toSnapIncomingRun ::
     (PrimMonad m, MonadMVar m)
  => IncomingRun m h
  -> m (SnapIncomingRun (Ref (Run m h)))
toSnapIncomingRun (Single r) = pure (SnapSingleRun r)
-- We need to know how many credits were yet unspent so we can restore merge
-- work on snapshot load. No need to snapshot the contents of totalStepsVar
-- here, since we still start counting from 0 again when loading the snapshot.
toSnapIncomingRun (Merging (DeRef MR.MergingRun {..})) = do
    unspentCredits <- readPrimVar (MR.getUnspentCreditsVar mergeUnspentCredits)
    smrs <- withMVar mergeState $ \mrs -> toSnapMergingRunState mrs
    pure $
      SnapMergingRun
        mergePolicy
        mergeNumRuns
        mergeNumEntries
        (UnspentCredits unspentCredits)
        smrs

{-# SPECIALISE toSnapMergingRunState ::
     MR.MergingRunState IO h
  -> IO (SnapMergingRunState (Ref (Run IO h))) #-}
toSnapMergingRunState ::
     PrimMonad m
  => MR.MergingRunState m h
  -> m (SnapMergingRunState (Ref (Run m h)))
toSnapMergingRunState (MR.CompletedMerge r) = pure (SnapCompletedMerge r)
-- We need to know how many credits were spent already so we can restore merge
-- work on snapshot load.
toSnapMergingRunState (MR.OngoingMerge rs (MR.SpentCreditsVar spentCreditsVar) m) = do
    spentCredits <- readPrimVar spentCreditsVar
    pure (SnapOngoingMerge rs (SpentCredits spentCredits) (Merge.mergeLevel m))

{-------------------------------------------------------------------------------
  Runs
-------------------------------------------------------------------------------}

{-# SPECIALISE snapshotRuns ::
     TempRegistry IO
  -> HasBlockIO IO h
  -> NamedSnapshotDir
  -> SnapLevels (Ref (Run IO h))
  -> IO (SnapLevels RunNumber) #-}
-- | @'snapshotRuns' _ _ targetDir levels@ creates hard links for all run files
-- associated with the runs in @levels@, and puts the new directory entries in
-- the @targetDir@ directory. The hard links and the @targetDir@ are made
-- durable on disk.
snapshotRuns ::
     (MonadMask m, MonadMVar m)
  => TempRegistry m
  -> HasBlockIO m h
  -> NamedSnapshotDir
  -> SnapLevels (Ref (Run m h))
  -> m (SnapLevels RunNumber)
snapshotRuns reg hbio0 (NamedSnapshotDir targetDir) levels = do
    levels' <-
      for levels $ \run@(DeRef Run.Run {
          Run.runHasFS = hfs,
          Run.runHasBlockIO = hbio
        }) -> do
          let sourcePaths = Run.runFsPaths run
          let targetPaths = sourcePaths { runDir = targetDir }
          hardLinkRunFiles reg hfs hbio HardLinkDurable sourcePaths targetPaths
          pure (runNumber targetPaths)
    FS.synchroniseDirectory hbio0 targetDir
    pure levels'

{-# SPECIALISE openRuns ::
     TempRegistry IO
  -> HasFS IO h
  -> HasBlockIO IO h
  -> TableConfig
  -> UniqCounter IO
  -> NamedSnapshotDir
  -> ActiveDir
  -> SnapLevels RunNumber
  -> IO (SnapLevels (Ref (Run IO h))) #-}
-- | @'openRuns' _ _ _ _ uniqCounter sourceDir targetDir levels@ takes all run
-- files that are referenced by @levels@, and hard links them from @sourceDir@
-- into @targetDir@ with new, unique names (using @uniqCounter@). Each set of
-- (hard linked) files that represents a run is opened and verified, returning
-- 'Run's as a result.
openRuns ::
     (MonadMask m, MonadSTM m, MonadST m, MonadMVar m)
  => TempRegistry m
  -> HasFS m h
  -> HasBlockIO m h
  -> TableConfig
  -> UniqCounter m
  -> NamedSnapshotDir
  -> ActiveDir
  -> SnapLevels RunNumber
  -> m (SnapLevels (Ref (Run m h)))
openRuns
  reg hfs hbio TableConfig{..} uc
  (NamedSnapshotDir sourceDir) (ActiveDir targetDir) (SnapLevels levels) = do
    levels' <-
      V.iforM levels $ \i level ->
        let ln = LevelNo (i+1) in
        let caching = diskCachePolicyForLevel confDiskCachePolicy ln in
        for level $ \runNum -> do
          let sourcePaths = RunFsPaths sourceDir runNum
          runNum' <- uniqueToRunNumber <$> incrUniqCounter uc
          let targetPaths = RunFsPaths targetDir runNum'
          hardLinkRunFiles reg hfs hbio NoHardLinkDurable sourcePaths targetPaths

          allocateTemp reg
            (Run.openFromDisk hfs hbio caching targetPaths)
            releaseRef
    pure (SnapLevels levels')

{-------------------------------------------------------------------------------
  Opening from levels snapshot format
-------------------------------------------------------------------------------}

{-# SPECIALISE fromSnapLevels ::
     TempRegistry IO
  -> HasFS IO h
  -> HasBlockIO IO h
  -> TableConfig
  -> UniqCounter IO
  -> ResolveSerialisedValue
  -> ActiveDir
  -> SnapLevels (Ref (Run IO h))
  -> IO (Levels IO h)
  #-}
fromSnapLevels ::
     forall m h. (MonadMask m, MonadMVar m, MonadSTM m, MonadST m)
  => TempRegistry m
  -> HasFS m h
  -> HasBlockIO m h
  -> TableConfig
  -> UniqCounter m
  -> ResolveSerialisedValue
  -> ActiveDir
  -> SnapLevels (Ref (Run m h))
  -> m (Levels m h)
fromSnapLevels reg hfs hbio conf@TableConfig{..} uc resolve dir (SnapLevels levels) =
    V.iforM levels $ \i -> fromSnapLevel (LevelNo (i+1))
  where
    mkPath = RunFsPaths (getActiveDir dir)

    fromSnapLevel :: LevelNo -> SnapLevel (Ref (Run m h)) -> m (Level m h)
    fromSnapLevel ln SnapLevel{..} = do
        incomingRun <- fromSnapIncomingRun snapIncoming
        pure Level {
            incomingRun
          , residentRuns = snapResidentRuns
          }
      where
        caching = diskCachePolicyForLevel confDiskCachePolicy ln
        alloc = bloomFilterAllocForLevel conf ln

        fromSnapIncomingRun ::
             SnapIncomingRun (Ref (Run m h))
          -> m (IncomingRun m h)
        fromSnapIncomingRun (SnapSingleRun run) =
            pure (Single run)
        fromSnapIncomingRun (SnapMergingRun mpfl nr ne unspentCredits smrs) = do
            Merging <$> case smrs of
              SnapCompletedMerge run ->
                allocateTemp reg (MR.newCompleted mpfl nr ne run) releaseRef

              SnapOngoingMerge runs spentCredits lvl -> do
                rn <- uniqueToRunNumber <$> incrUniqCounter uc
                mr <- allocateTemp reg
                  (MR.new hfs hbio resolve caching alloc lvl mpfl (mkPath rn) runs)
                  releaseRef

                -- When a snapshot is created, merge progress is lost, so we
                -- have to redo merging work here. UnspentCredits and
                -- SpentCredits track how many credits were supplied before the
                -- snapshot was taken.
                let c = getUnspentCredits unspentCredits
                      + getSpentCredits spentCredits
                MR.supplyCredits (MR.Credits c) (creditThresholdForLevel conf ln) mr
                return mr

{-------------------------------------------------------------------------------
  Hard links
-------------------------------------------------------------------------------}

data HardLinkDurable = HardLinkDurable | NoHardLinkDurable
  deriving stock Eq

{-# SPECIALISE hardLinkRunFiles ::
     TempRegistry IO
  -> HasFS IO h
  -> HasBlockIO IO h
  -> HardLinkDurable
  -> RunFsPaths
  -> RunFsPaths
  -> IO () #-}
-- | @'hardLinkRunFiles' _ _ _ dur sourcePaths targetPaths@ creates a hard link
-- for each @sourcePaths@ path using the corresponding @targetPaths@ path as the
-- name for the new directory entry. If @dur == HardLinkDurabl@, the links will
-- also be made durable on disk.
hardLinkRunFiles ::
     (MonadMask m, MonadMVar m)
  => TempRegistry m
  -> HasFS m h
  -> HasBlockIO m h
  -> HardLinkDurable
  -> RunFsPaths
  -> RunFsPaths
  -> m ()
hardLinkRunFiles reg hfs hbio dur sourceRunFsPaths targetRunFsPaths = do
    let sourcePaths = pathsForRunFiles sourceRunFsPaths
        targetPaths = pathsForRunFiles targetRunFsPaths
    sequenceA_ (hardLinkTemp <$> sourcePaths <*> targetPaths)
    hardLinkTemp (runChecksumsPath sourceRunFsPaths) (runChecksumsPath targetRunFsPaths)
  where
    hardLinkTemp sourcePath targetPath = do
        allocateTemp reg
          (FS.createHardLink hbio sourcePath targetPath)
          (\_ -> FS.removeFile hfs targetPath)
        when (dur == HardLinkDurable) $
          FS.synchroniseFile hfs hbio targetPath
