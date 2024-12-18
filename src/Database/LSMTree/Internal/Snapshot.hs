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
  , hardLinkRunFiles
  ) where

import           Control.Concurrent.Class.MonadMVar.Strict
import           Control.Concurrent.Class.MonadSTM (MonadSTM)
import           Control.Monad (void)
import           Control.Monad.Class.MonadST (MonadST)
import           Control.Monad.Class.MonadThrow (MonadMask, MonadThrow)
import           Control.Monad.Primitive (PrimMonad)
import           Control.RefCount
import           Control.TempRegistry
import           Data.Foldable (sequenceA_)
import           Data.Primitive (readMutVar)
import           Data.Primitive.PrimVar
import           Data.Text (Text)
import           Data.Traversable (for)
import qualified Data.Vector as V
import           Database.LSMTree.Internal.Config
import           Database.LSMTree.Internal.Entry
import           Database.LSMTree.Internal.Index (Index,
                     IndexAcc (ResultingIndex, newWithDefaults))
import           Database.LSMTree.Internal.Lookup (ResolveSerialisedValue)
import qualified Database.LSMTree.Internal.Merge as Merge
import           Database.LSMTree.Internal.MergeSchedule
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
import qualified System.FS.API.Lazy as FS
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

-- TODO: revisit if we need three table types.
data SnapshotTableType = SnapNormalTable | SnapMonoidalTable | SnapFullTable
  deriving stock (Show, Eq)

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

{-------------------------------------------------------------------------------
  Levels snapshot format
-------------------------------------------------------------------------------}

newtype SnapLevels r = SnapLevels { getSnapLevels :: V.Vector (SnapLevel r) }
  deriving stock (Show, Eq, Functor, Foldable, Traversable)

data SnapLevel r = SnapLevel {
    snapIncoming     :: !(SnapIncomingRun r)
  , snapResidentRuns :: !(V.Vector r)
  }
  deriving stock (Show, Eq, Functor, Foldable, Traversable)

data SnapIncomingRun r =
    SnapMergingRun !MergePolicyForLevel !NumRuns !NumEntries !UnspentCredits !MergeKnownCompleted !(SnapMergingRunState r)
  | SnapSingleRun !r
  deriving stock (Show, Eq, Functor, Foldable, Traversable)

-- | The total number of unspent credits. This total is used in combination with
-- 'SpentCredits' on snapshot load to restore merging work that was lost when
-- the snapshot was created.
newtype UnspentCredits = UnspentCredits { getUnspentCredits :: Int }
  deriving stock (Show, Eq, Read)

data SnapMergingRunState r =
    SnapCompletedMerge !r
  | SnapOngoingMerge !(V.Vector r) !SpentCredits !Merge.Level
  deriving stock (Show, Eq, Functor, Foldable, Traversable)

-- | The total number of spent credits. This total is used in combination with
-- 'UnspentCedits' on snapshot load to restore merging work that was lost when
-- the snapshot was created.
newtype SpentCredits = SpentCredits { getSpentCredits :: Int }
  deriving stock (Show, Eq, Read)

{-------------------------------------------------------------------------------
  Conversion to levels snapshot format
-------------------------------------------------------------------------------}

{-# SPECIALISE toSnapLevels :: Levels j IO h -> IO (SnapLevels (Ref (Run (ResultingIndex j) IO h))) #-}
toSnapLevels ::
     (PrimMonad m, MonadMVar m)
  => Levels j m h
  -> m (SnapLevels (Ref (Run (ResultingIndex j) m h)))
toSnapLevels levels = SnapLevels <$> V.mapM toSnapLevel levels

{-# SPECIALISE toSnapLevel :: Level j IO h -> IO (SnapLevel (Ref (Run (ResultingIndex j) IO h))) #-}
toSnapLevel ::
     (PrimMonad m, MonadMVar m)
  => Level j m h
  -> m (SnapLevel (Ref (Run (ResultingIndex j) m h)))
toSnapLevel Level{..} = do
    sir <- toSnapIncomingRun incomingRun
    pure (SnapLevel sir residentRuns)

{-# SPECIALISE toSnapIncomingRun :: IncomingRun j IO h -> IO (SnapIncomingRun (Ref (Run (ResultingIndex j) IO h))) #-}
toSnapIncomingRun ::
     (PrimMonad m, MonadMVar m)
  => IncomingRun j m h
  -> m (SnapIncomingRun (Ref (Run (ResultingIndex j) m h)))
toSnapIncomingRun (Single r) = pure (SnapSingleRun r)
-- We need to know how many credits were yet unspent so we can restore merge
-- work on snapshot load. No need to snapshot the contents of totalStepsVar
-- here, since we still start counting from 0 again when loading the snapshot.
toSnapIncomingRun (Merging (DeRef MergingRun {..})) = do
    unspentCredits <- readPrimVar (getUnspentCreditsVar mergeUnspentCredits)
    mergeCompletedCache <- readMutVar mergeKnownCompleted
    smrs <- withMVar mergeState $ \mrs -> toSnapMergingRunState mrs
    pure $
      SnapMergingRun
        mergePolicy
        mergeNumRuns
        mergeNumEntries
        (UnspentCredits unspentCredits)
        mergeCompletedCache
        smrs

{-# SPECIALISE toSnapMergingRunState ::
     MergingRunState j IO h
  -> IO (SnapMergingRunState (Ref (Run (ResultingIndex j) IO h))) #-}
toSnapMergingRunState ::
     PrimMonad m
  => MergingRunState j m h
  -> m (SnapMergingRunState (Ref (Run (ResultingIndex j) m h)))
toSnapMergingRunState (CompletedMerge r) = pure (SnapCompletedMerge r)
-- We need to know how many credits were spent already so we can restore merge
-- work on snapshot load.
toSnapMergingRunState (OngoingMerge rs (SpentCreditsVar spentCreditsVar) m) = do
    spentCredits <- readPrimVar spentCreditsVar
    pure (SnapOngoingMerge rs (SpentCredits spentCredits) (Merge.mergeLevel m))

{-------------------------------------------------------------------------------
  Runs
-------------------------------------------------------------------------------}

{-# SPECIALISE snapshotRuns ::
     TempRegistry IO
  -> NamedSnapshotDir
  -> SnapLevels (Ref (Run i IO h))
  -> IO (SnapLevels RunNumber) #-}
-- | @'snapshotRuns' _ targetDir levels@ creates hard links for all run files
-- associated with the runs in @levels@, and puts the new directory entries in
-- the @targetDir@ directory.
snapshotRuns ::
     (MonadMask m, MonadMVar m)
  => TempRegistry m
  -> NamedSnapshotDir
  -> SnapLevels (Ref (Run i m h))
  -> m (SnapLevels RunNumber)
snapshotRuns reg (NamedSnapshotDir targetDir) levels =
    for levels $ \run@(DeRef Run.Run { Run.runHasFS = hfs,
                                       Run.runHasBlockIO = hbio }) -> do
      let sourcePaths = Run.runFsPaths run
      let targetPaths = sourcePaths { runDir = targetDir }
      hardLinkRunFiles reg hfs hbio sourcePaths targetPaths
      pure (runNumber targetPaths)

{-# SPECIALISE openRuns ::
     Index i
  => TempRegistry IO
  -> HasFS IO h
  -> HasBlockIO IO h
  -> TableConfig
  -> UniqCounter IO
  -> NamedSnapshotDir
  -> ActiveDir
  -> SnapLevels RunNumber
  -> IO (SnapLevels (Ref (Run i IO h))) #-}
-- | @'openRuns' _ _ _ _ uniqCounter sourceDir targetDir levels@ takes all run
-- files that are referenced by @levels@, and hard links them from @sourceDir@
-- into @targetDir@ with new, unique names (using @uniqCounter@). Each set of
-- (hard linked) files that represents a run is opened and verified, returning
-- 'Run's as a result.
openRuns ::
     (Index i, MonadMask m, MonadSTM m, MonadST m, MonadMVar m)
  => TempRegistry m
  -> HasFS m h
  -> HasBlockIO m h
  -> TableConfig
  -> UniqCounter m
  -> NamedSnapshotDir
  -> ActiveDir
  -> SnapLevels RunNumber
  -> m (SnapLevels (Ref (Run i m h)))
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
          hardLinkRunFiles reg hfs hbio sourcePaths targetPaths

          allocateTemp reg
            (Run.openFromDisk hfs hbio caching targetPaths)
            releaseRef
    pure (SnapLevels levels')

{-------------------------------------------------------------------------------
  Opening from levels snapshot format
-------------------------------------------------------------------------------}

{-# SPECIALISE fromSnapLevels ::
     IndexAcc j
  => TempRegistry IO
  -> HasFS IO h
  -> HasBlockIO IO h
  -> TableConfig
  -> UniqCounter IO
  -> ResolveSerialisedValue
  -> ActiveDir
  -> SnapLevels (Ref (Run (ResultingIndex j) IO h))
  -> IO (Levels j IO h)
  #-}
fromSnapLevels ::
     forall j m h. (IndexAcc j, MonadMask m, MonadMVar m, MonadSTM m, MonadST m)
  => TempRegistry m
  -> HasFS m h
  -> HasBlockIO m h
  -> TableConfig
  -> UniqCounter m
  -> ResolveSerialisedValue
  -> ActiveDir
  -> SnapLevels (Ref (Run (ResultingIndex j) m h))
  -> m (Levels j m h)
fromSnapLevels reg hfs hbio conf@TableConfig{..} uc resolve dir (SnapLevels levels) =
    V.iforM levels $ \i -> fromSnapLevel (LevelNo (i+1))
  where
    mkPath = RunFsPaths (getActiveDir dir)

    fromSnapLevel :: LevelNo
                  -> SnapLevel (Ref (Run (ResultingIndex j) m h))
                  -> m (Level j m h)
    fromSnapLevel ln SnapLevel{..} = do
        (unspentCreditsMay, spentCreditsMay, incomingRun) <- fromSnapIncomingRun snapIncoming
        -- When a snapshot is created, merge progress is lost, so we have to
        -- redo merging work here. UnspentCredits and SpentCredits track how
        -- many credits were supplied before the snapshot was taken.
        --
        -- TODO: this use of supplyMergeCredits is leaky! If a merge completes
        -- in supplyMergeCredits, then the resulting run is not tracked in the
        -- registry, and closing the input runs is also not tracked in the
        -- registry. Note, however, that this bit of code is likely to change in
        -- #392.
        let c = maybe 0 getUnspentCredits unspentCreditsMay
              + maybe 0 getSpentCredits spentCreditsMay
        supplyMergeCredits
          (ScaledCredits c)
          (creditThresholdForLevel conf ln)
          incomingRun
        pure Level{
            residentRuns = snapResidentRuns
          , ..
          }
      where
        caching = diskCachePolicyForLevel confDiskCachePolicy ln
        alloc = bloomFilterAllocForLevel conf ln
        newIndex = newWithDefaults

        fromSnapIncomingRun ::
             SnapIncomingRun (Ref (Run (ResultingIndex j) m h))
          -> m (Maybe UnspentCredits, Maybe SpentCredits, IncomingRun j m h)
        fromSnapIncomingRun (SnapMergingRun mpfl nr ne unspentCredits knownCompleted smrs) = do
            (spentCreditsMay, mrs) <- fromSnapMergingRunState smrs
            (Just unspentCredits, spentCreditsMay,) . Merging <$>
              newMergingRun mpfl nr ne knownCompleted mrs
        fromSnapIncomingRun (SnapSingleRun run) =
            pure (Nothing, Nothing, Single run)

        fromSnapMergingRunState ::
             SnapMergingRunState (Ref (Run (ResultingIndex j) m h))
          -> m (Maybe SpentCredits, MergingRunState j m h)
        fromSnapMergingRunState (SnapCompletedMerge run) =
            pure (Nothing, CompletedMerge run)
        fromSnapMergingRunState (SnapOngoingMerge runs spentCredits mergeLast) = do
            -- Initialse the variable with 0. Credits will be re-supplied later,
            -- which will ensure that this variable is updated.
            spentCreditsVar <- SpentCreditsVar <$> newPrimVar 0
            rn <- uniqueToRunNumber <$> incrUniqCounter uc
            mergeMaybe <- allocateMaybeTemp reg
              (Merge.new hfs hbio caching alloc newIndex mergeLast resolve (mkPath rn) runs)
              Merge.abort
            case mergeMaybe of
              Nothing -> error "openLevels: merges can not be empty"
              Just m  -> pure (Just spentCredits, OngoingMerge runs spentCreditsVar m)

{-------------------------------------------------------------------------------
  Hard links
-------------------------------------------------------------------------------}

{-# SPECIALISE hardLinkRunFiles ::
     TempRegistry IO
  -> HasFS IO h
  -> HasBlockIO IO h
  -> RunFsPaths
  -> RunFsPaths
  -> IO () #-}
-- | @'hardLinkRunFiles' _hfs hbio sourcePaths targetPaths@ creates a hard link
-- for each @sourcePaths@ path using the corresponding @targetPaths@ path as the
-- name for the new directory entry.
hardLinkRunFiles ::
     (MonadMask m, MonadMVar m)
  => TempRegistry m
  -> HasFS m h
  -> HasBlockIO m h
  -> RunFsPaths
  -> RunFsPaths
  -> m ()
hardLinkRunFiles reg hfs hbio sourceRunFsPaths targetRunFsPaths = do
    let sourcePaths = pathsForRunFiles sourceRunFsPaths
        targetPaths = pathsForRunFiles targetRunFsPaths
    sequenceA_ (hardLinkTemp <$> sourcePaths <*> targetPaths)
    hardLink hfs hbio (runChecksumsPath sourceRunFsPaths) (runChecksumsPath targetRunFsPaths)
  where
    hardLinkTemp sourcePath targetPath =
        allocateTemp reg
          (hardLink hfs hbio sourcePath targetPath)
          (\_ -> FS.removeFile hfs targetPath)

{-# SPECIALISE hardLink ::
     HasFS IO h
  -> HasBlockIO IO h
  -> FS.FsPath
  -> FS.FsPath
  -> IO () #-}
-- | @'hardLink' hfs hbio source target@ creates a hard link for the @source@
-- path at the @target@ path.
--
-- TODO: as a temporary implementation/hack, this copies file contents instead
-- of creating hard links.
hardLink :: MonadThrow m => HasFS m h -> HasBlockIO m h -> FS.FsPath -> FS.FsPath -> m ()
hardLink hfs _hbio sourcePath targetPath =
    FS.withFile hfs sourcePath FS.ReadMode $ \sourceHandle ->
    FS.withFile hfs targetPath (FS.WriteMode FS.MustBeNew) $ \targetHandle -> do
      -- TODO: this is obviously not creating any hard links, but until we have
      -- functions to create hard links in HasBlockIO, this is the temporary
      -- implementation/hack to "emulate" hard links.
      --
      -- This should /hopefully/ stream using lazy IO, though even if it does
      -- not, it is only a temporary placeholder hack.
      bs <- FS.hGetAll hfs sourceHandle
      void $ FS.hPutAll hfs targetHandle bs
