module Database.LSMTree.Internal.Snapshot (
    -- * Snapshot metadata
    SnapshotMetaData (..)
  , SnapshotLabel (..)
  , SnapshotTableType (..)
    -- * Levels snapshot format
  , SnapLevels (..)
  , SnapLevel (..)
  , SnapIncomingRun (..)
  , SuppliedCredits (..)
  , SnapMergingRunState (..)
    -- * Conversion to levels snapshot format
  , toSnapLevels
    -- * Write buffer
  , snapshotWriteBuffer
  , openWriteBuffer
    -- * Runs
  , snapshotRuns
  , openRuns
  , releaseRuns
    -- * Opening from levels snapshot format
  , fromSnapLevels
    -- * Hard links
  , hardLinkRunFiles
  ) where

import           Control.ActionRegistry
import           Control.Concurrent.Class.MonadMVar.Strict
import           Control.Concurrent.Class.MonadSTM (MonadSTM)
import           Control.DeepSeq (NFData (..))
import           Control.Exception (assert)
import           Control.Monad (void)
import           Control.Monad.Class.MonadST (MonadST)
import           Control.Monad.Class.MonadThrow (MonadMask)
import           Control.Monad.Primitive (PrimMonad)
import           Control.RefCount
import           Control.Tracer (Tracer, nullTracer)
import           Data.Foldable (sequenceA_, traverse_)
import           Data.Text (Text)
import           Data.Traversable (for)
import qualified Data.Vector as V
import           Database.LSMTree.Internal.Config
import           Database.LSMTree.Internal.CRC32C (checkCRC)
import qualified Database.LSMTree.Internal.CRC32C as CRC
import           Database.LSMTree.Internal.Entry
import           Database.LSMTree.Internal.Lookup (ResolveSerialisedValue)
import qualified Database.LSMTree.Internal.Merge as Merge
import           Database.LSMTree.Internal.MergeSchedule
import           Database.LSMTree.Internal.MergingRun (NumRuns (..))
import qualified Database.LSMTree.Internal.MergingRun as MR
import           Database.LSMTree.Internal.Paths (ActiveDir (..), ForBlob (..),
                     ForKOps (..), NamedSnapshotDir (..), RunFsPaths (..),
                     WriteBufferFsPaths (..),
                     fromChecksumsFileForWriteBufferFiles, pathsForRunFiles,
                     runChecksumsPath, writeBufferBlobPath,
                     writeBufferChecksumsPath, writeBufferKOpsPath)
import           Database.LSMTree.Internal.Run (Run)
import qualified Database.LSMTree.Internal.Run as Run
import           Database.LSMTree.Internal.RunNumber
import           Database.LSMTree.Internal.UniqCounter (UniqCounter,
                     incrUniqCounter, uniqueToInt, uniqueToRunNumber)
import           Database.LSMTree.Internal.WriteBuffer (WriteBuffer)
import           Database.LSMTree.Internal.WriteBufferBlobs (WriteBufferBlobs)
import qualified Database.LSMTree.Internal.WriteBufferBlobs as WBB
import qualified Database.LSMTree.Internal.WriteBufferReader as WBR
import qualified Database.LSMTree.Internal.WriteBufferWriter as WBW
import qualified System.FS.API as FS
import           System.FS.API (HasFS, (<.>), (</>))
import qualified System.FS.API.Lazy as FSL
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
  deriving stock (Eq, Show)

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
    -- | The write buffer.
  , snapWriteBuffer   :: !RunNumber
    -- | The shape of the levels of the LSM tree.
  , snapMetaLevels    :: !(SnapLevels RunNumber)
  }
  deriving stock Eq

instance NFData SnapshotMetaData where
  rnf (SnapshotMetaData a b c d e) = rnf a `seq` rnf b `seq` rnf c `seq` rnf d `seq` rnf e

{-------------------------------------------------------------------------------
  Levels snapshot format
-------------------------------------------------------------------------------}

newtype SnapLevels r = SnapLevels { getSnapLevels :: V.Vector (SnapLevel r) }
  deriving stock (Eq, Functor, Foldable, Traversable)
  deriving newtype NFData

data SnapLevel r = SnapLevel {
    snapIncoming     :: !(SnapIncomingRun r)
  , snapResidentRuns :: !(V.Vector r)
  }
  deriving stock (Eq, Functor, Foldable, Traversable)

instance NFData r => NFData (SnapLevel r) where
  rnf (SnapLevel a b) = rnf a `seq` rnf b

data SnapIncomingRun r =
    SnapMergingRun !MergePolicyForLevel
                   !NumRuns
                   !NumEntries
                   !SuppliedCredits
                   !(SnapMergingRunState MR.LevelMergeType r)
  | SnapSingleRun !r
  deriving stock (Eq, Functor, Foldable, Traversable)

instance NFData r => NFData (SnapIncomingRun r) where
  rnf (SnapMergingRun a b c d e) =
      rnf a `seq` rnf b `seq` rnf c `seq` rnf d `seq` rnf e
  rnf (SnapSingleRun a) = rnf a

-- | The total number of supplied credits. This total is used on snapshot load
-- to restore merging work that was lost when the snapshot was created.
newtype SuppliedCredits = SuppliedCredits { getSuppliedCredits :: Int }
  deriving stock (Eq, Read)
  deriving newtype NFData

data SnapMergingRunState t r =
    SnapCompletedMerge !r
  | SnapOngoingMerge !(V.Vector r) !t
  deriving stock (Eq, Functor, Foldable, Traversable)

instance (NFData t, NFData r) => NFData (SnapMergingRunState t r) where
  rnf (SnapCompletedMerge a) = rnf a
  rnf (SnapOngoingMerge a b) = rnf a `seq` rnf b

{-------------------------------------------------------------------------------
  Conversion to levels snapshot format
-------------------------------------------------------------------------------}

--TODO: probably generally all the Ref (Run _) here ought to be fresh
-- references, created as we snapshot the levels, so that the runs don't
-- disappear under our feet during the process of making the snapshot durable.
-- At minimum the volatile runs are the inputs to merging runs, but it may be
-- simpler to duplicate them all, and release them all at the end.

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
toSnapIncomingRun (Merging mergePolicy _mergeNominalDebt _mergeNominalCreditVar
                           mergingRun) = do
    -- We need to know how many credits were supplied so we can restore merge
    -- work on snapshot load.
    (mergingRunState,
     MR.SuppliedCredits (MR.Credits suppliedCredits),
     mergeNumRuns,
     mergeNumEntries) <- MR.snapshot mergingRun
    -- TODO: MR.snapshot needs to return duplicated run references, and we
    -- need to arrange to release them when the snapshoting is done.
    let smrs = toSnapMergingRunState mergingRunState
    pure $
      SnapMergingRun
        mergePolicy
        mergeNumRuns
        mergeNumEntries
        (SuppliedCredits suppliedCredits)
        smrs

toSnapMergingRunState ::
     MR.MergingRunState t m h
  -> SnapMergingRunState t (Ref (Run m h))
toSnapMergingRunState = \case
    MR.CompletedMerge r  -> SnapCompletedMerge r
    MR.OngoingMerge rs m -> SnapOngoingMerge rs (Merge.mergeType m)

{-------------------------------------------------------------------------------
  Write Buffer
-------------------------------------------------------------------------------}

{-# SPECIALISE
  snapshotWriteBuffer ::
       ActionRegistry IO
    -> HasFS IO h
    -> HasBlockIO IO h
    -> UniqCounter IO
    -> UniqCounter IO
    -> ActiveDir
    -> NamedSnapshotDir
    -> WriteBuffer
    -> Ref (WriteBufferBlobs IO h)
    -> IO WriteBufferFsPaths
  #-}
snapshotWriteBuffer ::
     (MonadMVar m, MonadSTM m, MonadST m, MonadMask m)
  => ActionRegistry m
  -> HasFS m h
  -> HasBlockIO m h
  -> UniqCounter m
  -> UniqCounter m
  -> ActiveDir
  -> NamedSnapshotDir
  -> WriteBuffer
  -> Ref (WriteBufferBlobs m h)
  -> m WriteBufferFsPaths
snapshotWriteBuffer reg hfs hbio activeUc snapUc activeDir snapDir wb wbb = do
  -- Write the write buffer and write buffer blobs to the active directory.
  activeWriteBufferNumber <- uniqueToRunNumber <$> incrUniqCounter activeUc
  let activeWriteBufferPaths = WriteBufferFsPaths (getActiveDir activeDir) activeWriteBufferNumber
  withRollback_ reg
    (WBW.writeWriteBuffer hfs hbio activeWriteBufferPaths wb wbb)
    -- TODO: it should probably be the responsibility of writeWriteBuffer to do
    -- cleanup
    $ do
      -- TODO: check files exist before removing them
      FS.removeFile hfs (writeBufferKOpsPath activeWriteBufferPaths)
      FS.removeFile hfs (writeBufferBlobPath activeWriteBufferPaths)
  -- Hard link the write buffer and write buffer blobs to the snapshot directory.
  snapWriteBufferNumber <- uniqueToRunNumber <$> incrUniqCounter snapUc
  let snapWriteBufferPaths = WriteBufferFsPaths (getNamedSnapshotDir snapDir) snapWriteBufferNumber
  hardLink reg hfs hbio
    (writeBufferKOpsPath activeWriteBufferPaths)
    (writeBufferKOpsPath snapWriteBufferPaths)
  hardLink reg hfs hbio
    (writeBufferBlobPath activeWriteBufferPaths)
    (writeBufferBlobPath snapWriteBufferPaths)
  hardLink reg hfs hbio
    (writeBufferChecksumsPath activeWriteBufferPaths)
    (writeBufferChecksumsPath snapWriteBufferPaths)
  pure snapWriteBufferPaths

{-# SPECIALISE
  openWriteBuffer ::
       ActionRegistry IO
    -> ResolveSerialisedValue
    -> HasFS IO h
    -> HasBlockIO IO h
    -> UniqCounter IO
    -> ActiveDir
    -> WriteBufferFsPaths
    -> IO (WriteBuffer, Ref (WriteBufferBlobs IO h))
  #-}
openWriteBuffer ::
     (MonadMVar m, MonadMask m, MonadSTM m, MonadST m)
  => ActionRegistry m
  -> ResolveSerialisedValue
  -> HasFS m h
  -> HasBlockIO m h
  -> UniqCounter m
  -> ActiveDir
  -> WriteBufferFsPaths
  -> m (WriteBuffer, Ref (WriteBufferBlobs m h))
openWriteBuffer reg resolve hfs hbio uc activeDir snapWriteBufferPaths = do
  -- Check the checksums
  -- TODO: This reads the blobfile twice: once to check the CRC and once more
  --       to copy it from the snapshot directory to the active directory.
  (expectedChecksumForKOps, expectedChecksumForBlob) <-
    CRC.expectValidFile (writeBufferChecksumsPath snapWriteBufferPaths) . fromChecksumsFileForWriteBufferFiles
      =<< CRC.readChecksumsFile hfs (writeBufferChecksumsPath snapWriteBufferPaths)
  checkCRC hfs hbio False (unForKOps expectedChecksumForKOps) (writeBufferKOpsPath snapWriteBufferPaths)
  checkCRC hfs hbio False (unForBlob expectedChecksumForBlob) (writeBufferBlobPath snapWriteBufferPaths)
  -- Copy the write buffer blobs file to the active directory and open it.
  activeWriteBufferNumber <- uniqueToInt <$> incrUniqCounter uc
  let activeWriteBufferBlobPath =
        getActiveDir activeDir </> FS.mkFsPath [show activeWriteBufferNumber] <.> "wbblobs"
  copyFile reg hfs hbio (writeBufferBlobPath snapWriteBufferPaths) activeWriteBufferBlobPath
  writeBufferBlobs <-
    withRollback reg
      (WBB.open hfs activeWriteBufferBlobPath FS.AllowExisting)
      releaseRef
  -- Read write buffer key/ops
  let kOpsPath = ForKOps (writeBufferKOpsPath snapWriteBufferPaths)
  writeBuffer <-
    withRef writeBufferBlobs $ \wbb ->
      WBR.readWriteBuffer resolve hfs hbio kOpsPath (WBB.blobFile wbb)
  pure (writeBuffer, writeBufferBlobs)

{-------------------------------------------------------------------------------
  Runs
-------------------------------------------------------------------------------}

{-# SPECIALISE snapshotRuns ::
     ActionRegistry IO
  -> UniqCounter IO
  -> NamedSnapshotDir
  -> SnapLevels (Ref (Run IO h))
  -> IO (SnapLevels RunNumber) #-}
-- | @'snapshotRuns' _ _ snapUc targetDir levels@ creates hard links for all run
-- files associated with the runs in @levels@, and puts the new directory
-- entries in the @targetDir@ directory. The entries are renamed using @snapUc@.
snapshotRuns ::
     (MonadMask m, PrimMonad m)
  => ActionRegistry m
  -> UniqCounter m
  -> NamedSnapshotDir
  -> SnapLevels (Ref (Run m h))
  -> m (SnapLevels RunNumber)
snapshotRuns reg snapUc (NamedSnapshotDir targetDir) levels = do
    for levels $ \run@(DeRef Run.Run {
        Run.runHasFS = hfs,
        Run.runHasBlockIO = hbio
      }) -> do
        rn <- uniqueToRunNumber <$> incrUniqCounter snapUc
        let sourcePaths = Run.runFsPaths run
        let targetPaths = sourcePaths { runDir = targetDir , runNumber = rn}
        hardLinkRunFiles reg hfs hbio sourcePaths targetPaths
        pure (runNumber targetPaths)

{-# SPECIALISE openRuns ::
     ActionRegistry IO
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
--
-- The result must ultimately be released using 'releaseRuns'.
openRuns ::
     (MonadMask m, MonadSTM m, MonadST m)
  => ActionRegistry m
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
        let
          caching   = diskCachePolicyForLevel confDiskCachePolicy ln
          indexType = indexTypeForRun confFencePointerIndex
        in
        for level $ \runNum -> do
          let sourcePaths = RunFsPaths sourceDir runNum
          runNum' <- uniqueToRunNumber <$> incrUniqCounter uc
          let targetPaths = RunFsPaths targetDir runNum'
          hardLinkRunFiles reg hfs hbio sourcePaths targetPaths

          withRollback reg
            (Run.openFromDisk hfs hbio caching indexType targetPaths)
            releaseRef
    pure (SnapLevels levels')

{-# SPECIALISE releaseRuns ::
     ActionRegistry IO -> SnapLevels (Ref (Run IO h)) -> IO ()
  #-}
releaseRuns ::
     (MonadMask m, MonadST m)
  => ActionRegistry m -> SnapLevels (Ref (Run m h)) -> m ()
releaseRuns reg = traverse_ $ \r -> delayedCommit reg (releaseRef r)

{-------------------------------------------------------------------------------
  Opening from levels snapshot format
-------------------------------------------------------------------------------}

{-# SPECIALISE fromSnapLevels ::
     ActionRegistry IO
  -> HasFS IO h
  -> HasBlockIO IO h
  -> TableConfig
  -> UniqCounter IO
  -> ResolveSerialisedValue
  -> ActiveDir
  -> SnapLevels (Ref (Run IO h))
  -> IO (Levels IO h)
  #-}
-- | Duplicates runs and re-creates merging runs.
fromSnapLevels ::
     forall m h. (MonadMask m, MonadMVar m, MonadSTM m, MonadST m)
  => ActionRegistry m
  -> HasFS m h
  -> HasBlockIO m h
  -> TableConfig
  -> UniqCounter m
  -> ResolveSerialisedValue
  -> ActiveDir
  -> SnapLevels (Ref (Run m h))
  -> m (Levels m h)
fromSnapLevels reg hfs hbio conf uc resolve dir (SnapLevels levels) =
    V.iforM levels $ \i -> fromSnapLevel (LevelNo (i+1))
  where
    -- TODO: we may wish to trace the merges created during snapshot restore:
    tr :: Tracer m (AtLevel MergeTrace)
    tr = nullTracer

    fromSnapLevel :: LevelNo -> SnapLevel (Ref (Run m h)) -> m (Level m h)
    fromSnapLevel ln SnapLevel{..} = do
        incomingRun <- fromSnapIncomingRun ln snapIncoming
        residentRuns <- V.mapM dupRun snapResidentRuns
        pure Level {incomingRun , residentRuns}

    fromSnapIncomingRun ::
         LevelNo
      -> SnapIncomingRun (Ref (Run m h))
      -> m (IncomingRun m h)
    fromSnapIncomingRun ln (SnapSingleRun run) =
        newIncomingSingleRun tr ln =<< dupRun run

    fromSnapIncomingRun ln (SnapMergingRun mpfl nr ne (SuppliedCredits sc) smrs) = do
      case smrs of
        SnapCompletedMerge r ->
          newIncomingCompletedMergingRun tr reg ln mpfl nr ne r

        SnapOngoingMerge rs mt -> do
          ir <- newIncomingMergingRun tr hfs hbio dir uc
                                      conf resolve reg
                                      mpfl mt ln rs
          -- When a snapshot is created, merge progress is lost, so we
          -- have to redo merging work here. SuppliedCredits tracks how
          -- many credits were supplied before the snapshot was taken.
          leftoverCredits <- supplyCreditsIncomingRun conf ln ir (MR.Credits sc)
          assert (leftoverCredits == 0) $ return ()
          return ir

    dupRun r = withRollback reg (dupRef r) releaseRef

{-------------------------------------------------------------------------------
  Hard links
-------------------------------------------------------------------------------}

{-# SPECIALISE hardLinkRunFiles ::
     ActionRegistry IO
  -> HasFS IO h
  -> HasBlockIO IO h
  -> RunFsPaths
  -> RunFsPaths
  -> IO () #-}
-- | @'hardLinkRunFiles' _ _ _ sourcePaths targetPaths@ creates a hard link for
-- each @sourcePaths@ path using the corresponding @targetPaths@ path as the
-- name for the new directory entry.
hardLinkRunFiles ::
     (MonadMask m, PrimMonad m)
  => ActionRegistry m
  -> HasFS m h
  -> HasBlockIO m h
  -> RunFsPaths
  -> RunFsPaths
  -> m ()
hardLinkRunFiles reg hfs hbio sourceRunFsPaths targetRunFsPaths = do
    let sourcePaths = pathsForRunFiles sourceRunFsPaths
        targetPaths = pathsForRunFiles targetRunFsPaths
    sequenceA_ (hardLink reg hfs hbio <$> sourcePaths <*> targetPaths)
    hardLink reg hfs hbio (runChecksumsPath sourceRunFsPaths) (runChecksumsPath targetRunFsPaths)

{-# SPECIALISE
  hardLink ::
       ActionRegistry IO
    -> HasFS IO h
    -> HasBlockIO IO h
    -> FS.FsPath
    -> FS.FsPath
    -> IO ()
  #-}
-- | @'hardLink' reg hfs hbio sourcePath targetPath@ creates a hard link from
-- @sourcePath@ to @targetPath@.
hardLink ::
     (MonadMask m, PrimMonad m)
  => ActionRegistry m
  -> HasFS m h
  -> HasBlockIO m h
  -> FS.FsPath
  -> FS.FsPath
  -> m ()
hardLink reg hfs hbio sourcePath targetPath = do
    withRollback_ reg
      (FS.createHardLink hbio sourcePath targetPath)
      (FS.removeFile hfs targetPath)

{-------------------------------------------------------------------------------
  Copy file
-------------------------------------------------------------------------------}

{-# SPECIALISE
  copyFile ::
       ActionRegistry IO
    -> HasFS IO h
    -> HasBlockIO IO h
    -> FS.FsPath
    -> FS.FsPath
    -> IO ()
  #-}
-- | @'copyFile' reg hfs hbio source target@ copies the @source@ path to the @target@ path.
copyFile ::
     (MonadMask m, PrimMonad m)
  => ActionRegistry m
  -> HasFS m h
  -> HasBlockIO m h
  -> FS.FsPath
  -> FS.FsPath
  -> m ()
copyFile reg hfs _hbio sourcePath targetPath =
    flip (withRollback_ reg) (FS.removeFile hfs targetPath) $
      FS.withFile hfs sourcePath FS.ReadMode $ \sourceHandle ->
        FS.withFile hfs targetPath (FS.WriteMode FS.MustBeNew) $ \targetHandle -> do
          bs <- FSL.hGetAll hfs sourceHandle
          void $ FSL.hPutAll hfs targetHandle bs
