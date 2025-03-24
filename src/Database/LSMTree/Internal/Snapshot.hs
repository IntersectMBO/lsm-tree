module Database.LSMTree.Internal.Snapshot (
    -- * Snapshot metadata
    SnapshotMetaData (..)
  , SnapshotLabel (..)
    -- * Levels snapshot format
  , SnapLevels (..)
  , SnapLevel (..)
  , SnapIncomingRun (..)
  , SnapMergingRun (..)
    -- * MergeTree snapshot format
  , SnapMergingTree(..)
  , SnapMergingTreeState(..)
  , SnapPendingMerge(..)
  , SnapPreExistingRun(..)
    -- * Conversion to levels snapshot format
  , toSnapLevels
    -- * Conversion to merging tree snapshot format
  , toSnapMergingTree
    -- * Write buffer
  , snapshotWriteBuffer
  , openWriteBuffer
    -- * Run
  , SnapshotRun (..)
  , snapshotRun
  , openRun
    -- * Opening snapshot formats
    -- ** Levels format
  , fromSnapLevels
    -- ** Merging Tree format
  , fromSnapMergingTree
    -- * Hard links
  , hardLinkRunFiles
  ) where

import           Control.ActionRegistry
import           Control.Concurrent.Class.MonadMVar.Strict
import           Control.Concurrent.Class.MonadSTM (MonadSTM)
import           Control.DeepSeq (NFData (..))
import           Control.Monad (void)
import           Control.Monad.Class.MonadST (MonadST)
import           Control.Monad.Class.MonadThrow (MonadMask, bracket,
                     bracketOnError)
import           Control.Monad.Primitive (PrimMonad)
import           Control.RefCount
import           Data.Foldable (sequenceA_, traverse_)
import           Data.String (IsString (..))
import           Data.Text (Text)
import qualified Data.Vector as V
import           Database.LSMTree.Internal.Config
import           Database.LSMTree.Internal.CRC32C (checkCRC)
import qualified Database.LSMTree.Internal.CRC32C as CRC
import           Database.LSMTree.Internal.IncomingRun
import           Database.LSMTree.Internal.Lookup (ResolveSerialisedValue)
import qualified Database.LSMTree.Internal.Merge as Merge
import           Database.LSMTree.Internal.MergeSchedule
import qualified Database.LSMTree.Internal.MergingRun as MR
import qualified Database.LSMTree.Internal.MergingTree as MT
import           Database.LSMTree.Internal.Paths (ActiveDir (..), ForBlob (..),
                     ForKOps (..), NamedSnapshotDir (..), RunFsPaths (..),
                     WriteBufferFsPaths (..),
                     fromChecksumsFileForWriteBufferFiles, pathsForRunFiles,
                     runChecksumsPath, runPath, writeBufferBlobPath,
                     writeBufferChecksumsPath, writeBufferKOpsPath)
import           Database.LSMTree.Internal.Run (Run, RunParams)
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
  deriving newtype (NFData, IsString)

data SnapshotMetaData = SnapshotMetaData {
    -- | See 'SnapshotLabel'.
    --
    -- One could argue that the 'SnapshotName' could be used to to hold this
    -- type information, but the file name of snapshot metadata is not guarded
    -- by a checksum, whereas the contents of the file are. Therefore using the
    -- 'SnapshotLabel' is safer.
    snapMetaLabel   :: !SnapshotLabel
    -- | The 'TableConfig' for the snapshotted table.
    --
    -- Some of these configuration options can be overridden when a snapshot is
    -- opened: see 'TableConfigOverride'.
  , snapMetaConfig  :: !TableConfig
    -- | The write buffer.
  , snapWriteBuffer :: !RunNumber
    -- | The shape of the levels of the LSM tree.
  , snapMetaLevels  :: !(SnapLevels SnapshotRun)
    -- | The state of tree merging of the LSM tree.
  , snapMergingTree :: !(Maybe (SnapMergingTree SnapshotRun))
  }
  deriving stock Eq

instance NFData SnapshotMetaData where
  rnf (SnapshotMetaData a b c d e) =
    rnf a `seq` rnf b `seq` rnf c `seq`
    rnf d `seq` rnf e

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

-- | Note that for snapshots of incoming runs, we store only the merge debt and
-- nominal credits, not the nominal debt or the merge credits. The rationale is
-- a bit subtle.
--
-- The nominal debt does not need to be stored because it can be derived based
-- on the table's write buffer size (which is stored in the snapshot's
-- TableConfig), and on the level number that the merge is at (which also known
-- from the snapshot structure).
--
-- The merge credits can be recalculated from the combination of the nominal debt,
-- nominal credits and merge debt.
--
-- The merge debt is always the sum of the size of the input runs, so at first
-- glance this seems redundant. However for completed merges we no longer have
-- the input runs, so we must store the merge debt if we are to perfectly round
-- trip the snapshot. This is a nice simple property to have though it is
-- probably not 100% essential. We could weaken the round trip property to
-- allow forgetting the merge debt and credit of completed merges (and set them
-- both to zero).
--
data SnapIncomingRun r =
    SnapIncomingMergingRun
      !MergePolicyForLevel
      !NominalDebt
      !NominalCredits -- ^ The nominal credits supplied, and that
                     -- need to be supplied on snapshot open.
      !(SnapMergingRun MR.LevelMergeType r)
  | SnapIncomingSingleRun !r
  deriving stock (Eq, Functor, Foldable, Traversable)

instance NFData r => NFData (SnapIncomingRun r) where
  rnf (SnapIncomingMergingRun a b c d) =
      rnf a `seq` rnf b `seq` rnf c `seq` rnf d
  rnf (SnapIncomingSingleRun a) = rnf a

-- | The total number of supplied credits. This total is used on snapshot load
-- to restore merging work that was lost when the snapshot was created.
newtype SuppliedCredits = SuppliedCredits { getSuppliedCredits :: Int }
  deriving stock (Eq, Read)
  deriving newtype NFData

data SnapMergingRun t r =
    SnapCompletedMerge !MergeDebt !r
  | SnapOngoingMerge   !RunParams !MergeCredits !(V.Vector r) !t
  deriving stock (Eq, Functor, Foldable, Traversable)

instance (NFData t, NFData r) => NFData (SnapMergingRun t r) where
  rnf (SnapCompletedMerge a b)     = rnf a `seq` rnf b
  rnf (SnapOngoingMerge   a b c d) = rnf a `seq` rnf b `seq` rnf c `seq` rnf d

{-------------------------------------------------------------------------------
  Snapshot MergingTree
-------------------------------------------------------------------------------}

newtype SnapMergingTree r = SnapMergingTree (SnapMergingTreeState r)
  deriving stock (Eq, Functor, Foldable, Traversable)
  deriving newtype NFData

data SnapMergingTreeState r =
    SnapCompletedTreeMerge !r
  | SnapPendingTreeMerge   !(SnapPendingMerge r)
  | SnapOngoingTreeMerge   !(SnapMergingRun MR.TreeMergeType r)
  deriving stock (Eq, Functor, Foldable, Traversable)

instance NFData r => NFData (SnapMergingTreeState r) where
  rnf (SnapCompletedTreeMerge a) = rnf a
  rnf (SnapPendingTreeMerge a)   = rnf a
  rnf (SnapOngoingTreeMerge a)   = rnf a

data SnapPendingMerge r =
    SnapPendingLevelMerge
      ![SnapPreExistingRun r]
      !(Maybe (SnapMergingTree r))
  | SnapPendingUnionMerge
      ![SnapMergingTree r]
  deriving stock (Eq, Functor, Foldable, Traversable)

instance NFData r => NFData (SnapPendingMerge r) where
  rnf (SnapPendingLevelMerge a b) = rnf a `seq` rnf b
  rnf (SnapPendingUnionMerge a)   = rnf a

data SnapPreExistingRun r =
    SnapPreExistingRun        !r
  | SnapPreExistingMergingRun !(SnapMergingRun MR.LevelMergeType r)
  deriving stock (Eq, Functor, Foldable, Traversable)

instance NFData r => NFData (SnapPreExistingRun r) where
  rnf (SnapPreExistingRun a)        = rnf a
  rnf (SnapPreExistingMergingRun a) = rnf a

{-------------------------------------------------------------------------------
  Opening from merging tree snapshot format
-------------------------------------------------------------------------------}

{-# SPECIALISE fromSnapMergingTree ::
     HasFS IO h
  -> HasBlockIO IO h
  -> UniqCounter IO
  -> ResolveSerialisedValue
  -> ActiveDir
  -> ActionRegistry IO
  -> SnapMergingTree (Ref (Run IO h))
  -> IO (Ref (MT.MergingTree IO h))
  #-}
-- | Converts a snapshot of a merging tree of runs to a real merging tree.
--
-- Returns a new reference. Input runs remain owned by the caller.
fromSnapMergingTree ::
     forall m h. (MonadMask m, MonadMVar m, MonadSTM m, MonadST m)
  => HasFS m h
  -> HasBlockIO m h
  -> UniqCounter m
  -> ResolveSerialisedValue
  -> ActiveDir
  -> ActionRegistry m
  -> SnapMergingTree (Ref (Run m h))
  -> m (Ref (MT.MergingTree m h))
fromSnapMergingTree hfs hbio uc resolve dir =
    go
  where
    -- Reference strategy:
    -- * go returns a fresh reference
    -- * go ensures the returned reference will be cleaned up on failure,
    --   using withRollback
    -- * All results from recursive calls must be released locally on the
    --   happy path.
    go :: ActionRegistry m
       -> SnapMergingTree (Ref (Run m h))
       -> m (Ref (MT.MergingTree m h))

    go reg (SnapMergingTree (SnapCompletedTreeMerge run)) =
      withRollback reg
        (MT.newCompletedMerge run)
        releaseRef

    go reg (SnapMergingTree (SnapPendingTreeMerge
                              (SnapPendingLevelMerge prs mmt))) = do
      prs' <- traverse (fromSnapPreExistingRun reg) prs
      mmt' <- traverse (go reg) mmt
      mt   <- withRollback reg
                (MT.newPendingLevelMerge prs' mmt')
                releaseRef
      traverse_ (delayedCommit reg . releasePER) prs'
      traverse_ (delayedCommit reg . releaseRef) mmt'
      return mt

    go reg (SnapMergingTree (SnapPendingTreeMerge
                              (SnapPendingUnionMerge mts))) = do
      mts' <- traverse (go reg) mts
      mt   <- withRollback reg
                (MT.newPendingUnionMerge mts')
                releaseRef
      traverse_ (delayedCommit reg . releaseRef) mts'
      return mt

    go reg (SnapMergingTree (SnapOngoingTreeMerge smrs)) = do
      mr <- withRollback reg
               (fromSnapMergingRun hfs hbio uc resolve dir smrs)
               releaseRef
      mt <- withRollback reg
              (MT.newOngoingMerge mr)
              releaseRef
      delayedCommit reg (releaseRef mr)
      return mt

    -- Returns fresh refs, which must be released locally.
    fromSnapPreExistingRun :: ActionRegistry m
                           -> SnapPreExistingRun (Ref (Run m h))
                           -> m (MT.PreExistingRun m h)
    fromSnapPreExistingRun reg (SnapPreExistingRun run) =
      MT.PreExistingRun <$>
        withRollback reg (dupRef run) releaseRef
    fromSnapPreExistingRun reg (SnapPreExistingMergingRun smrs) =
      MT.PreExistingMergingRun <$>
        withRollback reg
          (fromSnapMergingRun hfs hbio uc resolve dir smrs)
          releaseRef

    releasePER (MT.PreExistingRun         r) = releaseRef r
    releasePER (MT.PreExistingMergingRun mr) = releaseRef mr

{-------------------------------------------------------------------------------
  Conversion to merge tree snapshot format
-------------------------------------------------------------------------------}

{-# SPECIALISE toSnapMergingTree :: Ref (MT.MergingTree IO h) -> IO (SnapMergingTree (Ref (Run IO h))) #-}
toSnapMergingTree ::
     (PrimMonad m, MonadMVar m)
  => Ref (MT.MergingTree m h)
  -> m (SnapMergingTree (Ref (Run m h)))
toSnapMergingTree (DeRef (MT.MergingTree mStateVar _mCounter)) =
  withMVar mStateVar $ \mState -> SnapMergingTree <$> toSnapMergingTreeState mState

{-# SPECIALISE toSnapMergingTreeState :: MT.MergingTreeState IO h -> IO (SnapMergingTreeState (Ref (Run IO h))) #-}
toSnapMergingTreeState ::
     (PrimMonad m, MonadMVar m)
  => MT.MergingTreeState m h
  -> m (SnapMergingTreeState (Ref (Run m h)))
toSnapMergingTreeState (MT.CompletedTreeMerge r) = pure $ SnapCompletedTreeMerge r
toSnapMergingTreeState (MT.PendingTreeMerge p) = SnapPendingTreeMerge <$> toSnapPendingMerge p
toSnapMergingTreeState (MT.OngoingTreeMerge mergingRun) =
  SnapOngoingTreeMerge <$> toSnapMergingRun mergingRun

{-# SPECIALISE toSnapPendingMerge :: MT.PendingMerge IO h -> IO (SnapPendingMerge (Ref (Run IO h))) #-}
toSnapPendingMerge ::
     (PrimMonad m, MonadMVar m)
  => MT.PendingMerge m h
  -> m (SnapPendingMerge (Ref (Run m h)))
toSnapPendingMerge (MT.PendingUnionMerge mts) =
  SnapPendingUnionMerge <$> traverse toSnapMergingTree (V.toList mts)
toSnapPendingMerge (MT.PendingLevelMerge pes mmt) = do
  pes' <- traverse toSnapPreExistingRun pes
  mmt' <- traverse toSnapMergingTree mmt
  pure $ SnapPendingLevelMerge (V.toList pes') mmt'

{-# SPECIALISE toSnapPreExistingRun :: MT.PreExistingRun IO h -> IO (SnapPreExistingRun (Ref (Run IO h))) #-}
toSnapPreExistingRun ::
     (PrimMonad m, MonadMVar m)
  => MT.PreExistingRun m h
  -> m (SnapPreExistingRun (Ref (Run m h)))
toSnapPreExistingRun (MT.PreExistingRun run) = pure $ SnapPreExistingRun run
toSnapPreExistingRun (MT.PreExistingMergingRun peMergingRun) =
  SnapPreExistingMergingRun <$> toSnapMergingRun peMergingRun

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
toSnapIncomingRun ir = do
    s <- snapshotIncomingRun ir
    case s of
      Left r -> pure $! SnapIncomingSingleRun r
      Right (mergePolicy,
             nominalDebt,
             nominalCredits,
             mergingRun) -> do
        -- We need to know how many credits were supplied so we can restore merge
        -- work on snapshot load.
        smrs <- toSnapMergingRun mergingRun
        pure $! SnapIncomingMergingRun mergePolicy nominalDebt nominalCredits smrs

{-# SPECIALISE toSnapMergingRun ::
     Ref (MR.MergingRun t IO h)
  -> IO (SnapMergingRun t (Ref (Run IO h))) #-}
toSnapMergingRun ::
     (PrimMonad m, MonadMVar m)
  => Ref (MR.MergingRun t m h)
  -> m (SnapMergingRun t (Ref (Run m h)))
toSnapMergingRun !mr = do
    -- TODO: MR.snapshot needs to return duplicated run references, and we
    -- need to arrange to release them when the snapshotting is done.
    ( mergeDebt, mergeCredits, state) <- MR.snapshot mr
    case state of
      MR.CompletedMerge r  ->
        pure $! SnapCompletedMerge mergeDebt r

      MR.OngoingMerge rs m ->
          pure $! SnapOngoingMerge runParams mergeCredits rs mergeType
        where
          runParams = Merge.mergeRunParams m
          mergeType = Merge.mergeType m

{-------------------------------------------------------------------------------
  Write Buffer
-------------------------------------------------------------------------------}

{-# SPECIALISE
  snapshotWriteBuffer ::
       HasFS IO h
    -> HasBlockIO IO h
    -> UniqCounter IO
    -> UniqCounter IO
    -> ActionRegistry IO
    -> ActiveDir
    -> NamedSnapshotDir
    -> WriteBuffer
    -> Ref (WriteBufferBlobs IO h)
    -> IO WriteBufferFsPaths
  #-}
snapshotWriteBuffer ::
     (MonadMVar m, MonadSTM m, MonadST m, MonadMask m)
  => HasFS m h
  -> HasBlockIO m h
  -> UniqCounter m
  -> UniqCounter m
  -> ActionRegistry m
  -> ActiveDir
  -> NamedSnapshotDir
  -> WriteBuffer
  -> Ref (WriteBufferBlobs m h)
  -> m WriteBufferFsPaths
snapshotWriteBuffer hfs hbio activeUc snapUc reg activeDir snapDir wb wbb = do
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
  hardLink hfs hbio reg
    (writeBufferKOpsPath activeWriteBufferPaths)
    (writeBufferKOpsPath snapWriteBufferPaths)
  hardLink hfs hbio reg
    (writeBufferBlobPath activeWriteBufferPaths)
    (writeBufferBlobPath snapWriteBufferPaths)
  hardLink hfs hbio reg
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
  copyFile hfs reg (writeBufferBlobPath snapWriteBufferPaths) activeWriteBufferBlobPath
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

-- | Information needed to open a 'Run' from disk using 'snapshotRun' and
-- 'openRun'.
--
-- TODO: one could imagine needing only the 'RunNumber' to identify the files
-- on disk, and the other parameters being stored with the run itself, rather
-- than needing to be supplied.
data SnapshotRun = SnapshotRun {
       snapRunNumber  :: !RunNumber,
       snapRunCaching :: !Run.RunDataCaching,
       snapRunIndex   :: !Run.IndexType
     }
  deriving stock Eq

instance NFData SnapshotRun where
  rnf (SnapshotRun a b c) = rnf a `seq` rnf b `seq` rnf c

{-# SPECIALISE snapshotRun ::
     HasFS IO h
  -> HasBlockIO IO h
  -> UniqCounter IO
  -> ActionRegistry IO
  -> NamedSnapshotDir
  -> Ref (Run IO h)
  -> IO SnapshotRun #-}
-- | @'snapshotRun' _ _ snapUc _ targetDir run@ creates hard links for all files
-- associated with the @run@, and puts the new directory entries in the
-- @targetDir@ directory. The entries are renamed using @snapUc@.
snapshotRun ::
     (MonadMask m, PrimMonad m)
  => HasFS m h
  -> HasBlockIO m h
  -> UniqCounter m
  -> ActionRegistry m
  -> NamedSnapshotDir
  -> Ref (Run m h)
  -> m SnapshotRun
snapshotRun hfs hbio snapUc reg (NamedSnapshotDir targetDir) run = do
    rn <- uniqueToRunNumber <$> incrUniqCounter snapUc
    let sourcePaths = Run.runFsPaths run
    let targetPaths = sourcePaths { runDir = targetDir , runNumber = rn}
    hardLinkRunFiles hfs hbio reg sourcePaths targetPaths
    pure SnapshotRun {
           snapRunNumber  = runNumber targetPaths,
           snapRunCaching = Run.runDataCaching run,
           snapRunIndex   = Run.runIndexType run
         }

{-# SPECIALISE openRun ::
     HasFS IO h
  -> HasBlockIO IO h
  -> UniqCounter IO
  -> ActionRegistry IO
  -> NamedSnapshotDir
  -> ActiveDir
  -> SnapshotRun
  -> IO (Ref (Run IO h)) #-}
-- | @'openRun' _ _ uniqCounter _ sourceDir targetDir snaprun@ takes all run
-- files that are referenced by @snaprun@, and hard links them from @sourceDir@
-- into @targetDir@ with new, unique names (using @uniqCounter@). Each set of
-- (hard linked) files that represents a run is opened and verified, returning
-- 'Run' as a result.
--
-- The result must ultimately be released using 'releaseRef'.
openRun ::
     (MonadMask m, MonadSTM m, MonadST m)
  => HasFS m h
  -> HasBlockIO m h
  -> UniqCounter m
  -> ActionRegistry m
  -> NamedSnapshotDir
  -> ActiveDir
  -> SnapshotRun
  -> m (Ref (Run m h))
openRun hfs hbio uc reg
        (NamedSnapshotDir sourceDir) (ActiveDir targetDir)
        SnapshotRun {
          snapRunNumber  = runNum,
          snapRunCaching = caching,
          snapRunIndex   = indexType
        } = do
    let sourcePaths = RunFsPaths sourceDir runNum
    runNum' <- uniqueToRunNumber <$> incrUniqCounter uc
    let targetPaths = RunFsPaths targetDir runNum'
    hardLinkRunFiles hfs hbio reg sourcePaths targetPaths

    withRollback reg
      (Run.openFromDisk hfs hbio caching indexType targetPaths)
      releaseRef

{-------------------------------------------------------------------------------
  Opening from levels snapshot format
-------------------------------------------------------------------------------}

{-# SPECIALISE fromSnapLevels ::
     HasFS IO h
  -> HasBlockIO IO h
  -> UniqCounter IO
  -> TableConfig
  -> ResolveSerialisedValue
  -> ActionRegistry IO
  -> ActiveDir
  -> SnapLevels (Ref (Run IO h))
  -> IO (Levels IO h)
  #-}
-- | Duplicates runs and re-creates merging runs.
fromSnapLevels ::
     forall m h. (MonadMask m, MonadMVar m, MonadSTM m, MonadST m)
  => HasFS m h
  -> HasBlockIO m h
  -> UniqCounter m
  -> TableConfig
  -> ResolveSerialisedValue
  -> ActionRegistry m
  -> ActiveDir
  -> SnapLevels (Ref (Run m h))
  -> m (Levels m h)
fromSnapLevels hfs hbio uc conf resolve reg dir (SnapLevels levels) =
    V.iforM levels $ \i -> fromSnapLevel (LevelNo (i+1))
  where
    -- TODO: we may wish to trace the merges created during snapshot restore:

    fromSnapLevel :: LevelNo -> SnapLevel (Ref (Run m h)) -> m (Level m h)
    fromSnapLevel ln SnapLevel{snapIncoming, snapResidentRuns} = do
        incomingRun <- withRollback reg
                         (fromSnapIncomingRun ln snapIncoming)
                         releaseIncomingRun
        residentRuns <- V.forM snapResidentRuns $ \r ->
                          withRollback reg
                            (dupRef r)
                            releaseRef
        pure Level {incomingRun , residentRuns}

    fromSnapIncomingRun ::
         LevelNo
      -> SnapIncomingRun (Ref (Run m h))
      -> m (IncomingRun m h)
    fromSnapIncomingRun _ln (SnapIncomingSingleRun run) =
        newIncomingSingleRun run

    fromSnapIncomingRun ln (SnapIncomingMergingRun mergePolicy nominalDebt
                                                   nominalCredits smrs) =
      bracket
        (fromSnapMergingRun hfs hbio uc resolve dir smrs)
        releaseRef $ \mr -> do

        ir <- newIncomingMergingRun mergePolicy nominalDebt mr
        -- This will set the correct nominal credits, but it will not do any
        -- more merging work because fromSnapMergingRun already supplies
        -- all the merging credits already.
        supplyCreditsIncomingRun conf ln ir nominalCredits
        return ir

{-# SPECIALISE fromSnapMergingRun ::
     MR.IsMergeType t
  => HasFS IO h
  -> HasBlockIO IO h
  -> UniqCounter IO
  -> ResolveSerialisedValue
  -> ActiveDir
  -> SnapMergingRun t (Ref (Run IO h))
  -> IO (Ref (MR.MergingRun t IO h)) #-}
fromSnapMergingRun ::
     (MonadMask m, MonadMVar m, MonadSTM m, MonadST m, MR.IsMergeType t)
  => HasFS m h
  -> HasBlockIO m h
  -> UniqCounter m
  -> ResolveSerialisedValue
  -> ActiveDir
  -> SnapMergingRun t (Ref (Run m h))
  -> m (Ref (MR.MergingRun t m h))
fromSnapMergingRun _ _ _ _ _ (SnapCompletedMerge mergeDebt r) =
    MR.newCompleted mergeDebt r

fromSnapMergingRun hfs hbio uc resolve dir
                   (SnapOngoingMerge runParams mergeCredits rs mergeType) = do
    bracketOnError
      (do uniq <- incrUniqCounter uc
          let runPaths = runPath dir (uniqueToRunNumber uniq)
          MR.new hfs hbio resolve runParams mergeType runPaths rs)
      releaseRef $ \mr -> do
        -- When a snapshot is created, merge progress is lost, so we have to
        -- redo merging work here. The MergeCredits in SnapMergingRun tracks
        -- how many credits were supplied before the snapshot was taken.

        --TODO: the threshold should be stored with the MergingRun
        -- here we want to supply the credits now, so we can use a threshold of 1
        let thresh = MR.CreditThreshold (MR.UnspentCredits 1)
        _ <- MR.supplyCreditsAbsolute mr thresh mergeCredits
        return mr

{-------------------------------------------------------------------------------
  Hard links
-------------------------------------------------------------------------------}

{-# SPECIALISE hardLinkRunFiles ::
     HasFS IO h
  -> HasBlockIO IO h
  -> ActionRegistry IO
  -> RunFsPaths
  -> RunFsPaths
  -> IO () #-}
-- | @'hardLinkRunFiles' _ _ _ sourcePaths targetPaths@ creates a hard link for
-- each @sourcePaths@ path using the corresponding @targetPaths@ path as the
-- name for the new directory entry.
hardLinkRunFiles ::
     (MonadMask m, PrimMonad m)
  => HasFS m h
  -> HasBlockIO m h
  -> ActionRegistry m
  -> RunFsPaths
  -> RunFsPaths
  -> m ()
hardLinkRunFiles hfs hbio reg sourceRunFsPaths targetRunFsPaths = do
    let sourcePaths = pathsForRunFiles sourceRunFsPaths
        targetPaths = pathsForRunFiles targetRunFsPaths
    sequenceA_ (hardLink hfs hbio reg <$> sourcePaths <*> targetPaths)
    hardLink hfs hbio reg (runChecksumsPath sourceRunFsPaths) (runChecksumsPath targetRunFsPaths)

{-# SPECIALISE
  hardLink ::
       HasFS IO h
    -> HasBlockIO IO h
    -> ActionRegistry IO
    -> FS.FsPath
    -> FS.FsPath
    -> IO ()
  #-}
-- | @'hardLink' hfs hbio reg sourcePath targetPath@ creates a hard link from
-- @sourcePath@ to @targetPath@.
hardLink ::
     (MonadMask m, PrimMonad m)
  => HasFS m h
  -> HasBlockIO m h
  -> ActionRegistry m
  -> FS.FsPath
  -> FS.FsPath
  -> m ()
hardLink hfs hbio reg sourcePath targetPath = do
    withRollback_ reg
      (FS.createHardLink hbio sourcePath targetPath)
      (FS.removeFile hfs targetPath)

{-------------------------------------------------------------------------------
  Copy file
-------------------------------------------------------------------------------}

{-# SPECIALISE
  copyFile ::
       HasFS IO h
    -> ActionRegistry IO
    -> FS.FsPath
    -> FS.FsPath
    -> IO ()
  #-}
-- | @'copyFile' hfs reg source target@ copies the @source@ path to the @target@ path.
copyFile ::
     (MonadMask m, PrimMonad m)
  => HasFS m h
  -> ActionRegistry m
  -> FS.FsPath
  -> FS.FsPath
  -> m ()
copyFile hfs reg sourcePath targetPath =
    flip (withRollback_ reg) (FS.removeFile hfs targetPath) $
      FS.withFile hfs sourcePath FS.ReadMode $ \sourceHandle ->
        FS.withFile hfs targetPath (FS.WriteMode FS.MustBeNew) $ \targetHandle -> do
          bs <- FSL.hGetAll hfs sourceHandle
          void $ FSL.hPutAll hfs targetHandle bs
