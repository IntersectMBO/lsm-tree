{-# LANGUAGE OverloadedStrings #-}

-- TODO: remove once we properly implement snapshots
{-# OPTIONS_GHC -Wno-orphans #-}

module Database.LSMTree.Internal.Snapshot (
    SnapshotVersion (..)
  , SnapshotLabel (..)
  , SnapshotMetaData (..)

  , encode
  , ToEncoding (..)

  , currentSnapshotVersion
  , encodeSnapshotMetaData

    -- * Snapshot format
  , numSnapRuns
  , SnapLevels
  , SnapLevel (..)
  , SnapMergingRun (..)
  , SnapMergingRunState (..)
    -- * Creating snapshots
  , snapLevels
    -- * Opening snapshots
  , openLevels
  ) where

import           Control.Concurrent.Class.MonadMVar.Strict
import           Control.Concurrent.Class.MonadSTM (MonadSTM)
import           Control.Monad.Class.MonadST (MonadST)
import           Control.Monad.Class.MonadThrow (MonadMask)
import           Control.Monad.Fix (MonadFix)
import           Control.Monad.Primitive (PrimMonad)
import           Control.TempRegistry
import           Data.Aeson hiding (encode)
import qualified Data.Aeson as A
import           Data.Aeson.Decoding
import           Data.Aeson.Encoding
import           Data.Aeson.Types
import           Data.ByteString.Lazy (ByteString)
import           Data.Foldable (forM_, traverse_)
import           Data.Primitive.PrimVar
import           Data.String
import qualified Data.Vector as V
import           Data.Word
import           Database.LSMTree.Internal.Config
import           Database.LSMTree.Internal.Entry
import           Database.LSMTree.Internal.Lookup (ResolveSerialisedValue)
import qualified Database.LSMTree.Internal.Merge as Merge
import           Database.LSMTree.Internal.MergeSchedule
import           Database.LSMTree.Internal.Paths (SessionRoot)
import qualified Database.LSMTree.Internal.Paths as Paths
import           Database.LSMTree.Internal.Run (Run)
import qualified Database.LSMTree.Internal.Run as Run
import           Database.LSMTree.Internal.RunNumber
import           Database.LSMTree.Internal.UniqCounter (UniqCounter,
                     incrUniqCounter, uniqueToRunNumber)
import           System.FS.API (HasFS)
import           System.FS.BlockIO.API (HasBlockIO)

{-------------------------------------------------------------------------------
  Snapshot metadata
-------------------------------------------------------------------------------}

-- | Version identifier
newtype SnapshotVersion = SnapshotVersion Word64
  deriving stock (Show, Eq)

currentSnapshotVersion :: SnapshotVersion
currentSnapshotVersion = SnapshotVersion 0

-- | Custom text to include in a snapshot file
newtype SnapshotLabel = SnapshotLabel String -- TODO: replace by Text?
  deriving stock (Show, Eq, Read)
  deriving newtype (Semigroup, IsString)

data SnapshotMetaData = SnapshotMetaData {
    -- | The version of the format for the snapshot metadata.
    snapMetaVersion :: !SnapshotVersion
    -- | Custom, user-supplied text that is included in the metadata.
    --
    -- The main use case for this field is for the user to supply textual
    -- information about the key\/value\/blob type for the table that
    -- corresponds to the snapshot. This information can then be used to
    -- dynamically check that a snapshot is opened at the correct
    -- key\/value\/blob type.
    --
    -- One could argue that the 'SnapshotName' could be used to to hold this
    -- information, but the file name of snapshot meta data is not guarded by
    -- checksum, wherease the contents of the file are. Therefore using the
    -- 'SnapshotLabel' is safer.
  , snapMetaLabel   :: !SnapshotLabel
    -- | The 'TableConfig' for the snapshotted table.
    --
    -- Some of these configuration options can be overridden when a snapshot is
    -- opened: see 'TableConfigOverride'.
  , snapMetaConfig  :: !TableConfig
    -- | The shape of the LSM tree.
  , snapMetaLevels  :: !SnapLevels -- TODO: rename
  }

{-------------------------------------------------------------------------------
  Levels snapshot format
-------------------------------------------------------------------------------}

numSnapRuns :: SnapLevels -> Int
numSnapRuns sl = V.sum $ V.map go1 sl
  where
    go1 (SnapLevel sir srr) = go2 sir + V.length srr
    go2 (SnapMergingRun _ _ _ smrs) = go3 smrs
    go2 (SnapSingleRun _rn)         = 1
    go3 (SnapCompletedMerge _rn)   = 1
    go3 (SnapOngoingMerge rns _ _) = V.length rns

type SnapLevels = V.Vector SnapLevel

data SnapLevel = SnapLevel {
    snapIncomingRuns :: !SnapMergingRun
  , snapResidentRuns :: !(V.Vector RunNumber)
  }
  deriving stock (Show, Eq, Read)

data SnapMergingRun =
    SnapMergingRun !MergePolicyForLevel !NumRuns !AccumulatedCredits !SnapMergingRunState
  | SnapSingleRun !RunNumber
  deriving stock (Show, Eq, Read)

newtype AccumulatedCredits = AccumulatedCredits {
    unAccumulatedCredits :: Int
  }
  deriving stock (Show, Eq, Read)

data SnapMergingRunState =
    SnapCompletedMerge !RunNumber
  | SnapOngoingMerge !(V.Vector RunNumber) !NumStepsDone {- merge -} !Merge.Level
  deriving stock (Show, Eq, Read)

newtype NumStepsDone = NumStepsDone { unNumStepsDone :: Int }
  deriving stock (Show, Eq)

{-------------------------------------------------------------------------------
  Conversion to snapshot format
-------------------------------------------------------------------------------}

{-# SPECIALISE snapLevels :: Levels IO h -> IO SnapLevels #-}
snapLevels ::
     (PrimMonad m, MonadMVar m)
  => Levels m h
  -> m SnapLevels
snapLevels = V.mapM snapLevel

{-# SPECIALISE snapLevel :: Level IO h -> IO SnapLevel #-}
snapLevel ::
     (PrimMonad m, MonadMVar m)
  => Level m h
  -> m SnapLevel
snapLevel Level{..} = do
    smr <- snapMergingRun incomingRuns
    pure (SnapLevel smr (V.map runNumber residentRuns))

{-# SPECIALISE snapMergingRun :: MergingRun IO h -> IO SnapMergingRun #-}
snapMergingRun ::
     (PrimMonad m, MonadMVar m)
  => MergingRun m h
  -> m SnapMergingRun
snapMergingRun (MergingRun mpfl nr acVar mrsVar) = do
    ac <- readPrimVar acVar
    smrs <- withMVar mrsVar $ \mrs -> snapMergingRunState mrs
    pure (SnapMergingRun mpfl nr (AccumulatedCredits ac) smrs)
snapMergingRun (SingleRun r) = pure (SnapSingleRun (runNumber r))

{-# SPECIALISE snapMergingRunState :: MergingRunState IO h -> IO SnapMergingRunState #-}
snapMergingRunState ::
     PrimMonad m
  => MergingRunState m h
  -> m SnapMergingRunState
snapMergingRunState (CompletedMerge r) = pure (SnapCompletedMerge (runNumber r))
snapMergingRunState (OngoingMerge rs nsdVar m) = do
    nsd <- readPrimVar nsdVar
    pure (SnapOngoingMerge (V.map runNumber rs) (NumStepsDone nsd) (Merge.mergeLevel m))

runNumber :: Run m h -> RunNumber
runNumber r = Paths.runNumber (Run.runRunFsPaths r)

{-------------------------------------------------------------------------------
  Opening from snapshot format
-------------------------------------------------------------------------------}

{-# SPECIALISE openLevels ::
     TempRegistry IO
  -> HasFS IO h
  -> HasBlockIO IO h
  -> TableConfig
  -> UniqCounter IO
  -> SessionRoot
  -> ResolveSerialisedValue
  -> SnapLevels
  -> IO (Levels IO h)
  #-}
openLevels ::
     forall m h. (MonadFix m, MonadMask m, MonadMVar m, MonadSTM m, MonadST m)
  => TempRegistry m
  -> HasFS m h
  -> HasBlockIO m h
  -> TableConfig
  -> UniqCounter m
  -> SessionRoot
  -> ResolveSerialisedValue
  -> SnapLevels
  -> m (Levels m h)
openLevels reg hfs hbio conf@TableConfig{..} uc sessionRoot resolve levels =
    V.iforM levels $ \i -> openLevel (LevelNo (i+1))
  where
    mkPath = Paths.RunFsPaths (Paths.activeDir sessionRoot)

    openLevel :: LevelNo -> SnapLevel -> m (Level m h)
    openLevel ln SnapLevel{..} = do
        (mmmay, incomingRuns) <- openMergingRun snapIncomingRuns
        -- When a snapshot is created, merge progress is lost, so we have to
        -- redo merging work here. NumStepsDone tracks how much merging work
        -- happened before the snapshot was taken.
        --
        -- TODO: this use of supplyMergeCredits is leaky! If a merge completes
        -- in supplyMergeCredits, then the resulting run is not tracked in the
        -- registry, and closing the input runs is also not tracked in the
        -- registry. Note, however, that this bit of code is likely to change in
        -- #392.
        forM_ mmmay $ \(NumStepsDone c) -> supplyMergeCredits (Credit c) (creditThresholdForLevel conf ln) incomingRuns
        residentRuns <- V.forM snapResidentRuns $ \rn ->
          allocateTemp reg
            (Run.openFromDisk hfs hbio caching (mkPath rn))
            Run.removeReference
        pure Level{..}
      where
        caching = diskCachePolicyForLevel confDiskCachePolicy ln
        alloc = bloomFilterAllocForLevel conf ln

        openMergingRun :: SnapMergingRun -> m (Maybe NumStepsDone, MergingRun m h)
        openMergingRun (SnapMergingRun mpfl nr ac smrs) = do
            (n, mrs) <- openMergingRunState smrs
            acVar <- newPrimVar (unAccumulatedCredits ac)
            (n,) . MergingRun mpfl nr acVar <$> newMVar mrs
        openMergingRun (SnapSingleRun rn) =
            (Nothing,) . SingleRun <$>
              allocateTemp reg
                (Run.openFromDisk hfs hbio caching (mkPath rn))
                Run.removeReference

        openMergingRunState :: SnapMergingRunState -> m (Maybe NumStepsDone, MergingRunState m h)
        openMergingRunState (SnapCompletedMerge rn) =
            (Nothing,) . CompletedMerge <$>
              allocateTemp reg
                (Run.openFromDisk hfs hbio caching (mkPath rn))
                Run.removeReference
        openMergingRunState (SnapOngoingMerge rns nsd mergeLast) = do
            rs <- V.forM rns $ \rn ->
              allocateTemp reg
                (Run.openFromDisk hfs hbio caching ((mkPath rn)))
                Run.removeReference
            nsdVar <- newPrimVar $! unNumStepsDone nsd
            rn <- uniqueToRunNumber <$> incrUniqCounter uc
            mergeMaybe <- allocateTemp reg
              (Merge.new hfs hbio caching alloc mergeLast resolve (mkPath rn) rs)
              (traverse_ Merge.removeReference)
            case mergeMaybe of
              Nothing -> error "openLevels: merges can not be empty"
              Just m  -> pure (Just nsd, OngoingMerge rs nsdVar m)

{-------------------------------------------------------------------------------
  Levels
-------------------------------------------------------------------------------}

-- TODO: remove all Read instances

deriving stock instance Read NumRuns
deriving stock instance Read MergePolicyForLevel
deriving stock instance Read NumStepsDone
deriving newtype instance Read RunNumber
deriving stock instance Read Merge.Level

{-------------------------------------------------------------------------------
  Config
-------------------------------------------------------------------------------}

-- TODO: remove all Read instances

deriving stock instance Read TableConfig
deriving stock instance Read WriteBufferAlloc
deriving stock instance Read NumEntries
deriving stock instance Read SizeRatio
deriving stock instance Read MergePolicy
deriving stock instance Read BloomFilterAlloc
deriving stock instance Read FencePointerIndex

{-------------------------------------------------------------------------------
  Encoding and decoding
-------------------------------------------------------------------------------}

encodeSnapshotMetaData :: SnapshotMetaData -> ByteString
encodeSnapshotMetaData = encodingToLazyByteString . encoder

encode :: ToEncoding a => a -> ByteString
encode = encodingToLazyByteString . encoder

-- | TODO: custom, because we do not use toJSON from ToJSON
class ToEncoding a where
  encoder :: a -> Encoding

instance ToEncoding SnapshotMetaData where
  encoder :: SnapshotMetaData -> Encoding
  encoder snap = pairs (
         explicitToField encoder "version" version
      <> explicitToField encoder "label"   label
      <> explicitToField encoder "config"  config
      <> explicitToField encoder "tree"    levels
      )
    where
      SnapshotMetaData
        (version :: SnapshotVersion)
        (label :: SnapshotLabel)
        (config :: TableConfig)
        (levels :: SnapLevels)
        = snap

instance ToEncoding SnapshotVersion where
  encoder :: SnapshotVersion -> Encoding
  encoder (SnapshotVersion (n :: Word64)) = word64 n

instance FromJSON SnapshotVersion where
  parseJSON v = SnapshotVersion <$> parseJSON v

instance ToEncoding SnapshotLabel where
  encoder :: SnapshotLabel -> Encoding
  encoder (SnapshotLabel (s :: String)) = string s

instance FromJSON SnapshotLabel where
  parseJSON v = SnapshotLabel <$> parseJSON v

instance ToEncoding TableConfig where
  encoder :: TableConfig -> Encoding
  encoder config = pairs (
         explicitToField encoder "mergePolicy"       mergePolicy
      <> explicitToField encoder "sizeRatio"         sizeRatio
      <> explicitToField encoder "writeBufferAlloc"  writeBufferAlloc
      <> explicitToField encoder "bloomFilterAlloc"  bloomFilterAlloc
      <> explicitToField encoder "fencePointerIndex" fencePointerIndex
      <> explicitToField encoder "diskCachePolicy"   diskCachePolicy
      <> explicitToField encoder "mergeSchedule"     mergeSchedule
      )
    where
      TableConfig
        (mergePolicy :: MergePolicy)
        (sizeRatio :: SizeRatio)
        (writeBufferAlloc :: WriteBufferAlloc)
        (bloomFilterAlloc :: BloomFilterAlloc)
        (fencePointerIndex :: FencePointerIndex)
        (diskCachePolicy :: DiskCachePolicy)
        (mergeSchedule :: MergeSchedule)
        = config

instance ToEncoding MergePolicy where
  encoder :: MergePolicy -> Encoding
  encoder MergePolicyLazyLevelling = string "lazyLevelling"

instance FromJSON MergePolicy where
  parseJSON v = do
    parseJSON v >>= \case
      "lazyLevelling" -> pure MergePolicyLazyLevelling
      (s :: String)   -> fail ("Expected lazyLevelling, but found " <> s)

instance ToEncoding SizeRatio where
  encoder :: SizeRatio -> Encoding
  encoder Four = word64 4

instance FromJSON SizeRatio where
  parseJSON v =
    parseJSON v >>= \case
      (4 :: Int) -> pure Four
      s          -> fail ("Expected 4, but found " <> show s)

instance ToEncoding WriteBufferAlloc where
  encoder :: WriteBufferAlloc -> Encoding
  encoder (AllocNumEntries (numEntries :: NumEntries)) =
      pairs (explicitToField encoder "numEntries" numEntries)

instance ToEncoding NumEntries where
  encoder :: NumEntries -> Encoding
  encoder (NumEntries (x :: Int)) = int x

instance ToEncoding BloomFilterAlloc where
  encoder :: BloomFilterAlloc -> Encoding
  encoder (AllocFixed (x :: Word64)) =
      pairs (explicitToField word64 "allocFixed" x)
  encoder (AllocRequestFPR (x :: Double)) =
      pairs (explicitToField double "allocRequestFPR" x)
  encoder (AllocMonkey (numBytes :: Word64) (numEntries :: NumEntries)) =
      pairs (explicitToField id "allocMonkey" (pairs (
           explicitToField word64  "numBytes"   numBytes
        <> explicitToField encoder "numEntries" numEntries
        )))

instance ToEncoding FencePointerIndex where
  encoder :: FencePointerIndex -> Encoding
  encoder CompactIndex  = string "compactIndex"
  encoder OrdinaryIndex = string "ordinaryIndex"

instance ToEncoding DiskCachePolicy where
  encoder :: DiskCachePolicy -> Encoding
  encoder DiskCacheAll = string "diskCacheAll"
  encoder (DiskCacheLevelsAtOrBelow (x :: Int)) =
      pairs (explicitToField int "diskCacheLevelsAtOrBelow" x)
  encoder DiskCacheNone = string "diskCacheNone"

instance ToEncoding MergeSchedule where
  encoder :: MergeSchedule -> Encoding
  encoder OneShot     = string "oneShot"
  encoder Incremental = string "incremental"

instance ToEncoding SnapLevel where
  encoder :: SnapLevel -> Encoding
  encoder level = pairs (
         explicitToField encoder "incomingRuns" incomingRuns
      <> explicitToField encoder "residentRuns" residentRuns
      )
    where
      SnapLevel
        (incomingRuns :: SnapMergingRun)
        (residentRuns :: V.Vector RunNumber)
        = level

instance ToEncoding a => ToEncoding (V.Vector a) where
  encoder :: V.Vector a -> Encoding
  encoder = vector encoder

vector :: (a -> Encoding) -> V.Vector a -> Encoding
vector enc = list enc . V.toList

instance ToEncoding RunNumber where
  encoder :: RunNumber -> Encoding
  encoder (RunNumber (x :: Word64)) = word64 x

instance ToEncoding SnapMergingRun where
  encoder :: SnapMergingRun -> Encoding
  encoder
    (SnapMergingRun
      (mpfl :: MergePolicyForLevel)
      (nr :: NumRuns)
      (ac :: AccumulatedCredits)
      (smrs :: SnapMergingRunState))
    = pairs (explicitToField id "mergingRun" (pairs (
           explicitToField encoder "mergePolicyForLevel" mpfl
        <> explicitToField encoder "numRuns"             nr
        <> explicitToField encoder "accumulatedCredits"  ac
        <> explicitToField encoder "mergingRunState"     smrs
        )))
  encoder (SnapSingleRun (x :: RunNumber)) =
      pairs (explicitToField encoder "singleRun" x)

instance ToEncoding MergePolicyForLevel where
  encoder :: MergePolicyForLevel -> Encoding
  encoder LevelTiering   = string "tiering"
  encoder LevelLevelling = string "levelling"

instance ToEncoding NumRuns where
  encoder :: NumRuns -> Encoding
  encoder (NumRuns (x :: Int)) = int x

instance ToEncoding AccumulatedCredits where
  encoder :: AccumulatedCredits -> Encoding
  encoder (AccumulatedCredits (x :: Int)) = int x

instance ToEncoding SnapMergingRunState where
  encoder :: SnapMergingRunState -> Encoding
  encoder (SnapCompletedMerge (x :: RunNumber)) =
      pairs (explicitToField id "completedMerge" (
          encoder x
        ))
  encoder
    (SnapOngoingMerge
      (rs :: V.Vector RunNumber)
      (nsd :: NumStepsDone)
      (ml :: Merge.Level))
    = pairs (explicitToField id "ongoingMerge" ( pairs (
           explicitToField encoder "inputRuns" rs
        <> explicitToField encoder "numStepsDone" nsd
        <> explicitToField encoder "mergeLevel" ml
        )))

instance ToEncoding NumStepsDone where
  encoder :: NumStepsDone -> Encoding
  encoder (NumStepsDone (x :: Int)) = int x

instance ToEncoding Merge.Level where
  encoder :: Merge.Level -> Encoding
  encoder Merge.MidLevel  = string "midLevel"
  encoder Merge.LastLevel = string "lastLevel"
