module Database.LSMTree.Internal.Snapshot (
    -- * Versioning
    SnapshotVersion (..)
  , prettySnapshotVersion
  , currentSnapshotVersion
    -- * Snapshot metadata
  , SnapshotMetaData (..)
  , SnapshotLabel (..)
  , SnapshotTableType (..)
    -- * Snapshot format
  , numSnapRuns
  , SnapLevels
  , SnapLevel (..)
  , SnapMergingRun (..)
  , SnapMergingRunState (..)
  , TotalCredits (..)
    -- * Creating snapshots
  , snapLevels
    -- * Opening snapshots
  , openLevels
    -- * Encoding and decoding
  , Encode (..)
  , Decode (..)
  , DecodeVersioned (..)
  , encodeSnapshotMetaData
  , decodeSnapshotMetaData
  , Versioned (..)
  ) where

import           Codec.CBOR.Decoding
import           Codec.CBOR.Encoding
import           Codec.CBOR.Read
import           Codec.CBOR.Write
import           Control.Concurrent.Class.MonadMVar.Strict
import           Control.Concurrent.Class.MonadSTM (MonadSTM)
import           Control.Monad.Class.MonadST (MonadST)
import           Control.Monad.Class.MonadThrow (MonadMask)
import           Control.Monad.Fix (MonadFix)
import           Control.Monad.Primitive (PrimMonad)
import           Control.TempRegistry
import           Data.Bifunctor
import           Data.ByteString.Lazy (ByteString)
import           Data.Foldable (forM_)
import           Data.Primitive.PrimVar
import           Data.Text (Text)
import qualified Data.Vector as V
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
import           Text.Printf

{-------------------------------------------------------------------------------
  Versioning
-------------------------------------------------------------------------------}

-- | The version of a snapshot.
--
-- A snapshot format version is a number. Version numbers are consecutive and
-- increasing. A single release of the library may support several older
-- snapshot format versions, and thereby provide backwards compatibility.
-- Support for old versions is not guaranteed indefinitely, but backwards
-- compatibility is guaranteed for at least the previous version, and preferably
-- for more. Forwards compatibility is not provided at all: snapshots with a
-- later version than the current version for the library release will always
-- fail.
data SnapshotVersion = V0
  deriving stock (Show, Eq)

-- >>> prettySnapshotVersion currentSnapshotVersion
-- "v0"
prettySnapshotVersion :: SnapshotVersion -> String
prettySnapshotVersion V0 = "v0"

-- >>> currentSnapshotVersion
-- V0
currentSnapshotVersion :: SnapshotVersion
currentSnapshotVersion = V0

isCompatible :: SnapshotVersion -> Either String ()
isCompatible otherVersion = do
    case ( currentSnapshotVersion, otherVersion ) of
      (V0, V0) -> Right ()

{-------------------------------------------------------------------------------
  Snapshot metadata
-------------------------------------------------------------------------------}

-- | Custom text to include in a snapshot file
newtype SnapshotLabel = SnapshotLabel Text
  deriving stock (Show, Eq)

data SnapshotTableType = SnapNormalTable | SnapMonoidalTable
  deriving stock (Show, Eq)

data SnapshotMetaData = SnapshotMetaData {
    -- | Custom, user-supplied text that is included in the metadata.
    --
    -- The main use case for this field is for the user to supply textual
    -- information about the key\/value\/blob type for the table that
    -- corresponds to the snapshot. This information can then be used to
    -- dynamically check that a snapshot is opened at the correct
    -- key\/value\/blob type.
    --
    -- One could argue that the 'SnapshotName' could be used to to hold this
    -- information, but the file name of snapshot meta data is not guarded by a
    -- checksum, wherease the contents of the file are. Therefore using the
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
  , snapMetaLevels    :: !SnapLevels
  }
  deriving stock (Show, Eq)

{-------------------------------------------------------------------------------
  Levels snapshot format
-------------------------------------------------------------------------------}

numSnapRuns :: SnapLevels -> Int
numSnapRuns sl = V.sum $ V.map go1 sl
  where
    go1 (SnapLevel sir srr) = go2 sir + V.length srr
    go2 (SnapMergingRun _ _ smrs) = go3 smrs
    go2 (SnapSingleRun _rn)       = 1
    go3 (SnapCompletedMerge _rn)   = 1
    go3 (SnapOngoingMerge rns _ _) = V.length rns

type SnapLevels = V.Vector SnapLevel

data SnapLevel = SnapLevel {
    snapIncomingRuns :: !SnapMergingRun
  , snapResidentRuns :: !(V.Vector RunNumber)
  }
  deriving stock (Show, Eq)

data SnapMergingRun =
    SnapMergingRun !MergePolicyForLevel !NumRuns !SnapMergingRunState
  | SnapSingleRun !RunNumber
  deriving stock (Show, Eq)

data SnapMergingRunState =
    SnapCompletedMerge !RunNumber
  | SnapOngoingMerge !(V.Vector RunNumber) !TotalCredits !Merge.Level
  deriving stock (Show, Eq)

-- | The total number of supplied credits. This total is used on snapshot load
-- to restore merging work that was lost when the snapshot was created.
newtype TotalCredits = TotalCredits Int
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
snapMergingRun (MergingRun mpfl nr mrsVar) = do
    smrs <- withMVar mrsVar $ \mrs -> snapMergingRunState mrs
    pure (SnapMergingRun mpfl nr smrs)
snapMergingRun (SingleRun r) = pure (SnapSingleRun (runNumber r))

{-# SPECIALISE snapMergingRunState :: MergingRunState IO h -> IO SnapMergingRunState #-}
snapMergingRunState ::
     PrimMonad m
  => MergingRunState m h
  -> m SnapMergingRunState
snapMergingRunState (CompletedMerge r) = pure (SnapCompletedMerge (runNumber r))
-- No need to snapshot the contents of totalStepsVar here, since we still start
-- ocunting from 0 again when loading the snapshot. We do need to know how many
-- credits were supplied, so we snapshot that part.
snapMergingRunState (OngoingMerge rs _totalStepsVar totalCreditsVar m) = do
    tc <- readPrimVar totalCreditsVar
    pure (SnapOngoingMerge (V.map runNumber rs) (TotalCredits tc) (Merge.mergeLevel m))

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
        -- redo merging work here. TotalCredits tracks how many credits were
        -- supplied before the snapshot was taken.
        --
        -- TODO: this use of supplyMergeCredits is leaky! If a merge completes
        -- in supplyMergeCredits, then the resulting run is not tracked in the
        -- registry, and closing the input runs is also not tracked in the
        -- registry. Note, however, that this bit of code is likely to change in
        -- #392.
        forM_ mmmay $ \(TotalCredits c) -> supplyMergeCredits (ScaledCredits c) incomingRuns
        residentRuns <- V.forM snapResidentRuns $ \rn ->
          allocateTemp reg
            (Run.openFromDisk hfs hbio caching (mkPath rn))
            Run.removeReference
        pure Level{..}
      where
        caching = diskCachePolicyForLevel confDiskCachePolicy ln
        alloc = bloomFilterAllocForLevel conf ln

        openMergingRun :: SnapMergingRun -> m (Maybe TotalCredits, MergingRun m h)
        openMergingRun (SnapMergingRun mpfl nr smrs) = do
            (n, mrs) <- openMergingRunState smrs
            (n,) . MergingRun mpfl nr <$> newMVar mrs
        openMergingRun (SnapSingleRun rn) =
            (Nothing,) . SingleRun <$>
              allocateTemp reg
                (Run.openFromDisk hfs hbio caching (mkPath rn))
                Run.removeReference

        openMergingRunState :: SnapMergingRunState -> m (Maybe TotalCredits, MergingRunState m h)
        openMergingRunState (SnapCompletedMerge rn) =
            (Nothing,) . CompletedMerge <$>
              allocateTemp reg
                (Run.openFromDisk hfs hbio caching (mkPath rn))
                Run.removeReference
        openMergingRunState (SnapOngoingMerge rns totalCredits mergeLast) = do
            rs <- V.forM rns $ \rn ->
              allocateTemp reg
                (Run.openFromDisk hfs hbio caching ((mkPath rn)))
                Run.removeReference
            -- Initialse the variable with 0, since at this point no merge steps
            -- have been performed yet.
            totalStepsVar <- newPrimVar 0
            -- Initialise the variable with 0. Credits will be re-supplied later
            -- by @supplyMergeCredits totalCredits@, which will ensure that
            -- @totalCreditsVar@ holds @totalCredits@ afterwards.
            totalCreditsVar <- newPrimVar 0
            rn <- uniqueToRunNumber <$> incrUniqCounter uc
            mergeMaybe <- allocateMaybeTemp reg
              (Merge.new hfs hbio caching alloc mergeLast resolve (mkPath rn) rs)
              Merge.removeReference
            case mergeMaybe of
              Nothing -> error "openLevels: merges can not be empty"
              Just m  -> pure (Just totalCredits, OngoingMerge rs totalStepsVar totalCreditsVar m)

{-------------------------------------------------------------------------------
  Encoding and decoding
-------------------------------------------------------------------------------}

class Encode a where
  encode :: a -> Encoding

class Decode a where
  decode :: Decoder s a

class DecodeVersioned a where
  decodeVersioned :: SnapshotVersion -> Decoder s a

encodeSnapshotMetaData :: SnapshotMetaData -> ByteString
encodeSnapshotMetaData = toLazyByteString . encode . Versioned

decodeSnapshotMetaData :: ByteString -> Either DeserialiseFailure SnapshotMetaData
decodeSnapshotMetaData bs = second (getVersioned . snd) (deserialiseFromBytes decode bs)

newtype Versioned a = Versioned { getVersioned :: a }
  deriving stock (Show, Eq)

instance Encode a => Encode (Versioned a) where
  encode (Versioned x) =
       encodeListLen 2
    <> encode currentSnapshotVersion
    <> encode x

instance DecodeVersioned a => Decode (Versioned a) where
  decode = do
      _ <- decodeListLenOf 2
      version <- decode
      case isCompatible version of
        Right () -> pure ()
        Left errMsg ->
          fail $
            printf "Incompatible version found. Version %s is not backwards \
                   \compatible with version %s : %s"
                   (prettySnapshotVersion currentSnapshotVersion)
                   (prettySnapshotVersion version)
                   errMsg
      Versioned <$> decodeVersioned version

{-------------------------------------------------------------------------------
  Encoding and decoding: Versioning
-------------------------------------------------------------------------------}

instance Encode SnapshotVersion where
  encode ver =
         encodeListLen 1
      <> case ver of
           V0 -> encodeWord 0

instance Decode SnapshotVersion where
  decode = do
      _ <- decodeListLenOf 1
      ver <- decodeWord
      case ver of
        0 -> pure V0
        _ -> fail ("Unknown version number: " <>  show ver)

{-------------------------------------------------------------------------------
  Encoding and decoding: SnapshotMetaData
-------------------------------------------------------------------------------}

-- SnapshotMetaData

instance Encode SnapshotMetaData where
  encode (SnapshotMetaData label tableType config levels) =
         encodeListLen 4
      <> encode label
      <> encode tableType
      <> encode config
      <> encode levels

instance DecodeVersioned SnapshotMetaData where
  decodeVersioned ver@V0 = do
      _ <- decodeListLenOf 4
      SnapshotMetaData
        <$> decodeVersioned ver <*> decodeVersioned ver
        <*> decodeVersioned ver <*> decodeVersioned ver

-- SnapshotLabel

instance Encode SnapshotLabel where
  encode (SnapshotLabel s) = encodeString s

instance DecodeVersioned SnapshotLabel where
  decodeVersioned V0 = SnapshotLabel <$> decodeString

-- TableType

instance Encode SnapshotTableType where
  encode SnapNormalTable   = encodeListLen 1 <> encodeWord 0
  encode SnapMonoidalTable = encodeListLen 1 <> encodeWord 1

instance DecodeVersioned SnapshotTableType where
  decodeVersioned V0 = do
      _ <- decodeListLenOf 1
      tag <- decodeWord
      case tag of
        0 -> pure SnapNormalTable
        1 -> pure SnapMonoidalTable
        _ -> fail ("[SnapshotTableType] Unexpected tag: " <> show tag)

{-------------------------------------------------------------------------------
  Encoding and decoding: TableConfig
-------------------------------------------------------------------------------}

-- TableConfig

instance Encode TableConfig where
  encode config =
         encodeListLen 7
      <> encode mergePolicy
      <> encode sizeRatio
      <> encode writeBufferAlloc
      <> encode bloomFilterAlloc
      <> encode fencePointerIndex
      <> encode diskCachePolicy
      <> encode mergeSchedule
    where
      TableConfig
        mergePolicy
        sizeRatio
        writeBufferAlloc
        bloomFilterAlloc
        fencePointerIndex
        diskCachePolicy
        mergeSchedule
        = config

instance DecodeVersioned TableConfig where
  decodeVersioned v@V0 = do
      _ <- decodeListLenOf 7
      TableConfig
        <$> decodeVersioned v <*> decodeVersioned v <*> decodeVersioned v
        <*> decodeVersioned v <*> decodeVersioned v <*> decodeVersioned v
        <*> decodeVersioned v

-- MergePolicy

instance Encode MergePolicy where
  encode MergePolicyLazyLevelling =
      encodeListLen 1 <> encodeWord 0

instance DecodeVersioned MergePolicy where
  decodeVersioned V0 =  do
      _ <- decodeListLenOf 1
      tag <- decodeWord
      case tag of
        0 -> pure MergePolicyLazyLevelling
        _ -> fail ("[MergePolicy] Unexpected tag: " <> show tag)

-- SizeRatio

instance Encode SizeRatio where
  encode Four = encodeWord64 4

instance DecodeVersioned SizeRatio where
  decodeVersioned V0 = do
      x <- decodeWord64
      case x of
        4 -> pure Four
        _ -> fail ("Expected 4, but found " <> show x)

-- WriteBufferAlloc

instance Encode WriteBufferAlloc where
  encode (AllocNumEntries numEntries) =
         encodeListLen 2
      <> encodeWord 0
      <> encode numEntries

instance DecodeVersioned WriteBufferAlloc where
  decodeVersioned v@V0 = do
      _ <- decodeListLenOf 2
      tag <- decodeWord
      case tag of
        0 -> AllocNumEntries <$> decodeVersioned v
        _ -> fail ("[WriteBufferAlloc] Unexpected tag: " <> show tag)

-- NumEntries

instance Encode NumEntries where
  encode (NumEntries x) = encodeInt x

instance DecodeVersioned NumEntries where
  decodeVersioned V0 = NumEntries <$> decodeInt

-- BloomFilterAlloc

instance Encode BloomFilterAlloc where
  encode (AllocFixed x) =
         encodeListLen 2
      <> encodeWord 0
      <> encodeWord64 x
  encode (AllocRequestFPR x) =
         encodeListLen 2
      <> encodeWord 1
      <> encodeDouble x
  encode (AllocMonkey numBytes numEntries) =
         encodeListLen 3
      <> encodeWord 2
      <> encodeWord64 numBytes
      <> encode numEntries

instance DecodeVersioned BloomFilterAlloc where
  decodeVersioned v@V0 = do
      n <- decodeListLen
      tag <- decodeWord
      case (n, tag) of
        (2, 0) -> AllocFixed <$> decodeWord64
        (2, 1) -> AllocRequestFPR <$> decodeDouble
        (3, 2) -> AllocMonkey <$> decodeWord64 <*> decodeVersioned v
        _ -> fail ("[BloomFilterAlloc] Unexpected combination of list length and tag: " <> show (n, tag))

-- FencePointerIndex

instance Encode FencePointerIndex where
  encode CompactIndex  = encodeListLen 1 <> encodeWord 0
  encode OrdinaryIndex = encodeListLen 1 <> encodeWord 1

instance DecodeVersioned FencePointerIndex where
   decodeVersioned V0 = do
      _ <- decodeListLenOf 1
      tag <- decodeWord
      case tag of
        0 -> pure CompactIndex
        1 -> pure OrdinaryIndex
        _ -> fail ("[FencePointerIndex] Unexpected tag: " <> show tag)

-- DiskCachePolicy

instance Encode DiskCachePolicy where
  encode DiskCacheAll =
         encodeListLen 1
      <> encodeWord 0
  encode (DiskCacheLevelsAtOrBelow x) =
         encodeListLen 2
      <> encodeWord 1
      <> encodeInt x
  encode DiskCacheNone =
         encodeListLen 1
      <> encodeWord 2

instance DecodeVersioned DiskCachePolicy where
  decodeVersioned V0 = do
      n <- decodeListLen
      tag <- decodeWord
      case (n, tag) of
        (1, 0) -> pure DiskCacheAll
        (2, 1) -> DiskCacheLevelsAtOrBelow <$> decodeInt
        (1, 2) -> pure DiskCacheNone
        _ -> fail ("[DiskCachePolicy] Unexpected combination of list length and tag: " <> show (n, tag))

-- MergeSchedule

instance Encode MergeSchedule where
  encode OneShot     = encodeListLen 1 <> encodeWord 0
  encode Incremental = encodeListLen 1 <> encodeWord 1

instance DecodeVersioned MergeSchedule where
  decodeVersioned V0 = do
      _ <- decodeListLenOf 1
      tag <- decodeWord
      case tag of
        0 -> pure OneShot
        1 -> pure Incremental
        _ -> fail ("[MergeSchedule] Unexpected tag: " <> show tag)

{-------------------------------------------------------------------------------
  Encoding and decoding: SnapLevels
-------------------------------------------------------------------------------}

-- SnapLevels

instance Encode SnapLevels where
  encode levels =
         encodeListLen (fromIntegral (V.length levels))
      <> V.foldMap encode levels

instance DecodeVersioned SnapLevels where
  decodeVersioned v@V0 = do
      n <- decodeListLen
      V.replicateM n (decodeVersioned v)

-- SnapLevel

instance Encode SnapLevel where
  encode (SnapLevel incomingRuns residentRuns) =
         encodeListLen 2
      <> encode incomingRuns
      <> encode residentRuns


instance DecodeVersioned SnapLevel where
  decodeVersioned v@V0 = do
      _ <- decodeListLenOf 2
      SnapLevel <$> decodeVersioned v <*> decodeVersioned v

-- Vector RunNumber

instance Encode (V.Vector RunNumber) where
  encode rns =
         encodeListLen (fromIntegral (V.length rns))
      <> V.foldMap encode rns

instance DecodeVersioned (V.Vector RunNumber) where
  decodeVersioned v@V0 = do
      n <- decodeListLen
      V.replicateM n (decodeVersioned v)

-- RunNumber

instance Encode RunNumber where
  encode (RunNumber x) = encodeWord64 x

instance DecodeVersioned RunNumber where
  decodeVersioned V0 = RunNumber <$> decodeWord64

-- SnapMergingRun

instance Encode SnapMergingRun where
  encode (SnapMergingRun mpfl nr smrs) =
       encodeListLen 4
    <> encodeWord 0
    <> encode mpfl
    <> encode nr
    <> encode smrs
  encode (SnapSingleRun x) =
       encodeListLen 2
    <> encodeWord 1
    <> encode x

instance DecodeVersioned SnapMergingRun where
  decodeVersioned v@V0 = do
      n <- decodeListLen
      tag <- decodeWord
      case (n, tag) of
        (4, 0) -> SnapMergingRun <$>
          decodeVersioned v <*> decodeVersioned v <*> decodeVersioned v
        (2, 1) -> SnapSingleRun <$> decodeVersioned v
        _ -> fail ("[SnapMergingRun] Unexpected combination of list length and tag: " <> show (n, tag))

-- NumRuns

instance Encode NumRuns where
  encode (NumRuns x) = encodeInt x

instance DecodeVersioned NumRuns where
  decodeVersioned V0 = NumRuns <$> decodeInt

-- MergePolicyForLevel

instance Encode MergePolicyForLevel where
  encode LevelTiering   = encodeListLen 1 <> encodeWord 0
  encode LevelLevelling = encodeListLen 1 <> encodeWord 1

instance DecodeVersioned MergePolicyForLevel where
  decodeVersioned V0 = do
      _ <- decodeListLenOf 1
      tag <- decodeWord
      case tag of
        0 -> pure LevelTiering
        1 -> pure LevelLevelling
        _ -> fail ("[MergePolicyForLevel] Unexpected tag: " <> show tag)

-- SnapMergingRunState

instance Encode SnapMergingRunState where
  encode (SnapCompletedMerge x) =
         encodeListLen 2
      <> encodeWord 0
      <> encode x
  encode (SnapOngoingMerge rs tc l) =
         encodeListLen 4
      <> encodeWord 1
      <> encode rs
      <> encode tc
      <> encode l

instance DecodeVersioned SnapMergingRunState where
  decodeVersioned v@V0 = do
      n <- decodeListLen
      tag <- decodeWord
      case (n, tag) of
        (2, 0) -> SnapCompletedMerge <$> decodeVersioned v
        (4, 1) -> SnapOngoingMerge <$>
          decodeVersioned v <*> decodeVersioned v <*> decodeVersioned v
        _ -> fail ("[SnapMergingRunState] Unexpected combination of list length and tag: " <> show (n, tag))

-- TotalCredits

instance Encode TotalCredits where
  encode (TotalCredits x) = encodeInt x

instance DecodeVersioned TotalCredits where
  decodeVersioned V0 = TotalCredits <$> decodeInt

  -- Merge.Level

instance Encode Merge.Level where
  encode Merge.MidLevel  = encodeListLen 1 <> encodeWord 0
  encode Merge.LastLevel = encodeListLen 1 <> encodeWord 1

instance DecodeVersioned Merge.Level where
  decodeVersioned V0 = do
      _ <- decodeListLenOf 1
      tag <- decodeWord
      case tag of
        0 -> pure Merge.MidLevel
        1 -> pure Merge.LastLevel
        _ -> fail ("[Merge.Level] Unexpected tag: " <> show tag)
