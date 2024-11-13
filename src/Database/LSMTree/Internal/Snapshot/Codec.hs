-- | Encoders and decoders for snapshot metadata
module Database.LSMTree.Internal.Snapshot.Codec (
    -- * Versioning
    SnapshotVersion (..)
  , prettySnapshotVersion
  , currentSnapshotVersion
    -- * Writing and reading files
  , writeFileSnapshotMetaData
  , readFileSnapshotMetaData
  , encodeSnapshotMetaData
  , decodeSnapshotMetaData
    -- * Encoding and decoding
  , Encode (..)
  , Decode (..)
  , DecodeVersioned (..)
  , Versioned (..)
  ) where

import           Codec.CBOR.Decoding
import           Codec.CBOR.Encoding
import           Codec.CBOR.Read
import           Codec.CBOR.Write
import           Control.Monad (when)
import           Control.Monad.Class.MonadThrow (MonadThrow (..))
import           Data.Bifunctor
import qualified Data.ByteString.Char8 as BSC
import           Data.ByteString.Lazy (ByteString)
import qualified Data.Map.Strict as Map
import qualified Data.Vector as V
import           Database.LSMTree.Internal.Config
import           Database.LSMTree.Internal.CRC32C
import qualified Database.LSMTree.Internal.CRC32C as FS
import           Database.LSMTree.Internal.Entry
import qualified Database.LSMTree.Internal.Merge as Merge
import           Database.LSMTree.Internal.MergeSchedule
import           Database.LSMTree.Internal.Run (ChecksumError (..),
                     FileFormatError (..))
import           Database.LSMTree.Internal.RunNumber
import           Database.LSMTree.Internal.Snapshot
import qualified System.FS.API as FS
import           System.FS.API (FsPath, HasFS)
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
  Writing and reading files
-------------------------------------------------------------------------------}

-- | Encode 'SnapshotMetaData' and write it to 'SnapshotMetaDataFile'.
writeFileSnapshotMetaData ::
     MonadThrow m
  => HasFS m h
  -> FsPath -- ^ Target file for snapshot metadata
  -> FsPath -- ^ Target file for checksum
  -> SnapshotMetaData
  -> m ()
writeFileSnapshotMetaData hfs contentPath checksumPath snapMetaData = do
    (_, checksum) <-
      FS.withFile hfs contentPath (FS.WriteMode FS.MustBeNew) $ \h ->
        hPutAllChunksCRC32C hfs h (encodeSnapshotMetaData snapMetaData) initialCRC32C

    let checksumFileName = ChecksumsFileName (BSC.pack "metadata")
        checksumFile = Map.singleton checksumFileName checksum
    writeChecksumsFile hfs checksumPath checksumFile

-- | Read from 'SnapshotMetaDataFile' and attempt to decode it to
-- 'SnapshotMetaData'.
readFileSnapshotMetaData ::
     MonadThrow m
  => HasFS m h
  -> FsPath -- ^ Source file for snapshot metadata
  -> FsPath -- ^ Source file for checksum
  -> m (Either DeserialiseFailure SnapshotMetaData)
readFileSnapshotMetaData hfs contentPath checksumPath = do
    checksumFile <- readChecksumsFile hfs checksumPath
    let checksumFileName = ChecksumsFileName (BSC.pack "metadata")

    expectedChecksum <-
      case Map.lookup checksumFileName checksumFile of
        Nothing ->
          throwIO $ FileFormatError
                      checksumPath
                      ("key not found: " <> show checksumFileName)
        Just checksum -> pure checksum

    (lbs, actualChecksum) <- FS.withFile hfs contentPath FS.ReadMode $ \h -> do
      n <- FS.hGetSize hfs h
      FS.hGetExactlyCRC32C hfs h n initialCRC32C

    when (expectedChecksum /= actualChecksum) $
      throwIO $ ChecksumError contentPath expectedChecksum actualChecksum

    pure $ decodeSnapshotMetaData lbs

encodeSnapshotMetaData :: SnapshotMetaData -> ByteString
encodeSnapshotMetaData = toLazyByteString . encode . Versioned

decodeSnapshotMetaData :: ByteString -> Either DeserialiseFailure SnapshotMetaData
decodeSnapshotMetaData bs = second (getVersioned . snd) (deserialiseFromBytes decode bs)

{-------------------------------------------------------------------------------
  Encoding and decoding
-------------------------------------------------------------------------------}

class Encode a where
  encode :: a -> Encoding

-- | Decoder that is not parameterised by a 'SnapshotVersion'.
--
-- Used only for 'SnapshotVersion' and 'Versioned', which live outside the
-- 'SnapshotMetaData' type hierachy.
class Decode a where
  decode :: Decoder s a

-- | Decoder parameterised by a 'SnapshotVersion'.
--
-- Used for every type in the 'SnapshotMetaData' type hierarchy.
class DecodeVersioned a where
  decodeVersioned :: SnapshotVersion -> Decoder s a

newtype Versioned a = Versioned { getVersioned :: a }
  deriving stock (Show, Eq)

instance Encode a => Encode (Versioned a) where
  encode (Versioned x) =
       encodeListLen 2
    <> encode currentSnapshotVersion
    <> encode x

-- | Decodes a 'SnapshotVersion' first, and then passes that into the versioned
-- decoder for @a@.
instance DecodeVersioned a => Decode (Versioned a) where
  decode = do
      _ <- decodeListLenOf 2
      version <- decode
      case isCompatible version of
        Right () -> pure ()
        Left errMsg ->
          fail $
            printf "Incompatible snapshot format version found. Version %s \
                   \is not backwards compatible with version %s : %s"
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
        _ -> fail ("Unknown snapshot format version number: " <>  show ver)

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
  encode SnapNormalTable   = encodeWord 0
  encode SnapMonoidalTable = encodeWord 1

instance DecodeVersioned SnapshotTableType where
  decodeVersioned V0 = do
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
  encode MergePolicyLazyLevelling = encodeWord 0

instance DecodeVersioned MergePolicy where
  decodeVersioned V0 =  do
      tag <- decodeWord
      case tag of
        0 -> pure MergePolicyLazyLevelling
        _ -> fail ("[MergePolicy] Unexpected tag: " <> show tag)

-- SizeRatio

instance Encode SizeRatio where
  encode Four = encodeInt 4

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
  encode CompactIndex  = encodeWord 0
  encode OrdinaryIndex = encodeWord 1

instance DecodeVersioned FencePointerIndex where
   decodeVersioned V0 = do
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
  encode OneShot     = encodeWord 0
  encode Incremental = encodeWord 1

instance DecodeVersioned MergeSchedule where
  decodeVersioned V0 = do
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

-- SnapIncomingRun

instance Encode SnapIncomingRun where
  encode (SnapMergingRun mpfl nr ne uc mkc smrs) =
       encodeListLen 7
    <> encodeWord 0
    <> encode mpfl
    <> encode nr
    <> encode ne
    <> encode uc
    <> encode mkc
    <> encode smrs
  encode (SnapSingleRun x) =
       encodeListLen 2
    <> encodeWord 1
    <> encode x

instance DecodeVersioned SnapIncomingRun where
  decodeVersioned v@V0 = do
      n <- decodeListLen
      tag <- decodeWord
      case (n, tag) of
        (7, 0) -> SnapMergingRun <$>
          decodeVersioned v <*> decodeVersioned v <*> decodeVersioned v <*>
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
  encode LevelTiering   = encodeWord 0
  encode LevelLevelling = encodeWord 1

instance DecodeVersioned MergePolicyForLevel where
  decodeVersioned V0 = do
      tag <- decodeWord
      case tag of
        0 -> pure LevelTiering
        1 -> pure LevelLevelling
        _ -> fail ("[MergePolicyForLevel] Unexpected tag: " <> show tag)

-- UnspentCredits

instance Encode UnspentCredits where
  encode (UnspentCredits x) = encodeInt x

instance DecodeVersioned UnspentCredits where
  decodeVersioned V0 = UnspentCredits <$> decodeInt

-- MergeKnownCompleted

instance Encode MergeKnownCompleted where
  encode MergeKnownCompleted = encodeWord 0
  encode MergeMaybeCompleted = encodeWord 1

instance DecodeVersioned MergeKnownCompleted where
  decodeVersioned V0 = do
      tag <- decodeWord
      case tag of
        0 -> pure MergeKnownCompleted
        1 -> pure MergeMaybeCompleted
        _ -> fail ("[MergeKnownCompleted] Unexpected tag: " <> show tag)

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

-- SpentCredits

instance Encode SpentCredits where
  encode (SpentCredits x) = encodeInt x

instance DecodeVersioned SpentCredits where
  decodeVersioned V0 = SpentCredits <$> decodeInt

  -- Merge.Level

instance Encode Merge.Level where
  encode Merge.MidLevel  = encodeWord 0
  encode Merge.LastLevel = encodeWord 1

instance DecodeVersioned Merge.Level where
  decodeVersioned V0 = do
      tag <- decodeWord
      case tag of
        0 -> pure Merge.MidLevel
        1 -> pure Merge.LastLevel
        _ -> fail ("[Merge.Level] Unexpected tag: " <> show tag)
