{-# OPTIONS_HADDOCK not-home #-}

-- | Encoders and decoders for snapshot metadata
--
module Database.LSMTree.Internal.Snapshot.Codec (
    -- * Versioning
    SnapshotVersion (..)
  , prettySnapshotVersion
  , currentSnapshotVersion
    -- * Writing and reading files
  , writeFileSnapshotMetaData
  , readFileSnapshotMetaData
  , encodeSnapshotMetaData
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
import           Control.Monad.Class.MonadThrow (Exception (displayException),
                     MonadThrow (..))
import           Data.Bifunctor (Bifunctor (..))
import qualified Data.ByteString.Char8 as BSC
import           Data.ByteString.Lazy (ByteString)
import qualified Data.Map.Strict as Map
import qualified Data.Vector as V
import           Database.LSMTree.Internal.Config
import           Database.LSMTree.Internal.CRC32C
import qualified Database.LSMTree.Internal.CRC32C as FS
import           Database.LSMTree.Internal.MergeSchedule
import qualified Database.LSMTree.Internal.MergingRun as MR
import           Database.LSMTree.Internal.RunBuilder (IndexType (..),
                     RunBloomFilterAlloc (..), RunDataCaching (..),
                     RunParams (..))
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

{-# SPECIALISE
  writeFileSnapshotMetaData ::
       HasFS IO h
    -> FsPath
    -> FsPath
    -> SnapshotMetaData
    -> IO ()
  #-}
-- | Encode 'SnapshotMetaData' and write it to 'SnapshotMetaDataFile'.
--
-- In the presence of exceptions, newly created files will not be removed. It is
-- up to the user of this function to clean up the files.
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

encodeSnapshotMetaData :: SnapshotMetaData -> ByteString
encodeSnapshotMetaData = toLazyByteString . encode . Versioned

{-# SPECIALISE
  readFileSnapshotMetaData ::
       HasFS IO h
    -> FsPath
    -> FsPath
    -> IO SnapshotMetaData
  #-}
-- | Read from 'SnapshotMetaDataFile' and attempt to decode it to
-- 'SnapshotMetaData'.
readFileSnapshotMetaData ::
     (MonadThrow m)
  => HasFS m h
  -> FsPath -- ^ Source file for snapshot metadata
  -> FsPath -- ^ Source file for checksum
  -> m SnapshotMetaData
readFileSnapshotMetaData hfs contentPath checksumPath = do
    checksumFile <- readChecksumsFile hfs checksumPath
    let checksumFileName = ChecksumsFileName (BSC.pack "metadata")

    expectedChecksum <- getChecksum hfs checksumPath checksumFile checksumFileName

    (lbs, actualChecksum) <- FS.withFile hfs contentPath FS.ReadMode $ \h -> do
      n <- FS.hGetSize hfs h
      FS.hGetExactlyCRC32C hfs h n initialCRC32C

    expectChecksum hfs contentPath expectedChecksum actualChecksum

    expectValidFile hfs contentPath FormatSnapshotMetaData (decodeSnapshotMetaData lbs)

decodeSnapshotMetaData :: ByteString -> Either String SnapshotMetaData
decodeSnapshotMetaData lbs = bimap displayException (getVersioned . snd) (deserialiseFromBytes decode lbs)

{-------------------------------------------------------------------------------
  Encoding and decoding
-------------------------------------------------------------------------------}

class Encode a where
  encode :: a -> Encoding

-- | Decoder that is not parameterised by a 'SnapshotVersion'.
--
-- Used only for 'SnapshotVersion' and 'Versioned', which live outside the
-- 'SnapshotMetaData' type hierarchy.
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
  encode (SnapshotMetaData label config writeBuffer levels mergingTree) =
         encodeListLen 5
      <> encode label
      <> encode config
      <> encode writeBuffer
      <> encode levels
      <> encodeMaybe mergingTree

instance DecodeVersioned SnapshotMetaData where
  decodeVersioned ver@V0 = do
      _ <- decodeListLenOf 5
      SnapshotMetaData
        <$> decodeVersioned ver
        <*> decodeVersioned ver
        <*> decodeVersioned ver
        <*> decodeVersioned ver
        <*> decodeMaybe ver

-- SnapshotLabel

instance Encode SnapshotLabel where
  encode (SnapshotLabel s) = encodeString s

instance DecodeVersioned SnapshotLabel where
  decodeVersioned V0 = SnapshotLabel <$> decodeString

instance Encode SnapshotRun where
  encode SnapshotRun { snapRunNumber, snapRunCaching, snapRunIndex } =
         encodeListLen 4
      <> encodeWord 0
      <> encode snapRunNumber
      <> encode snapRunCaching
      <> encode snapRunIndex

instance DecodeVersioned SnapshotRun where
  decodeVersioned v@V0 = do
      n <- decodeListLen
      tag <- decodeWord
      case (n, tag) of
        (4, 0) -> do snapRunNumber  <- decodeVersioned v
                     snapRunCaching <- decodeVersioned v
                     snapRunIndex   <- decodeVersioned v
                     pure SnapshotRun{..}
        _ -> fail ("[SnapshotRun] Unexpected combination of list length and tag: " <> show (n, tag))

{-------------------------------------------------------------------------------
  Encoding and decoding: TableConfig
-------------------------------------------------------------------------------}

-- TableConfig

instance Encode TableConfig where
  encode
    ( TableConfig
        { confMergePolicy = mergePolicy
        , confMergeSchedule = mergeSchedule
        , confSizeRatio = sizeRatio
        , confWriteBufferAlloc = writeBufferAlloc
        , confBloomFilterAlloc = bloomFilterAlloc
        , confFencePointerIndex = fencePointerIndex
        , confDiskCachePolicy = diskCachePolicy
        }
      ) =
      encodeListLen 7
        <> encode mergePolicy
        <> encode mergeSchedule
        <> encode sizeRatio
        <> encode writeBufferAlloc
        <> encode bloomFilterAlloc
        <> encode fencePointerIndex
        <> encode diskCachePolicy

instance DecodeVersioned TableConfig where
  decodeVersioned v@V0 = do
      _ <- decodeListLenOf 7
      confMergePolicy <- decodeVersioned v
      confMergeSchedule <- decodeVersioned v
      confSizeRatio <- decodeVersioned v
      confWriteBufferAlloc <- decodeVersioned v
      confBloomFilterAlloc <- decodeVersioned v
      confFencePointerIndex <- decodeVersioned v
      confDiskCachePolicy <- decodeVersioned v
      pure TableConfig {..}

-- MergePolicy

instance Encode MergePolicy where
  encode LazyLevelling = encodeWord 0

instance DecodeVersioned MergePolicy where
  decodeVersioned V0 =  do
      tag <- decodeWord
      case tag of
        0 -> pure LazyLevelling
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
      <> encodeInt numEntries

instance DecodeVersioned WriteBufferAlloc where
  decodeVersioned V0 = do
      _ <- decodeListLenOf 2
      tag <- decodeWord
      case tag of
        0 -> AllocNumEntries <$> decodeInt
        _ -> fail ("[WriteBufferAlloc] Unexpected tag: " <> show tag)

-- RunParams and friends

instance Encode RunParams where
  encode RunParams { runParamCaching, runParamAlloc, runParamIndex } =
         encodeListLen 4
      <> encodeWord 0
      <> encode runParamCaching
      <> encode runParamAlloc
      <> encode runParamIndex

instance DecodeVersioned RunParams where
  decodeVersioned v@V0 = do
      n <- decodeListLen
      tag <- decodeWord
      case (n, tag) of
        (4, 0) -> do runParamCaching <- decodeVersioned v
                     runParamAlloc   <- decodeVersioned v
                     runParamIndex   <- decodeVersioned v
                     pure RunParams{..}
        _ -> fail ("[RunParams] Unexpected combination of list length and tag: " <> show (n, tag))

instance Encode RunDataCaching where
  encode CacheRunData   = encodeWord 0
  encode NoCacheRunData = encodeWord 1

instance DecodeVersioned RunDataCaching where
  decodeVersioned V0 = do
    tag <- decodeWord
    case tag of
      0 -> pure CacheRunData
      1 -> pure NoCacheRunData
      _ -> fail ("[RunDataCaching] Unexpected tag: " <> show tag)

instance Encode IndexType where
  encode Ordinary = encodeWord 0
  encode Compact  = encodeWord 1

instance DecodeVersioned IndexType where
  decodeVersioned V0 = do
    tag <- decodeWord
    case tag of
      0 -> pure Ordinary
      1 -> pure Compact
      _ -> fail ("[IndexType] Unexpected tag: " <> show tag)

instance Encode RunBloomFilterAlloc where
  encode (RunAllocFixed bits) =
         encodeListLen 2
      <> encodeWord 0
      <> encodeDouble bits
  encode (RunAllocRequestFPR fpr) =
         encodeListLen 2
      <> encodeWord 1
      <> encodeDouble fpr

instance DecodeVersioned RunBloomFilterAlloc where
  decodeVersioned V0 = do
      n <- decodeListLen
      tag <- decodeWord
      case (n, tag) of
        (2, 0) -> RunAllocFixed      <$> decodeDouble
        (2, 1) -> RunAllocRequestFPR <$> decodeDouble
        _ -> fail ("[RunBloomFilterAlloc] Unexpected combination of list length and tag: " <> show (n, tag))

-- BloomFilterAlloc

instance Encode BloomFilterAlloc where
  encode (AllocFixed x) =
         encodeListLen 2
      <> encodeWord 0
      <> encodeDouble x
  encode (AllocRequestFPR x) =
         encodeListLen 2
      <> encodeWord 1
      <> encodeDouble x

instance DecodeVersioned BloomFilterAlloc where
  decodeVersioned V0 = do
      n <- decodeListLen
      tag <- decodeWord
      case (n, tag) of
        (2, 0) -> AllocFixed      <$> decodeDouble
        (2, 1) -> AllocRequestFPR <$> decodeDouble
        _ -> fail ("[BloomFilterAlloc] Unexpected combination of list length and tag: " <> show (n, tag))

-- FencePointerIndexType

instance Encode FencePointerIndexType where
  encode CompactIndex  = encodeWord 0
  encode OrdinaryIndex = encodeWord 1

instance DecodeVersioned FencePointerIndexType where
   decodeVersioned V0 = do
      tag <- decodeWord
      case tag of
        0 -> pure CompactIndex
        1 -> pure OrdinaryIndex
        _ -> fail ("[FencePointerIndexType] Unexpected tag: " <> show tag)

-- DiskCachePolicy

instance Encode DiskCachePolicy where
  encode DiskCacheAll =
         encodeListLen 1
      <> encodeWord 0
  encode (DiskCacheLevelOneTo x) =
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
        (2, 1) -> DiskCacheLevelOneTo <$> decodeInt
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

instance Encode r => Encode (SnapLevels r) where
  encode (SnapLevels levels) = encode levels

instance DecodeVersioned r => DecodeVersioned (SnapLevels r) where
  decodeVersioned v@V0 = SnapLevels <$> decodeVersioned v

-- SnapLevel

instance Encode r => Encode (SnapLevel r) where
  encode (SnapLevel incomingRuns residentRuns) =
         encodeListLen 2
      <> encode incomingRuns
      <> encode residentRuns


instance DecodeVersioned r => DecodeVersioned (SnapLevel r) where
  decodeVersioned v@V0 = do
      _ <- decodeListLenOf 2
      SnapLevel <$> decodeVersioned v <*> decodeVersioned v

-- Vector

instance Encode r => Encode (V.Vector r) where
  encode = encodeVector

instance DecodeVersioned r => DecodeVersioned (V.Vector r) where
  decodeVersioned = decodeVector

-- RunNumber

instance Encode RunNumber where
  encode (RunNumber x) = encodeInt x

instance DecodeVersioned RunNumber where
  decodeVersioned V0 = RunNumber <$> decodeInt

-- SnapIncomingRun

instance Encode r => Encode (SnapIncomingRun r) where
  encode (SnapIncomingMergingRun mpfl nd nc smrs) =
       encodeListLen 5
    <> encodeWord 0
    <> encode mpfl
    <> encode nd
    <> encode nc
    <> encode smrs
  encode (SnapIncomingSingleRun x) =
       encodeListLen 2
    <> encodeWord 1
    <> encode x

instance DecodeVersioned r => DecodeVersioned (SnapIncomingRun r) where
  decodeVersioned v@V0 = do
      n <- decodeListLen
      tag <- decodeWord
      case (n, tag) of
        (5, 0) -> SnapIncomingMergingRun
                    <$> decodeVersioned v <*> decodeVersioned v
                    <*> decodeVersioned v <*> decodeVersioned v
        (2, 1) -> SnapIncomingSingleRun <$> decodeVersioned v
        _ -> fail ("[SnapIncomingRun] Unexpected combination of list length and tag: " <> show (n, tag))

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

-- SnapMergingRun

instance (Encode t, Encode r) => Encode (SnapMergingRun t r) where
  encode (SnapCompletedMerge md r) =
         encodeListLen 3
      <> encodeWord 0
      <> encode md
      <> encode r
  encode (SnapOngoingMerge rp mc rs mt) =
         encodeListLen 5
      <> encodeWord 1
      <> encode rp
      <> encode mc
      <> encode rs
      <> encode mt

instance (DecodeVersioned t, DecodeVersioned r) => DecodeVersioned (SnapMergingRun t r) where
  decodeVersioned v@V0 = do
      n <- decodeListLen
      tag <- decodeWord
      case (n, tag) of
        (3, 0) -> SnapCompletedMerge <$> decodeVersioned v
                                     <*> decodeVersioned v
        (5, 1) -> SnapOngoingMerge <$> decodeVersioned v <*> decodeVersioned v
                                   <*> decodeVersioned v <*> decodeVersioned v
        _ -> fail ("[SnapMergingRun] Unexpected combination of list length and tag: " <> show (n, tag))

-- NominalDebt, NominalCredits, MergeDebt and MergeCredits

instance Encode NominalDebt where
  encode (NominalDebt x) = encodeInt x

instance DecodeVersioned NominalDebt where
  decodeVersioned V0 = NominalDebt <$> decodeInt

instance Encode NominalCredits where
  encode (NominalCredits x) = encodeInt x

instance DecodeVersioned NominalCredits where
  decodeVersioned V0 = NominalCredits <$> decodeInt

instance Encode MergeDebt where
  encode (MergeDebt (MergeCredits x)) = encodeInt x

instance DecodeVersioned MergeDebt where
  decodeVersioned V0 = (MergeDebt . MergeCredits) <$> decodeInt

instance Encode MergeCredits where
  encode (MergeCredits x) = encodeInt x

instance DecodeVersioned MergeCredits where
  decodeVersioned V0 = MergeCredits <$> decodeInt

-- MergeType

instance Encode MR.LevelMergeType  where
  encode MR.MergeMidLevel  = encodeWord 0
  encode MR.MergeLastLevel = encodeWord 1

instance DecodeVersioned MR.LevelMergeType where
  decodeVersioned V0 = do
      tag <- decodeWord
      case tag of
        0 -> pure MR.MergeMidLevel
        1 -> pure MR.MergeLastLevel
        _ -> fail ("[LevelMergeType] Unexpected tag: " <> show tag)

-- | We start the tags for these merge types at an offset. This way, if we
-- serialise @MR.MergeMidLevel :: MR.LevelMergeType@ as 0 and then accidentally
-- try deserialising it as a @MR.TreeMergeType@, that will fail.
--
-- However, 'MR.LevelMergeType' and 'MR.TreeMergeType' are only different
-- (overlapping) subsets of 'MR.MergeType'. In particular, 'MR.MergeLastLevel'
-- and 'MR.MergeLevel' are semantically the same. Encoding them as the same
-- number leaves the door open to relaxing the restrictions on which merge types
-- can occur where, e.g. decoding them as a general 'MR.MergeType', without
-- having to change the file format.
instance Encode MR.TreeMergeType  where
  encode MR.MergeLevel = encodeWord 1
  encode MR.MergeUnion = encodeWord 2

instance DecodeVersioned MR.TreeMergeType where
  decodeVersioned V0 = do
      tag <- decodeWord
      case tag of
        1 -> pure MR.MergeLevel
        2 -> pure MR.MergeUnion
        _ -> fail ("[TreeMergeType] Unexpected tag: " <> show tag)

{-------------------------------------------------------------------------------
  Encoding and decoding: SnapMergingTree
-------------------------------------------------------------------------------}

-- SnapMergingTree

instance Encode r => Encode (SnapMergingTree r) where
  encode (SnapMergingTree tState) = encode tState

instance DecodeVersioned r => DecodeVersioned (SnapMergingTree r) where
  decodeVersioned ver@V0 = SnapMergingTree <$> decodeVersioned ver

-- SnapMergingTreeState

instance Encode r => Encode (SnapMergingTreeState r) where
  encode (SnapCompletedTreeMerge x) =
       encodeListLen 2
    <> encodeWord 0
    <> encode x
  encode (SnapPendingTreeMerge x) =
       encodeListLen 2
    <> encodeWord 1
    <> encode x
  encode (SnapOngoingTreeMerge smrs) =
       encodeListLen 2
    <> encodeWord 2
    <> encode smrs

instance DecodeVersioned r => DecodeVersioned (SnapMergingTreeState r) where
  decodeVersioned v@V0 = do
      n <- decodeListLen
      tag <- decodeWord
      case (n, tag) of
        (2, 0) -> SnapCompletedTreeMerge <$> decodeVersioned v
        (2, 1) -> SnapPendingTreeMerge <$> decodeVersioned v
        (2, 2) -> SnapOngoingTreeMerge <$> decodeVersioned v
        _ -> fail ("[SnapMergingTreeState] Unexpected combination of list length and tag: " <> show (n, tag))

-- SnapPendingMerge

instance Encode r => Encode (SnapPendingMerge r) where
  encode (SnapPendingLevelMerge pe mt) =
      encodeListLen 3
   <> encodeWord 0
   <> encodeList pe
   <> encodeMaybe mt
  encode (SnapPendingUnionMerge mts) =
      encodeListLen 2
   <> encodeWord 1
   <> encodeList mts

instance DecodeVersioned r => DecodeVersioned (SnapPendingMerge r) where
  decodeVersioned v@V0 = do
      n <- decodeListLen
      tag <- decodeWord
      case (n, tag) of
        (3, 0) -> SnapPendingLevelMerge <$> decodeList v <*> decodeMaybe v
        (2, 1) -> SnapPendingUnionMerge <$> decodeList v
        _ -> fail ("[SnapPendingMerge] Unexpected combination of list length and tag: " <> show (n, tag))

-- SnapPreExistingRun

instance Encode r => Encode (SnapPreExistingRun r) where
  encode (SnapPreExistingRun x) =
       encodeListLen 2
    <> encodeWord 0
    <> encode x
  encode (SnapPreExistingMergingRun smrs) =
       encodeListLen 2
    <> encodeWord 1
    <> encode smrs

instance DecodeVersioned r => DecodeVersioned (SnapPreExistingRun r) where
  decodeVersioned v@V0 = do
      n <- decodeListLen
      tag <- decodeWord
      case (n, tag) of
        (2, 0) -> SnapPreExistingRun <$> decodeVersioned v
        (2, 1) -> SnapPreExistingMergingRun <$> decodeVersioned v
        _ -> fail ("[SnapPreExistingRun] Unexpected combination of list length and tag: " <> show (n, tag))

-- Utilities for encoding/decoding Maybe values

--Note: the Maybe encoding cannot be nested like (Maybe (Maybe a)), but it is
-- ok for fields of records.
encodeMaybe :: Encode a => Maybe a -> Encoding
encodeMaybe = \case
  Nothing -> encodeNull
  Just en -> encode en

decodeMaybe :: DecodeVersioned a => SnapshotVersion -> Decoder s (Maybe a)
decodeMaybe v@V0 = do
    tok <- peekTokenType
    case tok of
      TypeNull -> Nothing <$ decodeNull
      _        -> Just <$> decodeVersioned v

encodeList :: Encode a => [a] -> Encoding
encodeList xs =
    encodeListLen (fromIntegral (length xs))
 <> foldr (\x r -> encode x <> r) mempty xs

decodeList :: DecodeVersioned a => SnapshotVersion -> Decoder s [a]
decodeList v = do
    n <- decodeListLen
    decodeSequenceLenN (flip (:)) [] reverse n (decodeVersioned v)

encodeVector :: Encode a => V.Vector a -> Encoding
encodeVector xs =
    encodeListLen (fromIntegral (V.length xs))
 <> foldr (\x r -> encode x <> r) mempty xs

decodeVector :: DecodeVersioned a => SnapshotVersion -> Decoder s (V.Vector a)
decodeVector v = do
    n <- decodeListLen
    decodeSequenceLenN (flip (:)) [] (V.reverse . V.fromList)
                       n (decodeVersioned v)
