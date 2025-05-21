{-# LANGUAGE OverloadedStrings    #-}
{-# LANGUAGE UndecidableInstances #-}

module Test.Database.LSMTree.Internal.Snapshot.Codec.Golden (
    tests
  , EnumGolden (..)
  , Annotation
  ) where

import           Codec.CBOR.Write (toLazyByteString)
import           Control.Monad (when)
import qualified Data.ByteString.Lazy as BSL (writeFile)
import qualified Data.Set as Set
import           Data.Typeable
import qualified Data.Vector as V
import           Database.LSMTree.Internal.Config (BloomFilterAlloc (..),
                     DiskCachePolicy (..), FencePointerIndexType (..),
                     MergePolicy (..), MergeSchedule (..), SizeRatio (..),
                     TableConfig (..), WriteBufferAlloc (..))
import           Database.LSMTree.Internal.MergeSchedule
                     (MergePolicyForLevel (..), NominalCredits (..),
                     NominalDebt (..))
import           Database.LSMTree.Internal.MergingRun as MR
import           Database.LSMTree.Internal.RunBuilder (IndexType (..),
                     RunBloomFilterAlloc (..), RunDataCaching (..))
import           Database.LSMTree.Internal.RunNumber (RunNumber (..))
import           Database.LSMTree.Internal.Snapshot
import           Database.LSMTree.Internal.Snapshot.Codec
import qualified System.FS.API as FS
import           System.FS.API.Types (FsPath, MountPoint (..), fsToFilePath,
                     mkFsPath, (<.>))
import           System.FS.IO (HandleIO, ioHasFS)
import           Test.QuickCheck (Property, counterexample, ioProperty, once,
                     (.&&.))
import qualified Test.Tasty as Tasty
import           Test.Tasty (TestTree, testGroup)
import qualified Test.Tasty.Golden as Au
import           Test.Tasty.QuickCheck (testProperty)

tests :: TestTree
tests =
    handleOutputFiles $
    testGroup "Test.Database.LSMTree.Internal.Snapshot.Codec.Golden" $
         concat (forallSnapshotTypes snapshotCodecGoldenTest)
      ++ [testProperty "prop_noUnexpectedOrMissingGoldenFiles" prop_noUnexpectedOrMissingGoldenFiles]

{-------------------------------------------------------------------------------
  Configuration
-------------------------------------------------------------------------------}

-- | The location of the golden file data directory relative to the project root.
goldenDataFilePath :: FilePath
goldenDataFilePath = "test/golden-file-data/snapshot-codec"

goldenDataMountPoint :: MountPoint
goldenDataMountPoint = MountPoint goldenDataFilePath

-- | Delete output files on test-case success.
-- Change the option here if this is undesirable.
handleOutputFiles :: TestTree -> TestTree
handleOutputFiles = Tasty.localOption Au.OnPass

{-------------------------------------------------------------------------------
  Golden tests
-------------------------------------------------------------------------------}

-- | Compare the serialization of snapshot metadata with a known reference file.
snapshotCodecGoldenTest ::
     forall a. (Typeable a, EnumGolden a, Encode a)
  => Proxy a
  -> [TestTree]
snapshotCodecGoldenTest proxy = [
      go (nameGolden proxy annotation) datum
    | (annotation, datum) <- enumGoldenAnnotated' proxy
    ]
  where
    go name datum =
      let -- Various paths
          --
          -- There are three paths for both the checksum and the snapshot files:
          --   1. The filepath of type @FsPath@ to which data is written.
          --   2. The filepath of type @FilePath@ from which data is read.
          --   3. The filepath of type @FilePath@ against which the data is compared.
          --
          -- These file types' bindings have the following infix annotations, respectively:
          --   1. (Fs) for FsPath
          --   2. (Hs) for "Haskell" path
          --   3. (Au) for "Golden file" path
          snapshotFsPath = mkFsPath [name] <.> "snapshot"
          snapshotHsPath = fsToFilePath goldenDataMountPoint snapshotFsPath
          snapshotAuPath = snapshotHsPath <> ".golden"

          -- IO actions
          runnerIO :: FS.HasFS IO HandleIO
          runnerIO = ioHasFS goldenDataMountPoint
          removeIfExists :: FsPath -> IO ()
          removeIfExists fp =
            FS.doesFileExist runnerIO fp >>= (`when` (FS.removeFile runnerIO fp))
          outputAction :: IO ()
          outputAction = do
            -- Ensure that if the output file already exists, we remove it and
            -- re-write out the serialized data. This ensures that there are no
            -- false-positives, false-negatives, or irrelevant I/O exceptions.
            removeIfExists snapshotFsPath
            BSL.writeFile snapshotHsPath . toLazyByteString $ encode datum

      in  Au.goldenVsFile name snapshotAuPath snapshotHsPath outputAction

-- | Check that are no missing or unexpected files in the output directory
prop_noUnexpectedOrMissingGoldenFiles :: Property
prop_noUnexpectedOrMissingGoldenFiles = once $ ioProperty $ do
    let expectedFiles = Set.fromList $ concat $ forallSnapshotTypes filePathsGolden


    let hfs = ioHasFS goldenDataMountPoint
    actualDirectoryEntries <- FS.listDirectory hfs (FS.mkFsPath [])

    let missingFiles = expectedFiles Set.\\ actualDirectoryEntries
        propMissing =
            counterexample ("Missing expected files: " ++ show missingFiles)
          $ counterexample ("Run the golden tests to regenerate the missing files")
          $ (Set.null missingFiles)

    let unexpectedFiles = actualDirectoryEntries Set.\\ expectedFiles
        propUnexpected =
            counterexample ("Found unexpected files: " ++ show unexpectedFiles)
          $ counterexample ("Delete the unexpected files manually from " ++ goldenDataFilePath)
            (Set.null unexpectedFiles)

    pure $ propMissing .&&. propUnexpected

{-------------------------------------------------------------------------------
  Mapping
-------------------------------------------------------------------------------}

type Constraints a = (Typeable a, Encode a, EnumGolden a)

-- | Do something for all snapshot types and collect the results
forallSnapshotTypes ::
     (forall a. Constraints a => Proxy a -> b)
  -> [b]
forallSnapshotTypes f = [
      -- SnapshotMetaData
      f (Proxy @SnapshotMetaData)
    , f (Proxy @SnapshotLabel)
    , f (Proxy @SnapshotRun)
      -- TableConfig
    , f (Proxy @TableConfig)
    , f (Proxy @MergePolicy)
    , f (Proxy @SizeRatio)
    , f (Proxy @WriteBufferAlloc)
    , f (Proxy @BloomFilterAlloc)
    , f (Proxy @FencePointerIndexType)
    , f (Proxy @DiskCachePolicy)
    , f (Proxy @MergeSchedule)
      -- SnapLevels
    , f (Proxy @(SnapLevels SnapshotRun))
    , f (Proxy @(SnapLevel SnapshotRun))
    , f (Proxy @(V.Vector SnapshotRun))
    , f (Proxy @RunNumber)
    , f (Proxy @(SnapIncomingRun SnapshotRun))
    , f (Proxy @MergePolicyForLevel)
    , f (Proxy @RunDataCaching)
    , f (Proxy @RunBloomFilterAlloc)
    , f (Proxy @IndexType)
    , f (Proxy @RunParams)
    , f (Proxy @(SnapMergingRun LevelMergeType SnapshotRun))
    , f (Proxy @MergeDebt)
    , f (Proxy @MergeCredits)
    , f (Proxy @NominalDebt)
    , f (Proxy @NominalCredits)
    , f (Proxy @LevelMergeType)
    , f (Proxy @TreeMergeType)
    , f (Proxy @(SnapMergingTree SnapshotRun))
    , f (Proxy @(SnapMergingTreeState SnapshotRun))
    , f (Proxy @(SnapMergingRun TreeMergeType SnapshotRun))
    , f (Proxy @(SnapPendingMerge SnapshotRun))
    , f (Proxy @(SnapPreExistingRun SnapshotRun))
    ]

{-------------------------------------------------------------------------------
  Enumeration class
-------------------------------------------------------------------------------}

-- | Enumerate values of type @a@ for golden tests
--
-- To prevent combinatorial explosion, the enumeration should generally be
-- /shallow/: the different constructors for type @a@ should be enumerated
-- without recursively enumerating the constructors' fields. For example,
-- enumerating @Maybe Int@ should give us something like:
--
-- > enumGolden @(Maybe Int) = [ Just 17, Nothing ]
--
-- This is generally a suitable approach, since the snapshot encodings do not
-- encode types differently depending on values in the constructor fields.
--
-- Example (recursive) instances that prevent combinatorial explosion:
--
-- @
--  instance EnumGolden a => EnumGolden (Maybe a) where
--    enumGolden = [ Just (singGolden @a), Nothing ]
--  instance EnumGolden Int where
--    enumGolden = [17, -72] -- singGolden = 17
-- @
--
-- If there are encoders that do require more elaborate (recursive)
-- enumerations, then it is okay to deviate from shallow enumerations, but take
-- care not to explode the combinatorics ;)
class EnumGolden a where
  {-# MINIMAL enumGolden | enumGoldenAnnotated | singGolden #-}

  -- | Enumerated values. The enumeration should be /shallow/.
  --
  -- The default implementation is to return a singleton list containing
  -- 'singGolden'.
  enumGolden :: [a]
  enumGolden = [ singGolden ]

  -- | Enumerated values with an annotation for naming purposes. The enumeration
  -- should be /shallow/, and the annotations should be unique.
  --
  -- The default implementation is to annotate 'enumGolden' with capital letters
  -- starting with @\'A\'@.
  enumGoldenAnnotated :: [(Annotation, a)]
  enumGoldenAnnotated = zip [[c] | c <- ['A' .. 'Z']] enumGolden

  -- | A singleton enumerated value. This is mainly useful for superclass
  -- instances.
  --
  -- The default implementation is to take the 'head' of 'enumGoldenAnnotated'.
  singGolden :: a
  singGolden = snd $ head enumGoldenAnnotated

type Annotation = String

enumGoldenAnnotated' :: EnumGolden a => Proxy a -> [(Annotation, a)]
enumGoldenAnnotated' _ = enumGoldenAnnotated

{-------------------------------------------------------------------------------
  Enumeration class: names and file paths
-------------------------------------------------------------------------------}

nameGolden :: Typeable a => Proxy a -> Annotation -> String
nameGolden p ann = map spaceToUnderscore (show $ typeRep p) ++ "." ++ ann

spaceToUnderscore :: Char -> Char
spaceToUnderscore ' ' = '_'
spaceToUnderscore c   = c

filePathsGolden :: (EnumGolden a, Typeable a) => Proxy a -> [String]
filePathsGolden p = [
      filePathGolden p annotation
    | (annotation, _) <- enumGoldenAnnotated' p
    ]

filePathGolden :: Typeable a => Proxy a -> String -> String
filePathGolden p ann = nameGolden p ann ++ ".snapshot.golden"

{-------------------------------------------------------------------------------
  Enumeration class: instances
-------------------------------------------------------------------------------}

instance EnumGolden SnapshotMetaData where
  singGolden = SnapshotMetaData singGolden singGolden singGolden singGolden singGolden
    where
      _coveredAllCases = \case
        SnapshotMetaData{} -> ()

instance EnumGolden SnapshotLabel where
  enumGolden = [
        SnapshotLabel "UserProvidedLabel"
      , SnapshotLabel ""
      ]
    where
      _coveredAllCases = \case
        SnapshotLabel{} -> ()

instance EnumGolden TableConfig where
  singGolden = TableConfig singGolden singGolden singGolden singGolden singGolden singGolden singGolden
    where
      _coveredAllCases = \case
        TableConfig{} -> ()

instance EnumGolden MergePolicy where
  singGolden = LazyLevelling
    where
      _coveredAllCases = \case
        LazyLevelling{} -> ()


instance EnumGolden SizeRatio where
  singGolden = Four
    where
      _coveredAllCases = \case
        Four{} -> ()

instance EnumGolden WriteBufferAlloc where
  singGolden = AllocNumEntries magicNumber2
    where
      _coveredAllCases = \case
        AllocNumEntries{} -> ()

instance EnumGolden BloomFilterAlloc where
  enumGolden = [ AllocFixed magicNumber3, AllocRequestFPR pi ]
    where
      _coveredAllCases = \case
        AllocFixed{} -> ()
        AllocRequestFPR{} -> ()

instance EnumGolden FencePointerIndexType where
  enumGolden = [ CompactIndex, OrdinaryIndex ]
    where
      _coveredAllCases = \case
        CompactIndex{} -> ()
        OrdinaryIndex{} -> ()

instance EnumGolden DiskCachePolicy where
  enumGolden = [ DiskCacheAll, DiskCacheLevelOneTo magicNumber3, DiskCacheNone ]
    where
      _coveredAllCases = \case
        DiskCacheAll{} -> ()
        DiskCacheLevelOneTo{} -> ()
        DiskCacheNone{} -> ()

instance EnumGolden MergeSchedule where
  enumGolden = [ OneShot, Incremental ]
    where
      _coveredAllCases = \case
        OneShot{} -> ()
        Incremental{} -> ()

instance EnumGolden (SnapLevels SnapshotRun) where
  singGolden = SnapLevels singGolden
    where
      _coveredAllCases = \case
        SnapLevels{} -> ()

instance EnumGolden (SnapLevel SnapshotRun) where
  singGolden = SnapLevel singGolden singGolden
    where
      _coveredAllCases = \case
        SnapLevel{} -> ()

instance EnumGolden (SnapIncomingRun SnapshotRun) where
  enumGolden = [
        SnapIncomingMergingRun singGolden singGolden singGolden singGolden
      , SnapIncomingSingleRun singGolden
      ]
    where
      _coveredAllCases = \case
        SnapIncomingMergingRun{} -> ()
        SnapIncomingSingleRun{} -> ()

instance EnumGolden MergePolicyForLevel where
  enumGolden = [ LevelTiering, LevelLevelling ]
    where
      _coveredAllCases = \case
        LevelTiering -> ()
        LevelLevelling -> ()

instance EnumGolden LevelMergeType where
  enumGolden = [ MergeMidLevel, MergeLastLevel ]
    where
      _coveredAllCases = \case
        MergeMidLevel{} -> ()
        MergeLastLevel{} -> ()

instance EnumGolden (SnapMergingTree SnapshotRun) where
  singGolden = SnapMergingTree singGolden
    where
      _coveredAllCases = \case
        SnapMergingTree{} -> ()

instance EnumGolden (SnapMergingTreeState SnapshotRun) where
  enumGolden = [
        SnapCompletedTreeMerge singGolden
      , SnapPendingTreeMerge singGolden
      , SnapOngoingTreeMerge singGolden
      ]
    where
      _coveredAllCases = \case
        SnapCompletedTreeMerge{} -> ()
        SnapPendingTreeMerge{} -> ()
        SnapOngoingTreeMerge{} -> ()

instance EnumGolden (SnapPendingMerge SnapshotRun) where
  enumGolden = [
        SnapPendingLevelMerge singGolden singGolden
      , SnapPendingUnionMerge singGolden
      ]
    where
      _coveredAllCases = \case
        SnapPendingLevelMerge{} -> ()
        SnapPendingUnionMerge{} -> ()

instance EnumGolden (SnapPreExistingRun SnapshotRun) where
  enumGolden = [
        SnapPreExistingRun singGolden
      , SnapPreExistingMergingRun singGolden
      ]
    where
      _coveredAllCases = \case
        SnapPreExistingRun{} -> ()
        SnapPreExistingMergingRun{} -> ()

instance EnumGolden TreeMergeType where
  enumGolden = [ MergeLevel, MergeUnion ]
    where
      _coveredAllCases = \case
        MergeLevel{} -> ()
        MergeUnion{} -> ()

instance EnumGolden a => EnumGolden (Maybe a) where
  enumGolden = [ Just singGolden, Nothing ]
    where
      _coveredAllCases = \case
        Just{} -> ()
        Nothing{} -> ()

instance EnumGolden a => EnumGolden (V.Vector a) where
  enumGolden = [
      V.fromList [ singGolden, singGolden ]
    , mempty
    , V.fromList [ singGolden ]
    ]

instance EnumGolden a => EnumGolden [a] where
  enumGolden = [
      [singGolden, singGolden]
    , []
    , [singGolden]
    ]

instance EnumGolden RunParams where
  singGolden = RunParams singGolden singGolden singGolden
    where
      _coveredAllCases = \case
        RunParams{} -> ()

instance EnumGolden t => EnumGolden (SnapMergingRun t SnapshotRun) where
  enumGolden = [
        SnapCompletedMerge singGolden singGolden
      , SnapOngoingMerge singGolden singGolden singGolden singGolden
      ]
    where
      _coveredAllCases = \case
        SnapCompletedMerge{} -> ()
        SnapOngoingMerge{} -> ()

instance EnumGolden RunBloomFilterAlloc where
  enumGolden = [
        RunAllocFixed magicNumber3
      , RunAllocRequestFPR pi
      ]
    where
      _coveredAllCases = \case
        RunAllocFixed{} -> ()
        RunAllocRequestFPR{} -> ()

instance EnumGolden RunNumber where
  singGolden = RunNumber magicNumber3
    where
      _coveredAllCases = \case
        RunNumber{} -> ()

instance EnumGolden IndexType where
  enumGolden = [
        Compact
      , Ordinary
      ]
    where
      _coveredAllCases = \case
        Compact{} -> ()
        Ordinary{} -> ()

instance EnumGolden RunDataCaching where
  enumGolden = [
        CacheRunData
      , NoCacheRunData
      ]
    where
      _coveredAllCases = \case
        CacheRunData{} -> ()
        NoCacheRunData{} -> ()

instance EnumGolden SnapshotRun where
  singGolden = SnapshotRun singGolden singGolden singGolden
    where
      _coveredAllCases = \case
        SnapshotRun{} -> ()

instance EnumGolden MergeDebt where
  singGolden = MergeDebt magicNumber2
    where
      _coveredAllCases = \case
        MergeDebt{} -> ()

instance EnumGolden MergeCredits where
  singGolden = MergeCredits magicNumber2
    where
      _coveredAllCases = \case
        MergeCredits{} -> ()

instance EnumGolden NominalDebt where
  singGolden = NominalDebt magicNumber2
    where
      _coveredAllCases = \case
        NominalDebt{} -> ()

instance EnumGolden NominalCredits where
  singGolden = NominalCredits magicNumber1
    where
      _coveredAllCases = \case
        NominalCredits{} -> ()

  -- Randomly chosen numbers
magicNumber1, magicNumber2, magicNumber3 :: Enum e => e
magicNumber1 = toEnum 42
magicNumber2 = toEnum 88
magicNumber3 = toEnum 1024
