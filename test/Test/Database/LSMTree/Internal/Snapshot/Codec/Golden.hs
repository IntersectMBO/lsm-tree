{-# LANGUAGE OverloadedStrings #-}
module Test.Database.LSMTree.Internal.Snapshot.Codec.Golden
  (tests) where

import           Codec.CBOR.Write (toLazyByteString)
import           Control.Monad (when)
import qualified Data.ByteString.Lazy as BSL (writeFile)
import           Data.Foldable (fold)
import qualified Data.List as List
import           Data.Vector (Vector)
import qualified Data.Vector as V
import           Database.LSMTree.Common (BloomFilterAlloc (..),
                     DiskCachePolicy (..), NumEntries (..), TableConfig (..),
                     WriteBufferAlloc (..), defaultTableConfig)
import           Database.LSMTree.Internal.Config (FencePointerIndex (..),
                     MergePolicy (..), MergeSchedule (..), SizeRatio (..))
import           Database.LSMTree.Internal.MergeSchedule
                     (MergePolicyForLevel (..), NominalCredits (..),
                     NominalDebt (..))
import           Database.LSMTree.Internal.MergingRun (NumRuns (..))
import qualified Database.LSMTree.Internal.MergingRun as MR
import           Database.LSMTree.Internal.RunBuilder (IndexType (..),
                     RunBloomFilterAlloc (..), RunDataCaching (..))
import           Database.LSMTree.Internal.RunNumber (RunNumber (..))
import           Database.LSMTree.Internal.Snapshot
import           Database.LSMTree.Internal.Snapshot.Codec
import qualified System.FS.API as FS
import           System.FS.API.Types (FsPath, MountPoint (..), fsToFilePath,
                     mkFsPath, (<.>))
import           System.FS.IO (HandleIO, ioHasFS)
import qualified Test.Tasty as Tasty
import           Test.Tasty (TestName, TestTree, testGroup)
import qualified Test.Tasty.Golden as Au

-- | Compare the serialization of snapshot metadata with a known reference file.
tests :: TestTree
tests =  handleOutputFiles . testGroup
    "Test.Database.LSMTree.Internal.Snapshot.Codec.Golden" $
    [ testCodecSnapshotLabel
    , testCodecSnapshotTableType
    , testCodecTableConfig
    , testCodecSnapLevels
    ]

-- | The mount point is defined as the location of the golden file data directory
-- relative to the project root.
goldenDataMountPoint :: MountPoint
goldenDataMountPoint = MountPoint "test/golden-file-data/snapshot-codec"

-- | Delete output files on test-case success.
-- Change the option here if this is undesireable.
handleOutputFiles :: TestTree -> TestTree
handleOutputFiles = Tasty.localOption Au.OnPass

-- | Internally, the function will infer the correct filepath names.
snapshotCodecTest
  :: String -- ^ Name of the test
  -> SnapshotMetaData -- ^ Data to be serialized
  -> TestTree
snapshotCodecTest name datum =
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
        -- false-positives, false-negatives, or irrelavent I/O exceptions.
        removeIfExists snapshotFsPath
        BSL.writeFile snapshotHsPath . toLazyByteString $ encode datum

  in  Au.goldenVsFile name snapshotAuPath snapshotHsPath outputAction

testCodecSnapshotLabel :: TestTree
testCodecSnapshotLabel =
  let assembler (tagA, valA) =
        let (tagB, valB) = basicSnapshotTableType
            (tagC, valC) = basicTableConfig
            valD = basicRunNumber
            (tagE, valE) = basicSnapLevels
        in  (fuseAnnotations [tagA, tagB, tagC, tagE ], SnapshotMetaData valA valB valC valD valE)
  in  testCodecBuilder "SnapshotLabels" $ assembler <$> enumerateSnapshotLabel

testCodecSnapshotTableType :: TestTree
testCodecSnapshotTableType =
  let assembler (tagB, valB) =
        let (tagA, valA) = basicSnapshotLabel
            (tagC, valC) = basicTableConfig
            valD = basicRunNumber
            (tagE, valE) = basicSnapLevels
        in  (fuseAnnotations [tagA, tagB, tagC, tagE ], SnapshotMetaData valA valB valC valD valE)
  in  testCodecBuilder "SnapshotTables" $ assembler <$> enumerateSnapshotTableType

testCodecTableConfig :: TestTree
testCodecTableConfig =
  let assembler (tagC, valC) =
        let (tagA, valA) = basicSnapshotLabel
            (tagB, valB) = basicSnapshotTableType
            valD = basicRunNumber
            (tagE, valE) = basicSnapLevels
        in  (fuseAnnotations [tagA, tagB, tagC, tagE ], SnapshotMetaData valA valB valC valD valE)
  in  testCodecBuilder "SnapshotConfig" $ assembler <$> enumerateTableConfig

testCodecSnapLevels :: TestTree
testCodecSnapLevels =
  let assembler (tagE, valE) =
        let (tagA, valA) = basicSnapshotLabel
            (tagB, valB) = basicSnapshotTableType
            (tagC, valC) = basicTableConfig
            valD = basicRunNumber
        in  (fuseAnnotations [tagA, tagB, tagC, tagE ], SnapshotMetaData valA valB valC valD valE)
  in  testCodecBuilder "SnapshotLevels" $ assembler <$> enumerateSnapLevels

testCodecBuilder :: TestName -> [(ComponentAnnotation, SnapshotMetaData)] -> TestTree
testCodecBuilder tName metadata =
  testGroup tName $ uncurry snapshotCodecTest <$> metadata

type ComponentAnnotation = String

fuseAnnotations :: [ComponentAnnotation] -> ComponentAnnotation
fuseAnnotations = List.intercalate "-"

blank :: ComponentAnnotation
blank = "__"

{----------------
Defaults used when the SnapshotMetaData sub-component is not under test
----------------}

basicSnapshotLabel :: (ComponentAnnotation, SnapshotLabel)
basicSnapshotLabel = head enumerateSnapshotLabel

basicSnapshotTableType :: (ComponentAnnotation, SnapshotTableType)
basicSnapshotTableType = head enumerateSnapshotTableType

basicTableConfig :: (ComponentAnnotation, TableConfig)
basicTableConfig = ( fuseAnnotations $ "T0" : replicate 4 blank, defaultTableConfig)

basicRunNumber :: RunNumber
basicRunNumber = enumerateRunNumbers

basicSnapLevels :: (ComponentAnnotation, SnapLevels RunNumber)
basicSnapLevels = head enumerateSnapLevels

{----------------
Enumeration of SnapshotMetaData sub-components
----------------}

enumerateSnapshotLabel :: [(ComponentAnnotation, SnapshotLabel)]
enumerateSnapshotLabel =
  [ ("B0", SnapshotLabel "UserProvidedLabel")
  , ("B1", SnapshotLabel "")
  ]

enumerateSnapshotTableType :: [(ComponentAnnotation, SnapshotTableType)]
enumerateSnapshotTableType =
  [ ("N0", SnapNormalTable)
  , ("N1", SnapMonoidalTable)
  , ("N2", SnapFullTable)
  ]

enumerateTableConfig :: [(ComponentAnnotation, TableConfig)]
enumerateTableConfig =
    [  ( fuseAnnotations [ "T1", d, e, f, g ]
      , TableConfig
        policy
        ratio
        allocs
        bloom
        fence
        cache
        merge
      )
    | (_, policy) <- [(blank, MergePolicyLazyLevelling)]
    , (_, ratio ) <- [(blank, Four)]
    , (_, allocs) <- fmap (AllocNumEntries . NumEntries) <$> [(blank, magicNumber1)]
    , (d, bloom ) <- enumerateBloomFilterAlloc
    , (e, fence ) <- [("I0", CompactIndex), ("I1", OrdinaryIndex)]
    , (f, cache ) <- enumerateDiskCachePolicy
    , (g, merge ) <- [("G0", OneShot), ("G1", Incremental)]
    ]

enumerateSnapLevels :: [(ComponentAnnotation, SnapLevels RunNumber)]
enumerateSnapLevels = fmap (SnapLevels . V.singleton) <$> enumerateSnapLevel

{----------------
Enumeration of SnapLevel sub-components
----------------}

enumerateSnapLevel :: [(ComponentAnnotation, SnapLevel RunNumber)]
enumerateSnapLevel = do
  (a, run) <- enumerateSnapIncomingRun
  (b, vec) <- enumerateVectorRunNumber
  [( fuseAnnotations [ a, b ], SnapLevel run vec)]

enumerateSnapIncomingRun :: [(ComponentAnnotation, SnapIncomingRun RunNumber)]
enumerateSnapIncomingRun =
  let
      inSnaps =
        [ (fuseAnnotations ["R1", a, b],
           SnapMergingRun policy nominalDebt nominalCredits sState)
        | (a, policy ) <- [("P0", LevelTiering), ("P1", LevelLevelling)]
        , nominalDebt    <- NominalDebt    <$> [ magicNumber2 ]
        , nominalCredits <- NominalCredits <$> [ magicNumber1 ]
        , (b, sState ) <- enumerateSnapMergingRunState enumerateLevelMergeType
        ]
  in  fold
      [ [(fuseAnnotations $ "R0" : replicate 4 blank, SnapSingleRun enumerateRunNumbers)]
      , inSnaps
      ]

enumerateSnapMergingRunState ::
     [(ComponentAnnotation, t)]
  -> [(ComponentAnnotation, SnapMergingRunState t RunNumber)]
enumerateSnapMergingRunState mTypes =
    [ (fuseAnnotations ["C0", blank, blank],
       SnapCompletedMerge numRuns mergeDebt enumerateRunNumbers)
    | numRuns   <- NumRuns <$> [ magicNumber1 ]
    , mergeDebt <- (MR.MergeDebt. MR.MergeCredits) <$> [ magicNumber2 ]
    ]
 ++ [ (fuseAnnotations ["C1", a, b],
       SnapOngoingMerge runParams mergeCredits runVec mType)
    | let runParams =
            MR.RunParams {
              MR.runParamCaching = NoCacheRunData,
              MR.runParamAlloc   = RunAllocFixed 10,
              MR.runParamIndex   = Compact
            }
    , mergeCredits <- MR.MergeCredits <$> [ magicNumber2 ]
    , (a, runVec ) <- enumerateVectorRunNumber
    , (b, mType  ) <- mTypes
    ]

enumerateLevelMergeType :: [(ComponentAnnotation, MR.LevelMergeType)]
enumerateLevelMergeType =
  [("L0", MR.MergeMidLevel), ("L1", MR.MergeLastLevel)]

enumerateVectorRunNumber :: [(ComponentAnnotation, Vector RunNumber)]
enumerateVectorRunNumber =
  [ ("V0", mempty)
  , ("V1", V.fromList [RunNumber magicNumber1])
  , ("V2", V.fromList [RunNumber magicNumber1, RunNumber magicNumber2 ])
  ]

{----------------
Enumeration of SnapshotMetaData sub-sub-components and so on...
----------------}

enumerateBloomFilterAlloc :: [(ComponentAnnotation, BloomFilterAlloc)]
enumerateBloomFilterAlloc =
  [ ("A0",AllocFixed magicNumber3)
  , ("A1",AllocRequestFPR pi)
  , ("A2",AllocMonkey magicNumber3 . NumEntries $ magicNumber3 `div` 4)
  ]

enumerateDiskCachePolicy :: [(ComponentAnnotation, DiskCachePolicy)]
enumerateDiskCachePolicy =
  [ ("D0", DiskCacheAll)
  , ("D1", DiskCacheNone)
  , ("D2", DiskCacheLevelsAtOrBelow 1)
  ]

enumerateRunNumbers :: RunNumber
enumerateRunNumbers = RunNumber magicNumber2

-- Randomly chosen numbers
magicNumber1, magicNumber2, magicNumber3 :: Enum e => e
magicNumber1 = toEnum 42
magicNumber2 = toEnum 88
magicNumber3 = toEnum 1024
