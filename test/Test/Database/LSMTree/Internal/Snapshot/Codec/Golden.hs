{-# LANGUAGE OverloadedStrings #-}
module Test.Database.LSMTree.Internal.Snapshot.Codec.Golden
  (goldenFileTests) where

import           Codec.CBOR.Write (toLazyByteString)
import           Control.Monad (when)
import qualified Data.ByteString.Lazy as BSL (writeFile)
import           Data.Foldable (fold)
import           Data.Vector (Vector)
import qualified Data.Vector as V
import           Database.LSMTree.Common (BloomFilterAlloc (..),
                     DiskCachePolicy (..), NumEntries (..), TableConfig (..),
                     WriteBufferAlloc (..), defaultTableConfig)
import           Database.LSMTree.Internal.Config (FencePointerIndex (..),
                     MergePolicy (..), MergeSchedule (..), SizeRatio (..))
import           Database.LSMTree.Internal.Merge (Level (..))
import           Database.LSMTree.Internal.MergingRun (MergePolicyForLevel (..),
                     NumRuns (..))
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

-- |
-- Compare the serialization of snapshot metadata with a known reference file.
goldenFileTests :: TestTree
goldenFileTests =  handleOutputFiles . testGroup "Golden File Comparisons" $
    [ testCodecSnapshotLabel
    , testCodecSnapshotTableType
    , testCodecTableConfig
    , testCodecSnapLevels
    ]

-- |
-- The mount point is defined as the location of the golden file data directory
-- relative to the project root.
goldenDataMountPoint :: MountPoint
goldenDataMountPoint = MountPoint "test/golden-file-data/snapshot-codec"

-- |
-- Delete output files on test-case success.
-- Change the option here if this is undesireable.
handleOutputFiles :: TestTree -> TestTree
handleOutputFiles = Tasty.localOption Au.OnPass

-- |
-- Internally, the function will infer the correct filepath names.
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
            (tagD, valD) = basicSnapLevels
        in  (fold [tagA, tagB, tagC, tagD ], SnapshotMetaData valA valB valC valD)
  in  testCodecBuilder "SnapshotLabels" $ assembler <$> enumerateSnapshotLabel

testCodecSnapshotTableType :: TestTree
testCodecSnapshotTableType =
  let assembler (tagB, valB) =
        let (tagA, valA) = basicSnapshotLabel
            (tagC, valC) = basicTableConfig
            (tagD, valD) = basicSnapLevels
        in  (fold [tagA, tagB, tagC, tagD ], SnapshotMetaData valA valB valC valD)
  in  testCodecBuilder "SnapshotTables" $ assembler <$> enumerateSnapshotTableType

testCodecTableConfig :: TestTree
testCodecTableConfig =
  let assembler (tagC, valC) =
        let (tagA, valA) = basicSnapshotLabel
            (tagB, valB) = basicSnapshotTableType
            (tagD, valD) = basicSnapLevels
        in  (fold [tagA, tagB, tagC, tagD ], SnapshotMetaData valA valB valC valD)
  in  testCodecBuilder "SnapshotConfig" $ assembler <$> enumerateTableConfig

testCodecSnapLevels :: TestTree
testCodecSnapLevels =
  let assembler (tagD, valD) =
        let (tagA, valA) = basicSnapshotLabel
            (tagB, valB) = basicSnapshotTableType
            (tagC, valC) = basicTableConfig
        in  (fold [tagA, tagB, tagC, tagD ], SnapshotMetaData valA valB valC valD)
  in  testCodecBuilder "SnapshotLevels" $ assembler <$> enumerateSnapLevels

testCodecBuilder :: TestName -> [(ComponentAnnotation, SnapshotMetaData)] -> TestTree
testCodecBuilder tName metadata =
  testGroup tName $ uncurry snapshotCodecTest <$> metadata

type ComponentAnnotation = String

{----------------
Defaults used when the SnapshotMetaData sub-component is not under test
----------------}

basicSnapshotLabel :: (ComponentAnnotation, SnapshotLabel)
basicSnapshotLabel = head enumerateSnapshotLabel

basicSnapshotTableType :: (ComponentAnnotation, SnapshotTableType)
basicSnapshotTableType = head enumerateSnapshotTableType

basicTableConfig :: (ComponentAnnotation, TableConfig)
basicTableConfig = ("T", defaultTableConfig)

basicSnapLevels :: (ComponentAnnotation, SnapLevels RunNumber)
basicSnapLevels = head enumerateSnapLevels

{----------------
Enumeration of SnapshotMetaData sub-components
----------------}

enumerateSnapshotLabel :: [(ComponentAnnotation, SnapshotLabel)]
enumerateSnapshotLabel =
  [ ("Bs", SnapshotLabel "UserProvidedLabel")
  , ("Bn", SnapshotLabel "")
  ]

enumerateSnapshotTableType :: [(ComponentAnnotation, SnapshotTableType)]
enumerateSnapshotTableType =
  [ ("Nn", SnapNormalTable)
  , ("Nm", SnapMonoidalTable)
  , ("Nf", SnapFullTable)
  ]

enumerateTableConfig :: [(ComponentAnnotation, TableConfig)]
enumerateTableConfig =
    [ ( fold [ "T", a, b, c, d, e, f, g ]
      , TableConfig
        policy
        ratio
        allocs
        bloom
        fence
        cache
        merge
      )
    | (a, policy) <- [("", MergePolicyLazyLevelling)]
    , (b, ratio ) <- [("", Four)]
    , (c, allocs) <- fmap (AllocNumEntries . NumEntries) <$> [("", magicNumber1)]
    , (d, bloom ) <- enumerateBloomFilterAlloc
    , (e, fence ) <- [("Ic", CompactIndex), ("Io", OrdinaryIndex)]
    , (f, cache ) <- enumerateDiskCachePolicy
    , (g, merge ) <- [("Go", OneShot), ("Gi", Incremental)]
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
  [(a <> b, SnapLevel run vec)]

enumerateSnapIncomingRun :: [(ComponentAnnotation, SnapIncomingRun RunNumber)]
enumerateSnapIncomingRun =
  let
      inSnaps =
        [ (fold ["Rm", "P", a, b], SnapMergingRun policy numRuns entries credits sState)
        | (a, policy ) <- [("t", LevelTiering), ("g", LevelLevelling)]
        , numRuns <- NumRuns <$> [ magicNumber1 ]
        , entries <- NumEntries  <$> [ magicNumber2 ]
        , credits <- UnspentCredits <$> [ magicNumber1 ]
        , (b, sState ) <- enumerateSnapMergingRunState
        ]
  in  fold
      [ [("Rs", SnapSingleRun enumerateRunNumbers)]
      , inSnaps
      ]

enumerateSnapMergingRunState :: [(ComponentAnnotation, SnapMergingRunState RunNumber)]
enumerateSnapMergingRunState = ("Mc", SnapCompletedMerge enumerateRunNumbers) :
  [ (fold ["Mo",a,b,c], SnapOngoingMerge runVec credits level)
  | (a, runVec ) <- enumerateVectorRunNumber
  , (b, credits) <- [("" , SpentCredits magicNumber1 )]
  , (c, level  ) <- [("Lm", MidLevel), ("L0", LastLevel)]
  ]

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
  [ ("Af",AllocFixed magicNumber3)
  , ("Ar",AllocRequestFPR pi)
  , ("Am",AllocMonkey magicNumber3 . NumEntries $ magicNumber3 `div` 4)
  ]

enumerateDiskCachePolicy :: [(ComponentAnnotation, DiskCachePolicy)]
enumerateDiskCachePolicy =
  [ ("Da", DiskCacheAll)
  , ("Dn", DiskCacheNone)
  , ("Db", DiskCacheLevelsAtOrBelow 1)
  ]

enumerateRunNumbers :: RunNumber
enumerateRunNumbers = RunNumber magicNumber2

-- Randomly chosen numbers
magicNumber1, magicNumber2, magicNumber3 :: Enum e => e
magicNumber1 = toEnum 42
magicNumber2 = toEnum 88
magicNumber3 = toEnum 1024