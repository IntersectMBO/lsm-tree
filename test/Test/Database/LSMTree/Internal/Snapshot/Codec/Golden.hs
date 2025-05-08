{-# LANGUAGE OverloadedStrings #-}
module Test.Database.LSMTree.Internal.Snapshot.Codec.Golden
  (tests) where

import           Codec.CBOR.Write (toLazyByteString)
import           Control.Monad (when)
import           Data.Bifunctor (second)
import qualified Data.ByteString.Lazy as BSL (writeFile)
import           Data.Foldable (fold)
import qualified Data.List as List
import           Data.Vector (Vector)
import qualified Data.Vector as V
import           Database.LSMTree.Internal.Config (BloomFilterAlloc (..),
                     DiskCachePolicy (..), FencePointerIndexType (..),
                     MergePolicy (..), MergeSchedule (..), SizeRatio (..),
                     TableConfig (..), WriteBufferAlloc (..),
                     defaultTableConfig)
import           Database.LSMTree.Internal.MergeSchedule
                     (MergePolicyForLevel (..), NominalCredits (..),
                     NominalDebt (..))
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
    , testCodecTableConfig
    , testCodecSnapLevels
    , testCodecMergingTree
    ]

-- | The mount point is defined as the location of the golden file data directory
-- relative to the project root.
goldenDataMountPoint :: MountPoint
goldenDataMountPoint = MountPoint "test/golden-file-data/snapshot-codec"

-- | Delete output files on test-case success.
-- Change the option here if this is undesirable.
handleOutputFiles :: TestTree -> TestTree
handleOutputFiles = Tasty.localOption Au.OnPass

-- | Internally, the function will infer the correct filepath names.
snapshotCodecTest ::
     String -- ^ Name of the test
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
        -- false-positives, false-negatives, or irrelevant I/O exceptions.
        removeIfExists snapshotFsPath
        BSL.writeFile snapshotHsPath . toLazyByteString $ encode datum

  in  Au.goldenVsFile name snapshotAuPath snapshotHsPath outputAction

testCodecSnapshotLabel :: TestTree
testCodecSnapshotLabel =
  let assembler (tagA, valA) =
        let (tagC, valC) = basicTableConfig
            valD = basicRunNumber
            (tagE, valE) = basicSnapLevels
            (tagF, valF) = basicSnapMergingTree
        in  (fuseAnnotations [tagA, tagC, tagE, tagF ], SnapshotMetaData valA valC valD valE valF)
  in  testCodecBuilder "SnapshotLabels" $ assembler <$> enumerateSnapshotLabel

testCodecTableConfig :: TestTree
testCodecTableConfig =
  let assembler (tagC, valC) =
        let (tagA, valA) = basicSnapshotLabel
            valD = basicRunNumber
            (tagE, valE) = basicSnapLevels
            (tagF, valF) = basicSnapMergingTree
        in  (fuseAnnotations [tagA, tagC, tagE, tagF ], SnapshotMetaData valA valC valD valE valF)
  in  testCodecBuilder "SnapshotConfig" $ assembler <$> enumerateTableConfig

testCodecSnapLevels :: TestTree
testCodecSnapLevels =
  let assembler (tagE, valE) =
        let (tagA, valA) = basicSnapshotLabel
            (tagC, valC) = basicTableConfig
            valD = basicRunNumber
            (tagF, valF) = basicSnapMergingTree
        in  (fuseAnnotations [tagA, tagC, tagE, tagF ], SnapshotMetaData valA valC valD valE valF)
  in  testCodecBuilder "SnapshotLevels" $ assembler <$> enumerateSnapLevels

testCodecMergingTree :: TestTree
testCodecMergingTree =
  let assembler (tagF, valF) =
        let (tagA, valA) = basicSnapshotLabel
            (tagC, valC) = basicTableConfig
            valD = basicRunNumber
            (tagE, valE) = basicSnapLevels
        in  (fuseAnnotations [tagA, tagC, tagE, tagF ], SnapshotMetaData valA valC valD valE valF)
  in  testCodecBuilder "SnapshotMergingTree" $ assembler <$> enumerateSnapMergingTree

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

basicTableConfig :: (ComponentAnnotation, TableConfig)
basicTableConfig = ( fuseAnnotations $ "T0" : replicate 4 blank
                   , defaultTableConfig {confFencePointerIndex = CompactIndex}
                   )

basicRunNumber :: RunNumber
basicRunNumber = enumerateRunNumbers

basicSnapLevels :: (ComponentAnnotation, SnapLevels SnapshotRun)
basicSnapLevels = head enumerateSnapLevels

basicSnapMergingTree :: (ComponentAnnotation, Maybe (SnapMergingTree SnapshotRun))
basicSnapMergingTree = head enumerateSnapMergingTree

{----------------
Enumeration of SnapshotMetaData sub-components
----------------}

enumerateSnapshotLabel :: [(ComponentAnnotation, SnapshotLabel)]
enumerateSnapshotLabel =
  [ ("B0", SnapshotLabel "UserProvidedLabel")
  , ("B1", SnapshotLabel "")
  ]

enumerateTableConfig :: [(ComponentAnnotation, TableConfig)]
enumerateTableConfig =
    [  ( fuseAnnotations [ "T1", d, e, f, g ]
      , TableConfig {..}
      )
    | (_, confMergePolicy) <- [(blank, LazyLevelling)]
    , (g, confMergeSchedule) <- [("G0", OneShot), ("G1", Incremental)]
    , (_, confSizeRatio) <- [(blank, Four)]
    , (_, confWriteBufferAlloc) <- fmap AllocNumEntries <$> [(blank, magicNumber1)]
    , (d, confBloomFilterAlloc) <- enumerateBloomFilterAlloc
    , (e, confFencePointerIndex) <- [("I0", CompactIndex), ("I1", OrdinaryIndex)]
    , (f, confDiskCachePolicy) <- enumerateDiskCachePolicy
    ]

enumerateSnapLevels :: [(ComponentAnnotation, SnapLevels SnapshotRun)]
enumerateSnapLevels = fmap (SnapLevels . V.singleton) <$> enumerateSnapLevel

{----------------
Enumeration of SnapLevel sub-components
----------------}

enumerateSnapLevel :: [(ComponentAnnotation, SnapLevel SnapshotRun)]
enumerateSnapLevel = do
  (a, run) <- enumerateSnapIncomingRun
  (b, vec) <- enumerateVectorRunInfo
  [( fuseAnnotations [ a, b ], SnapLevel run vec)]

enumerateSnapIncomingRun :: [(ComponentAnnotation, SnapIncomingRun SnapshotRun)]
enumerateSnapIncomingRun =
  let
      inSnaps =
        [ (fuseAnnotations ["R1", a, b],
           SnapIncomingMergingRun policy nominalDebt nominalCredits sState)
        | (a, policy ) <- [("P0", LevelTiering), ("P1", LevelLevelling)]
        , nominalDebt    <- NominalDebt    <$> [ magicNumber2 ]
        , nominalCredits <- NominalCredits <$> [ magicNumber1 ]
        , (b, sState ) <- enumerateSnapMergingRun enumerateLevelMergeType
        ]
  in  fold
      [ [(fuseAnnotations $ "R0" : replicate 4 blank,
          SnapIncomingSingleRun enumerateOpenRunInfo)]
      , inSnaps
      ]

enumerateSnapMergingRun ::
     [(ComponentAnnotation, t)]
  -> [(ComponentAnnotation, SnapMergingRun t SnapshotRun)]
enumerateSnapMergingRun mTypes =
    [ (fuseAnnotations ["C0", blank, blank],
       SnapCompletedMerge mergeDebt enumerateOpenRunInfo)
    | mergeDebt <- (MR.MergeDebt. MR.MergeCredits) <$> [ magicNumber2 ]
    ]
 ++ [ (fuseAnnotations ["C1", a, b],
       SnapOngoingMerge runParams mergeCredits runVec mType)
    | let runParams = enumerateRunParams
    , mergeCredits <- MR.MergeCredits <$> [ magicNumber2 ]
    , (a, runVec ) <- enumerateVectorRunInfo
    , (b, mType  ) <- mTypes
    ]

enumerateLevelMergeType :: [(ComponentAnnotation, MR.LevelMergeType)]
enumerateLevelMergeType =
  [("L0", MR.MergeMidLevel), ("L1", MR.MergeLastLevel)]

enumerateVectorRunInfo :: [(ComponentAnnotation, Vector SnapshotRun)]
enumerateVectorRunInfo =
  [ ("V0", mempty)
  , ("V1", V.fromList [enumerateOpenRunInfo])
  , ("V2", V.fromList [enumerateOpenRunInfo,
                       enumerateOpenRunInfo {
                         snapRunNumber = RunNumber magicNumber2
                       } ])
  ]

{----------------
Enumeration of SnapMergingTree sub-components
----------------}

enumerateSnapMergingTree :: [(ComponentAnnotation, Maybe (SnapMergingTree SnapshotRun))]
enumerateSnapMergingTree =
  let noneTrees = (fuseAnnotations $ "M0" : replicate 11 blank, Nothing)
      someTrees = reannotate <$> enumerateSnapMergingTreeState True
      reannotate (tag, val) = (fuseAnnotations ["M1", tag], Just val)
  in  noneTrees : someTrees

enumerateSnapMergingTreeState :: Bool -> [(ComponentAnnotation, SnapMergingTree SnapshotRun)]
enumerateSnapMergingTreeState expandable =
  let s0 = [ (fuseAnnotations $ "S0" : replicate 10 blank, SnapCompletedTreeMerge enumerateOpenRunInfo) ]
      s1 = do
          (tagX, valX) <- enumerateSnapPendingMerge expandable
          [ (fuseAnnotations ["S1", tagX], SnapPendingTreeMerge valX) ]
      s2 = do
          (tagX, valX) <- enumerateSnapOngoingTreeMerge
          [ (fuseAnnotations ["S2", tagX], valX) ]
  in second SnapMergingTree <$> fold [ s0, s1, s2 ]

enumerateSnapOngoingTreeMerge :: [(ComponentAnnotation, SnapMergingTreeState SnapshotRun)]
enumerateSnapOngoingTreeMerge = do
  (tagX, valX) <- enumerateSnapMergingRun enumerateTreeMergeType
  let value = SnapOngoingTreeMerge valX
  pure ( fuseAnnotations $ ["G0", blank, tagX] <> replicate 5 blank, value )

enumerateSnapPendingMerge :: Bool -> [(ComponentAnnotation, SnapPendingMerge SnapshotRun)]
enumerateSnapPendingMerge expandable =
  let (tagTrees, subTrees)
        | not expandable = ("M0", [])
        | otherwise = ("M1", snd <$> enumerateSnapMergingTreeState False)
      headMay []    = Nothing
      headMay (x:_) = Just x
      prefix = do
        extra <- [False, True ]
        (tagPre, valPre) <- enumerateSnapPreExistingRun
        (tagExt, valExt) <-
          if extra
          then second pure <$> enumerateSnapPreExistingRun
          else [(fuseAnnotations $ replicate 4 blank, [])]
        let preValues = [ valPre ] <> valExt
        pure (fuseAnnotations [ "P0", tagPre, tagExt, tagTrees], SnapPendingLevelMerge preValues $ headMay subTrees)
  in  prefix <> [(fuseAnnotations $ fold [["P1"], replicate 8 blank, [tagTrees]], SnapPendingUnionMerge subTrees)]

enumerateSnapPreExistingRun :: [(ComponentAnnotation, SnapPreExistingRun SnapshotRun)]
enumerateSnapPreExistingRun =
    ( fuseAnnotations ("E0" : replicate 3 blank), SnapPreExistingRun enumerateOpenRunInfo)
  : [ (fuseAnnotations ["E1", tagX], SnapPreExistingMergingRun valX)
    | (tagX, valX) <- enumerateSnapMergingRun enumerateLevelMergeType
    ]

enumerateTreeMergeType :: [(ComponentAnnotation, MR.TreeMergeType)]
enumerateTreeMergeType =
  [("T0", MR.MergeLevel), ("T1", MR.MergeUnion)]

{----------------
Enumeration of SnapshotMetaData sub-sub-components and so on...
----------------}

enumerateBloomFilterAlloc :: [(ComponentAnnotation, BloomFilterAlloc)]
enumerateBloomFilterAlloc =
  [ ("A0",AllocFixed magicNumber3)
  , ("A1",AllocRequestFPR pi)
  ]

enumerateDiskCachePolicy :: [(ComponentAnnotation, DiskCachePolicy)]
enumerateDiskCachePolicy =
  [ ("D0", DiskCacheAll)
  , ("D1", DiskCacheNone)
  , ("D2", DiskCacheLevelsAtOrBelow 1)
  ]

enumerateRunNumbers :: RunNumber
enumerateRunNumbers = RunNumber magicNumber2

--TODO: use a proper enumeration, but don't cause a combinatorial explosion.
enumerateRunParams :: MR.RunParams
enumerateRunParams =
    MR.RunParams {
      MR.runParamCaching = NoCacheRunData,
      MR.runParamAlloc   = RunAllocFixed 10,
      MR.runParamIndex   = Compact
    }

--TODO: use a proper enumeration, but don't cause a combinatorial explosion of
-- golden tests. Perhaps do all combos as a direct golden test, but then where
-- it is embedded, just use one combo.
enumerateOpenRunInfo :: SnapshotRun
enumerateOpenRunInfo =
    SnapshotRun {
      snapRunNumber  = enumerateRunNumbers,
      snapRunCaching = CacheRunData,
      snapRunIndex   = Compact
    }

-- Randomly chosen numbers
magicNumber1, magicNumber2, magicNumber3 :: Enum e => e
magicNumber1 = toEnum 42
magicNumber2 = toEnum 88
magicNumber3 = toEnum 1024
