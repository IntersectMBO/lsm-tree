{-# LANGUAGE OverloadedStrings #-}
module Test.Database.LSMTree.Internal.Snapshot.Codec.MergingTree
  (tests) where

import           Codec.CBOR.Write (toLazyByteString)
import           Control.Monad (when)
import           Data.Bifunctor (second)
import qualified Data.ByteString.Lazy as BSL (writeFile)
import           Data.Foldable (fold)
import qualified Data.List as List
import           Data.Vector (Vector)
import qualified Data.Vector as V
import           Database.LSMTree.Internal.MergingRun (MergeCredits (..),
                     MergeDebt (..), NumRuns (..))
import qualified Database.LSMTree.Internal.MergingRun as MR
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
    "Test.Database.LSMTree.Internal.Snapshot.Codec.MergingTree" $
    [ testCodecSnapshotMergingTree
    ]

-- | The mount point is defined as the location of the golden file data directory
-- relative to the project root.
goldenDataMountPoint :: MountPoint
goldenDataMountPoint = MountPoint "test/golden-file-data/snapshot-merging-tree"

-- | Delete output files on test-case success.
-- Change the option here if this is undesireable.
handleOutputFiles :: TestTree -> TestTree
handleOutputFiles = Tasty.localOption Au.OnPass

-- | Internally, the function will infer the correct filepath names.
snapshotCodecTest
  :: String -- ^ Name of the test
  -> SnapshotMergingTree -- ^ Data to be serialized
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

testCodecSnapshotMergingTree :: TestTree
testCodecSnapshotMergingTree = testCodecBuilder "SnapshotMergingTree" enumerateSnaphotMergingTree

testCodecBuilder :: TestName -> [(ComponentAnnotation, SnapshotMergingTree)] -> TestTree
testCodecBuilder tName metadata =
  testGroup tName $ uncurry snapshotCodecTest <$> metadata

type ComponentAnnotation = String

fuseAnnotations :: [ComponentAnnotation] -> ComponentAnnotation
fuseAnnotations = List.intercalate "-"

blank :: ComponentAnnotation
blank = "__"

{----------------
Enumeration of SnapshotMergingTree sub-components
----------------}

enumerateSnaphotMergingTree :: [(ComponentAnnotation, SnapshotMergingTree)]
enumerateSnaphotMergingTree =
  second SnapshotMergingTree <$> enumerateSnapMergingTreeState True

enumerateSnapMergingTreeState :: Bool -> [(ComponentAnnotation, SnapMergingTree RunNumber)]
enumerateSnapMergingTreeState expandable =
  let s0 = [ (fuseAnnotations $ "S0" : replicate 10 blank, SnapCompletedTreeMerge $ RunNumber magicNumber2) ]
      s1 = do
          (tagX, valX) <- enumerateSnapPendingMerge expandable
          [ (fuseAnnotations ["S1", tagX], SnapPendingTreeMerge valX) ]
      s2 = do
          (tagX, valX) <- enumerateSnapOngoingTreeMerge
          [ (fuseAnnotations ["S2", tagX], valX) ]
  in second SnapMergingTree <$> fold [ s0, s1, s2 ]

enumerateSnapOngoingTreeMerge :: [(ComponentAnnotation, SnapMergingTreeState RunNumber)]
enumerateSnapOngoingTreeMerge = do
  (tagX, valX) <- enumerateSnapMergingRunState enumerateTreeMergeType
  let value = SnapOngoingTreeMerge
        (NumRuns magicNumber1)
        (MergeDebt (MergeCredits magicNumber2))
        (MergeCredits magicNumber3)
        valX
  pure ( fuseAnnotations $ ["G0", blank, tagX] <> replicate 5 blank, value)

enumerateSnapPendingMerge :: Bool -> [(ComponentAnnotation, SnapPendingMerge RunNumber)]
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

enumerateSnapPreExistingRun :: [(ComponentAnnotation, SnapPreExistingRun RunNumber)]
enumerateSnapPreExistingRun = ( fuseAnnotations $ "E0" : replicate 3 blank, SnapPreExistingRun (RunNumber magicNumber1) ) : do
  (tagX, valX) <- enumerateSnapMergingRunState enumerateLevelMergeType
  let value = SnapPreExistingMergingRun
        (NumRuns magicNumber1)
        (MergeDebt (MergeCredits magicNumber2))
        (MergeCredits magicNumber3)
        valX
  pure ( fuseAnnotations ["E1", tagX], value )

enumerateSnapMergingRunState ::
     [(ComponentAnnotation, t)] -> [(ComponentAnnotation, SnapMergingRunState t RunNumber)]
enumerateSnapMergingRunState mTypes =
  (fuseAnnotations ["C0", blank, blank], SnapCompletedMerge enumerateRunNumbers) :
    [ (fuseAnnotations ["C1", a, b], SnapOngoingMerge runVec mType)
    | (a, runVec ) <- enumerateVectorRunNumber
    , (b, mType  ) <- mTypes
    ]

enumerateTreeMergeType :: [(ComponentAnnotation, MR.TreeMergeType)]
enumerateTreeMergeType =
  [("T0", MR.MergeLevel), ("T1", MR.MergeUnion)]

enumerateLevelMergeType :: [(ComponentAnnotation, MR.LevelMergeType)]
enumerateLevelMergeType =
  [("L0", MR.MergeMidLevel), ("L1", MR.MergeLastLevel)]

enumerateVectorRunNumber :: [(ComponentAnnotation, Vector RunNumber)]
enumerateVectorRunNumber =
  [ ("V0", mempty)
  , ("V1", V.fromList [RunNumber magicNumber1])
  , ("V2", V.fromList [RunNumber magicNumber1, RunNumber magicNumber2 ])
  ]

enumerateRunNumbers :: RunNumber
enumerateRunNumbers = RunNumber magicNumber2

-- Randomly chosen numbers
magicNumber1, magicNumber2, magicNumber3 :: Enum e => e
magicNumber1 = toEnum 42
magicNumber2 = toEnum 88
magicNumber3 = toEnum 1024
