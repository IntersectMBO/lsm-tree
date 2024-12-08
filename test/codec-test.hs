{-# LANGUAGE CPP                        #-}
{-# LANGUAGE DerivingStrategies         #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE RankNTypes                 #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# OPTIONS_GHC -fspecialize-aggressively #-}
module Main (main) where

import           Control.Monad (when)
import           Control.Monad.ST.Strict (ST, runST)
import           Data.Foldable (fold)
import           Data.STRef
import           Data.Vector (Vector)
import qualified Data.Vector as V
import           Database.LSMTree.Common (BloomFilterAlloc (..),
                     DiskCachePolicy (..), NumEntries (..), TableConfig (..),
                     WriteBufferAlloc (..), defaultTableConfig)
import           Database.LSMTree.Internal.MergeSchedule (NumRuns (..))
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
import           Text.Printf (printf)


-- |
-- Delete output files on test-case success.
-- Change the option here if this is undesireable.
handleOutputFiles :: TestTree -> TestTree
handleOutputFiles = Tasty.localOption Au.OnPass

-- |
--
-- Internally, the function will infer the correct filepath names.
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
snapshotCodecTest
  :: String -- ^ Name of the test
  -> SnapshotMetaData -- ^ Data to be serialized
  -> TestTree
snapshotCodecTest name datum =
  let -- Various paths
      checksumFsPath = mkFsPath [name] <.> "checksum"
      checksumHsPath = fsToFilePath mountingAtPath checksumFsPath
      checksumAuPath = checksumHsPath <> ".golden"
      snapshotFsPath = mkFsPath [name] <.> "snapshot"
      snapshotHsPath = fsToFilePath mountingAtPath snapshotFsPath
      snapshotAuPath = snapshotHsPath <> ".golden"
      mountingAtPath = MountPoint "test/codec-data"

      -- IO actions (or lack thereof)
      noWorkAction :: IO ()
      noWorkAction = pure ()
      tidyUpAction :: a -> IO ()
      tidyUpAction = const noWorkAction
      runnerIO :: FS.HasFS IO HandleIO
      runnerIO = ioHasFS mountingAtPath
      removeIfExists :: FsPath -> IO ()
      removeIfExists fp =
        FS.doesFileExist runnerIO fp >>= (`when` (FS.removeFile runnerIO fp))
      outputAction :: IO ()
      outputAction = do
        removeIfExists snapshotFsPath
        removeIfExists checksumFsPath
        writeFileSnapshotMetaData
          runnerIO
          snapshotFsPath
          checksumFsPath
          datum

      -- File comparison test-cases against authoritative "golden" files
      testCaseChecksum :: TestTree
      testCaseChecksum = Au.goldenVsFile "checksum" checksumAuPath checksumHsPath noWorkAction
      testCaseSnapshot :: TestTree
      testCaseSnapshot = Au.goldenVsFile "snapshot" snapshotAuPath snapshotHsPath noWorkAction

      -- Be sure to use Tasty's @withResource@ constructor to ensure that the
      -- codec serialization occurs only /once/ and is memoized for the /two/
      -- golden tests (snapshot & checksum).
  in  Tasty.withResource outputAction tidyUpAction $ \tok -> tok `seq` testGroup name
        [ testCaseSnapshot
        , testCaseChecksum
        ]

main :: IO ()
main = do
  Tasty.defaultMain . handleOutputFiles . testGroup "codec" $
        [ testCodecSnapshotLabel
        , testCodecSnapshotTableType
        , testCodecTableConfig
        , testCodecSnapLevels
        ]

testCodecSnapshotLabel :: TestTree
testCodecSnapshotLabel =
  let assembler label =
        SnapshotMetaData
          label
          basicSnapshotTableType
          basicTableConfig
          basicSnapLevels
  in  testCodecBuilder "SnapshotLabel" $ assembler <$> enumerateSnapshotLabel

testCodecSnapshotTableType :: TestTree
testCodecSnapshotTableType =
  let assembler tType =
        SnapshotMetaData
          basicSnapshotLabel
          tType
          basicTableConfig
          basicSnapLevels
  in  testCodecBuilder "SnapshotTableType" $ assembler <$> enumerateSnapshotTableType

testCodecTableConfig :: TestTree
testCodecTableConfig =
  let assembler tConfig =
        SnapshotMetaData
          basicSnapshotLabel
          basicSnapshotTableType
          tConfig
          basicSnapLevels
  in  testCodecBuilder "TableConfig" $ assembler <$> enumerateTableConfig

testCodecSnapLevels :: TestTree
testCodecSnapLevels =
  let assembler levels =
        SnapshotMetaData
          basicSnapshotLabel
          basicSnapshotTableType
          basicTableConfig
          levels
  in  testCodecBuilder "SnapLevels" $ assembler <$> enumerateSnapLevels

testCodecBuilder :: TestName -> [SnapshotMetaData] -> TestTree
testCodecBuilder tName metadata =
    let finalizer :: STRef s Word -> SnapshotMetaData -> ST s TestTree
        finalizer ref datum = do
          curr <- readSTRef ref
          modifySTRef' ref succ
          pure $ snapshotCodecTest (tName <> printf "-%03d" curr) datum

        testSubtree = runST $ do
          counter <- newSTRef 0
          traverse (finalizer counter) metadata

    in  testGroup tName testSubtree

{----------------
Defaults used when the SnapshotMetaData sub-component is not under test
----------------}

basicSnapshotLabel :: SnapshotLabel
basicSnapshotLabel = head enumerateSnapshotLabel

basicSnapshotTableType :: SnapshotTableType
basicSnapshotTableType = minBound

basicTableConfig :: TableConfig
basicTableConfig = defaultTableConfig

basicSnapLevels :: SnapLevels RunNumber
basicSnapLevels = head enumerateSnapLevels

{----------------
Enumeration of SnapshotMetaData sub-components
----------------}

enumerateSnapshotLabel :: [SnapshotLabel]
enumerateSnapshotLabel = [ SnapshotLabel "UserProvidedLabel", SnapshotLabel "" ]

enumerateSnapshotTableType :: [SnapshotTableType]
enumerateSnapshotTableType = [minBound .. maxBound]

enumerateTableConfig :: [TableConfig]
enumerateTableConfig =
    [ TableConfig
        policy
        ratio
        allocs
        bloom
        fence
        cache
        merge
    | policy <- [minBound .. maxBound]
    , ratio <- [minBound .. maxBound]
    , allocs <- AllocNumEntries . NumEntries <$> [magicNumber1]
    , bloom <- enumerateBloomFilterAlloc
    , fence <- [minBound .. maxBound]
    , cache <- enumerateDiskCachePolicy
    , merge <- [minBound .. maxBound]
    ]

enumerateSnapLevels :: [SnapLevels RunNumber]
enumerateSnapLevels = SnapLevels . V.singleton <$> enumerateSnapLevel

{----------------
Enumeration of SnapshotMetaData sub-sub-components and so on...
----------------}

enumerateBloomFilterAlloc :: [BloomFilterAlloc]
enumerateBloomFilterAlloc =
  [ AllocFixed magicNumber3
  , AllocRequestFPR pi
  , AllocMonkey magicNumber3 . NumEntries $ magicNumber3 `div` 4
  ]

enumerateDiskCachePolicy :: [DiskCachePolicy]
enumerateDiskCachePolicy =
  [ DiskCacheAll
  , DiskCacheNone
  , DiskCacheLevelsAtOrBelow 1
  ]

enumerateSnapLevel :: [SnapLevel RunNumber]
enumerateSnapLevel = SnapLevel <$> enumerateSnapIncomingRun <*> enumerateVectorRunNumber

enumerateSnapIncomingRun :: [SnapIncomingRun RunNumber]
enumerateSnapIncomingRun =
  let
      inSnaps = (SnapSingleRun <$> enumerateRunNumbers) <>
        [ SnapMergingRun policy numRuns entries credits known sState
        | policy <- [minBound .. maxBound]
        , numRuns <- NumRuns <$> [ magicNumber1 ]
        , entries <- NumEntries  <$> [ magicNumber2 ]
        , credits <- UnspentCredits <$> [ magicNumber1 ]
        , known <- [minBound .. maxBound]
        , sState <- enumerateSnapMergingRunState
        ]
  in  fold
      [ SnapSingleRun <$> enumerateRunNumbers
      , inSnaps
      ]

enumerateSnapMergingRunState :: [SnapMergingRunState RunNumber]
enumerateSnapMergingRunState = (SnapCompletedMerge <$> enumerateRunNumbers) <>
  [ SnapOngoingMerge runVec credits level
  | runVec <- enumerateVectorRunNumber
  , credits <- SpentCredits <$> [ magicNumber1]
  , level <- [minBound .. maxBound]
  ]

enumerateVectorRunNumber :: [Vector RunNumber]
enumerateVectorRunNumber =
  [ mempty
  , V.fromList [RunNumber magicNumber1]
  , V.fromList [RunNumber magicNumber1, RunNumber magicNumber2 ]
  ]

enumerateRunNumbers :: [RunNumber]
enumerateRunNumbers = RunNumber <$> [ magicNumber2 ]

-- Randomly chosen numbers
magicNumber1, magicNumber2, magicNumber3 :: Enum e => e
magicNumber1 = toEnum 42
magicNumber2 = toEnum 88
magicNumber3 = toEnum 1024
