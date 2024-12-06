{-# LANGUAGE CPP                        #-}
{-# LANGUAGE DerivingStrategies         #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE RankNTypes                 #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# OPTIONS_GHC -fspecialize-aggressively #-}
module Main (main) where

import           Control.DeepSeq (NFData (..), force)
import           Control.Exception (evaluate)
import           Control.Monad.ST.Strict (ST, runST)
import           Data.Bits (unsafeShiftR)
import qualified Data.Heap as Heap
import           Data.IORef
import qualified Data.List as L
import           Data.List.NonEmpty (NonEmpty (..))
import           Data.Primitive.ByteArray (compareByteArrays)
import qualified Data.Vector.Primitive as VP
import           Data.Word (Word64, Word8)
import           System.IO.Unsafe (unsafePerformIO)
import qualified System.Random.SplitMix as SM
import           Test.Tasty (TestName, TestTree, defaultMainWithIngredients,
                     testGroup)

import           Control.Monad (when)
import           Database.LSMTree.Common (defaultTableConfig)
import           Database.LSMTree.Internal.RunNumber (RunNumber (..))
import           Database.LSMTree.Internal.Snapshot
import           Database.LSMTree.Internal.Snapshot.Codec
import qualified System.FS.API as FS
import           System.FS.API.Types (FsPath, MountPoint (..), fsToFilePath,
                     mkFsPath, (<.>))
import           System.FS.IO (HandleIO, ioHasFS)
import qualified Test.Tasty as Tasty
import qualified Test.Tasty.Golden as Au

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
--    defaultMainWithIngredients $ testGroup "codec"
  Tasty.defaultMain . handleOutputFiles $ testGroup "codec"
        [ testGroup "tests"
          [ snapshotCodecTest "Test1" $ SnapshotMetaData
            { snapMetaLabel = SnapshotLabel "user-supplied-label"
            , snapMetaTableType = SnapNormalTable
            , snapMetaConfig = defaultTableConfig
            , snapMetaLevels = SnapLevels
                (pure (SnapLevel
                  (SnapSingleRun (RunNumber 42))
                  (pure (RunNumber 42))))
            }

          ]
        ]
