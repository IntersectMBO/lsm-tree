{-# OPTIONS_GHC -Wno-orphans #-}

-- | Tests for snapshots and their interaction with the file system
--
-- TODO: add fault injection tests using fs-sim
module Test.Database.LSMTree.Internal.Snapshot.FS (tests) where

import           Database.LSMTree.Internal.Snapshot
import           Database.LSMTree.Internal.Snapshot.Codec
import qualified System.FS.API as FS
import           Test.Database.LSMTree.Internal.Snapshot.Codec ()
import           Test.Tasty
import           Test.Tasty.QuickCheck
import           Test.Util.FS

tests :: TestTree
tests = testGroup "Test.Database.LSMTree.Internal.Snapshot.FS" [
      testProperty "prop_fsRoundtripSnapshotMetaData"
        prop_fsRoundtripSnapshotMetaData
    ]

-- | @readFileSnapshotMetaData . writeFileSnapshotMetaData = id@
prop_fsRoundtripSnapshotMetaData :: SnapshotMetaData -> Property
prop_fsRoundtripSnapshotMetaData metaData =
    ioProperty $
    withTempIOHasFS "temp" $ \hfs -> do
      writeFileSnapshotMetaData hfs contentPath checksumPath metaData
      eMetaData' <- readFileSnapshotMetaData hfs contentPath checksumPath
      pure $ case eMetaData' of
        Left e          -> counterexample (show e) False
        Right metaData' -> metaData === metaData'
  where
    contentPath  = FS.mkFsPath ["content"]
    checksumPath = FS.mkFsPath ["checksum"]
