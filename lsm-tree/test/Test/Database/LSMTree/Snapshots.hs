{-# LANGUAGE OverloadedStrings #-}

module Test.Database.LSMTree.Snapshots (tests) where

import           Control.Tracer (nullTracer)
import           Data.Monoid (Sum (Sum))
import qualified Data.Vector as V
import           Data.Void (Void)
import           Data.Word (Word64)
import           Database.LSMTree (ResolveValue, Salt, SerialiseKey,
                     SerialiseValue, Table, TableConfig (confWriteBufferAlloc),
                     WriteBufferAlloc (AllocNumEntries), defaultTableConfig,
                     exportSnapshot, getValue, importSnapshot, inserts, lookups,
                     saveSnapshot, withOpenSession, withTableFromSnapshot,
                     withTableWith)

import           Database.LSMTree.Extras (showRangesOf)
import qualified System.FS.API as FS
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.QuickCheck (Arbitrary, Property, Small (Small),
                     checkCoverage, ioProperty, tabulate, testProperty, (===))
import           Test.Util.FS (withTempIOHasBlockIO)

tests :: TestTree
tests = testGroup "Test.Database.LSMTree.Snapshots" [
      testProperty "prop_exportImportSnapshot" prop_exportImportSnapshot
    ]

{-------------------------------------------------------------------------------
  Snapshot import and export
-------------------------------------------------------------------------------}

newtype Key = Key Word64
  deriving stock (Show, Eq, Ord)
  deriving newtype SerialiseKey
  deriving Arbitrary via Small Word64

newtype Value = Value Word64
  deriving stock (Show, Eq)
  deriving newtype (SerialiseValue)
  deriving ResolveValue via Sum Word64
  deriving Arbitrary via Small Word64

-- | Tables opened from imported snapshots behave the same as the original table
-- that the snapshot was created for.
--
-- We test this by looking at the observable behaviour of the two tables. If
-- lookups on both tables produce the same result, then they are logically
-- equivalent.
--
-- Moreover, if importing or exporting were to corrupt the snapshots, then
-- opening the snapshot would throw an error, which this test would catch.
prop_exportImportSnapshot ::
     V.Vector (Key, Value)
  -> V.Vector Key
  -> Property
prop_exportImportSnapshot ins los =
    checkCoverage $
    ioProperty $
    withTempIOHasBlockIO "prop_exportImportSnapshot" $ \hfs hbio -> do
      FS.createDirectoryIfMissing hfs True sessionDir

      -- Open a session and create a snapshot for some arbitrary table contents
      withOpenSession nullTracer hfs hbio salt sessionDir $ \session ->
        withTableWith conf session $ \(table1 :: Table IO Key Value Void) -> do
          inserts table1 $ V.map (\(k, v) -> (k, v, Nothing)) ins
          saveSnapshot "snap1" "KeyValueBlob" table1

          -- Export then re-import the snapshot
          exportSnapshot session "snap1" exportDir
          importSnapshot session "snap2" exportDir

          -- Open a table from the re-imported snapshot. Any corruption of the
          -- snapshot would be identified here.
          withTableFromSnapshot session "snap2" "KeyValueBlob" $ \(table2 :: Table IO Key Value Void) -> do

            -- Check that lookups on both the original and re-imported table
            -- match
            lrs1 <- V.map getValue <$> lookups table1 los
            lrs2 <- V.map getValue <$> lookups table2 los

            pure $
              tabulate "# of physical table entries" [ showRangesOf 5 $ V.length ins ] $
              tabulate "# of lookups"                [ showRangesOf 5 $ V.length los ] $
              tabulate "# of successful lookups"     [ showRangesOf 5 $ V.length $ V.catMaybes lrs1 ] $
              tabulate "# of unsuccessful lookups"   [ showRangesOf 5 $ V.length $ V.filter (==Nothing) lrs1 ] $
              lrs1 === lrs2
  where
    sessionDir = FS.mkFsPath ["session"]

    -- | Directory for storing exported snapshots
    exportDir = FS.mkFsPath ["export"]

    salt :: Salt
    salt = 17

    conf :: TableConfig
    conf = defaultTableConfig {
        confWriteBufferAlloc = AllocNumEntries 3
      }
