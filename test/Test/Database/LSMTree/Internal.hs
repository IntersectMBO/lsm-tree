{-# LANGUAGE LambdaCase      #-}
{-# LANGUAGE RecordWildCards #-}

-- TODO: generalise tests in this module for IOSim, not just IO. Do this once we
-- add proper support for IOSim for fault testing.
module Test.Database.LSMTree.Internal (tests) where

import           Control.Exception
import           Control.Monad (void)
import           Data.Bifunctor
import qualified Data.Map.Strict as Map
import           Data.Maybe (fromMaybe)
import qualified Data.Vector as V
import           Database.LSMTree.Internal
import           Database.LSMTree.Internal.Entry
import           Database.LSMTree.Internal.Paths (mkSnapshotName)
import           Database.LSMTree.Internal.Serialise
import qualified System.FS.API as FS
import qualified Test.Database.LSMTree.Internal.Lookup as Test
import           Test.QuickCheck
import           Test.Tasty
import           Test.Tasty.HUnit
import           Test.Tasty.QuickCheck
import           Test.Util.FS

tests :: TestTree
tests = testGroup "Test.Database.LSMTree.Internal" [
      testCase "newSession" newSession
    , testCase "restoreSession" restoreSession
    , testCase "twiceOpenSession" twiceOpenSession
    , testCase "sessionDirLayoutMismatch" sessionDirLayoutMismatch
    , testCase "sessionDirDoesNotExist" sessionDirDoesNotExist
    , testProperty "prop_interimOpenTable" prop_interimOpenTable
    ]

newSession :: Assertion
newSession = withTempIOHasBlockIO "newSession" $ \hfs hbio ->
    void $ openSession hfs hbio (FS.mkFsPath [])

restoreSession :: Assertion
restoreSession = withTempIOHasBlockIO "restoreSession" $ \hfs hbio -> do
    session1 <- openSession hfs hbio (FS.mkFsPath [])
    closeSession session1
    void $ openSession hfs hbio (FS.mkFsPath [])

twiceOpenSession :: Assertion
twiceOpenSession = withTempIOHasBlockIO "twiceOpenSession" $ \hfs hbio -> do
    void $ openSession hfs hbio (FS.mkFsPath [])
    try @LSMTreeError (openSession hfs hbio (FS.mkFsPath [])) >>= \case
      Left (SessionDirLocked _) -> pure ()
      x -> assertFailure $ "Opening a session twice in the same directory \
                           \should fail with an SessionDirLocked error, but \
                           \it returned this instead: " <> showLeft "Session" x

sessionDirLayoutMismatch :: Assertion
sessionDirLayoutMismatch = withTempIOHasBlockIO "sessionDirLayoutMismatch" $ \hfs hbio -> do
    FS.createDirectory hfs (FS.mkFsPath ["unexpected-directory"])
    try @LSMTreeError (openSession hfs hbio (FS.mkFsPath [])) >>= \case
      Left (SessionDirMalformed _) -> pure ()
      x -> assertFailure $ "Restoring a session in a directory with a wrong \
                           \layout should fail with a SessionDirMalformed, but \
                           \it returned this instead: " <> showLeft "Session" x

sessionDirDoesNotExist :: Assertion
sessionDirDoesNotExist = withTempIOHasBlockIO "sessionDirDoesNotExist" $ \hfs hbio -> do
    try @LSMTreeError (openSession hfs hbio (FS.mkFsPath ["missing-dir"])) >>= \case
      Left (SessionDirDoesNotExist _) -> pure ()
      x -> assertFailure $ "Opening a session in a non-existent directory should \
                           \fail with a SessionDirDoesNotExist error, but it \
                           \returned this instead: " <> showLeft "Session" x

showLeft :: Show a => String -> Either a b -> String
showLeft x = \case
    Left e -> show e
    Right _ -> x

-- | Check that opening a populated table via the interim table loading function
-- works as expected. Roughly, we test:
--
-- @
--  inserts th kvs == open' (snapshot' (inserts th kvs))
-- @
--
-- TODO: tables are not yet populated until we have an implementation for
-- updates to tables.
prop_interimOpenTable ::
     Test.InMemLookupData SerialisedKey SerialisedValue SerialisedBlob
  -> Property
prop_interimOpenTable dat = ioProperty $
    withTempIOHasBlockIO "prop_interimOpenTable" $ \hfs hbio -> do
      withSession hfs hbio (FS.mkFsPath []) $ \sesh -> do
        th <- new sesh conf
        -- Insert each key into the table one by one, so each can trigger a
        -- flush on its own.
        mapM_ (\(k,e) -> updates (V.singleton (k, e)) th) upds
        let snap = fromMaybe (error "invalid name") $ mkSnapshotName "snap"
        numRunsSnapped <- snapshot snap "someLabel" th
        th' <- open sesh "someLabel" snap
        lhs <- lookups ks th id
        rhs <- lookups ks th' id
        close th
        close th'
        -- TODO: checking lookups is a simple check, but we could have stronger
        -- guarantee. For example, we might check that the internal structures
        -- match.
        --
        -- TODO: remove opaqueifyBlobs once blobs are implemented.
        pure $ tabulate "Number of runs snapshotted" [show numRunsSnapped]
             $ (Test.opaqueifyBlobs lhs === Test.opaqueifyBlobs rhs)
  where
    conf = TableConfig {
        confMergePolicy = MergePolicyLazyLevelling
      , confSizeRatio = Four
        -- Write buffer size is small on purpose, so that the test actually
        -- flushes and merges.
      , confWriteBufferAlloc = AllocNumEntries (NumEntries 3)
      , confBloomFilterAlloc = AllocFixed 10
      , confResolveMupsert = Nothing
      }

    Test.InMemLookupData { runData, lookups = keysToLookup } = dat
    -- TODO: remove filtering once blobs are implemented
    runDataFiltered = Map.filter (not . hasBlob) runData
    ks = V.map serialiseKey (V.fromList keysToLookup)
    upds = V.fromList $ fmap (bimap serialiseKey (bimap serialiseValue serialiseBlob))
                      $ Map.toList runDataFiltered
