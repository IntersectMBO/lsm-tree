{-# LANGUAGE LambdaCase      #-}
{-# LANGUAGE RecordWildCards #-}

-- TODO: generalise tests in this module for IOSim, not just IO. Do this once we
-- add proper support for IOSim for fault testing.
module Test.Database.LSMTree.Internal (tests) where

import qualified Control.Concurrent.Class.MonadSTM.RWVar as RW
import           Control.Exception
import           Control.Monad (void)
import           Control.Tracer
import           Data.Bifunctor
import           Data.Coerce (coerce)
import           Data.Foldable (traverse_)
import qualified Data.Map.Strict as Map
import           Data.Maybe (fromMaybe, mapMaybe)
import           Data.Monoid (Sum (..))
import qualified Data.Vector as V
import           Data.Word (Word64)
import           Database.LSMTree.Extras (showPowersOf)
import           Database.LSMTree.Extras.Generators (KeyForIndexCompact (..))
import           Database.LSMTree.Internal
import           Database.LSMTree.Internal.Config
import           Database.LSMTree.Internal.Entry
import           Database.LSMTree.Internal.MergeSchedule
import           Database.LSMTree.Internal.Paths (mkSnapshotName)
import           Database.LSMTree.Internal.Serialise
import qualified System.FS.API as FS
import qualified Test.Database.LSMTree.Internal.Lookup as Test
import           Test.Database.LSMTree.Internal.Lookup
                     (InMemLookupData (runData))
import           Test.QuickCheck
import           Test.Tasty
import           Test.Tasty.HUnit
import           Test.Tasty.QuickCheck
import           Test.Util.FS

tests :: TestTree
tests = testGroup "Test.Database.LSMTree.Internal" [
      testGroup "Session" [
          testCase "newSession" newSession
        , testCase "restoreSession" restoreSession
        , testProperty "twiceOpenSession" twiceOpenSession
        , testCase "sessionDirLayoutMismatch" sessionDirLayoutMismatch
        , testCase "sessionDirDoesNotExist" sessionDirDoesNotExist
        , testProperty "prop_interimRestoreSessionUniqueRunNames"
            prop_interimRestoreSessionUniqueRunNames
        ]
    , testGroup "Table" [
          testProperty "prop_interimOpenTable" prop_interimOpenTable
        ]
    , testGroup "Cursor" [
          testProperty "prop_roundtripCursor" $
            prop_roundtripCursor Nothing
        , testProperty "prop_roundtripCursor at offset" $
            prop_roundtripCursor . Just
        ]
    ]

testTableConfig :: TableConfig
testTableConfig = defaultTableConfig {
      -- Write buffer size is small on purpose, so that the test actually
      -- flushes and merges.
      confWriteBufferAlloc = AllocNumEntries (NumEntries 3)
    }

newSession :: Assertion
newSession = withTempIOHasBlockIO "newSession" $ \hfs hbio ->
    void $ openSession nullTracer hfs hbio (FS.mkFsPath [])

restoreSession :: Assertion
restoreSession = withTempIOHasBlockIO "restoreSession" $ \hfs hbio -> do
    session1 <- openSession nullTracer hfs hbio (FS.mkFsPath [])
    closeSession session1
    void $ openSession nullTracer hfs hbio (FS.mkFsPath [])

twiceOpenSession :: Property
twiceOpenSession = ioProperty $
    withTempIOHasBlockIO "twiceOpenSession" $ \hfs hbio -> do
      bracket (openSession nullTracer hfs hbio (FS.mkFsPath []))
              closeSession $ \_ ->
        bracket (try @LSMTreeError (openSession nullTracer hfs hbio (FS.mkFsPath [])))
                (traverse_ closeSession) $ \case
          Left (SessionDirLocked _) -> pure ()
          x -> assertFailure $ "Opening a session twice in the same directory \
                              \should fail with an SessionDirLocked error, but \
                              \it returned this instead: " <> showLeft "Session" x

sessionDirLayoutMismatch :: Assertion
sessionDirLayoutMismatch = withTempIOHasBlockIO "sessionDirLayoutMismatch" $ \hfs hbio -> do
    FS.createDirectory hfs (FS.mkFsPath ["unexpected-directory"])
    try @LSMTreeError (openSession nullTracer hfs hbio (FS.mkFsPath [])) >>= \case
      Left (SessionDirMalformed _) -> pure ()
      x -> assertFailure $ "Restoring a session in a directory with a wrong \
                           \layout should fail with a SessionDirMalformed, but \
                           \it returned this instead: " <> showLeft "Session" x

sessionDirDoesNotExist :: Assertion
sessionDirDoesNotExist = withTempIOHasBlockIO "sessionDirDoesNotExist" $ \hfs hbio -> do
    try @LSMTreeError (openSession nullTracer hfs hbio (FS.mkFsPath ["missing-dir"])) >>= \case
      Left (SessionDirDoesNotExist _) -> pure ()
      x -> assertFailure $ "Opening a session in a non-existent directory should \
                           \fail with a SessionDirDoesNotExist error, but it \
                           \returned this instead: " <> showLeft "Session" x

showLeft :: Show a => String -> Either a b -> String
showLeft x = \case
    Left e -> show e
    Right _ -> x

-- | Runs are currently not deleted when they become unreferenced. As such, when
-- a session is restored, there are still runs in the active directory. When we
-- restore a session, we must ensure that we do not use names for new runs that
-- are already used for existing runs. As such, we should set the
-- @sessionUniqCounter@ accordingly, such that it starts at a number strictly
-- larger then numbers of the runs in the active directory.
--
-- TODO: remove once we have proper snapshotting, in which case files in the
-- active directory are deleted when a session is restored: loading snapshots is
-- the only way to get active runs into the active directory.
prop_interimRestoreSessionUniqueRunNames ::
     Positive (Small Int)
  -> NonNegative Int
  -> Property
prop_interimRestoreSessionUniqueRunNames (Positive (Small n)) (NonNegative m) = ioProperty $
    withTempIOHasBlockIO "TODO" $ \hfs hbio -> do
      prop1 <- withSession nullTracer hfs hbio (FS.mkFsPath []) $ \sesh -> do
        withTable sesh conf $ \th -> do
          updates const upds th
          withOpenTable th $ \thEnv -> do
            RW.withReadAccess (tableContent thEnv) $ \tc -> do
              let (Sum nruns) = V.foldMap
                                  (V.foldMap (const (Sum (1 :: Int))) . residentRuns)
                                  (tableLevels tc)
              pure $ tabulate "number of runs on disk" [showPowersOf 2 nruns]
                  $ True

      withSession nullTracer hfs hbio (FS.mkFsPath []) $ \sesh -> do
        withTable sesh conf $ \th -> do
          eith <- try (updates const upds th)
          fmap (prop1 .&&.) $ case eith of
            Left (e :: FS.FsError)
              | FS.fsErrorType e == FS.FsResourceAlreadyExist
              -> pure $ counterexample "Test failed... found an FsResourceAlreadyExist error" False
              | otherwise
              -> throwIO e
            Right () -> pure $ property True
  where
    conf = testTableConfig {
        confWriteBufferAlloc = AllocNumEntries (NumEntries n)
      }

    upds = V.fromList [ (serialiseKey i, Insert (serialiseValue i))
                      | (i :: Word64) <- fmap fromIntegral [1..m]
                      ]

-- | Check that opening a populated table via the interim table loading function
-- works as expected. Roughly, we test:
--
-- @
--  inserts th kvs == open' (snapshot' (inserts th kvs))
-- @
--
-- TODO: remove once we have proper snapshotting
prop_interimOpenTable ::
     Test.InMemLookupData SerialisedKey SerialisedValue SerialisedBlob
  -> Property
prop_interimOpenTable dat = ioProperty $
    withTempIOHasBlockIO "prop_interimOpenTable" $ \hfs hbio -> do
      withSession nullTracer hfs hbio (FS.mkFsPath []) $ \sesh -> do
        withTable sesh conf $ \th -> do
          updates const upds th
          let snap = fromMaybe (error "invalid name") $ mkSnapshotName "snap"
          numRunsSnapped <- snapshot const snap "someLabel" th
          th' <- open sesh "someLabel" configNoOverride snap
          lhs <- lookups const ks th
          rhs <- lookups const ks th'
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
    conf = testTableConfig

    Test.InMemLookupData { runData, lookups = keysToLookup } = dat
    -- TODO: include inserts with blobs once blob retrieval is implemented
    runDataWithoutblobs = Map.map f runData
      where f (InsertWithBlob v _) = Insert v
            f x                    = x
    ks = V.map serialiseKey (V.fromList keysToLookup)
    upds = V.fromList $ fmap (bimap serialiseKey (bimap serialiseValue serialiseBlob))
                      $ Map.toList runDataWithoutblobs

-- | Check that reading from a cursor returns exactly the entries that have
-- been inserted into the table. Roughly:
--
-- @
--  readCursor . newCursor . inserts == id
-- @
--
-- If an offset is provided:
--
-- @
--  readCursor . newCursorAt offset . inserts == dropWhile ((< offset) . key)
-- @
prop_roundtripCursor ::
     Maybe KeyForIndexCompact
  -> V.Vector (KeyForIndexCompact, Entry SerialisedValue SerialisedBlob)
  -> Property
prop_roundtripCursor offset kops = ioProperty $
    withTempIOHasBlockIO "prop_roundtripCursor" $ \hfs hbio -> do
      withSession nullTracer hfs hbio (FS.mkFsPath []) $ \sesh -> do
        withTable sesh conf $ \th -> do
          updates appendSerialisedValue (coerce kopsWithoutblobs) th
          fromCursor <- withCursor offsetKey th $
            readWholeCursor appendSerialisedValue
          return $ tabulate "duplicates" (show <$> Map.elems duplicates) $
            expected === fromCursor
  where
    conf = testTableConfig

    -- TODO: include inserts with blobs once write buffer returns BlobRefs
    kopsWithoutblobs = V.map (fmap f) kops
      where f (InsertWithBlob v _) = Insert v
            f x                    = x

    offsetKey = maybe NoOffsetKey (OffsetKey . coerce) offset

    expected =
      V.fromList . mapMaybe (traverse entryToValue) $
        maybe id (\k -> dropWhile ((< k) . fst)) offset $
          Map.assocs . Map.fromListWith (combine appendSerialisedValue) $
            V.toList kopsWithoutblobs

    entryToValue :: Entry v b -> Maybe (v, Maybe b)
    entryToValue = \case
      Insert v           -> Just (v, Nothing)
      InsertWithBlob v b -> Just (v, Just b)
      Mupdate v          -> Just (v, Nothing)
      Delete             -> Nothing

    duplicates :: Map.Map KeyForIndexCompact Int
    duplicates =
      Map.filter (> 1) $
        Map.fromListWith (+) . map (\(k, _) -> (k, 1)) $
          V.toList kops

readWholeCursor ::
     ResolveSerialisedValue
  -> Cursor IO h
  -> IO (V.Vector (KeyForIndexCompact, (SerialisedValue, Maybe b)))
readWholeCursor resolve cursor = go V.empty
  where
    chunkSize = 50
    toResult k v b = (coerce k, (v, fmap toBlob b))

    go !acc = do
      res <- readCursor resolve chunkSize cursor toResult
      if V.length res < chunkSize then return (acc <> res)
                                  else go (acc <> res)

    toBlob = error "blobs not supported yet"

appendSerialisedValue :: ResolveSerialisedValue
appendSerialisedValue (SerialisedValue x) (SerialisedValue y) =
    SerialisedValue (x <> y)
