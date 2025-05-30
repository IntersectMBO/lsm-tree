{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module Test.Database.LSMTree.Internal (tests) where

import           Control.Concurrent.Class.MonadMVar (MonadMVar)
import           Control.Concurrent.Class.MonadSTM (MonadSTM)
import           Control.Exception
import           Control.Monad.Class.MonadThrow (MonadMask)
import           Control.Monad.Primitive (PrimMonad)
import           Control.Tracer
import           Data.Bifunctor (Bifunctor (..))
import           Data.Coerce (coerce)
import qualified Data.Map.Strict as Map
import           Data.Maybe (isJust, mapMaybe)
import qualified Data.Vector as V
import           Data.Word
import           Database.LSMTree.Extras.Generators ()
import           Database.LSMTree.Internal.BlobRef
import           Database.LSMTree.Internal.Config
import           Database.LSMTree.Internal.Entry
import           Database.LSMTree.Internal.Serialise
import           Database.LSMTree.Internal.Unsafe
import qualified System.FS.API as FS
import           Test.QuickCheck
import           Test.Tasty
import           Test.Tasty.HUnit
import           Test.Tasty.QuickCheck
import           Test.Util.FS

tests :: TestTree
tests = testGroup "Test.Database.LSMTree.Internal" [
      testGroup "Session" [
          testProperty "newSession" newSession
        , testProperty "restoreSession" restoreSession
        , testProperty "sessionDirLocked" sessionDirLocked
        , testCase "sessionDirCorrupted" sessionDirCorrupted
        , testCase "sessionDirDoesNotExist" sessionDirDoesNotExist
        ]
    , testGroup "Cursor" [
          testProperty "prop_roundtripCursor" $ withMaxSuccess 500 $
            prop_roundtripCursor
        ]
    ]

testTableConfig :: TableConfig
testTableConfig = defaultTableConfig {
      -- Write buffer size is small on purpose, so that the test actually
      -- flushes and merges.
      confWriteBufferAlloc = AllocNumEntries 3
    }

newSession ::
     Positive (Small Int)
  -> V.Vector (Word64, Entry Word64 Word64)
  -> Property
newSession (Positive (Small bufferSize)) es =
    ioProperty $
    withTempIOHasBlockIO "newSession" $ \hfs hbio ->
    withSession nullTracer hfs hbio (FS.mkFsPath []) $ \session ->
      withTable session conf (updates const es')
  where
    conf = testTableConfig {
        confWriteBufferAlloc = AllocNumEntries bufferSize
      }
    es' = fmap (bimap serialiseKey (bimap serialiseValue serialiseBlob)) es

restoreSession ::
     Positive (Small Int)
  -> V.Vector (Word64, Entry Word64 Word64)
  -> Property
restoreSession (Positive (Small bufferSize)) es =
    ioProperty $
    withTempIOHasBlockIO "restoreSession" $ \hfs hbio -> do
      withSession nullTracer hfs hbio (FS.mkFsPath []) $ \session1 ->
        withTable session1 conf (updates const es')
      withSession nullTracer hfs hbio (FS.mkFsPath []) $ \session2 ->
        withTable session2 conf (updates const es')
  where
    conf = testTableConfig {
        confWriteBufferAlloc = AllocNumEntries bufferSize
      }
    es' = fmap (bimap serialiseKey (bimap serialiseValue serialiseBlob)) es

sessionDirLocked :: Property
sessionDirLocked = ioProperty $
    withTempIOHasBlockIO "sessionDirLocked" $ \hfs hbio -> do
      bracket (openSession nullTracer hfs hbio (FS.mkFsPath [])) closeSession $ \_sesh1 ->
        bracket (try @SessionDirLockedError $ openSession nullTracer hfs hbio (FS.mkFsPath [])) tryCloseSession $ \case
          Left (ErrSessionDirLocked _dir) -> pure ()
          x -> assertFailure $ "Opening a session twice in the same directory \
                              \should fail with an ErrSessionDirLocked error, but \
                              \it returned this instead: " <> showLeft "Session" x

sessionDirCorrupted :: Assertion
sessionDirCorrupted =
  withTempIOHasBlockIO "sessionDirCorrupted" $ \hfs hbio -> do
    FS.createDirectory hfs (FS.mkFsPath ["unexpected-directory"])
    bracket (try @SessionDirCorruptedError (openSession nullTracer hfs hbio (FS.mkFsPath []))) tryCloseSession $ \case
      Left (ErrSessionDirCorrupted _dir) -> pure ()
      x -> assertFailure $ "Restoring a session in a directory with a wrong \
                           \layout should fail with a ErrSessionDirCorrupted, but \
                           \it returned this instead: " <> showLeft "Session" x

sessionDirDoesNotExist :: Assertion
sessionDirDoesNotExist = withTempIOHasBlockIO "sessionDirDoesNotExist" $ \hfs hbio -> do
  bracket (try @SessionDirDoesNotExistError (openSession nullTracer hfs hbio (FS.mkFsPath ["missing-dir"]))) tryCloseSession $ \case
      Left (ErrSessionDirDoesNotExist _dir) -> pure ()
      x -> assertFailure $ "Opening a session in a non-existent directory should \
                           \fail with a ErrSessionDirDoesNotExist error, but it \
                           \returned this instead: " <> showLeft "Session" x

-- | Internal helper: close a session opened with 'try'.
tryCloseSession :: (MonadMask m, MonadSTM m, MonadMVar m, PrimMonad m) => Either e (Session m h) -> m ()
tryCloseSession = either (const $ pure ()) closeSession

showLeft :: Show a => String -> Either a b -> String
showLeft x = \case
    Left e -> show e
    Right _ -> x

-- | Check that reading from a cursor returns exactly the entries that have
-- been inserted into the table. Roughly:
--
-- @
--  readCursor . newCursor . inserts == id
-- @
--
-- If lower and upper bound are provided:
--
-- @
--  readCursorWhile (<= ub) . newCursorAt lb . inserts
--    == takeWhile ((<= ub) . key) . dropWhile ((< lb) . key)
-- @
prop_roundtripCursor ::
     Maybe SerialisedKey  -- ^ Inclusive lower bound
  -> Maybe SerialisedKey  -- ^ Inclusive upper bound
  -> V.Vector (SerialisedKey, Entry SerialisedValue SerialisedBlob)
  -> Property
prop_roundtripCursor lb ub kops = ioProperty $
    withTempIOHasBlockIO "prop_roundtripCursor" $ \hfs hbio -> do
      withSession nullTracer hfs hbio (FS.mkFsPath []) $ \sesh -> do
        withTable sesh conf $ \t -> do
          updates resolve (coerce kops) t
          fromCursor <- withCursor resolve (toOffsetKey lb) t $ \c ->
            fetchBlobs hfs =<< readCursorUntil ub c
          pure $
            tabulate "duplicates" (show <$> Map.elems duplicates) $
            tabulate "any blobs" [show (any (isJust . snd . snd) fromCursor)] $
            expected === fromCursor
  where
    conf = testTableConfig

    fetchBlobs :: FS.HasFS IO h
             ->     V.Vector (k, (v, Maybe (WeakBlobRef IO h)))
             -> IO (V.Vector (k, (v, Maybe SerialisedBlob)))
    fetchBlobs hfs = traverse (traverse (traverse (traverse (readWeakBlobRef hfs))))

    toOffsetKey = maybe NoOffsetKey (OffsetKey . coerce)

    expected =
      V.fromList . mapMaybe (traverse entryToValue) $
        maybe id (\k -> takeWhile ((<= k) . fst)) ub $
          maybe id (\k -> dropWhile ((< k) . fst)) lb $
            Map.assocs . Map.fromListWith (combine resolve) $
              V.toList kops

    entryToValue :: Entry v b -> Maybe (v, Maybe b)
    entryToValue = \case
      Insert v           -> Just (v, Nothing)
      InsertWithBlob v b -> Just (v, Just b)
      Upsert v          -> Just (v, Nothing)
      Delete             -> Nothing

    duplicates :: Map.Map SerialisedKey Int
    duplicates =
      Map.filter (> 1) $
        Map.fromListWith (+) . map (\(k, _) -> (k, 1)) $
          V.toList kops

readCursorUntil ::
     Maybe SerialisedKey  -- Inclusive upper bound
  -> Cursor IO h
  -> IO (V.Vector (SerialisedKey,
                   (SerialisedValue,
                    Maybe (WeakBlobRef IO h))))
readCursorUntil ub cursor = go V.empty
  where
    chunkSize = 50
    toResult k v b = (coerce k, (v, b))

    go !acc = do
      res <- case ub of
        Nothing -> readCursor resolve chunkSize cursor toResult
        Just k  -> readCursorWhile resolve (<= coerce k) chunkSize cursor toResult
      if V.length res < chunkSize then pure (acc <> res)
                                  else go (acc <> res)

resolve :: ResolveSerialisedValue
resolve (SerialisedValue x) (SerialisedValue y) = SerialisedValue (x <> y)
