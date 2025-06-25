{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module Test.Database.LSMTree (tests) where

import           Control.Tracer
import           Data.Function (on)
import           Data.IORef
import           Data.Monoid
import qualified Data.Vector as V
import qualified Data.Vector.Algorithms as VA
import           Data.Void
import           Data.Word
import           Database.LSMTree
import           Database.LSMTree.Extras (showRangesOf)
import           Database.LSMTree.Extras.Generators ()
import qualified System.FS.API as FS
import           Test.QuickCheck
import           Test.Tasty
import           Test.Tasty.QuickCheck
import           Test.Util.FS

tests :: TestTree
tests = testGroup "Test.Database.LSMTree" [
      testGroup "Session" [
          -- openSession
          testProperty "prop_openSession_newSession" prop_openSession_newSession
        , testProperty "prop_openSession_restoreSession" prop_openSession_restoreSession
          -- salt
        , testProperty "prop_goodAndBadSessionSalt" prop_goodAndBadSessionSalt
        ]
    ]

{-------------------------------------------------------------------------------
  Test types and utilities
-------------------------------------------------------------------------------}

newtype Key = Key Word64
  deriving stock (Show, Eq, Ord)
  deriving newtype (Arbitrary, SerialiseKey)

newtype Value = Value Word64
  deriving stock (Show, Eq)
  deriving newtype (Arbitrary, SerialiseValue)
  deriving ResolveValue via Sum Word64

newtype Blob = Blob Word64
  deriving stock (Show, Eq)
  deriving newtype (Arbitrary, SerialiseValue)

data NewOrRestore = New | Restore
  deriving stock (Show, Eq, Bounded, Enum)

instance Arbitrary NewOrRestore where
  arbitrary = arbitraryBoundedEnum
  shrink = shrinkBoundedEnum

{-------------------------------------------------------------------------------
  Session: openSession
-------------------------------------------------------------------------------}

-- | When the session directory is empty, 'openSession' will call 'newSession'
prop_openSession_newSession :: Property
prop_openSession_newSession =
    ioProperty $
    withTempIOHasBlockIO "prop_openSession_newSession" $ \hfs hbio -> do
    -- Use resultsVar to record which session functions were called
    resultsVar <- newIORef []
    withOpenSession
      (mkSessionOpenModeTracer resultsVar) hfs hbio
      testSalt (FS.mkFsPath [])
      $ \_session -> pure ()
    results <- readIORef resultsVar
    -- Check that we first called openSession, then newSession
    pure $ results === ["New", "Open"]
  where
    testSalt = 6

-- | When the session directory is non-empty, 'openSession' will call 'restoreSession'
prop_openSession_restoreSession :: Property
prop_openSession_restoreSession =
    ioProperty $
    withTempIOHasBlockIO "prop_openSession_restoreSession" $ \hfs hbio -> do
    withOpenSession nullTracer hfs hbio testSalt (FS.mkFsPath [])
      $ \_session1 -> pure ()
    -- Use resultsVar to record which session functions were called
    resultsVar <- newIORef []
    withOpenSession
      (mkSessionOpenModeTracer resultsVar) hfs hbio
      testSalt (FS.mkFsPath [])
      $ \_session2 -> pure ()
    results <- readIORef resultsVar
    -- Check that we first called openSession, then restoreSession
    pure $ results === ["Restore", "Open"]
  where
    testSalt = 6

-- | A tracer that records session open, session new, and session restore
-- messages in a mutable variable.
mkSessionOpenModeTracer :: IORef [String] -> Tracer IO LSMTreeTrace
mkSessionOpenModeTracer var = Tracer $ emit $ \case
    TraceOpenSession{} -> modifyIORef var ("Open" :)
    TraceNewSession{} -> modifyIORef var ("New" :)
    TraceRestoreSession{} -> modifyIORef var ("Restore" :)
    _ -> pure ()

{-------------------------------------------------------------------------------
  Session: salt
-------------------------------------------------------------------------------}

-- | When we call 'openSession' on an existing session directory, then the salt
-- value we pass in is ignored and the actual salt is restored from a metatada
-- file instead. This property verifies that we indeed ignore the salt value by
-- checking that lookups return the right results, which wouldn't happen if the
-- wrong salt was used.
--
-- NOTE: this only tests with /positive/ lookups, i.e., lookups for keys that
-- are known to exist in the tables.
prop_goodAndBadSessionSalt ::
     Positive (Small Int)
  -> V.Vector (Key, Value)
  -> Property
prop_goodAndBadSessionSalt (Positive (Small bufferSize)) ins =
    checkCoverage $
    ioProperty $
    withTempIOHasBlockIO "prop_sessionSalt" $ \hfs hbio -> do
      -- Open a session and create a snapshot for some arbitrary table contents
      withOpenSession nullTracer hfs hbio goodSalt sessionDir $ \session ->
        withTableWith conf session $ \(table :: Table IO Key Value Void) -> do
          inserts table $ V.map (\(k, v) -> (k, v, Nothing)) insWithoutDupKeys
          saveSnapshot "snap" "KeyValueBlob" table

      -- Determine the expected results of key lookups
      let
        expectedValues :: V.Vector (Maybe Value)
        expectedValues = V.map (Just . snd) insWithoutDupKeys

      -- Open the session using the good salt, open the snapshot, perform lookups
      goodSaltLookups <-
        withOpenSession nullTracer hfs hbio goodSalt sessionDir $ \session ->
          withTableFromSnapshot session "snap" "KeyValueBlob" $ \(table :: Table IO Key Value Void) -> do
            lookups table $ V.map fst insWithoutDupKeys

      -- Determine the result of key lookups using the good salt
      let
        goodSaltValues :: V.Vector (Maybe Value)
        goodSaltValues = V.map getValue goodSaltLookups

      -- Open the session using a bad salt, open the snapshot, perform lookups
      badSaltLookups <-
        withOpenSession nullTracer hfs hbio badSalt sessionDir $ \session ->
          withTableFromSnapshot session "snap" "KeyValueBlob" $ \(table :: Table IO Key Value Void) -> do
            lookups table $ V.map fst insWithoutDupKeys

      -- Determine the result of key lookups using a bad salt
      let
        badSaltValues :: V.Vector (Maybe Value)
        badSaltValues = V.map getValue badSaltLookups

      pure $
        tabulate "number of keys" [ showRangesOf 10 (V.length insWithoutDupKeys) ] $
        -- Regardless of whether the salt we passed to 'openSession' was a good
        -- or bad salt, the lookup results are correct.
        expectedValues === badSaltValues .&&.
        expectedValues === goodSaltValues
  where
    -- Duplicate keys in inserts make the property more complicated, because
    -- keys that are inserted /earlier/ (towards the head of the vector) are
    -- overridden by keys that are inserted /later/ (towards the tail of the
    -- vector). So, we remove duplicate keys instead
    insWithoutDupKeys :: V.Vector (Key, Value)
    insWithoutDupKeys = VA.nubBy (compare `on` fst) ins

    goodSalt :: Salt
    goodSalt = 17

    badSalt :: Salt
    badSalt = 19

    sessionDir = FS.mkFsPath []

    conf = defaultTableConfig {
        confWriteBufferAlloc = AllocNumEntries bufferSize
      }
