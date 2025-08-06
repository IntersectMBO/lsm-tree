{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module Test.Database.LSMTree (tests) where

import           Control.Exception
import           Control.Tracer
import           Data.Function (on)
import           Data.IORef
import           Data.Monoid
import           Data.Typeable (Typeable)
import qualified Data.Vector as V
import qualified Data.Vector.Algorithms as VA
import           Data.Void
import           Data.Word
import           Database.LSMTree
import           Database.LSMTree.Extras (showRangesOf)
import           Database.LSMTree.Extras.Generators ()
import qualified System.FS.API as FS
import qualified System.FS.BlockIO.API as FS
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
          -- happy path
        , testProperty "prop_newSession_restoreSession_happyPath" prop_newSession_restoreSession_happyPath
          -- missing session directory
        , testProperty "prop_sessionDirDoesNotExist" prop_sessionDirDoesNotExist
          -- session directory already locked
        , testProperty "prop_sessionDirLocked" prop_sessionDirLocked
          -- malformed session directory
        , testProperty "prop_sessionDirCorrupted" prop_sessionDirCorrupted
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

-- | If 'New', use 'newSession', otherwise if 'Restore', use 'restoreSession'.
--
-- This allows us to run properties on both 'newSession' and 'restoreSession',
-- without having to write almost identical code twice.
--
-- In a sense, this is somewhat similar to 'openSession', but whereas
-- 'openSession' would defer to 'newSession' or 'restoreSession' based on the
-- directory contents, here the user gets to pick whether to use 'newSession' or
-- 'restoreSession'.
withNewSessionOrRestoreSession ::
     (IOLike m, Typeable h)
  => NewOrRestore
  -> Tracer m LSMTreeTrace
  -> FS.HasFS m h
  -> FS.HasBlockIO m h
  -> Salt
  -> FS.FsPath
  -> (Session m -> m a)
  -> m a
withNewSessionOrRestoreSession newOrRestore tr hfs hbio salt path =
    case newOrRestore of
      New     -> withNewSession tr hfs hbio salt path
      Restore -> withRestoreSession tr hfs hbio path

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
    pure $ results === ["Created", "New", "Open"]
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
    pure $ results === ["Created", "Restore", "Open"]
  where
    testSalt = 6

-- | A tracer that records session open, session new, and session restore
-- messages in a mutable variable.
mkSessionOpenModeTracer :: IORef [String] -> Tracer IO LSMTreeTrace
mkSessionOpenModeTracer var = Tracer $ emit $ \case
    TraceSession _ TraceOpenSession{} -> modifyIORef var ("Open" :)
    TraceSession _ TraceNewSession{} -> modifyIORef var ("New" :)
    TraceSession _ TraceRestoreSession{} -> modifyIORef var ("Restore" :)
    TraceSession _ TraceCreatedSession{} -> modifyIORef var ("Created" :)
    _ -> pure ()

{-------------------------------------------------------------------------------
  Session: happy path
-------------------------------------------------------------------------------}

prop_newSession_restoreSession_happyPath ::
     Positive (Small Int)
  -> V.Vector (Key, Value)
  -> Property
prop_newSession_restoreSession_happyPath (Positive (Small bufferSize)) ins =
    ioProperty $
    withTempIOHasBlockIO "prop_newSession_restoreSession_happyPath" $ \hfs hbio -> do
    withNewSession nullTracer hfs hbio testSalt (FS.mkFsPath []) $ \session1 ->
      withTableWith conf session1 $ \(table :: Table IO Key Value Blob) -> do
        inserts table $ V.map (\(k, v) -> (k, v, Nothing)) ins
        saveSnapshot "snap" "KeyValueBlob" table
    withRestoreSession nullTracer hfs hbio (FS.mkFsPath []) $ \session2 ->
      withTableFromSnapshot session2 "snap" "KeyValueBlob"
        $ \(_ :: Table IO Key Value Blob) -> pure ()
  where
    testSalt = 6
    conf = defaultTableConfig {
        confWriteBufferAlloc = AllocNumEntries bufferSize
      }

{-------------------------------------------------------------------------------
  Session: missing session directory
-------------------------------------------------------------------------------}

prop_sessionDirDoesNotExist :: NewOrRestore -> Property
prop_sessionDirDoesNotExist newOrRestore =
    ioProperty $
    withTempIOHasBlockIO "prop_sessionDirDoesNotExist" $ \hfs hbio -> do
    result <- try @SessionDirDoesNotExistError $
      withNewSessionOrRestoreSession
        newOrRestore
        nullTracer hfs hbio testSalt (FS.mkFsPath ["missing-dir"])
        $ \_session -> pure ()
    pure
      $ counterexample
          ("Expecting an ErrSessionDirDoesNotExist error, but got: " ++ show result)
      $ case result of
          Left ErrSessionDirDoesNotExist{} -> True
          _                                -> False
  where
    testSalt = 6

{-------------------------------------------------------------------------------
  Session: session directory already locked
-------------------------------------------------------------------------------}

prop_sessionDirLocked :: NewOrRestore -> Property
prop_sessionDirLocked newOrRestore =
    ioProperty $
    withTempIOHasBlockIO "prop_sessionDirLocked" $ \hfs hbio -> do
    result <-
      withNewSession nullTracer hfs hbio testSalt (FS.mkFsPath []) $ \_session1 -> do
        try @SessionDirLockedError $
          withNewSessionOrRestoreSession
            newOrRestore
            nullTracer hfs hbio testSalt (FS.mkFsPath [])
            $ \_session2 -> pure ()
    pure
      $ counterexample
          ("Expecting an ErrSessionDirLocked error, but got: " ++ show result)
      $ case result of
        Left ErrSessionDirLocked{} -> True
        _                          -> False
  where
    testSalt = 6

{-------------------------------------------------------------------------------
  Session: malformed session directory
-------------------------------------------------------------------------------}

prop_sessionDirCorrupted :: NewOrRestore -> Property
prop_sessionDirCorrupted newOrRestore =
    ioProperty $
    withTempIOHasBlockIO "sessionDirCorrupted" $ \hfs hbio -> do
    FS.createDirectory hfs (FS.mkFsPath ["unexpected-directory"])
    result <- try @SessionDirCorruptedError $
      withNewSessionOrRestoreSession
        newOrRestore
        nullTracer hfs hbio testSalt (FS.mkFsPath [])
        $ \_session -> pure ()
    pure
      $ counterexample
          ("Expecting an ErrSessionDirCorrupted error, but got: " ++ show result)
      $ case result of
        Left ErrSessionDirCorrupted{} -> True
        _                             -> False
  where
    testSalt = 6

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
