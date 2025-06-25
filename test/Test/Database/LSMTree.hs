{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module Test.Database.LSMTree (tests) where

import           Control.Tracer
import           Data.Function (on)
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
      testProperty "prop_goodAndBadSessionSalt" prop_goodAndBadSessionSalt
    ]

-- | For now, the session salt is always set by the user when opening a session.
-- This is fine for new sessions, because there is no data in the session
-- directory yet, but if the user passes the wrong salt when restoring a
-- session, then bad things can happen. In particular, bloom filters stored in a
-- snapshot might have been created with one salt, but if we query them with a
-- different salt, then the query results are bad. This test verifies that
-- reopening a session and snapshot with a good salt leads to good lookup
-- results, and it verifies that doing the same with a bad salt leads to bad
-- lookup results (most of the time).
--
-- NOTE: this only tests with /positive/ lookups, i.e., lookups for keys that
-- are known to exist in the tables.
--
-- TODO: store the session salt in the session directory, so that the user can
-- not set a bad salt.
prop_goodAndBadSessionSalt ::
     Positive (Small Int)
  -> V.Vector (Key, Value)
  -> Property
prop_goodAndBadSessionSalt (Positive (Small bufferSize)) ins =
    checkCoverage $
    ioProperty $
    withTempIOHasBlockIO "prop_sessionSalt" $ \hfs hbio -> do
      -- Open a session and create a snapshot for some arbitrary table contents
      withSession nullTracer hfs hbio goodSalt sessionDir $ \session ->
        withTableWith conf session $ \(table :: Table IO Key Value Void) -> do
          inserts table $ V.map (\(k, v) -> (k, v, Nothing)) insWithoutDupKeys
          saveSnapshot "snap" "KeyValueBlob" table

      -- Determine the expected results of key lookups
      let
        expectedValues :: V.Vector (Maybe Value)
        expectedValues = V.map (Just . snd) insWithoutDupKeys

      -- Restore the session using the good salt, open the snapshot, perform lookups
      goodLookups <-
        withSession nullTracer hfs hbio goodSalt sessionDir $ \session ->
          withTableFromSnapshot session "snap" "KeyValueBlob" $ \(table :: Table IO Key Value Void) -> do
            lookups table $ V.map fst insWithoutDupKeys

      -- Determine the result of key lookups using the good salt
      let
        goodValues :: V.Vector (Maybe Value)
        goodValues = V.map getValue goodLookups

      -- Restore the session using a bad salt, open the snapshot, perform lookups
      badLookups <-
        withSession nullTracer hfs hbio badSalt sessionDir $ \session ->
          withTableFromSnapshot session "snap" "KeyValueBlob" $ \(table :: Table IO Key Value Void) -> do
            lookups table $ V.map fst insWithoutDupKeys

      -- Determine the result of key lookups using a bad salt
      let
        badValues :: V.Vector (Maybe Value)
        badValues = V.map getValue badLookups

      pure $
        tabulate "number of keys" [ showRangesOf 10 (V.length insWithoutDupKeys) ] $
        -- For a significant portion of the cases, the lookups results obtained
        -- using a bad salt should mismatch the expected lookup results
        cover 40 ( expectedValues /= badValues ) "bad salt leads to bad lookups" $
        -- The lookup results using a good salt should /always/ match the
        -- expected lookup results.
        expectedValues === goodValues
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

newtype Key = Key Word64
  deriving stock (Show, Eq, Ord)
  deriving newtype (Arbitrary, SerialiseKey)

newtype Value = Value Word64
  deriving stock (Show, Eq)
  deriving newtype (Arbitrary, SerialiseValue)
  deriving ResolveValue via Sum Word64

newtype Blob = Blob Void
  deriving stock (Show, Eq)
  deriving newtype SerialiseValue
