{-# LANGUAGE LambdaCase #-}

module Test.Database.LSMTree.Internal (tests) where

import           Control.Exception
import           Control.Monad (void)
import           Database.LSMTree.Internal
import qualified System.FS.API as FS
import           Test.Tasty
import           Test.Tasty.HUnit
import           Test.Util.FS

tests :: TestTree
tests = testGroup "Test.Database.LSMTree.Internal" [
      testCase "newSession" newSession
    , testCase "restoreSession" restoreSession
    , testCase "twiceOpenSession" twiceOpenSession
    , testCase "sessionDirLayoutMismatch" sessionDirLayoutMismatch
    , testCase "sessionDirDoesNotExist" sessionDirDoesNotExist
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
      _ -> assertFailure "Opening a session twice in the same directory \
                         \should fail with an SessionDirLocked error."

sessionDirLayoutMismatch :: Assertion
sessionDirLayoutMismatch = withTempIOHasBlockIO "sessionDirLayoutMismatch" $ \hfs hbio -> do
    FS.createDirectory hfs (FS.mkFsPath ["unexpected-directory"])
    try @LSMTreeError (openSession hfs hbio (FS.mkFsPath [])) >>= \case
      Left (SessionDirMalformed _) -> pure ()
      _ -> assertFailure "Restoring a session in a directory with a wrong \
                         \layout should fail with a SessionDirMalformed."

sessionDirDoesNotExist :: Assertion
sessionDirDoesNotExist = withTempIOHasBlockIO "sessionDirDoesNotExist" $ \hfs hbio -> do
    try @LSMTreeError (openSession hfs hbio (FS.mkFsPath ["missing-dir"])) >>= \case
      Left (SessionDirDoesNotExist _) -> pure ()
      _ -> assertFailure "Opening a session in a non-existent directory should \
                         \fail with a SessionDirDoesNotExist error."
