{-# LANGUAGE LambdaCase #-}

module Test.Database.LSMTree.Internal.RunBuilder (tests) where

import           Control.Monad (void)
import           Control.Monad.Class.MonadThrow
import           Data.Functor ((<&>))
import           Database.LSMTree.Internal.Entry (NumEntries (..))
import           Database.LSMTree.Internal.Paths (RunFsPaths (..))
import           Database.LSMTree.Internal.RunAcc (RunBloomFilterAlloc (..))
import qualified Database.LSMTree.Internal.RunBuilder as RunBuilder
import           Database.LSMTree.Internal.RunNumber
import qualified System.FS.API as FS
import           System.FS.API (HasFS)
import           System.FS.IO (HandleIO)
import           Test.Tasty
import           Test.Tasty.QuickCheck
import           Test.Util.FS (withTempIOHasFS)

tests :: TestTree
tests = testGroup "Test.Database.LSMTree.Internal.RunBuilder" [
      testProperty "prop_newInExistingDir" $ ioProperty $
        withTempIOHasFS "prop_newInExistingDir" prop_newInExistingDir
    , testProperty "prop_newInNonExistingDir" $ ioProperty $
        withTempIOHasFS "prop_newInNonExistingDir" prop_newInNonExistingDir
    , testProperty "prop_newTwice" $ ioProperty $
        withTempIOHasFS "prop_newTwice" prop_newTwice
    ]

-- | 'new' in an existing directory should be succesfull.
prop_newInExistingDir :: HasFS IO HandleIO -> IO Property
prop_newInExistingDir hfs = do
    let runDir = FS.mkFsPath ["a", "b", "c"]
    FS.createDirectoryIfMissing hfs True runDir
    try (RunBuilder.new hfs (RunFsPaths runDir (RunNumber 17)) (NumEntries 0) (RunAllocFixed 10)) <&> \case
      Left e@FS.FsError{} ->
        counterexample ("expected a success, but got: " <> show e) $ property False
      Right _ -> property True

-- | 'new' in a non-existing directory should throw an error.
prop_newInNonExistingDir :: HasFS IO HandleIO -> IO Property
prop_newInNonExistingDir hfs = do
    let runDir = FS.mkFsPath ["a", "b", "c"]
    try (RunBuilder.new hfs (RunFsPaths runDir (RunNumber 17)) (NumEntries 0) (RunAllocFixed 10)) <&> \case
      Left FS.FsError{} -> property True
      Right _  ->
        counterexample ("expected an FsError, but got a RunBuilder") $ property False

-- | Calling 'new' twice with the same arguments should throw an error.
--
-- TODO: maybe in this case a custom error should be thrown? Does the thrown
-- 'FsError' cause file resources to leak?
prop_newTwice :: HasFS IO HandleIO -> IO Property
prop_newTwice hfs = do
    let runDir = FS.mkFsPath []
    void $ RunBuilder.new hfs (RunFsPaths runDir (RunNumber 17)) (NumEntries 0) (RunAllocFixed 10)
    try (RunBuilder.new hfs (RunFsPaths runDir (RunNumber 17)) (NumEntries 0) (RunAllocFixed 10)) <&> \case
      Left FS.FsError{} -> property True
      Right _  ->
        counterexample ("expected an FsError, but got a RunBuilder") $ property False
