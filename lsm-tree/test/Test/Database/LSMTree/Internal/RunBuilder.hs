{-# LANGUAGE LambdaCase #-}

module Test.Database.LSMTree.Internal.RunBuilder (tests) where

import           Control.Monad.Class.MonadThrow
import           Data.Foldable (traverse_)
import qualified Database.LSMTree.Internal.BloomFilter as Bloom
import           Database.LSMTree.Internal.Entry (NumEntries (..))
import qualified Database.LSMTree.Internal.Index as Index
import           Database.LSMTree.Internal.Paths (RunFsPaths (..))
import qualified Database.LSMTree.Internal.RunAcc as RunAcc
import qualified Database.LSMTree.Internal.RunBuilder as RunBuilder
import           Database.LSMTree.Internal.RunNumber
import qualified System.FS.API as FS
import           System.FS.API (HasFS)
import qualified System.FS.BlockIO.API as FS
import qualified System.FS.Sim.MockFS as MockFS
import           Test.Tasty
import           Test.Tasty.QuickCheck
import           Test.Util.FS (propNoOpenHandles, withSimHasBlockIO,
                     withTempIOHasBlockIO)

tests :: TestTree
tests = testGroup "Test.Database.LSMTree.Internal.RunBuilder" [
      testGroup "ioHasFS" [
          testProperty "prop_newInExistingDir" $ ioProperty $
            withTempIOHasBlockIO "prop_newInExistingDir" prop_newInExistingDir
        , testProperty "prop_newInNonExistingDir" $ ioProperty $
            withTempIOHasBlockIO "prop_newInNonExistingDir" prop_newInNonExistingDir
        , testProperty "prop_newTwice" $ ioProperty $
            withTempIOHasBlockIO "prop_newTwice" prop_newTwice
        ]
    , testGroup "simHasFS" [
          testProperty "prop_newInExistingDir" $ ioProperty $
            withSimHasBlockIO propNoOpenHandles MockFS.empty $
              \hfs hbio _ -> prop_newInExistingDir hfs hbio
        , testProperty "prop_newInNonExistingDir" $ ioProperty $
            withSimHasBlockIO propNoOpenHandles MockFS.empty $
              \hfs hbio _ -> prop_newInNonExistingDir hfs hbio
        , testProperty "prop_newTwice" $ ioProperty $
            withSimHasBlockIO propNoOpenHandles MockFS.empty $
              \hfs hbio _ -> prop_newTwice hfs hbio
        ]
    ]

runParams :: RunBuilder.RunParams
runParams =
    RunBuilder.RunParams {
      runParamCaching = RunBuilder.CacheRunData,
      runParamAlloc   = RunAcc.RunAllocFixed 10,
      runParamIndex   = Index.Ordinary
    }

testSalt :: Bloom.Salt
testSalt = 4

-- | 'new' in an existing directory should be successful.
prop_newInExistingDir :: HasFS IO h -> FS.HasBlockIO IO h -> IO Property
prop_newInExistingDir hfs hbio = do
    let runDir = FS.mkFsPath ["a", "b", "c"]
    FS.createDirectoryIfMissing hfs True runDir
    bracket
      (try (RunBuilder.new hfs hbio testSalt runParams (RunFsPaths runDir (RunNumber 17)) (NumEntries 0)))
      (traverse_ RunBuilder.close) $ pure . \case
        Left e@FS.FsError{} ->
          counterexample ("expected a success, but got: " <> show e) $ property False
        Right _ -> property True

-- | 'new' in a non-existing directory should throw an error.
prop_newInNonExistingDir :: HasFS IO h -> FS.HasBlockIO IO h -> IO Property
prop_newInNonExistingDir hfs hbio = do
    let runDir = FS.mkFsPath ["a", "b", "c"]
    bracket
      (try (RunBuilder.new hfs hbio testSalt runParams (RunFsPaths runDir (RunNumber 17)) (NumEntries 0)))
      (traverse_ RunBuilder.close) $ pure . \case
        Left FS.FsError{} -> property True
        Right _  ->
          counterexample ("expected an FsError, but got a RunBuilder") $ property False

-- | Calling 'new' twice with the same arguments should throw an error.
--
-- TODO: maybe in this case a custom error should be thrown? Does the thrown
-- 'FsError' cause file resources to leak?
prop_newTwice :: HasFS IO h -> FS.HasBlockIO IO h -> IO Property
prop_newTwice hfs hbio = do
    let runDir = FS.mkFsPath []
    bracket
      (RunBuilder.new hfs hbio testSalt runParams (RunFsPaths runDir (RunNumber 17)) (NumEntries 0))
      RunBuilder.close $ \_ ->
        bracket
          (try (RunBuilder.new hfs hbio testSalt runParams (RunFsPaths runDir (RunNumber 17)) (NumEntries 0)))
          (traverse_ RunBuilder.close) $ pure . \case
            Left FS.FsError{} -> property True
            Right _  ->
              counterexample ("expected an FsError, but got a RunBuilder") $ property False
