module Test.Database.LSMTree.Internal.BlobFile.FS (tests) where

import           Control.Concurrent.Class.MonadSTM.Strict
import           Control.Monad.Class.MonadThrow
import           Control.RefCount
import           Database.LSMTree.Internal.BlobFile
import           System.FS.API
import           System.FS.Sim.Error hiding (genErrors)
import qualified System.FS.Sim.MockFS as MockFS
import qualified System.FS.Sim.Stream as Stream
import           Test.Tasty
import           Test.Tasty.QuickCheck as QC
import           Test.Util.FS

tests :: TestTree
tests = testGroup "Test.Database.LSMTree.Internal.BlobFile.FS" [
      testProperty "prop_fault_openRelease" prop_fault_openRelease
    ]

-- Test that opening and releasing a blob file properly cleans handles and files
-- in the presence of disk faults.
prop_fault_openRelease ::
     TestErrors
  -> TestErrors
  -> Property
prop_fault_openRelease (TestErrors openErrors) (TestErrors releaseErrors) =
    ioProperty $
    withSimErrorHasFS propPost MockFS.empty emptyErrors $ \hfs fsVar errsVar -> do
      eith <- try @_ @FsError $
        bracket (acquire hfs errsVar) (release errsVar) $ \_blobFile -> do
          fs' <- atomically $ readTMVar fsVar
          pure $ propNumOpenHandles 1 fs' .&&. propNumDirEntries (mkFsPath []) 1 fs'
      pure $ case eith of
        Left{}     ->
          label "FsError" $ property True
        Right prop ->
          label "Success" $ prop
  where
    acquire hfs errsVar =
        withErrors errsVar openErrors $
          openBlobFile hfs (mkFsPath ["blobfile"]) (ReadWriteMode MustBeNew)

    release errsVar blobFile =
      withErrors errsVar releaseErrors $ releaseRef blobFile

    propPost fs = propNoOpenHandles fs .&&. propNoDirEntries (mkFsPath []) fs

-- | No errors on closing file handles and removing files
data TestErrors = TestErrors Errors
  deriving stock Show

mkTestErrors :: Errors -> TestErrors
mkTestErrors errs = TestErrors $ errs {
      hCloseE = Stream.empty
    , removeFileE = Stream.empty
    }

instance Arbitrary TestErrors where
  arbitrary = do
      errs <- arbitrary
      pure $ mkTestErrors errs

  shrink (TestErrors errs) = TestErrors <$> shrink errs
