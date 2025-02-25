module Test.Database.LSMTree.Internal.BlobFile.FS (tests) where

import           Control.Concurrent.Class.MonadSTM.Strict
import           Control.Monad
import           Control.Monad.Class.MonadThrow
import           Control.RefCount
import           Database.LSMTree.Internal.BlobFile
import           System.FS.API
import           System.FS.Sim.Error hiding (genErrors)
import qualified System.FS.Sim.MockFS as MockFS
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
     Bool -- ^ create the file or not
  -> OpenMode
  -> NoCleanupErrors
  -> NoCleanupErrors
  -> Property
prop_fault_openRelease doCreateFile om
  (NoCleanupErrors openErrors)
  (NoCleanupErrors releaseErrors) =
    ioProperty $
    withSimErrorHasFS propPost MockFS.empty emptyErrors $ \hfs fsVar errsVar -> do
      when doCreateFile $
        withFile hfs path (WriteMode MustBeNew) $ \_ -> pure ()
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
    root = mkFsPath []
    path = mkFsPath ["blobfile"]

    acquire hfs errsVar =
        withErrors errsVar openErrors $ openBlobFile hfs path om

    release errsVar blobFile =
        withErrors errsVar releaseErrors $ releaseRef blobFile

    propPost fs = propNoOpenHandles fs .&&.
        if doCreateFile then
          case allowExisting om of
            AllowExisting ->
              -- TODO: fix, see the TODO on openBlobFile
              propNoDirEntries root fs .||. propNumDirEntries root 1 fs
            MustBeNew ->
              propNumDirEntries root 1 fs
            MustExist ->
              -- TODO: fix, see the TODO on openBlobFile
              propNoDirEntries root fs .||. propNumDirEntries root 1 fs
        else
          propNoDirEntries root fs

