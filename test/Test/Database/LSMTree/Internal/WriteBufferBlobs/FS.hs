module Test.Database.LSMTree.Internal.WriteBufferBlobs.FS (tests) where

import           Control.Concurrent.Class.MonadSTM.Strict
import           Control.Monad
import           Control.Monad.Class.MonadThrow
import           Control.RefCount
import           Database.LSMTree.Internal.WriteBufferBlobs
import           System.FS.API
import           System.FS.Sim.Error hiding (genErrors)
import qualified System.FS.Sim.MockFS as MockFS
import           Test.Tasty
import           Test.Tasty.QuickCheck as QC
import           Test.Util.FS

tests :: TestTree
tests = testGroup "Test.Database.LSMTree.Internal.WriteBufferBlobs.FS" [
      testProperty "prop_fault_openRelease" prop_fault_openRelease
    ]

-- Test that opening and releasing a 'WriteBufferBlobs' properly cleans handles
-- and files in the presence of disk faults.
--
-- By testing 'open', we also test 'new'.
prop_fault_openRelease ::
     Bool -- ^ create the file or not
  -> AllowExisting
  -> NoCleanupErrors
  -> NoCleanupErrors
  -> Property
prop_fault_openRelease doCreateFile ae
  (NoCleanupErrors openErrors)
  (NoCleanupErrors releaseErrors) =
    ioProperty $
    withSimErrorHasFS propPost MockFS.empty emptyErrors $ \hfs fsVar errsVar -> do
      when doCreateFile $
        withFile hfs path (WriteMode MustBeNew) $ \_ -> pure ()
      eith <- try @_ @FsError $
        bracket (acquire hfs errsVar) (release errsVar) $ \_wbb -> do
          fs' <- atomically $ readTMVar fsVar
          pure $ propNumOpenHandles 1 fs' .&&. propNumDirEntries root 1 fs'
      pure $ case eith of
        Left{}     -> do
          label "FsError" $ property True
        Right prop ->
          label "Success" $ prop
  where
    root = mkFsPath []
    path = mkFsPath ["wbb"]

    acquire hfs errsVar = withErrors errsVar openErrors $ open hfs path ae

    release errsVar wbb = withErrors errsVar releaseErrors $ releaseRef wbb

    propPost fs = propNoOpenHandles fs .&&.
        if doCreateFile then
          case ae of
            AllowExisting ->
              -- TODO: fix, see the TODO on openBlobFile
              propNoDirEntries root fs .||. propNumDirEntries root 1 fs
            MustBeNew ->
              propNumDirEntries root 1 fs
        else
          propNoDirEntries root fs
