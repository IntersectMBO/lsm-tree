module Test.Database.LSMTree.Internal.WriteBufferBlobs.FS (tests) where

import           Control.Concurrent.Class.MonadSTM.Strict
import           Control.Monad
import           Control.Monad.Class.MonadThrow
import           Control.RefCount
import           Database.LSMTree.Extras.Generators ()
import           Database.LSMTree.Internal.BlobRef (readRawBlobRef)
import           Database.LSMTree.Internal.Serialise (SerialisedBlob)
import           Database.LSMTree.Internal.WriteBufferBlobs
import           System.FS.API
import           System.FS.Sim.Error hiding (genErrors)
import qualified System.FS.Sim.MockFS as MockFS
import           Test.Tasty
import           Test.Tasty.QuickCheck as QC
import           Test.Util.FS

tests :: TestTree
tests = testGroup "Test.Database.LSMTree.Internal.WriteBufferBlobs.FS" [
      testProperty "prop_fault_WriteBufferBlobs" prop_fault_WriteBufferBlobs
    ]

-- Test that opening and releasing a 'WriteBufferBlobs' properly cleans handles
-- and files in the presence of disk faults. Also test that we can write then
-- read blobs correctly in the presence of disk faults.
--
-- By testing 'open', we also test 'new'.
prop_fault_WriteBufferBlobs ::
     Bool -- ^ create the file or not
  -> AllowExisting
  -> NoCleanupErrors
  -> Errors
  -> NoCleanupErrors
  -> SerialisedBlob
  -> SerialisedBlob
  -> Property
prop_fault_WriteBufferBlobs doCreateFile ae
  (NoCleanupErrors openErrors)
  errs
  (NoCleanupErrors releaseErrors)
  b1 b2 =
    ioProperty $
    withSimErrorHasFS propPost MockFS.empty emptyErrors $ \hfs fsVar errsVar -> do
      when doCreateFile $
        withFile hfs path (WriteMode MustBeNew) $ \_ -> pure ()
      eith <- try @_ @FsError $
        bracket (acquire hfs errsVar) (release errsVar) $ \wbb -> do
          fs' <- atomically $ readTMVar fsVar
          let prop = propNumOpenHandles 1 fs' .&&. propNumDirEntries root 1 fs'
          props <- blobRoundtrips hfs errsVar wbb
          pure (prop .&&. props)
      pure $ case eith of
        Left{}     -> do
          label "FsError" $ property True
        Right prop ->
          label "Success" $ prop
  where
    root = mkFsPath []
    path = mkFsPath ["wbb"]

    acquire hfs errsVar = withErrors errsVar openErrors $ open hfs path ae

    -- Test that we can roundtrip blobs
    blobRoundtrips hfs errsVar wbb = withErrors errsVar errs $ do
        props <-
          forM [b1, b2] $ \b -> do
            bspan <- addBlob hfs wbb b
            let bref = mkRawBlobRef wbb bspan
            b' <- readRawBlobRef hfs bref
            pure (b === b')
        pure $ conjoin props

    release errsVar wbb = withErrors errsVar releaseErrors $ releaseRef wbb

    propPost fs = propNoOpenHandles fs .&&.
        if doCreateFile then
          case ae of
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
