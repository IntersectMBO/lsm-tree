module Test.Database.LSMTree.Internal.WriteBufferReader.FS (tests) where


import           Control.Concurrent.Class.MonadSTM.Strict (MonadSTM (..))
import           Control.Concurrent.Class.MonadSTM.Strict.TMVar
import           Control.Monad.Class.MonadThrow
import           Control.RefCount
import           Database.LSMTree.Extras.Generators ()
import           Database.LSMTree.Extras.RunData (RunData,
                     withRunDataAsWriteBuffer, withSerialisedWriteBuffer)
import           Database.LSMTree.Internal.Paths (ForKOps (ForKOps),
                     WriteBufferFsPaths (WriteBufferFsPaths),
                     writeBufferKOpsPath)
import           Database.LSMTree.Internal.RunNumber (RunNumber (RunNumber))
import           Database.LSMTree.Internal.Serialise (SerialisedBlob,
                     SerialisedKey, SerialisedValue (..))
import qualified Database.LSMTree.Internal.WriteBufferBlobs as WBB
import           Database.LSMTree.Internal.WriteBufferReader
import           System.FS.API
import           System.FS.Sim.Error hiding (genErrors)
import qualified System.FS.Sim.MockFS as MockFS
import qualified System.FS.Sim.Stream as Stream
import           Test.Tasty
import           Test.Tasty.QuickCheck as QC
import           Test.Util.FS as FS

tests :: TestTree
tests = testGroup "Test.Database.LSMTree.Internal.WriteBufferReader.FS" [
      testProperty "prop_fault_WriteBufferReader" prop_fault_WriteBufferReader
    ]

-- | Test that 'writeWriteBuffer' roundtrips with 'readWriteBuffer', and test
-- that the presence of disk faults for the latter does not leak file handles
-- and files.
prop_fault_WriteBufferReader ::
     NoCleanupErrors
  -> RunData SerialisedKey SerialisedValue SerialisedBlob
  -> Property
prop_fault_WriteBufferReader (NoCleanupErrors readErrors) rdata =
    ioProperty $
    withSimErrorHasBlockIO propPost MockFS.empty emptyErrors $ \hfs hbio fsVar errsVar ->
    withRunDataAsWriteBuffer hfs resolve inPath rdata $ \wb wbb ->
    withSerialisedWriteBuffer hfs hbio outPath wb wbb $ do
      fsBefore <- atomically $ readTMVar fsVar
      eith <-
        try @_ @FsError $
          withErrors errsVar readErrors' $
            withRef wbb $ \wbb' -> do
              wb' <- readWriteBuffer resolve hfs hbio outKOpsPath (WBB.blobFile wbb')
              pure (wb === wb')

      fsAfter <- atomically $ readTMVar fsVar
      pure $
        case eith of
          Left{}     -> do
            label "FsError" $ property True
          Right prop ->
            label "Success" $ prop .&&. propEqNumDirEntries root fsBefore fsAfter
  where
    root = mkFsPath []
    -- The run number for the original write buffer. Primarily used to name the
    -- 'WriteBufferBlobs' corresponding to the write buffer.
    inPath = WriteBufferFsPaths root (RunNumber 0)
    -- The run number for the serialised write buffer. Used to name all files
    -- that are the result of serialising the write buffer.
    outPath = WriteBufferFsPaths root (RunNumber 1)
    outKOpsPath = ForKOps (writeBufferKOpsPath outPath)
    resolve (SerialisedValue x) (SerialisedValue y) = SerialisedValue (x <> y)
    propPost fs = propNoOpenHandles fs .&&. propNoDirEntries root fs

    -- TODO: fix, see the TODO on readDiskPage
    readErrors' = readErrors {
          hGetBufSomeE = Stream.filter (not . isFsReachedEOFError) (hGetBufSomeE readErrors)
        }

    isFsReachedEOFError = maybe False (either isFsReachedEOF (const False))
