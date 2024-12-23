-- | Tests for snapshots and their interaction with the file system
module Test.Database.LSMTree.Internal.Snapshot.FS (tests) where

import           Codec.CBOR.Read (DeserialiseFailure)
import           Control.Exception
import           Database.LSMTree.Internal.CRC32C
import           Database.LSMTree.Internal.Run
import           Database.LSMTree.Internal.Snapshot
import           Database.LSMTree.Internal.Snapshot.Codec
import           System.FS.API
import           System.FS.Sim.Error hiding (genErrors)
import qualified System.FS.Sim.MockFS as MockFS
import           Test.Database.LSMTree.Internal.Snapshot.Codec ()
import           Test.Tasty
import           Test.Tasty.QuickCheck as QC
import           Test.Util.FS

tests :: TestTree
tests = testGroup "Test.Database.LSMTree.Internal.Snapshot.FS" [
      testProperty "prop_fsRoundtripSnapshotMetaData" $
        prop_fsRoundtripSnapshotMetaData
    , testProperty "prop_fault_fsRoundtripSnapshotMetaData"
        prop_fault_fsRoundtripSnapshotMetaData
    ]

-- | @readFileSnapshotMetaData . writeFileSnapshotMetaData = id@
--
-- NOTE: prop_fault_fsRoundtripSnapshotMetaData with empty errors is equivalent
-- to prop_fsRoundtripSnapshotMetaData. I (Joris) chose to keep the properties
-- separate, so that there are fewer cases to account for (like @allNull@
-- errors) in prop_fault_fsRoundtripSnapshotMetaData
prop_fsRoundtripSnapshotMetaData :: SnapshotMetaData -> Property
prop_fsRoundtripSnapshotMetaData metaData =
    ioProperty $
    withTempIOHasFS "temp" $ \hfs -> do
      writeFileSnapshotMetaData hfs contentPath checksumPath metaData
      eMetaData' <- readFileSnapshotMetaData hfs contentPath checksumPath
      pure $ case eMetaData' of
        Left e          -> counterexample (show e) False
        Right metaData' -> metaData === metaData'
  where
    contentPath  = mkFsPath ["content"]
    checksumPath = mkFsPath ["checksum"]

-- | @readFileSnapshotMetaData . writeFileSnapshotMetaData = id@, even if
-- exceptions happened.
--
-- NOTE: we can be more precise about the success scenarios for this property,
-- but it complicates the test a lot, so I (Joris) decided not to include it for
-- now. For example, if the read part fails with a deserialise failure, then we
-- *could* check that file corruption took place during the write part.
prop_fault_fsRoundtripSnapshotMetaData ::
     TestErrors
  -> SnapshotMetaData
  -> Property
prop_fault_fsRoundtripSnapshotMetaData testErrs metadata =
    ioProperty $
    withSimErrorHasFS propNoOpenHandles MockFS.empty emptyErrors $ \hfs _fsVar errsVar -> do
      writeResult <-
        try @FsError $
          withErrors errsVar (writeErrors testErrs) $
            writeFileSnapshotMetaData hfs metadataPath checksumPath metadata

      readResult <-
        try @SomeException $
          withErrors errsVar (readErrors testErrs) $
            readFileSnapshotMetaData hfs metadataPath checksumPath

      let
        -- Regardless of whether the write part failed with an exception, if
        -- metadata was returned from read+deserialise it should be exactly
        -- equal to the metadata that was written to disk.
        prop =
          case readResult of
            Right (Right metadata') -> metadata === metadata'
            _                       -> property True

      pure $
        -- TODO: there are some scenarios that we never hit, like deserialise
        -- failures. We could tweak the error(stream) generator distribution to
        -- hit these cases more often. One neat idea would be to "prime" the
        -- generator for errors as follows:
        --
        -- 1. run the property without errors, but count how often each
        --    primitive is used
        -- 2. generate errors based on the counts/distribution we obtained in step 1
        -- 3. run the property with these errors
        tabulate "Write result vs. read result" [mkLabel writeResult readResult] $
        prop
  where
    metadataPath = mkFsPath ["metadata"]
    checksumPath = mkFsPath ["checksum"]

    -- This label is mainly there to print the success/failure of the write
    -- part, the read part, and the deserialisation part. The concrete error
    -- contents are not printed.
    mkLabel ::
         Either FsError ()
      -> Either SomeException (Either DeserialiseFailure SnapshotMetaData)
      -> String
    mkLabel writeResult readResult =
        "("  <> mkLabelWriteResult writeResult <>
        ", " <> mkLabelReadResult readResult <>
        ")"

    mkLabelWriteResult :: Either FsError () -> String
    mkLabelWriteResult = \case
        Left FsError{} -> "Left FSError"
        Right ()       -> "Right ()"

    mkLabelReadResult ::
         Either SomeException (Either DeserialiseFailure SnapshotMetaData)
      -> String
    mkLabelReadResult = \case
        Left e
          | Just FsError{} <- fromException e ->
              "Left FSError"
          | Just FileFormatError{} <- fromException e ->
              "Left FileFormatError"
          | Just ChecksumsFileFormatError{} <- fromException e ->
              "Left ChecksumsFileFormatError"
          | otherwise ->
              error ("impossible: " <> show e)
        Right (Left (_ :: DeserialiseFailure)) ->
          "Right (Left DeserialiseFailure)"
        Right (Right (_ :: SnapshotMetaData)) ->
          "Right (Right SnapshotMetaData)"

data TestErrors = TestErrors {
    writeErrors :: Errors
  , readErrors  :: Errors
  }
  deriving stock Show

instance Arbitrary TestErrors where
  arbitrary = TestErrors <$> arbitrary <*> arbitrary
  shrink TestErrors{writeErrors, readErrors} =
      [ TestErrors writeErrors' readErrors'
      | (writeErrors', readErrors') <- shrink (writeErrors, readErrors)
      ]
