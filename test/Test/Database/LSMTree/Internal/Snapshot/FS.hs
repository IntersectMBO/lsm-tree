{-# LANGUAGE MagicHash #-}

{-# OPTIONS_GHC -Wno-orphans #-}

-- | Tests for snapshots and their interaction with the file system
--
-- TODO: add fault injection tests using fs-sim
module Test.Database.LSMTree.Internal.Snapshot.FS (tests) where

import           Codec.CBOR.Read (DeserialiseFailure)
import           Control.Exception
import           Database.LSMTree.Internal.CRC32C
import           Database.LSMTree.Internal.Run
import           Database.LSMTree.Internal.Snapshot
import           Database.LSMTree.Internal.Snapshot.Codec
import           GHC.Exts
import qualified System.FS.API as FS
import           System.FS.API
import           System.FS.Sim.Error
import qualified System.FS.Sim.MockFS as MockFS
import           System.FS.Sim.Stream as Stream
import           Test.Database.LSMTree.Internal.Snapshot.Codec ()
import           Test.Tasty
import           Test.Tasty.QuickCheck as QC
import           Test.Util.FS

tests :: TestTree
tests = testGroup "Test.Database.LSMTree.Internal.Snapshot.FS" [
      testProperty "prop_fsRoundtripSnapshotMetaData"
        prop_fsRoundtripSnapshotMetaData

    -- Fault injection
    , testProperty "prop_injectFault" $ prop_injectFault
    ]

-- | @readFileSnapshotMetaData . writeFileSnapshotMetaData = id@
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
    contentPath  = FS.mkFsPath ["content"]
    checksumPath = FS.mkFsPath ["checksum"]

{-------------------------------------------------------------------------------
  Fault injection
-------------------------------------------------------------------------------}

data PartialWrites = PartialWrites | NoPartialWrites
  deriving stock Show

instance Arbitrary PartialWrites where
  arbitrary = elements [PartialWrites, NoPartialWrites]
  shrink _ = []

data TestErrors = TestErrors PartialWrites Errors
  deriving stock Show

instance Arbitrary TestErrors where
  arbitrary = do
      pw <- arbitrary
      errs <-
        case pw of
          PartialWrites -> arbitrary
          NoPartialWrites -> genErrors False True
      pure (TestErrors pw errs)
  shrink (TestErrors pw errs) = TestErrors pw <$> shrink errs



prop_injectFault :: TestErrors -> SnapshotMetaData -> Property
prop_injectFault (TestErrors pw errs) metadata =
    ioProperty $
    withSimErrorHasFS (\fs -> propNoOpenHandles fs .&&. propIsEmpty fs) MockFS.empty errs $ \hfs _fsVar errVar -> do
      e1 <- try @FsError $
        writeFileSnapshotMetaData hfs metadataPath checksumPath metadata

      e2 <- try @SomeException $
        withErrors errVar emptyErrors $
          readFileSnapshotMetaData hfs metadataPath checksumPath

      case e1 of
        Right{} -> withErrors errVar emptyErrors (removeFile hfs metadataPath
                                               >> removeFile hfs checksumPath)
        Left{} -> pure ()

      pure $ label (show pw) $ label (mkLabel e1 e2) $ case (e1, e2) of
        (Right (), Right metaDataE) ->
          case metaDataE of
            Left _deserialiseFailure ->
              counterexample
                "Found a deserialisation failure, but there were\
                \no injected faults, so there could not have been\
                \non-silent corruption!"
                False
            Right metadata'         ->
              metadata === metadata'
        (Left e, Right metaDataE) ->
          case metaDataE of
            Left deserialiseFailure ->
              tabulate "DeserialiseFailure" [show deserialiseFailure] $
              True
            Right metadata'         ->
              counterexample
                ("Found metadata, but there was a write error: " <> show e)
                (case pw of
                  PartialWrites   -> metadata === metadata'
                  NoPartialWrites -> metadata =/= metadata'
                )
        (_, _) -> property True

  where
    metadataPath = mkFsPath ["metadata"]
    checksumPath = mkFsPath ["checksum"]

    mkLabel e1 e2 = "(" <> mkLabel1 e1 <> ", " <> mkLabel2 e2 <> ")"

    mkLabel1 e1 = case e1 of
        Left FsError{} -> "Left FSError"
        Right ()       -> "Right ()"

    mkLabel2 e2 = case e2 of
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

