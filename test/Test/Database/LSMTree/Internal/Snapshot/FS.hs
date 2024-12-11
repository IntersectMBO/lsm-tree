{-# OPTIONS_GHC -Wno-orphans #-}

-- | Tests for snapshots and their interaction with the file system
--
-- TODO: add fault injection tests using fs-sim
module Test.Database.LSMTree.Internal.Snapshot.FS (tests) where

import           Control.Exception
import           Database.LSMTree.Internal.Run
import           Database.LSMTree.Internal.Snapshot
import           Database.LSMTree.Internal.Snapshot.Codec
import qualified System.FS.API as FS
import           System.FS.API
import           System.FS.Sim.Error
import           System.FS.Sim.Stream
import           Test.Database.LSMTree.Internal.Snapshot.Codec ()
import           Test.Tasty
import           Test.Tasty.QuickCheck
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

newtype SimpleErrors = SimpleErrors (Errors)
  deriving stock Show

instance Arbitrary SimpleErrors where
  arbitrary = do
    es <- genInfinite (genMaybe 4 1 arbitrary)
    pure $ SimpleErrors $ simpleErrors es
  shrink (SimpleErrors errs) = SimpleErrors <$> shrink errs

instance Arbitrary FsErrorType where
  arbitrary = pure FsTooManyOpenFiles
  shrink _ = []

prop_injectFault :: SimpleErrors -> SnapshotMetaData -> Property
prop_injectFault (SimpleErrors errs) metadata =
    ioProperty $ do
    withSimErrorHasFS' propNoOpenHandles errs $ \hfs _fsVar errVar -> do
      e1 <- try @FsError $
        writeFileSnapshotMetaData hfs metadataPath checksumPath metadata

      e2 <- try @SomeException $
        withErrors errVar emptyErrors $
          readFileSnapshotMetaData hfs metadataPath checksumPath
      pure $ label (mkLabel e1 e2) $ case (e1, e2) of
        (Right (), Right metaDataE) ->
          case metaDataE of
            Left _deserialiseFailure ->
              counterexample
                "Found a deserialisation failure, but there were\
                \no injected faults, so there could not have been\
                \non-silent corruption!"
                False
            Right metadata'         ->
              metadata =/= metadata'
        (_, _) -> property True

  where
    metadataPath = mkFsPath ["metadata"]
    checksumPath = mkFsPath ["checksum"]

    mkLabel e1 e2 = case (e1, e2) of
      (Left{}, Left{}) -> "A"
      (Left{}, Right (Left{})) -> "B"
      (Left{}, Right (Right{})) -> "C"
      (Right{}, Left e)
        | Just FsError{} <- fromException e -> "D1"
        | Just FileFormatError{} <- fromException e -> "D2"
        | otherwise -> error "aaaa"
      (Right{}, Right(Left{})) -> "E"
      (Right{}, Right(Right{})) -> "F"
