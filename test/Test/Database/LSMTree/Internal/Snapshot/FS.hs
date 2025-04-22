{-# LANGUAGE OverloadedStrings #-}

-- | Tests for snapshots and their interaction with the file system
module Test.Database.LSMTree.Internal.Snapshot.FS (tests) where

import           Control.Monad.Class.MonadThrow
import           Control.Monad.IOSim (runSimOrThrow)
import           Control.Tracer
import           Data.Bifunctor (Bifunctor (..))
import qualified Data.Vector as V
import           Data.Word
import           Database.LSMTree.Extras (showPowersOf10)
import           Database.LSMTree.Extras.Generators ()
import           Database.LSMTree.Internal.Config
import           Database.LSMTree.Internal.Config.Override
                     (OverrideDiskCachePolicy (..))
import           Database.LSMTree.Internal.Entry
import           Database.LSMTree.Internal.Paths
import           Database.LSMTree.Internal.Serialise
import           Database.LSMTree.Internal.Snapshot
import           Database.LSMTree.Internal.Snapshot.Codec
import           Database.LSMTree.Internal.Unsafe
import qualified System.FS.API as FS
import           System.FS.API
import           System.FS.Sim.Error hiding (genErrors)
import qualified System.FS.Sim.MockFS as MockFS
import           Test.Database.LSMTree.Internal.Snapshot.Codec ()
import           Test.QuickCheck
import           Test.Tasty
import           Test.Tasty.QuickCheck
import           Test.Util.FS
import           Test.Util.QC (Choice)

tests :: TestTree
tests = testGroup "Test.Database.LSMTree.Internal.Snapshot.FS" [
      testProperty "prop_fsRoundtripSnapshotMetaData" $
        prop_fsRoundtripSnapshotMetaData
    , testProperty "prop_fault_fsRoundtripSnapshotMetaData"
        prop_fault_fsRoundtripSnapshotMetaData
    , testProperty "prop_flipSnapshotBit" prop_flipSnapshotBit
    ]

-- | @readFileSnapshotMetaData . writeFileSnapshotMetaData = id@
--
-- NOTE: prop_fault_fsRoundtripSnapshotMetaData with empty errors is equivalent
-- to prop_fsRoundtripSnapshotMetaData. I (Joris) chose to keep the properties
-- separate, so that there are fewer cases to account for (like @allNull@
-- errors) in prop_fault_fsRoundtripSnapshotMetaData
prop_fsRoundtripSnapshotMetaData :: SnapshotMetaData -> Property
prop_fsRoundtripSnapshotMetaData metadata =
    ioProperty $
    withTempIOHasFS "temp" $ \hfs -> do
      writeFileSnapshotMetaData hfs contentPath checksumPath metadata
      snapshotMetaData' <-
        try @_ @FileCorruptedError (readFileSnapshotMetaData hfs contentPath checksumPath)
      pure $ case snapshotMetaData' of
        Left e          -> counterexample (show e) False
        Right metadata' -> metadata === metadata'
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
        try @_ @FsError $
          withErrors errsVar (writeErrors testErrs) $
            writeFileSnapshotMetaData hfs metadataPath checksumPath metadata

      readResult <-
        try @_ @SomeException $
          withErrors errsVar (readErrors testErrs) $
            readFileSnapshotMetaData hfs metadataPath checksumPath

      let
        -- Regardless of whether the write part failed with an exception, if
        -- metadata was returned from read+deserialise it should be exactly
        -- equal to the metadata that was written to disk.
        prop =
          case readResult of
            Right metadata' -> metadata === metadata'
            _               -> property True

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
      -> Either SomeException SnapshotMetaData
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
         Either SomeException SnapshotMetaData
      -> String
    mkLabelReadResult = \case
        Left e
          | Just FsError{} <- fromException e ->
              "Left FSError"
          | Just ErrFileFormatInvalid{} <- fromException e ->
              "Left ErrFileFormatInvalid"
          | Just ErrFileChecksumMismatch{} <- fromException e ->
              "Left ErrFileChecksumMismatch"
          | otherwise ->
              error ("impossible: " <> show e)
        Right (_ :: SnapshotMetaData) ->
          "Right SnapshotMetaData"

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

{-------------------------------------------------------------------------------
  Snapshot corruption
-------------------------------------------------------------------------------}

-- TODO: an alternative to generating a Choice a priori is to run the monadic
-- code in @PropertyM (IOSim s)@, and then we can do quantification inside the
-- monadic property using @pick@. This complicates matters, however, because
-- functions like @withSimHasBlockIO@ and @withTable@ would have to run in
-- @PropertyM (IOSim s)@ as well. It's not clear whether the refactoring is
-- worth it.
prop_flipSnapshotBit ::
     Positive (Small Int)
  -> V.Vector (Word64, Entry Word64 Word64)
  -> Choice -- ^ Used to pick which file/bit to corrupt.
  -> Property
prop_flipSnapshotBit (Positive (Small bufferSize)) es pickFileBit =
    runSimOrThrow $
    withSimHasBlockIO propNoOpenHandles MockFS.empty $ \hfs hbio _fsVar ->
    withSession nullTracer hfs hbio root $ \s ->
    withTable s conf $ \t -> do
      -- Create a table, populate it, and create a snapshot
      updates resolve es' t
      createSnap t

      -- Corrupt the snapshot
      flipRandomBitInRandomFile hfs pickFileBit (getNamedSnapshotDir namedSnapDir) >>= \case
        Nothing -> pure $ property False
        Just (path, j) -> do
          -- Some info for the test output
          let tabCorruptedFile = tabulate "Corrupted file" [show path]
              counterCorruptedFile = counterexample ("Corrupted file: " ++ show path)
              tabFlippedBit = tabulate "Flipped bit" [showPowersOf10 j]
              counterFlippedBit = counterexample ("Flipped bit: " ++ show j)

          t' <- try @_ @SomeException $ bracket (openSnap s) close $ \_ -> pure ()
          pure $
            tabCorruptedFile $ counterCorruptedFile $ tabFlippedBit $ counterFlippedBit $
            case t' of
              -- If we find an error, we detected corruption. Success!
              Left e ->
                tabulate
                  "Result"
                  ["Corruption detected: " <> getConstructorName e]
                  True
              -- The corruption was not detected. Failure!
              Right _ -> property False
  where
    root = FS.mkFsPath []
    namedSnapDir = namedSnapshotDir (SessionRoot root) snapName

    conf = defaultTableConfig {
        confWriteBufferAlloc  = AllocNumEntries bufferSize
      , confFencePointerIndex = CompactIndex
      }
    es' = fmap (bimap serialiseKey (bimap serialiseValue serialiseBlob)) es

    resolve (SerialisedValue x) (SerialisedValue y) =
        SerialisedValue (x <> y)

    snapName = toSnapshotName "snap"
    snapLabel = SnapshotLabel "label"

    createSnap t =
        saveSnapshot snapName snapLabel SnapFullTable t

    openSnap s =
        openTableFromSnapshot NoOverrideDiskCachePolicy s snapName snapLabel SnapFullTable resolve

    getConstructorName e = takeWhile (/= ' ') (show e)
