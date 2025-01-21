{-# LANGUAGE OverloadedStrings #-}

-- | Tests for snapshots and their interaction with the file system
module Test.Database.LSMTree.Internal.Snapshot.FS (tests) where

import           Codec.CBOR.Read (DeserialiseFailure)
import           Control.Monad.Class.MonadThrow
import           Control.Monad.IOSim (runSimOrThrow)
import           Control.Tracer
import           Data.Bifunctor (Bifunctor (..))
import           Data.Maybe (fromJust)
import qualified Data.Set as Set
import qualified Data.Vector as V
import           Data.Word
import           Database.LSMTree.Extras (showPowersOf10)
import           Database.LSMTree.Extras.Generators ()
import           Database.LSMTree.Internal
import           Database.LSMTree.Internal.Config
import           Database.LSMTree.Internal.CRC32C
import           Database.LSMTree.Internal.Entry
import           Database.LSMTree.Internal.Paths
import           Database.LSMTree.Internal.Run
import           Database.LSMTree.Internal.Serialise
import           Database.LSMTree.Internal.Snapshot
import           Database.LSMTree.Internal.Snapshot.Codec
import qualified System.FS.API as FS
import           System.FS.API
import           System.FS.Sim.Error hiding (genErrors)
import qualified System.FS.Sim.MockFS as MockFS
import           Test.Database.LSMTree.Internal.Snapshot.Codec ()
import           Test.QuickCheck
import           Test.QuickCheck.Gen (genDouble)
import           Test.Tasty
import           Test.Tasty.QuickCheck
import           Test.Util.FS

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
            Right (Right metadata') -> metadata === metadata'
            _                       -> property True

      pure $
        -- TODO: there are some scenarios that we never hit, like deserialise
        -- failures. We could tweak the error(stream) generator distribution to        -- hit these cases more often. One neat idea would be to "prime" the
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

{-------------------------------------------------------------------------------
  Snapshot corruption
-------------------------------------------------------------------------------}

-- | A 'Double' in the @[0, 1)@ range.
newtype Double_0_1 = Double_0_1 Double
  deriving stock (Show, Eq)

instance Arbitrary Double_0_1 where
  arbitrary = Double_0_1 <$> genDouble
  shrink (Double_0_1 x) = [Double_0_1 x' | x' <- shrink x, 0 <= x', x' < 1]

prop_flipSnapshotBit ::
     Positive (Small Int)
  -> V.Vector (Word64, Entry Word64 Word64)
  -> Double_0_1
  -> Double_0_1
  -> Property
prop_flipSnapshotBit (Positive (Small bufferSize)) es (Double_0_1 pickFile) (Double_0_1 pickBit) =
    runSimOrThrow $
    withSimHasBlockIO propNoOpenHandles MockFS.empty $ \hfs hbio _fsVar ->
    withSession nullTracer hfs hbio root $ \s ->
    withTable s conf $ \t -> do
      updates resolve es' t
      createSnap t

      files <- listDirectoryFiles hfs (getNamedSnapshotDir namedSnapDir)
      let i = round (fromIntegral (Set.size files - 1) * pickFile)
      let file = Set.elemAt i files
      n <- withFile hfs file ReadMode $ hGetSize hfs
      let j = round (fromIntegral (n * 8 - 1) * pickBit)

      let
        tabCorruptedFile = tabulate "Corrupted file" [show file]
        counterCorruptedFile = counterexample ("Corrupted file: " ++ show file)
        tabFlippedBit = tabulate "Flipped bit" [showPowersOf10 j]
        counterFlippedBit = counterexample ("Flipped bit: " ++ show j)

      let isUncheckedFile =
               file == getNamedSnapshotDir namedSnapDir </> FS.mkFsPath ["0.keyops"]
            || file == getNamedSnapshotDir namedSnapDir </> FS.mkFsPath ["0.blobs"]
            || file == getNamedSnapshotDir namedSnapDir </> FS.mkFsPath ["0.checksums"]

      -- TODO: check forgotten refs

      if isUncheckedFile then -- TODO: remove once write buffer files have checksum verification
        pure discard
      else if n <= 0 then -- file is empty
        pure $ tabulate "Result" ["No corruption applied"] True
      else do -- file is non-empty

        flipFileBit hfs file j

        t' <- try @_ @SomeException $ bracket (openSnap s) close $ \_ -> pure ()

        pure $
          tabCorruptedFile $
          counterCorruptedFile $
          tabFlippedBit $
          counterFlippedBit $
          case t' of
            Left e -> tabulate "Result" ["Corruption detected: " <> takeWhile (/= ' ') (show e)] True
            Right _ -> tabulate "Result" ["No corruption detected"] False
  where
    root = FS.mkFsPath []
    namedSnapDir = namedSnapshotDir (SessionRoot root) snapName

    conf = defaultTableConfig {
        confWriteBufferAlloc = AllocNumEntries (NumEntries bufferSize)
      }
    es' = fmap (bimap serialiseKey (bimap serialiseValue serialiseBlob)) es

    resolve (SerialisedValue x) (SerialisedValue y) =
        SerialisedValue (x <> y)

    snapName = fromJust $ mkSnapshotName "snap"
    snapLabel = SnapshotLabel "label"

    createSnap t =
        createSnapshot snapName snapLabel SnapFullTable t

    openSnap s =
        openSnapshot s snapLabel SnapFullTable configNoOverride snapName resolve
