{-# LANGUAGE OverloadedStrings #-}

module Test.Database.LSMTree.Internal.Run (
    -- * Main test tree
    tests,
) where

import           Control.ActionRegistry (withActionRegistry)
import           Control.RefCount
import           Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Short as SBS
import           Data.Coerce (coerce)
import qualified Data.Map.Strict as Map
import           Data.Maybe (fromJust)
import qualified Data.Primitive.ByteArray as BA
import           Database.LSMTree.Extras.Generators (KeyForIndexCompact (..))
import           Database.LSMTree.Extras.RunData
import           Database.LSMTree.Internal.BlobRef (BlobSpan (..))
import qualified Database.LSMTree.Internal.CRC32C as CRC
import           Database.LSMTree.Internal.Entry
import           Database.LSMTree.Internal.Paths (RunFsPaths (..),
                     WriteBufferFsPaths (..))
import qualified Database.LSMTree.Internal.Paths as Paths
import qualified Database.LSMTree.Internal.RawBytes as RB
import           Database.LSMTree.Internal.RawPage
import           Database.LSMTree.Internal.Run as Run
import           Database.LSMTree.Internal.RunNumber
import           Database.LSMTree.Internal.Serialise
import           Database.LSMTree.Internal.Snapshot
import qualified Database.LSMTree.Internal.WriteBufferBlobs as WBB
import           Database.LSMTree.Internal.WriteBufferReader (readWriteBuffer)
import qualified FormatPage as Proto
import           System.FilePath
import qualified System.FS.API as FS
import qualified System.FS.API.Lazy as FSL
import qualified System.FS.BlockIO.API as FS
import qualified System.FS.BlockIO.IO as FS
import qualified System.FS.IO as FsIO
import qualified System.FS.Sim.MockFS as MockFS
import qualified System.IO.Temp as Temp
import           Test.Database.LSMTree.Internal.RunReader (readKOps)
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.HUnit (assertEqual, testCase, (@=?), (@?))
import           Test.Tasty.QuickCheck
import           Test.Util.FS (propNoOpenHandles, withSimHasBlockIO)


tests :: TestTree
tests = testGroup "Database.LSMTree.Internal.Run"
    [ testGroup "Write buffer to disk"
      [ testCase "Single insert (small)" $ do
          withSessionDir $ \sessionRoot ->
            testSingleInsert sessionRoot
              (mkKey "test-key")
              (mkVal "test-value")
              Nothing
      , testCase "Single insert (blob)" $ do
          withSessionDir $ \sessionRoot -> do
            testSingleInsert sessionRoot
              (mkKey "test-key")
              (mkVal "test-value")
              (Just (mkBlob "test-blob"))
      , testCase "Single insert (larger-than-page)" $ do
          withSessionDir $ \sessionRoot -> do
            testSingleInsert sessionRoot
              (mkKey "test-key")
              (mkVal ("test-value-" <> BS.concat (replicate 500 "0123456789")))
              Nothing
      , testProperty "prop_WriteAndOpen" $ \wb ->
          ioProperty $ withSimHasBlockIO propNoOpenHandles MockFS.empty $ \hfs hbio _ ->
            prop_WriteAndOpen hfs hbio wb
      , testProperty "prop_WriteNumEntries" $ \wb ->
          ioProperty $ withSimHasBlockIO propNoOpenHandles MockFS.empty $ \hfs hbio _ ->
            prop_WriteNumEntries hfs hbio wb
      , testProperty "prop_WriteAndOpenWriteBuffer" $ \wb ->
          ioProperty $ withSimHasBlockIO propNoOpenHandles MockFS.empty $ \hfs hbio _ ->
            prop_WriteAndOpenWriteBuffer hfs hbio wb
      , testProperty "prop_WriteRunEqWriteWriteBuffer" $ \wb ->
          ioProperty $ withSimHasBlockIO propNoOpenHandles MockFS.empty $ \hfs hbio _ ->
            prop_WriteRunEqWriteWriteBuffer hfs hbio wb
      ]
    ]
  where
    withSessionDir = Temp.withSystemTempDirectory "session-run"

    mkKey = SerialisedKey . RB.fromByteString
    mkVal = SerialisedValue . RB.fromByteString
    mkBlob = SerialisedBlob . RB.fromByteString

-- | Runs in IO, with a real file system.
testSingleInsert :: FilePath -> SerialisedKey -> SerialisedValue -> Maybe SerialisedBlob -> IO ()
testSingleInsert sessionRoot key val mblob =
    let fs = FsIO.ioHasFS (FS.MountPoint sessionRoot) in
    FS.withIOHasBlockIO fs FS.defaultIOCtxParams $ \hbio -> do
    -- flush write buffer
    let e = case mblob of Nothing -> Insert val; Just blob -> InsertWithBlob val blob
        wb = Map.singleton key e
    withRun fs hbio (simplePath 42) (RunData wb) $ \_ -> do
      -- check all files have been written
      let activeDir = sessionRoot
      bsKOps <- BS.readFile (activeDir </> "42.keyops")
      bsBlobs <- BS.readFile (activeDir </> "42.blobs")
      bsFilter <- BS.readFile (activeDir </> "42.filter")
      bsIndex <- BS.readFile (activeDir </> "42.index")
      not (BS.null bsKOps) @? "k/ops file is empty"
      null mblob @=? BS.null bsBlobs  -- blob file might be empty
      not (BS.null bsFilter) @? "filter file is empty"
      not (BS.null bsIndex) @? "index file is empty"
      -- checksums
      checksums <- CRC.readChecksumsFile fs (FS.mkFsPath ["42.checksums"])
      Map.lookup (CRC.ChecksumsFileName "keyops") checksums
        @=? Just (CRC.updateCRC32C bsKOps CRC.initialCRC32C)
      Map.lookup (CRC.ChecksumsFileName "blobs") checksums
        @=? Just (CRC.updateCRC32C bsBlobs CRC.initialCRC32C)
      Map.lookup (CRC.ChecksumsFileName "filter") checksums
        @=? Just (CRC.updateCRC32C bsFilter CRC.initialCRC32C)
      Map.lookup (CRC.ChecksumsFileName "index") checksums
        @=? Just (CRC.updateCRC32C bsIndex CRC.initialCRC32C)
      -- check page
      let page = rawPageFromByteString bsKOps 0
      1 @=? rawPageNumKeys page

      let pagesize :: Int
          pagesize = fromJust $
            Proto.pageSizeBytes <$> Proto.calcPageSize Proto.DiskPage4k
                [ ( Proto.Key (coerce RB.toByteString key)
                  , Proto.Insert (Proto.Value (coerce RB.toByteString val))
                                  Nothing
                  ) ]
          suffix, prefix :: Int
          suffix = max 0 (pagesize - 4096)
          prefix = coerce RB.size val - suffix
      let expectedEntry = case mblob of
            Nothing -> Insert         (coerce RB.take prefix val)
            Just b  -> InsertWithBlob (coerce RB.take prefix val) b
      let expectedResult
            | suffix > 0 = LookupEntryOverflow expectedEntry (fromIntegral suffix)
            | otherwise  = LookupEntry         expectedEntry

      let actualEntry = fmap (readBlobFromBS bsBlobs) <$> rawPageLookup page key

      -- the lookup result is as expected, possibly with a prefix of the value
      expectedResult @=? actualEntry

      -- the value is as expected, including any overflow suffix
      let valPrefix = coerce RB.take prefix val
          valSuffix = (RB.fromByteString . BS.take suffix . BS.drop 4096) bsKOps
      val @=? SerialisedValue (valPrefix <> valSuffix)

      -- blob sanity checks
      length mblob @=? fromIntegral (rawPageNumBlobs page)

rawPageFromByteString :: ByteString -> Int -> RawPage
rawPageFromByteString bs off =
    makeRawPage (toBA bs) off
  where
    -- ensure that the resulting RawPage has no trailing data that could
    -- accidentally be read.
    toBA = (\(SBS.SBS ba) -> BA.ByteArray ba) . SBS.toShort . BS.take (off+4096)

readBlobFromBS :: ByteString -> BlobSpan -> SerialisedBlob
readBlobFromBS bs (BlobSpan off sz) =
    serialiseBlob $ BS.take (fromIntegral sz) (BS.drop (fromIntegral off) bs)

{-------------------------------------------------------------------------------
  Properties
-------------------------------------------------------------------------------}

-- | Flushing a write buffer results in a run with the desired elements.
--
-- To assert that the run really contains the right entries, there are
-- additional tests in "Test.Database.LSMTree.Internal.RunReader".
--
prop_WriteNumEntries ::
     FS.HasFS IO h
  -> FS.HasBlockIO IO h
  -> RunData KeyForIndexCompact SerialisedValue SerialisedBlob
  -> IO Property
prop_WriteNumEntries fs hbio wb@(RunData m) =
    withRun fs hbio (simplePath 42) wb' $ \run -> do
      let !runSize = Run.size run

      return . labelRunData wb' $
        NumEntries (Map.size m) === runSize
  where
    wb' = serialiseRunData wb

-- | Loading a run (written out from a write buffer) from disk gives the same
-- in-memory representation as the original run.
--
-- @openFromDisk . flush === flush@
prop_WriteAndOpen ::
     FS.HasFS IO h
  -> FS.HasBlockIO IO h
  -> RunData KeyForIndexCompact SerialisedValue SerialisedBlob
  -> IO Property
prop_WriteAndOpen fs hbio wb =
    withRun fs hbio (simplePath 1337) (serialiseRunData wb) $ \written ->
    withActionRegistry $ \reg -> do
      let paths = Run.runFsPaths written
          paths' = paths { runNumber = RunNumber 17}
      hardLinkRunFiles reg fs hbio paths paths'
      loaded <- openFromDisk fs hbio CacheRunData (simplePath 17)

      Run.size written @=? Run.size loaded
      withRef written $ \written' ->
        withRef loaded $ \loaded' -> do
          runFilter written' @=? runFilter loaded'
          runIndex  written' @=? runIndex  loaded'

      writtenKOps <- readKOps Nothing written
      loadedKOps  <- readKOps Nothing loaded

      assertEqual "k/ops" writtenKOps loadedKOps

      -- make sure runs get closed again
      releaseRef loaded

      -- TODO: return a proper Property instead of using assertEqual etc.
      return (property True)

-- | Writing and loading a 'WriteBuffer' gives the same in-memory
--   representation as the original write buffer.
prop_WriteAndOpenWriteBuffer ::
     FS.HasFS IO h
  -> FS.HasBlockIO IO h
  -> RunData KeyForIndexCompact SerialisedValue SerialisedBlob
  -> IO Property
prop_WriteAndOpenWriteBuffer hfs hbio rd = do
  -- Serialise run data as write buffer:
  let srd = serialiseRunData rd
  let inPaths = WrapRunFsPaths $ simplePath 1111
  let resolve (SerialisedValue x) (SerialisedValue y) = SerialisedValue (x <> y)
  withRunDataAsWriteBuffer hfs resolve inPaths srd $ \wb wbb -> do
    -- Write write buffer to disk:
    let wbPaths = WrapRunFsPaths $ simplePath 1312
    withSerialisedWriteBuffer hfs hbio wbPaths wb wbb $ do
      -- Read write buffer from disk:
      let kOpsPath = Paths.ForKOps (Paths.writeBufferKOpsPath wbPaths)
      withRef wbb $ \wbb' -> do
        wb' <- readWriteBuffer resolve hfs hbio kOpsPath (WBB.blobFile wbb')
        assertEqual "k/ops" wb wb'
  -- TODO: return a proper Property instead of using assertEqual etc.
  return (property True)

-- | Writing run data to the disk via 'writeWriteBuffer' gives the same key/ops
--   and blob files as when written out as a run.
prop_WriteRunEqWriteWriteBuffer ::
     FS.HasFS IO h
  -> FS.HasBlockIO IO h
  -> RunData KeyForIndexCompact SerialisedValue SerialisedBlob
  -> IO Property
prop_WriteRunEqWriteWriteBuffer hfs hbio rd = do
  -- Serialise run data as run:
  let srd = serialiseRunData rd
  let rdPaths = simplePath 1337
  let rdKOpsFile = Paths.runKOpsPath rdPaths
  let rdBlobFile = Paths.runBlobPath rdPaths
  withRun hfs hbio rdPaths srd $ \_run -> do
    -- Serialise run data as write buffer:
    let f (SerialisedValue x) (SerialisedValue y) = SerialisedValue (x <> y)
    let inPaths = WrapRunFsPaths $ simplePath 1111
    withRunDataAsWriteBuffer hfs f inPaths srd $ \wb wbb -> do
      let wbPaths = WrapRunFsPaths $ simplePath 1312
      let wbKOpsPath = Paths.writeBufferKOpsPath wbPaths
      let wbBlobPath = Paths.writeBufferBlobPath wbPaths
      withSerialisedWriteBuffer hfs hbio wbPaths wb wbb $ do
        -- Compare KOps:
        FS.withFile hfs rdKOpsFile FS.ReadMode $ \rdKOpsHandle ->
          FS.withFile hfs wbKOpsPath FS.ReadMode $ \wbKOpsHandle -> do
            rdKOps <- FSL.hGetAll hfs rdKOpsHandle
            wbKOps <- FSL.hGetAll hfs wbKOpsHandle
            assertEqual "writtenRunKOps/writtenWriteBufferKOps" rdKOps wbKOps
        -- Compare Blob:
        FS.withFile hfs rdBlobFile FS.ReadMode $ \rdBlobHandle ->
          FS.withFile hfs wbBlobPath FS.ReadMode $ \wbBlobHandle -> do
            rdBlob <- FSL.hGetAll hfs rdBlobHandle
            wbBlob <- FSL.hGetAll hfs wbBlobHandle
            assertEqual "writtenRunBlob/writtenWriteBufferBlob" rdBlob wbBlob
  -- TODO: return a proper Property instead of using assertEqual etc.
  return (property True)
