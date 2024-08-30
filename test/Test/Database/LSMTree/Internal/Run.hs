{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module Test.Database.LSMTree.Internal.Run (
    -- * Main test tree
    tests,
    -- * Utilities
    readKOps,
    isLargeKOp,
) where

import           Data.Bifoldable (bifoldMap, bisum)
import           Data.Bifunctor (bimap)
import           Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Short as SBS
import           Data.Coerce (coerce)
import qualified Data.Map.Strict as Map
import           Data.Maybe (fromJust)
import qualified Data.Primitive.ByteArray as BA
import           System.FilePath
import qualified System.FS.API as FS
import qualified System.FS.BlockIO.API as FS
import qualified System.FS.BlockIO.IO as FS
import qualified System.FS.BlockIO.Sim as FsSim
import qualified System.FS.IO as FsIO
import qualified System.FS.Sim.Error as FsSim
import qualified System.FS.Sim.MockFS as FsSim
import qualified System.IO.Temp as Temp
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.HUnit (assertEqual, testCase, (@=?), (@?))
import           Test.Tasty.QuickCheck

import           Control.RefCount (RefCount (..), readRefCount)
import           Database.LSMTree.Extras (showPowersOf10)
import           Database.LSMTree.Extras.Generators (KeyForIndexCompact (..),
                     TypedWriteBuffer (..))
import           Database.LSMTree.Internal.BlobRef (BlobRef (..), BlobSpan (..))
import qualified Database.LSMTree.Internal.CRC32C as CRC
import           Database.LSMTree.Internal.Entry
import qualified Database.LSMTree.Internal.Normal as N
import           Database.LSMTree.Internal.Paths (RunFsPaths (..))
import qualified Database.LSMTree.Internal.RawBytes as RB
import           Database.LSMTree.Internal.RawPage
import           Database.LSMTree.Internal.Run
import           Database.LSMTree.Internal.RunAcc (RunBloomFilterAlloc (..))
import           Database.LSMTree.Internal.RunNumber
import qualified Database.LSMTree.Internal.RunReader as Reader
import           Database.LSMTree.Internal.Serialise
import qualified Database.LSMTree.Internal.WriteBuffer as WB

import qualified FormatPage as Proto


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
      , testProperty "prop_WriteAndRead" $ \wb ->
          ioPropertyWithMockFS $ \hfs hbio ->
            prop_WriteAndRead hfs hbio wb
      , testProperty "prop_WriteAndOpen" $ \wb ->
          ioPropertyWithMockFS $ \hfs hbio ->
            prop_WriteAndOpen hfs hbio wb
      ]
    ]
  where
    withSessionDir = Temp.withSystemTempDirectory "session-run"

    ioPropertyWithMockFS ::
         Testable p
      => (FS.HasFS IO FsSim.HandleMock -> FS.HasBlockIO IO FsSim.HandleMock -> IO p)
      -> Property
    ioPropertyWithMockFS prop = ioProperty $ do
        (res, mockFS) <-
          FsSim.runSimErrorFS FsSim.empty FsSim.emptyErrors $ \_ fs -> do
            hbio <- FsSim.fromHasFS fs
            prop fs hbio
        return $ res
            .&&. counterexample "open handles"
                   (FsSim.numOpenHandles mockFS === 0)

    mkKey = SerialisedKey . RB.fromByteString
    mkVal = SerialisedValue . RB.fromByteString
    mkBlob = SerialisedBlob . RB.fromByteString

-- | Runs in IO, with a real file system.
testSingleInsert :: FilePath -> SerialisedKey -> SerialisedValue -> Maybe SerialisedBlob -> IO ()
testSingleInsert sessionRoot key val mblob =
    let fs = FsIO.ioHasFS (FS.MountPoint sessionRoot) in
    FS.withIOHasBlockIO fs FS.defaultIOCtxParams $ \hbio -> do
    -- flush write buffer
    let wb = WB.addEntryNormal key (N.Insert val mblob) WB.empty
    run <- fromWriteBuffer fs hbio CacheRunData (RunAllocFixed 10) (RunFsPaths (FS.mkFsPath []) (RunNumber 42)) wb
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

    -- make sure run gets closed again
    removeReference run

rawPageFromByteString :: ByteString -> Int -> RawPage
rawPageFromByteString bs off =
    makeRawPage (toBA bs) off
  where
    -- ensure that the resulting RawPage has no trailing data that could
    -- accidentally be read.
    toBA = (\(SBS.SBS ba) -> BA.ByteArray ba) . SBS.toShort . BS.take (off+4096)

readBlobFromBS :: ByteString -> BlobSpan -> SerialisedBlob
readBlobFromBS bs (BlobSpan offset size) =
    serialiseBlob $ BS.take (fromIntegral size) (BS.drop (fromIntegral offset) bs)

{-------------------------------------------------------------------------------
  Properties
-------------------------------------------------------------------------------}

-- | Creating a run from a write buffer and reading from the run yields the
-- original elements.
--
-- @entries wb === readEntries (flush wb)@
--
-- TODO: @id === readEntries . flush . toWriteBuffer@ ?
prop_WriteAndRead ::
     FS.HasFS IO h
  -> FS.HasBlockIO IO h
  -> TypedWriteBuffer KeyForIndexCompact SerialisedValue SerialisedBlob
  -> IO Property
prop_WriteAndRead fs hbio (TypedWriteBuffer wb) = do
    run <- flush (RunNumber 42) wb
    rhs <- readKOps fs hbio run

    -- make sure run gets closed again
    removeReference run

    return $ stats $
           counterexample "number of elements"
             (WB.numEntries wb === runNumEntries run)
      .&&. kops === rhs
  where
    flush n = fromWriteBuffer fs hbio CacheRunData (RunAllocFixed 10) (RunFsPaths (FS.mkFsPath []) n)

    stats = tabulate "value size" (map (showPowersOf10 . sizeofValue) vals)
          . label (if any isLargeKOp kops then "has large k/op" else "no large k/op")
    kops = WB.toList wb
    vals = concatMap (bifoldMap pure mempty . snd) kops

-- | Loading a run (written out from a write buffer) from disk gives the same
-- in-memory representation as the original run.
--
-- @openFromDisk . flush === flush@
prop_WriteAndOpen ::
     FS.HasFS IO h
  -> FS.HasBlockIO IO h
  -> TypedWriteBuffer KeyForIndexCompact SerialisedValue SerialisedBlob
  -> IO ()
prop_WriteAndOpen fs hbio (TypedWriteBuffer wb) = do
    -- flush write buffer
    let fsPaths = RunFsPaths (FS.mkFsPath []) (RunNumber 1337)
    written <- fromWriteBuffer fs hbio CacheRunData (RunAllocFixed 10) fsPaths wb
    loaded <- openFromDisk fs hbio CacheRunData fsPaths

    (RefCount 1 @=?) =<< readRefCount (runRefCounter written)
    (RefCount 1 @=?) =<< readRefCount (runRefCounter loaded)

    runNumEntries written @=? runNumEntries loaded
    runFilter written @=? runFilter loaded
    runIndex written @=? runIndex loaded

    assertEqual "k/ops file"
      (FS.handlePath (runKOpsFile written))
      (FS.handlePath (runKOpsFile loaded))
    assertEqual "blob file"
      (FS.handlePath (runBlobFile written))
      (FS.handlePath (runBlobFile loaded))

    -- make sure runs get closed again
    removeReference written
    removeReference loaded

{-------------------------------------------------------------------------------
  Utilities
-------------------------------------------------------------------------------}

type SerialisedEntry = Entry SerialisedValue SerialisedBlob
type SerialisedKOp = (SerialisedKey, SerialisedEntry)

-- | Simplification with a few false negatives.
isLargeKOp :: SerialisedKOp -> Bool
isLargeKOp (key, entry) = size > pageSize
  where
    pageSize = 4096
    size = sizeofKey key + bisum (bimap sizeofValue sizeofBlob entry)

readKOps :: FS.HasFS IO h -> FS.HasBlockIO IO h -> Run IO (FS.Handle h) -> IO [SerialisedKOp]
readKOps fs hbio run = do
    reader <- Reader.new fs hbio run
    go reader
  where
    go reader = do
      Reader.next fs hbio reader >>= \case
        Reader.Empty -> return []
        Reader.ReadEntry key e -> do
          e' <- traverse resolveBlob $ Reader.toFullEntry e
          ((key, e') :) <$> go reader

    resolveBlob (BlobRef r s) = readBlob fs r s
