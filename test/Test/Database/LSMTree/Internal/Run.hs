{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}

module Test.Database.LSMTree.Internal.Run (
    -- * Main test tree
    tests,
    -- * Utilities
    readKOps,
    isLargeKOp,
) where

import           Control.Exception (assert)
import           Data.Bifoldable (bifoldMap, bisum)
import           Data.Bifunctor (bimap, first)
import           Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Short as SBS
import           Data.Coerce (coerce)
import           Data.IORef (readIORef)
import qualified Data.Map.Strict as Map
import           Data.Maybe (fromJust)
import qualified Data.Primitive.ByteArray as BA
import           System.FilePath
import qualified System.FS.API as FS
import qualified System.FS.IO as FsIO
import qualified System.FS.Sim.Error as FsSim
import qualified System.FS.Sim.MockFS as FsSim
import qualified System.IO.Temp as Temp
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.HUnit (assertEqual, testCase, (@=?), (@?))
import           Test.Tasty.QuickCheck

import           Database.LSMTree.Extras (showPowersOf10)
import           Database.LSMTree.Extras.Generators (KeyForIndexCompact (..))
import           Database.LSMTree.Internal.BitMath
import           Database.LSMTree.Internal.BlobRef (BlobRef (..), BlobSpan (..))
import qualified Database.LSMTree.Internal.CRC32C as CRC
import           Database.LSMTree.Internal.Entry
import qualified Database.LSMTree.Internal.Normal as N
import qualified Database.LSMTree.Internal.RawBytes as RB
import           Database.LSMTree.Internal.RawOverflowPage
                     (rawOverflowPageRawBytes)
import           Database.LSMTree.Internal.RawPage
import           Database.LSMTree.Internal.Run
import qualified Database.LSMTree.Internal.RunReader as Reader
import           Database.LSMTree.Internal.Serialise
import           Database.LSMTree.Internal.WriteBuffer (WriteBuffer)
import qualified Database.LSMTree.Internal.WriteBuffer as WB

import qualified FormatPage as Proto

import           Test.Database.LSMTree.Internal.IndexCompact ()

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
          ioPropertyWithRealFS $ \fs bfs ->
            prop_WriteAndRead fs bfs wb
      , testProperty "prop_WriteAndOpen" $ \wb ->
          ioPropertyWithMockFS $ \fs ->
            prop_WriteAndOpen fs wb
      ]
    ]
  where
    withSessionDir = Temp.withSystemTempDirectory "session-run"

    -- Currently doesn't support HasBufFS, so we still need the other version.
    -- TODO: add support once simulation is merged:
    -- https://github.com/input-output-hk/fs-sim/pull/48
    -- TODO: Also test file system errors.
    ioPropertyWithMockFS prop = ioProperty $ do
        (res, mockFS) <-
          FsSim.runSimErrorFS FsSim.empty FsSim.emptyErrors $ \_ fs -> prop fs
        return $ res
            .&&. counterexample "open handles"
                   (FsSim.numOpenHandles mockFS === 0)

    ioPropertyWithRealFS prop =
        ioProperty $ withSessionDir $ \sessionRoot -> do
          let mountPoint = FS.MountPoint sessionRoot
          prop (FsIO.ioHasFS mountPoint) (FsIO.ioHasBufFS mountPoint)

    mkKey = SerialisedKey . RB.fromByteString
    mkVal = SerialisedValue . RB.fromByteString
    mkBlob = SerialisedBlob . RB.fromByteString

-- | Runs in IO, with a real file system.
testSingleInsert :: FilePath -> SerialisedKey -> SerialisedValue -> Maybe SerialisedBlob -> IO ()
testSingleInsert sessionRoot key val mblob = do
    let fs = FsIO.ioHasFS (FS.MountPoint sessionRoot)
    -- flush write buffer
    let wb = WB.addEntryNormal key (N.Insert val mblob) WB.empty
    run <- fromWriteBuffer fs (RunFsPaths 42) wb
    -- check all files have been written
    let activeDir = sessionRoot </> "active"
    bsKOps <- BS.readFile (activeDir </> "42.keyops")
    bsBlobs <- BS.readFile (activeDir </> "42.blobs")
    bsFilter <- BS.readFile (activeDir </> "42.filter")
    bsIndex <- BS.readFile (activeDir </> "42.index")
    not (BS.null bsKOps) @? "k/ops file is empty"
    null mblob @=? BS.null bsBlobs  -- blob file might be empty
    not (BS.null bsFilter) @? "filter file is empty"
    not (BS.null bsIndex) @? "index file is empty"
    -- checksums
    checksums <- CRC.readChecksumsFile fs (FS.mkFsPath ["active", "42.checksums"])
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
           Proto.pageSizeBytes <$> Proto.calcPageSize
             (Proto.PageLogical
               [ ( Proto.Key (coerce RB.toByteString key)
                 , Proto.Insert (Proto.Value (coerce RB.toByteString val))
                 , Nothing ) ])
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
    removeReference fs run

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
     FS.HasFS IO h -> FS.HasBufFS IO h
  -> WriteBuffer KeyForIndexCompact SerialisedValue SerialisedBlob
  -> IO Property
prop_WriteAndRead fs bfs wb = do
    run <- flush 42 wb
    rhs <- readKOps fs bfs run

    -- make sure run gets closed again
    removeReference fs run

    return $ stats $
           counterexample "number of elements"
             (WB.numEntries wb === runNumEntries run)
      .&&. kops === rhs
  where
    flush n = fromWriteBuffer fs (RunFsPaths n)

    stats = tabulate "value size" (map (showPowersOf10 . sizeofValue) vals)
          . label (if any isLargeKOp kops then "has large k/op" else "no large k/op")
    kops = WB.content wb
    vals = concatMap (bifoldMap pure mempty . snd) kops

-- | Loading a run (written out from a write buffer) from disk gives the same
-- in-memory representation as the original run.
--
-- @openFromDisk . flush === flush@
prop_WriteAndOpen ::
     FS.HasFS IO h
  -> WriteBuffer KeyForIndexCompact SerialisedValue SerialisedBlob
  -> IO ()
prop_WriteAndOpen fs wb = do
    -- flush write buffer
    let fsPaths = RunFsPaths 1337
    written <- fromWriteBuffer fs fsPaths wb
    loaded <- openFromDisk fs fsPaths

    (RefCount 1 @=?) =<< readIORef (runRefCount written)
    (RefCount 1 @=?) =<< readIORef (runRefCount loaded)

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
    removeReference fs written
    removeReference fs loaded

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

readKOps :: FS.HasFS IO h -> FS.HasBufFS IO h -> Run (FS.Handle h) -> IO [SerialisedKOp]
readKOps fs bfs run = do
    reader <- Reader.new fs bfs run
    go reader
  where
    go reader = do
      Reader.next fs bfs reader >>= \case
        Reader.Empty -> return []
        Reader.ReadSmallEntry key entry -> do
          entry' <- traverse resolveBlob entry
          ((key, entry') :) <$> go reader
        Reader.ReadLargeEntry key entry _ lenSuffix overflowPages -> do
          entry' <- traverse resolveBlob
                      (first (appendSuffix lenSuffix overflowPages)entry)
          ((key, entry') :) <$> go reader

    resolveBlob (BlobRef r s) = readBlob fs bfs r s

    appendSuffix lenSuffix overflowPages (SerialisedValue prefix) =
      assert (ceilDivPageSize (fromIntegral lenSuffix) == length overflowPages) $
        SerialisedValue $ RB.take (RB.size prefix + fromIntegral lenSuffix) $
          mconcat (prefix : map rawOverflowPageRawBytes overflowPages)
