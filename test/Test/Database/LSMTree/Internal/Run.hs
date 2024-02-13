{-# LANGUAGE DerivingStrategies         #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}

module Test.Database.LSMTree.Internal.Run (
    -- * Main test tree
    tests,
) where

import           Data.Bifoldable (bifoldMap)
import           Data.Bifunctor (bimap)
import           Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Short as SBS
import qualified Data.Primitive.ByteArray as BA
import qualified Data.Vector.Primitive as V
import           System.FilePath
import qualified System.FS.API as FS
import qualified System.FS.API.Lazy as FS
import qualified System.FS.IO as FsIO
import qualified System.FS.Sim.Error as FsSim
import qualified System.FS.Sim.MockFS as FsSim
import           System.IO.Temp
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.HUnit (testCase, (@=?))
import           Test.Tasty.QuickCheck

import           Database.LSMTree.Generators ()
import           Database.LSMTree.Internal.BlobRef (BlobSpan (..))
import           Database.LSMTree.Internal.Entry
import qualified Database.LSMTree.Internal.Normal as N
import           Database.LSMTree.Internal.RawPage
import           Database.LSMTree.Internal.Run
import           Database.LSMTree.Internal.Serialise
import           Database.LSMTree.Internal.WriteBuffer (WriteBuffer)
import qualified Database.LSMTree.Internal.WriteBuffer as WB
import           Database.LSMTree.Util (showPowersOf10)

type Key  = ByteString
type Blob = ByteString

newtype Val  = Val ByteString
  deriving newtype SerialiseValue

tests :: TestTree
tests = testGroup "Database.LSMTree.Internal.Run"
    [ testGroup "Write buffer to disk"
      [ testCase "Single insert (small)" $ do
          withSessionDir $ \sessionRoot ->
            testSingleInsert sessionRoot
              "test-key"
              (Val "test-value")
              Nothing
      , testCase "Single insert (blob)" $ do
          withSessionDir $ \sessionRoot -> do
            testSingleInsert sessionRoot
              "test-key"
              (Val "test-value")
              (Just "test-blob")
      , testCase "Single insert (larger-than-page)" $ do
          withSessionDir $ \sessionRoot -> do
            testSingleInsert sessionRoot
              "test-key"
              (Val ("test-value-" <> BS.concat (replicate 500 "0123456789")))
              Nothing
      , testProperty "Written pages can be read again" $ \wb ->
            WB.numEntries wb > NumEntries 0 ==> prop_WriteAndRead wb
      ]
    ]
  where
    withSessionDir = withTempDirectory "" "session"

-- | Runs in IO, with a real file system.
testSingleInsert :: FilePath -> Key -> Val -> Maybe Blob -> IO ()
testSingleInsert sessionRoot key val mblob = do
    let fs = FsIO.ioHasFS (FS.MountPoint sessionRoot)
    -- flush write buffer
    let wb = WB.addEntryNormal key (N.Insert val mblob) WB.empty
    _ <- fromWriteBuffer fs (RunFsPaths 42) wb
    -- check all files have been written
    bsFilter <- BS.readFile (sessionRoot </> "active" </> "42.filter")
    mempty @=? bsFilter  -- TODO: empty for now, should be written later
    bsIndex <- BS.readFile (sessionRoot </> "active" </> "42.index")
    mempty @=? bsIndex   -- TODO: empty for now, should be written later
    bsBlobs <- BS.readFile (sessionRoot </> "active" </> "42.blobs")
    bsKops <- BS.readFile (sessionRoot </> "active" </> "42.keyops")
    let page = rawPageFromByteString bsKops 0
    -- check page
    1 @=? rawPageNumKeys page
    let SerialisedKey' key' = serialiseKey key
    let SerialisedValue' val' = serialiseValue val
    case mblob of
      Nothing -> do
        0 @=? rawPageNumBlobs page
        mempty @=? bsBlobs
        Just (Insert val') @=? rawPageEntry page key'
      Just b -> do
        1 @=? rawPageNumBlobs page
        let entry = fmap (readBlob bsBlobs) <$> rawPageEntry page key'
        Just (InsertWithBlob val' (serialiseBlob b)) @=? entry

-- | Runs in IO, but using a mock file system.
--
-- TODO: Also test file system errors.
prop_WriteAndRead :: WriteBuffer Key Val Blob -> Property
prop_WriteAndRead wb = ioProperty $ do
    fs <- FsSim.mkSimErrorHasFS' FsSim.empty FsSim.emptyErrors
    -- flush write buffer
    let fsPaths = RunFsPaths 42
    _ <- fromWriteBuffer fs fsPaths wb
    -- read pages
    bsBlobs <- getFile fs (runBlobPath fsPaths)
    bsKops <- getFile fs (runKOpsPath fsPaths)
    let pages = rawPageFromByteString bsKops <$> [0, 4096 .. (BS.length bsKops - 1)]
    -- check pages
    return $ label ("Number of pages: " <> showPowersOf10 (length pages)) $ do
      let vals = concatMap (bifoldMap pure mempty . snd) (WB.content wb)
      tabulate "Value size" (map (showPowersOf10 . sizeofValue) vals) $
        pagesContainEntries bsBlobs pages (WB.content wb)
  where
    getFile fs path =
      FS.withFile fs path FS.ReadMode (fmap BS.toStrict . FS.hGetAll fs)

pagesContainEntries :: ByteString -> [RawPage] ->
                       [(SerialisedKey, Entry SerialisedValue SerialisedBlob)] ->
                       Property
pagesContainEntries _ [] es = counterexample ("k/ops left: " <> show es) (null es)
pagesContainEntries bsBlobs (page : pages) kops = do
    let (kopsHere, kopsRest) = splitAt (fromIntegral (rawPageNumKeys page)) kops

    let pageBytes = fromIntegral (V.last (rawPageValueOffsets page))
    let overflowPages = (pageBytes - 1) `div` 4096

    classify (overflowPages > 0) "larger-than-page value" $
          map (Just . snd) kopsHere === map (readEntry . fst) kopsHere
     .&&. pagesContainEntries bsBlobs (drop overflowPages pages) kopsRest

  where
    readEntry :: SerialisedKey -> Maybe (Entry SerialisedValue SerialisedBlob)
    readEntry =
        fmap (bimap SerialisedValue' (readBlob bsBlobs))
      . rawPageEntry page
      . (\(SerialisedKey' pvec) -> pvec)

rawPageFromByteString :: ByteString -> Int -> RawPage
rawPageFromByteString =
    makeRawPage . (\(SBS.SBS ba) -> BA.ByteArray ba) . SBS.toShort

readBlob :: ByteString -> BlobSpan -> SerialisedBlob
readBlob bs (BlobSpan offset size) =
    serialiseBlob $ BS.take (fromIntegral size) (BS.drop (fromIntegral offset) bs)

instance Arbitrary Val where
  arbitrary = fmap Val $
    frequency [ (20, arbitrary)
              , (1,  scale (*100) arbitrary)  -- trigger larger-than-page
              ]
  shrink (Val bs) = fmap Val (shrink bs)
