{-# LANGUAGE NamedFieldPuns #-}
{-# OPTIONS_GHC -Wno-orphans #-}

module Test.Database.LSMTree.Internal.CRC32C (
    -- * Main test tree
    tests
  ) where

import           Control.Monad
import qualified Control.Monad.IOSim as IOSim
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Short as SBS
import qualified Data.Digest.CRC32C
import qualified Data.Foldable as Fold
import           Database.LSMTree.Extras (showPowersOf)
import           Database.LSMTree.Internal.CRC32C
import           System.FS.API
import           System.FS.API.Lazy
import           System.FS.API.Strict
import qualified System.FS.Sim.Error as FsSim
import qualified System.FS.Sim.MockFS as FsSim
import qualified System.FS.Sim.Stream as FsSim.Stream
import           System.Posix.Types
import           Test.QuickCheck
import           Test.QuickCheck.Instances ()
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.QuickCheck (testProperty)
import           Test.Util.FS (withTempIOHasFS)


tests :: TestTree
tests = testGroup "Database.LSMTree.Internal.CRC32C" [
      testProperty "incremental" prop_incremental
    , testProperty "all splits"  prop_splits
    , testProperty "put/get"     prop_putGet
    , testProperty "write/read"  prop_writeRead
    , testProperty "prop_hGetExactlyCRC32C_SBS" prop_hGetExactlyCRC32C_SBS
    , testProperty "prop_hGetAllCRC32C'" prop_hGetAllCRC32C'
    ]

{-------------------------------------------------------------------------------
  Properties
-------------------------------------------------------------------------------}

prop_incremental :: [BS.ByteString] -> Bool
prop_incremental bss =
    Fold.foldl' (flip updateCRC32C) initialCRC32C bss
 == CRC32C (Data.Digest.CRC32C.crc32c (BS.concat bss))

prop_splits :: BS.ByteString -> Bool
prop_splits bs =
    and [ updateCRC32C b2 (updateCRC32C b1 initialCRC32C) == expected
        | let expected = CRC32C (Data.Digest.CRC32C.crc32c bs)
        , n <- [0 .. BS.length bs]
        , let (b1, b2) = BS.splitAt n bs ]

prop_putGet :: PartialIOErrors -> [BS.ByteString] -> Bool
prop_putGet (PartialIOErrors errs) bss =
    fst $ IOSim.runSimOrThrow $
          FsSim.runSimErrorFS FsSim.empty errs $ \_ fs -> do
      let path = mkFsPath ["foo"]
      crc1 <- withFile fs path (WriteMode MustBeNew) $ \h ->
                foldM (\crc bs -> snd <$> hPutAllCRC32C fs h bs crc)
                      initialCRC32C bss
      crc2 <- withFile fs path ReadMode $ \h ->
                let go crc = do
                      (bs,crc') <- hGetSomeCRC32C fs h 42 crc
                      if BS.null bs then pure crc' else go crc'
                 in go initialCRC32C
      crc3 <- readFileCRC32C fs path
      let expected = CRC32C (Data.Digest.CRC32C.crc32c (BS.concat bss))
      pure (all (expected==) [crc1, crc2, crc3])

-- TODO: test like prop_putGet but with put corruption, to detect that the
-- crc doesn't match, to detect the corruption.

prop_writeRead :: PartialIOErrors -> ChecksumsFile -> Bool
prop_writeRead (PartialIOErrors errs) chks =
    fst $ IOSim.runSimOrThrow $
          FsSim.runSimErrorFS FsSim.empty errs $ \_ fs -> do
      let path = mkFsPath ["foo.checksums"]
      writeChecksumsFile fs path chks
      chks' <- readChecksumsFile fs path
      pure (chks == chks')

instance Arbitrary CRC32C where
    arbitrary = CRC32C <$> arbitraryBoundedIntegral

instance Arbitrary ChecksumsFileName where
    arbitrary = ChecksumsFileName <$> name
      where
        name     = fmap BS.pack (listOf nameChar)
        nameChar = arbitrary `suchThat` \w8 ->
                     let c = toEnum (fromIntegral w8)
                      in c /= '(' && c /= ')' && c /= '\n'

-- | File system simulated errors for partial reads and writes. No other
-- silent corruption or noisy exceptions. Used to test that handling of
-- partial read\/writes is robust.
--
newtype PartialIOErrors = PartialIOErrors FsSim.Errors
  deriving stock Show

instance Arbitrary PartialIOErrors where
    arbitrary = do
        hGetSomeE <- genPartialErrorStream
        hPutSomeE <- genPartialErrorStream
        pure $ PartialIOErrors FsSim.emptyErrors {
                                   FsSim.hGetSomeE,
                                   FsSim.hPutSomeE
                                 }

    shrink (PartialIOErrors errs) = map PartialIOErrors (shrink errs)

genPartialErrorStream :: Gen (FsSim.Stream.Stream (Either a FsSim.Partial))
genPartialErrorStream =
    FsSim.Stream.genInfinite
  . FsSim.Stream.genMaybe 2 1  -- 2:1  normal:partial
  . fmap Right
  $ arbitrary

{-------------------------------------------------------------------------------
  Properties for functions with user-supplied buffers
-------------------------------------------------------------------------------}

newtype BS = BS BS.ByteString
  deriving stock (Show, Eq)

instance Arbitrary BS where
  arbitrary = BS . BS.pack <$> scale (2*) arbitrary
  shrink = fmap (BS . BS.pack) . shrink . BS.unpack . unBS
    where unBS (BS x) = x

prop_hGetExactlyCRC32C_SBS :: BS -> Property
prop_hGetExactlyCRC32C_SBS (BS bs) =
  ioProperty $ withTempIOHasFS "tempDir" $ \hfs -> do
      withFile hfs (mkFsPath ["temp.file"]) (WriteMode MustBeNew) $ \h ->
        void $ hPutAllStrict hfs h bs
      x <- withFile hfs (mkFsPath ["temp.file"]) ReadMode $ \h ->
        simpl hfs h initialCRC32C
      y <- withFile hfs (mkFsPath ["temp.file"]) ReadMode $ \h ->
        hGetExactlyCRC32C_SBS hfs h (fromIntegral $ BS.length bs) initialCRC32C

      pure $ x === y
  where
    simpl fs h crc = do
      lbs <- hGetAll fs h
      let !crc' = BSL.foldlChunks (flip updateCRC32C) crc lbs
      pure (SBS.toShort (BS.toStrict lbs), crc')


prop_hGetAllCRC32C':: Positive (Small ByteCount) -> BS -> Property
prop_hGetAllCRC32C' (Positive (Small chunkSize)) (BS bs) =
  ioProperty $ withTempIOHasFS "tempDir" $ \hfs -> do
      withFile hfs (mkFsPath ["temp.file"]) (WriteMode MustBeNew) $ \h ->
        void $ hPutAllStrict hfs h bs
      x <- withFile hfs (mkFsPath ["temp.file"]) ReadMode $ \h ->
        simpl hfs h initialCRC32C
      y <- withFile hfs (mkFsPath ["temp.file"]) ReadMode $ \h ->
        hGetAllCRC32C'  hfs h (ChunkSize chunkSize) initialCRC32C

      pure
        $ tabulate "number of chunks"
                   [ showPowersOf 2 $ ceiling @Double $
                        fromIntegral (BS.length bs) / (fromIntegral chunkSize) ]
        $ x === y
  where
    simpl fs h crc = do
      lbs <- hGetAll fs h
      let !crc' = BSL.foldlChunks (flip updateCRC32C) crc lbs
      pure crc'
