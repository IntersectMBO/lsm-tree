{-# LANGUAGE NamedFieldPuns #-}
{-# OPTIONS_GHC -Wno-orphans #-}
{- HLINT ignore "Use camelCase" -}

module Test.Database.LSMTree.Internal.CRC32C (
    -- * Main test tree
    tests
  ) where

import           Database.LSMTree.Internal.CRC32C

import qualified Data.ByteString as BS
import qualified Data.Digest.CRC32C
import           Data.List (foldl')

import           Control.Monad

import qualified Control.Monad.IOSim as IOSim
import           System.FS.API
import qualified System.FS.Sim.Error as FsSim
import qualified System.FS.Sim.MockFS as FsSim
import qualified System.FS.Sim.Stream as FsSim.Stream

import           Test.QuickCheck
import           Test.QuickCheck.Instances ()
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.QuickCheck (testProperty)


tests :: TestTree
tests = testGroup "Database.LSMTree.Internal.CRC32C" [
      testProperty "incremental" prop_incremental
    , testProperty "all splits"  prop_splits
    , testProperty "put/get"     prop_putGet
    , testProperty "write/read"  prop_writeRead
    ]

{-------------------------------------------------------------------------------
  Properties
-------------------------------------------------------------------------------}

prop_incremental :: [BS.ByteString] -> Bool
prop_incremental bss =
    foldl' (flip updateCRC32C) initialCRC32C bss
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
                      if BS.null bs then return crc' else go crc'
                 in go initialCRC32C
      crc3 <- readFileCRC32C fs path
      let expected = CRC32C (Data.Digest.CRC32C.crc32c (BS.concat bss))
      return (all (expected==) [crc1, crc2, crc3])

-- TODO: test like prop_putGet but with put curruption, to detect that the
-- crc doesn't match, to detect the corruption.

prop_writeRead :: PartialIOErrors -> ChecksumsFile -> Bool
prop_writeRead (PartialIOErrors errs) chks =
    fst $ IOSim.runSimOrThrow $
          FsSim.runSimErrorFS FsSim.empty errs $ \_ fs -> do
      let path = mkFsPath ["foo.checksums"]
      writeChecksumsFile fs path chks
      chks' <- readChecksumsFile fs path
      return (chks == chks')

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
  deriving Show

instance Arbitrary PartialIOErrors where
    arbitrary = do
        hGetSomeE <- genPartialErrorStream
        hPutSomeE <- genPartialErrorStream
        return $ PartialIOErrors FsSim.emptyErrors {
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

