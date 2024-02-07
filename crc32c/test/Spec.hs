{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE OverloadedStrings #-}
module Main where

import qualified Data.ByteString.Char8 as C
import           Data.Digest.CRC32C
import           Test.Hspec
import           Test.QuickCheck

main :: IO ()
main = hspec spec
spec :: Spec
spec = do
  describe "Data.Digest.CRC32C" specProperty
  where
    specProperty = do
      context "edge-test" $ do
        it "empty-0" $ do
          crc32c ""          `shouldBe` 0x00000000
        it "empty-1" $ do
          crc32c "123456789" `shouldBe` 0xE3069283
          let c = crc32c "123456789"
          crc32c_update c "" `shouldBe` 0xE3069283
        it "empty-2" $ do
          crc32c "123456789" `shouldBe` 0xE3069283
          let d = crc32c_update (crc32c "") "123456789"
          d                  `shouldBe` 0xE3069283
      context "random-str" $ do
        it "QuickCheck" $ property $ \(i :: Word) ->
          let str = C.pack $ take 100 $ cycle (show i)
              (sa,sb) = C.splitAt 50 str
          in crc32c str == crc32c_update (crc32c sa) sb
