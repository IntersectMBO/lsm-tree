{-# LANGUAGE DerivingStrategies  #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# OPTIONS_GHC -Wno-orphans #-}

module Test.Database.LSMTree.Internal.Serialise (tests) where

import           Data.Bits
import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as BB
import qualified Data.ByteString.Short as SBS
import qualified Data.Vector.Primitive as VP
import           Data.Word
import qualified Database.LSMTree.Internal.RawBytes as RB
import           Database.LSMTree.Internal.Serialise
import           Test.Tasty
import           Test.Tasty.HUnit

tests :: TestTree
tests = testGroup "Test.Database.LSMTree.Internal.Serialise" [
      testCase "example keyTopBits64" $ do
        let k = SerialisedKey' (VP.fromList [0, 0, 0, 0, 37, 42, 204, 130])
            expected :: Word64
            expected = 37 `shiftL` 24 + 42 `shiftL` 16 + 204 `shiftL` 8 + 130
        expected                   @=? keyTopBits64 k
    , testCase "example keyTopBits64 on sliced byte array" $ do
        let pvec = VP.fromList [0, 0, 0, 0, 0, 37, 42, 204, 130]
            k = SerialisedKey' (VP.slice 1 (VP.length pvec - 1) pvec)
            expected :: Word64
            expected = 37 `shiftL` 24 + 42 `shiftL` 16 + 204 `shiftL` 8 + 130
        expected                   @=? keyTopBits64 k
    , testCase "example unsafeFromByteString and fromShortByteString" $ do
        let bb = mconcat [BB.word64LE x | x <- [0..100]]
            bs = BS.toStrict . BB.toLazyByteString $ bb
            k1 = RB.unsafeFromByteString bs
            k2 = RB.fromShortByteString (SBS.toShort bs)
        k1 @=? k2
    ]
