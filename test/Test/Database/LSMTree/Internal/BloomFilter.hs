{-# LANGUAGE ViewPatterns #-}
module Test.Database.LSMTree.Internal.BloomFilter (tests) where

import           Data.Bits ((.&.))
import qualified Data.ByteString.Builder as B
import qualified Data.ByteString.Lazy as LBS
import qualified Data.ByteString.Short as SBS
import           Data.Word (Word64)
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.QuickCheck (Positive (..), Property, Small (..),
                     counterexample, testProperty, (===))

import qualified Data.BloomFilter as BF
import           Database.LSMTree.Internal.BloomFilter

tests :: TestTree
tests = testGroup "Database.LSMTree.Internal.BloomFilter"
    [ testProperty "roundtrip" roundtrip_prop
      -- a specific case: 300 bits is just under 5x 64 bit words
    , testProperty "roundtrip-3-300" $ roundtrip_prop (Positive (Small 3)) 300
    ]

roundtrip_prop :: Positive (Small Int) -> Word64 ->  [Word64] -> Property
roundtrip_prop (Positive (Small hfN)) (limitBits -> bits) ws =
    counterexample (show sbs) $
    Right lhs === rhs
  where
    lhs = BF.fromList hfN bits ws
    sbs = SBS.toShort (LBS.toStrict (B.toLazyByteString (bloomFilterToBuilder lhs)))
    rhs = bloomFilterFromSBS sbs

limitBits :: Word64 -> Word64
limitBits b = b .&. 0xffffff
