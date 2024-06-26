module Test.Database.LSMTree.Internal.BloomFilter (tests) where

import           Control.DeepSeq (deepseq)
import           Data.Bits (unsafeShiftL, unsafeShiftR, (.&.))
import qualified Data.ByteString.Lazy as LBS
import qualified Data.ByteString.Short as SBS
import           Data.Primitive.ByteArray (ByteArray (..), byteArrayFromList)
import           Data.Word (Word32, Word64)
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.QuickCheck (Positive (..), Property, Small (..),
                     counterexample, forAll, label, property, testProperty,
                     vector, withMaxSuccess, (===))

import qualified Data.BloomFilter as BF
import qualified Data.BloomFilter.Internal as BF (bloomInvariant)
import           Database.LSMTree.Internal.BloomFilter

tests :: TestTree
tests = testGroup "Database.LSMTree.Internal.BloomFilter"
    [ testProperty "roundtrip" roundtrip_prop
      -- a specific case: 300 bits is just under 5x 64 bit words
    , testProperty "roundtrip-3-300" $ roundtrip_prop (Positive (Small 3)) 300
    , testProperty "total-deserialisation" $ withMaxSuccess 10000 $
        prop_total_deserialisation
    , testProperty "total-deserialisation-whitebox" $ withMaxSuccess 10000 $
        prop_total_deserialisation_whitebox
    ]

roundtrip_prop :: Positive (Small Int) -> Word64 ->  [Word64] -> Property
roundtrip_prop (Positive (Small hfN)) (limitBits -> bits) ws =
    counterexample (show sbs) $
    Right lhs === rhs
  where
    lhs = BF.fromList hfN bits ws
    sbs = SBS.toShort (LBS.toStrict (bloomFilterToLBS lhs))
    rhs = bloomFilterFromSBS sbs

limitBits :: Word64 -> Word64
limitBits b = b .&. 0xffffff

prop_total_deserialisation :: [Word32] -> Property
prop_total_deserialisation word32s =
    case bloomFilterFromSBS (SBS.SBS ba) of
      Left err -> label err $ property True
      Right bf -> label "parsed successfully" $ property $
        -- Just forcing the filter is not enough (e.g. the bit vector might
        -- point outside of the byte array).
        bf `deepseq` BF.bloomInvariant bf
  where
    !(ByteArray ba) = byteArrayFromList word32s

-- Length is in Word64s. A large length would require significant amount of
-- memory, so we make it 'Small'.
prop_total_deserialisation_whitebox :: Word32 -> Small Word32 -> Property
prop_total_deserialisation_whitebox hsn (Small len64) =
      forAll (vector (fromIntegral len64 * 2)) $ \word32s ->
        prop_total_deserialisation (prefix <> word32s)
  where
    prefix =
      [ 1 {- version -}
      , hsn
      , unsafeShiftL len64 6         -- len64 * 64 (lower 32 bits)
      , unsafeShiftR len64 (32 - 6)  -- len64 * 64 (upper 32 bits)
      ]
