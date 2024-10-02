{-# LANGUAGE CPP #-}
{-# OPTIONS_GHC -Wno-orphans #-}
module Test.Database.LSMTree.Internal.BloomFilter (tests) where

import           Control.DeepSeq (deepseq)
import           Data.Bits (unsafeShiftL, unsafeShiftR, (.&.))
import qualified Data.ByteString.Lazy as LBS
import qualified Data.ByteString.Short as SBS
import           Data.Primitive.ByteArray (ByteArray (..), byteArrayFromList)
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import           Data.Word (Word32, Word64)

import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.QuickCheck hiding ((.&.))

import qualified Data.BloomFilter as BF
import qualified Data.BloomFilter.Easy as BF
import qualified Data.BloomFilter.Internal as BF (bloomInvariant)
import           Database.LSMTree.Internal.BloomFilter
import qualified Database.LSMTree.Internal.BloomFilterQuery1 as Bloom1
import           Database.LSMTree.Internal.Serialise (SerialisedKey,
                     serialiseKey)

#ifdef BLOOM_QUERY_FAST
import qualified Data.Vector.Primitive as VP
import qualified Database.LSMTree.Internal.BloomFilterQuery2 as Bloom2
import           Test.QuickCheck.Classes (primLaws)
import           Test.Util.QC
#endif

tests :: TestTree
tests = testGroup "Database.LSMTree.Internal.BloomFilter"
    [ testProperty "roundtrip" roundtrip_prop
      -- a specific case: 300 bits is just under 5x 64 bit words
    , testProperty "roundtrip-3-300" $ roundtrip_prop (Positive (Small 3)) 300
    , testProperty "total-deserialisation" $ withMaxSuccess 10000 $
        prop_total_deserialisation
    , testProperty "total-deserialisation-whitebox" $ withMaxSuccess 10000 $
        prop_total_deserialisation_whitebox
    , testProperty "bloomQueries (bulk)" $
        prop_bloomQueries1
#ifdef BLOOM_QUERY_FAST
    , testClassLaws "CandidateProbe" (primLaws (Proxy :: Proxy Bloom2.CandidateProbe))
    , testProperty "bloomQueries (bulk, prefetching)" $
        prop_bloomQueries2
#endif
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

prop_bloomQueries1 :: [[Small Word64]]
                   -> [Small Word64]
                   -> Property
prop_bloomQueries1 filters keys =
    let filters' :: [BF.Bloom SerialisedKey]
        filters' = map (BF.easyList 0.1 . map (\(Small k) -> serialiseKey k)) filters

        keys' :: [SerialisedKey]
        keys' = map (\(Small k) -> serialiseKey k) keys

     in [ (f_i, k_i)
        | (f, f_i) <- zip filters' [0..]
        , (k, k_i) <- zip keys' [0..]
        , BF.elem k f
        ]
       ===
        VU.toList (Bloom1.bloomQueriesDefault (V.fromList filters')
                                              (V.fromList keys'))

#ifdef BLOOM_QUERY_FAST
prop_bloomQueries2 :: [[Small Word64]]
                   -> [Small Word64]
                   -> Property
prop_bloomQueries2 filters keys =
    let filters' :: [BF.Bloom SerialisedKey]
        filters' = map (BF.easyList 0.1 . map (\(Small k) -> serialiseKey k)) filters

        keys' :: [SerialisedKey]
        keys' = map (\(Small k) -> serialiseKey k) keys

     in [ (f_i, k_i)
        | (f, f_i) <- zip filters' [0..]
        , (k, k_i) <- zip keys' [0..]
        , BF.elem k f
        ]
       ===
        map (\(Bloom2.RunIxKeyIx rix kix) -> (rix, kix))
            (VP.toList (Bloom2.bloomQueriesDefault (V.fromList filters')
                                                   (V.fromList keys')))

instance Arbitrary Bloom2.CandidateProbe where
  arbitrary = Bloom2.MkCandidateProbe <$> arbitrary <*> arbitrary

deriving stock instance Eq Bloom2.CandidateProbe
#endif
