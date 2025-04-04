{-# LANGUAGE CPP #-}
{-# OPTIONS_GHC -Wno-orphans #-}
module Test.Database.LSMTree.Internal.BloomFilter (tests) where

import           Control.Exception (displayException)
import           Control.DeepSeq (deepseq)
import           Control.Monad (void)
import qualified Control.Monad.IOSim as IOSim
import           Data.Bits ( (.&.))
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
import qualified Data.ByteString.Builder as BS.Builder
import qualified Data.ByteString.Builder.Extra as BS.Builder
import qualified Data.Set as Set
import qualified Data.Vector as V
import qualified Data.Vector.Primitive as VP
import           Data.Word (Word32, Word64)
import qualified System.FS.API as FS
import qualified System.FS.API.Strict as FS
import qualified System.FS.Sim.MockFS as MockFS
import qualified System.FS.Sim.STM as FSSim

import           Test.QuickCheck.Gen (genDouble)
import           Test.QuickCheck.Instances ()
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
import qualified Database.LSMTree.Internal.BloomFilterQuery2 as Bloom2
import           Test.QuickCheck.Classes (primLaws)
import           Test.Util.QC
#endif

tests :: TestTree
tests = testGroup "Database.LSMTree.Internal.BloomFilter"
    [ testProperty "roundtrip" roundtrip_prop
      -- a specific case: 300 bits is just under 5x 64 bit words
    , testProperty "roundtrip-3-300" $ roundtrip_prop (Positive (Small 3)) (Positive 300)
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

roundtrip_prop :: Positive (Small Int) -> Positive Int ->  [Word64] -> Property
roundtrip_prop (Positive (Small hfN)) (Positive bits) ws =
    counterexample (show bs) $
    case bloomFilterFromBS bs of
      Left  err -> label (displayException err) $ property True
      Right rhs -> lhs === rhs
  where
    sz  = BF.BloomSize { bloomNumBits   = limitBits bits,
                         bloomNumHashes = hfN }
    lhs = BF.fromList sz ws
    bs  = LBS.toStrict (bloomFilterToLBS lhs)

limitBits :: Int -> Int
limitBits b = b .&. 0xffffff

prop_total_deserialisation :: BS.ByteString -> Property
prop_total_deserialisation bs =
    case bloomFilterFromBS bs of
      Left err -> label (displayException err) $ property True
      Right bf -> label "parsed successfully" $ property $
        -- Just forcing the filter is not enough (e.g. the bit vector might
        -- point outside of the byte array).
        bf `deepseq` BF.bloomInvariant bf

-- | Write the bytestring to a file in the mock file system and then use
-- 'bloomFilterFromFile'.
bloomFilterFromBS :: BS.ByteString -> Either IOSim.Failure (BF.Bloom a)
bloomFilterFromBS bs =
    IOSim.runSim $ do
      hfs <- FSSim.simHasFS' MockFS.empty
      let file = FS.mkFsPath ["filter"]
      -- write the bytestring
      FS.withFile hfs file (FS.WriteMode FS.MustBeNew) $ \h -> do
        void $ FS.hPutAllStrict hfs h bs
      -- deserialise from file
      FS.withFile hfs file FS.ReadMode $ \h ->
        bloomFilterFromFile hfs file h

-- Length is in Word64s. A large length would require significant amount of
-- memory, so we make it 'Small'.
prop_total_deserialisation_whitebox :: Word32 -> Small Word32 -> Property
prop_total_deserialisation_whitebox hsn (Small nword64s) =
      forAll (vector (fromIntegral nword64s * 8)) $ \bytes ->
        prop_total_deserialisation (prefix <> BS.pack bytes)
  where
    prefix = LBS.toStrict $ BS.Builder.toLazyByteString $
                 BS.Builder.word32Host 1 {- version -}
              <> BS.Builder.word32Host hsn
              <> BS.Builder.word64Host (fromIntegral nword64s)

newtype FPR = FPR Double deriving stock Show

instance Arbitrary FPR where
  arbitrary =
    FPR <$> frequency
      [ (1, pure 0.999)
      , (9, (fmap (/2) genDouble) `suchThat` \fpr -> fpr > 0) ]

prop_bloomQueries1 :: FPR
                   -> [[Small Word64]]
                   -> [Small Word64]
                   -> Property
prop_bloomQueries1 (FPR fpr) filters keys =
    let filters' :: [BF.Bloom SerialisedKey]
        filters' = map (BF.easyList fpr . map (\(Small k) -> serialiseKey k))
                       filters

        keys' :: [SerialisedKey]
        keys' = map (\(Small k) -> serialiseKey k) keys

        referenceResults :: [(Int, Int)]
        referenceResults =
          [ (f_i, k_i)
          | (f, f_i) <- zip filters' [0..]
          , (k, k_i) <- zip keys' [0..]
          , BF.elem k f
          ]

        filterSets = map (Set.fromList . map (\(Small k) -> serialiseKey k)) filters
        referenceCmp =
          [ (BF.elem k f, k `Set.member` f')
          | (f, f') <- zip filters' filterSets
          , k       <- keys'
          ]
        truePositives  = [ "true positives"  | (True,  True)  <- referenceCmp ]
        falsePositives = [ "false positives" | (True,  False) <- referenceCmp ]
        trueNegatives  = [ "true negatives"  | (False, False) <- referenceCmp ]
        falseNegatives = [ "false negatives" | (False, True)  <- referenceCmp ]
        distribution   = truePositives ++ falsePositives
                      ++ trueNegatives ++ falseNegatives

    -- To get coverage of Bloom1.bloomQueries array resizing we want some
    -- cases with high FPRs.
     in tabulate "FPR" [show (round (fpr * 10) * 10 :: Int) ++ "%"] $
        coverTable "FPR" [("100%", 5)] $
        tabulate "distribution of true/false positives/negatives" distribution $
        referenceResults
       ===
        map (\(Bloom1.RunIxKeyIx rix kix) -> (rix, kix))
            (VP.toList (Bloom1.bloomQueries (V.fromList filters')
                                            (V.fromList keys')))

#ifdef BLOOM_QUERY_FAST
prop_bloomQueries2 :: FPR
                   -> [[Small Word64]]
                   -> [Small Word64]
                   -> Property
prop_bloomQueries2 (FPR fpr) filters keys =
    let filters' :: [BF.Bloom SerialisedKey]
        filters' = map (BF.easyList fpr . map (\(Small k) -> serialiseKey k)) filters

        keys' :: [SerialisedKey]
        keys' = map (\(Small k) -> serialiseKey k) keys

        referenceResults :: [(Int, Int)]
        referenceResults =
          [ (f_i, k_i)
          | (f, f_i) <- zip filters' [0..]
          , (k, k_i) <- zip keys' [0..]
          , BF.elem k f
          ]

    -- To get coverage of Bloom2.bloomQueries array resizing we want some
    -- cases with high FPRs.
     in tabulate "FPR" [show (round (fpr * 10) * 10 :: Int) ++ "%"] $
        coverTable "FPR" [("100%", 5)] $

        referenceResults
       ===
        map (\(Bloom2.RunIxKeyIx rix kix) -> (rix, kix))
            (VP.toList (Bloom2.bloomQueries (V.fromList filters')
                                            (V.fromList keys')))

instance Arbitrary Bloom2.CandidateProbe where
  arbitrary = Bloom2.MkCandidateProbe <$> arbitrary <*> arbitrary

deriving stock instance Eq Bloom2.CandidateProbe
#endif
