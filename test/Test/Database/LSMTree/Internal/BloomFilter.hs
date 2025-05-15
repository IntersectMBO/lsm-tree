module Test.Database.LSMTree.Internal.BloomFilter (tests) where

import           Control.DeepSeq (deepseq)
import           Control.Exception (displayException)
import           Control.Monad (void)
import qualified Control.Monad.IOSim as IOSim
import           Data.Bits ((.&.))
import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as BS.Builder
import qualified Data.ByteString.Builder.Extra as BS.Builder
import qualified Data.ByteString.Lazy as LBS
import qualified Data.Set as Set
import qualified Data.Vector as V
import qualified Data.Vector.Primitive as VP
import           Data.Word (Word32, Word64, byteSwap32)
import qualified System.FS.API as FS
import qualified System.FS.API.Strict as FS
import qualified System.FS.Sim.MockFS as MockFS
import qualified System.FS.Sim.STM as FSSim

import           Test.QuickCheck.Gen (genDouble)
import           Test.QuickCheck.Instances ()
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.QuickCheck hiding ((.&.))

import qualified Data.BloomFilter.Blocked as BF
import           Database.LSMTree.Internal.BloomFilter
import           Database.LSMTree.Internal.Serialise (SerialisedKey,
                     serialiseKey)

--TODO: add a golden test for the BloomFilter format vs the 'formatVersion'
-- to ensure we don't change the format without conciously bumping the version.
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
        prop_bloomQueries
    ]

roundtrip_prop :: Positive (Small Int) -> Positive Int ->  [Word64] -> Property
roundtrip_prop (Positive (Small hfN)) (Positive bits) ws =
    counterexample (show bs) $
    case bloomFilterFromBS bs of
      Left  err -> counterexample (displayException err) $ property False
      Right rhs -> lhs === rhs
  where
    sz  = BF.BloomSize { sizeBits   = limitBits bits,
                         sizeHashes = hfN }
    lhs = BF.create sz (\b -> mapM_ (BF.insert b) ws)
    bs  = LBS.toStrict (bloomFilterToLBS lhs)

limitBits :: Int -> Int
limitBits b = b .&. 0xffffff

prop_total_deserialisation :: BS.ByteString -> Property
prop_total_deserialisation bs =
    case bloomFilterFromBS bs of
      Left err -> label (displayException err) $ property True
      Right bf -> label "parsed successfully" $ property $
        bf `deepseq` True

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
        bloomFilterFromFile hfs h

-- Length is in bits. A large length would require significant amount of
-- memory, so we make it 'Small'.
prop_total_deserialisation_whitebox :: Word32 -> Small Word64 -> Property
prop_total_deserialisation_whitebox hsn (Small nbits) =
      forAll genBytes $ \bytes ->
      forAll genVersion $ \version ->
        prop_total_deserialisation (prefix version <> BS.pack bytes)
  where
    genBytes   = do n <- choose (-1,1)
                    vector (nbytes' + n)
    nbytes     = (fromIntegral nbits+7) `div` 8
    nbytes'    = ((nbytes + 63) `div` 64) * 64 -- rounded to nearest 64 bytes
    genVersion = frequency [
                   (6, pure bloomFilterVersion),
                   (1, pure (byteSwap32 bloomFilterVersion)),
                   (1, arbitrary)
                 ]
    prefix version =
      LBS.toStrict $ BS.Builder.toLazyByteString $
          BS.Builder.word32Host version
       <> BS.Builder.word32Host hsn
       <> BS.Builder.word64Host nbits

newtype FPR = FPR Double deriving stock Show

instance Arbitrary FPR where
  arbitrary =
    FPR <$> frequency
      [ (1, pure 0.999)
      , (9, (fmap (/2) genDouble) `suchThat` \fpr -> fpr > 0) ]

prop_bloomQueries :: FPR
                  -> [[Small Word64]]
                  -> [Small Word64]
                  -> Property
prop_bloomQueries (FPR fpr) filters keys =
    let filters' :: [BF.Bloom SerialisedKey]
        filters' = map (BF.fromList (BF.policyForFPR fpr) . map (\(Small k) -> serialiseKey k))
                       filters

        keys' :: [SerialisedKey]
        keys' = map (\(Small k) -> serialiseKey k) keys

        referenceResults :: [(Int, Int)]
        referenceResults =
          [ (f_i, k_i)
          | (k, k_i) <- zip keys' [0..]
          , (f, f_i) <- zip filters' [0..]
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

    -- To get coverage of BF.bloomQueries array resizing we want some
    -- cases with high FPRs.
     in tabulate "FPR" [show (round (fpr * 10) * 10 :: Int) ++ "%"] $
        coverTable "FPR" [("100%", 5)] $
        tabulate "distribution of true/false positives/negatives" distribution $
        referenceResults
       ===
        map (\(RunIxKeyIx rix kix) -> (rix, kix))
            (VP.toList (bloomQueries (V.fromList filters')
                                     (V.fromList keys')))
