module Main (main) where

import qualified Data.BloomFilter.Classic as B
import           Data.BloomFilter.Hash (Hashable (..), hash64)

import           Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
import           Data.Int (Int64)
import           Data.Word (Word32, Word64)

import           Test.Tasty
import           Test.Tasty.QuickCheck
import           Test.QuickCheck.Instances ()

main :: IO ()
main = defaultMain tests

tests :: TestTree
tests = testGroup "bloomfilter"
    [ testGroup "calculations"
        [ testProperty "prop_calc_policy_fpr"       prop_calc_policy_fpr
        , testProperty "prop_calc_size_hashes_bits" prop_calc_size_hashes_bits
        , testProperty "prop_calc_size_fpr_fpr"     prop_calc_size_fpr_fpr
        , testProperty "prop_calc_size_fpr_bits"    prop_calc_size_fpr_bits
        ]
    , testGroup "fromList"
        [ testProperty "()" $ prop_pai ()
        , testProperty "Char" $ prop_pai (undefined :: Char)
        , testProperty "Word32" $ prop_pai (undefined :: Word32)
        , testProperty "Word64" $ prop_pai (undefined :: Word64)
        , testProperty "ByteString" $ prop_pai (undefined :: ByteString)
        , testProperty "LBS.ByteString" $ prop_pai (undefined :: LBS.ByteString)
        , testProperty "LBS.ByteString" $ prop_pai (undefined :: String)
        ]
    , testGroup "hashes"
        [ testProperty "prop_rechunked_eq" prop_rechunked_eq
        , testProperty "prop_tuple_ex" $
          hash64 (BS.empty, BS.pack [120]) =/= hash64 (BS.pack [120], BS.empty)
        , testProperty "prop_list_ex" $
          hash64 [[],[],[BS.empty]] =/= hash64 [[],[BS.empty],[]]
        ]
    ]

-------------------------------------------------------------------------------
-- Element is in a Bloom filter
-------------------------------------------------------------------------------

prop_pai :: (Hashable a) => a -> a -> [a] -> FPR -> Property
prop_pai _ x xs (FPR q) = let bf = B.fromList (B.policyForFPR q) (x:xs) in
    B.elem x bf .&&. not (B.notElem x bf)

-------------------------------------------------------------------------------
-- Bloom filter size calculations
-------------------------------------------------------------------------------

prop_calc_policy_fpr :: FPR -> Property
prop_calc_policy_fpr (FPR fpr) =
  let policy = B.policyForFPR fpr
   in B.policyFPR policy ~~~ fpr

prop_calc_size_hashes_bits :: BitsPerEntry -> NumEntries -> Property
prop_calc_size_hashes_bits (BitsPerEntry c) (NumEntries numEntries) =
  let bsize = B.sizeForBits c numEntries
   in numHashFunctions (fromIntegral (B.sizeBits bsize))
                       (fromIntegral numEntries)
  === fromIntegral (B.sizeHashes bsize)

prop_calc_size_fpr_fpr :: FPR -> NumEntries -> Property
prop_calc_size_fpr_fpr (FPR fpr) (NumEntries numEntries) =
  let bsize = B.sizeForFPR fpr numEntries
   in falsePositiveRate (fromIntegral (B.sizeBits bsize))
                        (fromIntegral numEntries)
                        (fromIntegral (B.sizeHashes bsize))
   ~~~ fpr

prop_calc_size_fpr_bits :: BitsPerEntry -> NumEntries -> Property
prop_calc_size_fpr_bits (BitsPerEntry c) (NumEntries numEntries) =
  let policy = B.policyForBits c
      bsize  = B.sizeForPolicy policy numEntries
   in falsePositiveRate (fromIntegral (B.sizeBits bsize))
                        (fromIntegral numEntries)
                        (fromIntegral (B.sizeHashes bsize))
   ~~~ B.policyFPR policy

-- reference implementations used for sanity checks

-- | Computes the optimal number of hash functions that minimises the false
-- positive rate for a bloom filter.
--
-- See Niv Dayan, Manos Athanassoulis, Stratos Idreos,
-- /Optimal Bloom Filters and Adaptive Merging for LSM-Trees/,
-- Footnote 2, page 6.
numHashFunctions ::
     Double -- ^ Number of bits assigned to the bloom filter.
  -> Double -- ^ Number of entries inserted into the bloom filter.
  -> Integer
numHashFunctions bits nentries =
    round $
      max 1 ((bits / nentries) * log 2)

-- | False positive rate
--
-- See <https://en.wikipedia.org/wiki/Bloom_filter#Probability_of_false_positives>
--
falsePositiveRate ::
     Double -- ^ Number of bits assigned to the bloom filter.
  -> Double -- ^ Number of entries inserted into the bloom filter.
  -> Double -- ^ Number of hash functions
  -> Double
falsePositiveRate m n k =
    (1 - exp (-(k * n / m))) ** k

(~~~) :: Double -> Double -> Property
a ~~~ b =
    counterexample (show a ++ " /= " ++ show b) $
      abs (a - b) < epsilon
  where
    epsilon = 1e-6 :: Double

-------------------------------------------------------------------------------
-- Chunking
-------------------------------------------------------------------------------

-- Ensure that a property over a lazy ByteString holds if we change
-- the chunk boundaries.

rechunk :: Int64 -> LBS.ByteString -> LBS.ByteString
rechunk k xs | k <= 0    = xs
             | otherwise = LBS.fromChunks (go xs)
    where go s | LBS.null s = []
               | otherwise = let (pre,suf) = LBS.splitAt k s
                             in  repack pre : go suf
          repack = BS.concat . LBS.toChunks


prop_rechunked :: (Eq a, Show a) => (LBS.ByteString -> a) -> LBS.ByteString -> Property
prop_rechunked f s =
    let l = LBS.length s
    in l > 0 ==> forAll (choose (1,l-1)) $ \k ->
        let n = k `mod` l
        in n > 0 ==> f s === f (rechunk n s)

prop_rechunked_eq :: LBS.ByteString -> Property
prop_rechunked_eq = prop_rechunked hash64

-------------------------------------------------------------------------------
-- QC generators
-------------------------------------------------------------------------------

newtype FPR = FPR Double
  deriving stock Show

instance Arbitrary FPR where
  -- The most significant effect of the FPR is from its (negative) exponent,
  -- which influences both filter bits and number of hashes. So we generate
  -- values with an exponent from 10^0 to 10^-6
  arbitrary = do
      m <- choose (epsilon, 1-epsilon)
      e <- choose (0, 6)
      pure (FPR (m * 10 ** (-e)))
    where
      epsilon = 1e-6 :: Double

newtype BitsPerEntry = BitsPerEntry Double
  deriving stock Show

instance Arbitrary BitsPerEntry where
  arbitrary = BitsPerEntry <$> choose (1, 50)

newtype NumEntries = NumEntries Int
  deriving stock Show

-- | The FPR calculations are approximations and are not expected to be
-- accurate for low numbers of entries or bits.
--
instance Arbitrary NumEntries where
  arbitrary = NumEntries <$> choose (1_000, 100_000_000)
  shrink (NumEntries n) =
    [ NumEntries n' | n' <- shrink n, n' >= 1000 ]
