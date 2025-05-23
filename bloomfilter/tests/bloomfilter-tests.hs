module Main (main) where

import qualified Data.BloomFilter.Blocked as Bloom.Blocked
import qualified Data.BloomFilter.Classic as B
import qualified Data.BloomFilter.Classic as Bloom.Classic
import           Data.BloomFilter.Hash (Hashable (..), hash64)

import           Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
import           Data.Int (Int64)
import           Data.Proxy (Proxy (..))
import           Data.Word (Word32, Word64)

import           Test.QuickCheck.Instances ()
import           Test.Tasty
import           Test.Tasty.QuickCheck

import           Prelude hiding (elem, notElem)

main :: IO ()
main = defaultMain tests

--TODO: add a golden test for the BloomFilter format vs the 'formatVersion'
-- to ensure we don't change the format without conciously bumping the version.
tests :: TestTree
tests =
  testGroup "Data.BloomFilter" $
    [ testGroup "Classic"
        [ testGroup "calculations" $
            test_calculations proxyClassic
              (FPR 1e-15, FPR 1) (BitsPerEntry 1, BitsPerEntry 75) 1e-6
         ++ test_calculations_classic
        , test_fromList     proxyClassic
        ]
    , testGroup "Blocked"
        [ testGroup "calculations" $
            -- For the Blocked impl, the calculations are approximations
            -- based on regressions. Since they are approximations then we have
            -- to use much looser tolerances. Also, the regression only covered
            -- the range of 2 bits to 24 bits, so we only cover that range here.
            -- And the precision at around 2 bits is poor, so we only look at 3
            -- bits and above.
            test_calculations proxyBlocked
              (FPR 1e-4, FPR 1e-1) (BitsPerEntry 3, BitsPerEntry 24) 1e-2
        , test_fromList     proxyBlocked
        , testProperty "prop_insertMany" prop_insertMany
        ]
    , tests_hashes
    ]
  where
    test_calculations proxy fprRrange bitsRange tolerance =
      [ testProperty "prop_calc_policy_fpr" $
          prop_calc_policy_fpr proxy fprRrange tolerance

      , testProperty "prop_calc_policy_bits" $
          prop_calc_policy_bits proxy bitsRange tolerance

      , testProperty "prop_calc_size_hashes_bits" $
          prop_calc_size_hashes_bits proxy
      ]

    -- These tests are only for the classic implementation because they use a
    -- test oracle ('falsePositiveRate') that is only appropriate for the
    -- classic implementation.
    test_calculations_classic =
      [ testProperty "prop_calc_size_fpr_fpr" $
          prop_calc_size_fpr_fpr proxyClassic

      , testProperty "prop_calc_size_fpr_bits" $
          prop_calc_size_fpr_bits proxyClassic
      ]

    test_fromList proxy =
      testGroup "fromList"
        [ testProperty "()"             $ prop_elem proxy (Proxy :: Proxy ())
        , testProperty "Char"           $ prop_elem proxy (Proxy :: Proxy Char)
        , testProperty "Word32"         $ prop_elem proxy (Proxy :: Proxy Word32)
        , testProperty "Word64"         $ prop_elem proxy (Proxy :: Proxy Word64)
        , testProperty "ByteString"     $ prop_elem proxy (Proxy :: Proxy ByteString)
        , testProperty "LBS.ByteString" $ prop_elem proxy (Proxy :: Proxy LBS.ByteString)
        , testProperty "String"         $ prop_elem proxy (Proxy :: Proxy String)
        ]

    tests_hashes =
      testGroup "hashes"
        [ testProperty "prop_rechunked_eq" prop_rechunked_eq
        , testProperty "prop_tuple_ex" $
          hash64 (BS.empty, BS.pack [120]) =/= hash64 (BS.pack [120], BS.empty)
        , testProperty "prop_list_ex" $
          hash64 [[],[],[BS.empty]] =/= hash64 [[],[BS.empty],[]]
        ]

proxyClassic :: Proxy Bloom.Classic.Bloom
proxyClassic = Proxy

proxyBlocked :: Proxy Bloom.Blocked.Bloom
proxyBlocked = Proxy

-------------------------------------------------------------------------------
-- Element is in a Bloom filter
-------------------------------------------------------------------------------

prop_elem :: forall bloom a. (BloomFilter bloom, Hashable a)
          => Proxy bloom -> Proxy a
          -> a -> [a] -> FPR -> Property
prop_elem proxy _ x xs (FPR q) =
    let bf :: bloom a
        bf = fromList (policyForFPR proxy q) (x:xs)
     in elem x bf .&&. not (notElem x bf)

-------------------------------------------------------------------------------
-- Bloom filter size calculations
-------------------------------------------------------------------------------

prop_calc_policy_fpr :: BloomFilter bloom => Proxy bloom
                     -> (FPR, FPR) -> Double
                     -> FPR -> Property
prop_calc_policy_fpr proxy (FPR lb, FPR ub) t (FPR fpr) =
  fpr > lb && fpr < ub ==>
  let policy = policyForFPR proxy fpr
   in policyFPR proxy policy ~~~ fpr
  where
    (~~~) = withinTolerance t

prop_calc_policy_bits :: BloomFilter bloom => Proxy bloom
                      -> (BitsPerEntry, BitsPerEntry) -> Double
                      -> BitsPerEntry -> Property
prop_calc_policy_bits proxy (BitsPerEntry lb, BitsPerEntry ub) t
                      (BitsPerEntry c) =
  c >= lb && c <= ub ==>
  let policy  = policyForBits proxy c
      c'      = B.policyBits policy
      fpr     = policyFPR proxy policy
      policy' = policyForFPR proxy fpr
      fpr'    = policyFPR proxy policy'
   in c === c' .&&. fpr ~~~ fpr'
  where
    (~~~) = withinTolerance t

-- | Compare @sizeHashes . sizeForBits@ against @numHashFunctions@
prop_calc_size_hashes_bits :: BloomFilter bloom => Proxy bloom
                           -> BitsPerEntry -> NumEntries -> Property
prop_calc_size_hashes_bits proxy (BitsPerEntry c) (NumEntries numEntries) =
  let bsize = sizeForBits proxy c numEntries
   in numHashFunctions (fromIntegral (B.sizeBits bsize))
                       (fromIntegral numEntries)
  === fromIntegral (B.sizeHashes bsize)

-- | Compare @sizeForFPR@ against @falsePositiveRate@ with some tolerance for deviations
prop_calc_size_fpr_fpr :: BloomFilter bloom => Proxy bloom
                       -> FPR -> NumEntries -> Property
prop_calc_size_fpr_fpr proxy (FPR fpr) (NumEntries numEntries) =
  let bsize = sizeForFPR proxy fpr numEntries
   in falsePositiveRate (fromIntegral (B.sizeBits bsize))
                        (fromIntegral numEntries)
                        (fromIntegral (B.sizeHashes bsize))
   ~~~ fpr
  where
    (~~~) = withinTolerance tolerance
    -- At small filter sizes (corresponding to high FPRs), we get significant
    -- reductions in accuracy due to rounding the number of bits to an integer.
    -- So we use greater tolerances for bigger FPRs.
    -- Contrast with prop_calc_policy_fpr which does not do rounding to an
    -- integer number of bits (it uses Double for bits per key), and thus can
    -- use a very small tolerance.
    tolerance | fpr <= 0.01 = 1e-6
              | fpr <= 0.05 = 1e-5
              | fpr <= 0.5  = 1e-4
              | otherwise   = 1e-3

-- | Compare @sizeForBits@ against @falsePositiveRate@ with some tolerance for deviations
prop_calc_size_fpr_bits :: BloomFilter bloom => Proxy bloom
                        -> BitsPerEntry -> NumEntries -> Property
prop_calc_size_fpr_bits proxy (BitsPerEntry bpe) (NumEntries numEntries) =
  let policy = policyForBits proxy bpe
      bsize  = sizeForPolicy proxy policy numEntries
   in falsePositiveRate (fromIntegral (B.sizeBits bsize))
                        (fromIntegral numEntries)
                        (fromIntegral (B.sizeHashes bsize))
   ~~~ policyFPR proxy policy
  where
    (~~~) = withinTolerance tolerance
    tolerance | bpe >= 18   = 1e-7
              | bpe >= 13   = 1e-6
              | bpe >= 8    = 1e-5
              | bpe >= 4    = 1e-4
              | otherwise   = 1e-3

-- reference implementations used for sanity checks

-- | Computes the optimal number of hash functions that minimises the false
-- positive rate for a bloom filter.
--
-- See <https://en.wikipedia.org/wiki/Bloom_filter#Optimal_number_of_hash_functions>
--
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

withinTolerance :: Double -> Double -> Double -> Property
withinTolerance t a b =
    counterexample (show a ++ " /= " ++ show b ++
                    " and not within (abs) tolerance of " ++ show t) $
      abs (a - b) < t

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
-- Bulk operations
-------------------------------------------------------------------------------

-- Currently only for Bloom.Blocked.
prop_insertMany :: FPR -> [Word64] -> Property
prop_insertMany (FPR fpr) keys =
     bloom_insert === bloom_insertMany
  where
    bloom_insert =
      Bloom.Blocked.create (Bloom.Blocked.sizeForFPR fpr n) $ \mb ->
        mapM_ (Bloom.Blocked.insert mb) keys

    bloom_insertMany =
      Bloom.Blocked.create (Bloom.Blocked.sizeForFPR fpr n) $ \mb ->
        Bloom.Blocked.insertMany mb (\k -> pure $ keys !! k) n

    !n = length keys

-------------------------------------------------------------------------------
-- Class to allow testing two filter implementations
-------------------------------------------------------------------------------

class BloomFilter bloom where
  fromList :: Hashable a => B.BloomPolicy -> [a] -> bloom a
  elem     :: Hashable a => a -> bloom a -> Bool
  notElem  :: Hashable a => a -> bloom a -> Bool

  sizeForFPR    :: Proxy bloom -> B.FPR          -> B.NumEntries -> B.BloomSize
  sizeForBits   :: Proxy bloom -> B.BitsPerEntry -> B.NumEntries -> B.BloomSize
  sizeForPolicy :: Proxy bloom -> B.BloomPolicy  -> B.NumEntries -> B.BloomSize
  policyForFPR  :: Proxy bloom -> B.FPR          -> B.BloomPolicy
  policyForBits :: Proxy bloom -> B.BitsPerEntry -> B.BloomPolicy
  policyFPR     :: Proxy bloom -> B.BloomPolicy -> B.FPR

instance BloomFilter Bloom.Classic.Bloom where
  fromList = Bloom.Classic.fromList
  elem     = Bloom.Classic.elem
  notElem  = Bloom.Classic.notElem

  sizeForFPR    _ = Bloom.Classic.sizeForFPR
  sizeForBits   _ = Bloom.Classic.sizeForBits
  sizeForPolicy _ = Bloom.Classic.sizeForPolicy
  policyForFPR  _ = Bloom.Classic.policyForFPR
  policyForBits _ = Bloom.Classic.policyForBits
  policyFPR     _ = Bloom.Classic.policyFPR

instance BloomFilter Bloom.Blocked.Bloom where
  fromList = Bloom.Blocked.fromList
  elem     = Bloom.Blocked.elem
  notElem  = Bloom.Blocked.notElem

  sizeForFPR    _ = Bloom.Blocked.sizeForFPR
  sizeForBits   _ = Bloom.Blocked.sizeForBits
  sizeForPolicy _ = Bloom.Blocked.sizeForPolicy
  policyForFPR  _ = Bloom.Blocked.policyForFPR
  policyForBits _ = Bloom.Blocked.policyForBits
  policyFPR     _ = Bloom.Blocked.policyFPR

-------------------------------------------------------------------------------
-- QC generators
-------------------------------------------------------------------------------

newtype FPR = FPR Double
  deriving stock Show

instance Arbitrary FPR where
  -- The most significant effect of the FPR is from its (negative) exponent,
  -- which influences both filter bits and number of hashes. So we generate
  -- values with an exponent from 10^0 to 10^-15
  arbitrary = do
      m <- choose (1, 9.99) -- not less than 1 or it's a different exponent
      e <- choose (1, 15)
      pure (FPR (m * 10 ** (-e)))

newtype BitsPerEntry = BitsPerEntry Double
  deriving stock Show

instance Arbitrary BitsPerEntry where
  arbitrary = BitsPerEntry <$> choose (1, 75)

newtype NumEntries = NumEntries Int
  deriving stock Show

-- | The FPR calculations are approximations and are not expected to be
-- accurate for low numbers of entries or bits.
--
instance Arbitrary NumEntries where
  arbitrary = NumEntries <$> choose (1_000, 100_000_000)
  shrink (NumEntries n) =
    [ NumEntries n' | n' <- shrink n, n' >= 1000 ]
