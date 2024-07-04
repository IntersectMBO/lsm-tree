{-# LANGUAGE BangPatterns               #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE InstanceSigs               #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE NumericUnderscores         #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE TypeApplications           #-}
{- HLINT ignore "Use camelCase" -}

module Test.Database.LSMTree.Internal.Monkey (
    -- * Main test tree
    tests
    -- * Bloom filter construction
    --
    -- A common interface to bloom filter construction, based on expected false
    -- positive rates.
  , BloomMaker
  , mkBloomST
  , mkBloomST_Monkey
  , mkBloomEasy
    -- * Verifying FPRs
  , measureApproximateFPR
  , measureExactFPR
  ) where

import           Control.Exception (assert)
import           Control.Monad.ST
import           Data.BloomFilter (Bloom)
import qualified Data.BloomFilter as Bloom
import qualified Data.BloomFilter.Easy as Bloom.Easy
import           Data.BloomFilter.Hash (Hashable)
import qualified Data.BloomFilter.Mutable as MBloom
import           Data.Foldable (Foldable (..))
import           Data.Proxy (Proxy (..))
import           Data.Set (Set)
import qualified Data.Set as Set
import           Data.Word (Word64)
import           Database.LSMTree.Extras.Random
import qualified Database.LSMTree.Internal.Monkey as Monkey
import           System.Random
import           Test.QuickCheck
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.QuickCheck (testProperty)
import           Text.Printf (printf)

tests :: TestTree
tests = testGroup "Database.LSMTree.Internal.Monkey" [
      testGroup "No false negatives" [
        testProperty "mkBloomEasy"      $ prop_noFalseNegatives (Proxy @Word64) mkBloomEasy
      , testProperty "mkBloomST"        $ prop_noFalseNegatives (Proxy @Word64) mkBloomST
      , testProperty "mkBloomST_Monkey" $ prop_noFalseNegatives (Proxy @Word64) mkBloomST_Monkey
      ]
    , testGroup "Verify FPR" [
          testProperty "mkBloomEasy"      $ prop_verifyFPR (Proxy @Word64) mkBloomEasy
        , testProperty "mkBloomST"        $ prop_verifyFPR (Proxy @Word64) mkBloomST
        , testProperty "mkBloomST_Monkey" $ expectFailure -- TODO: see 'mkBloomST_Monkey'.
                                          $ prop_verifyFPR (Proxy @Word64) mkBloomST_Monkey
        ]
    ]

{-------------------------------------------------------------------------------
  Properties
-------------------------------------------------------------------------------}

prop_noFalseNegatives :: forall a proxy. Hashable a
  => proxy a
  -> (Double -> BloomMaker a)
  -> FPR                      -- ^ Requested FPR
  -> UniformWithoutReplacement a
  -> Property
prop_noFalseNegatives _ mkBloom (FPR requestedFPR) (UniformWithoutReplacement xs) =
    let xsBloom = mkBloom requestedFPR xs
    in  property $ all (`Bloom.elem` xsBloom) xs

prop_verifyFPR ::
     (Ord a, Uniform a, Hashable a)
  => proxy a
  -> (Double -> BloomMaker a)
  -> FPR                      -- ^ Requested FPR
  -> NumEntries               -- ^ @numEntries@
  -> Seed                     -- ^ 'StdGen' seed
  -> Property
prop_verifyFPR p mkBloom (FPR requestedFPR) (NumEntries numEntries) (Seed seed) =
  let stdgen      = mkStdGen seed
      measuredFPR = measureApproximateFPR p (mkBloom requestedFPR) numEntries stdgen
      requestedFPR' = requestedFPR + 0.03 -- @requestedFPR@ with an error margin
  in  counterexample (printf "expected %f <= %f" measuredFPR requestedFPR') $
      FPR measuredFPR <= FPR requestedFPR'

{-------------------------------------------------------------------------------
  Modifiers
-------------------------------------------------------------------------------}

--
-- FPR
--

newtype FPR = FPR { getFPR :: Double }
  deriving stock (Show, Eq, Ord)
  deriving newtype (Num, Fractional, Floating)

instance Arbitrary FPR where
  arbitrary = FPR <$> arbitrary `suchThat` fprInvariant
  shrink (FPR x) = [FPR x' | x' <- shrink x, fprInvariant x']

fprInvariant :: Double -> Bool
fprInvariant x = x >= 0.01 && x <= 0.99

--
-- NumEntries
--

newtype NumEntries = NumEntries { getNumEntries :: Int }
  deriving stock Show

instance Arbitrary NumEntries where
  arbitrary = NumEntries <$> chooseInt (numEntriesLB, numEntriesUB)
  shrink (NumEntries x) = [NumEntries x' | x' <- shrink x, numEntriesInvariant x']

numEntriesLB :: Int
numEntriesLB = 10_000

numEntriesUB :: Int
numEntriesUB = 100_000

numEntriesInvariant :: Int -> Bool
numEntriesInvariant x = x >= numEntriesLB && x <= numEntriesUB

--
-- Seed
--

newtype Seed = Seed { getSeed :: Int }
  deriving stock Show

instance Arbitrary Seed where
  arbitrary = Seed <$> arbitraryBoundedIntegral
  shrink (Seed x) = Seed <$> shrink x

--
-- UniformWithoutReplacement
--

newtype UniformWithoutReplacement a = UniformWithoutReplacement [a]

instance Show (UniformWithoutReplacement a) where
  show (UniformWithoutReplacement xs) = "UniformWithoutReplacement " <> show (length xs)

instance (Ord a, Uniform a) => Arbitrary (UniformWithoutReplacement a) where
  arbitrary = do
    stdgen <- mkStdGen . getSeed <$> arbitrary
    numEntries <- getNumEntries <$> arbitrary
    pure $ UniformWithoutReplacement $ uniformWithoutReplacement stdgen numEntries

{-------------------------------------------------------------------------------
  Verifying FPRs
-------------------------------------------------------------------------------}

-- | Measure the /approximate/ FPR for a bloom filter.
--
-- Ensure that @a@ is large enough to draw @2 * numEntries@ uniformly random
-- values, or the computation will get stuck.
--
-- REF: based on https://stackoverflow.com/questions/74999807/how-to-measure-the-rate-of-false-positives-in-a-bloom-filter
--
-- REF: https://en.wikipedia.org/wiki/False_positive_rate
measureApproximateFPR ::
     forall a proxy. (Ord a, Uniform a, Hashable a)
  => proxy a      -- ^ The types of values to generate.
  -> BloomMaker a -- ^ How to construct the bloom filter.
  -> Int          -- ^ @numEntries@: number of entries to put into the bloom filter.
  -> StdGen
  -> Double
measureApproximateFPR _ mkBloom numEntries stdgen =
    let !xs         = uniformWithoutReplacement @a stdgen (2 * numEntries)
        (!ys, !zs)  = splitAt numEntries xs
        !ysBloom    = mkBloom ys
        !ysSet      = Set.fromList ys
        oneIfElem z = assert (not $ Set.member z ysSet)
                        $ if Bloom.elem z ysBloom then 1 else 0
        !fp         = foldl' (\acc x -> acc + oneIfElem x) (0 :: Int) zs
        !fp'        = fromIntegral fp :: Double
    in  fp' / fromIntegral numEntries -- FPR = FP / FP + TN

-- | Measure the /exact/ FPR for a bloom filter.
--
-- Ensure that @a@ is small enough that we can enumare it within reasonable
-- time. For example, a 'Word16' would be fine, but a 'Word32' would take much
-- too long.
measureExactFPR ::
     forall a proxy. (Ord a, Enum a, Bounded a, Uniform a, Hashable a)
  => proxy a      -- ^ The types of values to generate.
  -> BloomMaker a -- ^ How to construct the bloom filter.
  -> Int          -- ^ @numEntries@: number of entries to put into the bloom filter.
  -> StdGen
  -> Double
measureExactFPR _ mkBloom numEntries stdgen  =
    let !xs                = uniformWithoutReplacement @a stdgen numEntries
        !xsBloom           = mkBloom xs
        !xsSet             = Set.fromList xs
        !aEnumerated       = [minBound .. maxBound]
        Counts _ !fp !tn _ = foldMap' (fromTest . analyse xsBloom xsSet) aEnumerated
        fp'                = fromIntegral fp :: Double
        tn'                = fromIntegral tn :: Double
    in  fp' / (fp' + tn') -- FPR = FP / FP + TN

data Test =
    TruePositive
  | FalsePositive
  | TrueNegative
  | FalseNegative

analyse :: (Ord a, Hashable a) => Bloom a -> Set a -> a -> Test
analyse xsBloom xsSet y
    |     isBloomMember &&     isTrueMember = TruePositive
    |     isBloomMember && not isTrueMember = FalsePositive
    | not isBloomMember && not isTrueMember = TrueNegative
    | otherwise                             = FalseNegative
  where
    isBloomMember = Bloom.elem y xsBloom
    isTrueMember  = Set.member y xsSet

fromTest :: Test -> Counts
fromTest = \case
    TruePositive  -> Counts 1 0 0 0
    FalsePositive -> Counts 0 1 0 0
    TrueNegative  -> Counts 0 0 1 0
    FalseNegative -> Counts 0 0 0 1


data Counts = Counts {
    _cTruePositives  :: !Int
  , _cFalsePositives :: !Int
  , _cTrueNegatives  :: !Int
  , _cFalseNegatives :: !Int
  }

instance Semigroup Counts where
  (<>) :: Counts -> Counts -> Counts
  (Counts tp1 fp1 tn1 fn1) <> (Counts tp2 fp2 tn2 fn2) =
    Counts (tp1 + tp2) (fp1 + fp2) (tn1 + tn2) (fn1 + fn2)

instance Monoid Counts where
  mempty :: Counts
  mempty = Counts 0 0 0 0

{-------------------------------------------------------------------------------
  Bloom filter construction
-------------------------------------------------------------------------------}

type BloomMaker a = [a] -> Bloom a

-- | Create a bloom filter through the 'MBloom' interface. Tunes the bloom
-- filter using 'suggestSizing'.
mkBloomST :: Hashable a => Double -> BloomMaker a
mkBloomST requestedFPR xs = runST $ do
    b <- MBloom.new numHashFuncs numBits
    mapM_ (MBloom.insert b) xs
    Bloom.freeze b
  where
    numEntries              = length xs
    (numBits, numHashFuncs) = Bloom.Easy.suggestSizing numEntries requestedFPR

-- | Create a bloom filter through the 'MBloom' interface. Tunes the bloom
-- filter a la Monkey.
--
-- === TODO
--
-- The measured FPR exceeds the requested FPR by a number of percentages.
-- Example: @withNewStdGen $ measureApproximateFPR (Proxy @Word64) (mkBloomST'
-- 0.37) 1000000@. I'm unsure why, but I have a number of ideas
--
-- * The FPR (and bits/hash functions) calculations are approximations.
-- * Rounding errors in the Haskell implementation of FPR calculations
-- * The Monkey tuning is incompatible with @bloomfilter@'s /next power of 2/
--   rounding of th ebits.
mkBloomST_Monkey :: Hashable a => Double -> BloomMaker a
mkBloomST_Monkey requestedFPR xs = runST $ do
    b <- MBloom.new numHashFuncs (fromIntegral numBits)
    mapM_ (MBloom.insert b) xs
    Bloom.freeze b
  where
    numEntries   = length xs
    numBits      = Monkey.monkeyBits numEntries requestedFPR
    numHashFuncs = Monkey.monkeyHashFuncs numBits numEntries

-- | Create a bloom filter through the "Data.BloomFilter.Easy" interface. Tunes
-- the bloom filter using 'suggestSizing'.
mkBloomEasy :: Hashable a => Double -> BloomMaker a
mkBloomEasy = Bloom.Easy.easyList
