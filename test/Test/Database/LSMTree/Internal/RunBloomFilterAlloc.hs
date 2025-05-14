{-# LANGUAGE BangPatterns               #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE InstanceSigs               #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE NumericUnderscores         #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE TypeApplications           #-}

{-# OPTIONS_GHC -Wno-orphans #-}

module Test.Database.LSMTree.Internal.RunBloomFilterAlloc (
    -- * Main test tree
    tests
    -- * Bloom filter construction
    --
    -- A common interface to bloom filter construction, based on expected false
    -- positive rates.
  , BloomMaker
  , mkBloomFromAlloc
    -- * Verifying FPRs
  , measureApproximateFPR
  , measureExactFPR
  ) where

import           Control.Exception (assert)
import           Control.Monad.ST
import           Data.BloomFilter.Blocked (Bloom)
import qualified Data.BloomFilter.Blocked as Bloom
import           Data.BloomFilter.Hash (Hashable)
import           Data.Foldable (Foldable (..))
import           Data.Proxy (Proxy (..))
import           Data.Set (Set)
import qualified Data.Set as Set
import           Data.Word (Word64)
import           Database.LSMTree.Extras.Random
import qualified Database.LSMTree.Internal.Entry as LSMT
import           Database.LSMTree.Internal.RunAcc (RunBloomFilterAlloc (..),
                     newMBloom)
import           System.Random hiding (Seed)
import           Test.QuickCheck
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.QuickCheck
import           Test.Util.Arbitrary (noTags,
                     prop_arbitraryAndShrinkPreserveInvariant)
import           Text.Printf (printf)

tests :: TestTree
tests = testGroup "Database.LSMTree.Internal.RunBloomFilterAlloc" [
      testProperty "prop_noFalseNegatives" $ prop_noFalseNegatives (Proxy @Word64)
    , testProperty "prop_verifyFPR" $ prop_verifyFPR (Proxy @Word64)
    , testGroup "RunBloomFilterAlloc" $
        prop_arbitraryAndShrinkPreserveInvariant noTags allocInvariant
    , testGroup "NumEntries" $
        prop_arbitraryAndShrinkPreserveInvariant noTags numEntriesInvariant
    ]

{-------------------------------------------------------------------------------
  Properties
-------------------------------------------------------------------------------}

prop_noFalseNegatives :: forall a proxy. Hashable a
  => proxy a
  -> RunBloomFilterAlloc
  -> UniformWithoutReplacement a
  -> Property
prop_noFalseNegatives _ alloc (UniformWithoutReplacement xs) =
    let xsBloom = mkBloomFromAlloc alloc xs
    in  property $ all (`Bloom.elem` xsBloom) xs

prop_verifyFPR ::
     (Ord a, Uniform a, Hashable a)
  => proxy a
  -> RunBloomFilterAlloc
  -> NumEntries               -- ^ @numEntries@
  -> Seed                     -- ^ 'StdGen' seed
  -> Property
prop_verifyFPR p alloc (NumEntries numEntries) (Seed seed) =
  let stdgen      = mkStdGen seed
      measuredFPR = measureApproximateFPR p (mkBloomFromAlloc alloc) numEntries stdgen
      expectedFPR = case alloc of
        RunAllocFixed bits -> Bloom.policyFPR (Bloom.policyForBits bits)
        RunAllocRequestFPR requestedFPR -> requestedFPR
      -- error margins
      lb = expectedFPR - 0.1
      ub = expectedFPR + 0.03
  in  counterexample (printf "expected %f <= %f <= %f" lb measuredFPR ub) $
      lb <= measuredFPR .&&. measuredFPR <= ub

{-------------------------------------------------------------------------------
  Modifiers
-------------------------------------------------------------------------------}

--
-- Alloc
--

instance Arbitrary RunBloomFilterAlloc where
  arbitrary = oneof [
        RunAllocFixed <$> genFixed
      , RunAllocRequestFPR <$> genFPR
      ]
  shrink (RunAllocFixed x)      = RunAllocFixed <$> shrinkFixed x
  shrink (RunAllocRequestFPR x) = RunAllocRequestFPR <$> shrinkFPR x

allocInvariant :: RunBloomFilterAlloc -> Bool
allocInvariant (RunAllocFixed x)      = fixedInvariant x
allocInvariant (RunAllocRequestFPR x) = fprInvariant x

genFixed :: Gen Double
genFixed = choose (fixedLB, fixedUB)

shrinkFixed :: Double -> [Double]
shrinkFixed x = [ x' | x' <- shrink x, fixedInvariant x']

fixedInvariant :: Double -> Bool
fixedInvariant x = fixedLB <= x && x <= fixedUB

fixedLB :: Double
fixedLB = 3 -- bits per entry

fixedUB :: Double
fixedUB = 24 -- bits per entry

genFPR :: Gen Double
genFPR = do m <- choose (1, 9.99) -- not less than 1 or it's a different exponent
            e <- choose (fpr_exponentLB, fpr_exponentUB)
            pure (m * 10 ^^ e)
        `suchThat` fprInvariant

fpr_exponentLB :: Int
fpr_exponentLB = -5 -- 1 in 10,000

fpr_exponentUB :: Int
fpr_exponentUB = -1 -- 1 in 10

shrinkFPR :: Double -> [Double]
shrinkFPR x = [ x' | x' <- shrink x, fprInvariant x']

-- | The FPR calculations are only accurate over the range 0.25 down to 0.00006
-- which corresponds to bits in the range 3 .. 24.
fprInvariant :: Double -> Bool
fprInvariant x = 6e-5 < x && x < 2.5e-1

--
-- NumEntries
--

newtype NumEntries = NumEntries { getNumEntries :: Int }
  deriving stock Show

instance Arbitrary NumEntries where
  arbitrary = NumEntries <$> chooseInt (numEntriesLB, numEntriesUB)
  shrink (NumEntries x) = [
        x''
      | x' <- shrink x
      , let x'' = NumEntries x'
      , numEntriesInvariant x''
      ]

numEntriesLB :: Int
numEntriesLB = 50_000

numEntriesUB :: Int
numEntriesUB = 100_000

numEntriesInvariant :: NumEntries -> Bool
numEntriesInvariant (NumEntries x) = x >= numEntriesLB && x <= numEntriesUB

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

-- | Create a bloom filter through the 'newMBloom' interface. Tunes the bloom
-- filter according to 'RunBloomFilterAlloc'.
mkBloomFromAlloc :: Hashable a => RunBloomFilterAlloc -> BloomMaker a
mkBloomFromAlloc alloc xs = runST $ do
    mb <- newMBloom n alloc
    mapM_ (Bloom.insert mb) xs
    Bloom.unsafeFreeze mb
  where
    n = LSMT.NumEntries $ length xs
