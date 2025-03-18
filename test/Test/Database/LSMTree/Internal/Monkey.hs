{-# LANGUAGE BangPatterns               #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE InstanceSigs               #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE NumericUnderscores         #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE TypeApplications           #-}
{-# OPTIONS_GHC -Wno-orphans #-}
{- HLINT ignore "Use camelCase" -}

module Test.Database.LSMTree.Internal.Monkey (
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
import           Data.BloomFilter (Bloom)
import qualified Data.BloomFilter as Bloom
import           Data.BloomFilter.Hash (Hashable)
import qualified Data.BloomFilter.Mutable as MBloom
import           Data.Foldable (Foldable (..))
import           Data.Proxy (Proxy (..))
import           Data.Set (Set)
import qualified Data.Set as Set
import           Data.Word (Word64)
import           Database.LSMTree.Extras.Random
import qualified Database.LSMTree.Internal.Entry as LSMT
import           Database.LSMTree.Internal.RunAcc (RunBloomFilterAlloc (..),
                     falsePositiveRate, newMBloom)
import           System.Random
import           Test.QuickCheck
import           Test.QuickCheck.Gen
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.QuickCheck
import           Test.Util.Arbitrary (noTags,
                     prop_arbitraryAndShrinkPreserveInvariant)
import           Text.Printf (printf)

tests :: TestTree
tests = testGroup "Database.LSMTree.Internal.Monkey" [
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
        RunAllocFixed bits ->
          falsePositiveRate (fromIntegral numEntries)
                            (fromIntegral bits * fromIntegral numEntries)
        RunAllocRequestFPR requestedFPR -> requestedFPR
      -- error margins
      lb = expectedFPR - 0.1
      ub = expectedFPR + 0.03
  in  assert (fprInvariant True measuredFPR) $ -- measured FPR is in the range [0,1]
      assert (fprInvariant True expectedFPR) $ -- expected FPR is in the range [0,1]
      counterexample (printf "expected $f <= %f <= %f" lb measuredFPR ub) $
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
allocInvariant (RunAllocRequestFPR x) = fprInvariant False x

genFixed :: Gen Word64
genFixed = choose (fixedLB, fixedUB)

shrinkFixed :: Word64 -> [Word64]
shrinkFixed x = [ x' | x' <- shrink x, fixedInvariant x']

fixedInvariant :: Word64 -> Bool
fixedInvariant x = fixedLB <= x && x <= fixedUB

fixedLB :: Word64
fixedLB = 0

fixedUB :: Word64
fixedUB = 20

genFPR :: Gen Double
genFPR = genDouble `suchThat` fprInvariant False

shrinkFPR :: Double -> [Double]
shrinkFPR x = [ x' | x' <- shrink x, fprInvariant False x']

fprInvariant :: Bool -> Double -> Bool
fprInvariant incl x
  | incl      = 0 <= x && x <= 1
  | otherwise = 0 < x && x < 1

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
    mapM_ (MBloom.insert mb) xs
    Bloom.unsafeFreeze mb
  where
    n = LSMT.NumEntries $ length xs
