-- | /Optimal Bloom Filters and Adaptive Merging for LSM-Trees/
--
-- Calculations are for tiering merge policy.
--
-- Calculations are done assuming that initial parameters are:
--
-- * Total memory available for filters
--
-- * Merge policy parameters (size ratio T, and level count L)
--
-- * Total (maximum) count of elements
--
-- The last two parameters fix the buffer size.
--
module Monkey (
    constantBits,
    monkeyBits,
    -- * Utilities
    runsMultiplies,
    falsePositiveRate,
    totalMemory,
    zeroResultCost,
    nonZeroResultCost,
) where

import           Data.Bifunctor (bimap)
import           Numeric.AD (Mode, Scalar, auto, conjugateGradientDescent)

-- $setup
--
-- >>> :set -XGeneralizedNewtypeDeriving
-- >>> import Numeric (showFFloat)
-- >>> import Data.Bifunctor (bimap)
--
-- RDouble for truncated outputs
--
-- >>> newtype RDouble = RDouble Double deriving (Floating, Fractional, Num)
-- >>> instance Show RDouble where showsPrec _ (RDouble d)= showFFloat (Just 5) d

-- | "state of the art" i.e. use constant bits per element at all levels.
--
-- == Example
--
-- Let's see a simple example with 100k elements, 1M bits (i.e. 10 bits per element),
-- with a tiering setup with 3 levels and size multiplier of 4
--
-- >>> let t :: Num a => a; t = 4
-- >>> let l :: Num a => a; l = 3
--
-- >>> let ex = constantBits 1_000_000 100_000 t l
-- >>> ex
-- [(1587,15873),(6349,63492),(25396,253968)]
--
-- Note that there @3 * (1 + 4 + 16) = 63@ single-buffer sized runs: 3 (= 4 - 1) runs per each level
--
-- >>> runsMultiplies t l
-- 63
--
-- We can check that bits are divided between levels completely:
--
-- >>> sum $ map (\(_, bits) -> bits * (t - 1)) ex
-- 999999
--
-- For the rest of computations we need bits and entries are floating numbers
--
-- >>> let ex' :: Floating a => [(a,a)]; ex' = map (bimap fromInteger fromInteger) ex
--
-- The false positive rates are constant across all levels:
--
-- >>> let ps = map (uncurry (falsePositiveRate @RDouble)) ex'
-- >>> ps
-- [0.00819,0.00819,0.00819]
--
-- We can check again that the total memory is roughly the same we allocated initially
--
-- >>> totalMemory 100_000 t ps
-- 984413.39327
--
-- Zero-result lookup cost, 7% of the time we'll do one I/O
--
-- >>> zeroResultCost t ps
-- 0.07370
--
-- Non-zero lookup cost, 6.5% of the time we'll do an extra I/O
--
-- >>> nonZeroResultCost t ps
-- 1.06551
--
constantBits
    :: Integer -- ^ total memory (in bits)
    -> Integer -- ^ total elements
    -> Integer -- ^ T: size ratio
    -> Integer -- ^ L: level count
    -> [(Integer, Integer)] -- ^ elements and bits per run in a level
constantBits m_max n t l =
    [ (div (n * ti) k, div (m_max * ti) k)
    | i <- [1..l]
    , let ti = t ^ (i - 1)
    ]
  where
    k = runsMultiplies t l

-- | Monkey bits allocation.
--
-- === Notes
--
-- Currently this optimises for zero-result lookup.
--
-- If we would optimise 100% for non-zero lookup, then we should not have bloom filter for the last tier (at least in a setup with single run on the last level).
--
-- === Example
--
-- Let's compare to the same example as in 'constantBits':
-- 100k elements, 1M bits (i.e. 10 bits per element),
-- with a tiering setup with 3 levels and size multiplier of 4
--
-- >>> let t :: Num a => a; t = 4
-- >>> let l :: Num a => a; l = 3
--
-- >>> let ex = monkeyBits 1_000_000 100_000 t l
-- >>> ex
-- [(1587,23721),(6349,76578),(25396,233034)]
--
-- We have almost 1.5 more memory used in first level compared to 'constantBits' allocation.
--
-- We can check that bits are divided between levels completely:
--
-- >>> sum $ map (\(_, bits) -> bits * (t - 1)) ex
-- 999999
--
-- For the rest of computations we need bits and entries are floating numbers
--
-- >>> let ex' :: Floating a => [(a,a)]; ex' = map (bimap fromInteger fromInteger) ex
--
-- The false positive rates are constant across all levels:
--
-- >>> let ps = map (uncurry (falsePositiveRate @RDouble)) ex'
-- >>> ps
-- [0.00076,0.00304,0.01217]
--
-- compare that to to 'constantBits' @[0.00819,0.00819,0.00819]@ false-positive rates.
-- First and second level are more precise, but the last one is less.
--
-- We can check again that the total memory is roughly the same we allocated initially
--
-- >>> totalMemory 100_000 t ps
-- 984417.04432
--
-- Zero-result lookup cost, 5% (c.f. 7% with constant bits) of the time we'll do one I/O
--
-- >>> zeroResultCost t ps
-- 0.04793
--
-- Non-zero lookup cost, 3.5% (c.f 6.5% with constant bits) of the time we'll do an extra I/O
--
-- >>> nonZeroResultCost t ps
-- 1.03575
--
monkeyBits
    :: Integer -- ^ total memory (in bits)
    -> Integer -- ^ total elements
    -> Integer -- ^ T: size ratio
    -> Integer -- ^ L: level count
    -> [(Integer, Integer) ]-- ^ elements and bits per run in a level
monkeyBits m_max n t l
    = zipWith (\(entries, _) bits -> (entries, round bits)) initial floating
  where
    initial  = constantBits  m_max n t l
    initial_ = map (bimap fromInteger fromInteger) initial
    floating = monkeyImpl (fromInteger m_max) (fromInteger t) initial_

-- We optimise 'zeroResultCost' function given bits per each level,
-- with a constraint that total memory stays the same
--
-- We enforce the constraint using substitution:
-- bits_1 = total_memory - sum (bits_2 ... bits_L)
--
-- For now we use conjugate gradient descent instead of an analytical solution.
-- Iterative version is easier to tweak.
--
monkeyImpl
    :: Double
    -> Double
    -> [(Double,Double)]
    -> [Double]
monkeyImpl _ _ [] = []
monkeyImpl m_max t ((entries0, _bits0) : initial) =
    let rest = last $ take 100 $ conjugateGradientDescent target (map snd initial)
    in (m_max / (t-1) - sum rest) : rest
  where
    target :: (Floating a, Mode a, Scalar a ~ Double) => [a] -> a
    target ms = zeroResultCost (auto t) $ map (uncurry falsePositiveRate) $
        (auto entries0 , auto (m_max / (t-1)) - sum ms) :
        [ (auto entries, bits)
        | ((entries, _), bits) <- zip initial ms
        ]

-- | Calculate how many buffers we can fit given size ratio T and level count L.
runsMultiplies
    :: Integer -- ^ T: size ratio
    -> Integer -- ^ L: level count
    -> Integer
runsMultiplies t l = t ^ l -1

-- | False positive rate
--
-- Equation 2.
falsePositiveRate
    :: Floating a
    => a  -- ^ entries
    -> a  -- ^ bits
    -> a
falsePositiveRate entries bits = exp ((-(bits / entries)) * sq (log 2))

-- | Worst-Case Zero-result Lookup Cost (equation 3).
zeroResultCost
    :: Floating t
    => t -- ^ T: Size ratio
    -> [t] -- ^ \(p_i)\
    -> t
zeroResultCost t ps =
    (t - 1) * sum ps

-- | Worst-Case Non-Zero-Result Lookup Cost (equation 9).
--
-- This applies only for non-monoidal lookups.
-- In monoidal setting we might need to read more than one run, if initial lookups are upserts.
-- (We need another parameter: a ratio of inserts and upserts).
--
nonZeroResultCost
    :: Floating t
    => t -- ^ T: Size ratio
    -> [t] -- ^ \(p_i\)
    -> t
nonZeroResultCost t ps =
    zeroResultCost t ps - last ps + 1

-- | Main memory footprint (equation 4).
--
totalMemory
    :: Floating t
    => t -- ^ N: total entries
    -> t -- ^ T: size ratio
    -> [t]     -- ^ \(p_i)\
    -> t
totalMemory n t ps =
    negate n / sq (log 2) * (t - 1) / t *
        sum
            [ log p_i / (t ^ (l - i))
            | (i, p_i) <- zip [1..] ps
            ]
  where
    l = length ps

sq :: Num a => a -> a
sq x = x * x
