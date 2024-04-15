-- | Various formulas for working with bloomfilters.
module Data.BloomFilter.Calc (
    falsePositiveProb,
    filterSize,
) where

import           Numeric (expm1)

-- $setup
-- >>> import Numeric (showFFloat)

-- | Approximate probability of false positives
-- \[
-- {\displaystyle \varepsilon =\left(1-\left[1-{\frac {1}{m}}\right]^{kn}\right)^{k}\approx \left(1-e^{-kn/m}\right)^{k}}
-- \]
--
-- >>> [ showFFloat (Just 5) (falsePositiveProb 10_000 100_000 k) "" | k <- [1..5] ]
-- ["0.09516","0.03286","0.01741","0.01181","0.00943"]
--
falsePositiveProb
    :: Double  -- ^ /n/, number of elements
    -> Double  -- ^ /m/, size of bloom filter
    -> Double  -- ^ /k/, number of hash functions
    -> Double
falsePositiveProb n m k =
    -- (1 - (1 - recip m) ** (k * n)) ** k
    negate (expm1 (negate (k * n / m))) ** k

-- | Filter size for given number of elements, false positive rate and
-- number of hash functions.
filterSize
    :: Double  -- ^ /n/, number of elements
    -> Double  -- ^ /e/, false positive rate
    -> Double  -- ^ /k/, number of hash functions
    -> Double
filterSize n e k  =
    -- recip (1 - (1 - e ** recip k) ** recip (k * n))
    negate k * n / log (1 - e ** recip k)
