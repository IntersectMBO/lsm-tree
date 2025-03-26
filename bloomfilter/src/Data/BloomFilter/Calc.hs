-- | Various formulas for working with bloomfilters.
module Data.BloomFilter.Calc (
    BloomSize (..),
    bloomSizeForPolicy,
    BloomPolicy (..),
    bloomPolicyFPR,
    bloomPolicyForFPR,
    bloomPolicyForBitsPerEntry,
) where

import           Numeric

type FPR          = Double
type BitsPerEntry = Double
type NumEntries   = Int

-- | A policy on intended bloom filter size -- independent of the number of
-- elements.
--
-- We can decide a policy based on:
--
-- 1. a target false positive rate (FPR) using 'bloomPolicyForFPR'
-- 2. a number of bits per entry using 'bloomPolicyForBitsPerEntry'
--
-- A policy can be turned into a 'BloomSize' given a target 'NumEntries' using
-- 'bloomSizeForPolicy'.
--
-- Either way we define the policy, we can inspect the result to see:
--
-- 1. The bits per entry 'bloomPolicyBitsPerEntry'. This will determine the
--    size of the bloom filter in bits. In general the bits per entry can be
--    fractional. The final bloom filter size in will be rounded to a whole
--    number of bits.
-- 2. the number of hashes 'bloomPolicyNumHashes'.
--
data BloomPolicy = BloomPolicy {
       bloomPolicyBitsPerEntry :: !Double,
       bloomPolicyNumHashes    :: !Int
     }
  deriving Show

bloomPolicyForFPR :: FPR -> BloomPolicy
bloomPolicyForFPR fpr | fpr <= 0 || fpr >= 1 =
    error "bloomPolicyForFPR: fpr out of range (0,1)"

bloomPolicyForFPR fpr =
    BloomPolicy {
      bloomPolicyBitsPerEntry = c,
      bloomPolicyNumHashes    = k
    }
  where
    -- There's a simper fomula to compute the number of bits, but it assumes
    -- that k is a real. We must however round k to the nearest natural, and
    -- so we have to use a more precise approximation, using the actual value
    -- of k.
    k       :: Int; k' :: Double
    k       = max 1 (round ((-recip_log2) * log_fpr))
    k'      = fromIntegral k
    c       = negate k' / log1mexp (log_fpr / k')
    log_fpr = log fpr

bloomPolicyForBitsPerEntry :: BitsPerEntry -> BloomPolicy
bloomPolicyForBitsPerEntry c | c < 1 || c > 64 =
    error "bloomPolicyForBitsPerEntry: out of ragnge [1,64]"

bloomPolicyForBitsPerEntry c =
    BloomPolicy {
      bloomPolicyBitsPerEntry = c,
      bloomPolicyNumHashes    = k
    }
  where
    k = max 1 (round (c * log2))

bloomPolicyFPR :: BloomPolicy -> FPR
bloomPolicyFPR BloomPolicy {
                 bloomPolicyBitsPerEntry = c,
                 bloomPolicyNumHashes    = k
               } =
    negate (expm1 (negate (k' / c))) ** k'
  where
    k' = fromIntegral k

-- | Parameters for constructing a Bloom filter.
--
data BloomSize = BloomSize {
                   -- | The requested number of bits in filter.
                   -- The actual size will be rounded up to the nearest 512.
                   bloomNumBits   :: !Int,

                   -- | The number of hash functions to use.
                   bloomNumHashes :: !Int
                 }
  deriving Show

bloomSizeForPolicy :: BloomPolicy -> NumEntries -> BloomSize
bloomSizeForPolicy BloomPolicy {
                     bloomPolicyBitsPerEntry = c,
                     bloomPolicyNumHashes    = k
                   } n =
    BloomSize {
      bloomNumBits   = max 0 (ceiling (fromIntegral n * c)),
      bloomNumHashes = max 1 k
    }

log2, recip_log2 :: Double
log2       = log 2
recip_log2 = recip log2
