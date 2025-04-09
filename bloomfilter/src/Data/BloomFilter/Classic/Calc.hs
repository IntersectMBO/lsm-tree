-- | Various formulas for working with bloomfilters.
module Data.BloomFilter.Classic.Calc (
    NumEntries,
    BloomSize (..),
    FPR,
    sizeForFPR,
    BitsPerEntry,
    sizeForBits,
    sizeForPolicy,
    BloomPolicy (..),
    policyFPR,
    policyForFPR,
    policyForBits,
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
-- 1. a target false positive rate (FPR) using 'policyForFPR'
-- 2. a number of bits per entry using 'policyForBits'
--
-- A policy can be turned into a 'BloomSize' given a target 'NumEntries' using
-- 'sizeForPolicy'.
--
-- Either way we define the policy, we can inspect the result to see:
--
-- 1. The bits per entry 'policyBits'. This will determine the
--    size of the bloom filter in bits. In general the bits per entry can be
--    fractional. The final bloom filter size in will be rounded to a whole
--    number of bits.
-- 2. The number of hashes 'policyHashes'.
-- 3. The expected FPR for the policy using 'policyFPR'.
--
data BloomPolicy = BloomPolicy {
       policyBits   :: !Double,
       policyHashes :: !Int
     }
  deriving Show

policyForFPR :: FPR -> BloomPolicy
policyForFPR fpr | fpr <= 0 || fpr >= 1 =
    error "bloomPolicyForFPR: fpr out of range (0,1)"

policyForFPR fpr =
    BloomPolicy {
      policyBits   = c,
      policyHashes = k
    }
  where
    -- There's a simper fomula to compute the number of bits, but it assumes
    -- that k is a real. We must however round k to the nearest natural, and
    -- so we have to use a more precise approximation, using the actual value
    -- of k.
    k       :: Int; k' :: Double
    k       = min 64 (max 1 (round ((-recip_log2) * log_fpr)))
    k'      = fromIntegral k
    c       = negate k' / log1mexp (log_fpr / k')
    log_fpr = log fpr

policyForBits :: BitsPerEntry -> BloomPolicy
policyForBits c | c < 1 || c > 64 =
    error "policyForBits: out of ragnge [1,64]"

policyForBits c =
    BloomPolicy {
      policyBits   = c,
      policyHashes = k
    }
  where
    k = max 1 (round (c * log2))

policyFPR :: BloomPolicy -> FPR
policyFPR BloomPolicy {
            policyBits   = c,
            policyHashes = k
          } =
    negate (expm1 (negate (k' / c))) ** k'
  where
    k' = fromIntegral k

-- | Parameters for constructing a Bloom filter.
--
data BloomSize = BloomSize {
                   -- | The requested number of bits in filter.
                   -- The actual size will be rounded up to the nearest 512.
                   sizeBits   :: !Int,

                   -- | The number of hash functions to use.
                   sizeHashes :: !Int
                 }
  deriving Show

sizeForFPR :: FPR -> NumEntries -> BloomSize
sizeForFPR = sizeForPolicy . policyForFPR

sizeForBits :: BitsPerEntry -> NumEntries -> BloomSize
sizeForBits = sizeForPolicy . policyForBits

sizeForPolicy :: BloomPolicy -> NumEntries -> BloomSize
sizeForPolicy BloomPolicy {
                policyBits   = c,
                policyHashes = k
              } n =
    BloomSize {
      sizeBits   = max 1 (ceiling (fromIntegral n * c)),
      sizeHashes = max 1 k
    }

log2, recip_log2 :: Double
log2       = log 2
recip_log2 = recip log2
