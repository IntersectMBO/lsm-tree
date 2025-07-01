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
  deriving stock Show

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
    k       = max 1 (round ((-recip_log2) * log_fpr))
    k'      = fromIntegral k
    c       = negate k' / log1mexp (log_fpr / k')
    log_fpr = log fpr
    -- For the source of this formula, see
    -- https://en.wikipedia.org/wiki/Bloom_filter#Probability_of_false_positives
    --
    -- We start with the FPR ε approximation that assumes independence for the
    -- probabilities of each bit being set.
    --
    --                         ε   = (1 - e^(-kn/m))^k
    --
    -- And noting that bits per entry @c = m/n@, hence @-kn/m = -k/c@, hence
    --
    --                         ε   = (1-e^(-k/c))^k
    --
    -- And then we rearrange to get c, the number of bits per entry:
    --
    --                            ε   =  (1-e^(-k/c))^k
    --                            ε   =  (1-exp (-k/c))^k
    --                            ε   =  exp (log (1 - exp (-k/c)) * k)
    --                        log ε   =  log (1 - exp (-k/c)) * k
    --                    log ε / k   =  log (1 - exp (-k/c))
    --               exp (log ε / k)  =  1 - exp (-k/c)
    --           1 - exp (log ε / k)  =  exp (-k/c)
    --      log (1 - exp (log ε / k)) =  -k/c
    -- -k / log (1 - exp (log ε / k)) =  c
    --     -k / log1mexp (log ε / k)  =  c

policyForBits :: BitsPerEntry -> BloomPolicy
policyForBits c | c <= 0 =
    error "policyForBits: bits per entry must be > 0"

policyForBits c =
    BloomPolicy {
      policyBits   = c,
      policyHashes = k
    }
  where
    k = max 1 (round (c * log2))
    -- For the source of this formula, see
    -- https://en.wikipedia.org/wiki/Bloom_filter#Optimal_number_of_hash_functions

policyFPR :: BloomPolicy -> FPR
policyFPR BloomPolicy {
            policyBits   = c,
            policyHashes = k
          } =
    negate (expm1 (negate (k' / c))) ** k'
  where
    k' = fromIntegral k
    -- For the source of this formula, see
    -- https://en.wikipedia.org/wiki/Bloom_filter#Probability_of_false_positives
    --
    -- We use the FPR ε approximation that assumes independence for the
    -- probabilities of each bit being set.
    --
    --                         ε   = (1 - e^(-kn/m))^k
    --
    -- And noting that bits per entry @c = m/n@, hence @-kn/m = -k/c@, hence
    --
    --                         ε   = (1-e^(-k/c))^k
    --

-- | Parameters for constructing a Bloom filter.
--
data BloomSize = BloomSize {
                   -- | The requested number of bits in the filter.
                   sizeBits   :: !Int,

                   -- | The number of hash functions to use.
                   sizeHashes :: !Int
                 }
  deriving stock Show

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
