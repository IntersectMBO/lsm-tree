-- | Various formulas for working with bloomfilters.
module Data.BloomFilter.Blocked.Calc (
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

import           Data.BloomFilter.Classic.Calc (BitsPerEntry, BloomPolicy (..),
                     BloomSize (..), FPR, NumEntries)

{-
Calculating the relationship between bits and FPR for the blocked
implementation:

While in principle there's a principled approach to this, it's complex to
calculate numerically. So instead we compute a regression from samples of bits
& FPR. The fpr-calc.hs program in this package does this for a range of bits,
and outputs out both graph data (to feed into gnuplot) and it also a regression
fit. The exact fit one gets depends on the PRNG seed used.

We calculate the regression two ways, one for FPR -> bits, and bits -> FPR.
We use a quadratic regression, with the FPR in log space.

The following is the sample of the regression fit output that we end up using
in the functions 'policyForFPR' and 'policyForBits'.

Blocked bloom filter quadratic regressions:
bits independent, FPR dependent:
Fit {
  fitParams = V3 (-5.03623760876204e-3) 0.5251544487138062 (-0.10110451821280719),
  fitErrors = V3 3.344945010267228e-5 8.905631581753235e-4 5.102181306816477e-3,
  fitNDF = 996, fitWSSR = 1.5016403117905384
}

FPR independent, bits dependent:
Fit {
  fitParams = V3 8.079418894776325e-2 1.6462569292513933 0.5550062950289885,
  fitErrors = V3 7.713375250014809e-4 8.542261871094414e-3 2.0678969159415226e-2,
  fitNDF = 996, fitWSSR = 19.00125036371992
}

-}

policyForFPR :: FPR -> BloomPolicy
policyForFPR fpr | fpr <= 0 || fpr >= 1 =
    error "bloomPolicyForFPR: fpr out of range (0,1)"

policyForFPR fpr =
    BloomPolicy {
      policyBits   = c,
      policyHashes = k
    }
  where
    k       :: Int
    k       = max 1 (round (recip_log2 * log_fpr))
    c       = log_fpr * log_fpr * f2
            +           log_fpr * f1
            +                     f0
    log_fpr = negate (log fpr)

    -- These parameters are from a (quadratic) linear regression in log space
    -- of samples of the actual FPR between 1 and 20 bits. This is with log FPR
    -- as the independent variable and bits as the dependent variable.
    f2,f1,f0 :: Double
    f2 = 8.079418894776325e-2
    f1 = 1.6462569292513933
    f0 = 0.5550062950289885

policyForBits :: BitsPerEntry -> BloomPolicy
policyForBits c | c < 0 || c > 64 =
    error "policyForBits: out of ragnge [0,64]"

policyForBits c =
    BloomPolicy {
      policyBits   = c,
      policyHashes = k
    }
  where
    k = max 1 (round (c * log2))

policyFPR :: BloomPolicy -> FPR
policyFPR BloomPolicy {
            policyBits = c
          } =
    exp (0 `min` negate (c*c*f2 + c*f1 + f0))
  where
    -- These parameters are from a (quadratic) linear regression in log space
    -- of samples of the actual FPR between 2 and 24 bits. This is with bits as
    -- the independent variable and log FPR as the dependent variable. We have to
    -- clamp the result to keep the FPR within sanity bounds, otherwise extreme
    -- bits per element (<0.1 or >104) give FPRs > 1. This is because it's
    -- just a regression, not a principled approach.
    f2,f1,f0 :: Double
    f2 = -5.03623760876204e-3
    f1 =  0.5251544487138062
    f0 = -0.10110451821280719

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
