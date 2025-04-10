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
    -- as the indepedent variable and bits as the depedent variable.
    f2,f1,f0 :: Double
    f2 = 8.035531421107756e-2
    f1 = 1.653017726702572
    f0 = 0.5343568065075601
{-
Regression, FPR indepedent, bits depedent:
Fit {fitParams = V3 8.035531421107756e-2 1.653017726702572 0.5343568065075601, fitErrors = V3 7.602655075308541e-4 8.422591688796256e-3 2.0396917012822195e-2, fitNDF = 996, fitWSSR = 18.362899348627252}
-}

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
    exp (negate (c*c*f2 + c*f1 + f0))
  where
    -- These parameters are from a (quadratic) linear regression in log space
    -- of samples of the actual FPR between 2 and 24 bits. This is with bits as
    -- the indepedent variable and log FPR as the depedent variable.
    f2,f1,f0 :: Double
    f2 = -4.990533525011442e-3
    f1 =  0.5236326626983274
    f0 = -9.08567744857578e-2
{-
Regression, bits indepedent, FPR depedent:
Fit {fitParams = V3 (-4.990533525011442e-3) 0.5236326626983274 (-9.08567744857578e-2), fitErrors = V3 3.2672398863476205e-5 8.69874829861453e-4 4.98365450607998e-3, fitNDF = 996, fitWSSR = 1.4326826384055948}
-}

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
