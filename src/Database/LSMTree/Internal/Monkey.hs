{-# LANGUAGE TypeApplications #-}

module Database.LSMTree.Internal.Monkey (
    -- * Monkey-style tuning of bloom filters
    -- $tuning
    monkeyFPR
  , monkeyBits
  , monkeyHashFuncs
  ) where

-- $tuning
--
-- These functions are experimental, and will not yet guarantee correct false
-- positive rates. For now, use 'Easy.suggestSizing' and 'Easy.safeSuggestSizing' instead.
--
-- TODO: un-experimental these functions.

-- | Compute the false positive rate for a bloom filter.
--
-- Assumes that the bloom filter uses 'monkeyHashFuncs' hash functions.
--
-- REF: Equation 2 from the paper /Optimal Bloom Filters and Adaptive Merging
-- for LSM-Trees/.
monkeyFPR ::
     Int    -- ^ Number of bits assigned to the bloom filter.
  -> Int    -- ^ Number of entries inserted into the bloom filter.
  -> Double
monkeyFPR numBits numEntries =
    exp ((-(fromIntegral numBits / fromIntegral numEntries)) * (log 2 ** 2))

-- | Compute the number of bits in a bloom filter.
--
-- Assumes that the bloom filter uses 'monkeyHashFuncs' hash functions.
--
-- REF: Equation 2 from the paper /Optimal Bloom Filters and Adaptive Merging
-- for LSM-Trees/, rewritten in terms of @bits@ on page 11.
monkeyBits ::
     Int    -- ^ Number of entries inserted into the bloom filter.
  -> Double -- ^ False positive rate.
  -> Int
monkeyBits numEntries fpr = ceiling $
  (- fromIntegral numEntries) * (log fpr / (log 2 ** 2))

-- | Computes the optimal number of hash functions that minimses the false
-- positive rate for a bloom filter.
--
-- REF: Footnote 2, page 6 from the paper /Optimal Bloom Filters and Adaptive
-- Merging for LSM-Trees/.
monkeyHashFuncs ::
     Int -- ^ Number of bits assigned to the bloom filter.
  -> Int -- ^ Number of entries inserted into the bloom filter.
  -> Int
monkeyHashFuncs numBits numEntries = truncate @Double $
    (fromIntegral numBits / fromIntegral numEntries) * log 2
