-- | By default, this module re-exports the classic bloom filter implementation
-- from "Data.BloomFilter.Classic". If you want to use the blocked bloom filter
-- implementation, import "Data.BloomFilter.Blocked".
module Data.BloomFilter (
    module Data.BloomFilter.Classic
    -- * Example: a spelling checker
    -- $example

    -- * Differences with the @bloomfilter@ package
    -- $differences
  ) where

import           Data.BloomFilter.Classic

-- $example
--
-- This example reads a dictionary file containing one word per line,
-- constructs a Bloom filter with a 1% false positive rate, and
-- spellchecks its standard input.  Like the Unix @spell@ command, it
-- prints each word that it does not recognize.
--
-- >>> import           Control.Monad (forM_)
-- >>> import           System.Environment (getArgs)
-- >>> import qualified Data.BloomFilter as B
--
-- >>> :{
-- main :: IO ()
-- main = do
--     files <- getArgs
--     dictionary <- readFile "/usr/share/dict/words"
--     let !bloom = B.fromList (B.policyForFPR 0.01) 4 (words dictionary)
--     forM_ files $ \file ->
--           putStrLn . unlines . filter (`B.notElem` bloom) . words
--       =<< readFile file
-- :}

-- $differences
--
-- This package is an entirely rewritten fork of the
-- [bloomfilter](https://hackage.haskell.org/package/bloomfilter) package.
--
-- The main differences are
--
-- * Support for both classic and \"blocked\" Bloom filters. Blocked-structured
--   Bloom filters arrange all the bits for each insert or lookup into a single
--   cache line, which greatly reduces the number of slow uncached memory reads.
--   The trade-off for this performance optimisation is a slightly worse
--   trade-off between bits per element and the FPR. In practice for typical
--   FPRs of @1-e3@ up to @1e-4@, this requires a couple extra bits per element.
--
-- * This package support Bloom filters of arbitrary sizes (not limited to powers
--   of two).
--
-- * Sizes over @2^32@ are supported up to @2^48@ for classic Bloom filters and
--   @2^41@ for blocked Bloom filters.
--
-- * The 'Bloom' and 'MBloom' types are parametrised over a 'Hashable' type
--   class, instead of having a @a -> ['Hash']@ typed field.
--   This separation allows clean (de-)serialisation of Bloom filters in this
--   package, as the hashing scheme is static.
--
-- * [@XXH3@ hash](https://xxhash.com/) is used instead of [Jenkins'
--   @lookup3@](https://en.wikipedia.org/wiki/Jenkins_hash_function#lookup3).
