-- | Macro-benchmarks for the @lsm-tree@ library.
--
-- === TODO
--
-- This is temporary module header documentation. The module will be
-- fleshed out more as we implement bits of it.
--
-- Related work packages: 8
--
module Main (main) where

import qualified Bench.Database.LSMTree.BloomFilter

import           System.IO

main :: IO ()
main = do
   hSetBuffering stdout NoBuffering
   Bench.Database.LSMTree.BloomFilter.benchmarks
