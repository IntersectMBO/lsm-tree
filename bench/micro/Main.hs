-- | Micro-benchmarks for the @lsm-tree@ library.
--
-- === TODO
--
-- This is temporary module header documentation. The module will be
-- fleshed out more as we implement bits of it.
--
-- Related work packages: 5, 6
--
module Main (main) where

import qualified Bench.Database.LSMTree.Internal.Run.BloomFilter
import           Criterion.Main

main :: IO ()
main = defaultMain [
      Bench.Database.LSMTree.Internal.Run.BloomFilter.benchmarks
    ]
