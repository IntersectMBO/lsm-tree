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

import qualified Bench.Database.LSMTree.Lookup
import qualified Bench.Database.LSMTree.RawPage
import qualified Bench.Database.LSMTree.Run.BloomFilter
import qualified Bench.Database.LSMTree.Run.Index.Compact
import           Criterion.Main (defaultMain)

main :: IO ()
main = defaultMain [
      Bench.Database.LSMTree.Lookup.benchmarks
    , Bench.Database.LSMTree.Run.BloomFilter.benchmarks
    , Bench.Database.LSMTree.Run.Index.Compact.benchmarks
    , Bench.Database.LSMTree.RawPage.benchmarks
    ]
