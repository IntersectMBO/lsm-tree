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

import qualified Bench.Database.LSMTree.Internal.BloomFilter
import qualified Bench.Database.LSMTree.Internal.IndexCompact
import qualified Bench.Database.LSMTree.Internal.Lookup
import qualified Bench.Database.LSMTree.Internal.RawPage
import           Criterion.Main (defaultMain)

main :: IO ()
main = defaultMain [
      Bench.Database.LSMTree.Internal.Lookup.benchmarks
    , Bench.Database.LSMTree.Internal.BloomFilter.benchmarks
    , Bench.Database.LSMTree.Internal.IndexCompact.benchmarks
    , Bench.Database.LSMTree.Internal.RawPage.benchmarks
    ]
