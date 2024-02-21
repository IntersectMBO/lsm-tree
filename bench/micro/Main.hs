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

import qualified Bench.Database.LSMTree.Internal.Lookup
import qualified Bench.Database.LSMTree.Internal.RawPage
import qualified Bench.Database.LSMTree.Internal.Run.BloomFilter
import qualified Bench.Database.LSMTree.Internal.Run.Index.Compact
import           Criterion.Main (defaultMain)

main :: IO ()
main = defaultMain [
      Bench.Database.LSMTree.Internal.Lookup.benchmarks
    , Bench.Database.LSMTree.Internal.Run.BloomFilter.benchmarks
    , Bench.Database.LSMTree.Internal.Run.Index.Compact.benchmarks
    , Bench.Database.LSMTree.Internal.RawPage.benchmarks
    ]
