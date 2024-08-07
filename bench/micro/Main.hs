{-# LANGUAGE CPP #-}

-- | Micro-benchmarks for the @lsm-tree@ library.
module Main (main) where

import qualified Bench.Database.LSMTree.Internal.BloomFilter
import qualified Bench.Database.LSMTree.Internal.IndexCompact
import qualified Bench.Database.LSMTree.Internal.Lookup
import qualified Bench.Database.LSMTree.Internal.Merge
import qualified Bench.Database.LSMTree.Internal.RawPage
import qualified Bench.Database.LSMTree.Internal.Serialise
import qualified Bench.Database.LSMTree.Internal.WriteBuffer
import qualified Bench.Database.LSMTree.Monoidal
import           Criterion.Main (defaultMain)

main :: IO ()
main = do
#ifdef NO_IGNORE_ASSERTS
    putStrLn "WARNING: BENCHMARKING A BUILD IN DEBUG MODE"
#endif
    defaultMain [
        Bench.Database.LSMTree.Internal.BloomFilter.benchmarks
      , Bench.Database.LSMTree.Internal.IndexCompact.benchmarks
      , Bench.Database.LSMTree.Internal.Lookup.benchmarks
      , Bench.Database.LSMTree.Internal.Merge.benchmarks
      , Bench.Database.LSMTree.Internal.RawPage.benchmarks
      , Bench.Database.LSMTree.Internal.Serialise.benchmarks
      , Bench.Database.LSMTree.Internal.WriteBuffer.benchmarks
      , Bench.Database.LSMTree.Monoidal.benchmarks
      ]
