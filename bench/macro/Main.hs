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

import qualified Bench.Database.LSMTree.Internal.BloomFilter
import qualified Bench.Database.LSMTree.Internal.Lookup

import           System.Environment (getArgs)
import           System.Exit (exitFailure)
import           System.IO

main :: IO ()
main = do
  hSetBuffering stdout NoBuffering
  args <- getArgs
  case args of
    [arg] | arg == "BloomFilter" ->
      Bench.Database.LSMTree.Internal.BloomFilter.benchmarks
          | arg == "Lookup" ->
      Bench.Database.LSMTree.Internal.Lookup.benchmarks
    _     -> do
      putStrLn "Wrong usage, pass one of two commands: [BloomFilter|Lookup]"
      exitFailure
