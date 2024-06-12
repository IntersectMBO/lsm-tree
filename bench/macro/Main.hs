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
    [arg] | arg == "Lookup" ->
      Bench.Database.LSMTree.Internal.Lookup.benchmarks Nothing
    [arg1, arg2] | arg1 == "Lookup"
                 , let !dir = read ("\"" <> arg2 <> "\"") ->
      Bench.Database.LSMTree.Internal.Lookup.benchmarks (Just dir)
    _     -> do
      putStrLn "Wrong usage, pass one of two commands: [BloomFilter|Lookup [optional filepath]]"
      exitFailure
