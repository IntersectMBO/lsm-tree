{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE TypeApplications   #-}


module Bench.Database.LSMTree.Internal.BloomFilter (
    benchmarks
    -- * Benchmarked functions
  , elems
  ) where

import           Criterion.Main
import           Data.BloomFilter (Bloom)
import qualified Data.BloomFilter as Bloom
import qualified Data.BloomFilter.Easy as Bloom.Easy
import           Data.BloomFilter.Hash (Hashable)
import qualified Data.Foldable as Fold
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import           Database.LSMTree.Extras.Random
import           Database.LSMTree.Extras.UTxO (UTxOKey)
import           Database.LSMTree.Internal.Serialise (SerialisedKey,
                     serialiseKey)
import           System.Random
import           Test.QuickCheck (generate, shuffle)

-- See 'utxoNumPages'.
benchmarks :: Benchmark
benchmarks = bgroup "Bench.Database.LSMTree.Internal.BloomFilter" [
      bgroup "elems" [
          env (elemEnv 0.1 2_500_000 1_000_000 0) $ \ ~(b, xs) ->
            bench "onlyTruePositives 0.1" $ whnf (elems b) xs
        , env (elemEnv 0.9 2_500_000 1_000_000 0) $ \ ~(b, xs) ->
            bench "onlyTruePositives 0.9" $ whnf (elems b) xs
        , env (elemEnv 0.1 2_500_000 0 1_000_000) $ \ ~(b, xs) ->
            bench "onlyNegatives 0.1" $ whnf (elems b) xs
        , env (elemEnv 0.9 2_500_000 0 1_000_000) $ \ ~(b, xs) ->
            bench "onlyNegatives 0.9" $ whnf (elems b) xs
        ]
    , env (constructionEnv 2_500_000) $ \ m ->
      bgroup "construction" [
          bench "easyList 0.1" $ whnf (constructBloom Bloom.Easy.easyList 0.1) m
        , bench "easyList 0.9" $ whnf (constructBloom Bloom.Easy.easyList 0.9) m
        ]
    ]

-- | Input environment for benchmarking 'Bloom.elem'.
elemEnv ::
     Double -- ^ False positive rate
  -> Int    -- ^ Number of entries in the bloom filter
  -> Int    -- ^ Number of positive lookups
  -> Int    -- ^ Number of negative lookups
  -> IO (Bloom SerialisedKey, [SerialisedKey])
elemEnv fpr nbloom nelemsPositive nelemsNegative = do
    stdgen  <- newStdGen
    stdgen' <- newStdGen
    let (xs, ys1) = splitAt nbloom
                  $ uniformWithoutReplacement    @UTxOKey stdgen  (nbloom + nelemsNegative)
        ys2       = sampleUniformWithReplacement @UTxOKey stdgen' nelemsPositive xs
    zs <- generate $ shuffle (ys1 ++ ys2)
    pure (Bloom.Easy.easyList fpr (fmap serialiseKey xs), fmap serialiseKey zs)

-- | Used for benchmarking 'Bloom.elem'.
elems :: Hashable a => Bloom a -> [a] -> ()
elems b xs = Fold.foldl' (\acc x -> Bloom.elem x b `seq` acc) () xs

-- | Input environment for benchmarking 'constructBloom'.
constructionEnv :: Int -> IO (Map SerialisedKey SerialisedKey)
constructionEnv n = do
    stdgen  <- newStdGen
    stdgen' <- newStdGen
    let ks = uniformWithoutReplacement @UTxOKey stdgen n
        vs = uniformWithReplacement @UTxOKey stdgen' n
    pure $ Map.fromList (zipWith (\k v -> (serialiseKey k, serialiseKey v)) ks vs)

-- | Used for benchmarking the construction of bloom filters from write buffers.
constructBloom ::
     (Double -> [SerialisedKey] -> Bloom SerialisedKey)
  -> Double
  -> Map SerialisedKey SerialisedKey
  -> Bloom SerialisedKey
constructBloom mkBloom fpr m = mkBloom fpr (Map.keys m)
