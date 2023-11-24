{-# LANGUAGE NumericUnderscores #-}
{- HLINT ignore "Use camelCase" -}
{- HLINT ignore "Eta reduce" -}

module Bench.Database.LSMTree.Internal.Run.BloomFilter (benchmarks) where

import           Criterion.Main
import qualified Data.BloomFilter.Easy as Bloom.Easy
import           Data.Foldable (Foldable (..))
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import           Database.LSMTree.Extras
import           Database.LSMTree.Generators
import           Database.LSMTree.Internal.Run.BloomFilter as Bloom
import           System.Random
import           System.Random.Extras
import           Test.QuickCheck (generate, shuffle)

-- See 'utxoNumPages'.
benchmarks :: Benchmark
benchmarks = bgroup "Bench.Database.LSMTree.Internal.Run.BloomFilter" [
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
          bench "incrementalST 0.1" $ whnf (constructBloom mkBloomST 0.1) m
        , bench "incrementalST 0.9" $ whnf (constructBloom mkBloomST 0.9) m
        , bench "easyList 0.1" $ whnf (constructBloom mkBloomEasy 0.1) m
        , bench "easyList 0.9" $ whnf (constructBloom mkBloomEasy 0.9) m
        ]
    ]

-- | Input environment for benchmarking 'Bloom.elem'.
elemEnv ::
     Double -- ^ False positive rate
  -> Int    -- ^ Number of entries in the bloom filter
  -> Int    -- ^ Number of positive lookups
  -> Int    -- ^ Number of negative lookups
  -> IO (Bloom UTxOKey, [UTxOKey])
elemEnv fpr nbloom nelemsPositive nelemsNegative = do
    stdgen  <- newStdGen
    stdgen' <- newStdGen
    let (xs, ys1) = splitAt nbloom
                  $ uniformWithoutReplacement    stdgen  (nbloom + nelemsNegative)
        ys2       = sampleUniformWithReplacement stdgen' nelemsPositive xs
    zs <- generate $ shuffle (ys1 ++ ys2)
    pure (Bloom.Easy.easyList fpr xs, zs)

-- | Used for benchmarking 'Bloom.elem'.
elems :: Bloom a -> [a] -> ()
elems b xs = foldl' (\acc x -> Bloom.elem x b `seq` acc) () xs

-- | Input environment for benchmarking 'constructBloom'.
constructionEnv :: Int -> IO (Map UTxOKey UTxOKey)
constructionEnv n = do
    stdgen  <- newStdGen
    stdgen' <- newStdGen
    let ks = uniformWithoutReplacement stdgen n
        vs = uniformWithReplacement stdgen' n
    pure $ Map.fromList (zip ks vs)

-- | Used for benchmarking the construction of bloom filters from write buffers.
constructBloom ::
     (Double -> BloomMaker UTxOKey)
  -> Double
  -> Map UTxOKey UTxOKey
  -> Bloom UTxOKey
constructBloom mkBloom fpr m = mkBloom fpr (Map.keys m)
