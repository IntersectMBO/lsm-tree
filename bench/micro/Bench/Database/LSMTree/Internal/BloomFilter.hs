{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE TypeApplications   #-}


module Bench.Database.LSMTree.Internal.BloomFilter (
    benchmarks
    -- * Benchmarked functions
  , elems
  ) where

import           Criterion.Main
import qualified Data.Bifoldable as BiFold
import           Data.BloomFilter (Bloom)
import qualified Data.BloomFilter as Bloom
import           Data.BloomFilter.Hash (Hashable)
import qualified Data.Foldable as Fold
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import           Database.LSMTree.Extras.Random
import           Database.LSMTree.Extras.UTxO (UTxOKey)
import           Database.LSMTree.Internal.Serialise (SerialisedKey,
                     serialiseKey)
import           System.Random as R

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
          bench "FPR = 0.1" $
            whnf (constructBloom 0.1) m

        , bench "FPR = 0.9" $
            whnf (constructBloom 0.9) m
        ]
    ]

benchSalt :: Bloom.Salt
benchSalt = 4

-- | Input environment for benchmarking 'Bloom.elem'.
elemEnv ::
     Double -- ^ False positive rate
  -> Int    -- ^ Number of entries in the bloom filter
  -> Int    -- ^ Number of positive lookups
  -> Int    -- ^ Number of negative lookups
  -> IO (Bloom SerialisedKey, [SerialisedKey])
elemEnv fpr nbloom nelemsPositive nelemsNegative = do
    let g = mkStdGen 100
        (g1, g') = R.splitGen g
        (g2, g3) = R.splitGen g'

    let (xs, ys1) = splitAt nbloom
                  $ uniformWithoutReplacement    @UTxOKey g1  (nbloom + nelemsNegative)
        ys2       = sampleUniformWithReplacement @UTxOKey g2 nelemsPositive xs
        zs        = shuffle (ys1 ++ ys2) g3
    pure ( Bloom.fromList (Bloom.policyForFPR fpr) benchSalt (fmap serialiseKey xs)
         , fmap serialiseKey zs
         )

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
     Double
  -> Map SerialisedKey SerialisedKey
  -> Bloom SerialisedKey
constructBloom fpr m =
    -- For faster construction, avoid going via lists and use Bloom.create,
    -- traversing the map inserting the keys
    Bloom.create (Bloom.sizeForFPR fpr (Map.size m)) benchSalt $ \b ->
      BiFold.bifoldMap (\k -> Bloom.insert b k) (\_v -> pure ()) m
