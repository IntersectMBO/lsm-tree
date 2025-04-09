module Main where

import qualified Data.BloomFilter as B
import           Data.BloomFilter.Hash (Hashable (..), hash64)

import           Data.Word (Word64)
import           System.Random

import           Criterion.Main

main :: IO ()
main =
    defaultMain [
      env newStdGen $ \g0 ->
      bench "construct bloom m=1e6 fpr=1%" $
        whnf (constructBloom 1_000_000 0.01) g0

    , env newStdGen $ \g0 ->
      bench "construct bloom m=1e6 fpr=0.1%" $
        whnf (constructBloom 1_000_000 0.001) g0

    , env newStdGen $ \g0 ->
      bench "construct bloom m=1e7 fpr=0.1%" $
        whnf (constructBloom 10_000_000 0.001) g0
    ]

constructBloom :: Int -> Double -> StdGen -> B.Bloom Word64
constructBloom n fpr g0 =
    B.unfold (B.sizeForFPR fpr n) nextElement (g0, 0)
  where
    nextElement :: (StdGen, Int) -> Maybe (Word64, (StdGen, Int))
    nextElement (!g, !i)
      | i >= n    = Nothing
      | otherwise = Just (x, (g', i+1))
        where
          (!x, !g') = uniform g

