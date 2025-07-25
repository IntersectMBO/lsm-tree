module Main (main) where

import           Criterion.Main (bench, bgroup, defaultMain, env, whnf)
import qualified Data.BloomFilter.Blocked as B.Blocked
import qualified Data.BloomFilter.Classic as B.Classic
import           Data.Word (Word64)
import           System.Random (StdGen, newStdGen, uniform)

main :: IO ()
main =
    defaultMain [
      bgroup "Data.BloomFilter.Classic" [
        env newStdGen $ \g0 ->
        bench "construct m=1e6 fpr=1%" $
          whnf (constructBloom_classic 1_000_000 0.01) g0

      , env newStdGen $ \g0 ->
        bench "construct m=1e6 fpr=0.1%" $
          whnf (constructBloom_classic 1_000_000 0.001) g0

      , env newStdGen $ \g0 ->
        bench "construct m=1e7 fpr=0.1%" $
          whnf (constructBloom_classic 10_000_000 0.001) g0
      ]
    , bgroup "Data.BloomFilter.Blocked" [
        env newStdGen $ \g0 ->
        bench "construct m=1e6 fpr=1%" $
          whnf (constructBloom_blocked 1_000_000 0.01) g0

      , env newStdGen $ \g0 ->
        bench "construct m=1e6 fpr=0.1%" $
          whnf (constructBloom_blocked 1_000_000 0.001) g0

      , env newStdGen $ \g0 ->
        bench "construct m=1e7 fpr=0.1%" $
          whnf (constructBloom_blocked 10_000_000 0.001) g0
      ]
    ]

constructBloom_classic :: Int -> Double -> StdGen -> B.Classic.Bloom Word64
constructBloom_classic n fpr g0 =
  let (!salt, !g1) = uniform g0 in
    B.Classic.unfold (B.Classic.sizeForFPR fpr n) salt (nextElement n) (g1, 0)

constructBloom_blocked :: Int -> Double -> StdGen -> B.Blocked.Bloom Word64
constructBloom_blocked n fpr g0 =
  let (!salt, !g1) = uniform g0 in
    B.Blocked.unfold (B.Blocked.sizeForFPR fpr n) salt (nextElement n) (g1, 0)

{-# INLINE nextElement #-}
nextElement :: Int -> (StdGen, Int) -> Maybe (Word64, (StdGen, Int))
nextElement !n (!g, !i)
  | i >= n    = Nothing
  | otherwise = Just (x, (g', i+1))
    where
      (!x, !g') = uniform g

