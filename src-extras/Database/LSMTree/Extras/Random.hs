{-# LANGUAGE BangPatterns #-}

module Database.LSMTree.Extras.Random (
    -- * Sampling from uniform distributions
    uniformWithoutReplacement
  , uniformWithReplacement
  , sampleUniformWithoutReplacement
  , sampleUniformWithReplacement
    -- * Sampling from multiple distributions
  , frequency
    -- * Generators for specific data types
  , randomByteStringR
  ) where

import qualified Data.ByteString as BS
import           Data.List (unfoldr)
import qualified Data.Set as Set
import qualified System.Random as R
import           System.Random (StdGen, Uniform, uniform, uniformR)
import           Text.Printf (printf)

{-------------------------------------------------------------------------------
  Sampling from uniform distributions
-------------------------------------------------------------------------------}

uniformWithoutReplacement :: (Ord a, Uniform a) => StdGen -> Int -> [a]
uniformWithoutReplacement rng0 n0 = take n0 $
    go Set.empty rng0
  where
    go !seen !rng
        | Set.member x seen =     go               seen  rng'
        | otherwise         = x : go (Set.insert x seen) rng'
      where
        (!x, !rng') = uniform rng

uniformWithReplacement :: Uniform a => StdGen -> Int -> [a]
uniformWithReplacement rng0 n0 = take n0 $
    unfoldr (Just . uniform) rng0

sampleUniformWithoutReplacement :: Ord a => StdGen -> Int -> [a] -> [a]
sampleUniformWithoutReplacement rng0 n xs0 = take n $
    go (Set.fromList xs0) rng0
  where
    go !xs !_rng | Set.null xs = error $
        printf "sampleUniformWithoutReplacement: n > length xs0 for n=%d, \
               \ length xs0=%d"
               n
               (length xs0)

    go !xs !rng = x : go xs' rng'
      where
        (i, rng') = uniformR (0, Set.size xs - 1) rng
        !x        = Set.elemAt i xs
        !xs'      = Set.deleteAt i xs

sampleUniformWithReplacement :: Ord a => StdGen -> Int -> [a] -> [a]
sampleUniformWithReplacement rng0 n xs0 = take n $
    go rng0
  where
    xs = Set.fromList xs0

    go !rng = x : go rng'
      where
        (i, rng') = uniformR (0, Set.size xs - 1) rng
        !x        = Set.elemAt i xs

{-------------------------------------------------------------------------------
  Sampling from multiple distributions
-------------------------------------------------------------------------------}

-- | Chooses one of the given generators, with a weighted random distribution.
-- The input list must be non-empty, weights should be non-negative, and the sum
-- of weights should be non-zero (i.e., at least one weight should be positive).
--
-- Based on the implementation in @QuickCheck@.
frequency :: [(Int, StdGen -> (a, StdGen))] -> StdGen -> (a, StdGen)
frequency xs0 g
  | any ((< 0) . fst) xs0 = error "frequency: frequencies must be non-negative"
  | tot == 0              = error "frequency: at least one frequency should be non-zero"
  | otherwise = pick i xs0
 where
  (i, g') = uniformR (1, tot) g

  tot = sum (map fst xs0)

  pick n ((k,x):xs)
    | n <= k    = x g'
    | otherwise = pick (n-k) xs
  pick _ _  = error "frequency: pick used with empty list"

{-------------------------------------------------------------------------------
  Generators for specific data types
-------------------------------------------------------------------------------}

-- | Generates a random bytestring. Its length is uniformly distributed within
-- the provided range.
randomByteStringR :: (Int, Int) -> StdGen -> (BS.ByteString, StdGen)
randomByteStringR range g =
    let (!l, !g')  = uniformR range g
    in  R.genByteString l g'
