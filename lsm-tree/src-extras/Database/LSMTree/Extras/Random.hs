{-# LANGUAGE BangPatterns #-}

module Database.LSMTree.Extras.Random (
    -- * Sampling from uniform distributions
    uniformWithoutReplacement
  , uniformWithReplacement
  , sampleUniformWithoutReplacement
  , sampleUniformWithReplacement
  , withoutReplacement
  , withReplacement
    -- * Sampling from multiple distributions
  , frequency
    -- * Shuffling
  , shuffle
    -- * Generators for specific data types
  , randomByteStringR
  ) where

import qualified Data.ByteString as BS
import           Data.List (sortBy, unfoldr)
import           Data.Ord (comparing)
import qualified Data.Set as Set
import qualified System.Random as R
import           System.Random (StdGen, Uniform, uniform, uniformR)
import           Text.Printf (printf)

{-------------------------------------------------------------------------------
  Sampling from uniform distributions
-------------------------------------------------------------------------------}

uniformWithoutReplacement :: (Ord a, Uniform a) => StdGen -> Int -> [a]
uniformWithoutReplacement rng n = withoutReplacement rng n uniform

uniformWithReplacement :: Uniform a => StdGen -> Int -> [a]
uniformWithReplacement rng n = withReplacement rng n uniform

sampleUniformWithoutReplacement :: Ord a => StdGen -> Int -> [a] -> [a]
sampleUniformWithoutReplacement rng0 n (Set.fromList -> xs0)
  | n > Set.size xs0 =
      error $
        printf "sampleUniformWithoutReplacement: n > length xs0 for n=%d, \
               \ length xs0=%d"
               n
               (Set.size xs0)
  | otherwise =
      -- Could use 'withoutReplacement', but this is more efficient.
      take n $ go xs0 rng0
  where
    go !xs !rng = x : go xs' rng'
      where
        (i, rng') = uniformR (0, Set.size xs - 1) rng
        !x        = Set.elemAt i xs
        !xs'      = Set.deleteAt i xs

sampleUniformWithReplacement :: Ord a => StdGen -> Int -> [a] -> [a]
sampleUniformWithReplacement rng0 n (Set.fromList -> xs) =
    withReplacement rng0 n $ \rng ->
      let (i, rng') = uniformR (0, Set.size xs - 1) rng
      in  (Set.elemAt i xs, rng')

withoutReplacement :: Ord a => StdGen -> Int -> (StdGen -> (a, StdGen)) -> [a]
withoutReplacement rng0 n0 sample = take n0 $
    go Set.empty rng0
  where
    go !seen !rng
        | Set.member x seen =     go               seen  rng'
        | otherwise         = x : go (Set.insert x seen) rng'
      where
        (!x, !rng') = sample rng

withReplacement :: StdGen -> Int -> (StdGen -> (a, StdGen)) -> [a]
withReplacement rng0 n0 sample =
    take n0 $ unfoldr (Just . sample) rng0

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
  Shuffling
-------------------------------------------------------------------------------}

-- | Create a random permutation of a list.
--
-- Based on the implementation in @QuickCheck@.
shuffle :: [a] -> StdGen -> [a]
shuffle xs g =
    let ns = R.randoms @Int g
    in  map snd (sortBy (comparing fst) (zip ns xs))

{-------------------------------------------------------------------------------
  Generators for specific data types
-------------------------------------------------------------------------------}

-- | Generates a random bytestring. Its length is uniformly distributed within
-- the provided range.
randomByteStringR :: (Int, Int) -> StdGen -> (BS.ByteString, StdGen)
randomByteStringR range g =
    let (!l, !g')  = uniformR range g
    in  R.uniformByteString l g'
