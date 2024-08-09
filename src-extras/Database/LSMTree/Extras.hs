module Database.LSMTree.Extras (
    showPowersOf10
  , showPowersOf
  , groupsOfN
  , vgroupsOfN
  ) where

import           Data.List (find)
import qualified Data.List as List
import           Data.List.NonEmpty (NonEmpty)
import qualified Data.List.NonEmpty as NE
import           Data.Maybe (fromJust)
import qualified Data.Vector as V
import           Text.Printf

showPowersOf10 :: Int -> String
showPowersOf10 = showPowersOf 10

showPowersOf :: Int -> Int -> String
showPowersOf factor n
  | factor <= 1 = error "showPowersOf: factor must be larger than 1"
  | n < 0       = "n < 0"
  | n == 0      = "n == 0"
  | otherwise   = printf "%d <= n < %d" lb ub
  where
    ub = fromJust (find (n <) (iterate (* factor) factor))
    lb = ub `div` factor

-- | Make groups of @n@ elements from a list @xs@
groupsOfN :: Int -> [a] -> [NonEmpty a]
groupsOfN n
  | n <= 0 = error "groupsOfN: n <= 0"
  | otherwise = List.unfoldr f
  where f xs = let (ys, zs) = List.splitAt n xs
               in  (,zs) <$> NE.nonEmpty ys

-- | Make groups of @n@ elements from a vector @xs@
vgroupsOfN :: Int -> V.Vector a -> V.Vector (V.Vector a)
vgroupsOfN n
  | n <= 0 = error "groupsOfN: n <= 0"
  | otherwise = V.unfoldr f
  where
    f xs
      | V.null xs
      = Nothing
      | otherwise
      = Just $ V.splitAt n xs
