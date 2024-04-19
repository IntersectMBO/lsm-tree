module Database.LSMTree.Extras (
    showPowersOf10
  ) where

import           Text.Printf

showPowersOf10 :: Int -> String
showPowersOf10 n0
  | n0 <= 0   = "n == 0"
  | n0 == 1   = "n == 1"
  | otherwise = go n0 1
  where
    go n m | n < m'    = printf "%d < n < %d" m m'
            | otherwise = go n m'
            where m' = 10*m
