{-# LANGUAGE BangPatterns #-}
module Main (main) where

import           Data.Bits ((.|.))
import           Data.Numbers.Primes

steps :: Int
steps = 5

-- calculate some primes exponentially spaced between 64..2^40
sparsePrimes :: [Int]
sparsePrimes = go (6 * steps) where
    go :: Int -> [Int]
    go !e = if e > 40 * steps then [] else go1 e (truncate' (k ^ e))

    go1 :: Int -> Int -> [Int]
    go1 !e !n = if isPrime n then n : go (e + 1) else go1 e (n - 2) -- we count down!

    k :: Double
    k = exp (log 2 / fromIntegral steps)

    -- truncate to odd
    truncate' n = truncate n .|. 1

main :: IO ()
main = print sparsePrimes
