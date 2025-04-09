{-# LANGUAGE BangPatterns #-}
module Main (main) where

import           Control.Monad (forM_, when)
import           System.Environment (getArgs)

import qualified Data.BloomFilter as B

main :: IO ()
main = do
    files <- getArgs
    dictionary <- readFile "/usr/share/dict/words"
    let !bloom = B.fromList (B.policyForFPR 0.01) (words dictionary)
    forM_ files $ \file -> do
        ws <- words <$> readFile file
        forM_ ws $ \w -> when (w `B.notElem` bloom) $ putStrLn w
