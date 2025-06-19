{-# LANGUAGE BangPatterns #-}
module Main (main) where

import           Control.Monad (forM_, unless, when)
import           System.Environment (getArgs)

import qualified Data.BloomFilter as B

main :: IO ()
main = do
    files <- getArgs
    unless (null files) $ do
      dictionary <- readFile "/usr/share/dict/words"
      let !bloom = B.fromList (B.policyForFPR 0.01) (words dictionary)
      forM_ files $ \file ->
            putStrLn . unlines . filter (`B.notElem` bloom) . words
        =<< readFile file
