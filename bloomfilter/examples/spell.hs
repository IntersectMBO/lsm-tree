{-# LANGUAGE BangPatterns #-}
module Main (main) where

import           Control.Exception (IOException, catch)
import           Control.Monad (forM_, when)
import           Data.Char (isLetter, toLower)
import           System.Environment (getArgs)

import           Data.BloomFilter.Classic.Easy (easyList, notElem)
import           Prelude hiding (notElem)

main :: IO ()
main = do
    files <- getArgs
    dictionary <- readFile "/usr/share/dict/words"
    let !bloom = easyList 0.01 (words dictionary)
    forM_ files $ \file -> do
        ws <- words <$> readFile file
        forM_ ws $ \w -> when (w `notElem` bloom) $ putStrLn w
