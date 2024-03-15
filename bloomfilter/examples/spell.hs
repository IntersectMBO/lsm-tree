{-# LANGUAGE BangPatterns #-}
module Main (main) where

import           Control.Exception (IOException, catch)
import           Control.Monad (forM_, when)
import           Data.Char (isLetter, toLower)
import           System.Environment (getArgs)

import           Data.BloomFilter.Easy (easyList, notElem)
import           Prelude hiding (notElem)

main :: IO ()
main = do
    files <- getArgs
    dictionary <- readFile "/usr/share/dict/words" `catchIO` \_ -> return "yes no"
    let !bloom = easyList 0.01 (words dictionary)
    forM_ files $ \file -> do
        ws <- words <$> readFile file
        forM_ ws $ \w -> when (w `notElem` bloom) $ putStrLn w

catchIO :: IO a -> (IOException -> IO a) -> IO a
catchIO = catch
