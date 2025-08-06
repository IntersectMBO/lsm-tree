{-# LANGUAGE BangPatterns #-}
module Main (main) where

import           Control.Monad (forM_, unless, when)
import           System.Directory
import           System.Environment (getArgs)
import           System.Exit

import qualified Data.BloomFilter as B

main :: IO ()
main = do
    files <- getArgs
    when (null files) $ do
      putStrLn "No files to spell"
      exitSuccess
    putStrLn $ "Spelling files: " ++ show files
    hasDictionary <- doesFileExist "/usr/share/dict/words"
    unless hasDictionary $ do
      putStrLn "No dictionary found"
      exitSuccess
    dictionary <- readFile "/usr/share/dict/words"
    let !bloom = B.fromList (B.policyForFPR 0.01) bSalt (words dictionary)
    forM_ files $ \file ->
          putStrLn . unlines . filter (`B.notElem` bloom) . words
      =<< readFile file

bSalt :: B.Salt
bSalt = 4
