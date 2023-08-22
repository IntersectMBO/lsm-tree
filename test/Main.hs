module Main (main) where

import           Test.Database.LSMTree (tests)
import           Test.Tasty

main :: IO ()
main = defaultMain tests
