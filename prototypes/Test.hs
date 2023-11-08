module Main (main) where

import Test.Tasty

import qualified ScheduledMergesTestQLS

main :: IO ()
main = defaultMain $ testGroup "prototype" [
      ScheduledMergesTestQLS.tests
    ]
