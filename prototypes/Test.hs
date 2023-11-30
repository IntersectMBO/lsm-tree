module Main (main) where

import           Test.Tasty

import qualified FormatPage
import qualified ScheduledMergesTestQLS

main :: IO ()
main = defaultMain $ testGroup "prototype" [
      ScheduledMergesTestQLS.tests,
      FormatPage.tests
    ]
