module Main (main) where

import           Test.Tasty

import qualified FormatPage
import qualified ScheduledMergesTest
import qualified ScheduledMergesTestQLS

main :: IO ()
main = defaultMain $ testGroup "prototype" [
      testGroup "ScheduledMerges" [
        ScheduledMergesTest.tests,
        ScheduledMergesTestQLS.tests
      ],
      FormatPage.tests
    ]
