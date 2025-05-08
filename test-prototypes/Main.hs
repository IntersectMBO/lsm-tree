module Main (main) where

import           Test.Tasty

import qualified FormatPage
import qualified Test.ScheduledMerges
import qualified Test.ScheduledMergesQLS

main :: IO ()
main = defaultMain $ testGroup "prototypes" [
      FormatPage.tests
    , Test.ScheduledMerges.tests
    , Test.ScheduledMergesQLS.tests
    ]
