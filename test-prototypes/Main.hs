module Main (main) where

import           Test.Tasty

import qualified Test.FormatPage
import qualified Test.ScheduledMerges
import qualified Test.ScheduledMergesQLS

main :: IO ()
main = defaultMain $ testGroup "prototypes" [
      Test.FormatPage.tests
    , Test.ScheduledMerges.tests
    , Test.ScheduledMergesQLS.tests
    ]
