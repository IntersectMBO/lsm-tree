module Test.Database.LSMTree (tests) where

import           Database.LSMTree
import           Test.Tasty
import           Test.Tasty.QuickCheck

tests :: TestTree
tests = testGroup "Test.Database.LSMTree" [
      testProperty "placeholder" (someFunc === True)
    ]
