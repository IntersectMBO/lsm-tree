module Test.Util.QC (
    testClassLaws,
    Proxy (..)
  ) where

import           Data.Proxy (Proxy (..))
import           Test.QuickCheck.Classes (Laws (..))
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.QuickCheck (testProperty)

testClassLaws :: String -> Laws -> TestTree
testClassLaws typename Laws {lawsTypeclass, lawsProperties} =
  testGroup ("class laws" ++ lawsTypeclass ++ " " ++ typename)
    [ testProperty name prop
    | (name, prop) <- lawsProperties ]


