module Test.Util.QC (
    testClassLaws
  , testClassLawsWith
  , Proxy (..)
  ) where

import           Data.Proxy (Proxy (..))
import           Test.QuickCheck.Classes (Laws (..))
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.QuickCheck (Property, testProperty)

testClassLaws :: String -> Laws -> TestTree
testClassLaws typename laws = testClassLawsWith typename laws testProperty

testClassLawsWith ::
     String -> Laws
  -> (String -> Property -> TestTree)
  -> TestTree
testClassLawsWith typename Laws {lawsTypeclass, lawsProperties} k =
  testGroup ("class laws" ++ lawsTypeclass ++ " " ++ typename)
    [ k name prop
    | (name, prop) <- lawsProperties ]


