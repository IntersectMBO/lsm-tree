module Test.Util.QC (
    testClassLaws,
    Proxy (..)
    -- * Modifiers
  , NubList (..)
  , nubListList
  , genNubList
  , genNubListBy
  , shrinkNubList
  , shrinkNubListBy
  ) where

import qualified Data.List as List
import           Data.Proxy (Proxy (..))
import           Test.QuickCheck.Classes (Laws (..))
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.QuickCheck (Arbitrary (..), Gen, testProperty)

testClassLaws :: String -> Laws -> TestTree
testClassLaws typename Laws {lawsTypeclass, lawsProperties} =
  testGroup ("class laws" ++ lawsTypeclass ++ " " ++ typename)
    [ testProperty name prop
    | (name, prop) <- lawsProperties ]

{-------------------------------------------------------------------------------
  Modifiers
-------------------------------------------------------------------------------}

newtype NubList a = NubList [a]
  deriving stock (Show, Functor, Foldable, Traversable)

nubListList :: NubList a -> [a]
nubListList (NubList xs) = xs

genNubList :: (Arbitrary a, Eq a) => Gen (NubList a)
genNubList = genNubListBy (==)

genNubListBy :: Arbitrary a => (a -> a -> Bool) -> Gen (NubList a)
genNubListBy eq = do
    xs <- arbitrary
    pure $ NubList (List.nubBy eq xs)

shrinkNubList :: (Arbitrary a, Eq a) => NubList a -> [NubList a]
shrinkNubList = shrinkNubListBy (==)

shrinkNubListBy :: Arbitrary a => (a -> a -> Bool) -> NubList a -> [NubList a]
shrinkNubListBy _ (NubList [])  = []
shrinkNubListBy eq (NubList xs) = NubList . List.nubBy eq <$> shrink xs

instance (Arbitrary a, Eq a) => Arbitrary (NubList a) where
  arbitrary = genNubList
  shrink = shrinkNubList
