module Test.Util.QC (
    testClassLaws
  , testClassLawsWith
  , Proxy (..)
  , Choice
  , getChoice
  ) where

import           Data.Proxy (Proxy (..))
import           Data.Word (Word64)
import           Test.QuickCheck.Classes (Laws (..))
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.QuickCheck (Arbitrary (..), Property,
                     arbitraryBoundedIntegral, shrinkIntegral, testProperty)

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


-- | A 'Choice' of a uniform random number in a range where shrinking picks smaller numbers.
newtype Choice = Choice Word64
  deriving stock (Show, Eq)

instance Arbitrary Choice where
  arbitrary = Choice <$> arbitraryBoundedIntegral
  shrink (Choice x) = Choice <$> shrinkIntegral x

-- | Use a 'Choice' to get a concrete 'Integral' in range @(a, a)@ inclusive.
--
--   The choice of integral is uniform as long as the range is smaller than or
--   equal to the maximum bound of `Word64`, i.e., 18446744073709551615.
getChoice :: (Integral a) => Choice -> (a, a) -> a
getChoice (Choice n) (l, u) = fromIntegral (((ni * (ui - li)) `div` mi) + li)
  where
    ni = toInteger n
    li = toInteger l
    ui = toInteger u
    mi = toInteger (maxBound :: Word64)

