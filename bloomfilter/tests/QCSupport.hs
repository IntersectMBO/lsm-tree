module QCSupport (P(..)) where

import           Test.QuickCheck (Arbitrary (..), choose)
import           Test.QuickCheck.Instances ()

newtype P = P { unP :: Double }
    deriving (Eq, Ord, Show)

instance Arbitrary P where
    arbitrary = P <$> choose (epsilon, 1 - epsilon)
        where epsilon = 1e-6 :: Double
