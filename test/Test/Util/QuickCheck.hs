module Test.Util.QuickCheck (
    liftArbitrary2Map
  , liftShrink2Map
  ) where

import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import           Test.QuickCheck

liftArbitrary2Map :: Ord k => Gen k -> Gen v -> Gen (Map k v)
liftArbitrary2Map genk genv = Map.fromList <$> liftArbitrary (liftArbitrary2 genk genv)

liftShrink2Map :: Ord k => (k -> [k]) -> (v -> [v]) -> Map k v -> [Map k v]
liftShrink2Map shrinkk shrinkv m = Map.fromList <$>
    liftShrink (liftShrink2 shrinkk shrinkv) (Map.toList m)
