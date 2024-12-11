{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}

module Test.Util.Arbitrary (
    prop_arbitraryAndShrinkPreserveInvariant
  , prop_forAllArbitraryAndShrinkPreserveInvariant
  , deepseqInvariant
  ) where

import           Control.DeepSeq (NFData, deepseq)
import           Test.QuickCheck
import           Test.Tasty (TestTree)
import           Test.Tasty.QuickCheck (testProperty)

prop_arbitraryAndShrinkPreserveInvariant ::
    forall a. (Arbitrary a, Show a) => (a -> Bool) -> [TestTree]
prop_arbitraryAndShrinkPreserveInvariant =
    prop_forAllArbitraryAndShrinkPreserveInvariant arbitrary shrink

prop_forAllArbitraryAndShrinkPreserveInvariant ::
    forall a. Show a => Gen a -> (a -> [a]) -> (a -> Bool) -> [TestTree]
prop_forAllArbitraryAndShrinkPreserveInvariant gen shr inv =
    [ testProperty "Arbitrary satisfies invariant" $
            property $ forAllShrink gen shr inv
    , testProperty "Shrinking satisfies invariant" $
            property $ forAll gen $ \x ->
                         case shr x of
                           [] -> label "no shrinks" $ property True
                           xs -> forAll (growingElements xs) inv
    ]

-- | Trivial invariant, but checks that the value is finite
deepseqInvariant :: NFData a => a -> Bool
deepseqInvariant x = x `deepseq` True
