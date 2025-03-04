module Test.Util.Arbitrary (
    prop_arbitraryAndShrinkPreserveInvariant
  , prop_forAllArbitraryAndShrinkPreserveInvariant
  , deepseqInvariant
  , noTags
  ) where

import           Control.DeepSeq (NFData, deepseq)
import           Database.LSMTree.Extras (showPowersOf10)
import           Test.QuickCheck
import           Test.Tasty (TestTree)
import           Test.Tasty.QuickCheck (testProperty)

prop_arbitraryAndShrinkPreserveInvariant ::
     forall a prop. (Arbitrary a, Show a, Testable prop)
  => (a -> Property -> Property) -> (a -> prop) -> [TestTree]
prop_arbitraryAndShrinkPreserveInvariant tag =
    prop_forAllArbitraryAndShrinkPreserveInvariant tag arbitrary shrink

prop_forAllArbitraryAndShrinkPreserveInvariant ::
     forall a prop. (Show a, Testable prop)
  => (a -> Property -> Property) -> Gen a -> (a -> [a]) -> (a -> prop) -> [TestTree]
prop_forAllArbitraryAndShrinkPreserveInvariant tag gen shr inv =
    [ testProperty "Arbitrary satisfies invariant" $
        forAllShrink gen shr $ \x ->
          tag x $ property $ inv x
    , testProperty "Shrinking satisfies invariant" $
        -- We don't use forallShrink here. If this property fails, it means that
        -- the shrinker is broken, so we don't want to rely on it.
        forAll gen $ \x ->
          case shr x of
            [] -> label "no shrinks" $ property True
            xs -> tabulate "number of shrinks" [showPowersOf10 (length xs)] $
              forAll (elements xs) inv  -- TODO: check more than one?
    ]

-- | Trivial invariant, but checks that the value is finite
deepseqInvariant :: NFData a => a -> Bool
deepseqInvariant x = x `deepseq` True

noTags :: a -> Property -> Property
noTags _ = id
