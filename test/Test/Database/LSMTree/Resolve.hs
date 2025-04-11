{-# LANGUAGE AllowAmbiguousTypes #-}

module Test.Database.LSMTree.Resolve (tests) where

import           Control.DeepSeq (NFData)
import           Data.Monoid (Sum (..))
import           Data.Word
import           Database.LSMTree
import           Database.LSMTree.Extras.Generators ()
import           Test.Tasty
import           Test.Tasty.QuickCheck

tests :: TestTree
tests = testGroup "Test.Database.LSMTree.Resolve"
    [ testGroup "Sum Word64" (allProperties @(Sum Word64))
    ]

allProperties ::
     forall v. (Show v, Arbitrary v, NFData v, ResolveValue v)
  => [TestTree]
allProperties =
    [ testProperty "prop_resolveValidOutput" $ withMaxSuccess 1000 $
        prop_resolveValidOutput @v
    , testProperty "prop_resolveAssociativity" $ withMaxSuccess 1000 $
        prop_resolveAssociativity @v
    ]

prop_resolveValidOutput ::
     forall v. (Show v, NFData v, ResolveValue v)
  => v -> v -> Property
prop_resolveValidOutput x y =
    counterexample ("inputs: " <> show (x, y)) $
      resolveValidOutput x y

prop_resolveAssociativity ::
     forall v. (Show v, ResolveValue v)
  => v -> v -> v -> Property
prop_resolveAssociativity x y z =
    counterexample ("inputs: " <> show (x, y)) $
      resolveAssociativity x y z
