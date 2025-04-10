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
     forall v. (Show v, Arbitrary v, NFData v, SerialiseValue v, ResolveValue v)
  => [TestTree]
allProperties =
    [ testProperty "prop_resolveValueValidOutput" $ withMaxSuccess 1000 $
        prop_resolveValueValidOutput @v
    , testProperty "prop_resolveValueAssociativity" $ withMaxSuccess 1000 $
        prop_resolveValueAssociativity @v
    ]

prop_resolveValueValidOutput ::
     forall v. (Show v, NFData v, SerialiseValue v, ResolveValue v)
  => v -> v -> Property
prop_resolveValueValidOutput x y =
    counterexample ("inputs: " <> show (x, y)) $
      resolveValueValidOutput x y

prop_resolveValueAssociativity ::
     forall v. (Show v, SerialiseValue v, ResolveValue v)
  => v -> v -> v -> Property
prop_resolveValueAssociativity x y z =
    counterexample ("inputs: " <> show (x, y)) $
      resolveValueAssociativity x y z
