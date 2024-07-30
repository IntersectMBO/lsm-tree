{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}

module Test.Database.LSMTree.Monoidal (tests) where

import           Control.DeepSeq (NFData)
import           Data.Proxy (Proxy (Proxy))
import           Data.Word
import           Database.LSMTree.Extras.Generators ()
import           Database.LSMTree.Internal.RawBytes (RawBytes)
import           Database.LSMTree.Monoidal
import           Test.Tasty
import           Test.Tasty.QuickCheck

tests :: TestTree
tests = testGroup "Test.Database.LSMTree.Monoidal"
    [ testGroup "Word64" (allProperties @Word64 False)
      -- TODO: revisit totality (drop requirement or fix @SerialiseValue Word64@)
    ]

allProperties ::
     forall v. (Show v, Arbitrary v, NFData v, SerialiseValue v, ResolveValue v)
  => Bool -> [TestTree]
allProperties expectTotality =
    [ testProperty "prop_resolveValueValidOutput" $ withMaxSuccess 1000 $
        prop_resolveValueValidOutput @v
    , testProperty "prop_resolveValueAssociativity" $ withMaxSuccess 1000 $
        prop_resolveValueAssociativity @v
    , testProperty "prop_resolveValueTotality" $ withMaxSuccess 1000 $ \x y ->
        (if expectTotality then id else expectFailure) $
          prop_resolveValueTotality @v x y
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

prop_resolveValueTotality ::
     forall v. ResolveValue v
  => RawBytes -> RawBytes -> Property
prop_resolveValueTotality x y =
    counterexample ("inputs: " <> show (x, y)) $
      resolveValueTotality (Proxy @v) x y
