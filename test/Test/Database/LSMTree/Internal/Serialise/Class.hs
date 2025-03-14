{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}

module Test.Database.LSMTree.Internal.Serialise.Class (tests) where

import           Data.ByteString (ByteString)
import           Data.ByteString.Lazy (LazyByteString)
import           Data.ByteString.Short (ShortByteString)
import           Data.Primitive (ByteArray)
import           Data.WideWord (Word128, Word256)
import           Data.Word
import           Database.LSMTree.Extras.Generators ()
import           Database.LSMTree.Extras.UTxO (UTxOKey, UTxOValue)
import           Database.LSMTree.Internal.Serialise.Class
import           Test.Tasty
import           Test.Tasty.QuickCheck

tests :: TestTree
tests = testGroup "Test.Database.LSMTree.Internal.Serialise.Class"
    [ testGroup "Word64"          (allProperties @Word64 True)
    , testGroup "ByteString"      (allProperties @ByteString True)
    , testGroup "LazyByteString"  (allProperties @LazyByteString True)
    , testGroup "ShortByteString" (allProperties @ShortByteString True)
    , testGroup "ByteArray"       (valueProperties @ByteArray)
    , testGroup "Word128"         (allProperties @Word128 True)
    , testGroup "Word256"         (allProperties @Word256 True)
    , testGroup "UTxOKey"         (keyProperties @UTxOKey False)
    , testGroup "UTxOValue"       (valueProperties @UTxOValue)
    ]

allProperties :: forall a. (Ord a, Show a, Arbitrary a, SerialiseKey a, SerialiseValue a) => Bool -> [TestTree]
allProperties orderPreserving = keyProperties @a orderPreserving <> valueProperties @a

keyProperties :: forall a. (Ord a, Show a, Arbitrary a, SerialiseKey a) => Bool -> [TestTree]
keyProperties orderPreserving =
    [ testProperty "prop_roundtripSerialiseKey" $
        prop_roundtripSerialiseKey @a
    , testProperty "prop_roundtripSerialiseKeyUpToSlicing" $
        prop_roundtripSerialiseKeyUpToSlicing @a
    , testProperty "prop_orderPreservationSerialiseKey" $ \k1 k2 ->
        (if orderPreserving then id else expectFailure)
          (prop_orderPreservationSerialiseKey @a k1 k2)
    ]

valueProperties :: forall a. (Ord a, Show a, Arbitrary a, SerialiseValue a) => [TestTree]
valueProperties =
    [ testProperty "prop_roundtripSerialiseValue" $
        prop_roundtripSerialiseValue @a
    , testProperty "prop_roundtripSerialiseValueUpToSlicing" $
        prop_roundtripSerialiseValueUpToSlicing @a
    ]

prop_roundtripSerialiseKey :: forall k. (Eq k, Show k, SerialiseKey k) => k -> Property
prop_roundtripSerialiseKey k =
    counterexample ("serialised: " <> show (serialiseKey k)) $
    counterexample ("deserialised: " <> show @k (deserialiseKey (serialiseKey k))) $
      serialiseKeyIdentity k

prop_roundtripSerialiseKeyUpToSlicing ::
     forall k. (Eq k, Show k, SerialiseKey k)
  => RawBytes -> k -> RawBytes -> Property
prop_roundtripSerialiseKeyUpToSlicing prefix x suffix =
    counterexample ("serialised: " <> show @RawBytes k) $
    counterexample ("serialised and sliced: " <> show @RawBytes k') $
    counterexample ("deserialised: " <> show @k x') $
      serialiseKeyIdentityUpToSlicing prefix x suffix
  where
    k = serialiseKey x
    k' = packSlice prefix k suffix
    x' = deserialiseKey k'

prop_orderPreservationSerialiseKey :: forall k. (Ord k, SerialiseKey k) => k -> k -> Property
prop_orderPreservationSerialiseKey x y =
    counterexample ("serialised: " <> show (serialiseKey x, serialiseKey y)) $
    counterexample ("compare: " <> show (compare x y)) $
    counterexample ("compare serialised: " <> show (compare (serialiseKey x) (serialiseKey y))) $
      serialiseKeyPreservesOrdering x y

prop_roundtripSerialiseValue :: forall v. (Eq v, Show v, SerialiseValue v) => v -> Property
prop_roundtripSerialiseValue v =
    counterexample ("serialised: " <> show (serialiseValue v)) $
    counterexample ("deserialised: " <> show @v (deserialiseValue (serialiseValue v))) $
      serialiseValueIdentity v

prop_roundtripSerialiseValueUpToSlicing ::
     forall v. (Eq v, Show v, SerialiseValue v)
  => RawBytes -> v -> RawBytes -> Property
prop_roundtripSerialiseValueUpToSlicing prefix x suffix =
    counterexample ("serialised: " <> show v) $
    counterexample ("serialised and sliced: " <> show @RawBytes v') $
    counterexample ("deserialised: " <> show @v x') $
      serialiseValueIdentityUpToSlicing prefix x suffix
  where
    v = serialiseValue x
    v' = packSlice prefix v suffix
    x' = deserialiseValue v'
