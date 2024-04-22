{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}

module Test.Database.LSMTree.Internal.Serialise.Class (tests) where

import           Data.ByteString (ByteString)
import           Data.ByteString.Lazy (LazyByteString)
import           Data.ByteString.Short (ShortByteString)
import           Data.Primitive (ByteArray)
import           Data.Proxy (Proxy (Proxy))
import           Data.Word
import           Database.LSMTree.Extras.Generators ()
import qualified Database.LSMTree.Internal.RawBytes as RB
import           Database.LSMTree.Internal.Serialise.Class
import           Test.Tasty
import           Test.Tasty.QuickCheck

tests :: TestTree
tests = testGroup "Test.Database.LSMTree.Internal.Serialise.Class"
    [ testGroup "Word64"          (allProperties @Word64)
    , testGroup "ByteString"      (allProperties @ByteString)
    , testGroup "LazyByteString"  (allProperties @LazyByteString)
    , testGroup "ShortByteString" (allProperties @ShortByteString)
    , testGroup "ByteArray"       (valueProperties @ByteArray)
    ]

allProperties :: forall a. (Ord a, Show a, Arbitrary a, SerialiseKey a, SerialiseValue a) => [TestTree]
allProperties = keyProperties @a <> valueProperties @a

keyProperties :: forall a. (Ord a, Show a, Arbitrary a, SerialiseKey a) => [TestTree]
keyProperties =
    [ testProperty "prop_roundtripSerialiseKey" $
        prop_roundtripSerialiseKey @a
    , testProperty "prop_orderPreservationSerialiseKey" $
        prop_orderPreservationSerialiseKey @a
    ]

valueProperties :: forall a. (Ord a, Show a, Arbitrary a, SerialiseValue a) => [TestTree]
valueProperties =
    [ testProperty "prop_roundtripSerialiseValue" $
        prop_roundtripSerialiseValue @a
    , testProperty "prop_concatDistributesSerialiseValue" $
        prop_concatDistributesSerialiseValue @a
    ]

prop_roundtripSerialiseKey :: forall k. (Eq k, Show k, SerialiseKey k) => k -> Property
prop_roundtripSerialiseKey k =
    counterexample ("serialised: " <> show (serialiseKey k)) $
    counterexample ("deserialised: " <> show @k (deserialiseKey (serialiseKey k))) $
      serialiseKeyIdentity k

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

prop_concatDistributesSerialiseValue :: forall v. (Ord v, Show v, SerialiseValue v) => v -> Property
prop_concatDistributesSerialiseValue v =
    forAllShrink (genChunks bytes) shrinkChunks $ (. map (RB.pack)) $ \chs ->
      counterexample ("from chunks: " <> show (deserialiseValueN @v chs)) $
      counterexample ("from whole: " <> show (deserialiseValue @v (mconcat chs))) $
        serialiseValueConcatDistributes (Proxy @v) chs
  where
    bytes = RB.unpack (serialiseValue v)

-- | Randomly splits the input list into non-empty chunks.
genChunks :: [a] -> Gen [[a]]
genChunks [] = pure []
genChunks xs = do
    n <- chooseInt (1, length xs)
    let (pre, post) = splitAt n xs
    (pre :) <$> genChunks post

-- | Shrinks by appending chunks where possible
shrinkChunks :: [[a]] -> [[[a]]]
shrinkChunks (x : y : ys) =
      ((x <> y) : ys)
    : map (x :) (shrinkChunks (y : ys))
shrinkChunks _ = []
