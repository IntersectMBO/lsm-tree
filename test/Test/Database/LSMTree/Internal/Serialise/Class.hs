{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}

module Test.Database.LSMTree.Internal.Serialise.Class (tests) where

import           Data.ByteString (ByteString)
import           Data.ByteString.Lazy (LazyByteString)
import           Data.ByteString.Short (ShortByteString)
import           Data.Primitive (ByteArray)
import           Data.Word
import           Database.LSMTree.Extras.Generators ()
import           Database.LSMTree.Internal.Serialise.Class
import           Test.Tasty
import           Test.Tasty.QuickCheck

tests :: TestTree
tests = testGroup "Test.Database.LSMTree.Internal.Serialise.Class" [
      testProperty "prop_roundtripSerialiseKey @Word64" $
        prop_roundtripSerialiseKey @Word64

    , testProperty "prop_roundtripSerialiseKey @ByteString" $
        prop_roundtripSerialiseKey @ByteString
    , testProperty "prop_roundtripSerialiseValue @ByteString" $
        prop_roundtripSerialiseValue @ByteString

    , testProperty "prop_roundtripSerialiseKey @LazyByteString" $
        prop_roundtripSerialiseKey @LazyByteString
    , testProperty "prop_roundtripSerialiseValue @LazyByteString" $
        prop_roundtripSerialiseValue @LazyByteString

    , testProperty "prop_roundtripSerialiseKey @ShortByteString" $
        prop_roundtripSerialiseKey @ShortByteString
    , testProperty "prop_roundtripSerialiseValue @ShortByteString" $
        prop_roundtripSerialiseValue @ShortByteString

    , testProperty "prop_roundtripSerialiseKey @ByteArray" $
        prop_roundtripSerialiseKey @ByteArray
    , testProperty "prop_roundtripSerialiseValue @ByteArray" $
        prop_roundtripSerialiseValue @ByteArray
    ]

prop_roundtripSerialiseKey :: forall k. (Eq k, Show k, SerialiseKey k) => k -> Property
prop_roundtripSerialiseKey k =
    counterexample ("serialised: " <> show (serialiseKey k)) $
    counterexample ("deserialised: " <> show @k (deserialiseKey (serialiseKey k))) $
      serialiseKeyIdentity k

prop_roundtripSerialiseValue :: forall v. (Eq v, Show v, SerialiseValue v) => v -> Property
prop_roundtripSerialiseValue v =
    counterexample ("serialised: " <> show (serialiseValue v)) $
    counterexample ("deserialised: " <> show @v (deserialiseValue (serialiseValue v))) $
      serialiseValueIdentity v
