{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}
module Test.Database.LSMTree.Common (tests) where

import           Control.DeepSeq (NFData, deepseq)
import qualified Data.ByteString as BS
import           Data.Proxy (Proxy (..))
import           Data.Word (Word64)
import           Database.LSMTree.Common (SomeSerialisationConstraint (..))
import           Test.QuickCheck.Instances ()
import           Test.Tasty
import           Test.Tasty.QuickCheck

tests :: TestTree
tests = testGroup "Database.LSMTree.Model.Common"
    [ testGroup "SomeSerialisationConstraint Word64"
       -- Note: unfortunately Arbitrary Word64 doesn't generate uniformly from whole range.
        [ decodeEncodeRoundtrip (Proxy @Word64)
        , totalDeserialiseProp (Proxy @Word64)
        , preservesOrderProp (Proxy @Word64)
        ]

    , testGroup "SomeSerialisationConstraint ByteString"
        [ decodeEncodeRoundtrip (Proxy @BS.ByteString)
        , totalDeserialiseProp (Proxy @BS.ByteString)
        , preservesOrderProp (Proxy @BS.ByteString)
        ]
    ]

-- | decode encode rountrip.
--
-- Note: this also indirectly tests that 'deserialise' doesn't fail
-- on bytestrings produced by 'serialise'.
--
-- All (?) serialisation libraries guarantee this.
--
decodeEncodeRoundtrip :: forall a.
    (Eq a, Show a, Arbitrary a, SomeSerialisationConstraint a)
    => Proxy a -> TestTree
decodeEncodeRoundtrip _ = testProperty "decode-encode roundtrip" prop where
    prop :: a -> Property
    prop x = deserialise (serialise x) === x

-- | Strong 'deserialise' check.
-- We check that 'deserialise' is indeed total function.
--
-- Hardly any general serialisation library guarantee this.
--
totalDeserialiseProp :: forall a.
    (NFData a, SomeSerialisationConstraint a)
    => Proxy a -> TestTree
totalDeserialiseProp _ = testProperty "deserialise total" prop where
    prop bs = property $ deserialise @a bs `deepseq` True

-- | Serialisation MUST preserve order.
preservesOrderProp :: forall a.
    (Ord a, Show a, Arbitrary a, SomeSerialisationConstraint a)
    => Proxy a -> TestTree
preservesOrderProp _ = testProperty "preserves order" prop where
    prop :: a -> a -> Property
    prop x y = compare x y === compare (serialise x) (serialise y)
