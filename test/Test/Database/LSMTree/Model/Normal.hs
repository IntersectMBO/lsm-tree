{-# OPTIONS_GHC -Wno-orphans #-}
module Test.Database.LSMTree.Model.Normal (tests) where

import qualified Data.ByteString as BS
import qualified Data.Vector as V
import           Database.LSMTree.Model.Normal
import           GHC.Exts (IsList (..))
import           Test.QuickCheck.Instances ()
import           Test.Tasty
import           Test.Tasty.QuickCheck

tests :: TestTree
tests = testGroup "Database.LSMTree.Model.Normal"
    [ testProperty "lookup-insert" prop_lookupInsert
    , testProperty "lookup-delete" prop_lookupDelete
    , testProperty "insert-insert" prop_insertInsert
    , testProperty "insert-commutes" prop_insertCommutes
    ]

type Key = BS.ByteString
type Value = BS.ByteString
type Blob = BS.ByteString

type Tbl = Table Key Value Blob

-- | You can lookup what you inserted.
prop_lookupInsert :: Key -> Value -> Tbl -> Property
prop_lookupInsert k v tbl =
    lookups (V.singleton k) (inserts [(k, v, Nothing)] tbl) === V.singleton (Found v)

-- | You cannot lookup what you have deleted
prop_lookupDelete :: Key -> Tbl -> Property
prop_lookupDelete k tbl =
    lookups (V.singleton k) (deletes [k] tbl) === V.singleton NotFound

-- | Last insert wins.
prop_insertInsert :: Key -> Key -> Value -> Tbl -> Property
prop_insertInsert k v1 v2 tbl =
    inserts [(k, v1, Nothing), (k, v2, Nothing)] tbl === inserts [(k, v2, Nothing)] tbl

-- | Different key inserts commute.
prop_insertCommutes :: Key -> Value -> Key -> Value -> Tbl -> Property
prop_insertCommutes k1 v1 k2 v2 tbl = k1 /= k2 ==>
    inserts [(k1, v1, Nothing), (k2, v2, Nothing)] tbl === inserts [(k2, v2, Nothing), (k1, v1, Nothing)] tbl

instance (SerialiseKey k, SerialiseValue v, SerialiseValue blob, Arbitrary k, Arbitrary v, Arbitrary blob) => Arbitrary (Table k v blob) where
    arbitrary = fromList <$> arbitrary
    shrink t  = fromList <$> shrink (toList t)
