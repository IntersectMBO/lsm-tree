{-# OPTIONS_GHC -Wno-orphans #-}
{-# OPTIONS_GHC -Wno-incomplete-uni-patterns #-}

module Test.Database.LSMTree.Model.Table (tests) where

import qualified Data.ByteString as BS
import qualified Data.Vector as V
import           Database.LSMTree (ResolveValue (..), resolveDeserialised)
import           Database.LSMTree.Common
import           Database.LSMTree.Model.Table (LookupResult (..), Table,
                     Update (..), lookups)
import qualified Database.LSMTree.Model.Table as Model
import           GHC.Exts (IsList (..))
import           Test.QuickCheck.Instances ()
import           Test.Tasty
import           Test.Tasty.QuickCheck

tests :: TestTree
tests = testGroup "Database.LSMTree.Model.Table"
    [ testProperty "lookup-insert" prop_lookupInsert
    , testProperty "lookup-delete" prop_lookupDelete
    , testProperty "insert-insert" prop_insertInsert
    , testProperty "upsert-insert" prop_upsertInsert
    , testProperty "upsert=lookup+insert" prop_upsertDef
    , testProperty "insert-commutes" prop_insertCommutes
    ]

type Key = BS.ByteString

newtype Value = Value BS.ByteString
  deriving stock (Eq, Show)
  deriving newtype (Arbitrary, SerialiseValue)

instance ResolveValue Value where
    resolveValue = resolveDeserialised resolve

resolve :: Value -> Value -> Value
resolve (Value x) (Value y) = Value (x <> y)

type Blob = BS.ByteString

type Tbl = Table Key Value Blob

updates :: V.Vector (Key, Update Value Blob) -> Table Key Value Blob -> Table Key Value Blob
updates = Model.updates Model.getResolve

inserts :: V.Vector (Key, Value, Maybe Blob) -> Table Key Value Blob -> Table Key Value Blob
inserts = Model.inserts Model.getResolve

deletes :: V.Vector Key -> Table Key Value Blob -> Table Key Value Blob
deletes = Model.deletes Model.getResolve

mupserts :: V.Vector (Key, Value) -> Table Key Value Blob -> Table Key Value Blob
mupserts = Model.mupserts Model.getResolve

-- | You can lookup what you inserted.
prop_lookupInsert :: Key -> Value -> Tbl -> Property
prop_lookupInsert k v tbl =
    lookups (V.singleton k) (inserts (V.singleton (k, v, Nothing)) tbl) === V.singleton (Found v)

-- | You cannot lookup what you have deleted
prop_lookupDelete :: Key -> Tbl -> Property
prop_lookupDelete k tbl =
    lookups (V.singleton k) (deletes (V.singleton k) tbl) === V.singleton NotFound

-- | Last insert wins.
prop_insertInsert :: Key -> Value -> Value -> Tbl -> Property
prop_insertInsert k v1 v2 tbl =
    inserts (V.fromList [(k, v1, Nothing), (k, v2, Nothing)]) tbl === inserts (V.singleton (k, v2, Nothing)) tbl

-- | Updating after insert is the same as inserting merged value.
--
-- Note: the order of merge.
prop_upsertInsert :: Key -> Value -> Value -> Tbl -> Property
prop_upsertInsert k v1 v2 tbl =
    updates (V.fromList [(k, Insert v1 Nothing), (k, Mupsert v2)]) tbl
      === inserts (V.singleton (k, resolve v2 v1, Nothing)) tbl

-- | Upsert is the same as lookup followed by an insert.
prop_upsertDef :: Key -> Value -> Tbl -> Property
prop_upsertDef k v tbl =
    tbl' === mupserts (V.singleton (k, v)) tbl
  where
    tbl' = case toList (lookups (V.singleton k) tbl) of
        [Found v'] -> inserts (V.singleton (k, resolve v v', Nothing)) tbl
        [FoundWithBlob v' _] -> inserts (V.singleton (k, resolve v v', Nothing)) tbl
        _          -> inserts (V.singleton (k, v, Nothing)) tbl

-- | Different key inserts commute.
prop_insertCommutes :: Key -> Value -> Key -> Value -> Tbl -> Property
prop_insertCommutes k1 v1 k2 v2 tbl = k1 /= k2 ==>
    inserts (V.fromList [(k1, v1, Nothing), (k2, v2, Nothing)]) tbl === inserts (V.fromList [(k2, v2, Nothing), (k1, v1, Nothing)]) tbl

instance (SerialiseKey k, SerialiseValue v, SerialiseValue b, Arbitrary k, Arbitrary v, Arbitrary b) => Arbitrary (Table k v b) where
    arbitrary = fromList <$> arbitrary
    shrink t  = fromList <$> shrink (toList t)
