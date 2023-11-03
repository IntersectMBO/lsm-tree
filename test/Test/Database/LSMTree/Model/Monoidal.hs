{-# OPTIONS_GHC -Wno-orphans #-}
module Test.Database.LSMTree.Model.Monoidal (tests) where

import qualified Data.ByteString as BS
import           Database.LSMTree.Model.Monoidal
import           GHC.Exts (IsList (..))
import           Test.Tasty
import           Test.Tasty.QuickCheck
import           Test.Util.Orphans ()

tests :: TestTree
tests = testGroup "Database.LSMTree.Model.Monoidal"
    [ testProperty "lookup-insert" prop_lookupInsert
    , testProperty "lookup-delete" prop_lookupDelete
    , testProperty "insert-insert" prop_insertInsert
    , testProperty "upsert-insert" prop_upsertInsert
    , testProperty "upsert=lookup+insert" prop_upsertDef
    , testProperty "insert-commutes" prop_insertCommutes
    ]

type Key = BS.ByteString
type Value = BS.ByteString

type Tbl = Table Key Value

-- | You can lookup what you inserted.
prop_lookupInsert :: Key -> Value -> Tbl -> Property
prop_lookupInsert k v tbl =
    lookups [k] (inserts [(k, v)] tbl) === [Found k v]

-- | You cannot lookup what you have deleted
prop_lookupDelete :: Key -> Tbl -> Property
prop_lookupDelete k tbl =
    lookups [k] (deletes [k] tbl) === [NotFound k]

-- | Last insert wins.
prop_insertInsert :: Key -> Key -> Value -> Tbl -> Property
prop_insertInsert k v1 v2 tbl =
    inserts [(k, v1), (k, v2)] tbl === inserts [(k, v2)] tbl

-- | Updating after insert is the same as inserting merged value.
--
-- Note: the order of merge.
prop_upsertInsert :: Key -> Key -> Value -> Tbl -> Property
prop_upsertInsert k v1 v2 tbl =
    updates [(k, Insert v1), (k, Mupsert v2)] tbl === inserts [(k, mergeU v2 v1)] tbl

-- | Upsert is the same as lookup followed by an insert.
prop_upsertDef :: Key -> Value -> Tbl -> Property
prop_upsertDef k v tbl =
    tbl' === mupserts [(k, v)] tbl
  where
    tbl' = case lookups [k] tbl of
        [Found _ v'] -> inserts [(k, mergeU v v')] tbl
        _            -> inserts [(k, v)] tbl

-- | Different key inserts commute.
prop_insertCommutes :: Key -> Value -> Key -> Value -> Tbl -> Property
prop_insertCommutes k1 v1 k2 v2 tbl = k1 /= k2 ==>
    inserts [(k1, v1), (k2, v2)] tbl === inserts [(k2, v2), (k1, v1)] tbl

instance (SomeSerialisationConstraint k, SomeSerialisationConstraint v, Arbitrary k, Arbitrary v) => Arbitrary (Table k v) where
    arbitrary = fromList <$> arbitrary
    shrink t  = fromList <$> shrink (toList t)
