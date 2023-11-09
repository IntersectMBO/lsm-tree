{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}
module Test.Database.LSMTree.ModelIO.Monoidal (tests) where

import qualified Data.ByteString as BS
import           Data.List (sortOn)
import           Data.Maybe (fromMaybe)
import           Data.Proxy (Proxy (..))
import           Data.Word (Word64)
import           Database.LSMTree.Common (SomeUpdateConstraint (..),
                     mkSnapshotName)
import           Database.LSMTree.ModelIO.Monoidal (LookupResult (..),
                     Range (..), RangeLookupResult (..), TableHandle,
                     Update (..))
import           Test.Database.LSMTree.ModelIO.Monoidal.Class
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.QuickCheck
import           Test.Util.Orphans ()

tests :: TestTree
tests = testGroup "Database.LSMTree.ModelIO.Monoidal"
    [ testProperty "lookup-insert" $ prop_lookupInsert tbl
    , testProperty "lookup-insert-else" $ prop_lookupInsertElse tbl
    , testProperty "lookup-delete" $ prop_lookupDelete tbl
    , testProperty "lookup-delete-else" $ prop_lookupDeleteElse tbl
    , testProperty "insert-insert" $ prop_insertInsert tbl
    , testProperty "insert-commutes" $ prop_insertCommutes tbl
    , testProperty "dup-insert-insert" $ prop_dupInsertInsert tbl
    , testProperty "dup-insert-comm" $ prop_dupInsertCommutes tbl
    , testProperty "dup-nochanges" $ prop_dupNoChanges tbl
    , testProperty "lookupRange-insert" $ prop_insertLookupRange tbl
    , testProperty "snapshot-nochanges" $ prop_snapshotNoChanges tbl
    , testProperty "snapshot-nochanges2" $ prop_snapshotNoChanges2 tbl
    , testProperty "lookup-mupsert" $ prop_lookupUpdate tbl
    , testProperty "merge" $ prop_merge tbl
    ]
  where
    tbl = Proxy :: Proxy TableHandle

-------------------------------------------------------------------------------
-- test setup and helpers
-------------------------------------------------------------------------------

type Key = Word64
type Value = BS.ByteString

makeNewTable :: forall h. IsTableHandle h => Proxy h
    -> [(Key, Update Value)]
    -> IO (Session h IO, h IO Key Value)
makeNewTable h ups = do
    s <- openSession
    hdl <- new @h s (testTableConfig h)
    updates hdl ups
    return (s, hdl)

-------------------------------------------------------------------------------
-- implement classic QC tests for basic k/v properties
-------------------------------------------------------------------------------

-- | You can lookup what you inserted.
prop_lookupInsert ::
     IsTableHandle h
  => Proxy h -> [(Key, Update Value)]
  -> Key  -> Value -> Property
prop_lookupInsert h ups k v = ioProperty $ do
    -- create session, table handle, and populate it with some data.
    (_, hdl) <- makeNewTable h ups

    -- the main dish
    inserts hdl [(k, v)]
    res <- lookups hdl [k]

    return $ res === [Found k v]

-- | Insert doesn't change the lookup results of other keys.
prop_lookupInsertElse ::
     IsTableHandle h
  => Proxy h -> [(Key, Update Value)]
  -> Key  -> Value -> [Key] -> Property
prop_lookupInsertElse h ups k v testKeys = ioProperty $ do
    -- create session, table handle, and populate it with some data.
    (_, hdl) <- makeNewTable h ups

    let testKeys' = filter (/= k) testKeys
    res1 <- lookups hdl testKeys'
    inserts hdl [(k, v)]
    res2 <-  lookups hdl testKeys'

    return $ res1 === res2

-- | You cannot lookup what you have just deleted
prop_lookupDelete ::
     IsTableHandle h
  => Proxy h -> [(Key, Update Value)]
  -> Key -> Property
prop_lookupDelete h ups k = ioProperty $ do
    (_, hdl) <- makeNewTable h ups
    deletes hdl [k]
    res <- lookups hdl [k]
    return $ res === [NotFound k]

-- | Delete doesn't change the lookup results of other keys
prop_lookupDeleteElse ::
     IsTableHandle h
  => Proxy h -> [(Key, Update Value)]
  -> Key  -> [Key] -> Property
prop_lookupDeleteElse h ups k testKeys = ioProperty $ do
    -- create session, table handle, and populate it with some data.
    (_, hdl) <- makeNewTable h ups

    let testKeys' = filter (/= k) testKeys
    res1 <- lookups hdl testKeys'
    deletes hdl [k]
    res2 <-  lookups hdl testKeys'

    return $ res1 === res2

-- | Last insert wins.
prop_insertInsert ::
     IsTableHandle h
  => Proxy h -> [(Key, Update Value)]
  -> Key -> Value -> Value -> Property
prop_insertInsert h ups k v1 v2 = ioProperty $ do
    (_, hdl) <- makeNewTable h ups
    inserts hdl [(k, v1), (k, v2)]
    res <- lookups hdl [k]
    return $ res === [Found k v2]

-- | Inserts with different keys don't interfere.
prop_insertCommutes ::
       IsTableHandle h
    => Proxy h -> [(Key, Update Value)]
    -> Key -> Value -> Key -> Value -> Property
prop_insertCommutes h ups k1 v1 k2 v2 = k1 /= k2 ==> ioProperty do
    (_, hdl) <- makeNewTable h ups
    inserts hdl [(k1, v1), (k2, v2)]

    res <- lookups hdl [k1,k2]
    return $ res === [Found k1 v1, Found k2 v2]

-------------------------------------------------------------------------------
-- implement classic QC tests for range lookups
-------------------------------------------------------------------------------

evalRange :: Ord k => Range k -> k -> Bool
evalRange (FromToExcluding lo hi) x = lo <= x && x < hi
evalRange (FromToIncluding lo hi) x = lo <= x && x <= hi

rangeLookupResultKey :: RangeLookupResult k v -> k
rangeLookupResultKey (FoundInRange k _)             = k

-- | Last insert wins.
prop_insertLookupRange ::
     IsTableHandle h
  => Proxy h -> [(Key, Update Value)]
  -> Key -> Value -> Range Key  -> Property
prop_insertLookupRange h ups k v r = ioProperty $ do
    (_, hdl) <- makeNewTable h ups

    res <- rangeLookup hdl r

    inserts hdl [(k, v)]

    res' <- rangeLookup hdl r

    let p :: RangeLookupResult Key Value -> Bool
        p rlr = rangeLookupResultKey rlr /= k

    if evalRange r k
    then return $ sortOn rangeLookupResultKey (FoundInRange k v : filter p res) === res'
    else return $ res === res'

-------------------------------------------------------------------------------
-- implement classic QC tests for split-value BLOB retrieval
-------------------------------------------------------------------------------

{- not applicable -}

-------------------------------------------------------------------------------
-- implement classic QC tests for monoidal updates
-------------------------------------------------------------------------------

-- | You can lookup what you inserted.
prop_lookupUpdate ::
     IsTableHandle h
  => Proxy h -> [(Key, Update Value)]
  -> Key  -> Value -> Value -> Property
prop_lookupUpdate h ups k v1 v2 = ioProperty $ do
    -- create session, table handle, and populate it with some data.
    (_, hdl) <- makeNewTable h ups

    -- the main dish
    inserts hdl [(k, v1)]
    mupserts hdl [(k, v2)]
    res <- lookups hdl [k]

    -- notice the order.
    return $ res === [Found k $ mergeU v2 v1]

-------------------------------------------------------------------------------
-- implement classic QC tests for monoidal table merges
-------------------------------------------------------------------------------

prop_merge :: forall h.
     IsTableHandle h
  => Proxy h -> [(Key, Update Value)] -> [(Key, Update Value)]
  -> [Key] -> Property
prop_merge h ups1 ups2 testKeys = ioProperty $ do
    -- create two tables, from ups1 and ups2
    (s, hdl1) <- makeNewTable h ups1

    hdl2 <- new @h s (testTableConfig h)
    updates hdl2 ups2

    -- merge them.
    hdl3 <- merge hdl1 hdl2

    -- results in parts and the merge table
    res1 <- lookups hdl1 testKeys
    res2 <- lookups hdl2 testKeys
    res3 <- lookups hdl3 testKeys

    let mergeResult :: LookupResult Key Value -> LookupResult Key Value -> LookupResult Key Value
        mergeResult r@(NotFound _)   (NotFound _) = r
        mergeResult   (NotFound _) r@(Found _ _)  = r
        mergeResult r@(Found _ _)    (NotFound _) = r
        mergeResult   (Found k v1)   (Found _ v2) = Found k (mergeU v1 v2)

    return $ zipWith mergeResult res1 res2  == res3

-------------------------------------------------------------------------------
-- implement classic QC tests for snapshots
-------------------------------------------------------------------------------

-- changes to handle would not change the snapshot
prop_snapshotNoChanges :: forall h.
     IsTableHandle h
    => Proxy h -> [(Key, Update Value)]
    -> [(Key, Update Value)] -> [Key] -> Property
prop_snapshotNoChanges h ups ups' testKeys = ioProperty $ do
    (sess, hdl1) <- makeNewTable h ups

    res <- lookups hdl1 testKeys

    let name = fromMaybe (error "invalid name") $ mkSnapshotName "foo"

    snapshot name hdl1
    updates hdl1 ups'

    hdl2 <- open @h sess name

    res' <- lookups hdl2 testKeys

    return $ res == res'

-- same snapshot may be opened multiple times,
-- and the handles are separate.
prop_snapshotNoChanges2 :: forall h.
     IsTableHandle h
    => Proxy h -> [(Key, Update Value)]
    -> [(Key, Update Value)] -> [Key] -> Property
prop_snapshotNoChanges2 h ups ups' testKeys = ioProperty $ do
    (sess, hdl0) <- makeNewTable h ups
    let name = fromMaybe (error "invalid name") $ mkSnapshotName "foo"
    snapshot name hdl0

    hdl1 <- open @h sess name
    hdl2 <- open @h sess name

    res <- lookups hdl1 testKeys
    updates hdl1 ups'
    res' <- lookups hdl2 testKeys

    return $ res == res'

-------------------------------------------------------------------------------
-- implement classic QC tests for multiple writable table handles
--  - results of insert/delete, monoidal update, lookups, and range lookups should
--    be equal if applied to two duplicated table handles
--  - changes to one handle should not cause any visible changes in any others
-------------------------------------------------------------------------------

-- | Last insert wins.
prop_dupInsertInsert ::
     IsTableHandle h
  => Proxy h -> [(Key, Update Value)]
  -> Key -> Value -> Value -> [Key] -> Property
prop_dupInsertInsert h ups k v1 v2 testKeys = ioProperty $ do
    (_, hdl1) <- makeNewTable h ups
    hdl2 <- duplicate hdl1

    inserts hdl1 [(k, v1), (k, v2)]
    inserts hdl2 [(k, v2)]

    res1 <- lookups hdl1 testKeys
    res2 <- lookups hdl2 testKeys
    return $ res1 === res2

-- | Different key inserts commute.
prop_dupInsertCommutes ::
     IsTableHandle h
    => Proxy h -> [(Key, Update Value)]
    -> Key -> Value -> Key -> Value -> [Key] -> Property
prop_dupInsertCommutes h ups k1 v1 k2 v2 testKeys = k1 /= k2 ==> ioProperty do
    (_, hdl1) <- makeNewTable h ups
    hdl2 <- duplicate hdl1

    inserts hdl1 [(k1, v1), (k2, v2)]
    inserts hdl2 [(k2, v2), (k1, v1)]

    res1 <- lookups hdl1 testKeys
    res2 <- lookups hdl2 testKeys
    return $ res1 === res2

-- changes to one handle should not cause any visible changes in any others
prop_dupNoChanges ::
     IsTableHandle h
    => Proxy h -> [(Key, Update Value)]
    -> [(Key, Update Value)] -> [Key] -> Property
prop_dupNoChanges h ups ups' testKeys = ioProperty $ do
    (_, hdl1) <- makeNewTable h ups

    res <- lookups hdl1 testKeys

    hdl2 <- duplicate hdl1
    updates hdl2 ups'

    -- lookup hdl1 again.
    res' <- lookups hdl1 testKeys

    return $ res == res'

