{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}
module Test.Database.LSMTree.ModelIO.Normal (tests) where

import qualified Data.ByteString as BS
import           Data.Functor (void)
import           Data.List (sortOn)
import           Data.Proxy (Proxy (..))
import           Data.Word (Word64)
import           Database.LSMTree.ModelIO.Normal (LookupResult (..), Range (..),
                     RangeLookupResult (..), TableHandle, Update (..))
import           Test.Database.LSMTree.ModelIO.Class
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.QuickCheck
import           Test.Util.Orphans ()

tests :: TestTree
tests = testGroup "Database.LSMTree.ModelIO.Normal"
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
    ]
  where
    tbl = Proxy :: Proxy TableHandle

-------------------------------------------------------------------------------
-- test setup and helpers
-------------------------------------------------------------------------------

type Key = Word64
type Value = BS.ByteString
type Blob = BS.ByteString

makeNewTable :: forall h. IsTableHandle h => Proxy h
    -> [(Key, Update Value Blob)]
    -> IO (Session h IO, h IO Key Value Blob)
makeNewTable h ups = do
    s <- newSession
    hdl <- new @h s (testTableConfig h)
    updates hdl ups
    return (s, hdl)

-------------------------------------------------------------------------------
-- implement classic QC tests for basic k/v properties
-------------------------------------------------------------------------------

-- | You can lookup what you inserted.
prop_lookupInsert ::
     IsTableHandle h
  => Proxy h -> [(Key, Update Value Blob)]
  -> Key  -> Value -> Property
prop_lookupInsert h ups k v = ioProperty $ do
    -- create session, table handle, and populate it with some data.
    (_, hdl) <- makeNewTable h ups

    -- the main dish
    inserts hdl [(k, v, Nothing)]
    res <- lookups hdl [k]

    -- void makes blobrefs into ()
    return $ fmap void res === [Found k v]

-- | Insert doesn't change the lookup results of other keys.
prop_lookupInsertElse ::
     IsTableHandle h
  => Proxy h -> [(Key, Update Value Blob)]
  -> Key  -> Value -> [Key] -> Property
prop_lookupInsertElse h ups k v testKeys = ioProperty $ do
    -- create session, table handle, and populate it with some data.
    (_, hdl) <- makeNewTable h ups

    let testKeys' = filter (/= k) testKeys
    res1 <- lookups hdl testKeys'
    inserts hdl [(k, v, Nothing)]
    res2 <-  lookups hdl testKeys'

    -- void makes blobrefs into ()
    return $ fmap void res1 === fmap void res2

-- | You cannot lookup what you have just deleted
prop_lookupDelete ::
     IsTableHandle h
  => Proxy h -> [(Key, Update Value Blob)]
  -> Key -> Property
prop_lookupDelete h ups k = ioProperty $ do
    (_, hdl) <- makeNewTable h ups
    deletes hdl [k]
    res <- lookups hdl [k]
    return $ fmap void res === [NotFound k]

-- | Delete doesn't change the lookup results of other keys
prop_lookupDeleteElse ::
     IsTableHandle h
  => Proxy h -> [(Key, Update Value Blob)]
  -> Key  -> [Key] -> Property
prop_lookupDeleteElse h ups k testKeys = ioProperty $ do
    -- create session, table handle, and populate it with some data.
    (_, hdl) <- makeNewTable h ups

    let testKeys' = filter (/= k) testKeys
    res1 <- lookups hdl testKeys'
    deletes hdl [k]
    res2 <-  lookups hdl testKeys'

    -- void makes blobrefs into ()
    return $ fmap void res1 === fmap void res2

-- | Last insert wins.
prop_insertInsert ::
     IsTableHandle h
  => Proxy h -> [(Key, Update Value Blob)]
  -> Key -> Value -> Value -> Property
prop_insertInsert h ups k v1 v2 = ioProperty $ do
    (_, hdl) <- makeNewTable h ups
    inserts hdl [(k, v1, Nothing), (k, v2, Nothing)]
    res <- lookups hdl [k]
    return $ fmap void res === [Found k v2]

-- | Inserts with different keys don't interfere.
prop_insertCommutes ::
       IsTableHandle h
    => Proxy h -> [(Key, Update Value Blob)]
    -> Key -> Value -> Key -> Value -> Property
prop_insertCommutes h ups k1 v1 k2 v2 = k1 /= k2 ==> ioProperty do
    (_, hdl) <- makeNewTable h ups
    inserts hdl [(k1, v1, Nothing), (k2, v2, Nothing)]

    res <- lookups hdl [k1,k2]
    return $ fmap void res === [Found k1 v1, Found k2 v2]

-------------------------------------------------------------------------------
-- implement classic QC tests for range lookups
-------------------------------------------------------------------------------

evalRange :: Ord k => Range k -> k -> Bool
evalRange (FromToExcluding lo hi) x = lo <= x && x < hi
evalRange (FromToIncluding lo hi) x = lo <= x && x <= hi

rangeLookupResultKey :: RangeLookupResult k v b -> k
rangeLookupResultKey (FoundInRange k _)             = k
rangeLookupResultKey (FoundInRangeWithBlob k _ _  ) = k

-- | Last insert wins.
prop_insertLookupRange ::
     IsTableHandle h
  => Proxy h -> [(Key, Update Value Blob)]
  -> Key -> Value -> Range Key  -> Property
prop_insertLookupRange h ups k v r = ioProperty $ do
    (_, hdl) <- makeNewTable h ups

    res <- rangeLookup hdl r

    inserts hdl [(k, v, Nothing)]

    res' <- rangeLookup hdl r

    let p :: RangeLookupResult Key Value b -> Bool
        p rlr = rangeLookupResultKey rlr /= k

    if evalRange r k
    then return $ sortOn rangeLookupResultKey (FoundInRange k v : fmap void (filter p res)) === fmap void res'
    else return $ fmap void res === fmap void res'

-------------------------------------------------------------------------------
-- implement classic QC tests for split-value BLOB retrieval
-------------------------------------------------------------------------------

-- TODO: split-value?

-------------------------------------------------------------------------------
-- implement classic QC tests for monoidal updates
-------------------------------------------------------------------------------

{- Not applicable -}

-------------------------------------------------------------------------------
-- implement classic QC tests for monoidal table merges
-------------------------------------------------------------------------------

{- Not applicable -}

-------------------------------------------------------------------------------
-- implement classic QC tests for snapshots
-------------------------------------------------------------------------------

{- TODO -}

-------------------------------------------------------------------------------
-- implement classic QC tests for multiple writable table handles
--  - results of insert/delete, monoidal update, lookups, and range lookups should
--    be equal if applied to two duplicated table handles
--  - changes to one handle should not cause any visible changes in any others
-------------------------------------------------------------------------------

-- | Last insert wins.
prop_dupInsertInsert ::
     IsTableHandle h
  => Proxy h -> [(Key, Update Value Blob)]
  -> Key -> Value -> Value -> [Key] -> Property
prop_dupInsertInsert h ups k v1 v2 testKeys = ioProperty $ do
    (_, hdl1) <- makeNewTable h ups
    hdl2 <- duplicate hdl1

    inserts hdl1 [(k, v1, Nothing), (k, v2, Nothing)]
    inserts hdl2 [(k, v2, Nothing)]

    res1 <- lookups hdl1 testKeys
    res2 <- lookups hdl2 testKeys
    return $ fmap void res1 === fmap void res2

-- | Different key inserts commute.
prop_dupInsertCommutes ::
     IsTableHandle h
    => Proxy h -> [(Key, Update Value Blob)]
    -> Key -> Value -> Key -> Value -> [Key] -> Property
prop_dupInsertCommutes h ups k1 v1 k2 v2 testKeys = k1 /= k2 ==> ioProperty do
    (_, hdl1) <- makeNewTable h ups
    hdl2 <- duplicate hdl1

    inserts hdl1 [(k1, v1, Nothing), (k2, v2, Nothing)]
    inserts hdl2 [(k2, v2, Nothing), (k1, v1, Nothing)]

    res1 <- lookups hdl1 testKeys
    res2 <- lookups hdl2 testKeys
    return $ fmap void res1 === fmap void res2

-- changes to one handle should not cause any visible changes in any others
prop_dupNoChanges ::
     IsTableHandle h
    => Proxy h -> [(Key, Update Value Blob)]
    -> [(Key, Update Value Blob)] -> [Key] -> Property
prop_dupNoChanges h ups ups' testKeys = ioProperty $ do
    (_, hdl1) <- makeNewTable h ups

    res <- lookups hdl1 testKeys

    hdl2 <- duplicate hdl1
    updates hdl2 ups'

    -- lookup hdl1 again.
    res' <- lookups hdl1 testKeys

    return $ fmap void res == fmap void res'
