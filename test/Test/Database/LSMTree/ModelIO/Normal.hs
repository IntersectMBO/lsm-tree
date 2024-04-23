{-# LANGUAGE BlockArguments      #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}
{-# LANGUAGE TypeFamilies        #-}

module Test.Database.LSMTree.ModelIO.Normal (tests) where

import           Control.Exception (SomeException, try)
import           Control.Monad.Trans.State
import qualified Data.ByteString as BS
import           Data.Foldable (toList)
import           Data.Functor.Compose (Compose (..))
import           Data.List (sortOn)
import           Data.Maybe (fromMaybe)
import           Data.Proxy (Proxy (..))
import           Data.Word (Word64)
import           Database.LSMTree.Common (mkSnapshotName)
import           Database.LSMTree.Extras.Generators ()
import           Database.LSMTree.ModelIO.Normal (IOLike, LookupResult (..),
                     Range (..), RangeLookupResult (..), SerialiseKey,
                     SerialiseValue, TableHandle, Update (..))
import           Test.Database.LSMTree.ModelIO.Class
import           Test.QuickCheck.Monadic (monadicIO, monitor, run)
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.QuickCheck

tests :: TestTree
tests = testGroup "Database.LSMTree.ModelIO.Normal"
    [ testProperty "lookup-insert" $ prop_lookupInsert tbl
    , testProperty "lookup-insert-else" $ prop_lookupInsertElse tbl
    , testProperty "lookup-insert-blob" $ prop_lookupInsertBlob tbl
    , testProperty "lookup-delete" $ prop_lookupDelete tbl
    , testProperty "lookup-delete-else" $ prop_lookupDeleteElse tbl
    , testProperty "insert-insert" $ prop_insertInsert tbl
    , testProperty "insert-insert-blob" $ prop_insertInsertBlob tbl
    , testProperty "insert-commutes" $ prop_insertCommutes tbl
    , testProperty "insert-commutes-blob" $ prop_insertCommutesBlob tbl
    , testProperty "invalidated-blob-references" $ prop_updatesMayInvalidateBlobRefs tbl
    , testProperty "dup-insert-insert" $ prop_dupInsertInsert tbl
    , testProperty "dup-insert-comm" $ prop_dupInsertCommutes tbl
    , testProperty "dup-nochanges" $ prop_dupNoChanges tbl
    , testProperty "lookupRange-insert" $ prop_insertLookupRange tbl
    , testProperty "snapshot-nochanges" $ prop_snapshotNoChanges tbl
    , testProperty "snapshot-nochanges2" $ prop_snapshotNoChanges2 tbl
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
    s <- openSession
    hdl <- new @h s (testTableConfig h)
    updates hdl ups
    return (s, hdl)

-- | Like 'retrieveBlobs' but works for any 'Traversable'.
--
-- Like 'partsOf' in @lens@ this uses state monad.
retrieveBlobsTrav ::
  (IsTableHandle h, IOLike m, SerialiseValue blob, Traversable t)
  => proxy h -> Session h m -> t (BlobRef h m blob) -> m (t blob)
retrieveBlobsTrav hdl ses brefs = do
  blobs <- retrieveBlobs hdl ses (toList brefs)
  evalStateT (traverse (\_ -> state un) brefs) blobs
  where
    un []     = error "invalid traversal"
    un (x:xs) = (x, xs)

lookupsWithBlobs ::
    forall h m k v blob. ( IsTableHandle h, IOLike m
    , SerialiseKey k, SerialiseValue v, SerialiseValue blob
    )
  => h m k v blob -> Session h m -> [k] -> m [LookupResult k v blob]
lookupsWithBlobs hdl ses ks = do
    res <- lookups hdl ks
    getCompose <$> retrieveBlobsTrav (Proxy @h) ses (Compose res)

rangeLookupWithBlobs ::
     forall h m k v blob. ( IsTableHandle h, IOLike m
     , SerialiseKey k, SerialiseValue v, SerialiseValue blob
     )
  => h m k v blob -> Session h m -> Range k -> m [RangeLookupResult k v blob]
rangeLookupWithBlobs hdl ses r = do
    res <- rangeLookup hdl r
    getCompose <$> retrieveBlobsTrav (Proxy @h) ses (Compose res)

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
    (ses, hdl) <- makeNewTable h ups

    -- the main dish
    inserts hdl [(k, v, Nothing)]
    res <- lookupsWithBlobs hdl ses [k]

    return $ res === [Found k v]

-- | Insert doesn't change the lookup results of other keys.
prop_lookupInsertElse ::
     IsTableHandle h
  => Proxy h -> [(Key, Update Value Blob)]
  -> Key  -> Value -> [Key] -> Property
prop_lookupInsertElse h ups k v testKeys = ioProperty $ do
    -- create session, table handle, and populate it with some data.
    (ses, hdl) <- makeNewTable h ups

    let testKeys' = filter (/= k) testKeys
    res1 <- lookupsWithBlobs hdl ses testKeys'
    inserts hdl [(k, v, Nothing)]
    res2 <-  lookupsWithBlobs hdl ses testKeys'

    return $ res1 === res2

-- | You cannot lookup what you have just deleted
prop_lookupDelete ::
     IsTableHandle h
  => Proxy h -> [(Key, Update Value Blob)]
  -> Key -> Property
prop_lookupDelete h ups k = ioProperty $ do
    (ses, hdl) <- makeNewTable h ups
    deletes hdl [k]
    res <- lookupsWithBlobs hdl ses [k]
    return $ res === [NotFound k]

-- | Delete doesn't change the lookup results of other keys
prop_lookupDeleteElse ::
     IsTableHandle h
  => Proxy h -> [(Key, Update Value Blob)]
  -> Key  -> [Key] -> Property
prop_lookupDeleteElse h ups k testKeys = ioProperty $ do
    -- create session, table handle, and populate it with some data.
    (ses, hdl) <- makeNewTable h ups

    let testKeys' = filter (/= k) testKeys
    res1 <- lookupsWithBlobs hdl ses testKeys'
    deletes hdl [k]
    res2 <-  lookupsWithBlobs hdl ses testKeys'

    return $ res1 === res2

-- | Last insert wins.
prop_insertInsert ::
     IsTableHandle h
  => Proxy h -> [(Key, Update Value Blob)]
  -> Key -> Value -> Value -> Property
prop_insertInsert h ups k v1 v2 = ioProperty $ do
    (ses, hdl) <- makeNewTable h ups
    inserts hdl [(k, v1, Nothing), (k, v2, Nothing)]
    res <- lookupsWithBlobs hdl ses [k]
    return $ res === [Found k v2]

-- | Inserts with different keys don't interfere.
prop_insertCommutes ::
       IsTableHandle h
    => Proxy h -> [(Key, Update Value Blob)]
    -> Key -> Value -> Key -> Value -> Property
prop_insertCommutes h ups k1 v1 k2 v2 = k1 /= k2 ==> ioProperty do
    (ses, hdl) <- makeNewTable h ups
    inserts hdl [(k1, v1, Nothing), (k2, v2, Nothing)]

    res <- lookupsWithBlobs hdl ses [k1,k2]
    return $ res === [Found k1 v1, Found k2 v2]

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
    (ses, hdl) <- makeNewTable h ups

    res <- rangeLookupWithBlobs hdl ses r

    inserts hdl [(k, v, Nothing)]

    res' <- rangeLookupWithBlobs hdl ses r

    let p :: RangeLookupResult Key Value b -> Bool
        p rlr = rangeLookupResultKey rlr /= k

    if evalRange r k
    then return $ sortOn rangeLookupResultKey (FoundInRange k v : filter p res) === res'
    else return $ res === res'

-------------------------------------------------------------------------------
-- implement classic QC tests for split-value BLOB retrieval
-------------------------------------------------------------------------------

-- | You can lookup what you inserted.
prop_lookupInsertBlob ::
     IsTableHandle h
  => Proxy h -> [(Key, Update Value Blob)]
  -> Key  -> Value -> Blob -> Property
prop_lookupInsertBlob h ups k v blob = ioProperty $ do
    -- create session, table handle, and populate it with some data.
    (ses, hdl) <- makeNewTable h ups

    -- the main dish
    inserts hdl [(k, v, Just blob)]
    res <- lookupsWithBlobs hdl ses [k]

    return $ res === [FoundWithBlob k v blob]

-- | Last insert wins.
prop_insertInsertBlob ::
     IsTableHandle h
  => Proxy h -> [(Key, Update Value Blob)]
  -> Key -> Value -> Value -> Maybe Blob -> Maybe Blob -> Property
prop_insertInsertBlob h ups k v1 v2 mblob1 mblob2 = ioProperty $ do
    (ses, hdl) <- makeNewTable h ups
    inserts hdl [(k, v1, mblob1), (k, v2, mblob2)]
    res <- lookupsWithBlobs hdl ses [k]
    return $ res === case mblob2 of
        Nothing    -> [Found k v2]
        Just blob2 -> [FoundWithBlob k v2 blob2]

-- | Inserts with different keys don't interfere.
prop_insertCommutesBlob ::
       IsTableHandle h
    => Proxy h -> [(Key, Update Value Blob)]
    -> Key -> Value -> Maybe Blob
    -> Key -> Value -> Maybe Blob -> Property
prop_insertCommutesBlob h ups k1 v1 mblob1 k2 v2 mblob2 = k1 /= k2 ==> ioProperty do
    (ses, hdl) <- makeNewTable h ups
    inserts hdl [(k1, v1, mblob1), (k2, v2, mblob2)]

    res <- lookupsWithBlobs hdl ses [k1,k2]
    return $ res === case (mblob1, mblob2) of
        (Nothing,    Nothing)    -> [Found k1 v1,               Found k2 v2]
        (Just blob1, Nothing)    -> [FoundWithBlob k1 v1 blob1, Found k2 v2]
        (Nothing,    Just blob2) -> [Found k1 v1,               FoundWithBlob k2 v2 blob2]
        (Just blob1, Just blob2) -> [FoundWithBlob k1 v1 blob1, FoundWithBlob k2 v2 blob2]

-- | A blob reference may be invalidated by an update.
prop_updatesMayInvalidateBlobRefs ::
     IsTableHandle h
  => Proxy h -> [(Key, Update Value Blob)]
  -> Key -> Value -> Blob
  -> [(Key, Update Value Blob)]
  -> Property
prop_updatesMayInvalidateBlobRefs h ups k1 v1 blob1 ups' = monadicIO $ do
    (res, blobs, res') <- run $ do
      (ses, hdl) <- makeNewTable h ups
      inserts hdl [(k1, v1, Just blob1)]
      res <- lookups hdl [k1]
      blobs <- getCompose <$> retrieveBlobsTrav h ses (Compose res)
      updates hdl ups'
      res' <- try @SomeException (getCompose <$> retrieveBlobsTrav h ses (Compose res))
      pure (res, blobs, res')

    case (res, blobs) of
      ([FoundWithBlob{}], [FoundWithBlob _ _ x])
        | Left _ <- res' ->
            monitor (label "blob reference invalidated") >> pure True
        | Right [FoundWithBlob _ _ y] <- res' ->
            monitor (label "blob reference valid") >> pure (x == y)
      _ -> monitor (counterexample "insert before lookup failed, somehow...") >> pure False

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

-- changes to handle would not change the snapshot
prop_snapshotNoChanges :: forall h.
     IsTableHandle h
    => Proxy h -> [(Key, Update Value Blob)]
    -> [(Key, Update Value Blob)] -> [Key] -> Property
prop_snapshotNoChanges h ups ups' testKeys = ioProperty $ do
    (sess, hdl1) <- makeNewTable h ups

    res <- lookupsWithBlobs hdl1 sess testKeys

    let name = fromMaybe (error "invalid name") $ mkSnapshotName "foo"

    snapshot name hdl1
    updates hdl1 ups'

    hdl2 <- open @h sess name

    res' <- lookupsWithBlobs hdl2 sess testKeys

    return $ res == res'

-- same snapshot may be opened multiple times,
-- and the handles are separate.
prop_snapshotNoChanges2 :: forall h.
     IsTableHandle h
    => Proxy h -> [(Key, Update Value Blob)]
    -> [(Key, Update Value Blob)] -> [Key] -> Property
prop_snapshotNoChanges2 h ups ups' testKeys = ioProperty $ do
    (sess, hdl0) <- makeNewTable h ups
    let name = fromMaybe (error "invalid name") $ mkSnapshotName "foo"
    snapshot name hdl0

    hdl1 <- open @h sess name
    hdl2 <- open @h sess name

    res <- lookupsWithBlobs hdl1 sess testKeys
    updates hdl1 ups'
    res' <- lookupsWithBlobs hdl2 sess testKeys

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
  => Proxy h -> [(Key, Update Value Blob)]
  -> Key -> Value -> Value -> [Key] -> Property
prop_dupInsertInsert h ups k v1 v2 testKeys = ioProperty $ do
    (sess, hdl1) <- makeNewTable h ups
    hdl2 <- duplicate hdl1

    inserts hdl1 [(k, v1, Nothing), (k, v2, Nothing)]
    inserts hdl2 [(k, v2, Nothing)]

    res1 <- lookupsWithBlobs hdl1 sess testKeys
    res2 <- lookupsWithBlobs hdl2 sess testKeys
    return $ res1 === res2

-- | Different key inserts commute.
prop_dupInsertCommutes ::
     IsTableHandle h
    => Proxy h -> [(Key, Update Value Blob)]
    -> Key -> Value -> Key -> Value -> [Key] -> Property
prop_dupInsertCommutes h ups k1 v1 k2 v2 testKeys = k1 /= k2 ==> ioProperty do
    (sess, hdl1) <- makeNewTable h ups
    hdl2 <- duplicate hdl1

    inserts hdl1 [(k1, v1, Nothing), (k2, v2, Nothing)]
    inserts hdl2 [(k2, v2, Nothing), (k1, v1, Nothing)]

    res1 <- lookupsWithBlobs hdl1 sess testKeys
    res2 <- lookupsWithBlobs hdl2 sess testKeys
    return $ res1 === res2

-- changes to one handle should not cause any visible changes in any others
prop_dupNoChanges ::
     IsTableHandle h
    => Proxy h -> [(Key, Update Value Blob)]
    -> [(Key, Update Value Blob)] -> [Key] -> Property
prop_dupNoChanges h ups ups' testKeys = ioProperty $ do
    (sess, hdl1) <- makeNewTable h ups

    res <- lookupsWithBlobs hdl1 sess testKeys

    hdl2 <- duplicate hdl1
    updates hdl2 ups'

    -- lookup hdl1 again.
    res' <- lookupsWithBlobs hdl1 sess testKeys

    return $ res == res'
