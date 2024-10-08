{-# LANGUAGE BlockArguments #-}

module Test.Database.LSMTree.Class.Normal (
    tests
  , testProperty'
  ) where
import           Control.Exception (SomeException, try)
import           Control.Monad.ST.Strict (runST)
import           Control.Monad.Trans.State
import qualified Data.ByteString as BS
import           Data.Foldable (toList)
import           Data.Functor.Compose (Compose (..))
import qualified Data.List as List
import qualified Data.List.NonEmpty as NE
import           Data.Maybe (fromMaybe)
import qualified Data.Proxy as Proxy
import qualified Data.Vector as V
import qualified Data.Vector.Algorithms.Merge as VA
import           Data.Word (Word64)
import           Database.LSMTree.Class.Normal hiding (withTableDuplicate,
                     withTableNew, withTableOpen)
import qualified Database.LSMTree.Class.Normal as Class
import           Database.LSMTree.Common (Labellable (..), mkSnapshotName)
import           Database.LSMTree.Extras.Generators ()
import           Database.LSMTree.ModelIO.Normal (IOLike, LookupResult (..),
                     QueryResult (..), Range (..), SerialiseKey, SerialiseValue,
                     Update (..))
import qualified Database.LSMTree.ModelIO.Normal as M
import qualified Database.LSMTree.Normal as R
import qualified System.FS.API as FS
import           Test.QuickCheck.Monadic (monadicIO, monitor, run)
import           Test.Tasty (TestName, TestTree, testGroup)
import           Test.Tasty.QuickCheck
import qualified Test.Util.FS as FS

tests :: TestTree
tests = testGroup "Test.Database.LSMTree.Class.Normal"
    [ testGroup "Model" $ zipWith ($) (props tbl1) expectFailures1
    , testGroup "Real"  $ zipWith ($) (props tbl2) expectFailures2
    ]
  where
    tbl1 :: Proxy M.TableHandle
    tbl1 = Setup {
          testTableConfig = M.TableConfig
        , testWithSessionArgs = \action -> action NoSessionArgs
        }

    expectFailures1 = repeat False

    tbl2 :: Proxy R.TableHandle
    tbl2 = Setup {
          testTableConfig = R.defaultTableConfig {
              R.confWriteBufferAlloc = R.AllocNumEntries (R.NumEntries 3)
            }
        , testWithSessionArgs = \action ->
            FS.withTempIOHasBlockIO "R" $ \hfs hbio ->
              action (SessionArgs hfs hbio (FS.mkFsPath []))
        }

    expectFailures2 = repeat False

    props tbl =
      [ testProperty' "lookup-insert" $ prop_lookupInsert tbl
      , testProperty' "lookup-insert-else" $ prop_lookupInsertElse tbl
      , testProperty' "lookup-insert-blob" $ prop_lookupInsertBlob tbl
      , testProperty' "lookup-delete" $ prop_lookupDelete tbl
      , testProperty' "lookup-delete-else" $ prop_lookupDeleteElse tbl
      , testProperty' "insert-insert" $ prop_insertInsert tbl
      , testProperty' "insert-insert-blob" $ prop_insertInsertBlob tbl
      , testProperty' "insert-commutes" $ prop_insertCommutes tbl
      , testProperty' "insert-commutes-blob" $ prop_insertCommutesBlob tbl
      , testProperty' "invalidated-blob-references" $ prop_updatesMayInvalidateBlobRefs tbl
      , testProperty' "dup-insert-insert" $ prop_dupInsertInsert tbl
      , testProperty' "dup-insert-comm" $ prop_dupInsertCommutes tbl
      , testProperty' "dup-nochanges" $ prop_dupNoChanges tbl
      , testProperty' "lookupRange-like-lookups" $ prop_lookupRangeLikeLookups tbl
      , testProperty' "lookupRange-insert" $ prop_insertLookupRange tbl
      , testProperty' "readCursor-sorted" $ prop_readCursorSorted tbl
      , testProperty' "readCursor-num-results" $ prop_readCursorNumResults tbl
      , testProperty' "readCursor-insert" $ prop_readCursorInsert tbl
      , testProperty' "readCursor-delete" $ prop_readCursorDelete tbl
      , testProperty' "readCursor-delete-else" $ prop_readCursorDeleteElse tbl
      , testProperty' "readCursor-stable-view" $ prop_readCursorStableView tbl
      , testProperty' "readCursor-offset" $ prop_readCursorOffset tbl
      , testProperty' "snapshot-nochanges" $ prop_snapshotNoChanges tbl
      , testProperty' "snapshot-nochanges2" $ prop_snapshotNoChanges2 tbl
      ]

testProperty' :: forall a. Testable a => TestName -> a -> Bool -> TestTree
testProperty' name prop = \b ->
  testProperty name ((if b then expectFailure else property) prop)

-------------------------------------------------------------------------------
-- test setup and helpers
-------------------------------------------------------------------------------

type Key = Word64
type Blob = BS.ByteString

newtype Value = Value BS.ByteString
  deriving stock (Eq, Show)
  deriving newtype (Arbitrary, R.SerialiseValue)

instance Labellable (Key, Value, Blob) where
  makeSnapshotLabel _ = "Word64 ByteString ByteString"

type Proxy h = Setup h IO

data Setup h m = Setup {
    testTableConfig     :: TableConfig h
  , testWithSessionArgs :: forall a. (SessionArgs (Session h) m -> m a) -> m a
  }

-- | create session, table handle, and populate it with some data.
withTableNew :: forall h m a.
     ( IsTableHandle h
     , IOLike m
     )
  => Setup h m
  -> [(Key, Update Value Blob)]
  -> (Session h m -> h m Key Value Blob -> m a)
  -> m a
withTableNew Setup{..} ups action =
    testWithSessionArgs $ \args ->
      withSession args $ \sesh ->
        Class.withTableNew sesh testTableConfig $ \table -> do
          updates table (V.fromList ups)
          action sesh table

-- | Like 'retrieveBlobs' but works for any 'Traversable'.
--
-- Like 'partsOf' in @lens@ this uses state monad.
retrieveBlobsTrav ::
     ( IsTableHandle h
     , IOLike m
     , SerialiseValue blob
     , Traversable t
     )
  => proxy h
  -> Session h m
  -> t (BlobRef h m blob)
  -> m (t blob)
retrieveBlobsTrav hdl ses brefs = do
  blobs <- retrieveBlobs hdl ses (V.fromList $ toList brefs)
  evalStateT (traverse (\_ -> state un) brefs) (V.toList blobs)
  where
    un []     = error "invalid traversal"
    un (x:xs) = (x, xs)

lookupsWithBlobs :: forall h m k v blob.
     ( IsTableHandle h
     , IOLike m
     , SerialiseKey k
     , SerialiseValue v
     , SerialiseValue blob
     )
  => h m k v blob
  -> Session h m
  -> V.Vector k
  -> m (V.Vector (LookupResult v blob))
lookupsWithBlobs hdl ses ks = do
    res <- lookups hdl ks
    getCompose <$> retrieveBlobsTrav (Proxy.Proxy @h) ses (Compose res)

rangeLookupWithBlobs :: forall h m k v blob.
     ( IsTableHandle h
     , IOLike m
     , SerialiseKey k
     , SerialiseValue v
     , SerialiseValue blob
     )
  => h m k v blob
  -> Session h m
  -> Range k
  -> m (V.Vector (QueryResult k v blob))
rangeLookupWithBlobs hdl ses r = do
    res <- rangeLookup hdl r
    getCompose <$> retrieveBlobsTrav (Proxy.Proxy @h) ses (Compose res)

readCursorWithBlobs :: forall h m k v blob proxy.
     ( IsTableHandle h
     , IOLike m
     , SerialiseKey k
     , SerialiseValue v
     , SerialiseValue blob
     )
  => proxy h
  -> Session h m
  -> Cursor h m k v blob
  -> Int
  -> m (V.Vector (QueryResult k v blob))
readCursorWithBlobs hdl ses cursor n = do
    res <- readCursor hdl n cursor
    getCompose <$> retrieveBlobsTrav hdl ses (Compose res)

readCursorAllWithBlobs :: forall h m k v blob proxy.
     ( IsTableHandle h
     , IOLike m
     , SerialiseKey k
     , SerialiseValue v
     , SerialiseValue blob
     )
  => proxy h
  -> Session h m
  -> Cursor h m k v blob
  -> CursorReadSchedule
  -> m [V.Vector (QueryResult k v blob)]
readCursorAllWithBlobs hdl ses cursor = go . getCursorReadSchedule
  where
    go [] = error "readCursorAllWithBlobs: finite infinite list"
    go (n : ns) = do
      res <- readCursorWithBlobs hdl ses cursor n
      if V.null res
        then return [res]
        else (res :) <$> go ns

type CursorReadSchedule = InfiniteList (Positive Int)

getCursorReadSchedule :: CursorReadSchedule -> [Int]
getCursorReadSchedule = map getPositive . getInfiniteList

-------------------------------------------------------------------------------
-- implement classic QC tests for basic k/v properties
-------------------------------------------------------------------------------

-- | You can lookup what you inserted.
prop_lookupInsert ::
     IsTableHandle h
  => Proxy h -> [(Key, Update Value Blob)]
  -> Key -> Value -> Property
prop_lookupInsert h ups k v = ioProperty $ do
    withTableNew h ups $ \ses hdl -> do

      -- the main dish
      inserts hdl (V.singleton (k, v, Nothing))
      res <- lookupsWithBlobs hdl ses (V.singleton k)

      return $ res === V.singleton (Found v)

-- | Insert doesn't change the lookup results of other keys.
prop_lookupInsertElse ::
     IsTableHandle h
  => Proxy h -> [(Key, Update Value Blob)]
  -> Key  -> Value -> [Key] -> Property
prop_lookupInsertElse h ups k v testKeys = ioProperty $ do
    withTableNew h ups $ \ses hdl -> do

      let testKeys' = V.fromList $ filter (/= k) testKeys
      res1 <- lookupsWithBlobs hdl ses testKeys'
      inserts hdl (V.singleton (k, v, Nothing))
      res2 <-  lookupsWithBlobs hdl ses testKeys'

      return $ res1 === res2

-- | You cannot lookup what you have just deleted
prop_lookupDelete ::
     IsTableHandle h
  => Proxy h -> [(Key, Update Value Blob)]
  -> Key -> Property
prop_lookupDelete h ups k = ioProperty $ do
    withTableNew h ups $ \ses hdl -> do
      deletes hdl (V.singleton k)
      res <- lookupsWithBlobs hdl ses (V.singleton k)
      return $ res === V.singleton NotFound

-- | Delete doesn't change the lookup results of other keys
prop_lookupDeleteElse ::
     IsTableHandle h
  => Proxy h -> [(Key, Update Value Blob)]
  -> Key  -> [Key] -> Property
prop_lookupDeleteElse h ups k testKeys = ioProperty $ do
    withTableNew h ups $ \ses hdl -> do

      let testKeys' = V.fromList $ filter (/= k) testKeys
      res1 <- lookupsWithBlobs hdl ses testKeys'
      deletes hdl (V.singleton k)
      res2 <-  lookupsWithBlobs hdl ses testKeys'

      return $ res1 === res2

-- | Last insert wins.
prop_insertInsert ::
     IsTableHandle h
  => Proxy h -> [(Key, Update Value Blob)]
  -> Key -> Value -> Value -> Property
prop_insertInsert h ups k v1 v2 = ioProperty $ do
    withTableNew h ups $ \ses hdl -> do
      inserts hdl (V.fromList [(k, v1, Nothing), (k, v2, Nothing)])
      res <- lookupsWithBlobs hdl ses (V.singleton k)
      return $ res === V.singleton (Found v2)

-- | Inserts with different keys don't interfere.
prop_insertCommutes ::
       IsTableHandle h
    => Proxy h -> [(Key, Update Value Blob)]
    -> Key -> Value -> Key -> Value -> Property
prop_insertCommutes h ups k1 v1 k2 v2 = k1 /= k2 ==> ioProperty do
    withTableNew h ups $ \ses hdl -> do
      inserts hdl (V.fromList [(k1, v1, Nothing), (k2, v2, Nothing)])

      res <- lookupsWithBlobs hdl ses (V.fromList [k1,k2])
      return $ res === V.fromList [Found v1, Found v2]

-------------------------------------------------------------------------------
-- implement classic QC tests for cursors
-------------------------------------------------------------------------------

-- | Cursor read results are sorted by key.
prop_readCursorSorted ::
     forall h. IsTableHandle h
  => Proxy h -> [(Key, Update Value Blob)]
  -> Maybe Key
  -> CursorReadSchedule
  -> Property
prop_readCursorSorted h ups offset ns = ioProperty $ do
    withTableNew h ups $ \ses hdl -> do
      res <- withCursor offset hdl $ \cursor -> do
        V.concat <$> readCursorAllWithBlobs (Proxy.Proxy @h) ses cursor ns
      let keys = map queryResultKey (V.toList res)
      return $ keys === List.sort keys

-- | Cursor reads return the requested number of results, until the end.
prop_readCursorNumResults ::
     forall h. IsTableHandle h
  => Proxy h -> [(Key, Update Value Blob)]
  -> Maybe Key
  -> CursorReadSchedule
  -> Property
prop_readCursorNumResults h ups offset ns = ioProperty $ do
    withTableNew h ups $ \ses hdl -> do
      res <- withCursor offset hdl $ \cursor -> do
        readCursorAllWithBlobs (Proxy.Proxy @h) ses cursor ns
      let elemsRead = map V.length res
      let numFullReads = length res - 2
      return $ last elemsRead === 0
          .&&. take numFullReads elemsRead
           === take numFullReads (getCursorReadSchedule ns)

-- | You can read what you inserted.
prop_readCursorInsert ::
     forall h. IsTableHandle h
  => Proxy h -> [(Key, Update Value Blob)]
  -> CursorReadSchedule
  -> Key -> Value -> Property
prop_readCursorInsert h ups ns k v = ioProperty $ do
    withTableNew h ups $ \ses hdl -> do
      inserts hdl (V.singleton (k, v, Nothing))
      res <- withCursor Nothing hdl $ \cursor ->
        V.concat <$> readCursorAllWithBlobs (Proxy.Proxy @h) ses cursor ns
      return $ V.find (\r -> queryResultKey r == k) res
           === Just (FoundInQuery k v)

-- | You can't read what you deleted.
prop_readCursorDelete ::
     forall h. IsTableHandle h
  => Proxy h -> [(Key, Update Value Blob)]
  -> CursorReadSchedule
  -> Key -> Property
prop_readCursorDelete h ups ns k = ioProperty $ do
    withTableNew h ups $ \ses hdl -> do
      deletes hdl (V.singleton k)
      res <- withCursor Nothing hdl $ \cursor -> do
        V.concat <$> readCursorAllWithBlobs (Proxy.Proxy @h) ses cursor ns
      return $ V.find (\r -> queryResultKey r == k) res === Nothing

-- | Updates don't change the cursor read results of other keys.
prop_readCursorDeleteElse ::
     forall h. IsTableHandle h
  => Proxy h -> [(Key, Update Value Blob)]
  -> Maybe Key
  -> CursorReadSchedule
  -> [(Key, Update Value Blob)] -> Property
prop_readCursorDeleteElse h ups offset ns ups2 = ioProperty $ do
    withTableNew h ups $ \ses hdl -> do
      res1 <- withCursor offset hdl $ \cursor -> do
        V.concat <$> readCursorAllWithBlobs (Proxy.Proxy @h) ses cursor ns
      updates hdl (V.fromList ups2)
      res2 <- withCursor offset hdl $ \cursor -> do
        V.concat <$> readCursorAllWithBlobs (Proxy.Proxy @h) ses cursor ns
      let updatedKeys = map fst ups2
      return $ V.filter (\r -> queryResultKey r `notElem` updatedKeys) res1
           === V.filter (\r -> queryResultKey r `notElem` updatedKeys) res2

-- | Updates don't affect previously created cursors.
prop_readCursorStableView ::
     forall h. IsTableHandle h
  => Proxy h -> [(Key, Update Value Blob)]
  -> Maybe Key
  -> CursorReadSchedule
  -> [(Key, Update Value Blob)] -> Property
prop_readCursorStableView h ups offset ns ups2 = ioProperty $ do
    withTableNew h ups $ \ses hdl -> do
      res1 <- withCursor offset hdl $ \cursor -> do
        readCursorAllWithBlobs (Proxy.Proxy @h) ses cursor ns
      res2 <- withCursor offset hdl $ \cursor -> do
        updates hdl (V.fromList ups2)
        readCursorAllWithBlobs (Proxy.Proxy @h) ses cursor ns
      return $ res1 === res2

-- | Creating a cursor at an offset simply skips a prefix.
prop_readCursorOffset ::
     forall h. IsTableHandle h
  => Proxy h -> [(Key, Update Value Blob)]
  -> Key
  -> CursorReadSchedule
  -> Property
prop_readCursorOffset h ups offset ns = ioProperty $ do
    withTableNew h ups $ \ses hdl -> do
      res1 <- withCursor (Just offset) hdl $ \cursor -> do
        V.concat <$> readCursorAllWithBlobs (Proxy.Proxy @h) ses cursor ns
      res2 <- withCursor Nothing hdl $ \cursor -> do
        V.concat <$> readCursorAllWithBlobs (Proxy.Proxy @h) ses cursor ns
      return $ res1 === V.dropWhile ((< offset) . queryResultKey) res2

-------------------------------------------------------------------------------
-- implement classic QC tests for range lookups
-------------------------------------------------------------------------------

evalRange :: Ord k => Range k -> k -> Bool
evalRange (FromToExcluding lo hi) x = lo <= x && x < hi
evalRange (FromToIncluding lo hi) x = lo <= x && x <= hi

queryResultKey :: QueryResult k v b -> k
queryResultKey (FoundInQuery k _)             = k
queryResultKey (FoundInQueryWithBlob k _ _  ) = k

queryResultFromLookup :: k -> LookupResult v b -> Maybe (QueryResult k v b)
queryResultFromLookup k = \case
   NotFound -> Nothing
   Found v -> Just (FoundInQuery k v)
   FoundWithBlob v b -> Just (FoundInQueryWithBlob k v b)

-- | A range lookup behaves like many point lookups.
prop_lookupRangeLikeLookups ::
     IsTableHandle h
  => Proxy h -> [(Key, Update Value Blob)]
  -> Range Key
  -> Property
prop_lookupRangeLikeLookups h ups r = ioProperty $ do
    withTableNew h ups $ \ses hdl -> do
      res1 <- rangeLookupWithBlobs hdl ses r

      let testKeys = V.fromList $ nubSort $ filter (evalRange r) $ map fst ups
      res2 <- fmap (V.catMaybes . V.zipWith queryResultFromLookup testKeys) $
        lookupsWithBlobs hdl ses testKeys

      return $ res1 === res2

  where
    nubSort = map NE.head . NE.group . List.sort

-- | Last insert wins.
prop_insertLookupRange ::
     IsTableHandle h
  => Proxy h -> [(Key, Update Value Blob)]
  -> Key -> Value -> Range Key  -> Property
prop_insertLookupRange h ups k v r = ioProperty $ do
    withTableNew h ups $ \ses hdl -> do

      res <- rangeLookupWithBlobs hdl ses r

      inserts hdl (V.singleton (k, v, Nothing))

      res' <- rangeLookupWithBlobs hdl ses r

      let p :: QueryResult Key Value b -> Bool
          p rlr = queryResultKey rlr /= k

      if evalRange r k
      then return $ vsortOn queryResultKey (V.cons (FoundInQuery k v) (V.filter p res)) === res'
      else return $ res === res'

  where
    vsortOn f vec = runST $ do
        mvec <- V.thaw vec
        VA.sortBy (\e1 e2 -> f e1 `compare` f e2) mvec
        V.unsafeFreeze mvec

-------------------------------------------------------------------------------
-- implement classic QC tests for split-value BLOB retrieval
-------------------------------------------------------------------------------

-- | You can lookup what you inserted.
prop_lookupInsertBlob ::
     IsTableHandle h
  => Proxy h -> [(Key, Update Value Blob)]
  -> Key  -> Value -> Blob -> Property
prop_lookupInsertBlob h ups k v blob = ioProperty $ do
    withTableNew h ups $ \ses hdl -> do

      -- the main dish
      inserts hdl (V.singleton (k, v, Just blob))
      res <- lookupsWithBlobs hdl ses (V.singleton k)

      return $ res === V.singleton (FoundWithBlob v blob)

-- | Last insert wins.
prop_insertInsertBlob ::
     IsTableHandle h
  => Proxy h -> [(Key, Update Value Blob)]
  -> Key -> Value -> Value -> Maybe Blob -> Maybe Blob -> Property
prop_insertInsertBlob h ups k v1 v2 mblob1 mblob2 = ioProperty $ do
    withTableNew h ups $ \ses hdl -> do
      inserts hdl (V.fromList [(k, v1, mblob1), (k, v2, mblob2)])
      res <- lookupsWithBlobs hdl ses (V.singleton k)
      return $ res === case mblob2 of
          Nothing    -> V.singleton (Found v2)
          Just blob2 -> V.singleton (FoundWithBlob v2 blob2)

-- | Inserts with different keys don't interfere.
prop_insertCommutesBlob ::
       IsTableHandle h
    => Proxy h -> [(Key, Update Value Blob)]
    -> Key -> Value -> Maybe Blob
    -> Key -> Value -> Maybe Blob -> Property
prop_insertCommutesBlob h ups k1 v1 mblob1 k2 v2 mblob2 = k1 /= k2 ==> ioProperty do
    withTableNew h ups $ \ses hdl -> do
      inserts hdl (V.fromList [(k1, v1, mblob1), (k2, v2, mblob2)])

      res <- lookupsWithBlobs hdl ses $ V.fromList [k1,k2]
      return $ res === case (mblob1, mblob2) of
          (Nothing,    Nothing)    -> V.fromList [Found v1,               Found v2]
          (Just blob1, Nothing)    -> V.fromList [FoundWithBlob v1 blob1, Found v2]
          (Nothing,    Just blob2) -> V.fromList [Found v1,               FoundWithBlob v2 blob2]
          (Just blob1, Just blob2) -> V.fromList [FoundWithBlob v1 blob1, FoundWithBlob v2 blob2]

-- | A blob reference may be invalidated by an update.
prop_updatesMayInvalidateBlobRefs ::
     forall h. IsTableHandle h
  => Proxy h -> [(Key, Update Value Blob)]
  -> Key -> Value -> Blob
  -> [(Key, Update Value Blob)]
  -> Property
prop_updatesMayInvalidateBlobRefs h ups k1 v1 blob1 ups' = monadicIO $ do
    (res, blobs, res') <- run $ do
      withTableNew h ups $ \ses hdl -> do
        inserts hdl (V.singleton (k1, v1, Just blob1))
        res <- lookups hdl (V.singleton k1)
        blobs <- getCompose <$> retrieveBlobsTrav (Proxy.Proxy @h) ses (Compose res)
        updates hdl (V.fromList ups')
        res' <- try @SomeException (getCompose <$> retrieveBlobsTrav (Proxy.Proxy @h) ses (Compose res))
        pure (res, blobs, res')

    case (V.toList res, V.toList blobs) of
      ([FoundWithBlob{}], [FoundWithBlob _ x])
        | Left _ <- res' ->
            monitor (label "blob reference invalidated") >> pure True
        | Right (V.toList -> [FoundWithBlob _ y]) <- res' ->
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
    withTableNew h ups $ \ses hdl1 -> do

      res <- lookupsWithBlobs hdl1 ses $ V.fromList testKeys

      let name = fromMaybe (error "invalid name") $ mkSnapshotName "foo"

      snapshot name hdl1
      updates hdl1 (V.fromList ups')

      Class.withTableOpen @h ses name$ \hdl2 -> do

        res' <- lookupsWithBlobs hdl2 ses $ V.fromList testKeys

        return $ res == res'

-- same snapshot may be opened multiple times,
-- and the handles are separate.
prop_snapshotNoChanges2 :: forall h.
     IsTableHandle h
    => Proxy h -> [(Key, Update Value Blob)]
    -> [(Key, Update Value Blob)] -> [Key] -> Property
prop_snapshotNoChanges2 h ups ups' testKeys = ioProperty $ do
    withTableNew h ups $ \sess hdl0 -> do
      let name = fromMaybe (error "invalid name") $ mkSnapshotName "foo"
      snapshot name hdl0

      Class.withTableOpen @h sess name $ \hdl1 ->
        Class.withTableOpen @h sess name $ \hdl2 -> do

          res <- lookupsWithBlobs hdl1 sess $ V.fromList testKeys
          updates hdl1 (V.fromList ups')
          res' <- lookupsWithBlobs hdl2 sess $ V.fromList testKeys

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
    withTableNew h ups $ \sess hdl1 -> do
      Class.withTableDuplicate hdl1 $ \hdl2 -> do

        inserts hdl1 (V.fromList [(k, v1, Nothing), (k, v2, Nothing)])
        inserts hdl2 (V.fromList [(k, v2, Nothing)])

        res1 <- lookupsWithBlobs hdl1 sess $ V.fromList testKeys
        res2 <- lookupsWithBlobs hdl2 sess $ V.fromList testKeys
        return $ res1 === res2

-- | Different key inserts commute.
prop_dupInsertCommutes ::
     IsTableHandle h
    => Proxy h -> [(Key, Update Value Blob)]
    -> Key -> Value -> Key -> Value -> [Key] -> Property
prop_dupInsertCommutes h ups k1 v1 k2 v2 testKeys = k1 /= k2 ==> ioProperty do
    withTableNew h ups $ \sess hdl1 -> do
      Class.withTableDuplicate hdl1 $ \hdl2 -> do

        inserts hdl1 (V.fromList [(k1, v1, Nothing), (k2, v2, Nothing)])
        inserts hdl2 (V.fromList [(k2, v2, Nothing), (k1, v1, Nothing)])

        res1 <- lookupsWithBlobs hdl1 sess $ V.fromList testKeys
        res2 <- lookupsWithBlobs hdl2 sess $ V.fromList testKeys
        return $ res1 === res2

-- changes to one handle should not cause any visible changes in any others
prop_dupNoChanges ::
     IsTableHandle h
    => Proxy h -> [(Key, Update Value Blob)]
    -> [(Key, Update Value Blob)] -> [Key] -> Property
prop_dupNoChanges h ups ups' testKeys = ioProperty $ do
    withTableNew h ups $ \sess hdl1 -> do

      res <- lookupsWithBlobs hdl1 sess $ V.fromList testKeys

      Class.withTableDuplicate hdl1 $ \hdl2 -> do
        updates hdl2 (V.fromList ups')

        -- lookup hdl1 again.
        res' <- lookupsWithBlobs hdl1 sess $ V.fromList testKeys

        return $ res == res'
