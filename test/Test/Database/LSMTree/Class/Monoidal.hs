{-# LANGUAGE BlockArguments    #-}
{-# LANGUAGE OverloadedStrings #-}

module Test.Database.LSMTree.Class.Monoidal (tests) where

import           Control.Monad.ST.Strict (runST)
import qualified Data.ByteString as BS
import qualified Data.List as List
import qualified Data.List.NonEmpty as NE
import           Data.Maybe (fromMaybe)
import qualified Data.Proxy as Proxy
import qualified Data.Vector as V
import qualified Data.Vector.Algorithms.Merge as VA
import           Data.Void (Void)
import           Data.Word (Word64)
import           Database.LSMTree.Class.Monoidal hiding (withTableDuplicate,
                     withTableFromSnapshot, withTableNew)
import qualified Database.LSMTree.Class.Monoidal as Class
import           Database.LSMTree.Common (mkSnapshotName)
import           Database.LSMTree.Extras.Generators ()
import qualified Database.LSMTree.Model.IO.Monoidal as ModelIO
import           Database.LSMTree.Monoidal (resolveDeserialised)
import qualified Database.LSMTree.Monoidal as R
import qualified System.FS.API as FS
import           Test.Database.LSMTree.Class.Normal (testProperty')
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.QuickCheck hiding (label)
import qualified Test.Util.FS as FS

tests :: TestTree
tests = testGroup "Test.Database.LSMTree.Class.Monoidal"
    [ testGroup "Model" $ zipWith ($) (props tbl1) expectFailures1
    , testGroup "Real"  $ zipWith ($) (props tbl2) expectFailures2
    ]
  where
    tbl1 :: Proxy ModelIO.Table
    tbl1 = Setup {
          testTableConfig = ModelIO.TableConfig
        , testWithSessionArgs = \action -> action ModelIO.NoSessionArgs
        }

    expectFailures1 = repeat False

    tbl2 :: Proxy R.Table
    tbl2 = Setup {
          testTableConfig = R.defaultTableConfig {
              R.confWriteBufferAlloc = R.AllocNumEntries (R.NumEntries 3)
            }
        , testWithSessionArgs = \action ->
            FS.withTempIOHasBlockIO "R" $ \hfs hbio ->
              action (SessionArgs hfs hbio (FS.mkFsPath []))
        }

    expectFailures2 = [
        False
      , False
      , False
      , False
      , False
      , False
      , False
      , False
      , False
      , False
      , False
      , False
      , False
      , False
      , False
      , False
      , False
      , False
      , False
      , False
      , False
      , True  -- merge
      ] ++ repeat False

    props tbl =
      [ testProperty' "lookup-insert" $ prop_lookupInsert tbl
      , testProperty' "lookup-insert-else" $ prop_lookupInsertElse tbl
      , testProperty' "lookup-delete" $ prop_lookupDelete tbl
      , testProperty' "lookup-delete-else" $ prop_lookupDeleteElse tbl
      , testProperty' "insert-insert" $ prop_insertInsert tbl
      , testProperty' "insert-commutes" $ prop_insertCommutes tbl
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
      , testProperty' "lookup-mupsert" $ prop_lookupUpdate tbl
      , testProperty' "merge" $ prop_union tbl
      ]

-------------------------------------------------------------------------------
-- test setup and helpers
-------------------------------------------------------------------------------

type Key = Word64

newtype Value = Value BS.ByteString
  deriving stock (Eq, Show)
  deriving newtype (Arbitrary, R.SerialiseValue)

instance ResolveValue Value where
    resolveValue = resolveDeserialised resolve

resolve :: Value -> Value -> Value
resolve (Value x) (Value y) = Value (x <> y)

label :: SnapshotLabel
label = SnapshotLabel "Word64 ByteString"

type Proxy h = Setup h IO

data Setup h m = Setup {
    testTableConfig     :: TableConfig h
  , testWithSessionArgs :: forall a. (SessionArgs (Session h) m -> m a) -> m a
  }

-- | create session, table, and populate it with some data.
withTableNew :: forall h m a.
     ( IsTable h
     , IOLike m
     )
  => Setup h m
  -> [(Key, Update Value)]
  -> (Session h m -> h m Key Value -> m a)
  -> m a
withTableNew Setup{..} ups action =
    testWithSessionArgs $ \args ->
      withSession args $ \sesh ->
        Class.withTableNew sesh testTableConfig $ \table -> do
          updates table (V.fromList ups)
          action sesh table

readCursorAll :: forall h m k v proxy.
     ( IsTable h
     , IOLike m
     , SerialiseKey k
     , SerialiseValue v
     , ResolveValue v
     , C k v Void
     )
  => proxy h
  -> Cursor h m k v
  -> CursorReadSchedule
  -> m [V.Vector (QueryResult k v)]
readCursorAll hdl cursor = go . getCursorReadSchedule
  where
    go [] = error "readCursorAll: finite infinite list"
    go (n : ns) = do
      res <- readCursor hdl n cursor
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
     IsTable h
  => Proxy h -> [(Key, Update Value)]
  -> Key  -> Value -> Property
prop_lookupInsert h ups k v = ioProperty $ do
    withTableNew h ups $ \_ hdl -> do
      -- the main dish
      inserts hdl (V.singleton (k, v))
      res <- lookups hdl (V.singleton k)

      return $ res === V.singleton (Found v)

-- | Insert doesn't change the lookup results of other keys.
prop_lookupInsertElse ::
     IsTable h
  => Proxy h -> [(Key, Update Value)]
  -> Key  -> Value -> [Key] -> Property
prop_lookupInsertElse h ups k v testKeys = ioProperty $ do
    withTableNew h ups $ \_ hdl -> do

      let testKeys' = V.fromList $ filter (/= k) testKeys
      res1 <- lookups hdl testKeys'
      inserts hdl (V.singleton (k, v))
      res2 <-  lookups hdl testKeys'

      return $ res1 === res2

-- | You cannot lookup what you have just deleted
prop_lookupDelete ::
     IsTable h
  => Proxy h -> [(Key, Update Value)]
  -> Key -> Property
prop_lookupDelete h ups k = ioProperty $ do
    withTableNew h ups $ \_ hdl -> do
      deletes hdl (V.singleton k)
      res <- lookups hdl (V.singleton k)
      return $ res === V.singleton NotFound

-- | Delete doesn't change the lookup results of other keys
prop_lookupDeleteElse ::
     IsTable h
  => Proxy h -> [(Key, Update Value)]
  -> Key  -> [Key] -> Property
prop_lookupDeleteElse h ups k testKeys = ioProperty $ do
    withTableNew h ups $ \_ hdl -> do

      let testKeys' = V.fromList $ filter (/= k) testKeys
      res1 <- lookups hdl testKeys'
      deletes hdl (V.singleton k)
      res2 <-  lookups hdl testKeys'

      return $ res1 === res2

-- | Last insert wins.
prop_insertInsert ::
     IsTable h
  => Proxy h -> [(Key, Update Value)]
  -> Key -> Value -> Value -> Property
prop_insertInsert h ups k v1 v2 = ioProperty $ do
    withTableNew h ups $ \_ hdl -> do
      inserts hdl (V.fromList [(k, v1), (k, v2)])
      res <- lookups hdl (V.singleton k)
      return $ res === V.singleton (Found v2)

-- | Inserts with different keys don't interfere.
prop_insertCommutes ::
       IsTable h
    => Proxy h -> [(Key, Update Value)]
    -> Key -> Value -> Key -> Value -> Property
prop_insertCommutes h ups k1 v1 k2 v2 = k1 /= k2 ==> ioProperty do
    withTableNew h ups $ \_ hdl -> do
      inserts hdl (V.fromList [(k1, v1), (k2, v2)])

      res <- lookups hdl (V.fromList [k1, k2])
      return $ res === V.fromList [Found v1, Found v2]

-------------------------------------------------------------------------------
-- implement classic QC tests for cursors
-------------------------------------------------------------------------------

-- | Cursor read results are sorted by key.
prop_readCursorSorted ::
     forall h. IsTable h
  => Proxy h -> [(Key, Update Value)]
  -> Maybe Key
  -> CursorReadSchedule
  -> Property
prop_readCursorSorted h ups offset ns = ioProperty $ do
    withTableNew h ups $ \_ hdl -> do
      res <- withCursor offset hdl $ \cursor -> do
        V.concat <$> readCursorAll (Proxy.Proxy @h) cursor ns
      let keys = map queryResultKey (V.toList res)
      return $ keys === List.sort keys

-- | Cursor reads return the requested number of results, until the end.
prop_readCursorNumResults ::
     forall h. IsTable h
  => Proxy h -> [(Key, Update Value)]
  -> Maybe Key
  -> CursorReadSchedule
  -> Property
prop_readCursorNumResults h ups offset ns = ioProperty $ do
    withTableNew h ups $ \_ hdl -> do
      res <- withCursor offset hdl $ \cursor -> do
        readCursorAll (Proxy.Proxy @h) cursor ns
      let elemsRead = map V.length res
      let numFullReads = length res - 2
      return $ last elemsRead === 0
          .&&. take numFullReads elemsRead
           === take numFullReads (getCursorReadSchedule ns)

-- | You can read what you inserted.
prop_readCursorInsert ::
     forall h. IsTable h
  => Proxy h -> [(Key, Update Value)]
  -> CursorReadSchedule
  -> Key -> Value -> Property
prop_readCursorInsert h ups ns k v = ioProperty $ do
    withTableNew h ups $ \_ hdl -> do
      inserts hdl (V.singleton (k, v))
      res <- withCursor Nothing hdl $ \cursor ->
        V.concat <$> readCursorAll (Proxy.Proxy @h) cursor ns
      return $ V.find (\r -> queryResultKey r == k) res
           === Just (FoundInQuery k v)

-- | You can't read what you deleted.
prop_readCursorDelete ::
     forall h. IsTable h
  => Proxy h -> [(Key, Update Value)]
  -> CursorReadSchedule
  -> Key -> Property
prop_readCursorDelete h ups ns k = ioProperty $ do
    withTableNew h ups $ \_ hdl -> do
      deletes hdl (V.singleton k)
      res <- withCursor Nothing hdl $ \cursor -> do
        V.concat <$> readCursorAll (Proxy.Proxy @h) cursor ns
      return $ V.find (\r -> queryResultKey r == k) res === Nothing

-- | Updates don't change the cursor read results of other keys.
prop_readCursorDeleteElse ::
     forall h. IsTable h
  => Proxy h -> [(Key, Update Value)]
  -> Maybe Key
  -> CursorReadSchedule
  -> [(Key, Update Value)] -> Property
prop_readCursorDeleteElse h ups offset ns ups2 = ioProperty $ do
    withTableNew h ups $ \_ hdl -> do
      res1 <- withCursor offset hdl $ \cursor -> do
        V.concat <$> readCursorAll (Proxy.Proxy @h) cursor ns
      updates hdl (V.fromList ups2)
      res2 <- withCursor offset hdl $ \cursor -> do
        V.concat <$> readCursorAll (Proxy.Proxy @h) cursor ns
      let updatedKeys = map fst ups2
      return $ V.filter (\r -> queryResultKey r `notElem` updatedKeys) res1
           === V.filter (\r -> queryResultKey r `notElem` updatedKeys) res2

-- | Updates don't affect previously created cursors.
prop_readCursorStableView ::
     forall h. IsTable h
  => Proxy h -> [(Key, Update Value)]
  -> Maybe Key
  -> CursorReadSchedule
  -> [(Key, Update Value)] -> Property
prop_readCursorStableView h ups offset ns ups2 = ioProperty $ do
    withTableNew h ups $ \_ hdl -> do
      res1 <- withCursor offset hdl $ \cursor -> do
        readCursorAll (Proxy.Proxy @h) cursor ns
      res2 <- withCursor offset hdl $ \cursor -> do
        updates hdl (V.fromList ups2)
        readCursorAll (Proxy.Proxy @h) cursor ns
      return $ res1 === res2

-- | Creating a cursor at an offset simply skips a prefix.
prop_readCursorOffset ::
     forall h. IsTable h
  => Proxy h -> [(Key, Update Value)]
  -> Key
  -> CursorReadSchedule
  -> Property
prop_readCursorOffset h ups offset ns = ioProperty $ do
    withTableNew h ups $ \_ hdl -> do
      res1 <- withCursor (Just offset) hdl $ \cursor -> do
        V.concat <$> readCursorAll (Proxy.Proxy @h) cursor ns
      res2 <- withCursor Nothing hdl $ \cursor -> do
        V.concat <$> readCursorAll (Proxy.Proxy @h) cursor ns
      return $ res1 === V.dropWhile ((< offset) . queryResultKey) res2

-------------------------------------------------------------------------------
-- implement classic QC tests for range lookups
-------------------------------------------------------------------------------

evalRange :: Ord k => Range k -> k -> Bool
evalRange (FromToExcluding lo hi) x = lo <= x && x < hi
evalRange (FromToIncluding lo hi) x = lo <= x && x <= hi

queryResultKey :: QueryResult k v -> k
queryResultKey (FoundInQuery k _) = k

queryResultFromLookup :: k -> LookupResult v -> Maybe (QueryResult k v)
queryResultFromLookup k = \case
   NotFound -> Nothing
   Found v -> Just (FoundInQuery k v)

-- | A range lookup behaves like many point lookups.
prop_lookupRangeLikeLookups ::
     IsTable h
  => Proxy h -> [(Key, Update Value)]
  -> Range Key
  -> Property
prop_lookupRangeLikeLookups h ups r = ioProperty $ do
    withTableNew h ups $ \_ hdl -> do
      res1 <- rangeLookup hdl r

      let testKeys = V.fromList $ nubSort $ filter (evalRange r) $ map fst ups
      res2 <- fmap (V.catMaybes . V.zipWith queryResultFromLookup testKeys) $
        lookups hdl testKeys

      return $ res1 === res2

  where
    nubSort = map NE.head . NE.group . List.sort

-- | Last insert wins.
prop_insertLookupRange ::
     IsTable h
  => Proxy h -> [(Key, Update Value)]
  -> Key -> Value -> Range Key  -> Property
prop_insertLookupRange h ups k v r = ioProperty $ do
    withTableNew h ups $ \_ hdl -> do

      res <- rangeLookup hdl r

      inserts hdl (V.singleton (k, v))

      res' <- rangeLookup hdl r

      let p :: QueryResult Key Value -> Bool
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

{- not applicable -}

-------------------------------------------------------------------------------
-- implement classic QC tests for monoidal updates
-------------------------------------------------------------------------------

-- | You can lookup what you inserted.
prop_lookupUpdate ::
     IsTable h
  => Proxy h -> [(Key, Update Value)]
  -> Key  -> Value -> Value -> Property
prop_lookupUpdate h ups k v1 v2 = ioProperty $ do
    withTableNew h ups $ \_ hdl -> do

      -- the main dish
      inserts hdl (V.singleton (k, v1))
      mupserts hdl (V.singleton (k, v2))
      res <- lookups hdl (V.singleton k)

      -- notice the order.
      return $ res === V.singleton (Found (resolve v2 v1))

-------------------------------------------------------------------------------
-- implement classic QC tests for monoidal table unions
-------------------------------------------------------------------------------

prop_union :: forall h.
     IsTable h
  => Proxy h -> [(Key, Update Value)] -> [(Key, Update Value)]
  -> [Key] -> Property
prop_union h ups1 ups2 (V.fromList -> testKeys) = ioProperty $ do
    withTableNew h ups1 $ \s hdl1 -> do
      Class.withTableNew  s (testTableConfig h) $ \hdl2 -> do
        updates hdl2 $ V.fromList ups2

        -- union them.
        Class.withTableUnion hdl1 hdl2 $ \hdl3 -> do

          -- results in parts and the union table
          res1 <- lookups hdl1 testKeys
          res2 <- lookups hdl2 testKeys
          res3 <- lookups hdl3 testKeys

          let unionResult :: LookupResult Value -> LookupResult Value -> LookupResult Value
              unionResult r@NotFound   NotFound     = r
              unionResult   NotFound   r@(Found _)  = r
              unionResult r@(Found _)    NotFound   = r
              unionResult   (Found v1)   (Found v2) = Found (resolve v1 v2)

          return $ V.zipWith unionResult res1 res2  == res3

-------------------------------------------------------------------------------
-- implement classic QC tests for snapshots
-------------------------------------------------------------------------------

-- changes to handle would not change the snapshot
prop_snapshotNoChanges :: forall h.
     IsTable h
    => Proxy h -> [(Key, Update Value)]
    -> [(Key, Update Value)] -> [Key] -> Property
prop_snapshotNoChanges h ups ups' (V.fromList -> testKeys) = ioProperty $ do
    withTableNew h ups $ \sess hdl1 -> do

      res <- lookups hdl1 testKeys

      let name = fromMaybe (error "invalid name") $ mkSnapshotName "foo"

      createSnapshot label name hdl1
      updates hdl1 $ V.fromList ups'

      Class.withTableFromSnapshot @h sess label name $ \hdl2 -> do

        res' <- lookups hdl2 testKeys

        return $ res == res'

-- same snapshot may be opened multiple times,
-- and the handles are separate.
prop_snapshotNoChanges2 :: forall h.
     IsTable h
    => Proxy h -> [(Key, Update Value)]
    -> [(Key, Update Value)] -> [Key] -> Property
prop_snapshotNoChanges2 h ups ups' (V.fromList -> testKeys) = ioProperty $ do
    withTableNew h ups $ \sess hdl0 -> do
      let name = fromMaybe (error "invalid name") $ mkSnapshotName "foo"
      createSnapshot label name hdl0

      Class.withTableFromSnapshot @h sess label name $ \hdl1 -> do
        Class.withTableFromSnapshot @h sess label name $ \hdl2 -> do

          res <- lookups hdl1 testKeys
          updates hdl1 $ V.fromList ups'
          res' <- lookups hdl2 testKeys

          return $ res == res'

-------------------------------------------------------------------------------
-- implement classic QC tests for multiple writable tables
--  - results of insert/delete, monoidal update, lookups, and range lookups should
--    be equal if applied to two duplicated tables
--  - changes to one table should not cause any visible changes in any others
-------------------------------------------------------------------------------

-- | Last insert wins.
prop_dupInsertInsert ::
     IsTable h
  => Proxy h -> [(Key, Update Value)]
  -> Key -> Value -> Value -> [Key] -> Property
prop_dupInsertInsert h ups k v1 v2 (V.fromList -> testKeys) = ioProperty $ do
    withTableNew h ups $ \_ hdl1 -> do
      Class.withTableDuplicate hdl1 $ \hdl2 -> do

        inserts hdl1 $ V.fromList [(k, v1), (k, v2)]
        inserts hdl2 $ V.singleton (k, v2)

        res1 <- lookups hdl1 testKeys
        res2 <- lookups hdl2 testKeys
        return $ res1 === res2

-- | Different key inserts commute.
prop_dupInsertCommutes ::
     IsTable h
    => Proxy h -> [(Key, Update Value)]
    -> Key -> Value -> Key -> Value -> [Key] -> Property
prop_dupInsertCommutes h ups k1 v1 k2 v2 (V.fromList -> testKeys) = k1 /= k2 ==> ioProperty do
    withTableNew h ups $ \_ hdl1 -> do
      Class.withTableDuplicate hdl1 $ \hdl2 -> do

        inserts hdl1 $ V.fromList [(k1, v1), (k2, v2)]
        inserts hdl2 $ V.fromList [(k2, v2), (k1, v1)]

        res1 <- lookups hdl1 testKeys
        res2 <- lookups hdl2 testKeys
        return $ res1 === res2

-- changes to one handle should not cause any visible changes in any others
prop_dupNoChanges ::
     IsTable h
    => Proxy h -> [(Key, Update Value)]
    -> [(Key, Update Value)] -> [Key] -> Property
prop_dupNoChanges h ups ups' (V.fromList -> testKeys) = ioProperty $ do
    withTableNew h ups $ \_ hdl1 -> do

      res <- lookups hdl1 testKeys

      Class.withTableDuplicate hdl1 $ \hdl2 -> do
        updates hdl2 $ V.fromList ups'

        -- lookup hdl1 again.
        res' <- lookups hdl1 testKeys

        return $ res == res'
