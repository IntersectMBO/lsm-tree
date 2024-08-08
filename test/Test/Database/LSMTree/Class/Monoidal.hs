{-# LANGUAGE BlockArguments #-}

module Test.Database.LSMTree.Class.Monoidal (tests) where

import           Control.Monad.ST.Strict (runST)
import qualified Data.ByteString as BS
import           Data.Maybe (fromMaybe)
import qualified Data.Vector as V
import qualified Data.Vector.Algorithms.Merge as VA
import           Data.Word (Word64)
import           Database.LSMTree.Class.Monoidal hiding (withTableDuplicate,
                     withTableNew, withTableOpen)
import qualified Database.LSMTree.Class.Monoidal as Class
import           Database.LSMTree.Common (Labellable (..), mkSnapshotName)
import           Database.LSMTree.Extras.Generators ()
import           Database.LSMTree.ModelIO.Monoidal (IOLike, LookupResult (..),
                     Range (..), RangeLookupResult (..), Update (..))
import qualified Database.LSMTree.ModelIO.Monoidal as M
import           Database.LSMTree.Monoidal (ResolveValue (..),
                     resolveDeserialised)
import qualified Database.LSMTree.Monoidal as R
import qualified System.FS.API as FS
import           Test.Database.LSMTree.Class.Normal (testProperty')
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.QuickCheck
import qualified Test.Util.FS as FS

tests :: TestTree
tests = testGroup "Test.Database.LSMTree.Class.Monoidal"
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
          testTableConfig = R.TableConfig {
              R.confMergePolicy = R.MergePolicyLazyLevelling
            , R.confSizeRatio = R.Four
            , R.confWriteBufferAlloc = R.AllocNumEntries (R.NumEntries 3)
            , R.confBloomFilterAlloc = R.AllocFixed 10
            , R.confFencePointerIndex = R.CompactIndex
            , R.confDiskCachePolicy = R.DiskCacheNone
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
      , True  -- lookupRange-insert
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
      , testProperty' "lookupRange-insert" $ prop_insertLookupRange tbl
      , testProperty' "snapshot-nochanges" $ prop_snapshotNoChanges tbl
      , testProperty' "snapshot-nochanges2" $ prop_snapshotNoChanges2 tbl
      , testProperty' "lookup-mupsert" $ prop_lookupUpdate tbl
      , testProperty' "merge" $ prop_merge tbl
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

instance Labellable (Key, Value) where
  makeSnapshotLabel _ = "Word64 ByteString"

type Proxy h = Setup h IO

data Setup h m = Setup {
    testTableConfig     :: TableConfig h
  , testWithSessionArgs :: forall a. (SessionArgs (Session h) m -> m a) -> m a
  }

-- | create session, table handle, and populate it with some data.
withTableNew ::
     forall h m a. (IsTableHandle h, IOLike m)
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

-------------------------------------------------------------------------------
-- implement classic QC tests for basic k/v properties
-------------------------------------------------------------------------------

-- | You can lookup what you inserted.
prop_lookupInsert ::
     IsTableHandle h
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
     IsTableHandle h
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
     IsTableHandle h
  => Proxy h -> [(Key, Update Value)]
  -> Key -> Property
prop_lookupDelete h ups k = ioProperty $ do
    withTableNew h ups $ \_ hdl -> do
      deletes hdl (V.singleton k)
      res <- lookups hdl (V.singleton k)
      return $ res === V.singleton NotFound

-- | Delete doesn't change the lookup results of other keys
prop_lookupDeleteElse ::
     IsTableHandle h
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
     IsTableHandle h
  => Proxy h -> [(Key, Update Value)]
  -> Key -> Value -> Value -> Property
prop_insertInsert h ups k v1 v2 = ioProperty $ do
    withTableNew h ups $ \_ hdl -> do
      inserts hdl (V.fromList [(k, v1), (k, v2)])
      res <- lookups hdl (V.singleton k)
      return $ res === V.singleton (Found v2)

-- | Inserts with different keys don't interfere.
prop_insertCommutes ::
       IsTableHandle h
    => Proxy h -> [(Key, Update Value)]
    -> Key -> Value -> Key -> Value -> Property
prop_insertCommutes h ups k1 v1 k2 v2 = k1 /= k2 ==> ioProperty do
    withTableNew h ups $ \_ hdl -> do
      inserts hdl (V.fromList [(k1, v1), (k2, v2)])

      res <- lookups hdl (V.fromList [k1, k2])
      return $ res === V.fromList [Found v1, Found v2]

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
    withTableNew h ups $ \_ hdl -> do

      res <- rangeLookup hdl r

      inserts hdl (V.singleton (k, v))

      res' <- rangeLookup hdl r

      let p :: RangeLookupResult Key Value -> Bool
          p rlr = rangeLookupResultKey rlr /= k

      if evalRange r k
      then return $ vsortOn rangeLookupResultKey (V.cons (FoundInRange k v) (V.filter p res)) === res'
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
     IsTableHandle h
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
-- implement classic QC tests for monoidal table merges
-------------------------------------------------------------------------------

prop_merge :: forall h.
     IsTableHandle h
  => Proxy h -> [(Key, Update Value)] -> [(Key, Update Value)]
  -> [Key] -> Property
prop_merge h ups1 ups2 (V.fromList -> testKeys) = ioProperty $ do
    withTableNew h ups1 $ \s hdl1 -> do
      Class.withTableNew  s (testTableConfig h) $ \hdl2 -> do
        updates hdl2 $ V.fromList ups2

        -- merge them.
        Class.withTableMerge hdl1 hdl2 $ \hdl3 -> do

          -- results in parts and the merge table
          res1 <- lookups hdl1 testKeys
          res2 <- lookups hdl2 testKeys
          res3 <- lookups hdl3 testKeys

          let mergeResult :: LookupResult Value -> LookupResult Value -> LookupResult Value
              mergeResult r@NotFound   NotFound     = r
              mergeResult   NotFound   r@(Found _)  = r
              mergeResult r@(Found _)    NotFound   = r
              mergeResult   (Found v1)   (Found v2) = Found (resolve v1 v2)

          return $ V.zipWith mergeResult res1 res2  == res3

-------------------------------------------------------------------------------
-- implement classic QC tests for snapshots
-------------------------------------------------------------------------------

-- changes to handle would not change the snapshot
prop_snapshotNoChanges :: forall h.
     IsTableHandle h
    => Proxy h -> [(Key, Update Value)]
    -> [(Key, Update Value)] -> [Key] -> Property
prop_snapshotNoChanges h ups ups' (V.fromList -> testKeys) = ioProperty $ do
    withTableNew h ups $ \sess hdl1 -> do

      res <- lookups hdl1 testKeys

      let name = fromMaybe (error "invalid name") $ mkSnapshotName "foo"

      snapshot name hdl1
      updates hdl1 $ V.fromList ups'

      Class.withTableOpen @h sess name $ \hdl2 -> do

        res' <- lookups hdl2 testKeys

        return $ res == res'

-- same snapshot may be opened multiple times,
-- and the handles are separate.
prop_snapshotNoChanges2 :: forall h.
     IsTableHandle h
    => Proxy h -> [(Key, Update Value)]
    -> [(Key, Update Value)] -> [Key] -> Property
prop_snapshotNoChanges2 h ups ups' (V.fromList -> testKeys) = ioProperty $ do
    withTableNew h ups $ \sess hdl0 -> do
      let name = fromMaybe (error "invalid name") $ mkSnapshotName "foo"
      snapshot name hdl0

      Class.withTableOpen @h sess name $ \hdl1 -> do
        Class.withTableOpen @h sess name $ \hdl2 -> do

          res <- lookups hdl1 testKeys
          updates hdl1 $ V.fromList ups'
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
     IsTableHandle h
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
     IsTableHandle h
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
