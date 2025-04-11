{-# LANGUAGE BlockArguments    #-}
{-# LANGUAGE OverloadedStrings #-}

module Test.Database.LSMTree.Class (
    tests
  ) where
import           Control.Exception (SomeException, assert, try)
import           Control.Monad (forM, when)
import           Control.Monad.ST.Strict (runST)
import           Control.Monad.Trans.State
import qualified Data.ByteString as BS
import           Data.Foldable (toList)
import           Data.Functor.Compose (Compose (..))
import qualified Data.List as List
import qualified Data.List.NonEmpty as NE
import qualified Data.Proxy as Proxy
import qualified Data.Vector as V
import qualified Data.Vector.Algorithms.Merge as VA
import           Data.Word (Word64)
import qualified Database.LSMTree as R
import           Database.LSMTree.Class
import           Database.LSMTree.Extras.Generators ()
import qualified Database.LSMTree.Model.IO as ModelIO
import qualified System.FS.API as FS
import           Test.Database.LSMTree.StateMachine ()
import           Test.QuickCheck.Monadic (monadicIO, monitor, run)
import           Test.Tasty (TestName, TestTree, testGroup)
import qualified Test.Tasty.QuickCheck as QC
import           Test.Tasty.QuickCheck hiding (label)
import qualified Test.Util.FS as FS

tests :: TestTree
tests = testGroup "Test.Database.LSMTree.Class"
    [ testGroup "Model" $ zipWith ($) (props tbl1) expectFailures1
    , testGroup "Real"  $ zipWith ($) (props tbl2) expectFailures2
    ]
  where
    tbl1 :: RunSetup ModelIO.Table IO
    tbl1 = RunSetup $ \conf -> Setup {
          testTableConfig = conf
        , testWithSessionArgs = \action -> action ModelIO.NoSessionArgs
        }

    expectFailures1 = repeat False

    tbl2 :: RunSetup R.Table IO
    tbl2 = RunSetup $ \conf -> Setup {
          testTableConfig = conf
        , testWithSessionArgs = \action ->
            FS.withTempIOHasBlockIO "R" $ \hfs hbio ->
              action (SessionArgs hfs hbio (FS.mkFsPath []))
        }

    expectFailures2 = repeat False

    props RunSetup {..} =
      [ testProperty' "lookup-insert" $ prop_lookupInsert . runSetup
      , testProperty' "lookup-insert-else" $ prop_lookupInsertElse . runSetup
      , testProperty' "lookup-insert-blob" $ prop_lookupInsertBlob . runSetup
      , testProperty' "lookup-delete" $ prop_lookupDelete . runSetup
      , testProperty' "lookup-delete-else" $ prop_lookupDeleteElse . runSetup
      , testProperty' "insert-insert" $ prop_insertInsert . runSetup
      , testProperty' "insert-insert-blob" $ prop_insertInsertBlob . runSetup
      , testProperty' "insert-commutes" $ prop_insertCommutes . runSetup
      , testProperty' "insert-commutes-blob" $ prop_insertCommutesBlob . runSetup
      , testProperty' "invalidated-blob-references" $ prop_updatesMayInvalidateBlobRefs . runSetup
      , testProperty' "dup-insert-insert" $ prop_dupInsertInsert . runSetup
      , testProperty' "dup-insert-comm" $ prop_dupInsertCommutes . runSetup
      , testProperty' "dup-nochanges" $ prop_dupNoChanges . runSetup
      , testProperty' "lookupRange-like-lookups" $ prop_lookupRangeLikeLookups . runSetup
      , testProperty' "lookupRange-insert" $ prop_insertLookupRange . runSetup
      , testProperty' "readCursor-sorted" $ prop_readCursorSorted . runSetup
      , testProperty' "readCursor-num-results" $ prop_readCursorNumResults . runSetup
      , testProperty' "readCursor-insert" $ prop_readCursorInsert . runSetup
      , testProperty' "readCursor-delete" $ prop_readCursorDelete . runSetup
      , testProperty' "readCursor-delete-else" $ prop_readCursorDeleteElse . runSetup
      , testProperty' "readCursor-stable-view" $ prop_readCursorStableView . runSetup
      , testProperty' "readCursor-offset" $ prop_readCursorOffset . runSetup
      , testProperty' "snapshot-nochanges" $ prop_snapshotNoChanges . runSetup
      , testProperty' "snapshot-nochanges2" $ prop_snapshotNoChanges2 . runSetup
      , testProperty' "lookup-mupsert" $ prop_lookupUpdate . runSetup
      , testProperty' "union" $ prop_union . runSetup
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
  deriving newtype (Arbitrary, Semigroup, SerialiseValue)
  deriving ResolveValue via (R.ResolveViaSemigroup Value)

label :: SnapshotLabel
label = SnapshotLabel "Word64 ByteString ByteString"

type Proxy h = Setup h IO

newtype RunSetup h m = RunSetup {
    runSetup :: TableConfig h -> Setup h m
  }

data Setup h m = Setup {
    testTableConfig     :: TableConfig h
  , testWithSessionArgs :: forall a. (SessionArgs (Session h) m -> m a) -> m a
  }

-- | create session, table, and populate it with some data.
withSessionAndTableNew :: forall h m a.
     ( IsTable h
     , IOLike m
     )
  => Setup h m
  -> [(Key, Update Value Blob)]
  -> (Session h m -> h m Key Value Blob -> m a)
  -> m a
withSessionAndTableNew Setup{..} ups action =
    testWithSessionArgs $ \args ->
      withSession args $ \sesh ->
        withTableNew sesh testTableConfig $ \table -> do
          updates table (V.fromList ups)
          action sesh table

-- | Like 'retrieveBlobs' but works for any 'Traversable'.
--
-- Like 'partsOf' in @lens@ this uses state monad.
retrieveBlobsTrav ::
     ( IsTable h
     , IOLike m
     , SerialiseValue b
     , Traversable t
     , C_ b
     )
  => proxy h
  -> Session h m
  -> t (BlobRef h m b)
  -> m (t b)
retrieveBlobsTrav tbl ses brefs = do
  blobs <- retrieveBlobs tbl ses (V.fromList $ toList brefs)
  evalStateT (traverse (\_ -> state un) brefs) (V.toList blobs)
  where
    un []     = error "invalid traversal"
    un (x:xs) = (x, xs)

lookupsWithBlobs :: forall h m k v b.
     ( IsTable h
     , IOLike m
     , C k v b
     )
  => h m k v b
  -> Session h m
  -> V.Vector k
  -> m (V.Vector (LookupResult v b))
lookupsWithBlobs tbl ses ks = do
    res <- lookups tbl ks
    getCompose <$> retrieveBlobsTrav (Proxy.Proxy @h) ses (Compose res)

rangeLookupWithBlobs :: forall h m k v b.
     ( IsTable h
     , IOLike m
     , C k v b
     )
  => h m k v b
  -> Session h m
  -> Range k
  -> m (V.Vector (Entry k v b))
rangeLookupWithBlobs tbl ses r = do
    res <- rangeLookup tbl r
    getCompose <$> retrieveBlobsTrav (Proxy.Proxy @h) ses (Compose res)

readCursorWithBlobs :: forall h m k v b proxy.
     ( IsTable h
     , IOLike m
     , C k v b
     )
  => proxy h
  -> Session h m
  -> Cursor h m k v b
  -> Int
  -> m (V.Vector (Entry k v b))
readCursorWithBlobs tbl ses cursor n = do
    res <- readCursor tbl n cursor
    getCompose <$> retrieveBlobsTrav tbl ses (Compose res)

readCursorAllWithBlobs :: forall h m k v b proxy.
     ( IsTable h
     , IOLike m
     , C k v b
     )
  => proxy h
  -> Session h m
  -> Cursor h m k v b
  -> CursorReadSchedule
  -> m [V.Vector (Entry k v b)]
readCursorAllWithBlobs tbl ses cursor = go . getCursorReadSchedule
  where
    go [] = error "readCursorAllWithBlobs: finite infinite list"
    go (n : ns) = do
      res <- readCursorWithBlobs tbl ses cursor n
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
  => Proxy h -> [(Key, Update Value Blob)]
  -> Key -> Value -> Property
prop_lookupInsert h ups k v = ioProperty $ do
    withSessionAndTableNew h ups $ \ses tbl -> do

      -- the main dish
      inserts tbl (V.singleton (k, v, Nothing))
      res <- lookupsWithBlobs tbl ses (V.singleton k)

      return $ res === V.singleton (Found v)

-- | Insert doesn't change the lookup results of other keys.
prop_lookupInsertElse ::
     IsTable h
  => Proxy h -> [(Key, Update Value Blob)]
  -> Key  -> Value -> [Key] -> Property
prop_lookupInsertElse h ups k v testKeys = ioProperty $ do
    withSessionAndTableNew h ups $ \ses tbl -> do

      let testKeys' = V.fromList $ filter (/= k) testKeys
      res1 <- lookupsWithBlobs tbl ses testKeys'
      inserts tbl (V.singleton (k, v, Nothing))
      res2 <-  lookupsWithBlobs tbl ses testKeys'

      return $ res1 === res2

-- | You cannot lookup what you have just deleted
prop_lookupDelete ::
     IsTable h
  => Proxy h -> [(Key, Update Value Blob)]
  -> Key -> Property
prop_lookupDelete h ups k = ioProperty $ do
    withSessionAndTableNew h ups $ \ses tbl -> do
      deletes tbl (V.singleton k)
      res <- lookupsWithBlobs tbl ses (V.singleton k)
      return $ res === V.singleton NotFound

-- | Delete doesn't change the lookup results of other keys
prop_lookupDeleteElse ::
     IsTable h
  => Proxy h -> [(Key, Update Value Blob)]
  -> Key  -> [Key] -> Property
prop_lookupDeleteElse h ups k testKeys = ioProperty $ do
    withSessionAndTableNew h ups $ \ses tbl -> do

      let testKeys' = V.fromList $ filter (/= k) testKeys
      res1 <- lookupsWithBlobs tbl ses testKeys'
      deletes tbl (V.singleton k)
      res2 <-  lookupsWithBlobs tbl ses testKeys'

      return $ res1 === res2

-- | Last insert wins.
prop_insertInsert ::
     IsTable h
  => Proxy h -> [(Key, Update Value Blob)]
  -> Key -> Value -> Value -> Property
prop_insertInsert h ups k v1 v2 = ioProperty $ do
    withSessionAndTableNew h ups $ \ses tbl -> do
      inserts tbl (V.fromList [(k, v1, Nothing), (k, v2, Nothing)])
      res <- lookupsWithBlobs tbl ses (V.singleton k)
      return $ res === V.singleton (Found v2)

-- | Inserts with different keys don't interfere.
prop_insertCommutes ::
       IsTable h
    => Proxy h -> [(Key, Update Value Blob)]
    -> Key -> Value -> Key -> Value -> Property
prop_insertCommutes h ups k1 v1 k2 v2 = k1 /= k2 ==> ioProperty do
    withSessionAndTableNew h ups $ \ses tbl -> do
      inserts tbl (V.fromList [(k1, v1, Nothing), (k2, v2, Nothing)])

      res <- lookupsWithBlobs tbl ses (V.fromList [k1,k2])
      return $ res === V.fromList [Found v1, Found v2]

-------------------------------------------------------------------------------
-- implement classic QC tests for cursors
-------------------------------------------------------------------------------

-- | Cursor read results are sorted by key.
prop_readCursorSorted ::
     forall h. IsTable h
  => Proxy h -> [(Key, Update Value Blob)]
  -> Maybe Key
  -> CursorReadSchedule
  -> Property
prop_readCursorSorted h ups offset ns = ioProperty $ do
    withSessionAndTableNew h ups $ \ses tbl -> do
      res <- withCursor offset tbl $ \cursor -> do
        V.concat <$> readCursorAllWithBlobs (Proxy.Proxy @h) ses cursor ns
      let keys = map queryResultKey (V.toList res)
      return $ keys === List.sort keys

-- | Cursor reads return the requested number of results, until the end.
prop_readCursorNumResults ::
     forall h. IsTable h
  => Proxy h -> [(Key, Update Value Blob)]
  -> Maybe Key
  -> CursorReadSchedule
  -> Property
prop_readCursorNumResults h ups offset ns = ioProperty $ do
    withSessionAndTableNew h ups $ \ses tbl -> do
      res <- withCursor offset tbl $ \cursor -> do
        readCursorAllWithBlobs (Proxy.Proxy @h) ses cursor ns
      let elemsRead = map V.length res
      let numFullReads = length res - 2
      return $ last elemsRead === 0
          .&&. take numFullReads elemsRead
           === take numFullReads (getCursorReadSchedule ns)

-- | You can read what you inserted.
prop_readCursorInsert ::
     forall h. IsTable h
  => Proxy h -> [(Key, Update Value Blob)]
  -> CursorReadSchedule
  -> Key -> Value -> Property
prop_readCursorInsert h ups ns k v = ioProperty $ do
    withSessionAndTableNew h ups $ \ses tbl -> do
      inserts tbl (V.singleton (k, v, Nothing))
      res <- withCursor Nothing tbl $ \cursor ->
        V.concat <$> readCursorAllWithBlobs (Proxy.Proxy @h) ses cursor ns
      return $ V.find (\r -> queryResultKey r == k) res
           === Just (Entry k v)

-- | You can't read what you deleted.
prop_readCursorDelete ::
     forall h. IsTable h
  => Proxy h -> [(Key, Update Value Blob)]
  -> CursorReadSchedule
  -> Key -> Property
prop_readCursorDelete h ups ns k = ioProperty $ do
    withSessionAndTableNew h ups $ \ses tbl -> do
      deletes tbl (V.singleton k)
      res <- withCursor Nothing tbl $ \cursor -> do
        V.concat <$> readCursorAllWithBlobs (Proxy.Proxy @h) ses cursor ns
      return $ V.find (\r -> queryResultKey r == k) res === Nothing

-- | Updates don't change the cursor read results of other keys.
prop_readCursorDeleteElse ::
     forall h. IsTable h
  => Proxy h -> [(Key, Update Value Blob)]
  -> Maybe Key
  -> CursorReadSchedule
  -> [(Key, Update Value Blob)] -> Property
prop_readCursorDeleteElse h ups offset ns ups2 = ioProperty $ do
    withSessionAndTableNew h ups $ \ses tbl -> do
      res1 <- withCursor offset tbl $ \cursor -> do
        V.concat <$> readCursorAllWithBlobs (Proxy.Proxy @h) ses cursor ns
      updates tbl (V.fromList ups2)
      res2 <- withCursor offset tbl $ \cursor -> do
        V.concat <$> readCursorAllWithBlobs (Proxy.Proxy @h) ses cursor ns
      let updatedKeys = map fst ups2
      return $ V.filter (\r -> queryResultKey r `notElem` updatedKeys) res1
           === V.filter (\r -> queryResultKey r `notElem` updatedKeys) res2

-- | Updates don't affect previously created cursors.
prop_readCursorStableView ::
     forall h. IsTable h
  => Proxy h -> [(Key, Update Value Blob)]
  -> Maybe Key
  -> CursorReadSchedule
  -> [(Key, Update Value Blob)] -> Property
prop_readCursorStableView h ups offset ns ups2 = ioProperty $ do
    withSessionAndTableNew h ups $ \ses tbl -> do
      res1 <- withCursor offset tbl $ \cursor -> do
        readCursorAllWithBlobs (Proxy.Proxy @h) ses cursor ns
      res2 <- withCursor offset tbl $ \cursor -> do
        updates tbl (V.fromList ups2)
        readCursorAllWithBlobs (Proxy.Proxy @h) ses cursor ns
      return $ res1 === res2

-- | Creating a cursor at an offset simply skips a prefix.
prop_readCursorOffset ::
     forall h. IsTable h
  => Proxy h -> [(Key, Update Value Blob)]
  -> Key
  -> CursorReadSchedule
  -> Property
prop_readCursorOffset h ups offset ns = ioProperty $ do
    withSessionAndTableNew h ups $ \ses tbl -> do
      res1 <- withCursor (Just offset) tbl $ \cursor -> do
        V.concat <$> readCursorAllWithBlobs (Proxy.Proxy @h) ses cursor ns
      res2 <- withCursor Nothing tbl $ \cursor -> do
        V.concat <$> readCursorAllWithBlobs (Proxy.Proxy @h) ses cursor ns
      return $ res1 === V.dropWhile ((< offset) . queryResultKey) res2

-------------------------------------------------------------------------------
-- implement classic QC tests for range lookups
-------------------------------------------------------------------------------

evalRange :: Ord k => Range k -> k -> Bool
evalRange (FromToExcluding lo hi) x = lo <= x && x < hi
evalRange (FromToIncluding lo hi) x = lo <= x && x <= hi

queryResultKey :: Entry k v b -> k
queryResultKey (Entry k _)             = k
queryResultKey (EntryWithBlob k _ _  ) = k

queryResultFromLookup :: k -> LookupResult v b -> Maybe (Entry k v b)
queryResultFromLookup k = \case
   NotFound -> Nothing
   Found v -> Just (Entry k v)
   FoundWithBlob v b -> Just (EntryWithBlob k v b)

-- | A range lookup behaves like many point lookups.
prop_lookupRangeLikeLookups ::
     IsTable h
  => Proxy h -> [(Key, Update Value Blob)]
  -> Range Key
  -> Property
prop_lookupRangeLikeLookups h ups r = ioProperty $ do
    withSessionAndTableNew h ups $ \ses tbl -> do
      res1 <- rangeLookupWithBlobs tbl ses r

      let testKeys = V.fromList $ nubSort $ filter (evalRange r) $ map fst ups
      res2 <- fmap (V.catMaybes . V.zipWith queryResultFromLookup testKeys) $
        lookupsWithBlobs tbl ses testKeys

      return $ res1 === res2

  where
    nubSort = map NE.head . NE.group . List.sort

-- | Last insert wins.
prop_insertLookupRange ::
     IsTable h
  => Proxy h -> [(Key, Update Value Blob)]
  -> Key -> Value -> Range Key  -> Property
prop_insertLookupRange h ups k v r = ioProperty $ do
    withSessionAndTableNew h ups $ \ses tbl -> do

      res <- rangeLookupWithBlobs tbl ses r

      inserts tbl (V.singleton (k, v, Nothing))

      res' <- rangeLookupWithBlobs tbl ses r

      let p :: Entry Key Value b -> Bool
          p rlr = queryResultKey rlr /= k

      if evalRange r k
      then return $ vsortOn queryResultKey (V.cons (Entry k v) (V.filter p res)) === res'
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
     IsTable h
  => Proxy h -> [(Key, Update Value Blob)]
  -> Key  -> Value -> Blob -> Property
prop_lookupInsertBlob h ups k v blob = ioProperty $ do
    withSessionAndTableNew h ups $ \ses tbl -> do

      -- the main dish
      inserts tbl (V.singleton (k, v, Just blob))
      res <- lookupsWithBlobs tbl ses (V.singleton k)

      return $ res === V.singleton (FoundWithBlob v blob)

-- | Last insert wins.
prop_insertInsertBlob ::
     IsTable h
  => Proxy h -> [(Key, Update Value Blob)]
  -> Key -> Value -> Value -> Maybe Blob -> Maybe Blob -> Property
prop_insertInsertBlob h ups k v1 v2 mblob1 mblob2 = ioProperty $ do
    withSessionAndTableNew h ups $ \ses tbl -> do
      inserts tbl (V.fromList [(k, v1, mblob1), (k, v2, mblob2)])
      res <- lookupsWithBlobs tbl ses (V.singleton k)
      return $ res === case mblob2 of
          Nothing    -> V.singleton (Found v2)
          Just blob2 -> V.singleton (FoundWithBlob v2 blob2)

-- | Inserts with different keys don't interfere.
prop_insertCommutesBlob ::
       IsTable h
    => Proxy h -> [(Key, Update Value Blob)]
    -> Key -> Value -> Maybe Blob
    -> Key -> Value -> Maybe Blob -> Property
prop_insertCommutesBlob h ups k1 v1 mblob1 k2 v2 mblob2 = k1 /= k2 ==> ioProperty do
    withSessionAndTableNew h ups $ \ses tbl -> do
      inserts tbl (V.fromList [(k1, v1, mblob1), (k2, v2, mblob2)])

      res <- lookupsWithBlobs tbl ses $ V.fromList [k1,k2]
      return $ res === case (mblob1, mblob2) of
          (Nothing,    Nothing)    -> V.fromList [Found v1,               Found v2]
          (Just blob1, Nothing)    -> V.fromList [FoundWithBlob v1 blob1, Found v2]
          (Nothing,    Just blob2) -> V.fromList [Found v1,               FoundWithBlob v2 blob2]
          (Just blob1, Just blob2) -> V.fromList [FoundWithBlob v1 blob1, FoundWithBlob v2 blob2]

-- | A blob reference may be invalidated by an update.
prop_updatesMayInvalidateBlobRefs ::
     forall h. IsTable h
  => Proxy h -> [(Key, Update Value Blob)]
  -> Key -> Value -> Blob
  -> [(Key, Update Value Blob)]
  -> Property
prop_updatesMayInvalidateBlobRefs h ups k1 v1 blob1 ups' = monadicIO $ do
    (res, blobs, res') <- run $ do
      withSessionAndTableNew h ups $ \ses tbl -> do
        inserts tbl (V.singleton (k1, v1, Just blob1))
        res <- lookups tbl (V.singleton k1)
        blobs <- getCompose <$> retrieveBlobsTrav (Proxy.Proxy @h) ses (Compose res)
        updates tbl (V.fromList ups')
        res' <- try @SomeException (getCompose <$> retrieveBlobsTrav (Proxy.Proxy @h) ses (Compose res))
        pure (res, blobs, res')

    case (V.toList res, V.toList blobs) of
      ([FoundWithBlob{}], [FoundWithBlob _ x])
        | Left _ <- res' ->
            monitor (QC.label "blob reference invalidated") >> pure True
        | Right (V.toList -> [FoundWithBlob _ y]) <- res' ->
            monitor (QC.label "blob reference valid") >> pure (x == y)
      _ -> monitor (counterexample "insert before lookup failed, somehow...") >> pure False

-------------------------------------------------------------------------------
-- implement classic QC tests for monoidal updates
-------------------------------------------------------------------------------

-- | You can lookup what you inserted.
prop_lookupUpdate ::
     IsTable h
  => Proxy h -> [(Key, Update Value Blob)]
  -> Key -> Value -> Maybe Blob -> Value -> Property
prop_lookupUpdate h ups k v1 mb1 v2 = ioProperty $ do
    withSessionAndTableNew h ups $ \s tbl -> do

      -- the main dish
      inserts tbl (V.singleton (k, v1, mb1))
      mupserts tbl (V.singleton (k, v2))
      res <- lookupsWithBlobs tbl s (V.singleton k)

      -- notice the order.
      return $ res === V.singleton (Found (resolve v2 v1))

-------------------------------------------------------------------------------
-- implement classic QC tests for monoidal table unions
-------------------------------------------------------------------------------

prop_union :: forall h.
     IsTable h
  => Proxy h -> [(Key, Update Value Blob)] -> [(Key, Update Value Blob)]
  -> [Key]
  -> Positive (Small Int) -- ^ Number of batches for supplying union credits
  -> Property
prop_union h ups1 ups2
  (V.fromList -> testKeys)
  (Positive (Small nSupplyBatches))
  = ioProperty $ do
    withSessionAndTableNew h ups1 $ \s tbl1 -> do
      withTableNew s (testTableConfig h) $ \tbl2 -> do
        updates tbl2 $ V.fromList ups2

        -- union them.
        withTableUnion tbl1 tbl2 $ \tbl3 -> do
          propBegin <- compareLookups s tbl1 tbl2 tbl3

          -- Supply union credits in @nSupplyBatches@ batches
          props <- forM (reverse [1 .. nSupplyBatches]) $ \i -> do
            -- Try to keep the batch sizes roughly the same size
            UnionDebt debt <- remainingUnionDebt tbl3
            _ <- supplyUnionCredits tbl3 (UnionCredits (debt `div` i))

            -- In case @i == 0@, then @debt `div` i == debt@, so the last supply
            -- should have finished the union.
            when (i == 1) $ do
              finalDebt <- remainingUnionDebt tbl3
              assert (finalDebt == UnionDebt 0) $ pure ()

            -- Check that the lookup results are still the same after each batch
            -- of union credits.
            compareLookups s tbl1 tbl2 tbl3

          pure (propBegin .&&. conjoin props)
  where
    compareLookups s tbl1 tbl2 tbl3 = do
      -- results in parts and the union table
      res1 <- lookupsWithBlobs tbl1 s testKeys
      res2 <- lookupsWithBlobs tbl2 s testKeys
      res3 <- lookupsWithBlobs tbl3 s testKeys

      let unionResult ::
               LookupResult Value Blob
            -> LookupResult Value Blob
            -> LookupResult Value Blob

          unionResult r@NotFound   NotFound            = r
          unionResult   NotFound r@(Found _)           = r
          unionResult   NotFound r@(FoundWithBlob _ _) = r

          unionResult r@(Found _)  NotFound
            = r
          unionResult   (Found v1) (Found v2)
            = Found (resolve v1 v2)
          unionResult   (Found v1) (FoundWithBlob v2 _)
            = Found (resolve v1 v2)

          unionResult r@(FoundWithBlob _ _)   NotFound
            = r
          unionResult   (FoundWithBlob v1 b1) (Found v2)
            = FoundWithBlob (resolve v1 v2) b1
          unionResult   (FoundWithBlob v1 b1) (FoundWithBlob v2 _b2)
            = FoundWithBlob (resolve v1 v2) b1

      return $ V.zipWith unionResult res1 res2 === res3

-------------------------------------------------------------------------------
-- implement classic QC tests for snapshots
-------------------------------------------------------------------------------

-- changes to handle would not change the snapshot
prop_snapshotNoChanges :: forall h.
     IsTable h
    => Proxy h -> [(Key, Update Value Blob)]
    -> [(Key, Update Value Blob)] -> [Key] -> Property
prop_snapshotNoChanges h ups ups' testKeys = ioProperty $ do
    withSessionAndTableNew h ups $ \ses tbl1 -> do

      res <- lookupsWithBlobs tbl1 ses $ V.fromList testKeys

      let name = R.toSnapshotName "foo"

      saveSnapshot name label tbl1
      updates tbl1 (V.fromList ups')

      withTableFromSnapshot @h ses label name$ \tbl2 -> do

        res' <- lookupsWithBlobs tbl2 ses $ V.fromList testKeys

        return $ res == res'

-- same snapshot may be opened multiple times,
-- and the handles are separate.
prop_snapshotNoChanges2 :: forall h.
     IsTable h
    => Proxy h -> [(Key, Update Value Blob)]
    -> [(Key, Update Value Blob)] -> [Key] -> Property
prop_snapshotNoChanges2 h ups ups' testKeys = ioProperty $ do
    withSessionAndTableNew h ups $ \sess tbl0 -> do
      let name = R.toSnapshotName "foo"
      saveSnapshot name label tbl0

      withTableFromSnapshot @h sess label name $ \tbl1 ->
        withTableFromSnapshot @h sess label name $ \tbl2 -> do

          res <- lookupsWithBlobs tbl1 sess $ V.fromList testKeys
          updates tbl1 (V.fromList ups')
          res' <- lookupsWithBlobs tbl2 sess $ V.fromList testKeys

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
  => Proxy h -> [(Key, Update Value Blob)]
  -> Key -> Value -> Value -> [Key] -> Property
prop_dupInsertInsert h ups k v1 v2 testKeys = ioProperty $ do
    withSessionAndTableNew h ups $ \sess tbl1 -> do
      withTableDuplicate tbl1 $ \tbl2 -> do

        inserts tbl1 (V.fromList [(k, v1, Nothing), (k, v2, Nothing)])
        inserts tbl2 (V.fromList [(k, v2, Nothing)])

        res1 <- lookupsWithBlobs tbl1 sess $ V.fromList testKeys
        res2 <- lookupsWithBlobs tbl2 sess $ V.fromList testKeys
        return $ res1 === res2

-- | Different key inserts commute.
prop_dupInsertCommutes ::
     IsTable h
    => Proxy h -> [(Key, Update Value Blob)]
    -> Key -> Value -> Key -> Value -> [Key] -> Property
prop_dupInsertCommutes h ups k1 v1 k2 v2 testKeys = k1 /= k2 ==> ioProperty do
    withSessionAndTableNew h ups $ \sess tbl1 -> do
      withTableDuplicate tbl1 $ \tbl2 -> do

        inserts tbl1 (V.fromList [(k1, v1, Nothing), (k2, v2, Nothing)])
        inserts tbl2 (V.fromList [(k2, v2, Nothing), (k1, v1, Nothing)])

        res1 <- lookupsWithBlobs tbl1 sess $ V.fromList testKeys
        res2 <- lookupsWithBlobs tbl2 sess $ V.fromList testKeys
        return $ res1 === res2

-- changes to one handle should not cause any visible changes in any others
prop_dupNoChanges ::
     IsTable h
    => Proxy h -> [(Key, Update Value Blob)]
    -> [(Key, Update Value Blob)] -> [Key] -> Property
prop_dupNoChanges h ups ups' testKeys = ioProperty $ do
    withSessionAndTableNew h ups $ \sess tbl1 -> do

      res <- lookupsWithBlobs tbl1 sess $ V.fromList testKeys

      withTableDuplicate tbl1 $ \tbl2 -> do
        updates tbl2 (V.fromList ups')

        -- lookup tbl1 again.
        res' <- lookupsWithBlobs tbl1 sess $ V.fromList testKeys

        return $ res == res'
