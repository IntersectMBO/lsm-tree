{-# LANGUAGE OverloadedLists   #-}
{-# LANGUAGE OverloadedStrings #-}

module Test.Database.LSMTree.UnitTests (tests) where

import           Control.Exception (Exception, bracket, try)
import           Control.Monad (void)
import           Control.Tracer (nullTracer)
import           Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as BS
import           Data.Foldable (for_)
import           Data.List.NonEmpty (NonEmpty (..))
import qualified Data.Map as Map
import qualified Data.Vector as V
import           Data.Word
import           Database.LSMTree as R
import           Database.LSMTree.Extras.Generators ()
import           Database.LSMTree.Internal.Serialise (SerialisedKey)
import qualified System.FS.API as FS
import qualified Test.QuickCheck.Arbitrary as QC
import qualified Test.QuickCheck.Gen as QC
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.HUnit
import           Test.Util.FS (withTempIOHasBlockIO)


tests :: TestTree
tests =
    testGroup "Test.Database.LSMTree.UnitTests"
      [ testCaseSteps "unit_blobs"          unit_blobs
      , testCase      "unit_closed_table"   unit_closed_table
      , testCase      "unit_closed_cursor"  unit_closed_cursor
      , testCase      "unit_twoTableTypes"  unit_twoTableTypes
      , testCase      "unit_snapshots"      unit_snapshots
      , testCase      "unit_unions_1"       unit_unions_1
      , testCase      "unit_union_credits"  unit_union_credits
      , testCase      "unit_union_credit_0" unit_union_credit_0
      , testCase      "unit_union_blobref_invalidation" unit_union_blobref_invalidation
      ]

testSalt :: R.Salt
testSalt = 4

unit_blobs :: (String -> IO ()) -> Assertion
unit_blobs info =
    withTempIOHasBlockIO "test" $ \hfs hbio ->
    withOpenSession nullTracer hfs hbio testSalt (FS.mkFsPath []) $ \sess -> do
      table <- newTable @_ @ByteString @(ResolveAsFirst ByteString) @ByteString sess
      inserts table [("key1", ResolveAsFirst "value1", Just "blob1")]

      res <- lookups table ["key1"]
      info (show res)

      case res of
        [FoundWithBlob val bref] -> do
          val @?= ResolveAsFirst "value1"
          blob <- retrieveBlobs sess [bref]
          info (show blob)
          blob @?= ["blob1"]
        _ -> assertFailure "expected FoundWithBlob"

unit_closed_table :: Assertion
unit_closed_table =
    withTempIOHasBlockIO "test" $ \hfs hbio ->
    withOpenSession nullTracer hfs hbio testSalt (FS.mkFsPath []) $ \sess -> do
      table <- newTable @_ @Key1 @Value1 @Blob1 sess
      inserts table [(Key1 42, Value1 42, Nothing)]
      r1 <- lookups table [Key1 42]
      V.map ignoreBlobRef r1 @?= [Found (Value1 42)]
      closeTable table
      -- Expect ErrTableClosed for operations after closeTable
      assertException ErrTableClosed $
        lookups table [Key1 42] >> pure ()
      -- But closing again is idempotent
      closeTable table

unit_closed_cursor :: Assertion
unit_closed_cursor =
    withTempIOHasBlockIO "test" $ \hfs hbio ->
    withOpenSession nullTracer hfs hbio testSalt (FS.mkFsPath []) $ \sess -> do
      table <- newTable @_ @Key1 @Value1 @Blob1 sess
      inserts table [(Key1 42, Value1 42, Nothing), (Key1 43, Value1 43, Nothing)]
      cur <- newCursor table
      -- closing the table should not affect the cursor
      closeTable table
      r1 <- next cur
      fmap ignoreBlobRef r1 @?= Just (Entry (Key1 42) (Value1 42))
      closeCursor cur
      -- Expect ErrCursorClosed for operations after closeCursor
      assertException ErrCursorClosed $
        next cur >> pure ()
      -- But closing again is idempotent
      closeCursor cur

unit_twoTableTypes :: Assertion
unit_twoTableTypes =
    withTempIOHasBlockIO "test" $ \hfs hbio ->
    withOpenSession nullTracer hfs hbio testSalt (FS.mkFsPath []) $ \sess -> do
      let tableConfig =
            defaultTableConfig {
              confWriteBufferAlloc = AllocNumEntries 10
            }
      table1 <- newTableWith @_ @Key1 @Value1 @Blob1 tableConfig sess
      table2 <- newTableWith @_ @Key2 @Value2 @Blob2 tableConfig sess

      kvs1 <- QC.generate (Map.fromList <$> QC.vectorOf 100 QC.arbitrary)
      kvs2 <- QC.generate (Map.fromList <$> QC.vectorOf 100 QC.arbitrary)
      let ins1 = V.fromList [ (Key1 k, Value1 v, Nothing)
                            | (k,v) <- Map.toList kvs1 ]
          ins2 = V.fromList [ (Key2 k, Value2 v, Nothing)
                            | (k,v) <- Map.toList kvs2 ]
      inserts table1 ins1
      inserts table2 ins2

      saveSnapshot snap1 label1 table1
      saveSnapshot snap2 label2 table2
      table1' <- openTableFromSnapshot @_ @Key1 @Value1 @Blob1 sess snap1 label1
      table2' <- openTableFromSnapshot @_ @Key2 @Value2 @Blob2 sess snap2 label2

      vs1 <- lookups table1' ((\(k,_,_)->k) <$> ins1)
      vs2 <- lookups table2' ((\(k,_,_)->k) <$> ins2)

      V.map ignoreBlobRef vs1 @?= ((\(_,v,_) -> Found v) <$> ins1)
      V.map ignoreBlobRef vs2 @?= ((\(_,v,_) -> Found v) <$> ins2)
  where
    snap1, snap2 :: SnapshotName
    snap1 = "table1"
    snap2 = "table2"

unit_snapshots :: Assertion
unit_snapshots =
    withTempIOHasBlockIO "test" $ \hfs hbio ->
    withOpenSession nullTracer hfs hbio testSalt (FS.mkFsPath []) $ \sess -> do
      table <- newTable @_ @Key1 @Value1 @Blob1 sess

      assertException (ErrSnapshotDoesNotExist snap2) $
        deleteSnapshot sess snap2

      saveSnapshot snap1 label1 table
      assertException (ErrSnapshotExists snap1) $
        saveSnapshot snap1 label1 table

      assertException (ErrSnapshotWrongLabel snap1
                        (SnapshotLabel "Key2 Value2 Blob2")
                        (SnapshotLabel "Key1 Value1 Blob1")) $ do
        _ <- openTableFromSnapshot @_ @Key2 @Value2 @Blob2 sess snap1 label2
        pure ()

      assertException (ErrSnapshotDoesNotExist snap2) $ do
        _ <- openTableFromSnapshot @_ @Key1 @Value1 @Blob1 sess snap2 label2
        pure ()
  where
    snap1, snap2 :: SnapshotName
    snap1 = "table1"
    snap2 = "table2"

-- | Unions of 1 table are equivalent to duplicate
unit_unions_1 :: Assertion
unit_unions_1 =
    withTempIOHasBlockIO "test" $ \hfs hbio ->
    withOpenSession nullTracer hfs hbio testSalt (FS.mkFsPath []) $ \sess ->
    withTable @_ @Key1 @Value1 @Blob1 sess $ \table -> do
      inserts table [(Key1 17, Value1 42, Nothing)]

      bracket (unions $ table :| []) closeTable $ \table' ->
        bracket (duplicate table) closeTable $ \table'' -> do
          inserts table' [(Key1 17, Value1 43, Nothing)]
          inserts table'' [(Key1 17, Value1 44, Nothing)]

          -- The original table is unmodified
          r <- lookups table [Key1 17]
          V.map ignoreBlobRef r @?= [Found (Value1 42)]

          -- The unioned table sees an updated value
          r' <- lookups table' [Key1 17]
          V.map ignoreBlobRef r' @?= [Found (Value1 43)]

          -- The duplicated table sees a different updated value
          r'' <- lookups table'' [Key1 17]
          V.map ignoreBlobRef r'' @?= [Found (Value1 44)]

-- | Querying or supplying union credits to non-union tables is trivial.
unit_union_credits :: Assertion
unit_union_credits =
    withTempIOHasBlockIO "test" $ \hfs hbio ->
    withOpenSession nullTracer hfs hbio testSalt (FS.mkFsPath []) $ \sess ->
    withTable @_ @Key1 @Value1 @Blob1 sess $ \table -> do
      inserts table [(Key1 17, Value1 42, Nothing)]

      -- The table is not the result of a union, so the debt is always 0,
      UnionDebt debt <- remainingUnionDebt table
      debt @?= 0

      -- and supplying credits returns them all as leftovers.
      UnionCredits leftover <- supplyUnionCredits table (UnionCredits 42)
      leftover @?= 42

-- | Supplying zero or negative credits to union tables works, but does nothing.
unit_union_credit_0 :: Assertion
unit_union_credit_0 =
    withTempIOHasBlockIO "test" $ \hfs hbio ->
    withOpenSession nullTracer hfs hbio testSalt (FS.mkFsPath []) $ \sess ->
    withTable @_ @Key1 @Value1 @Blob1 sess $ \table -> do
      inserts table [(Key1 17, Value1 42, Nothing)]

      bracket (table `union` table) closeTable $ \table' -> do
        -- Supplying 0 credits works and returns 0 leftovers.
        UnionCredits leftover <- supplyUnionCredits table' (UnionCredits 0)
        leftover @?= 0

        -- Supplying negative credits also works and returns 0 leftovers.
        UnionCredits leftover' <- supplyUnionCredits table' (UnionCredits (-42))
        leftover' @?= 0

        -- And the table is still vaguely cromulent
        r <- lookups table' [Key1 17]
        V.map ignoreBlobRef r @?= [Found (Value1 42)]

-- | Blob refs into a union don't get invalidated when updating the union's
-- input tables.
unit_union_blobref_invalidation :: Assertion
unit_union_blobref_invalidation =
    withTempIOHasBlockIO "test" $ \hfs hbio ->
    withOpenSession nullTracer hfs hbio testSalt (FS.mkFsPath []) $ \sess ->
    withTableWith config sess $ \t1 -> do
      for_ ([0..99] :: [Word64]) $ \i ->
        inserts t1 [(Key1 i, Value1 i, Just (Blob1 i))]
      withUnion t1 t1 $ \t2 -> do
        -- do lookups on the union table (the result contains blob refs)
        res <- lookups t2 (Key1 <$> [0..99])

        -- progress original table (supplying merge credits would be most direct),
        -- so merges complete
        inserts t1 (fmap (\i -> (Key1 i, Value1 i, Nothing)) [1000..2000])

        -- try to resolve the blob refs we obtained earlier
        void $ retrieveBlobs sess (V.mapMaybe R.getBlob res)
  where
    config = defaultTableConfig {
        confWriteBufferAlloc = AllocNumEntries 4
      }

ignoreBlobRef :: Functor f => f (BlobRef m b) -> f ()
ignoreBlobRef = fmap (const ())

assertException :: (Exception e, Eq e) => e -> Assertion -> Assertion
assertException e assertion = do
   r <- try assertion
   r @?= Left e

{-------------------------------------------------------------------------------
  Key and value types
-------------------------------------------------------------------------------}

newtype Key1   = Key1 Word64
  deriving stock (Show, Eq, Ord)
  deriving newtype (SerialiseKey)

newtype Value1 = Value1 Word64
  deriving stock (Show, Eq, Ord)
  deriving newtype (SerialiseValue)

deriving via ResolveAsFirst Word64 instance ResolveValue Value1

newtype Blob1  = Blob1 Word64
  deriving stock (Show, Eq, Ord)
  deriving newtype (SerialiseValue)

label1 :: SnapshotLabel
label1 = SnapshotLabel "Key1 Value1 Blob1"

newtype Key2 = Key2 SerialisedKey
  deriving stock (Show, Eq, Ord)
  deriving newtype (QC.Arbitrary, SerialiseKey)

newtype Value2 = Value2 BS.ByteString
  deriving stock (Show, Eq, Ord)
  deriving newtype (QC.Arbitrary, SerialiseValue)

deriving via ResolveAsFirst BS.ByteString instance ResolveValue Value2

newtype Blob2  = Blob2 BS.ByteString
  deriving stock (Show, Eq, Ord)
  deriving newtype (SerialiseValue)

label2 :: SnapshotLabel
label2 = SnapshotLabel "Key2 Value2 Blob2"
