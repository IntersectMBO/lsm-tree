{-# LANGUAGE OverloadedLists   #-}
{-# LANGUAGE OverloadedStrings #-}

{- HLINT ignore "Use void" -}

module Test.Database.LSMTree.UnitTests (tests) where

import           Control.Tracer (nullTracer)
import           Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as BS
import           Data.List.NonEmpty (NonEmpty (..))
import qualified Data.Map as Map
import qualified Data.Vector as V
import           Data.Word
import qualified System.FS.API as FS

import           Database.LSMTree as R

import           Control.Exception (Exception, bracket, try)
import           Database.LSMTree.Extras.Generators ()
import           Database.LSMTree.Internal.Serialise (SerialisedKey)
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
      ]

unit_blobs :: (String -> IO ()) -> Assertion
unit_blobs info =
    withTempIOHasBlockIO "test" $ \hfs hbio ->
    withSession nullTracer hfs hbio (FS.mkFsPath []) $ \sess -> do
      table <- new @_ @ByteString @(ResolveAsFirst ByteString) @ByteString sess defaultTableConfig
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
    withSession nullTracer hfs hbio (FS.mkFsPath []) $ \sess -> do
      table <- new @_ @Key1 @Value1 @Blob1 sess defaultTableConfig
      inserts table [(Key1 42, Value1 42, Nothing)]
      r1 <- lookups table [Key1 42]
      V.map ignoreBlobRef r1 @?= [Found (Value1 42)]
      close table
      -- Expect ErrTableClosed for operations after close
      assertException ErrTableClosed $
        lookups table [Key1 42] >> pure ()
      -- But closing again is idempotent
      close table

unit_closed_cursor :: Assertion
unit_closed_cursor =
    withTempIOHasBlockIO "test" $ \hfs hbio ->
    withSession nullTracer hfs hbio (FS.mkFsPath []) $ \sess -> do
      table <- new @_ @Key1 @Value1 @Blob1 sess defaultTableConfig
      inserts table [(Key1 42, Value1 42, Nothing), (Key1 43, Value1 43, Nothing)]
      cur <- newCursor table
      -- closing the table should not affect the cursor
      close table
      r1 <- readCursor 1 cur
      V.map ignoreBlobRef r1 @?= [FoundInQuery (Key1 42) (Value1 42)]
      closeCursor cur
      -- Expect ErrCursorClosed for operations after closeCursor
      assertException ErrCursorClosed $
        readCursor 1 cur >> pure ()
      -- But closing again is idempotent
      closeCursor cur

unit_twoTableTypes :: Assertion
unit_twoTableTypes =
    withTempIOHasBlockIO "test" $ \hfs hbio ->
    withSession nullTracer hfs hbio (FS.mkFsPath []) $ \sess -> do
      let tableConfig =
            defaultTableConfig {
              confWriteBufferAlloc = AllocNumEntries (NumEntries 10)
            }
      table1 <- new @_ @Key1 @Value1 @Blob1 sess tableConfig
      table2 <- new @_ @Key2 @Value2 @Blob2 sess tableConfig

      kvs1 <- QC.generate (Map.fromList <$> QC.vectorOf 100 QC.arbitrary)
      kvs2 <- QC.generate (Map.fromList <$> QC.vectorOf 100 QC.arbitrary)
      let ins1 = V.fromList [ (Key1 k, Value1 v, Nothing)
                            | (k,v) <- Map.toList kvs1 ]
          ins2 = V.fromList [ (Key2 k, Value2 v, Nothing)
                            | (k,v) <- Map.toList kvs2 ]
      inserts table1 ins1
      inserts table2 ins2

      createSnapshot label1 snap1 table1
      createSnapshot label2 snap2 table2
      table1' <- openSnapshot @_ @Key1 @Value1 @Blob1 sess configNoOverride label1 snap1
      table2' <- openSnapshot @_ @Key2 @Value2 @Blob2 sess configNoOverride label2 snap2

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
    withSession nullTracer hfs hbio (FS.mkFsPath []) $ \sess -> do
      table <- new @_ @Key1 @Value1 @Blob1 sess defaultTableConfig

      assertException (ErrSnapshotDoesNotExist snap2) $
        deleteSnapshot sess snap2

      createSnapshot label1 snap1 table
      assertException (ErrSnapshotExists snap1) $
        createSnapshot label1 snap1 table

      assertException (ErrSnapshotWrongLabel snap1
                        (SnapshotLabel "Key2 Value2 Blob2")
                        (SnapshotLabel "Key1 Value1 Blob1")) $ do
        _ <- openSnapshot @_ @Key2 @Value2 @Blob2 sess configNoOverride label2 snap1
        return ()

      assertException (ErrSnapshotDoesNotExist snap2) $ do
        _ <- openSnapshot @_ @Key1 @Value1 @Blob1 sess configNoOverride label2 snap2
        return ()
  where
    snap1, snap2 :: SnapshotName
    snap1 = "table1"
    snap2 = "table2"

-- | Unions of 1 table are equivalent to duplicate
unit_unions_1 :: Assertion
unit_unions_1 =
    withTempIOHasBlockIO "test" $ \hfs hbio ->
    withSession nullTracer hfs hbio (FS.mkFsPath []) $ \sess ->
    withTable @_ @Key1 @Value1 @Blob1 sess defaultTableConfig $ \table -> do
      inserts table [(Key1 17, Value1 42, Nothing)]

      bracket (unions $ table :| []) close $ \table' ->
        bracket (duplicate table) close $ \table'' -> do
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
    withSession nullTracer hfs hbio (FS.mkFsPath []) $ \sess ->
    withTable @_ @Key1 @Value1 @Blob1 sess defaultTableConfig $ \table -> do
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
    withSession nullTracer hfs hbio (FS.mkFsPath []) $ \sess ->
    withTable @_ @Key1 @Value1 @Blob1 sess defaultTableConfig $ \table -> do
      inserts table [(Key1 17, Value1 42, Nothing)]

      bracket (table `union` table) close $ \table' -> do
        -- Supplying 0 credits works and returns 0 leftovers.
        UnionCredits leftover <- supplyUnionCredits table' (UnionCredits 0)
        leftover @?= 0

        -- Supplying negative credits also works and returns 0 leftovers.
        UnionCredits leftover' <- supplyUnionCredits table' (UnionCredits (-42))
        leftover' @?= 0

        -- And the table is still vaguely cromulent
        r <- lookups table' [Key1 17]
        V.map ignoreBlobRef r @?= [Found (Value1 42)]

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
