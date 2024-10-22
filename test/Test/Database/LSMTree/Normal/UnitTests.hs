{-# LANGUAGE OverloadedLists   #-}
{-# LANGUAGE OverloadedStrings #-}
module Test.Database.LSMTree.Normal.UnitTests (tests) where

import           Control.Tracer (nullTracer)
import           Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as BS
import qualified Data.Map as Map
import qualified Data.Vector as V
import           Data.Word
import qualified System.FS.API as FS

import           Database.LSMTree.Normal as R

import           Control.Exception (Exception, try)
import           Database.LSMTree.Extras.Generators (KeyForIndexCompact)
import qualified Test.QuickCheck as QC
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.HUnit
import           Test.Util.FS (withTempIOHasBlockIO)


tests :: TestTree
tests =
    testGroup "Normal.UnitTests"
      [ testCaseSteps "unit_blobs"         unit_blobs
      , testCase      "unit_closed_table"  unit_closed_table
      , testCase      "unit_closed_cursor" unit_closed_cursor
      , testCase      "unit_twoTableTypes" unit_twoTableTypes
      , testCase      "unit_snapshots"     unit_snapshots
      ]

unit_blobs :: (String -> IO ()) -> Assertion
unit_blobs info =
    withTempIOHasBlockIO "test" $ \hfs hbio ->
    withSession nullTracer hfs hbio (FS.mkFsPath []) $ \sess -> do
      tbh <- new @_ @ByteString @ByteString @ByteString sess defaultTableConfig
      inserts [("key1", "value1", Just "blob1")] tbh

      res <- lookups ["key1"] tbh
      info (show res)

      case res of
        [FoundWithBlob val bref] -> do
          val @?= "value1"
          blob <- retrieveBlobs sess [bref]
          info (show blob)
          blob @?= ["blob1"]
        _ -> assertFailure "expected FoundWithBlob"

unit_closed_table :: Assertion
unit_closed_table =
    withTempIOHasBlockIO "test" $ \hfs hbio ->
    withSession nullTracer hfs hbio (FS.mkFsPath []) $ \sess -> do
      tbl <- new @_ @Key1 @Value1 @Blob1 sess defaultTableConfig
      inserts [(Key1 42, Value1 42, Nothing)] tbl
      r1 <- lookups [Key1 42] tbl
      V.map ignoreBlobRef r1 @?= [Found (Value1 42)]
      close tbl
      -- Expect ErrTableClosed for operations after close
      assertException ErrTableClosed $
        lookups [Key1 42] tbl >> pure ()
      -- But closing again is idempotent
      close tbl

unit_closed_cursor :: Assertion
unit_closed_cursor =
    withTempIOHasBlockIO "test" $ \hfs hbio ->
    withSession nullTracer hfs hbio (FS.mkFsPath []) $ \sess -> do
      tbl <- new @_ @Key1 @Value1 @Blob1 sess defaultTableConfig
      inserts [(Key1 42, Value1 42, Nothing), (Key1 43, Value1 43, Nothing)] tbl
      cur <- newCursor tbl
      -- closing the table should not affect the cursor
      close tbl
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
      tbl1 <- new @_ @Key1 @Value1 @Blob1 sess tableConfig
      tbl2 <- new @_ @Key2 @Value2 @Blob2 sess tableConfig

      kvs1 <- QC.generate (Map.fromList <$> QC.vectorOf 100 QC.arbitrary)
      kvs2 <- QC.generate (Map.fromList <$> QC.vectorOf 100 QC.arbitrary)
      let ins1 = V.fromList [ (Key1 k, Value1 v, Nothing)
                            | (k,v) <- Map.toList kvs1 ]
          ins2 = V.fromList [ (Key2 k, Value2 v, Nothing)
                            | (k,v) <- Map.toList kvs2 ]
      inserts ins1 tbl1
      inserts ins2 tbl2

      snapshot snap1 tbl1
      snapshot snap2 tbl2
      tbl1' <- open @_ @Key1 @Value1 @Blob1 sess configNoOverride snap1
      tbl2' <- open @_ @Key2 @Value2 @Blob2 sess configNoOverride snap2

      vs1 <- lookups ((\(k,_,_)->k) <$> ins1) tbl1'
      vs2 <- lookups ((\(k,_,_)->k) <$> ins2) tbl2'

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
      tbl <- new @_ @Key1 @Value1 @Blob1 sess defaultTableConfig

      assertException (ErrSnapshotNotExists snap2) $
        deleteSnapshot sess snap2

      snapshot snap1 tbl
      assertException (ErrSnapshotExists snap1) $
        snapshot snap1 tbl

      assertException (ErrSnapshotWrongType snap1) $ do
        _ <- open @_ @Key2 @Value2 @Blob2 sess configNoOverride snap1
        return ()

      assertException (ErrSnapshotNotExists snap2) $ do
        _ <- open @_ @Key1 @Value1 @Blob1 sess configNoOverride snap2
        return ()
  where
    snap1, snap2 :: SnapshotName
    snap1 = "table1"
    snap2 = "table2"

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

newtype Blob1  = Blob1 Word64
  deriving stock (Show, Eq, Ord)
  deriving newtype (SerialiseValue)

instance Labellable (Key1, Value1, Blob1) where
  makeSnapshotLabel _ = "Key1 Value1 Blob1"

newtype Key2 = Key2 KeyForIndexCompact
  deriving stock (Show, Eq, Ord)
  deriving newtype (QC.Arbitrary, SerialiseKey)

newtype Value2 = Value2 BS.ByteString
  deriving stock (Show, Eq, Ord)
  deriving newtype (QC.Arbitrary, SerialiseValue)

newtype Blob2  = Blob2 BS.ByteString
  deriving stock (Show, Eq, Ord)
  deriving newtype (SerialiseValue)

instance Labellable (Key2, Value2, Blob2) where
  makeSnapshotLabel _ = "Key2 Value2 Blob2"
