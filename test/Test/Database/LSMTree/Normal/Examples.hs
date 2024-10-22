{-# LANGUAGE OverloadedLists   #-}
{-# LANGUAGE OverloadedStrings #-}
module Test.Database.LSMTree.Normal.Examples (tests) where

import           Control.Tracer (nullTracer)
import           Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as BS
import qualified Data.Map as Map
import qualified Data.Vector as V
import           Data.Word
import qualified System.FS.API as FS

import           Database.LSMTree.Normal as R

import           Database.LSMTree.Extras.Generators (KeyForIndexCompact)
import qualified Test.QuickCheck as QC
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.HUnit
import           Test.Util.FS (withTempIOHasBlockIO)


tests :: TestTree
tests =
    testGroup "Normal.Examples"
      [ testCaseSteps "unit_blobs"         unit_blobs
      , testCase      "unit_twoTableTypes" unit_twoTableTypes
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

ignoreBlobRef :: LookupResult v (BlobRef m b) -> LookupResult v ()
ignoreBlobRef = fmap (const ())

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
