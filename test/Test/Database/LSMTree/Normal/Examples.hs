{-# LANGUAGE OverloadedLists   #-}
{-# LANGUAGE OverloadedStrings #-}
module Test.Database.LSMTree.Normal.Examples (tests) where

import           Data.ByteString (ByteString)
import           Control.Tracer (nullTracer)
import qualified System.FS.API as FS

import           Database.LSMTree.Normal as R

import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.HUnit
import           Test.Util.FS (withTempIOHasBlockIO)


tests :: TestTree
tests =
    testGroup "Normal.Examples"
      [ testCaseSteps "unit_blobs"         unit_blobs
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
