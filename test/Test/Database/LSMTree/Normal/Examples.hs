{-# LANGUAGE OverloadedLists   #-}
{-# LANGUAGE OverloadedStrings #-}
module Test.Database.LSMTree.Normal.Examples (tests) where

import           Control.Tracer (nullTracer)
import           Data.ByteString (ByteString)
import qualified System.FS.API as FS
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.HUnit
import           Test.Util.FS (withTempIOHasBlockIO)

import           Database.LSMTree.Normal

tests :: TestTree
tests = testGroup "Normal.Examples"
    [ testCaseSteps "blobs" $ \ info ->
        withTempIOHasBlockIO "test" $ \hfs hbio ->
        withSession nullTracer hfs hbio (FS.mkFsPath []) $ \sess ->
        withTable @_ @ByteString @ByteString @ByteString sess defaultTableConfig $ \tbh -> do
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
    ]
