{-# LANGUAGE OverloadedStrings #-}
module Test.Database.LSMTree.Normal.Examples (tests) where

import           Control.Tracer (nullTracer)
import           Data.ByteString (ByteString)
import qualified Data.Vector as V
import qualified System.FS.API as FS
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.HUnit (testCaseSteps)
import           Test.Util.FS (withTempIOHasBlockIO)

import           Database.LSMTree.Normal

tests :: TestTree
tests = testGroup "Normal.Examples"
    [ testCaseSteps "blobs" $ \ _info ->
        withTempIOHasBlockIO "test" $ \hfs hbio ->
        withSession nullTracer hfs hbio (FS.mkFsPath []) $ \sess ->
        withTable @_ @ByteString @ByteString @ByteString sess defaultTableConfig $ \tbh -> do
            flip inserts tbh $ V.fromList
                [ ("key1", "value1", Just "blob1")
                ]

            -- This doesn't work yet
            -- res <- flip lookups tbh $ V.fromList
            --    [ "key1"
            --    ]
            --
            -- info $ show res

            -- TODO: we expect to get lookup with blob back
            -- TODO: fetch the blob.
    ]
