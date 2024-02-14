{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}

module Test.Database.LSMTree.Generators (tests) where

import           Data.ByteString (ByteString)
import           Data.Word (Word64)
import           Database.LSMTree.Generators (ChunkSize, LogicalPageSummaries,
                     RFPrecision, chunkSizeInvariant, pagesInvariant,
                     rfprecInvariant, writeBufferInvariant)
import           Test.Database.LSMTree.Internal.Run.Index.Compact ()
import           Test.QuickCheck (Arbitrary (..), Testable (..))
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.QuickCheck (testProperty)

tests :: TestTree
tests = testGroup "Test.Database.LSMTree.Generators" [
      testGroup "WriteBuffer" [
          testProperty "Arbitrary satisfies invariant" $ property $
            writeBufferInvariant @ByteString @ByteString @ByteString
        , testProperty "Shrinking satisfies invariant" $ property $
            all (writeBufferInvariant @ByteString @ByteString @ByteString)
              . shrink
      ]
    , testGroup "Range-finder bit-precision" [
          testProperty "Arbitrary satisfies invariant" $
            property . rfprecInvariant
        , testProperty "Shrinking satisfies invariant" $
            property . all rfprecInvariant . shrink @RFPrecision
      ]
    , testGroup "LogicalPageSummaries" [
          testProperty "Arbitrary satisfies invariant" $
            property . pagesInvariant @Word64
        , testProperty "Shrinking satisfies invariant" $
            property . all pagesInvariant . shrink @(LogicalPageSummaries Word64)
        ]
    , testGroup "Chunk size" [
        testProperty "Arbitrary satisfies invariant" $
            property . chunkSizeInvariant
        , testProperty "Shrinking satisfies invariant" $
            property . all chunkSizeInvariant . shrink @ChunkSize
        ]
    ]
