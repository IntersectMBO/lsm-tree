{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}

module Test.Database.LSMTree.Generators (tests) where

import           Data.Word (Word64)
import           Database.LSMTree.Generators
import           Test.QuickCheck
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.QuickCheck (testProperty)

tests :: TestTree
tests = testGroup "Test.Database.LSMTree.Generators" [
      testGroup "Range-finder bit-precision" [
          testProperty "Arbitrary satisfies invariant" $
            property . rfprecInvariant
        , testProperty "Shrinking satisfies invariant" $
            property . all rfprecInvariant . shrink @RFPrecision
        ]
    , testGroup "Pages (not partitioned)" [
          testProperty "Arbitrary satisfies invariant" $
            property . pagesInvariant @Word64
        , testProperty "Shrinking satisfies invariant" $
            property . all pagesInvariant . shrink @(Pages Word64)
        ]
    , testGroup "Pages (partitioned)" [
          testProperty "Arbitrary satisfies invariant" $
            property . partitionedPagesInvariant @Word64
        , testProperty "Shrinking satisfies invariant" $
            property . all partitionedPagesInvariant . shrink @(PartitionedPages Word64)
        ]
    ]
