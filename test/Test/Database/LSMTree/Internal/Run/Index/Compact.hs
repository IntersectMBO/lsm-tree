{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}

module Test.Database.LSMTree.Internal.Run.Index.Compact (tests) where

import           Data.Word (Word64)
import           Database.LSMTree.Generators
import           Database.LSMTree.Internal.Run.Index.Compact
import           Test.QuickCheck
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.QuickCheck (testProperty)

tests :: TestTree
tests = testGroup "Test.Database.LSMTree.Internal.Run.Index.Compact" [
      testGroup "Pages (not partitioned)" [
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
    , testProperty "prop_searchMinMaxKeysAfterConstruction" $
        prop_searchMinMaxKeysAfterConstruction @Word64
    ]

{-------------------------------------------------------------------------------
  Properties
-------------------------------------------------------------------------------}

-- | After construction, searching for the minimum/maximum key of every page
-- @pageNr@ returns the @pageNr@.
--
-- Example: @search minKey (fromList rfprec [(minKey, maxKey)]) == 0@.
prop_searchMinMaxKeysAfterConstruction ::
     (SliceBits k, Integral k, Show k)
  => PartitionedPages k
  -> Property
prop_searchMinMaxKeysAfterConstruction (PartitionedPages (RFPrecision rfprec) ks) =
      classify (hasClashes ci) "Compact index contains clashes"
    $ tabulate "Range-finder bit-precision" [show rfprec]
    $ counterexample (show idxs)
    $ counterexample (dumpInternals ci)
    $ property $ all p idxs
  where
    ci = fromList rfprec ks

    f idx (minKey, maxKey) =
        ( idx
        , search minKey ci
        , search maxKey ci
        , search (minKey + (maxKey - minKey) `div` 2) ci
        )

    idxs = zipWith f [0..] ks

    p (idx, x, y, z) =
         Just idx == x && Just idx == y && Just idx == z

-- TODO: test for arbitrary keys instead of only min and max keys on pages.
