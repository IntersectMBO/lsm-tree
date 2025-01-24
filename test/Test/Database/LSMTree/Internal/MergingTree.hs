module Test.Database.LSMTree.Internal.MergingTree (tests) where

import           Control.Exception (bracket)
import           Control.RefCount
import           Database.LSMTree.Internal.MergingTree
import           Test.QuickCheck
import           Test.Tasty
import           Test.Tasty.QuickCheck

tests :: TestTree
tests = testGroup "Test.Database.LSMTree.Internal.MergingTree"
    [ testProperty "prop_isStructurallyEmpty" prop_isStructurallyEmpty
    ]

-- | Check that the merging tree constructor functions preserve the property
-- that if the inputs are obviously empty, the output is also obviously empty.
--
prop_isStructurallyEmpty :: EmptyMergingTree -> Property
prop_isStructurallyEmpty emt =
    ioProperty $
      bracket (mkEmptyMergingTree emt)
              releaseRef
              isStructurallyEmpty

-- | An expression to specify the shape of an empty 'MergingTree'
--
data EmptyMergingTree = ObviouslyEmptyLevelMerge
                      | ObviouslyEmptyUnionMerge
                      | NonObviouslyEmptyLevelMerge EmptyMergingTree
                      | NonObviouslyEmptyUnionMerge [EmptyMergingTree]
  deriving stock (Eq, Show)

instance Arbitrary EmptyMergingTree where
    arbitrary =
      sized $ \sz ->
        frequency $
        take (1 + sz)
        [ (1, pure ObviouslyEmptyLevelMerge)
        , (1, pure ObviouslyEmptyUnionMerge)
        , (2, NonObviouslyEmptyLevelMerge <$> resize (sz `div` 2) arbitrary)
        , (2, NonObviouslyEmptyUnionMerge <$> resize (sz `div` 2) arbitrary)
        ]
    shrink ObviouslyEmptyLevelMerge         = []
    shrink ObviouslyEmptyUnionMerge         = [ObviouslyEmptyLevelMerge]
    shrink (NonObviouslyEmptyLevelMerge mt) = ObviouslyEmptyLevelMerge
                                            : [ NonObviouslyEmptyLevelMerge mt'
                                              | mt' <- shrink mt ]
    shrink (NonObviouslyEmptyUnionMerge mt) = ObviouslyEmptyUnionMerge
                                            : [ NonObviouslyEmptyUnionMerge mt'
                                              | mt' <- shrink mt ]

mkEmptyMergingTree :: EmptyMergingTree -> IO (Ref (MergingTree IO h))
mkEmptyMergingTree ObviouslyEmptyLevelMerge = newPendingLevelMerge [] Nothing
mkEmptyMergingTree ObviouslyEmptyUnionMerge = newPendingUnionMerge []
mkEmptyMergingTree (NonObviouslyEmptyLevelMerge emt) = do
    mt  <- mkEmptyMergingTree emt
    mt' <- newPendingLevelMerge [] (Just mt)
    releaseRef mt
    return mt'
mkEmptyMergingTree (NonObviouslyEmptyUnionMerge emts) = do
    mts <- mapM mkEmptyMergingTree emts
    mt' <- newPendingUnionMerge mts
    mapM_ releaseRef mts
    return mt'

