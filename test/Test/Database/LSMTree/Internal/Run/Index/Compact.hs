{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}

module Test.Database.LSMTree.Internal.Run.Index.Compact (tests) where

import           Data.List (nub, sort)
import           Data.Word (Word64)
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

{-------------------------------------------------------------------------------
  Range-finder precision
-------------------------------------------------------------------------------}

newtype RFPrecision = RFPrecision Int
  deriving Show

instance Arbitrary RFPrecision where
  arbitrary = RFPrecision <$>
      (arbitrary `suchThat` (\x -> x >= rfprecLB && x <= rfprecUB))
  shrink (RFPrecision x) = [RFPrecision x' | x' <- shrink x
                                           , x' >= rfprecLB && x' <= rfprecUB]

rfprecLB, rfprecUB :: Int
(rfprecLB, rfprecUB) = rangeFinderPrecisionBounds

{-------------------------------------------------------------------------------
  Pages (non-partitioned)
-------------------------------------------------------------------------------}

-- | We model a disk page in a run as a pair of its minimum and maximum key. A
-- run consists of multiple pages in sorted order, and keys are unique.
newtype Pages k = Pages { getPages :: [(k, k)]}
  deriving Show

instance (Arbitrary k, Ord k) => Arbitrary (Pages k) where
  arbitrary = mkPages <$> scale (2*) (arbitrary @[k])
  shrink (Pages ks) = [Pages ks' | ks' <- shrink ks, pagesInvariant (Pages ks')]

mkPages :: Ord k => [k] -> Pages k
mkPages ks0 = Pages $ go $ nub $ sort ks0
  where
    go :: [k] -> [(k, k)]
    go []             = []
    go [_]            = []
    go (k1 : k2 : ks) = (k1, k2) : go ks

pagesInvariant ::  Ord k => Pages k -> Bool
pagesInvariant (Pages ks) = nub ks' == ks' && sort ks' == ks'
  where ks' = flatten ks

flatten :: [(k, k)] -> [k]
flatten []              = []
flatten ((k1, k2) : ks) = k1 : k2 : flatten ks

{-------------------------------------------------------------------------------
  Pages (partitioned)
-------------------------------------------------------------------------------}

-- | In partitioned pages, all keys inside a page have the same range-finder
-- bits.
data PartitionedPages k = PartitionedPages {
    getRangeFinderPrecision :: RFPrecision
  , getPartitionedPages     :: [(k, k)]
  }
  deriving Show

instance (Arbitrary k, SliceBits k, Integral k) => Arbitrary (PartitionedPages k) where
  arbitrary = mkPartitionedPages <$> arbitrary <*> arbitrary
  shrink (PartitionedPages rfprec ks) = [
        PartitionedPages rfprec ks'
      | ks' <- shrink ks
      , partitionedPagesInvariant (PartitionedPages rfprec ks')
      ] <> [
        PartitionedPages rfprec' ks
      | rfprec' <- shrink rfprec
      , partitionedPagesInvariant (PartitionedPages rfprec' ks)
      ] <> [
        PartitionedPages rfprec' ks'
      | ks' <- shrink ks
      , rfprec' <- shrink rfprec
      , partitionedPagesInvariant (PartitionedPages rfprec' ks')
      ]

mkPartitionedPages ::
     (SliceBits k, Integral k)
  => RFPrecision
  -> Pages k
  -> PartitionedPages k
mkPartitionedPages rfprec (Pages ks) = PartitionedPages rfprec $ filter f ks
  where f (kmin, kmax) = topBits16 rfprecUB kmin == topBits16 rfprecUB kmax

partitionedPagesInvariant :: (SliceBits k, Integral k) => PartitionedPages k -> Bool
partitionedPagesInvariant (PartitionedPages (RFPrecision rfprec) ks) =
    pagesInvariant (Pages ks) && properPartitioning
  where
    properPartitioning = all p ks
    p (kmin, kmax) = topBits16 rfprec kmin == topBits16 rfprec kmax
