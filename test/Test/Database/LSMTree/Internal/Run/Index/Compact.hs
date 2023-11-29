{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving  #-}
{-# LANGUAGE TypeApplications    #-}

{-# OPTIONS_GHC -Wno-orphans #-}

module Test.Database.LSMTree.Internal.Run.Index.Compact (tests) where

import           Control.Monad (forM)
import           Control.Monad.ST.Strict (runST)
import           Data.Containers.ListUtils (nubOrd)
import           Data.List (sort)
import           Data.List.NonEmpty (NonEmpty)
import qualified Data.List.NonEmpty as NonEmpty
import           Data.Word (Word64)
import           Database.LSMTree.Internal.Run.Index.Compact
import           Test.QuickCheck
import           Test.Tasty (TestTree, adjustOption, testGroup)
import           Test.Tasty.QuickCheck (QuickCheckMaxSize (..), testProperty)

tests :: TestTree
tests = testGroup "Test.Database.LSMTree.Internal.Run.Index.Compact" [
      testGroup "Range-finder bit-precision" [
          testProperty "Arbitrary satisfies invariant" $
            property . rfprecInvariant
        , testProperty "Shrinking satisfies invariant" $
            property . all rfprecInvariant . shrink @RFPrecision
      ]
    , testGroup "Pages (partitioned)" [
          testProperty "Arbitrary satisfies invariant" $
            property . partitionedPagesInvariant @Word64
        , testProperty "Shrinking satisfies invariant" $
            property . all partitionedPagesInvariant . shrink @(PartitionedPages Word64)
        ]
    , testGroup "Chunk size" [
        testProperty "Arbitrary satisfies invariant" $
            property . chunkSizeInvariant
        , testProperty "Shrinking satisfies invariant" $
            property . all chunkSizeInvariant . shrink @ChunkSize
        ]
      -- Increasing the maximum size has the effect of generating more
      -- interesting numbers of partitioned pages. With a max size of 100, the
      -- tests are very likely to generate only 1 partitioned page.
    , adjustOption (const $ QuickCheckMaxSize 5000) $
      testGroup "Contruction, searching, chunking" [
        testProperty "prop_searchMinMaxKeysAfterConstruction" $
        prop_searchMinMaxKeysAfterConstruction @Word64
      , testProperty "prop_chunksPreserveConstruction" $
          prop_chunksPreserveConstruction @Word64
      ]
    ]

{-------------------------------------------------------------------------------
  Properties
-------------------------------------------------------------------------------}

-- | After construction, searching for the minimum/maximum key of every page
-- @pageNr@ returns the @pageNr@.
--
-- Example: @search minKey (fromList rfprec [(minKey, maxKey)]) == 0@.
prop_searchMinMaxKeysAfterConstruction ::
     (SliceBits k, Integral k)
  => PartitionedPages k
  -> Property
prop_searchMinMaxKeysAfterConstruction pps@(PartitionedPages (RFPrecision rfprec) ks) =
      classify (hasClashes ci) "Compact index contains clashes"
    $ labelPartitionedPages pps
    $ counterexample (show idxs)
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

prop_chunksPreserveConstruction ::
     (SliceBits k, Integral k, Show k)
  => ChunkSize
  -> PartitionedPages k
  -> Property
prop_chunksPreserveConstruction
  (ChunkSize csize)
  pps@(PartitionedPages (RFPrecision rfprec) ks)  =
      labelPartitionedPages pps
    $ ci === ci'
  where
    mci          = fromList' rfprec ks
    ci           = runST $ unsafeFreeze =<< mci
    nks          = length ks
    (nchunks, r) = nks `divMod` csize
    slices       = [(i * csize, csize) | i <- [0..nchunks-1]] ++ [(nchunks * csize, r)]
    ci'          = runST $ do
      mci0 <- mci
      cs <- forM slices $ \(i, n) ->
        sliceChunk i n mci0
      fc <- getFinalChunk mci0
      pure $ fromChunks cs fc

deriving instance Eq k => Eq (CompactIndex k)
deriving instance Show k => Show (CompactIndex k)

{-------------------------------------------------------------------------------
  Range-finder precision
-------------------------------------------------------------------------------}

newtype RFPrecision = RFPrecision Int
  deriving Show

instance Arbitrary RFPrecision where
  arbitrary = RFPrecision <$> chooseInt (rfprecLB, rfprecUB)
    where (rfprecLB, rfprecUB) = rangeFinderPrecisionBounds
  shrink (RFPrecision x) = [RFPrecision x' | x' <- shrink x
                                           , rfprecInvariant (RFPrecision x')
                                           ]

rfprecInvariant :: RFPrecision -> Bool
rfprecInvariant (RFPrecision x) = x >= rfprecLB && x <= rfprecUB
  where (rfprecLB, rfprecUB) = rangeFinderPrecisionBounds

{-------------------------------------------------------------------------------
  Pages (partitioned)
-------------------------------------------------------------------------------}

-- | We model a disk page in a run as a pair of its minimum and maximum key.
--
-- A run consists of multiple pages in sorted order, and keys are unique. Pages
-- are partitioned, meaning all keys inside a page have the same range-finder
-- bits. A run can not be empty, and a page can not be empty.
data PartitionedPages k = PartitionedPages {
    getRangeFinderPrecision :: RFPrecision
  , getPartitionedPages     :: [(k, k)]
  }
  deriving Show

instance (Arbitrary k, SliceBits k, Integral k) => Arbitrary (PartitionedPages k) where
  arbitrary = mkPartitionedPages
      <$> arbitrary
      <*> (NonEmpty.fromList . getNonEmpty <$> scale (2*) arbitrary)
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
     forall k. (SliceBits k, Integral k)
  => RFPrecision
  -> NonEmpty k
  -> PartitionedPages k
mkPartitionedPages rfprec@(RFPrecision n) =
    PartitionedPages rfprec . go . nubOrd . sort . NonEmpty.toList
  where
    go :: [k] -> [(k, k)]
    go []              = []
    go [k]             = [(k, k)]
    go  (k1 : k2 : ks) | topBits16 n k1 == topBits16 n k2
                       = (k1, k2) : go ks
                       | otherwise
                       = (k1, k1) : go (k2 : ks)

partitionedPagesInvariant :: (SliceBits k, Integral k) => PartitionedPages k -> Bool
partitionedPagesInvariant (PartitionedPages (RFPrecision rfprec) ks) =
       sort ks'   == ks'
    && nubOrd ks' == ks'
    && not (null ks)
    && all partitioned ks
  where
    ks' = flatten ks
    partitioned (kmin, kmax) = topBits16 rfprec kmin == topBits16 rfprec kmax

    flatten :: Eq k => [(k, k)] -> [k]
    flatten []               = []
                             -- the min and max key are allowed to be the same
    flatten ((k1, k2) : ks0) | k1 == k2  = k1 : flatten ks0
                             | otherwise = k1 : k2 : flatten ks0

labelPartitionedPages :: PartitionedPages k -> (Property -> Property)
labelPartitionedPages (PartitionedPages (RFPrecision rfprec) ks) =
      tabulate "Optimal range-finder bit-precision"
        [show (suggestRangeFinderPrecision npages)]
    . tabulate "Range-finder bit-precision" [show rfprec]
    . tabulate "Distance between optimal and actual rfprec" [show dist]
    . tabulate "Number of pages" [show npages]
  where
    npages = length ks
    suggestedRfprec = suggestRangeFinderPrecision npages
    dist = suggestedRfprec - rfprec

{-------------------------------------------------------------------------------
  Chunking size
-------------------------------------------------------------------------------}

newtype ChunkSize = ChunkSize Int
  deriving Show

instance Arbitrary ChunkSize where
  arbitrary = ChunkSize <$> chooseInt (1, 10)
  shrink (ChunkSize csize) = [ChunkSize csize' | csize' <- shrink csize
                                               , chunkSizeInvariant (ChunkSize csize')
                                               ]

chunkSizeLB, chunkSizeUB :: Int
chunkSizeLB = 1
chunkSizeUB = 20

chunkSizeInvariant :: ChunkSize -> Bool
chunkSizeInvariant (ChunkSize csize) = chunkSizeLB <= csize && csize <= chunkSizeUB
