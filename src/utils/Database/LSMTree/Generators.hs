{-# LANGUAGE DeriveAnyClass             #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE DerivingStrategies         #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE TypeApplications           #-}

module Database.LSMTree.Generators (
    -- * Range-finder precision
    RFPrecision (..)
  , rfprecInvariant
    -- * Pages (non-partitioned)
  , Pages (..)
  , mkPages
  , pagesInvariant
    -- * Pages (partitioned)
  , PartitionedPages (..)
  , mkPartitionedPages
  , partitionedPagesInvariant
  ) where

import           Control.DeepSeq (NFData)
import           Data.List (sort)
import           Database.LSMTree.Internal.Run.Index.Compact (SliceBits,
                     rangeFinderPrecisionBounds, topBits16)
import           GHC.Generics (Generic)

import           Data.Containers.ListUtils
import           Test.QuickCheck (Arbitrary (..), scale, suchThat)

{-------------------------------------------------------------------------------
  Range-finder precision
-------------------------------------------------------------------------------}

newtype RFPrecision = RFPrecision Int
  deriving stock (Show, Eq, Ord)
  deriving newtype (NFData, Num)

instance Arbitrary RFPrecision where
  arbitrary = (RFPrecision <$> arbitrary) `suchThat` rfprecInvariant
  shrink (RFPrecision x) = [RFPrecision x' | x' <- shrink x
                                           , rfprecInvariant (RFPrecision x')
                                           ]

rfprecInvariant :: RFPrecision -> Bool
rfprecInvariant (RFPrecision x) =
       x >= fst rangeFinderPrecisionBounds
    && x <= snd rangeFinderPrecisionBounds

{-------------------------------------------------------------------------------
  Pages (non-partitioned)
-------------------------------------------------------------------------------}

-- | We model a disk page in a run as a pair of its minimum and maximum key. A
-- run consists of multiple pages in sorted order, and keys are unique.
newtype Pages k = Pages { getPages :: [(k, k)]}
  deriving stock Show
  deriving newtype NFData

instance (Arbitrary k, Ord k) => Arbitrary (Pages k) where
  arbitrary = mkPages <$> scale (2*) (arbitrary @[k])
  shrink (Pages ks) = [Pages ks' | ks' <- shrink ks, pagesInvariant (Pages ks')]

mkPages :: Ord k => [k] -> Pages k
mkPages ks = Pages $ inPairs $ nubOrd $ sort ks

inPairs :: [k] -> [(k, k)]
inPairs []             = []
inPairs [_]            = []
inPairs (k1 : k2 : ks) = (k1, k2) : inPairs ks

pagesInvariant ::  Ord k => Pages k -> Bool
pagesInvariant (Pages ks) = nubOrd ks' == ks' && sort ks' == ks'
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
  deriving stock (Show, Generic)
  deriving anyclass NFData

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
mkPartitionedPages rfprec@(RFPrecision n) (Pages ks) =
    PartitionedPages rfprec $ filter f ks
  where
    f (kmin, kmax) = topBits16 n kmin == topBits16 n kmax

partitionedPagesInvariant :: (SliceBits k, Integral k) => PartitionedPages k -> Bool
partitionedPagesInvariant (PartitionedPages (RFPrecision rfprec) ks) =
    pagesInvariant (Pages ks) && properPartitioning
  where
    properPartitioning = all p ks
    p (kmin, kmax) = topBits16 rfprec kmin == topBits16 rfprec kmax
