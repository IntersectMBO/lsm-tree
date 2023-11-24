{-# LANGUAGE DeriveAnyClass             #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE DerivingVia                #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE StandaloneDeriving         #-}
{-# OPTIONS_GHC -Wno-orphans #-}

module Database.LSMTree.Generators (
    -- * UTxO keys
    UTxOKey (..)
    -- * Range-finder precision
  , RFPrecision (..)
  , rfprecInvariant
    -- * Pages (non-partitioned)
  , Pages (..)
  , mkPages
  , pagesInvariant
  , labelPages
    -- * Chunking size
  , ChunkSize (..)
  , chunkSizeInvariant
  ) where

import           Control.DeepSeq (NFData)
import           Data.Containers.ListUtils (nubOrd)
import           Data.List (sort)
import           Data.List.NonEmpty (NonEmpty)
import qualified Data.List.NonEmpty as NonEmpty
import           Data.WideWord.Word256 (Word256 (..))
import           Database.LSMTree.Internal.Run.BloomFilter (Hashable (..))
import           Database.LSMTree.Internal.Run.Index.Compact (FiniteB (..),
                     SliceBits, rangeFinderPrecisionBounds,
                     suggestRangeFinderPrecision, topBits16)
import           GHC.Generics (Generic)
import           System.Random (Uniform)
import           Test.QuickCheck (Arbitrary (..), NonEmptyList (..), Property,
                     chooseInt, scale, tabulate)

{-------------------------------------------------------------------------------
  UTxO keys
-------------------------------------------------------------------------------}

-- | A model of a UTxO key (256-bit hash)
newtype UTxOKey = UTxOKey Word256
  deriving stock (Show, Generic)
  deriving newtype ( Eq, Ord, NFData, SliceBits, Num, Real, Enum, Integral
                   , Hashable
                   )
  deriving anyclass (Uniform)

instance Arbitrary UTxOKey where
  arbitrary = UTxOKey <$>
      (Word256 <$> arbitrary <*> arbitrary <*> arbitrary <*> arbitrary)
  shrink (UTxOKey w256) = [
        UTxOKey w256'
      | let i256 = toInteger w256
      , i256' <- shrink i256
      , toInteger (minBound :: Word256) <= i256'
      , toInteger (maxBound :: Word256) >= i256'
      , let w256' = fromIntegral i256'
      ]

deriving anyclass instance Uniform Word256
deriving via FiniteB Word256 instance SliceBits Word256

instance Hashable Word256 where
  hashIO32 (Word256 a b c d) = hashIO32 (a, b, c, d)

{-------------------------------------------------------------------------------
  Range-finder precision
-------------------------------------------------------------------------------}

newtype RFPrecision = RFPrecision Int
  deriving stock (Show, Generic)
  deriving newtype Num
  deriving anyclass NFData

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
data Pages k = Pages {
    getRangeFinderPrecision :: RFPrecision
  , getPages                :: [(k, k)]
  }
  deriving stock (Show, Generic)
  deriving anyclass NFData

instance (Arbitrary k, SliceBits k, Integral k) => Arbitrary (Pages k) where
  arbitrary = mkPages
      <$> arbitrary
      <*> (NonEmpty.fromList . getNonEmpty <$> scale (2*) arbitrary)
  shrink (Pages rfprec ks) = [
        Pages rfprec ks'
      | ks' <- shrink ks
      , pagesInvariant (Pages rfprec ks')
      ] <> [
        Pages rfprec' ks
      | rfprec' <- shrink rfprec
      , pagesInvariant (Pages rfprec' ks)
      ] <> [
        Pages rfprec' ks'
      | ks' <- shrink ks
      , rfprec' <- shrink rfprec
      , pagesInvariant (Pages rfprec' ks')
      ]

mkPages ::
     forall k. (SliceBits k, Integral k)
  => RFPrecision
  -> NonEmpty k
  -> Pages k
mkPages rfprec@(RFPrecision n) =
    Pages rfprec . go . nubOrd . sort . NonEmpty.toList
  where
    go :: [k] -> [(k, k)]
    go []              = []
    go [k]             = [(k, k)]
    go  (k1 : k2 : ks) | topBits16 n k1 == topBits16 n k2
                       = (k1, k2) : go ks
                       | otherwise
                       = (k1, k1) : go (k2 : ks)

pagesInvariant :: (SliceBits k, Integral k) => Pages k -> Bool
pagesInvariant (Pages (RFPrecision rfprec) ks) =
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

labelPages :: Pages k -> (Property -> Property)
labelPages (Pages (RFPrecision rfprec) ks) =
      tabulate "Optimal range-finder bit-precision"
        [show suggestedRfprec]
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
