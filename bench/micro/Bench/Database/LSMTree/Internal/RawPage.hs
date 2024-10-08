{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE TypeApplications   #-}

{-# OPTIONS_GHC -Wno-incomplete-uni-patterns #-}

module Bench.Database.LSMTree.Internal.RawPage (
    benchmarks
    -- * Benchmarked functions
  ) where

import           Control.DeepSeq (deepseq)
import qualified Data.ByteString as BS

import           Database.LSMTree.Extras.ReferenceImpl
import qualified Database.LSMTree.Internal.RawBytes as RB
import           Database.LSMTree.Internal.RawPage
import           Database.LSMTree.Internal.Serialise

import           Criterion.Main
import           Test.QuickCheck
import           Test.QuickCheck.Gen (Gen (..))
import           Test.QuickCheck.Random (mkQCGen)

benchmarks :: Benchmark
benchmarks = rawpage `deepseq` bgroup "Bench.Database.LSMTree.Internal.RawPage"
    [ bRawPageFindKey
    , bRawPageLookup
    ]
  where
    bRawPageFindKey = bgroup "rawPageFindKey"
      [ bench "missing"       $ whnf (rawPageFindKey rawpage) missing
      , bench "existing-head" $ whnf (rawPageFindKey rawpage) existingHead
      , bench "existing-last" $ whnf (rawPageFindKey rawpage) existingLast
      ]

    bRawPageLookup = bgroup "rawPageLookup"
      [ bench "missing"       $ whnf (rawPageLookup rawpage) missing
      , bench "existing-head" $ whnf (rawPageLookup rawpage) existingHead
      , bench "existing-last" $ whnf (rawPageLookup rawpage) existingLast
      ]

    kops :: [(Key, Operation)]
    kops = unGen genPage (mkQCGen 42) 200
      where
        genPage = orderdKeyOps <$>
                    genPageContentNearFull DiskPage4k genSmallKey genSmallValue

    rawpage :: RawPage
    rawpage = fst $ toRawPage (PageContentFits kops)

    genSmallKey :: Gen Key
    genSmallKey = Key . BS.pack <$> vectorOf 8 arbitrary

    genSmallValue :: Gen Value
    genSmallValue = Value . BS.pack <$> vectorOf 8 arbitrary

    missing :: SerialisedKey
    missing = SerialisedKey $ RB.pack [1, 2, 3]

    keys :: [Key]
    keys = map fst kops

    existingHead :: SerialisedKey
    existingHead = SerialisedKey $ RB.fromByteString $ unKey $ head keys

    existingLast :: SerialisedKey
    existingLast = SerialisedKey $ RB.fromByteString $ unKey $ last keys

