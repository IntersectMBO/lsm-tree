{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE TypeApplications   #-}

{-# OPTIONS_GHC -Wno-incomplete-uni-patterns #-}

module Bench.Database.LSMTree.Internal.RawPage (
    benchmarks
    -- * Benchmarked functions
  ) where

import           Control.DeepSeq (deepseq)
import           Criterion.Main
import qualified Data.ByteString as BS
import qualified Data.ByteString.Short as SBS
import           Data.Primitive.ByteArray (ByteArray (..))
import qualified Database.LSMTree.Internal.RawBytes as RB
import           Database.LSMTree.Internal.RawPage
import           Database.LSMTree.Internal.Serialise
import           FormatPage
import           Test.QuickCheck
import           Test.QuickCheck.Gen (Gen (..))
import           Test.QuickCheck.Random (mkQCGen)

benchmarks :: Benchmark
benchmarks = rawpage `deepseq` bgroup "Bench.Database.LSMTree.Internal.RawPage"
    [ bench "missing" $ whnf (rawPageLookup rawpage) missing
    , bench "existing-head" $ whnf (rawPageLookup rawpage) existingHead
    , bench "existing-last" $ whnf (rawPageLookup rawpage) existingLast
    ]
  where
    page :: PageLogical
    page = unGen (genFullPageLogical DiskPage4k genSmallKey genSmallValue)
                 (mkQCGen 42) 200

    rawpage :: RawPage
    rawpage = fst $ toRawPage $ page

    genSmallKey :: Gen Key
    genSmallKey = Key . BS.pack <$> vectorOf 8 arbitrary

    genSmallValue :: Gen Value
    genSmallValue = Value . BS.pack <$> vectorOf 8 arbitrary

    missing :: SerialisedKey
    missing = SerialisedKey $ RB.pack [1, 2, 3]

    keys :: [Key]
    keys = case page of PageLogical xs -> map fst xs

    existingHead :: SerialisedKey
    existingHead = SerialisedKey $ RB.fromByteString $ unKey $ head keys

    existingLast :: SerialisedKey
    existingLast = SerialisedKey $ RB.fromByteString $ unKey $ last keys

toRawPage :: PageLogical -> (RawPage, BS.ByteString)
toRawPage (PageLogical kops) = (page, sfx)
  where
    Just bs = serialisePage <$> encodePage DiskPage4k kops
    (pfx, sfx) = BS.splitAt 4096 bs -- hardcoded page size.
    page = case SBS.toShort pfx of SBS.SBS ba -> makeRawPage (ByteArray ba) 0
