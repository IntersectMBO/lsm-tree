{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE TypeApplications   #-}


module Bench.Database.LSMTree.Internal.RawPage (
    benchmarks
    -- * Benchmarked functions
  ) where

import           Control.DeepSeq (deepseq)
import           Criterion.Main
import qualified Data.ByteString as BS
import qualified Data.ByteString.Short as SBS
import           Data.Primitive.ByteArray (ByteArray (..))
import qualified Data.Vector.Primitive as P
import           Data.Word (Word8)
import           Database.LSMTree.Internal.RawPage
import           Database.LSMTree.Util.Orphans ()
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
    page = unGen (genFullPageLogical genSmallKey genSmallValue) (mkQCGen 42) 200

    rawpage :: RawPage
    rawpage = fst $ toRawPage $ page

    genSmallKey :: Gen Key
    genSmallKey = Key . BS.pack <$> vectorOf 8 arbitrary

    genSmallValue :: Gen Value
    genSmallValue = Value . BS.pack <$> vectorOf 8 arbitrary

    missing :: P.Vector Word8
    missing = P.fromList [1, 2, 3]

    keys :: [Key]
    keys = case page of
        PageLogical xs -> map (\(k,_,_) -> k) xs

    existingHead :: P.Vector Word8
    existingHead = bsToVector $ unKey $ head keys

    existingLast :: P.Vector Word8
    existingLast = bsToVector $ unKey $ last keys

toRawPage :: PageLogical -> (RawPage, BS.ByteString)
toRawPage p = (page, sfx)
  where
    bs = serialisePage $ encodePage p
    (pfx, sfx) = BS.splitAt 4096 bs -- hardcoded page size.
    page = case SBS.toShort pfx of SBS.SBS ba -> makeRawPage (ByteArray ba) 0

bsToVector :: BS.ByteString -> P.Vector Word8
bsToVector = P.fromList . BS.unpack
