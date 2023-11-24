{-# LANGUAGE DeriveAnyClass     #-}
{-# LANGUAGE DeriveGeneric      #-}
{-# LANGUAGE FlexibleInstances  #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE StandaloneDeriving #-}
{-# OPTIONS_GHC -Wno-orphans #-}
{- HLINT ignore "Eta reduce" -}

module Bench.Database.LSMTree.Internal.Run.Index.Compact (benchmarks) where

import           Control.DeepSeq
import           Criterion.Main
import qualified Data.Array.Unboxed as A
import           Data.Foldable (Foldable (..))
import qualified Data.List.NonEmpty as NonEmpty
import           Data.Maybe (fromMaybe)
import           Database.LSMTree.Generators
import           Database.LSMTree.Internal.Run.Index.Compact
import           GHC.Generics
import           System.Random
import           System.Random.Extras

-- See 'utxoNumPages'.
benchmarks :: Benchmark
benchmarks = bgroup "Bench.Database.LSMTree.Internal.Run.Index.Compact" [
      bgroup "searches" [
          env (searchEnv 0  2_500_000 1_000_000) $ \ ~(ci, ks) ->
            bench "searches with 0-bit  rfprec" $ whnf (searches ci) ks
        , env (searchEnv 16 2_500_000 1_000_000) $ \ ~(ci, ks) ->
            bench "searches with 16-bit rfprec" $ whnf (searches ci) ks
        ]
    , bgroup "construction" [
          env (constructionEnv 0  2_500_000) $ \ pages ->
            bench "construction with 0-bit  rfprec" $ whnf constructCompactIndex pages
        , env (constructionEnv 16 2_500_000) $ \ pages ->
            bench "construction with 16-bit rfprec" $ whnf constructCompactIndex pages
        ]
    ]

-- | Input environment for benchmarking 'searches'.
searchEnv ::
     RFPrecision -- ^ Range-finder bit-precision
  -> Int         -- ^ Number of pages
  -> Int         -- ^ Number of searches
  -> IO (CompactIndex UTxOKey, [UTxOKey])
searchEnv fpr npages nsearches = do
    ci <- constructCompactIndex <$> constructionEnv fpr npages
    stdgen  <- newStdGen
    let ks = uniformWithReplacement stdgen nsearches
    pure (ci, ks)

-- | Used for benchmarking 'search'.
searches ::
     (SliceBits k, Integral k)
  => CompactIndex k
  -> [k]            -- ^ Keys to search for
  -> ()
searches ci ks = foldl' (\acc k -> f (search k ci) `seq` acc) () ks
  where f = fromMaybe (-1)

-- | Input environment for benchmarking 'constructCompactIndex'.
constructionEnv ::
     RFPrecision -- ^ Range-finder bit-precision
  -> Int         -- ^ Number of pages
  -> IO (Pages UTxOKey)
constructionEnv rfprec n = do
    stdgen <- newStdGen
    let ks = uniformWithoutReplacement stdgen (2 * n)
    pure $ mkPages rfprec (NonEmpty.fromList ks)

-- | Used for benchmarking the incremental construction of a 'CompactIndex'.
constructCompactIndex ::
     (SliceBits k, Integral k)
  => Pages k -- ^ Pages to add in succession
  -> CompactIndex k
constructCompactIndex (Pages (RFPrecision rfprec) ks) =
    -- under the hood, 'fromList' uses the incremental construction interface
    fromList rfprec ks

deriving instance Generic (CompactIndex k)
deriving instance NFData k => NFData (CompactIndex k)
instance NFData (A.UArray Int Bool) where
  rnf x = rnf (A.bounds x, A.elems x)
