{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE TypeApplications   #-}
{- HLINT ignore "Eta reduce" -}

module Bench.Database.LSMTree.Internal.IndexCompact (
    benchmarks
    -- * Benchmarked functions
  , searches
  , constructIndexCompact
  ) where

import           Control.DeepSeq (deepseq)
import           Control.Exception (assert)
import           Control.Monad.ST.Strict
import           Criterion.Main
import           Data.Foldable (Foldable (..))
import qualified Data.List as List
import           Data.List.NonEmpty (NonEmpty (..))
import qualified Data.List.NonEmpty as NE
import           Data.Map.Range
import qualified Data.Vector.Unboxed.Mutable as VUM
import           Data.Word
import           Database.LSMTree.Extras.Generators
import           Database.LSMTree.Extras.Random
import           Database.LSMTree.Extras.UTxO
import           Database.LSMTree.Internal.IndexCompact
import           Database.LSMTree.Internal.IndexCompactAcc
import           Database.LSMTree.Internal.Serialise (SerialisedKey,
                     serialiseKey)
import           System.Random
import           Test.QuickCheck (generate)

-- See 'utxoNumPages'.
benchmarks :: Benchmark
benchmarks = bgroup "Bench.Database.LSMTree.Internal.IndexCompact" [
      bgroup "searches" [
          env (searchEnv 0  10000 1000) $ \ ~(ic, ks) ->
            bench "searches with 0-bit  rfprec" $ whnf (searches ic) ks
        , env (searchEnv 16 10000 1000) $ \ ~(ic, ks) ->
            bench "searches with 16-bit rfprec" $ whnf (searches ic) ks
        ]
    , bgroup "construction" [
          env (constructionEnv 0  1000) $ \ pages ->
            bench "construction with 0-bit  rfprec and chunk size 100" $ whnf (constructIndexCompact 100) pages
        , env (constructionEnv 16 1000) $ \ pages ->
            bench "construction with 16-bit rfprec and chunk size 100" $ whnf (constructIndexCompact 100) pages
        , env (VUM.replicate 3000 (7 :: Word32)) $ \ mv ->
            bench "unsafeWriteRange-1k" $
              whnfAppIO (\x -> stToIO (unsafeWriteRange mv (BoundInclusive 1000) (BoundInclusive 2000) x)) 17
        , env (VUM.replicate 30000 (7 :: Word32)) $ \ mv ->
            bench "unsafeWriteRange-10k" $
              whnfAppIO (\x -> stToIO (unsafeWriteRange mv (BoundInclusive 10000) (BoundInclusive 20000) x)) 17
        ]
    , benchNonUniform
    ]

-- | Input environment for benchmarking 'searches'.
searchEnv ::
     RFPrecision -- ^ Range-finder bit-precision
  -> Int         -- ^ Number of pages
  -> Int         -- ^ Number of searches
  -> IO (IndexCompact, [SerialisedKey])
searchEnv rfprec npages nsearches = do
    ic <- constructIndexCompact 100 <$> constructionEnv rfprec npages
    let stdgen = mkStdGen 17
    let ks = serialiseKey <$> uniformWithReplacement @UTxOKey stdgen nsearches
    pure (ic, ks)

-- | Used for benchmarking 'search'.
searches ::
     IndexCompact
  -> [SerialisedKey]            -- ^ Keys to search for
  -> ()
searches ic ks = foldl' (\acc k -> search k ic `deepseq` acc) () ks

-- | Input environment for benchmarking 'constructIndexCompact'.
constructionEnv ::
     RFPrecision -- ^ Range-finder bit-precision
  -> Int         -- ^ Number of pages
  -> IO (RFPrecision, [Append])
constructionEnv rfprec n = do
    let stdgen = mkStdGen 17
    let ks = uniformWithoutReplacement @UTxOKey stdgen (2 * n)
    ps <- generate (mkPages 0 (error "unused in constructionEnv") rfprec ks)
    pure (rfprec, toAppends ps)

-- | Used for benchmarking the incremental construction of a 'IndexCompact'.
constructIndexCompact ::
     ChunkSize
  -> (RFPrecision, [Append]) -- ^ Pages to add in succession
  -> IndexCompact
constructIndexCompact (ChunkSize csize) (RFPrecision rfprec, apps) = runST $ do
    ica <- new rfprec csize
    mapM_ (`append` ica) apps
    (_, index) <- unsafeEnd ica
    pure index

{-------------------------------------------------------------------------------
  Benchmarks for UTxO keys that are /almost/ uniformly distributed
-------------------------------------------------------------------------------}

-- | UTXO keys are not truly uniformly distrbuted. The 'txId' is a uniformly
-- distributed hash, but the same 'txId' can appear in multiple UTXO keys, but
-- with a different 'txIx'. In the worst case, this means that we have a clash
-- in the compact index for /every page/. The following benchmarks show
benchNonUniform :: Benchmark
benchNonUniform =
    bgroup "non-uniformity" [
        -- construction
        env (pure $ (0, appsWithNearDups (mkStdGen 17) 1000)) $ \as ->
          bench ("construct appsWithNearDups") $ whnf (constructIndexCompact 1000) as
      , env (pure $ (0, appsWithoutNearDups (mkStdGen 17) 1000)) $ \as ->
          bench ("construct appsWithoutNearDups") $ whnf (constructIndexCompact 1000) as
        -- search
      , env ( let ic = constructIndexCompact 100 (0, appsWithNearDups (mkStdGen 17) 1000)
                  g  = mkStdGen 42
                  ks = serialiseKey <$> uniformWithReplacement @UTxOKey g 1000
              in  pure (ic, ks) ) $ \ ~(ic, ks) ->
          bench "search appsWithNearDups" $ whnf (searches ic) ks
      , env ( let ic = constructIndexCompact 100 (0, appsWithoutNearDups (mkStdGen 17) 1000)
                  g  = mkStdGen 42
                  ks = serialiseKey <$> uniformWithReplacement @UTxOKey g 1000
              in  pure (ic, ks) ) $ \ ~(ic, ks) ->
          bench "search appsWithoutNearDups" $ whnf (searches ic) ks
      ]

-- | 'Append's with truly uniformly distributed UTXO keys.
appsWithoutNearDups ::
     StdGen
  -> Int -- ^ Number of pages
  -> [Append]
appsWithoutNearDups g n = assert (suggestRangeFinderPrecision n == 0) $
    let ks  = uniformWithoutReplacement @UTxOKey g n
        ks' = List.sort ks
        -- append a dummy UTXO key because appsWithNearDups does so too.
        ps  = groupsOfN 2 (UTxOKey 0 0 : ks')
    in  fmap fromNE ps

-- | 'Append's with worst-case near-duplicates. Each page boundary splits UTXO
-- keys with the same 'txId' but different a 'txIx'.
appsWithNearDups ::
     StdGen
  -> Int -- ^ Number of pages
  -> [Append]
appsWithNearDups g n = assert (suggestRangeFinderPrecision n == 0) $
    let ks  = uniformWithoutReplacement @UTxOKey g n
        ks' = flip concatMap (List.sort ks) $ \k -> [k {txIx = 0}, k {txIx = 1}]
        -- append a dummy UTXO key so that each pair of near-duplicate keys is
        -- split between pages. That is, the left element of the pair is the
        -- maximum key in a page, and the right element of the pair is the
        -- minimum key on the next page.
        ps  = groupsOfN 2 (UTxOKey 0 0 : ks')
    in  fmap fromNE ps

fromNE :: NonEmpty UTxOKey -> Append
fromNE xs =
    assert (NE.sort xs == xs) $
    assert (NE.nub xs == xs) $
    AppendSinglePage (serialiseKey $ NE.head xs) (serialiseKey $ NE.last xs)

-- | Make groups of @n@ elements from a list @xs@
groupsOfN :: Int -> [a] -> [NonEmpty a]
groupsOfN n
  | n < 0 = error "groupsOfN: n <= 0"
  | otherwise = List.unfoldr f
  where f xs = let (ys, zs) = List.splitAt n xs
               in  (,zs) <$> NE.nonEmpty ys
