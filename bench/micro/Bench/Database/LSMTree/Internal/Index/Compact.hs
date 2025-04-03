{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE TypeApplications   #-}

module Bench.Database.LSMTree.Internal.Index.Compact (
    benchmarks
    -- * Benchmarked functions
  , searches
  , constructIndexCompact
  ) where

import           Control.DeepSeq (deepseq)
import           Control.Exception (assert)
import           Control.Monad.ST.Strict
import           Criterion.Main
import qualified Data.Foldable as Fold
import qualified Data.List as List
import           Data.List.NonEmpty (NonEmpty (..))
import qualified Data.List.NonEmpty as NE
import qualified Data.Vector.Unboxed.Mutable as VUM
import           Data.Word
import           Database.LSMTree.Extras
import           Database.LSMTree.Extras.Generators
import           Database.LSMTree.Extras.Index
import           Database.LSMTree.Extras.Random
import           Database.LSMTree.Extras.UTxO
import           Database.LSMTree.Internal.Index.Compact
import           Database.LSMTree.Internal.Index.CompactAcc
import           Database.LSMTree.Internal.Map.Range
import           Database.LSMTree.Internal.Serialise (SerialisedKey,
                     serialiseKey)
import           System.Random
import           Test.QuickCheck (generate)

benchmarks :: Benchmark
benchmarks = bgroup "Bench.Database.LSMTree.Internal.Index.Compact" [
      env (searchEnv 10000 1000) $ \ ~(ic, ks) ->
        bench "searches-10-1k" $ whnf (searches ic) ks
    , bgroup "construction" [
          env (constructionEnv 1000) $ \ pages ->
            bench "construction-1k-100" $ whnf (constructIndexCompact 100) pages
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
     Int         -- ^ Number of pages
  -> Int         -- ^ Number of searches
  -> IO (IndexCompact, [SerialisedKey])
searchEnv npages nsearches = do
    ic <- constructIndexCompact 100 <$> constructionEnv npages
    let stdgen = mkStdGen 17
    let ks = serialiseKey <$> uniformWithReplacement @UTxOKey stdgen nsearches
    pure (ic, ks)

-- | Used for benchmarking 'search'.
searches ::
     IndexCompact
  -> [SerialisedKey]            -- ^ Keys to search for
  -> ()
searches ic ks = Fold.foldl' (\acc k -> search k ic `deepseq` acc) () ks

-- | Input environment for benchmarking 'constructIndexCompact'.
constructionEnv ::
     Int         -- ^ Number of pages
  -> IO [Append]
constructionEnv n = do
    let stdgen = mkStdGen 17
    let ks = uniformWithoutReplacement @UTxOKey stdgen (2 * n)
    ps <- generate (mkPages 0 (error "unused in constructionEnv") 0 ks)
    pure (toAppends ps)

-- | Used for benchmarking the incremental construction of a 'IndexCompact'.
constructIndexCompact ::
     ChunkSize
  -> [Append] -- ^ Pages to add in succession
  -> IndexCompact
constructIndexCompact (ChunkSize csize) apps = runST $ do
    ica <- new csize
    mapM_ (`appendToCompact` ica) apps
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
        env (pure $ (appsWithNearDups (mkStdGen 17) 1000)) $ \as ->
          bench ("construct appsWithNearDups") $ whnf (constructIndexCompact 1000) as
      , env (pure $ (appsWithoutNearDups (mkStdGen 17) 1000)) $ \as ->
          bench ("construct appsWithoutNearDups") $ whnf (constructIndexCompact 1000) as
        -- search
      , env ( let ic = constructIndexCompact 100 (appsWithNearDups (mkStdGen 17) 1000)
                  g  = mkStdGen 42
                  ks = serialiseKey <$> uniformWithReplacement @UTxOKey g 1000
              in  pure (ic, ks) ) $ \ ~(ic, ks) ->
          bench "search appsWithNearDups" $ whnf (searches ic) ks
      , env ( let ic = constructIndexCompact 100 (appsWithoutNearDups (mkStdGen 17) 1000)
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
appsWithoutNearDups g n =
    let ks  = uniformWithoutReplacement @UTxOKey g (n * 2)
        ks' = List.sort ks
        -- append a dummy UTXO key because appsWithNearDups does so too.
        ps  = groupsOfN 2 (UTxOKey 0 0 : ks')
    in  assert (length ps == n + 1) $
        fmap fromNE ps

-- | 'Append's with worst-case near-duplicates. Each page boundary splits UTXO
-- keys with the same 'txId' but different a 'txIx'.
appsWithNearDups ::
     StdGen
  -> Int -- ^ Number of pages
  -> [Append]
appsWithNearDups g n =
    let ks  = uniformWithoutReplacement @UTxOKey g n
        ks' = flip concatMap (List.sort ks) $ \k -> [k {txIx = 0}, k {txIx = 1}]
        -- append a dummy UTXO key so that each pair of near-duplicate keys is
        -- split between pages. That is, the left element of the pair is the
        -- maximum key in a page, and the right element of the pair is the
        -- minimum key on the next page.
        ps  = groupsOfN 2 (UTxOKey 0 0 : ks')
    in  -- check that the number of pages
        assert (length ps == n + 1) $
        fmap fromNE ps

fromNE :: NonEmpty UTxOKey -> Append
fromNE xs =
    assert (NE.sort xs == xs) $
    assert (NE.nub xs == xs) $
    AppendSinglePage (serialiseKey $ NE.head xs) (serialiseKey $ NE.last xs)
