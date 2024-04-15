{-# LANGUAGE DeriveAnyClass             #-}
{-# LANGUAGE DeriveFunctor              #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE DerivingStrategies         #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE NamedFieldPuns             #-}
{-# LANGUAGE NumericUnderscores         #-}
{-# LANGUAGE RecordWildCards            #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE StandaloneDeriving         #-}
{-# LANGUAGE TypeApplications           #-}
{-# OPTIONS_GHC -Wno-orphans #-}
{- HLINT ignore "Use const" -}

module Bench.Database.LSMTree.Internal.Lookup (benchmarks) where

import           Bench.Database.LSMTree.Internal.IndexCompact
                     (constructIndexCompact)
import           Control.DeepSeq (NFData)
import           Control.Monad
import           Criterion.Main (Benchmark, bench, bgroup, env, nfAppIO, whnf,
                     whnfAppIO)
import qualified Data.BloomFilter.Easy as Bloom.Easy
import           Data.List (sort)
import           Data.Maybe (fromMaybe)
import           Data.Proxy (Proxy (..))
import           Data.Vector (Vector)
import qualified Data.Vector as V
import           Database.LSMTree.Extras.Generators (ChunkSize (..),
                     RFPrecision (..), UTxOKey)
import           Database.LSMTree.Extras.Random (sampleUniformWithReplacement,
                     uniformWithoutReplacement)
import qualified Database.LSMTree.Internal.IndexCompact as Index
import           Database.LSMTree.Internal.IndexCompactAcc (Append (..))
import           Database.LSMTree.Internal.Lookup (RunLookupView (..),
                     bloomQueriesDefault, indexSearches, prepLookups)
import           Database.LSMTree.Internal.Serialise (SerialiseKey,
                     SerialisedKey, serialiseKey)
import           GHC.Generics (Generic)
import           Prelude hiding (getContents)
import           System.FS.API
import           System.FS.BlockIO.API (IOOp (..))
import           System.Posix.Types (COff (..))
import           System.Random as R
import           Test.QuickCheck (generate, shuffle)

-- | TODO: add a separate micro-benchmark that includes multi-pages.
benchmarks :: Benchmark
benchmarks = bgroup "Bench.Database.LSMTree.Internal.Lookup" [
      bgroup "prepLookups for a single run" [
          benchPrepLookups defaultConfig
        , benchPrepLookups (defaultConfig {
              name = "default onlyPos"
            , nneg = 0
            })
        , benchPrepLookups (defaultConfig {
              name = "default onlyNeg"
            , npos = 0
            })
        , benchPrepLookups (defaultConfig {
              name = "default high fpr"
            , fpr  = 0.9
            })
        ]
    ]
  where
    benchPrepLookups :: Config -> Benchmark
    benchPrepLookups conf@Config{name} =
      env (prepLookupsEnv (Proxy @UTxOKey) conf) $ \ ~(rs, ks) ->
        bgroup name [
            -- The bloomfilter is queried for all lookup keys. The result is an
            -- unboxed vector, so only use whnf.
            bench "Bloomfilter query" $
              whnf (\ks' -> bloomQueriesDefault rs ks') ks
            -- The compact index is only searched for (true and false) positive
            -- lookup keys. We use whnf here because the result is an unboxed
            -- vector.
          , env (pure $ bloomQueriesDefault rs ks) $ \rkixs ->
              bench "Compact index search" $
                whnfAppIO (\ks' -> indexSearches rs ks' rkixs) ks
            -- All prepped lookups are going to be used eventually so we use
            -- @nf@ on the vector of 'IOOp's. We only evaluate the vector of
            -- indexes to WHNF, because it is an unboxed vector.
          , bench "In-memory lookup" $
              nfAppIO (\ks' -> prepLookups rs ks' >>= \(rkixs, ioops) -> pure (rkixs `seq` ioops)) ks
          ]

{-------------------------------------------------------------------------------
  Orphans
-------------------------------------------------------------------------------}

deriving newtype instance NFData BufferOffset
deriving newtype instance NFData COff
deriving instance Generic (IOOp s h)
deriving instance NFData h => NFData (IOOp s h)
deriving anyclass instance NFData FsPath
deriving instance NFData h => NFData (Handle h)
deriving stock instance Generic (RunLookupView h)
deriving anyclass instance NFData h => NFData (RunLookupView h)

{-------------------------------------------------------------------------------
  Environments
-------------------------------------------------------------------------------}

-- | Config options describing a benchmarking scenario
data Config = Config {
    -- | Name for the benchmark scenario described by this config.
    name         :: !String
    -- | If 'Nothing', use 'suggestRangeFinderPrecision'.
  , rfprecDef    :: !(Maybe Int)
    -- | Chunk size for compact index construction
  , csize        :: !ChunkSize
    -- | Number of pages in total
  , npages       :: !Int
    -- | Number of entries per page
  , npageEntries :: !Int
    -- | Number of positive lookups
  , npos         :: !Int
    -- | Number of negative lookups
  , nneg         :: !Int
  , fpr          :: !Double
  }

defaultConfig :: Config
defaultConfig = Config {
    name         = "default config"
  , rfprecDef    = Nothing
  , csize        = ChunkSize 100
  , npages       = 50_000
  , npageEntries = 40
  , npos         = 10_000
  , nneg         = 10_000
  , fpr          = 0.1
  }

-- | Use 'lookupsEnv' to set up an environment for the in-memory aspect of
-- lookups.
prepLookupsEnv ::
     forall k. (Ord k, Uniform k, SerialiseKey k)
  => Proxy k
  -> Config
  -> IO (Vector (RunLookupView (Handle ())), Vector SerialisedKey)
prepLookupsEnv _ Config {..} = do
    (storedKeys, lookupKeys) <- lookupsEnv @k (mkStdGen 17) totalEntries npos nneg
    let b    = Bloom.Easy.easyList fpr $ fmap serialiseKey storedKeys
        -- This doesn't ensure partitioning, but it means we can keep page
        -- generation simple. The consequence is that index search can return
        -- off-by-one results, but we take that as a minor inconvience.
        ps   = groupsOfN npageEntries storedKeys
        apps = mkAppend <$> fmap (fmap serialiseKey) ps
        ic   = constructIndexCompact csize (rfprec, apps)
    pure ( V.singleton (RunLookupView (Handle () (mkFsPath [])) b ic)
         , serialiseKey <$> V.fromList lookupKeys
         )
  where
    totalEntries = npages * npageEntries
    rfprec = RFPrecision $
      fromMaybe (Index.suggestRangeFinderPrecision npages) rfprecDef

-- | Generate keys to store and keys to lookup
lookupsEnv ::
     (Ord k, Uniform k)
  => StdGen -- ^ RNG
  -> Int -- ^ Number of stored keys
  -> Int -- ^ Number of positive lookups
  -> Int -- ^ Number of negative lookups
  -> IO ([k], [k])
lookupsEnv g nkeys npos nneg = do
    let  (g1, g2) = R.split g
    let (xs, ys1) = splitAt nkeys
                  $ uniformWithoutReplacement    g1 (nkeys + nneg)
        ys2       = sampleUniformWithReplacement g2 npos xs
    zs <- generate $ shuffle (ys1 ++ ys2)
    pure (sort xs, zs)

groupsOfN :: Int -> [a] -> [[a]]
groupsOfN _ [] = []
groupsOfN n xs = let (ys, zs) = splitAt n xs
                 in  ys : groupsOfN n zs

mkAppend :: [SerialisedKey] -> Append
mkAppend []  = error "Pages must be non-empty"
mkAppend [k] = AppendSinglePage k k
mkAppend ks  = AppendSinglePage (head ks) (last ks)
