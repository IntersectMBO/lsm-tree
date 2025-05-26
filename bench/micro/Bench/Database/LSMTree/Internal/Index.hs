{-# LANGUAGE CPP #-}

module Bench.Database.LSMTree.Internal.Index (benchmarks) where

import           Control.Category ((>>>))
import           Control.DeepSeq (rnf)
import           Control.Monad.ST.Strict (runST)
import           Criterion.Main (Benchmark, Benchmarkable, bench, bgroup, env,
                     whnf)
#if !MIN_VERSION_base(4,20,0)
import           Data.List (foldl')
                 -- foldl' is included in the Prelude from base 4.20 onwards
#endif
import           Database.LSMTree.Extras.Generators (getKeyForIndexCompact,
                     mkPages, toAppends)
                     -- also for @Arbitrary@ instantiation of @SerialisedKey@
import           Database.LSMTree.Extras.Index (Append, append)
import           Database.LSMTree.Internal.Index (Index,
                     IndexType (Compact, Ordinary), newWithDefaults, search,
                     unsafeEnd)
import           Database.LSMTree.Internal.Serialise
                     (SerialisedKey (SerialisedKey))
import           Test.QuickCheck (choose, vector)
import           Test.QuickCheck.Gen (Gen (MkGen))
import           Test.QuickCheck.Random (mkQCGen)

-- * Benchmarks

benchmarks :: Benchmark
benchmarks = bgroup "Bench.Database.LSMTree.Internal.Index" $
             map (uncurry benchmarksForSingleType)          $
             [("Compact", Compact), ("Ordinary", Ordinary)]
    where

    benchmarksForSingleType :: String -> IndexType -> Benchmark
    benchmarksForSingleType indexTypeName indexType
        = bgroup (indexTypeName ++ " index") $
          [
              -- Search
              env (pure $ searchIndex indexType 10000) $ \ index ->
              env (pure $ searchKeys 1000)             $ \ keys  ->
              bench "Search" $
              searchBenchmarkable index keys,

              -- Incremental construction
              env (pure $ incrementalConstructionAppends 10000) $ \ appends ->
              bench "Incremental construction" $
              incrementalConstructionBenchmarkable indexType appends
          ]

-- * Utilities

-- | Deterministically constructs a value using a QuickCheck generator.
generated :: Gen a -> a
generated (MkGen exec) = exec (mkQCGen 411) 30

{-|
    Constructs serialised keys that conform to the key size constraint of
    compact indexes.
-}
keysForIndexCompact :: Int             -- ^ Number of keys
                    -> [SerialisedKey] -- ^ Constructed keys
keysForIndexCompact = vector                                        >>>
                      generated                                     >>>
                      map (getKeyForIndexCompact >>> SerialisedKey)

{-|
    Constructs append operations whose serialised keys conform to the key size
    constraint of compact indexes.
-}
appendsForIndexCompact :: Int      -- ^ Number of keys used in the construction
                       -> [Append] -- ^ Constructed append operations
appendsForIndexCompact = keysForIndexCompact                >>>
                         mkPages 0.03 (choose (0, 16)) 0.01 >>>
                         generated                          >>>
                         toAppends
{-
    The arguments used for 'mkPages' are the same as the ones used for
    'genPages' in the instantiation of 'Arbitrary' for 'LogicalPageSummaries' at
    the time of writing.
-}

{-|
    Constructs an index by applying append operations to an initially empty
    index.
-}
indexFromAppends :: IndexType -> [Append] -> Index
indexFromAppends indexType appends = runST $ do
    indexAcc <- newWithDefaults indexType
    mapM_ (flip append indexAcc) appends
    snd <$> unsafeEnd indexAcc

-- * Benchmark ingredients

-- ** Search

-- | Constructs an index to be searched.
searchIndex :: IndexType -- ^ Type of index to construct
            -> Int       -- ^ Number of keys used in the construction
            -> Index     -- ^ Constructed index
searchIndex indexType keyCount
    = indexFromAppends indexType (appendsForIndexCompact keyCount)

-- | Constructs a list of keys to search for.
searchKeys :: Int             -- ^ Number of searches
           -> [SerialisedKey] -- ^ Constructed search keys
searchKeys = keysForIndexCompact

-- | The action to be performed by a search benchmark.
searchBenchmarkable :: Index -> [SerialisedKey] -> Benchmarkable
searchBenchmarkable index = whnf $ foldl' (\ _ key -> rnf (search key index)) ()

-- ** Incremental construction

-- | Constructs append operations to be used in index construction.
incrementalConstructionAppends ::
       Int      -- ^ Number of keys used in the construction
    -> [Append] -- ^ Constructed append operations
incrementalConstructionAppends = appendsForIndexCompact

-- | The action to be performed by an incremental-construction benchmark.
incrementalConstructionBenchmarkable :: IndexType -> [Append] -> Benchmarkable
incrementalConstructionBenchmarkable indexType appends
    = whnf (indexFromAppends indexType) appends
