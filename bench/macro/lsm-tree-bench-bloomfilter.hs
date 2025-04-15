{-# LANGUAGE BangPatterns       #-}
{-# LANGUAGE CPP                #-}
{-# LANGUAGE NumericUnderscores #-}

module Main ( main ) where

import           Control.Exception (evaluate)
import           Control.Monad
import           Control.Monad.ST
import           Control.Monad.ST.Unsafe
import           Data.Bits ((.&.))
import           Data.BloomFilter.Blocked (Bloom, BloomSize)
import qualified Data.BloomFilter.Blocked as Bloom
import           Data.Time
import           Data.Vector (Vector)
import qualified Data.Vector as V
import qualified Data.Vector.Primitive as VP
import           Data.WideWord.Word256 (Word256)
import           GHC.Stats
import           Numeric
import           System.IO
import           System.Mem (performMajorGC)
import           System.Random
import           Text.Printf (printf)

import           Database.LSMTree.Extras.Orphans ()
import           Database.LSMTree.Internal.Assertions (fromIntegralChecked)
import qualified Database.LSMTree.Internal.BloomFilterQuery1 as Bloom1
import           Database.LSMTree.Internal.Serialise (SerialisedKey,
                     serialiseKey)

#ifdef BLOOM_QUERY_FAST
import qualified Database.LSMTree.Internal.BloomFilterQuery2 as Bloom2
#endif

main :: IO ()
main = do
  hSetBuffering stdout NoBuffering
  benchmarks

-- Benchmark parameters you can tweak. The defaults are rather small, so the
-- runtime is short and do not show the effects of data sizes no longer fitting
-- into the CPU cache.

-- | The number of entries in the filter in the smallest LSM runs is
-- @2^benchmarkSizeBase@
benchmarkSizeBase :: SizeBase
benchmarkSizeBase = 16

-- | The number of lookups to do. This has to be smaller than the total size of
-- all the filters (otherwise we will not get true positive probes, which is
-- part of the point of this benchmark).
benchmarkNumLookups :: Integer
benchmarkNumLookups = 25_000_000

-- | The number of lookups to do in a single batch.
benchmarkBatchSize :: Int
benchmarkBatchSize = 256

benchmarkNumBitsPerEntry :: RequestedBitsPerEntry
benchmarkNumBitsPerEntry = 10

benchmarks :: IO ()
benchmarks = do
#ifdef NO_IGNORE_ASSERTS
    putStrLn "WARNING: Benchmarking in debug mode."
    putStrLn "         To benchmark in release mode, pass:"
    putStrLn "         --project-file=cabal.project.release"
#endif

    enabled <- getRTSStatsEnabled
    unless enabled $ fail "Need RTS +T statistics enabled"
    let filterSizes = lsmStyleBloomFilters benchmarkSizeBase
                                           benchmarkNumBitsPerEntry
    putStrLn "Bloom filter stats:"
    putStrLn "(numEntries, sizeFactor, BloomSize { sizeBits, sizeHashes })"
    mapM_ print filterSizes
    putStrLn $ "total number of entries:\t " ++ show (totalNumEntries filterSizes)
    putStrLn $ "total filter size in bytes:\t " ++ show (totalNumBytes filterSizes)
    putStrLn $ "total number of key lookups:\t " ++ show benchmarkNumLookups

    unless (totalNumEntriesSanityCheck benchmarkSizeBase filterSizes) $
      fail "totalNumEntriesSanityCheck failed"
    unless (totalNumEntries filterSizes >= benchmarkNumLookups) $
      fail "number of key lookups is more than number of entries"

    putStrLn "Generating bloom filters..."
    let rng0 = mkStdGen 42
    vbs <- elemManyEnv filterSizes rng0
    putStrLn " finished."
    putStrLn ""

    hashcost <-
      benchmark "makeHashes"
                "(This baseline is the cost of computing and hashing the keys)"
                (benchInBatches benchmarkBatchSize rng0
                   (benchMakeHashes vbs))
                (fromIntegralChecked benchmarkNumLookups)
                (0, 0)
                289

    _ <-
      benchmark "elemHashes"
                "(this is the simple one-by-one lookup, less the cost of computing and hashing the keys)"
                (benchInBatches benchmarkBatchSize rng0
                  (benchElemHashes vbs))
                (fromIntegralChecked benchmarkNumLookups)
                hashcost
                0

    _ <-
      benchmark "bloomQueries1"
                "(this is the batch lookup, less the cost of computing and hashing the keys)"
                (benchInBatches benchmarkBatchSize rng0
                  (\ks -> Bloom1.bloomQueries vbs ks `seq` ()))
                (fromIntegralChecked benchmarkNumLookups)
                hashcost
                0

#ifdef BLOOM_QUERY_FAST
    _ <-
      benchmark "bloomQueries2"
                "(this is the optimised batch lookup, less the cost of computing and hashing the keys)"
                (benchInBatches benchmarkBatchSize rng0
                  (\ks -> Bloom2.bloomQueries vbs ks `seq` ()))
                (fromIntegralChecked benchmarkNumLookups)
                hashcost
                0
#endif

    return ()

type Alloc = Int

benchmark :: String
          -> String
          -> (Int -> ())
          -> Int
          -> (NominalDiffTime, Alloc)
          -> Int
          -> IO (NominalDiffTime, Alloc)
benchmark name description action n (subtractTime, subtractAlloc) expectedAlloc = do
    putStrLn $ "Benchmarking " ++ name ++ " ... "
    putStrLn description
    performMajorGC
    allocBefore <- allocated_bytes <$> getRTSStats
    timeBefore  <- getCurrentTime
    evaluate (action n)
    timeAfter   <- getCurrentTime
    performMajorGC
    allocAfter  <- allocated_bytes <$> getRTSStats
    putStrLn "Finished."
    let allocTotal :: Alloc
        timeTotal  = timeAfter `diffUTCTime` timeBefore
        allocTotal = fromIntegral allocAfter - fromIntegral allocBefore
        timeNet    = timeTotal - subtractTime
        allocNet   = allocTotal - subtractAlloc

        timePerKey, allocPerKey :: Double
        timePerKey     = realToFrac timeNet / fromIntegral n
        allocPerKey    = fromIntegral allocNet / fromIntegral n
    let printStat :: String -> Double -> String -> IO ()
        printStat label v unit =
          putStrLn $ label ++ showGFloat (Just 2) v (' ':unit)
    printStat "Time total:        " (realToFrac timeTotal) "seconds"
    printStat "Alloc total:       " (fromIntegral allocTotal) "bytes"
    printStat "Time net:          " (realToFrac timeNet) "seconds"
    printStat "Alloc net:         " (fromIntegral allocNet) "bytes"
    printStat "Time net per key:  " timePerKey "seconds"
    printStat "Alloc net per key: " allocPerKey "bytes"

    unless (truncate allocPerKey == expectedAlloc) $ do
        printf "WARNING: expecting %d, got %d bytes allocated per key\n"
            expectedAlloc
            (truncate allocPerKey :: Int)

    putStrLn ""
    return (timeNet, allocNet)

-- | (numEntries, sizeFactor, (BloomSize numBits numHashFuncs))
type BloomFilterSizeInfo = (Integer, Integer, BloomSize)
type SizeBase     = Int
type RequestedBitsPerEntry = Double

-- | Calculate the sizes of a realistic LSM style set of Bloom filters, one
-- for each LSM run. This uses base 4, with 4 disk levels, using tiering
-- for internal levels and leveling for the final (biggest) level.
--
-- Due to the incremental merging, each level actually has (in the worst case)
-- 2x the number of runs, hence 8 per level for tiering levels.
--
lsmStyleBloomFilters :: SizeBase -> RequestedBitsPerEntry -> [BloomFilterSizeInfo]
lsmStyleBloomFilters l1 requestedBitsPerEntry =
    [ (numEntries, sizeFactor, bsize)
    | (numEntries, sizeFactor)
        <- replicate 8 (2^(l1+0), 1)   -- 8 runs at level 1 (tiering)
        ++ replicate 8 (2^(l1+2), 4)   -- 8 runs at level 2 (tiering)
        ++ replicate 8 (2^(l1+4),16)   -- 8 runs at level 3 (tiering)
        ++            [(2^(l1+8),256)] -- 1 run  at level 4 (leveling)
    , let bsize = Bloom.sizeForBits requestedBitsPerEntry (fromIntegral numEntries)
    ]

totalNumEntries, totalNumBytes :: [BloomFilterSizeInfo] -> Integer
totalNumEntries filterSizes =
    sum [ numEntries | (numEntries, _, _) <- filterSizes ]

totalNumBytes filterSizes =
    sum [ toInteger (Bloom.sizeBits bsize)
        | (_,_,bsize) <- filterSizes ]
      `div` 8

totalNumEntriesSanityCheck :: SizeBase -> [BloomFilterSizeInfo] -> Bool
totalNumEntriesSanityCheck l1 filterSizes =
    totalNumEntries filterSizes
    ==
    sum [ 2^l1 * sizeFactor | (_, sizeFactor, _) <- filterSizes ]


-- | Input environment for benchmarking 'Bloom.elemMany'.
--
-- The idea here is to have a collection of bloom filters corresponding to
-- the sizes used in a largeish LSM. In particular, the sizes are in increasing
-- powers of 4, and the largest ones should be bigger than the CPU L3 cache.
-- Furthermore, the keys in the filters are non-overlapping, and lookups will
-- be true positives in only one filter. Thus most lookups will be true
-- negatives.
--
-- The goal is to benchmark the benefits of optimisations for the LSM situation:
--
-- * where the same key is being looked up in many filters,
-- * with a hit in only one filter expected, and
-- * where the total size of the filters is too large to fully fit in cache
--   (though the smaller ones may fit in the caches).
--
elemManyEnv :: [BloomFilterSizeInfo]
            -> StdGen
            -> IO (Vector (Bloom SerialisedKey))
elemManyEnv filterSizes rng0 =
  stToIO $ do
    -- create the filters
    mbs <- sequence [ Bloom.new bsize | (_, _, bsize) <- filterSizes ]
    -- add elements
    foldM_
      (\rng (i, mb) -> do
         -- progress
         when (i .&. 0xFFFF == 0) (unsafeIOToST $ putStr ".")
         -- insert n elements into filter b
         let k :: Word256
             (!k, !rng') = uniform rng
         Bloom.insert mb (serialiseKey k)
         return rng'
      )
      rng0
      (zip [0 .. totalNumEntries filterSizes - 1]
           (cycle [ mb'
                  | (mb, (_, sizeFactor, _)) <- zip mbs filterSizes
                  , mb' <- replicate (fromIntegralChecked sizeFactor) mb ]))
    V.fromList <$> mapM Bloom.unsafeFreeze mbs

type BatchBench = V.Vector SerialisedKey -> ()

{-# NOINLINE benchInBatches #-}
benchInBatches :: Int -> StdGen -> BatchBench -> Int -> ()
benchInBatches !b !rng0 !action =
    go rng0
  where
    go !rng !n
      | n <= 0    = ()
      | otherwise =
        let (!rng'', !rng') = splitGen rng
            ks  :: VP.Vector Word256
            !ks  = VP.unfoldrExactN b uniform rng'
            ks' :: V.Vector SerialisedKey
            !ks' = V.map serialiseKey (V.convert ks)
        in action ks' `seq` go rng'' (n-b)

-- | This gives us a combined cost of calculating the series of keys and their
-- hashes (when used with 'benchInBatches').
benchMakeHashes :: Vector (Bloom SerialisedKey) -> BatchBench
benchMakeHashes !_bs !ks =
    let khs :: VP.Vector (Bloom.Hashes SerialisedKey)
        !khs = V.convert (V.map Bloom.hashes ks)
     in khs `seq` ()

-- | This gives us a combined cost of calculating the series of keys, their
-- hashes, and then using 'Bloom.elemHashes' with each filter  (when used
-- with 'benchInBatches').
benchElemHashes :: Vector (Bloom SerialisedKey) -> BatchBench
benchElemHashes !bs !ks =
    let khs :: VP.Vector (Bloom.Hashes SerialisedKey)
        !khs = V.convert (V.map Bloom.hashes ks)
     in V.foldl'
          (\_ b -> VP.foldl'
                     (\_ kh -> Bloom.elemHashes b kh `seq` ())
                     () khs)
          () bs
