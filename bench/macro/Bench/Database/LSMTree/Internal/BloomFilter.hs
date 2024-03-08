{-# LANGUAGE BangPatterns       #-}
{-# LANGUAGE NumericUnderscores #-}

module Bench.Database.LSMTree.Internal.BloomFilter (
    benchmarks
  ) where

import           Control.Exception (assert, evaluate)
import           Control.Monad
import           Control.Monad.ST
import           Control.Monad.ST.Unsafe
import           Data.Bits ((.&.))
import qualified Data.BloomFilter as Bloom
import qualified Data.BloomFilter.Hash as Bloom
import qualified Data.BloomFilter.Mutable as MBloom
import           Data.List (foldl')
import           Data.Time
import           Data.Vector (Vector)
import qualified Data.Vector as Vector
import           Data.WideWord.Word256 (Word256)
import           Database.LSMTree.Internal.Run.BloomFilter as Bloom
import           Database.LSMTree.Internal.Serialise (SerialisedKey,
                     serialiseKey)
import           Database.LSMTree.Util.Orphans ()
import           System.Random


-- Benchmark parameters you can tweak. The defaults are rather small, so the
-- runtime is short and do not show the effects of data sizes no longer fitting
-- into the CPU cache.

-- | The 2^n number of entries in the smallest LSM run filters.
benchmarkSizeBase :: SizeBase
benchmarkSizeBase = 16

-- | The number of lookups to do. This has to be smaller than the total size of
-- all the filters (otherwise we will not get true positive probes, which is
-- part of the point of this benchmark).
benchmarkNumLookups :: Int
benchmarkNumLookups = 25_000_000

-- A value of 0.02 results in 5 hashes and just over 8 bits per key.
benchmarkRequestedFPR :: RequestedFPR
benchmarkRequestedFPR = 0.02

benchmarks :: IO ()
benchmarks = do
   let filterSizes = lsmStyleBloomFilters benchmarkSizeBase
                                          benchmarkRequestedFPR
   assert (totalNumEntriesSanityCheck benchmarkSizeBase filterSizes) (return ())
   putStrLn "Bloom filter stats:"
   putStrLn "(numEntries, sizeFactor, numBits, numHashFuncs)"
   mapM_ print filterSizes
   putStrLn $ "total number of entries:\t " ++ show (totalNumEntries filterSizes)
   putStrLn $ "total filter size in bytes:\t " ++ show (totalNumBytes filterSizes)
   putStrLn $ "total number of key lookups:\t " ++ show benchmarkNumLookups

   putStrLn "Generating bloom filters..."
   let rng0 = mkStdGen 42
   vbs <- elemManyEnv filterSizes rng0
   putStrLn " finished."
   putStrLn ""

   do putStrLn "Benchmarking baseline cost ... "
      putStrLn "(This is the cost of just computing the keys."
      putStrLn "Subtract this from all subsequent benchmarks.)"
      before <- getCurrentTime
      evaluate (benchBaseline vbs rng0 benchmarkNumLookups)
      after <- getCurrentTime
      putStr "Finished: "
      print (after `diffUTCTime` before)
      putStrLn ""

   do putStrLn "Benchmarking makeCheapHashes cost ... "
      putStrLn "(This includes the cost of hashing the keys. Subtract this"
      putStrLn "from all subsequent benchmarks to isolate non-hash costs.)"
      before <- getCurrentTime
      evaluate (benchMakeCheapHashes vbs rng0 benchmarkNumLookups)
      after <- getCurrentTime
      putStr "Finished: "
      print (after `diffUTCTime` before)
      putStrLn ""

   do putStrLn "Benchmarking elemCheapHashes ... "
      putStrLn "(this is the simple one-by-one lookup, not bulk lookup)"
      before <- getCurrentTime
      evaluate (benchElemCheapHashes vbs rng0 benchmarkNumLookups)
      after <- getCurrentTime
      putStr "Finished: "
      print (after `diffUTCTime` before)
      putStrLn ""

   do putStrLn "Benchmarking elemsMany ... "
      putStrLn "(this is the bulk lookup)"
      before <- getCurrentTime
      evaluate (benchElemsMany vbs rng0 benchmarkNumLookups)
      after <- getCurrentTime
      putStr "Finished: "
      print (after `diffUTCTime` before)
      putStrLn ""

-- | (numEntries, sizeFactor, numBits, numHashFuncs)
type BloomFilterSizeInfo = (Int, Int, Int, Int)
type SizeBase     = Int
type RequestedFPR = Double

-- | Calculate the sizes of a realistic LSM style set of Bloom filters, one
-- for each LSM run. This uses base 4, with 4 disk levels, using tiering
-- for internal levels and leveling for the final (biggest) level.
--
lsmStyleBloomFilters :: SizeBase -> RequestedFPR -> [BloomFilterSizeInfo]
lsmStyleBloomFilters l1 requestedFPR =
    [ (numEntries, sizeFactor, numBits, numHashFuncs)
    | (numEntries, sizeFactor)
        <- replicate 4 (2^(l1+0), 1)   -- 4 runs at level 1 (tiering)
        ++ replicate 8 (2^(l1+2), 4)   -- 8 runs at level 2 (tiering)
        ++ replicate 8 (2^(l1+4),16)   -- 8 runs at level 3 (tiering)
        ++            [(2^(l1+8),256)] -- 1 run  at level 4 (leveling)
    , let numBits      = Bloom.monkeyBits numEntries requestedFPR
          numHashFuncs = Bloom.monkeyHashFuncs numBits numEntries
    ]

totalNumEntries, totalNumBytes :: [BloomFilterSizeInfo] -> Int
totalNumEntries filterSizes =
    sum [ numEntries | (numEntries, _, _, _) <- filterSizes ]

totalNumBytes filterSizes =
    sum [ numBits | (_,_,numBits,_) <- filterSizes ] `div` 8

totalNumEntriesSanityCheck :: SizeBase -> [BloomFilterSizeInfo] -> Bool
totalNumEntriesSanityCheck l1 filterSizes =
    totalNumEntries filterSizes
    ==
    sum [ 2^l1 * sizeFactor | (_, sizeFactor, _, _) <- filterSizes ]


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
-- * where the total size of the filters is too large to fully cache (though
--   the smaller ones may fit in the caches).
--
elemManyEnv :: [BloomFilterSizeInfo]
            -> StdGen
            -> IO (Vector (Bloom SerialisedKey))
elemManyEnv filterSizes rng0 =
  stToIO $ do
    -- create the filters
    mbs <- sequence
             [ MBloom.new numHashFuncs numBits
             | (_, _, numBits, numHashFuncs) <- filterSizes ]
    -- add elements
    foldM_
      (\rng (i, mb) -> do
         -- progress
         when (i .&. 0xFFFF == 0) (unsafeIOToST $ putStr ".")
         -- insert n elements into filter b
         let k :: Word256
             (!k, !rng') = uniform rng
         MBloom.insert mb (serialiseKey k)
         return rng'
      )
      rng0
      (zip [0 .. totalNumEntries filterSizes - 1]
           (cycle [ mb'
                  | (mb, (_, sizeFactor, _, _)) <- zip mbs filterSizes
                  , mb' <- replicate sizeFactor mb ]))
    Vector.fromList <$> mapM unsafeFreeze mbs

-- | This gives us a baseline cost of just calculating the series of keys.
benchBaseline :: Vector (Bloom SerialisedKey) -> StdGen -> Int -> ()
benchBaseline !_  !_   0 = ()
benchBaseline !bs !rng !n =
    let k :: Word256
        (!k, !rng') = uniform rng
        !kh = serialiseKey k
     in kh `seq` benchBaseline bs rng' (n-1)

-- | This gives us a combined cost of calculating the series of keys and their
-- hashes.
benchMakeCheapHashes :: Vector (Bloom SerialisedKey) -> StdGen -> Int -> ()
benchMakeCheapHashes !_  !_   0 = ()
benchMakeCheapHashes !bs !rng !n =
    let k :: Word256
        (!k, !rng') = uniform rng
        !kh = Bloom.makeCheapHashes (serialiseKey k)
     in kh `seq` benchMakeCheapHashes bs rng' (n-1)

-- | This gives us a combined cost of calculating the series of keys, their
-- hashes, and then using 'Bloom.elemCheapHashes' with each filter.
benchElemCheapHashes :: Vector (Bloom SerialisedKey) -> StdGen -> Int -> ()
benchElemCheapHashes !_  !_   0 = ()
benchElemCheapHashes !bs !rng !n =
    let k :: Word256
        (!k, !rng') = uniform rng
        !kh =  Bloom.makeCheapHashes (serialiseKey k)
     in foldl' (\_ b -> Bloom.elemCheapHashes kh b `seq` ()) () bs
  `seq` benchElemCheapHashes bs rng' (n-1)

-- | This gives us a combined cost of calculating the series of keys, and
-- using 'Bloom.elemMany' for each one.
benchElemsMany :: Vector (Bloom SerialisedKey) -> StdGen -> Int -> ()
benchElemsMany !_  !_   0 = ()
benchElemsMany !bs !rng !n =
    let k :: Word256
        (!k, !rng') = uniform rng
        !k' =  serialiseKey k
     in Bloom.elemMany k' bs `seq` benchElemsMany bs rng' (n-1)

