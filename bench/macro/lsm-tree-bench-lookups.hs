{-# LANGUAGE CPP #-}

module Main ( main ) where

import           Control.DeepSeq
import           Control.Exception (bracket)
import           Control.Monad
import           Control.Monad.Class.MonadST
import           Control.Monad.Primitive
import           Control.Monad.ST.Strict (ST, runST)
import           Control.RefCount
import           Data.Bits ((.&.))
import           Data.BloomFilter.Blocked (Bloom)
import qualified Data.BloomFilter.Blocked as Bloom
import           Data.Time
import qualified Data.Vector as V
import           Data.Vector.Algorithms.Merge as Merge
import qualified Data.Vector.Generic.Mutable as VGM
import qualified Data.Vector.Mutable as VM
import qualified Data.Vector.Primitive as VP
import qualified Data.Vector.Unboxed.Mutable as VUM
import           Database.LSMTree.Extras.Orphans ()
import           Database.LSMTree.Extras.UTxO
import           Database.LSMTree.Internal.Arena (ArenaManager, newArenaManager,
                     withArena)
import           Database.LSMTree.Internal.Entry (Entry (Insert),
                     NumEntries (..))
import           Database.LSMTree.Internal.Index (Index)
import qualified Database.LSMTree.Internal.Index as Index (IndexType (Compact))
import           Database.LSMTree.Internal.Lookup
import           Database.LSMTree.Internal.Paths (RunFsPaths (RunFsPaths))
import           Database.LSMTree.Internal.Run (Run)
import qualified Database.LSMTree.Internal.Run as Run
import           Database.LSMTree.Internal.RunAcc (RunBloomFilterAlloc (..))
import           Database.LSMTree.Internal.RunBuilder (RunParams (..))
import qualified Database.LSMTree.Internal.RunBuilder as RunBuilder
import           Database.LSMTree.Internal.RunNumber
import           Database.LSMTree.Internal.Serialise (ResolveSerialisedValue,
                     SerialisedKey, serialiseKey, serialiseValue)
import qualified Database.LSMTree.Internal.WriteBuffer as WB
import qualified Database.LSMTree.Internal.WriteBufferBlobs as WBB
import           Debug.Trace (traceMarkerIO)
import           GHC.Stats
import           Numeric
import           System.Environment (getArgs)
import           System.Exit (exitFailure)
import qualified System.FS.API as FS
import qualified System.FS.BlockIO.API as FS
import qualified System.FS.BlockIO.IO as FS
import qualified System.FS.IO as FS
import           System.IO
import           System.IO.Unsafe (unsafePerformIO)
import           System.Mem (performMajorGC)
import           System.Random

main :: IO ()
main = do
  hSetBuffering stdout NoBuffering
  args <- getArgs
  case args of
    [arg] | let cache = read arg ->
      benchmarks (if cache then Run.CacheRunData else Run.NoCacheRunData)
    _     -> do
      putStrLn "Wrong usage, pass in [True] or [False] for the caching flag."
      exitFailure

-- | The number of entries in the smallest LSM runs is @2^benchmarkSizeBase@.
--
-- This is currently set to however many UTXO entries fit into 2MB of disk
-- pages.
--
-- >>> benchmarkSizeBase
-- 14
benchmarkSizeBase :: SizeBase
benchmarkSizeBase = floor @Double $
    logBase 2 ( fromIntegral @Int ((2 * 1024 * 1024 `div` 4096)
              * (floor @Double numEntriesFitInPage)) )

-- | The number of lookups to do. This has to be smaller than the total size of
-- all the runs (otherwise we will not get true positive probes, which is part
-- of the point of this benchmark).
--
-- The benchmark might do slightly more lookups, because it generates batches of
-- keys of size 'benchmarkGenBatchSize'. This shouldn't affect the results
-- significantly, unless 'benchmarkNumLookups' approaches
-- 'benchmarkGenBatchSize'.
benchmarkNumLookups :: Int
benchmarkNumLookups = 1_000_000 -- 10 * the stretch target

-- | The size of batches as they are generated by the benchmark.
benchmarkGenBatchSize :: Int
benchmarkGenBatchSize = 256

benchmarkNumBitsPerEntry :: Double
benchmarkNumBitsPerEntry = 10

benchmarkResolveSerialisedValue :: ResolveSerialisedValue
benchmarkResolveSerialisedValue = const

-- >>> pageBits
-- 32768
pageBits :: Int
pageBits = 4096 * 8 -- page size in bits

-- >>> unusedPageBits
-- 32688
unusedPageBits :: Int
unusedPageBits = pageBits -- page size in bits
               - 8 * 8    -- directory
               - 16       -- last value offset

-- >>> entryBits
-- 752
entryBits :: Int
entryBits = 34 * 8 -- key size
          + 60 * 8 -- value size

-- >>> entryBitsWithOverhead
-- 787
entryBitsWithOverhead :: Int
entryBitsWithOverhead = entryBits -- key and value size
                      + 1         -- blobref indicator
                      + 2         -- operation type
                      + 16        -- key offset
                      + 16        -- value offset

-- >>> numEntriesFitInPage
-- 41.53494282083863
numEntriesFitInPage :: Fractional a => a
numEntriesFitInPage = fromIntegral unusedPageBits / fromIntegral entryBitsWithOverhead

benchSalt :: Bloom.Salt
benchSalt = 4

benchmarks :: Run.RunDataCaching -> IO ()
benchmarks !caching = withFS $ \hfs hbio -> do
#ifdef NO_IGNORE_ASSERTS
    putStrLn "WARNING: Benchmarking in debug mode."
    putStrLn "         To benchmark in release mode, pass:"
    putStrLn "         --project-file=cabal.project.release"
#endif
    arenaManager <- newArenaManager
    enabled <- getRTSStatsEnabled
    unless enabled $ fail "Need RTS +T statistics enabled"
    let runSizes = lsmStyleRuns benchmarkSizeBase
    putStrLn "Precomputed run stats:"
    putStrLn "(numEntries, sizeFactor)"
    mapM_ print runSizes
    putStrLn $ "total number of entries:\t " ++ show (totalNumEntries runSizes)
    putStrLn $ "total number of key lookups:\t " ++ show benchmarkNumLookups

    unless (totalNumEntriesSanityCheck benchmarkSizeBase runSizes) $
      fail "totalNumEntriesSanityCheck failed"
    unless (totalNumEntries runSizes >= benchmarkNumLookups) $
      fail "number of key lookups is more than number of entries"

    traceMarkerIO "Generating runs"
    putStr "<Generating runs>"
    -- This initial key RNG is used for both generating the runs and generating
    -- lookup batches. This ensures that we only only perform true positive
    -- lookups. Also, lookupsEnv shuffles the generated keys into the different
    -- runs, such that the generated lookups access the runs in random places
    -- instead of sequentially.
    let keyRng0 = mkStdGen 17

    (!runs, !blooms, !indexes, !handles) <- lookupsEnv runSizes keyRng0 hfs hbio caching
    putStrLn "<finished>"

    traceMarkerIO "Computing statistics for generated runs"
    let numEntries = V.map Run.size runs
        numPages   = V.map Run.sizeInPages runs
        nhashes    = V.map (Bloom.sizeHashes . Bloom.size) blooms
        bitsPerEntry = V.zipWith
                         (\b (NumEntries n) ->
                             fromIntegral (Bloom.sizeBits (Bloom.size b))
                           / fromIntegral n :: Double)
                         blooms
                         numEntries
        stats = V.zip4 numEntries numPages nhashes bitsPerEntry
    putStrLn "Actual stats for generated runs:"
    putStrLn "(numEntries, numPages, numHashes, bits per entry)"
    mapM_ print stats

    _ <- putStr "Pausing. Drop caches now! When ready, press enter." >> getLine

    traceMarkerIO "Running benchmark"
    putStrLn ""
    bgenKeyBatches@(x1, y1) <-
      benchmark "benchGenKeyBatches"
                "Calculate batches of keys. This serves as a baseline for later benchmark runs to compare against."
                (pure . benchGenKeyBatches blooms keyRng0) benchmarkNumLookups
                (0, 0)
    _bbloomQueries@(x2, y2) <-
      benchmark "benchBloomQueries"
                "Calculate batches of keys, and perform bloom queries for each batch. Net time/allocation is the result of subtracting the cost of benchGenKeyBatches."
                (pure . benchBloomQueries blooms keyRng0) benchmarkNumLookups
                bgenKeyBatches
    _bindexSearches <-
      benchmark "benchIndexSearches"
                "Calculate batches of keys, perform bloom queries for each batch, and perform index searches for positively queried keys in each batch. Net time/allocation is the result of subtracting the cost of benchGenKeyBatches and benchBloomQueries."
                (benchIndexSearches arenaManager blooms indexes handles keyRng0) benchmarkNumLookups
                (x1 + x2, y1 + y2)
    _bprepLookups <-
      benchmark "benchPrepLookups"
                "Calculate batches of keys, and prepare lookups for each batch. This is roughly doing the same amount of work as benchBloomQueries and benchIndexSearches. Net time/allocation is the result of subtracting the cost of benchGenKeyBatches."
                (benchPrepLookups arenaManager blooms indexes handles keyRng0) benchmarkNumLookups
                bgenKeyBatches
    _blookupsIO <-
      benchmark "benchLookupsIO"
                "Calculate batches of keys, and perform disk lookups for each batch. This is roughly doing the same as benchPrepLookups, but also performing the disk I/O and resolving values. Net time/allocation is the result of subtracting the cost of benchGenKeyBatches."
                (\n -> do
                    let wb_unused = WB.empty
                    bracket (WBB.new hfs (FS.mkFsPath ["wbblobs_unused"])) releaseRef $ \wbblobs_unused ->
                      benchLookupsIO hbio arenaManager benchmarkResolveSerialisedValue
                                     wb_unused wbblobs_unused runs blooms indexes handles
                                     keyRng0 n)
                benchmarkNumLookups
                bgenKeyBatches
    --TODO: consider adding benchmarks that also use the write buffer

    traceMarkerIO "Cleaning up"
    putStrLn "Cleaning up"
    V.mapM_ releaseRef runs

    traceMarkerIO "Computing statistics for prepLookups results"
    putStr "<Computing statistics for prepLookups>"
    let !x = classifyLookups blooms keyRng0 benchmarkNumLookups
    putStrLn "<finished>"
    putStrLn "Statistics for prepLookups:"
    putStrLn "(positives, fpr, tp, fn, fp, tn)"
    print x

type Alloc = Int

benchmark :: String
          -> String
          -> (Int -> IO ())
          -> Int
          -> (NominalDiffTime, Alloc)
          -> IO (NominalDiffTime, Alloc)
benchmark name description action n (subtractTime, subtractAlloc) = do
    traceMarkerIO ("Benchmarking " ++ name)
    putStrLn $ "Benchmarking " ++ name ++ " ... "
    putStrLn description
    performMajorGC
    allocBefore <- allocated_bytes <$> getRTSStats
    timeBefore  <- getCurrentTime
    () <- action n
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

    putStrLn ""
    pure (timeNet, allocNet)

-- | (numEntries, sizeFactor)
type RunSizeInfo = (Int, Int)
type SizeBase     = Int

-- | Calculate the sizes of a realistic LSM style set of runs. This uses base 4,
-- with 4 disk levels, using tiering for internal levels and leveling for the
-- final (biggest) level.
--
-- Due to the incremental merging, each level actually has (in the worst case)
-- 2x the number of runs, hence 8 per level for tiering levels.
--
lsmStyleRuns :: SizeBase -> [RunSizeInfo]
lsmStyleRuns l1 =
       replicate 8 (2^(l1+ 0),   1)  -- 8 runs at level 1 (tiering)
    ++ replicate 8 (2^(l1+ 2),   4)  -- 8 runs at level 2 (tiering)
    ++ replicate 8 (2^(l1+ 4),  16)  -- 8 runs at level 3 (tiering)
    ++ replicate 8 (2^(l1+ 6),  64)  -- 8 runs at level 4 (tiering)
    ++ replicate 8 (2^(l1+ 8), 256)  -- 8 runs at level 5 (tiering)
    ++            [(2^(l1+12),4096)] -- 1 run  at level 6 (leveling)

-- | The total number of entries.
--
-- This should be roughly @100_000_000@ when we pass in @benchmarkSizeBase@.
-- >>> totalNumEntries (lsmStyleRuns benchmarkSizeBase)
-- 111804416
totalNumEntries :: [RunSizeInfo] -> Int
totalNumEntries runSizes =
    sum [ numEntries | (numEntries, _) <- runSizes ]

totalNumEntriesSanityCheck :: SizeBase -> [RunSizeInfo] -> Bool
totalNumEntriesSanityCheck l1 runSizes =
    totalNumEntries runSizes
    ==
    sum [ 2^l1 * sizeFactor | (_, sizeFactor) <- runSizes ]

withFS ::
     (FS.HasFS IO FS.HandleIO -> FS.HasBlockIO IO FS.HandleIO -> IO a)
  -> IO a
withFS action =
    FS.withIOHasBlockIO (FS.MountPoint "_bench_lookups") FS.defaultIOCtxParams $ \hfs hbio -> do
      exists <- FS.doesDirectoryExist hfs (FS.mkFsPath [""])
      unless exists $ error ("_bench_lookups directory does not exist")
      action hfs hbio

-- | Input environment for benchmarking lookup functions.
--
-- The idea here is to have a collection of runs corresponding to the sizes used
-- in a largeish LSM. In particular, the sizes are in increasing powers of 4.
-- The keys in the runs are non-overlapping, and lookups will be true positives
-- in only one run. Thus most lookups will be true negatives.
--
-- The goal is to benchmark the critical path of performing asynchronous lookups
-- serially.
--
-- * where the same key is being looked up in many runs,
-- * with a true positive lookup in only one run,
-- * with true negative lookups in the other runs
-- * with false positives lookups in a fraction of the runs according to the
--   bloom filters' false positive rates
lookupsEnv ::
     [RunSizeInfo]
  -> StdGen -- ^ Key RNG
  -> FS.HasFS IO FS.HandleIO
  -> FS.HasBlockIO IO FS.HandleIO
  -> Run.RunDataCaching
  -> IO ( V.Vector (Ref (Run IO FS.HandleIO))
        , V.Vector (Bloom SerialisedKey)
        , V.Vector Index
        , V.Vector (FS.Handle FS.HandleIO)
        )
lookupsEnv runSizes keyRng0 hfs hbio caching = do
    -- create the vector of initial keys
    (mvec :: VUM.MVector RealWorld UTxOKey) <- VUM.unsafeNew (totalNumEntries runSizes)
    !keyRng1 <- vectorOfUniforms mvec keyRng0
    -- we reuse keyRng0 to generate batches of lookups, so by shuffling the
    -- vector we ensure that these batches of lookups will do random disk
    -- access.
    !_ <- shuffle mvec keyRng1

    -- create the runs
    rbs <- sequence
            [ RunBuilder.new hfs hbio benchSalt
                RunParams {
                  runParamCaching = caching,
                  runParamAlloc   = RunAllocFixed benchmarkNumBitsPerEntry,
                  runParamIndex   = Index.Compact
                }
                (RunFsPaths (FS.mkFsPath []) (RunNumber i))
                (NumEntries numEntries)
            | ((numEntries, _), i) <- zip runSizes [0..] ]

    -- fill the runs
    putStr "addKeyOp"
    let zero = serialiseValue zeroUTxOValue
    foldM_
      (\ !i (!rb, !n) -> do
        let !mvecLocal = VUM.unsafeSlice i n mvec
        Merge.sort mvecLocal
        flip VUM.imapM_ mvecLocal $ \ !j !k -> do
          -- progress
          when (j .&. 0xFFFF == 0) (putStr ".")
          void $ RunBuilder.addKeyOp rb (serialiseKey k) (Insert zero)
        pure (i+n)
      )
      0
      (zip rbs (fmap fst runSizes))
    putStr "DONE"

    -- return runs
    runs <- V.fromList <$> mapM Run.fromBuilder rbs
    let blooms  = V.map (\(DeRef r) -> Run.runFilter   r) runs
        indexes = V.map (\(DeRef r) -> Run.runIndex    r) runs
        handles = V.map (\(DeRef r) -> Run.runKOpsFile r) runs
    pure $!! (runs, blooms, indexes, handles)

genLookupBatch :: StdGen -> Int -> (V.Vector SerialisedKey, StdGen)
genLookupBatch !rng0 !n0
  | n0 <= 0   = error "mkBatch: must be positive"
  | otherwise = runST $ do
      mres <- VM.unsafeNew n0
      go rng0 0 mres
  where
    go ::
         StdGen -> Int -> VM.MVector s SerialisedKey
      -> ST s (V.Vector SerialisedKey, StdGen)
    go !rng !i !mres
      | n0 == i   = do
          !res <- V.unsafeFreeze mres
          pure (res, rng)
      | otherwise = do
          let (!k, !rng') = uniform @UTxOKey @StdGen rng
              !sk = serialiseKey k
          VM.write mres i $! sk
          go rng' (i+1) mres

-- | This gives us the baseline cost of calculating batches of keys.
benchGenKeyBatches ::
     V.Vector (Bloom SerialisedKey)
  -> StdGen
  -> Int
  -> ()
benchGenKeyBatches !bs !keyRng !n
  | n <= 0 = ()
  | otherwise =
      let (!_ks, !keyRng') = genLookupBatch keyRng benchmarkGenBatchSize
      in  benchGenKeyBatches bs keyRng' (n-benchmarkGenBatchSize)

-- | This gives us the combined cost of calculating batches of keys, and
-- performing bloom queries for each batch.
benchBloomQueries ::
     V.Vector (Bloom SerialisedKey)
  -> StdGen
  -> Int
  -> ()
benchBloomQueries !bs !keyRng !n
  | n <= 0 = ()
  | otherwise =
      let (!ks, !keyRng') = genLookupBatch keyRng benchmarkGenBatchSize
      in  bloomQueries benchSalt bs ks `seq`
          benchBloomQueries bs keyRng' (n-benchmarkGenBatchSize)

-- | This gives us the combined cost of calculating batches of keys, performing
-- bloom queries for each batch, and performing index searches for each batch.
benchIndexSearches ::
     ArenaManager RealWorld
  -> V.Vector (Bloom SerialisedKey)
  -> V.Vector Index
  -> V.Vector (FS.Handle h)
  -> StdGen
  -> Int
  -> IO ()
benchIndexSearches !arenaManager !bs !ics !hs !keyRng !n
  | n <= 0 = pure ()
  | otherwise = do
    let (!ks, !keyRng') = genLookupBatch keyRng benchmarkGenBatchSize
        !rkixs = bloomQueries benchSalt bs ks
    !_ioops <- withArena arenaManager $ \arena -> stToIO $ indexSearches arena ics hs ks rkixs
    benchIndexSearches arenaManager bs ics hs keyRng' (n-benchmarkGenBatchSize)

-- | This gives us the combined cost of calculating batches of keys, and
-- preparing lookups for each batch.
benchPrepLookups ::
     ArenaManager RealWorld
  -> V.Vector (Bloom SerialisedKey)
  -> V.Vector Index
  -> V.Vector (FS.Handle h)
  -> StdGen
  -> Int
  -> IO ()
benchPrepLookups !arenaManager !bs !ics !hs !keyRng !n
  | n <= 0 = pure ()
  | otherwise = do
      let (!ks, !keyRng') = genLookupBatch keyRng benchmarkGenBatchSize
      (!_rkixs, !_ioops) <- withArena arenaManager $ \arena -> stToIO $ prepLookups arena benchSalt bs ics hs ks
      benchPrepLookups arenaManager bs ics hs keyRng' (n-benchmarkGenBatchSize)

-- | This gives us the combined cost of calculating batches of keys, and
-- performing disk lookups for each batch.
benchLookupsIO ::
     FS.HasBlockIO IO h
  -> ArenaManager RealWorld
  -> ResolveSerialisedValue
  -> WB.WriteBuffer
  -> Ref (WBB.WriteBufferBlobs IO h)
  -> V.Vector (Ref (Run IO h))
  -> V.Vector (Bloom SerialisedKey)
  -> V.Vector Index
  -> V.Vector (FS.Handle h)
  -> StdGen
  -> Int
  -> IO ()
benchLookupsIO !hbio !arenaManager !resolve !wb !wbblobs !rs !bs !ics !hs =
    go
  where
    go !keyRng !n
      | n <= 0    = pure ()
      | otherwise = do
          let (!ks, !keyRng') = genLookupBatch keyRng benchmarkGenBatchSize
          !_ <- lookupsIOWithWriteBuffer
                  hbio arenaManager resolve benchSalt wb wbblobs rs bs ics hs ks
          go keyRng' (n-benchmarkGenBatchSize)

{-------------------------------------------------------------------------------
  Utilities
-------------------------------------------------------------------------------}

classifyLookups ::
     V.Vector (Bloom SerialisedKey)
  -> StdGen
  -> Int
  -> ( Int, Double -- (all) positives, fpr
     , Int, Int    -- true  positives, false negatives
     , Int, Int    -- false positives, true  negatives
     )
classifyLookups !bs !keyRng0 !n0 =
    let !positives = unsafePerformIO (putStr "classifyLookups")
               `seq` loop 0 keyRng0 n0
        !tp = n0
        !fn = 0
        !fp = positives - tp
        !tn = (V.length bs - 1) * n0
        !fpr = fromIntegral fp / (fromIntegral fp + fromIntegral tn)
    in  positives `seq`
        ( positives, fpr
        , tp, fn
        , fp, tn)
  where
    loop !positives !keyRng !n
      | n <= 0 =
            unsafePerformIO (putStr "DONE") `seq`
            positives
      | otherwise =
          unsafePerformIO (putStr ".") `seq`
          let (!ks, !keyRng') = genLookupBatch keyRng benchmarkGenBatchSize
              !rkixs = bloomQueries benchSalt bs ks
          in  loop (positives + VP.length rkixs) keyRng' (n-benchmarkGenBatchSize)

-- | Fill a mutable vector with uniformly random values.
vectorOfUniforms ::
     (PrimMonad m, VGM.MVector v a, RandomGen g, Uniform a)
  => v (PrimState m) a
  -> g
  -> m g
vectorOfUniforms !vec !g0 = do
    unsafeIOToPrim $ putStr "vectorOfUniforms"
    !g0' <- loop 0 g0
    unsafeIOToPrim $ putStr "DONE"
    pure g0'
  where
    !n = VGM.length vec
    loop !i !g
      | i == n-1 = pure g
      | otherwise = do
          when (i .&. 0xFFFF == 0) (unsafeIOToPrim $ putStr ".")
          let (!x, !g') = uniform g
          VGM.unsafeWrite vec i x
          loop (i+1) g'

-- https://en.wikipedia.org/wiki/Fisher%E2%80%93Yates_shuffle
shuffle ::
     (PrimMonad m, VGM.MVector v a, RandomGen g)
  => v (PrimState m) a
  -> g
  -> m g
shuffle !xs !g0 = do
    unsafeIOToPrim $ putStr "shuffle"
    !g0' <- loop 0 g0
    unsafeIOToPrim $ putStr "DONE"
    pure g0'
  where
    !n = VGM.length xs
    loop !i !g
      | i == n-1 = pure g
      | otherwise = do
          when (i .&. 0xFFFF == 0) (unsafeIOToPrim $ putStr ".")
          let (!j, !g') = randomR (i, n-1) g
          VGM.swap xs i j
          loop (i+1) g'
