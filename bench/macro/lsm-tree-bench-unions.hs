{-# LANGUAGE CPP                   #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# OPTIONS_GHC -Wno-orphans #-}

module Main (main) where

import           Control.Applicative ((<**>))
import           Control.Concurrent.Async (forConcurrently_)
import           Control.Monad ((>=>), forM, forM_, void, when)
import           Control.Monad.Identity (runIdentity)
import           Control.Monad.State.Strict
import qualified Data.ByteString.Short as BS
import qualified Data.Colour.Names as Color
import qualified Data.Colour.SRGB as Color
import qualified Data.Foldable as Fold
import           Data.Functor ((<&>))
import qualified Data.List as List
import           Data.List.NonEmpty (NonEmpty (..), (<|))
import qualified Data.List.NonEmpty as NE
import           Data.Monoid
import qualified Data.Primitive as P
import           Data.Ratio
import qualified Data.Set as Set
import qualified Data.Time.Format as Time
import qualified Data.Time.LocalTime as Time
import qualified Data.Vector as V
import           Data.Word (Word64)
import qualified Graphics.Rendering.Chart.Backend.Diagrams as Plot
import           Graphics.Rendering.Chart.Easy ((.=))
import qualified Graphics.Rendering.Chart.Easy as Plot
import qualified Options.Applicative as O
import           Prelude hiding (lookup)
import qualified System.Clock as Clock
import           System.Directory (createDirectoryIfMissing)
import           System.IO
import           System.Mem (performMajorGC)
import qualified System.Random as Random
import           Text.Printf (printf)
import qualified Text.Read as Read

import           Database.LSMTree.Extras (groupsOfN)
import qualified Database.LSMTree.Extras.Random as Random
import           Database.LSMTree.Internal.ByteString (byteArrayToSBS)

-- We should be able to write this benchmark
-- using only use public lsm-tree interface
import qualified Database.LSMTree.Simple as LSM

-------------------------------------------------------------------------------
-- Constant Values
-------------------------------------------------------------------------------

baselineTableID :: Int
baselineTableID = 0

baselineTableName :: LSM.SnapshotName
baselineTableName = makeTableName baselineTableID

-------------------------------------------------------------------------------
-- Keys and values
-------------------------------------------------------------------------------

type K = BS.ShortByteString
type V = BS.ShortByteString

label :: LSM.SnapshotLabel
label = LSM.SnapshotLabel "K V B"

-- | We generate 34 byte keys by using a PRNG to extend a word64 to 32 bytes
-- and then appending two constant bytes. This corresponds relatively closely
-- to UTxO keys, which are 32 byte cryptographic hashes, followed by two bytes
-- which are typically the 16bit value 0 or 1 (a transaction output index).
--
makeKey :: Word64 -> K
makeKey seed =
    case P.runPrimArray $ do
           v <- P.newPrimArray 5
           let g0 = Random.mkStdGen (fromIntegral seed)
           let (!w0, !g1) = Random.uniform g0
           P.writePrimArray v 0 w0
           let (!w1, !g2) = Random.uniform g1
           P.writePrimArray v 1 w1
           let (!w2, !g3) = Random.uniform g2
           P.writePrimArray v 2 w2
           let (!w3, _g4) = Random.uniform g3
           P.writePrimArray v 3 w3
           P.writePrimArray v 4 0x3d3d3d3d3d3d3d3d -- ========
           case v of
             P.MutablePrimArray mba -> do
               _ <- P.resizeMutableByteArray (P.MutableByteArray mba) 34
               return v

      of (P.PrimArray ba :: P.PrimArray Word64) ->
           byteArrayToSBS (P.ByteArray ba)

-- We use constant value. This shouldn't affect anything.
theValue :: V
theValue = BS.replicate 60 120 -- 'x'
{-# NOINLINE theValue #-}

-------------------------------------------------------------------------------
-- Options and commands
-------------------------------------------------------------------------------

data GlobalOpts = GlobalOpts
    { rootDir     :: !FilePath -- ^ session directory.
    , tableCount  :: !Int   -- ^ Number of  tables in the benchmark
    , initialSize :: !Int  -- ^ Initial size of each table in the benchmark
    , seed        :: !(Maybe Int) -- ^ The seed for the RNG for deterministic behavior,
                           --   use system entropy when not explicitly provided.
    }
  deriving stock Show

data RunOpts = RunOpts
    { batchSamplesPerTick :: !Int -- ^ Increase for greater measurement fidelity
    , batchSizeOfSample   :: !Int -- ^ Tune for your deisred use case.
    , check               :: !Bool
    }
  deriving stock Show

data Cmd
    -- | Setup benchmark: generate initial LSM trees etc.
    = CmdSetup

    -- | Make a dry run, measure the overhead.
    | CmdCollisions

    -- | Run the actual benchmark
    | CmdRun RunOpts
  deriving stock Show

-------------------------------------------------------------------------------
-- command line interface
-------------------------------------------------------------------------------

globalOptsP :: O.Parser GlobalOpts
globalOptsP = pure GlobalOpts
    <*> O.option O.str (O.long "bench-dir" <> O.value "_bench_unions" <> O.showDefault <> O.help "Benchmark directory to put files in")
    <*> O.option (greaterThanOneParser "number of tables") (O.long "table-count" <> O.value 10 <> O.showDefault <> O.help "Number of tables to benchmark")
    <*> O.option (greaterThanOneParser "size of tables") (O.long "initial-size" <> O.value 1_000_000 <> O.showDefault <> O.help "Initial size of each table")
    <*> O.option (Just <$> O.auto) (O.long "seed" <> O.value Nothing <> O.showDefault <> O.help "Random seed, uses system entropy when not explicitly specified")
  where
    greaterThanOneParser str =
        let validator v
                | v >= 2 = Right v
                | otherwise = Left $ unwords
                    [ "Negative number for", str <> ":", show v ]
        in  O.eitherReader (Read.readEither >=> validator)

cmdP :: O.Parser Cmd
cmdP = O.subparser $ mconcat
    [ O.command "setup" $ O.info
        (CmdSetup <$ O.helper)
        (O.progDesc "Setup benchmark by generating required tables")
    , O.command "collisions" $ O.info
        (CmdCollisions <$ O.helper)
        (O.progDesc "Collision analysis, compute theoretical and actual shared keys between tables")

    , O.command "run" $ O.info
        (CmdRun <$> runOptsP <**> O.helper)
        (O.progDesc "Proper run, measuring performance and generating a benchmark report")
    ]

runOptsP :: O.Parser RunOpts
runOptsP = pure RunOpts
    <*> O.option O.auto (O.long "batch-count" <> O.value 200 <> O.showDefault <> O.help "Batch count")
    <*> O.option O.auto (O.long "batch-size" <> O.value 256 <> O.showDefault <> O.help "Batch size")
    <*> O.switch (O.long "check" <> O.help "Check generated key distribution")

-------------------------------------------------------------------------------
-- measurements
-------------------------------------------------------------------------------

timed :: IO a -> IO (a, Integer)
timed action = do
    performMajorGC
    t1 <- Clock.getTime Clock.Monotonic
    x  <- action
    t2 <- Clock.getTime Clock.Monotonic
    performMajorGC
    let !ms = Clock.toNanoSecs (Clock.diffTimeSpec t2 t1) `div` 1_000_000
    return (x, ms)

-- | The 'Integer' is the number of /milliseconds/ elapsed.
timed_ :: IO () -> IO Integer
timed_ action = do
    ((), t) <- timed action
    pure t

-------------------------------------------------------------------------------
-- setup
-------------------------------------------------------------------------------

doSetup :: GlobalOpts -> IO ()
doSetup gopts = do
    void $ timed_ $ doSetup' gopts

doSetup' :: GlobalOpts -> IO ()
doSetup' gopts = do
    -- Define some constants
    let populationBatchSize = 256
        entryCount = initialSize gopts
        -- The key size is twice the specified size because we will delete half
        -- of the keys in the domain of each table uniformly at random.
        keyMax = 2 * entryCount
        keyMin = 1
        tableIDs = tableRange gopts

    -- Setup RNG
    initializeRNG gopts
    tableRNGs <- deriveMultipleRNGs $ length tableIDs

    -- Ensure that our mount point exists on the real file system
    createDirectoryIfMissing True $ rootDir gopts

    -- Populate the specified number of tables
    LSM.withSession (rootDir gopts) $ \session -> do
      -- Create a "baseline" table
      --
      -- We create a single table that *already has* all the same key value pairs
      -- which exist in the union of all tables *which are going* to be unioned.
      -- This way we can compare performance of the union of tables to a
      -- "baseline" table with since both share all the same key value pairs.
      table_0 <- LSM.newTable @K @V session

      forConcurrently_ (NE.zip tableIDs tableRNGs) $ \(tID, tRNG) -> do
        -- Create a new table
        table_n <- LSM.newTable @K @V session
        -- Populate the table in batches
        forM_ (groupsOfN populationBatchSize [ keyMin .. keyMax ]) $ \batch -> do
            let prunedBatch =
                    Random.sampleUniformWithoutReplacement
                       tRNG entryCount $ NE.toList batch
            let keyInserts = V.fromList [
                    (makeKey (fromIntegral k), theValue)
                  | k <- prunedBatch
                  ]
            -- Insert the batch of the randomly selected keys
            -- into both the baseline table (0) and the current table
            LSM.inserts table_0 keyInserts
            LSM.inserts table_n keyInserts
        LSM.saveSnapshot (makeTableName tID) label table_n

      -- Finally, save the baseline table
      LSM.saveSnapshot baselineTableName label table_0

makeTableName :: Show a => a -> LSM.SnapshotName
makeTableName n = LSM.toSnapshotName $ "bench_" <> show n

tableRange :: GlobalOpts -> NonEmpty Int
tableRange gopts =
    let n1 = succ baselineTableID
        n2 = succ n1
    in  n1 :| [ n2 .. tableCount gopts + baselineTableID ]

-------------------------------------------------------------------------------
-- Collision analysis
-------------------------------------------------------------------------------

-- |
-- Calculate the expected and actual number of collisions occuring in the tables
-- to be unioned.
doCollisionAnalysis :: GlobalOpts -> IO ()
doCollisionAnalysis gopts = do
    -- Calculated some expected statistics for generated batches
    do
       let d = toInteger $ tableCount gopts
       -- Consider the case that we generate @n@ random numbers in the range @[ 1 .. d ]@.
       -- What is the chance they are all distinct?
       -- This scenario is an example of the well known Birthday Problem.
       --
       -- Source:
       --     https://en.wikipedia.org/wiki/Birthday_problem
       --
       -- Luckily, this scenario is also what occurs when we insert the entries into
       -- the tables to be unioned and want to know how likely it is that one table
       -- possibly shares a key in another table. We have each key amongst the many tables
       -- corresponding to a person and each table corresponding to a day. The problem
       -- becomes which keys share a table. This is a sound a correspondance because
       -- no individual table has duplicate keys.
       --
       let n = toInteger $ initialSize gopts * tableCount gopts

       -- We want to compute the number of keys shared amongst the tables.
       -- This is equivelant to computing the number of people with a shared birthday.
       --
       -- Source:
       --     https://en.wikipedia.org/wiki/Birthday_problem#Number_of_people_with_a_shared_birthday
       let q = (n % 1) * (1 - ((d - 1) % d) ^ (n - 1))

       printf "Expected number of duplicates (extreme upper bound): %9s out of %d\n" (renderRational 5 q) n

    -- Next we inspect the generated tables and count the /actual/ number of shared keys.
    let keyRange :: LSM.Range K
        keyRange = LSM.FromToIncluding (makeKey minBound) (makeKey maxBound)

    duplicates <- LSM.withSession (rootDir gopts) $ \session -> do
      tKeySets <- forM (tableRange gopts) $ \tID -> do
        let name = makeTableName tID
        table <- LSM.openTableFromSnapshot session name label
        (vKeys :: V.Vector (K, V)) <- LSM.rangeLookup table keyRange
        pure . Set.fromList $ fst <$> V.toList vKeys

      let gatherUniques tKetSet = foldr Set.difference tKetSet tKeySets
          unions = Set.unions tKeySets
          uniques = Fold.foldl' (\x -> Set.union (gatherUniques x)) mempty tKeySets
      pure $ unions `Set.difference` uniques

    printf "True duplicates: %d\n" $ Set.size duplicates

-- |
-- From StackOverflow: https://stackoverflow.com/a/30938328
renderRational :: Int -> Rational -> String
renderRational len rat = sign <> shows prefix ("." ++ suffix)
    where
      sign
        | num < 0 = "-"
        | otherwise = ""

      (prefix, next) = abs num `quotRem` den

      suffix = case next of
        0 -> "0"
        n -> take len $ go n

      num = numerator rat
      den = denominator rat
      go 0 = ""
      go x = let (d', next') = (10 * x) `quotRem` den
             in shows d' (go next')

-------------------------------------------------------------------------------
-- run
-------------------------------------------------------------------------------

doRun :: GlobalOpts -> RunOpts -> IO ()
doRun gopts opts = do
    -- Record the time the benchmark was initiated as a @String@.
    benchStartTime <- Time.formatTime Time.defaultTimeLocale "%F-%T" <$> Time.getZonedTime

    -- Setup RNG for randomized lookups
    initializeRNG gopts

    -- Perform 3 measurement phases
    --   * Phase 1: Measure performance before supplying any credits.
    --   * Phase 2: Measure performance as credits are incrementally supplied and debt is repaid.
    --   * Phase 3: Measure performance when debt is 0.
    let tickCountPrefix = 50
        tickCountMiddle = 100
        tickCountSuffix = 50
        tickCountEnding = maximum indicesPhase3
        samplingLookups = batchSizeOfSample opts
        samplesEachTick = batchSamplesPerTick opts
        queriesEachTick = samplesEachTick * samplingLookups
        indicesPhase1 = [ negate tickCountPrefix .. 0 ]
        indicesPhase2 = [ 1 .. tickCountMiddle ]
        indicesPhase3 = [ tickCountMiddle + 1 .. tickCountMiddle + tickCountSuffix ]
        indicesDomain = indicesPhase1 <> indicesPhase2 <> indicesPhase3
    
    measurements <- LSM.withSession (rootDir gopts) $ \session -> do
        -- Load the baseline table
        table_0 <- LSM.openTableFromSnapshot session baselineTableName label

        -- Load the union tables
        tables <- forM (tableRange gopts) $ \tID ->
            LSM.openTableFromSnapshot session (makeTableName tID) label

        LSM.withIncrementalUnions tables $ \table -> do
          LSM.UnionDebt totalDebt <- LSM.remainingUnionDebt table
          -- Determine the number of credits to supply per tick in order to
          -- all debt repaid at the time specified by the rpayment rate.
          -- Each tick should supply credits equal to:
          --     paymentRate * totalDebt / tickCountMiddle
          let paymentPerTick = ceiling $ toInteger totalDebt % tickCountMiddle
          let measurePerformance :: Integer -> IO (Int, Int, TimingResult, TimingResult)
              measurePerformance tickIndex = do
                -- Note this tick's debt for subsequent measurement purposes.
                LSM.UnionDebt debtCurr <- LSM.remainingUnionDebt table
                -- Note the cumulative credits supplied through this tick.
                let paidCurr = max 0 $ totalDebt - fromInteger (max 0 tickIndex) * paymentPerTick
                -- Clone the RNG to supply to both tables
                -- First split off a fresh, local RNG from the system-level RNG.
                rng0:|_ <- deriveMultipleRNGs 1
                let -- Extract the seed from the fresh RNG
                    sVal = Random.toSeed rng0
                    -- Create a second, duplicate RNG by using
                    -- the indentical seed from the first RNG
                    rng1 = Random.fromSeed sVal
                -- Generate the randomized lookup batches.
                -- There are two important aspects to consider when preparing for the measurements.
                --   1. Generate all the lookup keys *before* starting the measurement!
                --      This ensures that the benchmark does not include RNG latency in the results.
                --   2. Use these exact same lookups for both the baseline table and the unioned table.
                --      This ensures that the comparison between a monolithic table and union of tables
                --      are appropriately comparable.                
                let thisMeasurement =
                      measureSampleSeriesUsingRNG samplesEachTick samplingLookups $ initialSize gopts
                -- Perform measurement of batched lookups
                -- First, benchmark the baseline table
                base <- thisMeasurement rng0 table_0
                -- Next, benchmark the union table using the cloned RNG
                time@(timeMean, _, _, _) <- thisMeasurement rng1 table
                -- Save the result for later to be included in the performance plot
                let rate :: Double
                    rate = fromRational $ fromIntegral queriesEachTick * 1_000 % timeMean
                -- Print a status report while running the benchmark
                printf
                  (Fold.fold [
                    "    [%",
                    show . length $ show tickCountEnding,
                    "d/",
                    show tickCountEnding,
                    "]:    %9.01f ops/sec",
                    "    with debt = %8d\n"
                  ])
                  tickIndex
                  rate
                  debtCurr
                pure (debtCurr, paidCurr, base, time)

          -- Phase 1 measurements: Debt = 100%
          resultsPhase1 <- forM indicesPhase1 $ \step -> do
            measurePerformance step

          -- Phase 2 measurements: Debt ∈ [0%, 99%]
          resultsPhase2 <- forM indicesPhase2 $ \step -> do
            measurePerformance step

          -- Phase 3 measurements: Debt = 0%
          resultsPhase3 <- forM indicesPhase3 $ \step -> do
            measurePerformance step

          pure $ mconcat [ resultsPhase1, resultsPhase2, resultsPhase3 ]

    let (balances', payments', baseline, operations) = List.unzip4 measurements
        maxValue = maximum $ getMeanTiming <$> operations
        standardize xs =
          let maxInput = toInteger $ maximum xs
              scale :: Integral i=> i -> Integer
              scale x = ceiling $ (fromIntegral x * maxValue) % maxInput
          in  scale <$> xs
        balances = standardize balances'
        payments = standardize payments'
        noDebtIndex = fst . head . dropWhile ((> 0) . snd) $ zip indicesDomain balances

        getMeanTiming (x,_,_,_) = x

        labelX = "Percent of debt repaid: \"clamped\" to range [0%, 100%]"
        labelY = "Time to perform " <> sep1000th ',' samplingLookups <> " lookups (ms)"
        labelP = unwords
          [ "Measuring Incremental Union of"
          , show $ tableCount gopts
          , "Tables Containing"
          , sep1000th ',' $ initialSize gopts
          , "Entries Each"
          ]

    -- Generate a performance plot based on the benchmark results.
    Plot.toFile Plot.def (rootDir gopts <> "/" <> deriveFileNameForPlot gopts benchStartTime) $ do
      let colorD = Color.sRGB 1.000 0.625 0.5 `Plot.withOpacity` 0.500
      let colorE = Color.sRGB 0.875 1.000 0.125 `Plot.withOpacity` 0.625

      Plot.layout_x_axis . Plot.laxis_override .= Plot.axisGridHide
      Plot.layout_x_axis . Plot.laxis_title    .= labelX
      Plot.layout_y_axis . Plot.laxis_title    .= labelY
      Plot.layout_title  .= labelP
      Plot.layout_margin .= 30
      Plot.layout_legend . Plot._Just . Plot.legend_margin .= 30

      Plot.setColors $ Plot.opaque <$> [ Color.royalblue ]
      Plot.plot $ Plot.line "Timing of Baseline Table (identical table without unions)"
        [ zip indicesDomain $ getMeanTiming <$> baseline ]

      Plot.setColors $ Plot.opaque <$> [ Color.red ]
      Plot.plot $ "Start Supplying Credits" `plotAsymptoteAt` (000 :: Integer)

      Plot.setColors $ Plot.opaque <$> [ Color.green ]
      Plot.plot $ "Debt Fully Repaid" `plotAsymptoteAt` noDebtIndex

      Plot.plot $ fillBetween colorD "Debt Balance Repaid (%)"
        [ (d, (0,v)) | (d, v) <- zip indicesDomain balances ]

      when (noDebtIndex /= 100) $ do
        Plot.setColors $ Plot.opaque <$> [ Color.blue ]
        Plot.plot $ "All Credits Supplied" `plotAsymptoteAt` (100 :: Integer)

      let notAllEqual = any (uncurry (/=)) $ zip balances payments
      when notAllEqual $ do
        Plot.plot $ fillBetween colorE "Excess Credits (%)"
          [ (d, (v,w)) | (d, v, w) <- zip3 indicesDomain balances payments ]

      Plot.setColors $ Plot.opaque <$> [ Color.black ]
      Plot.plot $ Plot.line "Timing of Unioned Table"
        [ zip indicesDomain $ getMeanTiming <$> operations ]

-------------------------------------------------------------------------------
-- Plotting results
-------------------------------------------------------------------------------

plotAsymptoteAt :: (Integral i, Num x) => String -> i -> Plot.EC l20 (Plot.PlotLines x y)
plotAsymptoteAt str i = do
      let x = makeValue $ fromIntegral i
      vLines <- Plot.line str [[]]
      pure $ vLines { Plot._plot_lines_limit_values = [[(x, Plot.LMin), (x, Plot.LMax)]] }

makeValue :: a -> Plot.Limit a
makeValue x = Plot.LValue x

fillBetween :: Plot.AlphaColour Double -> String -> [(x, (y, y))] -> Plot.EC l20 (Plot.PlotFillBetween x y)
fillBetween color title vs = Plot.liftEC $ do
  Plot.plot_fillbetween_title .= title
  Plot.plot_fillbetween_style .= Plot.solidFillStyle color
  Plot.plot_fillbetween_values .= vs

deriveFileNameForPlot :: GlobalOpts -> String -> FilePath
deriveFileNameForPlot gOpts benchStartTime =
    let partTable = show $ tableCount gOpts
        partWidth = sep1000th '_' $ initialSize gOpts
        partLabel = case seed gOpts of
            -- Without an explicit seed, label with the system time
            Nothing          -> "TIME_" <> benchStartTime
            -- When run deterministically with a given seed, record the seed as the label
            Just explictSeed -> printf "SEED_%016x" explictSeed
    in  List.intercalate "-"
          [ "benchmark"
          , partTable <> "x" <> partWidth
          , partLabel
          ] <> ".png"

sep1000th :: Integral i => Char -> i -> String
sep1000th sep = reverse . List.intercalate [sep] . fmap Fold.toList . groupsOfN 3 . reverse . show . toInteger

-------------------------------------------------------------------------------
-- Sampling generation & measurements
-------------------------------------------------------------------------------

-- | The tuple of measured timings over a sample series @(mean, min, max, variance)@.
type TimingResult = (Integer, Integer, Integer, Integer)

-- | A single batch to be sampled as part of a sampling series for a measurement.
{-# INLINE generateBatchSampling #-}
generateBatchSampling  ::
       Int       -- ^ initial size of the collection
    -> Int       -- ^ batch size
    -> IO (V.Vector K)
generateBatchSampling initialSize batchSize = toOperations <$> V.replicateM batchSize randomKey
  where
    randomKey :: IO Word64
    randomKey = Random.randomRIO (0, 2 * fromIntegral initialSize - 1)

-- | Generate operation inputs
{-# INLINE toOperations #-}
toOperations :: V.Vector Word64 -> V.Vector K
toOperations lookups = batch1
  where
    batch1 :: V.Vector K
    batch1 = V.map makeKey lookups

-- | Measure a series of of batched lookups.
measureUsingSampleSeries ::
       V.Vector (V.Vector K)
    -> LSM.Table K V
    -> IO TimingResult
measureUsingSampleSeries sampleSeries table =
    forM sampleSeries (measureBatchedLookup table) <&> \sampledTimings ->
        let !popSize = toInteger $ V.length sampleSeries
            !timingMean = round $ sum sampledTimings % popSize
            !timingMax  = maximum sampledTimings
            !timingMin  = minimum sampledTimings
            !timingVariance =
                let diffMeanSquared =
                        (\x -> let diff = (x - timingMean) in diff * diff) <$> sampledTimings
                in  round $ sum diffMeanSquared % popSize
        in  (timingMean, timingMax, timingMin, timingVariance)

-- | Measure a series of of batched lookups.
measureSampleSeriesUsingRNG ::
       Int       -- ^ Number of batches
    -> Int       -- ^ batch size
    -> Int       -- ^ initial size of the collection
    -> Random.StdGen -- ^ RNG
    -> LSM.Table K V
    -> IO TimingResult
measureSampleSeriesUsingRNG samplesEachTick samplingSize tableSize rng table =
    let -- | Sample a single batch of randomly generated keys
        sampler :: Random.StdGen -> Int -> IO (Integer, Random.StdGen)
        sampler gen = const $ measureSamplingUsingRNG samplingSize tableSize table gen
    in  traverseWithRNG rng sampler [1 .. samplesEachTick] <&> \(sampledTimings, _) ->
            let !popSize = toInteger samplesEachTick
                !timingMean = round $ sum sampledTimings % popSize
                !timingMax  = maximum sampledTimings
                !timingMin  = minimum sampledTimings
                !timingVariance =
                    let diffMeanSquared =
                            (\x -> let diff = (x - timingMean) in diff * diff) <$> sampledTimings
                    in  round $ sum diffMeanSquared % popSize
            in  (timingMean, timingMax, timingMin, timingVariance)

-- | A single batch to be sampled as part of a sampling series for a measurement.
{-# INLINE measureSamplingUsingRNG #-}
measureSamplingUsingRNG ::
       Int -- ^ Size of batch
    -> Int -- ^ Size of table
    -> LSM.Table K V
    -> Random.StdGen
    -> IO (Integer, Random.StdGen)
measureSamplingUsingRNG batchSize tableSize table rng =
    let randomKey :: Monad m => Random.StdGen -> () -> m (Word64, Random.StdGen)
        randomKey g _ = pure $ Random.randomR (0, 2 * fromIntegral tableSize - 1) g
        (keyBatch, nextRNG) = runIdentity . traverseWithRNG rng (randomKey) $ V.replicate batchSize ()
    in  do  result <- measureBatchedLookup table $ toOperations keyBatch
            pure (result, nextRNG)
    
-- | Measure the time to perform the batch of lookups on the supplied table.
{-# INLINE measureBatchedLookup #-}
measureBatchedLookup :: LSM.Table K V -> V.Vector K -> IO Integer
measureBatchedLookup table = timed_ . void . LSM.lookups table

-------------------------------------------------------------------------------
-- PRNG initialisation
-------------------------------------------------------------------------------

initializeRNG :: GlobalOpts -> IO ()
initializeRNG gopts =
    -- Create an RNG for randomized insertion of entries
    case seed gopts of
        -- When no seed was explicitly supplied, used system entropy.
        -- This is the default semantics of the @StdGen@ data-type,
        -- hence no action in required.
        Nothing           -> pure ()
        -- When the seed was explicitly specified by the user,
        -- overide the system RNG with the explict seed to ensure determinism.
        Just explicitSeed -> Random.setStdGen $ Random.mkStdGen explicitSeed

deriveMultipleRNGs :: Int -> IO (NonEmpty Random.StdGen)
deriveMultipleRNGs amount | amount <= 0 = fail "Must specify a POSITIVE number of RNGs to request"
deriveMultipleRNGs amount =
   let splitContinuation 0 gens done = (done, gens)
       splitContinuation n gens prev =
              let (curr, next) = Random.splitGen prev
              in  splitContinuation (n - 1) (curr <| gens) next
   in  do  rng0 <- Random.getStdGen
           let (rng1, rng2) = Random.splitGen rng0
               (rngN, splitRNGs) = splitContinuation amount (rng1 :| []) rng2
           Random.setStdGen rngN
           pure splitRNGs

traverseWithRNG :: forall a b g m t.
       ( Monad m,
         Traversable t)
    => g
    -> (g -> a -> m (b, g))
    -> t a
    -> m (t b, g)
traverseWithRNG openRNG f xs =
    let step :: a -> StateT g m b
        step x = do
            currRNG <- get
            (v, nextRNG) <- lift $ f currRNG x
            put nextRNG
            pure v
    in  runStateT (traverse step xs) openRNG    

-------------------------------------------------------------------------------
-- main
-------------------------------------------------------------------------------

main :: IO ()
main = do
    hSetBuffering stdout NoBuffering
#ifdef NO_IGNORE_ASSERTS
    putStrLn "WARNING: Benchmarking in debug mode."
    putStrLn "         To benchmark in release mode, pass:"
    putStrLn "         --project-file=cabal.project.release"
#endif
    (gopts, cmd) <- O.customExecParser prefs cliP
    print gopts
    print cmd
    case cmd of
        CmdCollisions -> doCollisionAnalysis gopts
        CmdRun opts   -> doRun gopts opts
        CmdSetup      -> doSetup gopts
  where
    cliP = O.info ((,) <$> globalOptsP <*> cmdP <**> O.helper) O.fullDesc
    prefs = O.prefs $ O.showHelpOnEmpty <> O.helpShowGlobals <> O.subparserInline
