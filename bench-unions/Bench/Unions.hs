{-# LANGUAGE CPP                      #-}
{-# LANGUAGE DuplicateRecordFields    #-}
{-# LANGUAGE NondecreasingIndentation #-}
{-# LANGUAGE OverloadedStrings        #-}

{-# OPTIONS_GHC -Wno-orphans #-}

{- | Benchmarks for table unions of an LSM tree.

Here is a summary of the table union benchmark.
Note that /all/ function calls are made through
the "Database.LSMTree.Simple" module's API.

__Phase 1: Setup__

The benchmark will setup an initial set of @--table-count@ tables
to be unioned together during "__Phase 2__."
The size of each generated table is the same
and is equal to @--initial-size@.
Each created table has @--initial-size@ insertions operations performed on it
before being written out to disk as a snapshot.
The @--initial-size@ inserted keys in each table are randomly selected
from the following range.
Each key is unique, meaning that keys are randomly sampled
from the range without replacement.

\[
\left[\quad 0,\quad 2 * initialSize \quad\right)
\]

Additionally, the directory in which to isolate the benchmark environment
is specified via the @--bench-dir@ command line option.

__Phase 2: Measurement__

When generating measurements for the table unions,
the benchmark will reload the snapshots of the tables generated
in __Phase 1__ from disk.
Subsequently, the tables will be "incrementally unioned" together.

Once the tables have been loaded and the union initiated,
serveral iterations of lookups will be performed.
One iteration involves performing a @--batch-count@ number of batches
of @--batch-size@ lookups each.
We measure the time spent on running an iteration,
and we compute how many lookups per second were performed during the iteration.

First, 50 iterations are performed /without/ supplying any credits to the unioned table.
This establishes a base-line performance picture.
Iterations \( \left[ 0, 50 \right) \) measure lookups per seconds
on the unioned table with \(100\%\) of the debt remaining.

Subsequently, 100 more iterations are performed.
Before each of these iterations,
a fixed number of credits are supplied to the incremental union table.
The series of measurements allows reasoning about table performance over time
as the tables debt decreases (at a uniform rate).
The number of credits supplied before each iteration is
\(1%\) of the total starting debt.
After 100 steps, \(100\%\) of the debt will be paid off.
Iterations \( \left[ 50, 100 \right) \) measure lookups per second
on the unioned table as the remaining debt decreases.

Finally, 50 concluding iterations are performed.
Since no debt is remaining, no credits are supplied.
Rather, these measurements create a "post-payoff" performance picture.
Iterations \( \left[ 150, 200 \right) \) measure lookups per seconds
on to the unioned table with \(0\%\) of the debt remaining.

__Results__

An informative gnuplot script and data file of the benchmark measurements is
generated and placed in the @bench-unions@ directory.
Run the following command in a shell to generate a PNG of the graph.

@
  cd bench-unions && gnuplot unions-bench.gnuplot && cd ..
@

TODO: explain the baseline table

TODO: explain the seed

TODO: explain collisions analysis
-}
module Bench.Unions (main) where

import           Control.Applicative ((<**>))
import           Control.Concurrent.Async (forConcurrently_)
import           Control.Monad (forM_, void, (>=>))
import           Control.Monad.State.Strict (MonadState (..), runState)
import qualified Data.ByteString.Short as BS
import           Data.Foldable (traverse_)
import           Data.IORef
import qualified Data.List as List
import           Data.List.NonEmpty (NonEmpty (..))
import qualified Data.List.NonEmpty as NE
import           Data.Monoid
import qualified Data.Primitive as P
import qualified Data.Set as Set
import qualified Data.Vector as V
import           Data.Word (Word64)
import qualified Options.Applicative as O
import           Prelude hiding (lookup)
import qualified System.Clock as Clock
import           System.Directory (createDirectoryIfMissing)
import           System.IO
import           System.Mem (performMajorGC)
import qualified System.Random as Random
import           System.Random (StdGen)
import           Text.Printf (printf)
import qualified Text.Read as Read

import           Database.LSMTree.Extras (groupsOfN)
import qualified Database.LSMTree.Extras.Random as Random
import           Database.LSMTree.Internal.ByteString (byteArrayToSBS)

import qualified Database.LSMTree.Simple as LSM

-------------------------------------------------------------------------------
-- Constant Values
-------------------------------------------------------------------------------

baselineTableID :: Int
baselineTableID = 0

baselineTableName :: LSM.SnapshotName
baselineTableName = makeTableName baselineTableID

defaultBenchDir :: FilePath
defaultBenchDir = "_bench_unions"

defaultInitialSize :: Int
defaultInitialSize = 1_000_000

defaultTableCount :: Int
defaultTableCount = 10

-- | The default seed is the first 20 digits of Pi.
defaultSeed :: Int
defaultSeed = 1415926535897932384

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
               pure v

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
    { rootDir     :: !FilePath -- ^ Session directory.
    , tableCount  :: !Int      -- ^ Number of  tables in the benchmark
    , initialSize :: !Int      -- ^ Initial size of each table in the benchmark
    , seed        :: !Int      -- ^ The seed for the RNG for deterministic behavior,
                               --   use system entropy when not explicitly provided.
    }
  deriving stock Show

data RunOpts = RunOpts
    { batchCount :: !Int
    , batchSize  :: !Int
    }
  deriving stock Show

data Cmd
    -- | Setup benchmark: generate initial LSM trees etc.
    = CmdSetup

    -- | Run collision analysis.
    | CmdCollisions

    -- | Run the actual benchmark
    | CmdRun RunOpts
  deriving stock Show

-------------------------------------------------------------------------------
-- command line interface
-------------------------------------------------------------------------------

globalOptsP :: O.Parser GlobalOpts
globalOptsP = pure GlobalOpts
    <*> O.option O.str (O.long "bench-dir" <> O.value defaultBenchDir <> O.showDefault <> O.help "Benchmark directory to put files in")
    <*> O.option (positiveParser "number of tables") (O.long "table-count" <> O.value defaultTableCount <> O.showDefault <> O.help "Number of tables to benchmark")
    <*> O.option (positiveParser "size of tables") (O.long "initial-size" <> O.value defaultInitialSize <> O.showDefault <> O.help "Initial size of each table")
    <*> O.option O.auto (O.long "seed" <> O.value defaultSeed <> O.showDefault <> O.help "Random seed, defaults to the first 20 digits of Pi")
  where
    positiveParser str =
        let validator v
                | v >= 1 = Right v
                | otherwise = Left $ unwords
                    [ "Non-positive number for", str <> ":", show v ]
        in  O.eitherReader $ Read.readEither >=> validator

cmdP :: O.Parser Cmd
cmdP = O.subparser $ mconcat
    [ O.command "setup" $ O.info
        (CmdSetup <$ O.helper)
        (O.progDesc "Setup benchmark by generating required tables")
    , O.command "collisions" $ O.info
        (CmdCollisions <$ O.helper)
        (O.progDesc "Collision analysis, compute shared keys between tables")
    , O.command "run" $ O.info
        (CmdRun <$> runOptsP <**> O.helper)
        (O.progDesc "Proper run, measuring performance and generating a benchmark report")
    ]

runOptsP :: O.Parser RunOpts
runOptsP = pure RunOpts
    <*> O.option O.auto (O.long "batch-count" <> O.value 200 <> O.showDefault <> O.help "Batch count")
    <*> O.option O.auto (O.long "batch-size" <> O.value 256 <> O.showDefault <> O.help "Batch size")

-------------------------------------------------------------------------------
-- measurements
-------------------------------------------------------------------------------


-- | Returns number of seconds elapsed
timed :: IO a -> IO (a, Double)
timed action = do
    performMajorGC
    t1 <- Clock.getTime Clock.Monotonic
    x  <- action
    t2 <- Clock.getTime Clock.Monotonic
    performMajorGC
    let !t = fromIntegral (Clock.toNanoSecs (Clock.diffTimeSpec t2 t1)) * 1e-9
    pure (x, t)

-- | Returns number of seconds elapsed
timed_ :: IO () -> IO Double
timed_ action = do
    ((), t) <- timed action
    pure t

-------------------------------------------------------------------------------
-- Setup
-------------------------------------------------------------------------------

doSetup :: GlobalOpts -> IO ()
doSetup gopts = do
    -- Define some constants
    let populationBatchSize = 256
        entryCount = initialSize gopts
        -- The key size is twice the specified size because we will delete half
        -- of the keys in the domain of each table uniformly at random.
        keyMax = 2 * entryCount - 1
        keyMin = 0
        tableIDs = tableRange gopts

    -- Setup RNG
    let tableRNGs = deriveSetupRNGs gopts $ length tableIDs

    -- Ensure that our mount point exists on the real file system
    createDirectoryIfMissing True $ rootDir gopts

    -- Populate the specified number of tables
    LSM.withOpenSession (rootDir gopts) $ \session -> do
      -- Create a "baseline" table
      --
      -- We create a single table that *already has* all the same key value pairs
      -- which exist in the union of all tables *which are going* to be unioned.
      -- This way we can compare performance of the union of tables to a
      -- "baseline" table since both share all the same key value pairs.
      table_0 <- LSM.newTable @K @V session

      forConcurrently_ (NE.zip tableIDs tableRNGs) $ \(tID, tRNG) -> do
        -- Create a new table
        table_n <- LSM.newTable @K @V session
        -- Populate the table in batches
        forM_ (groupsOfN (populationBatchSize * 2) [ keyMin .. keyMax ]) $ \batch -> do
            let prunedBatch =
                    Random.sampleUniformWithoutReplacement
                       tRNG (NE.length batch `div` 2) $ NE.toList batch
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
    in  NE.fromList [ n1 .. tableCount gopts + baselineTableID ]

-------------------------------------------------------------------------------
-- Collision analysis
-------------------------------------------------------------------------------

-- | Count duplicate keys in all tables that will be unioned together
doCollisionAnalysis :: GlobalOpts -> IO ()
doCollisionAnalysis gopts = do
    LSM.withOpenSession (rootDir gopts) $ \session -> do
      seenRef <- newIORef Set.empty
      dupRef <- newIORef Set.empty

      forM_ (tableRange gopts) $ \tID -> do
        let name = makeTableName tID
        LSM.withTableFromSnapshot session name label $ \(table :: LSM.Table K V) -> do
          LSM.withCursor table $ \cursor -> do
            streamCursor cursor $ \(k, _) -> do
              seen <- readIORef seenRef
              if Set.member k seen then
                modifyIORef dupRef $ Set.insert k
              else
                modifyIORef seenRef $ Set.insert k

      seen <- readIORef seenRef
      dups <- readIORef dupRef
      printf "Keys seen at least once: %d\n" $ Set.size seen
      printf "Keys seen at least twice: %d\n" $ Set.size dups

streamCursor :: LSM.Cursor K V -> ((K, V) -> IO ()) -> IO ()
streamCursor cursor f = go
  where
    go = LSM.next cursor >>= \case
          Nothing -> pure ()
          Just kv -> f kv >> go

-------------------------------------------------------------------------------
-- run
-------------------------------------------------------------------------------

doRun :: GlobalOpts -> RunOpts -> IO ()
doRun gopts opts = do
    -- Perform 3 measurement phases
    --   * Phase 1: Measure performance before supplying any credits.
    --   * Phase 2: Measure performance as credits are incrementally supplied and debt is repaid.
    --   * Phase 3: Measure performance when debt is 0.

    let rng = deriveRunRNG gopts
        dataPath = "bench-unions/unions-bench.dat"

    withFile dataPath WriteMode $ \h -> do
    hPutStrLn h "# iteration \t baseline (ops/sec) \t union (ops/sec) \t union debt"

    LSM.withOpenSession (rootDir gopts) $ \session -> do
    -- Load the baseline table
    LSM.withTableFromSnapshot session baselineTableName label
      $ \baselineTable -> do
    -- Load the union tables
    withTablesFromSnapshots session label (makeTableName <$> tableRange gopts)
      $ \inputTables -> do
    -- Start the incremental union
    LSM.withIncrementalUnions inputTables $ \unionedTable -> do
      let measurePerformance :: Int -> Maybe LSM.UnionCredits -> IO ()
          measurePerformance iteration mayCredits = do
            LSM.supplyUnionCredits unionedTable `traverse_` mayCredits
            LSM.UnionDebt currDebt <- LSM.remainingUnionDebt unionedTable

            baselineOpsSec <- timeOpsPerSecond gopts opts baselineTable rng
            unionOpsSec <- timeOpsPerSecond gopts opts unionedTable rng

            printf "iteration: %d, baseline: %7.01f ops/sec, union: %7.01f ops/sec, debt: %d\n"
              iteration baselineOpsSec unionOpsSec currDebt

            hPutStrLn h $ unwords [ show iteration, show baselineOpsSec
                                  , show unionOpsSec, show currDebt ]

      LSM.UnionDebt totalDebt <- LSM.remainingUnionDebt unionedTable

      -- Phase 1 measurements: Debt = 100%
      forM_ [0..50-1] $ \step -> do
        measurePerformance step Nothing

      -- Phase 2 measurements: Debt ∈ [0%, 99%]
      forM_ [50..150-1] $ \step -> do
        let creditsPerIteration = LSM.UnionCredits ((totalDebt + 99) `div` 100)
        measurePerformance step (Just creditsPerIteration)

      -- Phase 3 measurements: Debt = 0%
      forM_ [150..200-1] $ \step -> do
        measurePerformance step Nothing

-- | Exception-safe opening of multiple snapshots
withTablesFromSnapshots ::
     LSM.Session
  -> LSM.SnapshotLabel
  -> (NonEmpty LSM.SnapshotName)
  -> (NonEmpty (LSM.Table K V) -> IO a)
  -> IO a
withTablesFromSnapshots session snapLabel (x0 :| xs0) k = do
    LSM.withTableFromSnapshot session x0 snapLabel $ \t0 -> go' (NE.singleton t0) xs0
  where
    go' acc []     = k (NE.reverse acc)
    go' acc (x:xs) =
        LSM.withTableFromSnapshot session x snapLabel $ \t ->
          go' (t NE.<| acc) xs

-- | Returns operations per second
timeOpsPerSecond :: GlobalOpts -> RunOpts -> LSM.Table K V -> StdGen -> IO Double
timeOpsPerSecond gopts opts table g = do
    t <- timed_ $
            sequentialIterations
                (initialSize gopts) (batchSize opts) (batchCount opts) table g

    let ops = batchCount opts * batchSize opts
        opsPerSec = fromIntegral ops / t

    pure opsPerSec

-------------------------------------------------------------------------------
-- PRNG initialisation
-------------------------------------------------------------------------------

deriveSetupRNGs :: GlobalOpts -> Int -> NonEmpty Random.StdGen
deriveSetupRNGs gOpts amount =
    let  -- g1 is reserved for the run command
        (_g1, !g2) = deriveInitialForkedRNGs gOpts
    in  NE.fromList $ take amount $ List.unfoldr (Just . Random.splitGen) g2

deriveRunRNG :: GlobalOpts -> Random.StdGen
deriveRunRNG gOpts =
    let  -- g2 and its splits are reserved for the setup command
        (!g1, _g2) = deriveInitialForkedRNGs gOpts
    in  g1

deriveInitialForkedRNGs :: GlobalOpts -> (Random.StdGen, Random.StdGen)
deriveInitialForkedRNGs = Random.splitGen . Random.mkStdGen . seed

-------------------------------------------------------------------------------
-- Batch generation
-------------------------------------------------------------------------------

generateBatch  ::
       Int       -- ^ initial size of the table
    -> Int       -- ^ batch size
    -> StdGen
    -> (StdGen, V.Vector K)
generateBatch initialSize batchSize g =
    (g', V.map makeKey lookups)
  where
    (!g', lookups) = generateBatch' initialSize batchSize g

{-# INLINE generateBatch' #-}
generateBatch' ::
       Int       -- ^ initial size of the table
    -> Int       -- ^ batch size
    -> StdGen
    -> (StdGen, V.Vector Word64)
generateBatch' initialSize batchSize g = (g', lookups)
  where
    randomKey :: StdGen -> (Word64, StdGen)
    randomKey = Random.uniformR (0, 2 * fromIntegral initialSize - 1)

    lookups :: V.Vector Word64
    (lookups, !g') =
       runState (V.replicateM batchSize (state randomKey)) g

-------------------------------------------------------------------------------
-- sequential
-------------------------------------------------------------------------------

{-# INLINE sequentialIteration #-}
sequentialIteration ::
     Int -- ^ initial size of the table
  -> Int -- ^ batch size
  -> LSM.Table K V
  -> StdGen
  -> IO StdGen
sequentialIteration !initialSize !batchSize !tbl !g = do
    let (!g', ls) = generateBatch initialSize batchSize g

    -- lookups
    !_ <- LSM.lookups tbl ls

    -- continue to the next batch
    pure g'

sequentialIterations ::
     Int -- ^ initial size of the table
  -> Int -- ^ batch size
  -> Int -- ^ batch count
  -> LSM.Table K V
  -> StdGen
  -> IO ()
sequentialIterations !initialSize !batchSize !batchCount !tbl !g0 = do
    void $ forFoldM_ g0 [ 0 .. batchCount - 1 ] $ \_b g ->
      sequentialIteration initialSize batchSize tbl g

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

-------------------------------------------------------------------------------
-- general utils
-------------------------------------------------------------------------------

forFoldM_ :: Monad m => s -> [a] -> (a -> s -> m s) -> m s
forFoldM_ !s0 xs0 f = go s0 xs0
  where
    go !s []     = pure s
    go !s (x:xs) = do
      !s' <- f x s
      go s' xs
