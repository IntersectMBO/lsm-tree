{-# LANGUAGE CPP                   #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# OPTIONS_GHC -Wno-orphans #-}

{- Benchmark requirements:

A. The benchmark should use the external interface of the disk backend,
   and no internal interfaces.
B. The benchmark should use a workload ratio of 1 insert, to 1 delete,
   to 1 lookup. This is the workload ratio for the UTxO. Thus the
   performance is to be evaluated on the combination of the operations,
   not on operations individually.
C. The benchmark should use 34 byte keys, and 60 byte values. This
   corresponds roughly to the UTxO.
D. The benchmark should use keys that are evenly spread through the
   key space, such as cryptographic hashes.
E. The benchmark should start with a table of 100 million entries.
   This corresponds to the stretch target for the UTxO size.
   This table may be pre-generated. The time to construct the table
   should not be included in the benchmark time.
F. The benchmark workload should ensure that all inserts are for
   `fresh' keys, and that all lookups and deletes are for keys that
   are present. This is the typical workload for the UTxO.
G. It is acceptable to pre-generate the sequence of operations for the
   benchmark, or to take any other measure to exclude the cost of
   generating or reading the sequence of operations.
H. The benchmark should use the external interface of the disk backend
   to present batches of operations: a first batch consisting of 256
   lookups, followed by a second batch consisting of 256 inserts plus
   256 deletes. This corresponds to the UTxO workload using 64kb
   blocks, with 512 byte txs with 2 inputs and 2 outputs.
I. The benchmark should be able to run in two modes, using the
   external interface of the disk backend in two ways: serially (in
   batches), or fully pipelined (in batches).
-}
module Main (main) where

import           Control.Applicative ((<**>))
import           Control.Concurrent (getNumCapabilities)
import           Control.Concurrent.Async
import           Control.Concurrent.MVar
import           Control.Monad (forM, forM_, void, when)
import           Control.Monad.Loops (whileM_)
import           Control.Monad.ST (ST, runST)
import           Control.Monad.Trans.State.Strict (runState, state)
import qualified Data.ByteString.Short as BS
import qualified Data.Colour.Names as Color
import qualified Data.Colour.SRGB as Color
import qualified Data.Foldable as Fold
import qualified Data.IntSet as ISet
import           Data.IORef
import qualified Data.List as List
import           Data.List.NonEmpty (NonEmpty (..))
import qualified Data.List.NonEmpty as NE
import           Data.Maybe (fromMaybe)
import           Data.Monoid
import qualified Data.Primitive as P
import           Data.Ratio
import qualified Data.Set as Set
import           Data.STRef
import           Data.Tuple (swap)
import qualified Data.Vector as V
import           Data.Word (Word32, Word64)
import qualified GHC.Stats as GHC
import qualified Graphics.Rendering.Chart.Backend.Diagrams as Plot
import           Graphics.Rendering.Chart.Easy ((.=))
import qualified Graphics.Rendering.Chart.Easy as Plot
import           Math.Combinatorics.Exact.Factorial (factorial)
import qualified MCG
import qualified Options.Applicative as O
import           Prelude hiding (lookup)
import qualified System.Clock as Clock
import           System.Directory (createDirectoryIfMissing)
import           System.IO
import           System.Mem (performMajorGC)
import qualified System.Random as Random
import           Text.Printf (printf)

import           Database.LSMTree.Extras (groupsOfN)
import           Database.LSMTree.Internal.ByteString (byteArrayToSBS)

-- We should be able to write this benchmark
-- using only use public lsm-tree interface
import qualified Database.LSMTree.Simple as LSM

-------------------------------------------------------------------------------
-- Constant Values
-------------------------------------------------------------------------------

benchPerformanceOf :: FilePath
benchPerformanceOf = "union"

benchWorkProductNo :: FilePath
benchWorkProductNo = "wp16"

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
    { rootDir     :: !FilePath  -- ^ session directory.
    , tableCount  :: !Int -- ^ Number of  tables in the benchmark
    , initialSize :: !Int
    , seed        :: !Word64
    }
  deriving stock Show

data RunOpts = RunOpts
    { batchCount :: !Int
    , batchSize  :: !Int
    , check      :: !Bool
    , pipelined  :: !Bool
    }
  deriving stock Show

data Cmd
    -- | Setup benchmark: generate initial LSM tree etc.
    = CmdSetup

    -- | Make a dry run, measure the overhead.
    | CmdDryRun RunOpts

    -- | Run the actual benchmark
    | CmdRun RunOpts
  deriving stock Show

-------------------------------------------------------------------------------
-- command line interface
-------------------------------------------------------------------------------

globalOptsP :: O.Parser GlobalOpts
globalOptsP = pure GlobalOpts
    <*> O.option O.str (O.long "bench-dir" <> O.value (Fold.fold ["_", benchPerformanceOf, "_", benchWorkProductNo]) <> O.showDefault <> O.help "Benchmark directory to put files in")
    <*> O.option O.auto (O.long "table-count" <> O.value 10 <> O.showDefault <> O.help "Number of tables to benchmark")
    <*> O.option O.auto (O.long "initial-size" <> O.value 1_000_000 <> O.showDefault <> O.help "Initial LSM tree size")
    <*> O.option O.auto (O.long "seed" <> O.value 1337 <> O.showDefault <> O.help "Random seed")

cmdP :: O.Parser Cmd
cmdP = O.subparser $ mconcat
    [ O.command "setup" $ O.info
        (CmdSetup <$ O.helper)
        (O.progDesc "Setup benchmark")
    , O.command "dry-run" $ O.info
        (CmdDryRun <$> runOptsP <**> O.helper)
        (O.progDesc "Dry run, measure overhead")

    , O.command "run" $ O.info
        (CmdRun <$> runOptsP <**> O.helper)
        (O.progDesc "Proper run")
    ]

runOptsP :: O.Parser RunOpts
runOptsP = pure RunOpts
    <*> O.option O.auto (O.long "batch-count" <> O.value 200 <> O.showDefault <> O.help "Batch count")
    <*> O.option O.auto (O.long "batch-size" <> O.value 256 <> O.showDefault <> O.help "Batch size")
    <*> O.switch (O.long "check" <> O.help "Check generated key distribution")
    <*> O.switch (O.long "pipelined" <> O.help "Use pipelined mode")

-------------------------------------------------------------------------------
-- measurements
-------------------------------------------------------------------------------

timed :: IO a -> IO (a, Integer, RTSStatsDiff Triple, ProcIODiff)
timed action = do
    !p1 <- getProcIO
    performMajorGC
    s1 <- GHC.getRTSStats
    t1 <- Clock.getTime Clock.Monotonic
    x  <- action
    t2 <- Clock.getTime Clock.Monotonic
    performMajorGC
    s2 <- GHC.getRTSStats
    !p2 <- getProcIO
    let !ms = Clock.toNanoSecs (Clock.diffTimeSpec t2 t1) `div` 1_000_000
        !s = s2 `diffRTSStats` s1
        !p = p2 `diffProcIO` p1
    return (x, ms, s, p)

-- | The 'Integer' is the number of /milliseconds/ elapsed.
timed_ :: IO () -> IO (Integer, RTSStatsDiff Triple, ProcIODiff)
timed_ action = do
    ((), t, sdiff, pdiff) <- timed action
    pure (t, sdiff, pdiff)

-- | This /should/ include the statistics of any child processes.
getProcIO :: IO ProcIO
getProcIO = do
    s <- readFile "/proc/self/io"
    let ss = concatMap words $ lines s
    pure $ parse ss
  where
    parse [
        "rchar:", rcharS
      , "wchar:", wcharS
      , "syscr:", syscrS
      , "syscw:", syscwS
      , "read_bytes:", read_bytesS
      , "write_bytes:", write_bytesS
      , "cancelled_write_bytes:", cancellled_write_bytesS
      ] = ProcIO {
          rchar = read rcharS
        , wchar = read wcharS
        , syscr = read syscrS
        , syscw = read syscwS
        , read_bytes = read read_bytesS
        , write_bytes = read write_bytesS
        , cancelled_write_bytes = read cancellled_write_bytesS
        }
    parse s = error $ "getProcIO: parse of /proc/self/io failed. Input is " <> show s

diffProcIO :: ProcIO -> ProcIO -> ProcIODiff
diffProcIO after before = ProcIODiff ProcIO {
      rchar = subtractOn rchar
    , wchar = subtractOn wchar
    , syscr = subtractOn syscr
    , syscw = subtractOn syscw
    , read_bytes = subtractOn read_bytes
    , write_bytes = subtractOn write_bytes
    , cancelled_write_bytes = subtractOn cancelled_write_bytes
    }
  where
    subtractOn f = f after - f before

newtype ProcIODiff = ProcIODiff ProcIO
  deriving stock Show

-- | See the @/proc/[pid]/io@ section in @man proc@
data ProcIO = ProcIO {
    rchar                 :: !Integer
  , wchar                 :: !Integer
  , syscr                 :: !Integer
  , syscw                 :: !Integer
  , read_bytes            :: !Integer
  , write_bytes           :: !Integer
  , cancelled_write_bytes :: !Integer
  }
  deriving stock Show

-- | 'diffRTSStats a b = b - a'
diffRTSStats :: GHC.RTSStats -> GHC.RTSStats -> RTSStatsDiff Triple
diffRTSStats after before = RTSStatsDiff {
      gcs = subtractOn GHC.gcs
    , major_gcs = subtractOn GHC.major_gcs
    , allocated_bytes = subtractOn GHC.allocated_bytes
    , max_live_bytes = subtractOn GHC.max_live_bytes
    , max_large_objects_bytes = subtractOn GHC.max_large_objects_bytes
    , max_compact_bytes = subtractOn GHC.max_compact_bytes
    , max_slop_bytes = subtractOn GHC.max_slop_bytes
    , max_mem_in_use_bytes = subtractOn GHC.max_mem_in_use_bytes
    , cumulative_live_bytes = subtractOn GHC.cumulative_live_bytes
    , copied_bytes = subtractOn GHC.copied_bytes
    , par_copied_bytes = subtractOn GHC.par_copied_bytes
    , cumulative_par_balanced_copied_bytes = subtractOn GHC.cumulative_par_balanced_copied_bytes
    , init_cpu_ns = subtractOn GHC.init_cpu_ns
    , init_elapsed_ns = subtractOn GHC.init_elapsed_ns
    , mutator_cpu_ns = subtractOn GHC.mutator_cpu_ns
    , mutator_elapsed_ns = subtractOn GHC.mutator_elapsed_ns
    , gc_cpu_ns = subtractOn GHC.gc_cpu_ns
    , gc_elapsed_ns = subtractOn GHC.gc_elapsed_ns
    , cpu_ns = subtractOn GHC.cpu_ns
    , elapsed_ns = subtractOn GHC.elapsed_ns
    }
  where
    subtractOn :: Num a => (GHC.RTSStats -> a) -> Triple a
    subtractOn f = Triple {before = x, after = y, difference = y - x}
      where x = f before
            y = f after

-- | A difference datatype for 'GHC.RTSStats'.
--
-- Most fields, like 'GHC.gcs' or 'GHC.cpu_ns', are an aggregate sum, and so a
-- diff can be computed by pointwise subtraction.
--
-- Others fields, like 'GHC.max_live_bytes' only record the maximum value thus
-- far seen. We report a triplet containing the maximum before and after, and
-- their difference.
data RTSStatsDiff f = RTSStatsDiff {
    gcs                                  :: !(f Word32)
  , major_gcs                            :: !(f Word32)
  , allocated_bytes                      :: !(f Word64)
  , max_live_bytes                       :: !(f Word64)
  , max_large_objects_bytes              :: !(f Word64)
  , max_compact_bytes                    :: !(f Word64)
  , max_slop_bytes                       :: !(f Word64)
  , max_mem_in_use_bytes                 :: !(f Word64)
  , cumulative_live_bytes                :: !(f Word64)
  , copied_bytes                         :: !(f Word64)
  , par_copied_bytes                     :: !(f Word64)
  , cumulative_par_balanced_copied_bytes :: !(f Word64)
  , init_cpu_ns                          :: !(f GHC.RtsTime)
  , init_elapsed_ns                      :: !(f GHC.RtsTime)
  , mutator_cpu_ns                       :: !(f GHC.RtsTime)
  , mutator_elapsed_ns                   :: !(f GHC.RtsTime)
  , gc_cpu_ns                            :: !(f GHC.RtsTime)
  , gc_elapsed_ns                        :: !(f GHC.RtsTime)
  , cpu_ns                               :: !(f GHC.RtsTime)
  , elapsed_ns                           :: !(f GHC.RtsTime)
  }

deriving stock instance Show (RTSStatsDiff Triple)

data Triple a = Triple {
    before     :: !a
  , after      :: !a
  , difference :: !a
  }
  deriving stock Show

-------------------------------------------------------------------------------
-- setup
-------------------------------------------------------------------------------

doSetup :: GlobalOpts -> IO ()
doSetup gopts = do
    void $ timed_ $ doSetup' gopts

doSetup' :: GlobalOpts -> IO ()
doSetup' gopts = do
    let rooting :: FilePath
        rooting = rootDir gopts

    -- Ensure that our mount point exists on the real file system
    createDirectoryIfMissing True rooting

    -- Define some constants
    let populationBatchSize = 256
        -- The key size is twice the specified size because we will delete half
        -- of the keys in the domain of each table uniformly at random.
        keyMax = 2 * initialSize gopts
        keyMin = 1

    -- Create an RNG for randomized deletions
    refRNG <- newIORef $ MCG.make
        (toEnum (2 * populationBatchSize))
        (seed gopts)

    -- Populate the specified number of tables
    LSM.withSession (rootDir gopts) $ \session -> do
      -- Create a baseline table
      table_0 <- LSM.newTable @K @V session

      forM_ (tableRange gopts) $ \tID -> do
        -- Create a new table
        table_n <- LSM.newTable @K @V session
        -- Populate the table in batches
        forM_ (groupsOfN populationBatchSize [ keyMin .. keyMax ]) $ \batch -> do
            currRNG <- readIORef refRNG
            let (nextRNG, prunedBatch) = randomlyPruneHalf currRNG $ NE.toList batch
            writeIORef refRNG nextRNG
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

randomlyPruneHalf :: forall a. MCG.MCG -> [a] -> (MCG.MCG, [a])
randomlyPruneHalf !prevRNG xs =
    let !n = length xs
        modulus = toEnum n
        required = n `div` 2
        notRemoved (k,_) = k `ISet.notMember` removeIndices
        removeSelected ys = fmap snd . filter notRemoved $ zip [0 ..] ys
        gatherIndices :: forall s. ST s (MCG.MCG, ISet.IntSet)
        gatherIndices = do
            refRNG <- newSTRef prevRNG
            refIndices <- newSTRef mempty
            let notHalf = readSTRef refIndices >>= \is -> pure $ ISet.size is < required
            whileM_ notHalf $ do
                currRNG <- readSTRef refRNG
                let (nextRNG, i) = fromEnum . (`mod` modulus) <$> swap (MCG.next currRNG)
                modifySTRef' refIndices $ ISet.insert i
                writeSTRef refRNG nextRNG
            lastRNG <- readSTRef refRNG
            indices <- readSTRef refIndices
            pure (lastRNG, indices)
        (doneRNG, removeIndices) = runST gatherIndices
    in  (doneRNG, removeSelected xs)

-------------------------------------------------------------------------------
-- dry-run
-------------------------------------------------------------------------------

doDryRun :: GlobalOpts -> RunOpts -> IO ()
doDryRun gopts opts = do
    void $ timed_ $ doDryRun' gopts opts

doDryRun' :: GlobalOpts -> RunOpts -> IO ()
doDryRun' gopts opts = do
    -- calculated some expected statistics for generated batches
    -- using nested do block to limit scope of intermediate bindings n, d, p, and q
    do
       let d = toInteger $ 2 * initialSize gopts
       -- we generate n random numbers in range of [ 1 .. d ]
       -- what is the chance they are all distinct
       -- In this case each key in a table is could possibly share a key in another table.
       -- This is the number of entries per table, times the number of *other* tables
       -- Hence we have n = initialSize * (tableCount - 1)
       let n = toInteger $ initialSize gopts * (tableCount gopts - 1)

       -- High fidelity approximation of the Bithday Problem's probability:
       --   https://en.wikipedia.org/wiki/Birthday_problem#Approximations
       -- Compute notP = e^( -1*n*(n-1)/(2*d) )
       -- Then yesP = 1 - notP
       -- To compute the exponentiation of e efficiently and precisely, we use the taylor series expansion.
       let x = negate (n * (n - 1)) % (2 * d)
           taylorSeries = sum . take 100 $ (\i -> x ^ i / (factorial i % 1)) <$> [ 0 .. ]
           prob = 1 - taylorSeries
           percentage = prob * 100

       -- number of people with a shared birthday
       -- https://en.wikipedia.org/wiki/Birthday_problem#Number_of_people_with_a_shared_birthday
       let q = (n % 1) * (1 - ((d - 1) % d)) ^ (n - 1)

       printf "Probability of a duplicate:                          %14s%%\n" $ renderRational 10 percentage
       printf "Expected number of duplicates (extreme upper bound): %9s out of %d\n" (renderRational 5 q) n

    let keyRange :: LSM.Range K
        keyRange = LSM.FromToIncluding (makeKey minBound) (makeKey maxBound)

    duplicates <- LSM.withSession (rootDir gopts) $ \session -> do
      tKeySets <- forM (tableRange gopts) $ \tID -> do
        let name = makeTableName tID
        table <- if check opts
          then LSM.newTable session
          else LSM.openTableFromSnapshot session name label
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
-- PRNG initialisation
-------------------------------------------------------------------------------

initGen :: Int -> Int -> Int -> Word64 -> MCG.MCG
initGen initialSize batchSize batchCount seed =
    let period = initialSize + batchSize * batchCount
     in MCG.make (fromIntegral period) seed

-------------------------------------------------------------------------------
-- Batch generation
-------------------------------------------------------------------------------

generateBatch  ::
       Int       -- ^ initial size of the collection
    -> Int       -- ^ batch size
    -> MCG.MCG   -- ^ generator
    -> Int       -- ^ batch number
    -> (MCG.MCG, V.Vector K)
generateBatch initialSize batchSize g b =
    (g', lookups')
  where
    (lookups')    = toOperations lookups
    (!g', lookups) = generateBatch' initialSize batchSize g b

{- | Implement generation of unbounded sequence of insert\/delete operations

matching UTxO style from spec: interleaved batches insert and lookup
configurable batch sizes
1 insert, 1 delete, 1 lookup per key.

Current approach is probabilistic, but uses very little state.
We could also make it exact, but then we'll need to carry some state around
(at least the difference).

-}
{-# INLINE generateBatch' #-}
generateBatch' ::
       Int       -- ^ initial size of the collection
    -> Int       -- ^ batch size
    -> MCG.MCG   -- ^ generator
    -> Int       -- ^ batch number
    -> (MCG.MCG, V.Vector Word64)
generateBatch' initialSize batchSize g b = (g'', lookups)
  where
    maxK :: Word64
    maxK = fromIntegral $ initialSize + batchSize * b

    lookups :: V.Vector Word64
    (lookups, !g'') =
       runState (V.replicateM batchSize (state (MCG.reject maxK))) g

-- | Generate operation inputs
{-# INLINE toOperations #-}
toOperations :: V.Vector Word64 -> V.Vector K
toOperations lookups = batch1
  where
    batch1 :: V.Vector K
    batch1 = V.map makeKey lookups

-------------------------------------------------------------------------------
-- run
-------------------------------------------------------------------------------

doRun :: GlobalOpts -> RunOpts -> IO ()
doRun gopts opts = do
    -- Perform 3 measurement phases
    --   * Phase 1: Measure performance before supplying any credits.
    --   * Phase 2: Measure performance as credits are incrementally supplied and debt is repaid.
    --   * Phase 3: Measure performance when debt is 0.
    let tickCountPrefix = 50
        tickCountMiddle = 100
        tickCountSuffix = 50
        tickCountEnding = maximum indicesPhase3
        queriesEachTick = batchCount opts * batchSize opts
        indicesPhase1 = negate <$> reverse [ 0 .. tickCountPrefix ]
        indicesPhase2 = [ 1 .. tickCountMiddle ]
        indicesPhase3 = [ tickCountMiddle + 1 .. tickCountMiddle + tickCountSuffix ]
        indicesDomain = indicesPhase1 <> indicesPhase2 <> indicesPhase3
        benchmarkIterations h
          | pipelined opts = pipelinedIterations h
          | otherwise = sequentialIterations h

    refRNG <- newIORef $ initGen
                (initialSize gopts)
                (batchSize opts)
                (batchCount opts)
                (seed gopts)

    putStrLn "Operations per second:"
    measurements <- LSM.withSession (rootDir gopts) $ \session ->
      withLatencyHandle $ \h -> do
        -- Load the baseline table
        table_0 <- LSM.openTableFromSnapshot session baselineTableName label

        -- Load the union tables
        tables <- forM (tableRange gopts) $ \tID -> do
          let name = makeTableName tID
          if check opts
          then LSM.newTable session
          else LSM.openTableFromSnapshot session name label

        LSM.withIncrementalUnions tables $ \table -> do
          LSM.UnionDebt totalDebt <- LSM.remainingUnionDebt table
          -- Determine the number of credits to supply per tick in order to
          -- all debt repaid at the time specified by the rpayment rate.
          -- Each tick should supply credits equal to:
          --     paymentRate * totalDebt / tickCountMiddle
          let paymentPerTick = ceiling $ toInteger totalDebt % tickCountMiddle

          let measurePerformance :: Integer -> IO (Int, Int, Integer, Integer)
              measurePerformance tickIndex = do
                -- Note this tick's debt for subsequent measurement purposes.
                LSM.UnionDebt debtCurr <- LSM.remainingUnionDebt table
                -- Note the cumulative credits supplied through this tick.
                let paidCurr = max 0 $ totalDebt - fromInteger (max 0 tickIndex) * paymentPerTick
                -- Generate the randomized lookup batches and update the RNG.
                -- Use these exact same lookups for both the baseline table and the unioned table
                currRNG <- readIORef refRNG
                let generator = generateBatch (initialSize gopts) (batchSize opts)
                    domain = [ 0 .. (batchCount opts) - 1 ]
                    (nextRNG, allBatches) = traverseWithRNG currRNG domain generator
                    thisMeasurement = benchmarkIterations h (\_ _ -> pure ()) allBatches
                writeIORef refRNG nextRNG
                -- Perform measurement of batched lookups
                -- First, benchmark the baseline table
                (base, _, _) <- thisMeasurement table_0
                -- Next, benchmark the union table
                (time, _, _) <- thisMeasurement table
                -- Save the result for later to be included in the performance plot
                let rate :: Double
                    rate = fromRational $ fromIntegral queriesEachTick * 1_000 % time
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
            LSM.UnionDebt debtPrev <- LSM.remainingUnionDebt table
            -- When there is debt remaining, supply the fixed credits-per-tick.
            when (debtPrev > 0) . void $
              LSM.supplyUnionCredits table (LSM.UnionCredits paymentPerTick)
            measurePerformance step

          -- Phase 3 measurements: Debt = 0%
          resultsPhase3 <- forM indicesPhase3 $ \step -> do
            measurePerformance step

          pure $ mconcat [ resultsPhase1, resultsPhase2, resultsPhase3 ]

    let (balances', payments', baseline, operations) = List.unzip4 measurements
        maxValue = maximum operations
        standardize xs =
          let maxInput = toInteger $ maximum xs
              scale :: Integral i=> i -> Integer
              scale x = ceiling $ (fromIntegral x * maxValue) % maxInput
          in  scale <$> xs
        balances = standardize balances'
        payments = standardize payments'
        noDebtIndex = fst . head . dropWhile ((> 0) . snd) $ zip indicesDomain balances

        labelX = "Percent of debt repaid: \"clamped\" to range [0%, 100%]"
        labelY = "Time to perform " <> sep1000th ',' queriesEachTick <> " lookups (ms)"
        labelP = unwords
          [ "Measuring Incremental Union of"
          , show $ tableCount gopts
          , "Tables Containing"
          , sep1000th ',' $ initialSize gopts
          , "Entries Each"
          ]

    -- Generate a performance plot based on the benchmark results.
    Plot.toFile Plot.def (rootDir gopts <> "/" <> deriveFileNameForPlot gopts) $ do
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
        [ zip indicesDomain baseline ]

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
        [ zip indicesDomain operations ]

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

deriveFileNameForPlot :: GlobalOpts  -> FilePath
deriveFileNameForPlot gOpts =
    let partTable = show $ tableCount gOpts
        partWidth = sep1000th '_' $ initialSize gOpts
        partSeed0 = printf "SEED_%016x" (seed gOpts)
    in  List.intercalate "-"
          [ "benchmark"
          , partTable <> "x" <> partWidth
          , partSeed0
          ] <> ".png"

sep1000th :: Integral i => Char -> i -> String
sep1000th sep = reverse . List.intercalate [sep] . fmap Fold.toList . groupsOfN 3 . reverse . show . toInteger

-------------------------------------------------------------------------------
-- sequential
-------------------------------------------------------------------------------

type LookupResults = V.Vector (K, Maybe V)

assignLookupResults :: V.Vector K -> V.Vector (Maybe V) -> LookupResults
assignLookupResults ls = V.zip ls . fmap (fmap (const mempty))

{-# INLINE sequentialIteration #-}
sequentialIteration :: LatencyHandle
                      -> (Int -> LookupResults -> IO ())
                      -> LSM.Table K V
                      -> (Int, V.Vector K)
                      -> IO ()
sequentialIteration h output !tbl (!b, !ls) = withTimedBatch h b $ \tref -> do
    -- lookups
    results <- timeLatency tref $ LSM.lookups tbl ls
    output b $ assignLookupResults ls results

sequentialIterations :: LatencyHandle
                       -> (Int -> LookupResults -> IO ())
                       -> [V.Vector K]
                       -> LSM.Table K V
                       -> IO (Integer, RTSStatsDiff Triple, ProcIODiff)
sequentialIterations h output allBatches tbl = do
    createGnuplotExampleFileSequential
    hPutHeaderSequential h
    (x,y,z) <- timed_ $ forM_ (zip [0 ..] allBatches) $ sequentialIteration h output tbl
    pure (x,y,z)

-------------------------------------------------------------------------------
-- pipelined
-------------------------------------------------------------------------------

{- One iteration of the protocol for one thread looks like this:

1. Lookups (db_n-1) tx_n+0
2. Sync ?  (db_n+0, updates)

Thus for two threads running iterations concurrently, it looks like this:

1. Lookups (db_n-1) tx_n+0        3. db_n+0 <- Dup (db_n-1)
                                     Updates (db_n+0) tx_n-1
2. Sync ?  (db_n+0, updates)  <-  4. Sync !  (db_n+0, updates)
3. db_n+1 <- Dup (db_n+0)         1. Lookups (db_n+0) tx_n+1
   Updates (db_n+1) tx_n+0
4. Sync !  (db_n+1, updates)  ->  2. Sync ?  (db_n+1, updates)
1. Lookups (db_n+1) tx_n+2        3. db_n+2 <- Dup (db_n+1)
                                     Updates (db_n+2) tx_n+1
2. Sync ?  (db_n+2, updates)  <-  4. Sync !  (db_n+2, updates)
3. db_n+3 <- Dup (db_n+2)         1. Lookups (db_n+2) tx_n+3
   Updates (db_n+3) tx_n+2
4. Sync !  (db_n+3, updates)  ->  2. Sync ?  (db_n+3, updates)
1. Lookups (db_n+3) tx_n+4        3. db_n+4 <- Dup (db_n+3)
                                     Updates (db_n+4) tx_n+3
2. Sync ?  (db_n+4, updates)  <-  4. Sync !  (db_n+4, updates)
3. db_n+5 <- Dup (db_n+4)         1. Lookups (db_n+4) tx_n+5
   Updates (db_n+5) tx_n+4
4. Sync !  (db_n+5, updates)  ->  2. Sync ?  (db_n+5, updates)

And the initial setup looks like this:

   db_1 <- Dup (db_0)
   Lookups (db_0) tx_0
   Updates (db_1, tx_0)
   Sync !  (db_1, updates)    ->
                                  1. Lookups (db_0) tx_1

                                  2. Sync ?  (db_1, updates)
1. Lookups (db_1) tx_2            3. db_2 <- Dup (db_1)
                                     Updates (db_2) tx_1
2. Sync ?  (db_2, updates)    <-  4. Sync !  (db_2, updates)
3. db_3 <- Dup (db_2)             1. Lookups (db_2) tx_3
   Updates (db_3) tx_2
4. Sync !  (db_3, updates)        2. Sync ?  (db_3, updates)
-}
pipelinedIteration :: LatencyHandle
                   -> (Int -> LookupResults -> IO ())
                   -> MVar (LSM.Table K V)
                   -> MVar (LSM.Table K V)
                   -> MVar (V.Vector K)
                   -> MVar (V.Vector K)
                   -> MVar [V.Vector K]
                   -> LSM.Table K V
                   -> Int
                   -> IO (LSM.Table K V)
pipelinedIteration h output
                   !syncTblIn !syncTblOut
                   !syncVecIn !syncVecOut
                   !queue
                   !tbl_n !b =
    withTimedBatch h b $ \tref -> do
    ls_curr <- takeMVar syncVecIn

    -- 1: perform the lookups
    lrs <- timeLatency tref $ LSM.lookups tbl_n ls_curr

    -- 2. sync: receive updates and new table
    tbl_n1 <- timeLatency tref $ do
      tbl_n1 <- takeMVar syncTblIn
      output b $ assignLookupResults ls_curr lrs

      -- At this point, after syncing, our peer is guaranteed to no longer be
      -- using tbl_n. They used it to generate tbl_n+1 (which they gave us).
      LSM.closeTable tbl_n
      pure tbl_n1

    ls_next <- dequeue queue
    putMVar syncTblOut tbl_n1
    putMVar syncVecOut ls_next
    pure tbl_n1

pipelinedIterations :: LatencyHandle
                    -> (Int -> LookupResults -> IO ())
                    -> [V.Vector K]
                    -> LSM.Table K V
                    -> IO (Integer, RTSStatsDiff Triple, ProcIODiff)
pipelinedIterations h output allBatches tbl_0 = do
    createGnuplotExampleFilePipelined
    hPutHeaderPipelined h
    n <- getNumCapabilities
    printf "INFO: the pipelined benchmark is running with %d capabilities.\n" n

    syncTblA2B <- newEmptyMVar
    syncTblB2A <- newEmptyMVar
    syncVecA2B <- newEmptyMVar
    syncVecB2A <- newEmptyMVar
    queryQueue <- newEmptyMVar

    putMVar queryQueue allBatches

    tbl_1 <- LSM.duplicate tbl_0
    let !batchSize = length allBatches

        prelude = do
          ls0 <- dequeue queryQueue
          lrs0 <- LSM.lookups tbl_0 ls0
          output 0 $! V.zip ls0 (fmap (fmap (const mempty)) lrs0)
          putMVar syncTblA2B tbl_1
          vecNext <- dequeue queryQueue
          putMVar syncVecA2B vecNext

        threadA =
          forFoldM_ tbl_1 [ 2, 4 .. batchSize - 1 ] $ \b tbl_n ->
            pipelinedIteration h output
                               syncTblB2A syncTblA2B -- in, out
                               syncVecB2A syncVecA2B -- in, out
                               queryQueue
                               tbl_n b

        threadB =
          forFoldM_ tbl_0 [ 1, 3 .. batchSize - 1 ] $ \b tbl_n ->
            pipelinedIteration h output
                               syncTblA2B syncTblB2A -- in, out
                               syncVecA2B syncVecB2A -- in, out
                               queryQueue
                               tbl_n b

    let operations = do
          -- We do batch 0 as a special prelude to get the pipeline started...
          prelude
          -- Run the pipeline: batches 2,4,6... concurrently with batches 1,3,5...
          -- If run with +RTS -N2 then we'll put each thread on a separate core.
          withAsyncOn 0 threadA $ \ta ->
            withAsyncOn 1 threadB $ \tb ->
              waitBoth ta tb >> return ()

    (x,y,z) <- timed_ operations
    pure (x,y,z)

dequeue :: Monoid a => MVar [a] -> IO a
dequeue q = modifyMVar q $ pure . swap . fromMaybe (mempty, []) . List.uncons

-------------------------------------------------------------------------------
-- measure batch latency
-------------------------------------------------------------------------------

_mEASURE_BATCH_LATENCY :: Bool
#ifdef MEASURE_BATCH_LATENCY
_mEASURE_BATCH_LATENCY = True
#else
_mEASURE_BATCH_LATENCY = False
#endif

type LatencyHandle = Handle

type TimeRef = IORef [Integer]

withLatencyHandle :: (LatencyHandle -> IO a) -> IO a
withLatencyHandle k
  | _mEASURE_BATCH_LATENCY = withFile "latency.dat" WriteMode k
  | otherwise = k (error "LatencyHandle: do not use")

{-# INLINE hPutHeaderSequential #-}
hPutHeaderSequential :: LatencyHandle -> IO ()
hPutHeaderSequential h
  | _mEASURE_BATCH_LATENCY = do
    hPutStrLn h "# batch number \t lookup time (ns) \t update time (ns)"
  | otherwise = pure ()

{-# INLINE createGnuplotExampleFileSequential #-}
createGnuplotExampleFileSequential :: IO ()
createGnuplotExampleFileSequential
  | _mEASURE_BATCH_LATENCY = do
    withFile "latency.gp" WriteMode $ \h -> do
      mapM_ (hPutStrLn h) [
          "set title \"Latency (sequential)\""
        , ""
        , "set xlabel \"Batch number\""
        , ""
        , "set logscale y"
        , "set ylabel \"Time (nanoseconds)\""
        , ""
        , "plot \"latency.dat\" using 1:2 title 'lookups' axis x1y1, \\"
        , "     \"latency.dat\" using 1:3 title 'updates' axis x1y1"
        ]
  | otherwise = pure ()

{-# INLINE hPutHeaderPipelined #-}
hPutHeaderPipelined :: LatencyHandle -> IO ()
hPutHeaderPipelined h
  | _mEASURE_BATCH_LATENCY = do
    hPutStr   h "# batch number"
    hPutStr   h "\t lookup time (ns) \t sync receive time (ns) "
    hPutStrLn h "\t update time (ns) \t sync send time (ns)"
  | otherwise = pure ()

{-# INLINE createGnuplotExampleFilePipelined #-}
createGnuplotExampleFilePipelined :: IO ()
createGnuplotExampleFilePipelined
  | _mEASURE_BATCH_LATENCY =
    withFile "latency.gp" WriteMode $ \h -> do
      mapM_ (hPutStrLn h) [
          "set title \"Latency (pipelined)\""
        , ""
        , "set xlabel \"Batch number\""
        , ""
        , "set logscale y"
        , "set ylabel \"Time (nanoseconds)\""
        , ""
        , "plot \"latency.dat\" using 1:2 title 'lookups' axis x1y1, \\"
        , "     \"latency.dat\" using 1:3 title 'sync receive' axis x1y1, \\"
        , "     \"latency.dat\" using 1:4 title 'updates' axis x1y1, \\"
        , "     \"latency.dat\" using 1:5 title 'sync send' axis x1y1"
        ]
  | otherwise = pure ()

{-# INLINE withTimedBatch #-}
withTimedBatch :: LatencyHandle -> Int -> (TimeRef -> IO a) -> IO a
withTimedBatch h b k
  | _mEASURE_BATCH_LATENCY = do
      tref <- newIORef []
      x <- k tref
      ts <- readIORef tref
      let s = shows b
            . getDual (foldMap (\t -> Dual (showString "\t" <> shows t)) ts)
      hPutStrLn h (s "")
      pure x
  | otherwise = k (error "TimeRef: do not use")

{-# INLINE timeLatency #-}
timeLatency :: TimeRef -> IO a -> IO a
timeLatency tref k
  | _mEASURE_BATCH_LATENCY = do
    t1 <- Clock.getTime Clock.Monotonic
    x  <- k
    t2 <- Clock.getTime Clock.Monotonic
    let !t = Clock.toNanoSecs (Clock.diffTimeSpec t2 t1)
    modifyIORef tref (t :)
    pure x
  | otherwise = k

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
        CmdSetup       -> doSetup gopts
        CmdDryRun opts -> doDryRun gopts opts
        CmdRun opts    -> doRun gopts opts
  where
    cliP = O.info ((,) <$> globalOptsP <*> cmdP <**> O.helper) O.fullDesc
    prefs = O.prefs $ O.showHelpOnEmpty <> O.helpShowGlobals <> O.subparserInline

-------------------------------------------------------------------------------
-- general utils
-------------------------------------------------------------------------------

forFoldM_ :: Monad m => s -> [a] -> (a -> s -> m s) -> m s
forFoldM_ !s0 xs0 f = go s0 xs0
  where
    go !s []     = return s
    go !s (x:xs) = do
      !s' <- f x s
      go s' xs

traverseWithRNG :: forall a b rng. rng -> [a] -> (rng -> a -> (rng,b)) -> (rng, [b])
traverseWithRNG !s0 xs0 f =
    let stateful :: forall s. ST s (rng, [b])
        stateful = do
          genRef <- newSTRef s0
          let action a = do
                sCurr <- readSTRef genRef
                let (sNext,b) = f sCurr a
                writeSTRef genRef sNext
                pure b
          result <- traverse action xs0
          sLast <- readSTRef genRef
          pure (sLast, result)
    in runST stateful

-------------------------------------------------------------------------------
-- unused for now
-------------------------------------------------------------------------------

_unused :: ()
_unused = const ()
    timed
