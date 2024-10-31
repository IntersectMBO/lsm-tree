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
import           Control.DeepSeq (force)
import           Control.Exception
import           Control.Monad (forM_, unless, void, when)
import           Control.Monad.Trans.State.Strict (runState, state)
import           Control.Tracer
import qualified Data.ByteString.Short as BS
import qualified Data.Foldable as Fold
import qualified Data.IntSet as IS
import           Data.IORef
import qualified Data.List.NonEmpty as NE
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import           Data.Monoid
import qualified Data.Primitive as P
import qualified Data.Vector as V
import           Data.Void (Void)
import           Data.Word (Word32, Word64)
import qualified GHC.Stats as GHC
import qualified MCG
import qualified Options.Applicative as O
import           Prelude hiding (lookup)
import qualified System.Clock as Clock
import qualified System.FS.API as FS
import qualified System.FS.BlockIO.API as FS
import qualified System.FS.BlockIO.IO as FsIO
import qualified System.FS.IO as FsIO
import           System.IO
import           System.Mem (performMajorGC)
import qualified System.Random as Random
import           Text.Printf (printf)
import           Text.Show.Pretty

import           Database.LSMTree.Extras (groupsOfN)
import           Database.LSMTree.Internal.ByteString (byteArrayToSBS)

-- We should be able to write this benchmark
-- using only use public lsm-tree interface
import qualified Database.LSMTree.Normal as LSM

-------------------------------------------------------------------------------
-- Keys and values
-------------------------------------------------------------------------------

type K = BS.ShortByteString
type V = BS.ShortByteString
type B = Void

instance LSM.Labellable (K, V, B) where
  makeSnapshotLabel _ = "K V B"

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
    { rootDir         :: !FilePath  -- ^ session directory.
    , initialSize     :: !Int
      -- | The cache policy for the LSM table. This configuration option is used
      -- both during setup, and during a run (where it is used to override the
      -- config option of the snapshot).
    , diskCachePolicy :: !LSM.DiskCachePolicy
      -- | Enable trace output
    , trace           :: !Bool
    }
  deriving stock Show

data SetupOpts = SetupOpts {
    bloomFilterAlloc :: !LSM.BloomFilterAlloc
  }
  deriving stock Show

data RunOpts = RunOpts
    { batchCount :: !Int
    , batchSize  :: !Int
    , check      :: !Bool
    , seed       :: !Word64
    , pipelined  :: !Bool
    , lookuponly :: !Bool
    }
  deriving stock Show

data Cmd
    -- | Setup benchmark: generate initial LSM tree etc.
    = CmdSetup SetupOpts

    -- | Make a dry run, measure the overhead.
    | CmdDryRun RunOpts

    -- | Run the actual benchmark
    | CmdRun RunOpts
  deriving stock Show

mkTableConfigSetup :: GlobalOpts -> SetupOpts -> LSM.TableConfig -> LSM.TableConfig
mkTableConfigSetup GlobalOpts{diskCachePolicy} SetupOpts{bloomFilterAlloc} conf = conf {
      LSM.confDiskCachePolicy = diskCachePolicy
    , LSM.confBloomFilterAlloc = bloomFilterAlloc
    }

mkTableConfigRun :: GlobalOpts -> LSM.TableConfig -> LSM.TableConfig
mkTableConfigRun GlobalOpts{diskCachePolicy} conf = conf {
      LSM.confDiskCachePolicy = diskCachePolicy
    }

mkTableConfigOverride :: GlobalOpts -> LSM.TableConfigOverride
mkTableConfigOverride GlobalOpts{diskCachePolicy} =
    LSM.configOverrideDiskCachePolicy diskCachePolicy

mkTracer :: GlobalOpts -> Tracer IO LSM.LSMTreeTrace
mkTracer gopts
  | trace gopts =
      -- Don't trace update/lookup messages, because they are too noisy
      squelchUnless
        (\case
          LSM.TraceTable _ LSM.TraceUpdates{} -> False
          LSM.TraceTable _ LSM.TraceLookups{} -> False
          _                                   -> True )
        (show `contramap` stdoutTracer)
  | otherwise   = nullTracer

-------------------------------------------------------------------------------
-- command line interface
-------------------------------------------------------------------------------

globalOptsP :: O.Parser GlobalOpts
globalOptsP = pure GlobalOpts
    <*> O.option O.str (O.long "bench-dir" <> O.value "_bench_wp8" <> O.showDefault <> O.help "Benchmark directory to put files in")
    <*> O.option O.auto (O.long "initial-size" <> O.value 100_000_000 <> O.showDefault <> O.help "Initial LSM tree size")
    <*> O.option O.auto (O.long "disk-cache-policy" <> O.value LSM.DiskCacheAll <> O.showDefault <> O.help "Disk cache policy [DiskCacheAll | DiskCacheLevelsAtOrBelow Int | DiskCacheNone]")
    <*> O.flag False True (O.long "trace" <> O.help "Enable trace messages (disabled by default)")

cmdP :: O.Parser Cmd
cmdP = O.subparser $ mconcat
    [ O.command "setup" $ O.info
        (CmdSetup <$> setupOptsP <**> O.helper)
        (O.progDesc "Setup benchmark")
    , O.command "dry-run" $ O.info
        (CmdDryRun <$> runOptsP <**> O.helper)
        (O.progDesc "Dry run, measure overhead")

    , O.command "run" $ O.info
        (CmdRun <$> runOptsP <**> O.helper)
        (O.progDesc "Proper run")
    ]

setupOptsP :: O.Parser SetupOpts
setupOptsP = pure SetupOpts
    <*> O.option O.auto (O.long "bloom-filter-alloc" <> O.value LSM.defaultBloomFilterAlloc <> O.showDefault <> O.help "Bloom filter allocation method [AllocFixed n | AllocRequestFPR d | AllocMonkey m (NumEntries n)]")

runOptsP :: O.Parser RunOpts
runOptsP = pure RunOpts
    <*> O.option O.auto (O.long "batch-count" <> O.value 200 <> O.showDefault <> O.help "Batch count")
    <*> O.option O.auto (O.long "batch-size" <> O.value 256 <> O.showDefault <> O.help "Batch size")
    <*> O.switch (O.long "check" <> O.help "Check generated key distribution")
    <*> O.option O.auto (O.long "seed" <> O.value 1337 <> O.showDefault <> O.help "Random seed")
    <*> O.switch (O.long "pipelined" <> O.help "Use pipelined mode")
    <*> O.switch (O.long "lookup-only" <> O.help "Use lookup only mode")

-------------------------------------------------------------------------------
-- measurements
-------------------------------------------------------------------------------

timed :: IO a -> IO (a, Double, RTSStatsDiff Triple, ProcIODiff)
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
    let !t = fromIntegral (Clock.toNanoSecs (Clock.diffTimeSpec t2 t1)) * 1e-9
        !s = s2 `diffRTSStats` s1
        !p = p2 `diffProcIO` p1
    printf "Running time:  %.03f sec\n" t
    printf "/proc/self/io after vs. before: %s\n" (ppShow p)
    printf "RTSStats after vs. before: %s\n" (ppShow s)
    return (x, t, s, p)

timed_ :: IO () -> IO (Double, RTSStatsDiff Triple, ProcIODiff)
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

doSetup :: GlobalOpts -> SetupOpts -> IO ()
doSetup gopts opts = do
    void $ timed_ $ doSetup' gopts opts

doSetup' :: GlobalOpts -> SetupOpts -> IO ()
doSetup' gopts opts = do
    let mountPoint :: FS.MountPoint
        mountPoint = FS.MountPoint (rootDir gopts)

    let hasFS :: FS.HasFS IO FsIO.HandleIO
        hasFS = FsIO.ioHasFS mountPoint

    hasBlockIO <- FsIO.ioHasBlockIO hasFS FS.defaultIOCtxParams

    name <- maybe (fail "invalid snapshot name") return $
        LSM.mkSnapshotName "bench"

    LSM.withSession (mkTracer gopts) hasFS hasBlockIO (FS.mkFsPath []) $ \session -> do
        tbh <- LSM.new @IO @K @V @B session (mkTableConfigSetup gopts opts LSM.defaultTableConfig)

        forM_ (groupsOfN 256 [ 0 .. initialSize gopts ]) $ \batch -> do
            -- TODO: this procedure simply inserts all the keys into initial lsm tree
            -- We might want to do deletes, so there would be delete-insert pairs
            -- Let's do that when we can actually test that benchmark works.
            --
            -- TODO: LSM.inserts has annoying order
            flip LSM.inserts tbh $ V.fromList [
                  (makeKey (fromIntegral i), theValue, Nothing)
                | i <- NE.toList batch
                ]

        LSM.snapshot name tbh

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
       -- we generate n random numbers in range of [ 1 .. d ]
       -- what is the chance they are all distinct
       let n = fromIntegral (batchCount opts * batchSize opts) :: Double
       let d = fromIntegral (initialSize gopts) :: Double
       -- this is birthday problem.
       let p = 1 - exp (negate $  (n * (n - 1)) / (2 * d))

       -- number of people with a shared birthday
       -- https://en.wikipedia.org/wiki/Birthday_problem#Number_of_people_with_a_shared_birthday
       let q = n * (1 - ((d - 1) / d) ** (n - 1))

       printf "Probability of a duplicate:                          %5f\n" p
       printf "Expected number of duplicates (extreme upper bound): %5f out of %f\n" q n

    let g0 = initGen (initialSize gopts) (batchSize opts) (batchCount opts) (seed opts)

    keysRef <- newIORef $
        if check opts
        then IS.fromList [ 0 .. (initialSize gopts) - 1 ]
        else IS.empty
    duplicateRef <- newIORef (0 :: Int)

    void $ forFoldM_ g0 [ 0 .. batchCount opts - 1 ] $ \b g -> do
        let lookups :: V.Vector Word64
            inserts :: V.Vector Word64
            (!g', lookups, inserts) = generateBatch' (initialSize gopts) (batchSize opts) g b

        when (check opts) $ do
            keys <- readIORef keysRef
            let new  = intSetFromVector lookups
            let diff = IS.difference new keys
            -- when (IS.notNull diff) $ printf "missing in batch %d %s\n" b (show diff)
            modifyIORef' duplicateRef $ \n -> n + IS.size diff
            writeIORef keysRef $! IS.union (IS.difference keys new)
                                           (intSetFromVector inserts)

        let (batch1, batch2) = toOperations lookups inserts
        _ <- evaluate $ force (batch1, batch2)

        return g'

    when (check opts) $ do
        duplicates <- readIORef duplicateRef
        printf "True duplicates: %d\n" duplicates

    -- See batchOverlaps for explanation of this check.
    when (check opts) $
        let anyOverlap = (not . null)
                           (batchOverlaps (initialSize gopts) (batchSize opts)
                                          (batchCount opts) (seed opts))
         in putStrLn $ "Any adjacent batches with overlap: " ++ show anyOverlap

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

generateBatch
    :: Int       -- ^ initial size of the collection
    -> Int       -- ^ batch size
    -> MCG.MCG   -- ^ generator
    -> Int       -- ^ batch number
    -> (MCG.MCG, V.Vector K, V.Vector (K, LSM.Update V B))
generateBatch initialSize batchSize g b =
    (g', lookups', inserts')
  where
    (lookups', inserts')    = toOperations lookups inserts
    (!g', lookups, inserts) = generateBatch' initialSize batchSize g b

{- | Implement generation of unbounded sequence of insert\/delete operations

matching UTxO style from spec: interleaved batches insert and lookup
configurable batch sizes
1 insert, 1 delete, 1 lookup per key.

Current approach is probabilistic, but uses very little state.
We could also make it exact, but then we'll need to carry some state around
(at least the difference).

-}
{-# INLINE generateBatch' #-}
generateBatch'
    :: Int       -- ^ initial size of the collection
    -> Int       -- ^ batch size
    -> MCG.MCG   -- ^ generator
    -> Int       -- ^ batch number
    -> (MCG.MCG, V.Vector Word64, V.Vector Word64)
generateBatch' initialSize batchSize g b = (g'', lookups, inserts)
  where
    maxK :: Word64
    maxK = fromIntegral $ initialSize + batchSize * b

    lookups :: V.Vector Word64
    (lookups, !g'') =
       runState (V.replicateM batchSize (state (MCG.reject maxK))) g

    inserts :: V.Vector Word64
    inserts = V.enumFromTo maxK (maxK + fromIntegral batchSize - 1)

-- | Generate operation inputs
{-# INLINE toOperations #-}
toOperations :: V.Vector Word64 -> V.Vector Word64 -> (V.Vector K, V.Vector (K, LSM.Update V B))
toOperations lookups inserts = (batch1, batch2)
  where
    batch1 :: V.Vector K
    batch1 = V.map makeKey lookups

    batch2 :: V.Vector (K, LSM.Update V B)
    batch2 = V.map (\k -> (k, LSM.Delete)) batch1 V.++
             V.map (\k -> (makeKey k, LSM.Insert theValue Nothing)) inserts

-------------------------------------------------------------------------------
-- run
-------------------------------------------------------------------------------

doRun :: GlobalOpts -> RunOpts -> IO ()
doRun gopts opts = do
    let mountPoint :: FS.MountPoint
        mountPoint = FS.MountPoint (rootDir gopts)

    let hasFS :: FS.HasFS IO FsIO.HandleIO
        hasFS = FsIO.ioHasFS mountPoint

    hasBlockIO <- FsIO.ioHasBlockIO hasFS FS.defaultIOCtxParams

    name <- maybe (fail "invalid snapshot name") return $
        LSM.mkSnapshotName "bench"

    LSM.withSession (mkTracer gopts) hasFS hasBlockIO (FS.mkFsPath []) $ \session ->
      withLatencyHandle $ \h -> do
        -- open snapshot
        -- In checking mode we start with an empty table, since our pure
        -- reference version starts with empty (as it's not practical or
        -- necessary for testing to load the whole snapshot).
        tbl <- if check opts
                then LSM.new  @IO @K @V @B session (mkTableConfigRun gopts LSM.defaultTableConfig)
                else LSM.open @IO @K @V @B session (mkTableConfigOverride gopts) name

        -- In checking mode, compare each output against a pure reference.
        checkvar <- newIORef $ pureReference
                                (initialSize gopts) (batchSize opts)
                                (batchCount opts) (seed opts)
        let fcheck | not (check opts) = \_ _ -> return ()
                   | otherwise = \b y -> do
              (x:xs) <- readIORef checkvar
              unless (x == y) $
                fail $ "lookup result mismatch in batch " ++ show b
              writeIORef checkvar xs

        let benchmarkIterations
              | pipelined opts = pipelinedIterations h
              | lookuponly opts= sequentialIterationsLO
              | otherwise      = sequentialIterations h
            !progressInterval  = max 1 ((batchCount opts) `div` 100)
            madeProgress b     = b `mod` progressInterval == 0
        (time, _, _) <- timed_ $ do
          benchmarkIterations
            (\b y -> fcheck b y >> when (madeProgress b) (putChar '.'))
            (initialSize gopts)
            (batchSize opts)
            (batchCount opts)
            (seed opts)
            tbl
          putStrLn ""

        let ops = batchCount opts * batchSize opts
        printf "Operations per second: %7.01f ops/sec\n" (fromIntegral ops / time)

-------------------------------------------------------------------------------
-- sequential
-------------------------------------------------------------------------------

type LookupResults = V.Vector (K, LSM.LookupResult V ())

{-# INLINE sequentialIteration #-}
sequentialIteration :: LatencyHandle
                    -> (Int -> LookupResults -> IO ())
                    -> Int
                    -> Int
                    -> LSM.Table IO K V B
                    -> Int
                    -> MCG.MCG
                    -> IO MCG.MCG
sequentialIteration h output !initialSize !batchSize !tbl !b !g =
    withTimedBatch h b $ \tref -> do
    let (!g', ls, is) = generateBatch initialSize batchSize g b

    -- lookups
    results <- timeLatency tref $ LSM.lookups ls tbl
    output b (V.zip ls (fmap (fmap (const ())) results))

    -- deletes and inserts
    _ <- timeLatency tref $ LSM.updates is tbl

    -- continue to the next batch
    return g'


sequentialIterations :: LatencyHandle
                     -> (Int -> LookupResults -> IO ())
                     -> Int -> Int -> Int -> Word64
                     -> LSM.Table IO K V B
                     -> IO ()
sequentialIterations h output !initialSize !batchSize !batchCount !seed !tbl = do
    createGnuplotExampleFileSequential
    hPutHeaderSequential h
    void $ forFoldM_ g0 [ 0 .. batchCount - 1 ] $ \b g ->
      sequentialIteration h output initialSize batchSize tbl b g
  where
    g0 = initGen initialSize batchSize batchCount seed

{-# INLINE sequentialIterationLO #-}
sequentialIterationLO :: (Int -> LookupResults -> IO ())
                      -> Int
                      -> Int
                      -> LSM.Table IO K V B
                      -> Int
                      -> MCG.MCG
                      -> IO MCG.MCG
sequentialIterationLO output !initialSize !batchSize !tbl !b !g = do
    let (!g', ls, _is) = generateBatch initialSize batchSize g b

    -- lookups
    results <- LSM.lookups ls tbl
    output b (V.zip ls (fmap (fmap (const ())) results))

    -- continue to the next batch
    return g'

sequentialIterationsLO :: (Int -> LookupResults -> IO ())
                       -> Int -> Int -> Int -> Word64
                       -> LSM.Table IO K V B
                       -> IO ()
sequentialIterationsLO output !initialSize !batchSize !batchCount !seed !tbl =
    void $ forFoldM_ g0 [ 0 .. batchCount - 1 ] $ \b g ->
      sequentialIterationLO output initialSize batchSize tbl b g
  where
    g0 = initGen initialSize batchSize batchCount seed

-------------------------------------------------------------------------------
-- pipelined
-------------------------------------------------------------------------------

{- One iteration of the protocol for one thread looks like this:

1. Lookups (db_n-1) tx_n+0
2. Sync ?  (db_n+0, updates)
3. db_n+1 <- Dup (db_n+0)
   Updates (db_n+1) tx_n+0
4. Sync !  (db_n+1, updates)

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
                   -> Int
                   -> Int
                   -> MVar (LSM.Table IO K V B, Map K (LSM.Update V B))
                   -> MVar (LSM.Table IO K V B, Map K (LSM.Update V B))
                   -> MVar MCG.MCG
                   -> MVar MCG.MCG
                   -> LSM.Table IO K V B
                   -> Int
                   -> IO (LSM.Table IO K V B)
pipelinedIteration h output !initialSize !batchSize
                   !syncTblIn !syncTblOut
                   !syncRngIn !syncRngOut
                   !tbl_n !b =
    withTimedBatch h b $ \tref -> do
    g <- takeMVar syncRngIn
    let (!g', !ls, !is) = generateBatch initialSize batchSize g b

    -- 1: perform the lookups
    lrs <- timeLatency tref $ LSM.lookups ls tbl_n

    -- 2. sync: receive updates and new table
    tbl_n1 <- timeLatency tref $ do
      putMVar syncRngOut g'
      (tbl_n1, delta) <- takeMVar syncTblIn

      -- At this point, after syncing, our peer is guaranteed to no longer be
      -- using tbl_n. They used it to generate tbl_n+1 (which they gave us).
      LSM.close tbl_n
      output b $! applyUpdates delta (V.zip ls lrs)
      pure tbl_n1

    -- 3. perform the inserts and report outputs (in any order)
    tbl_n2 <- timeLatency tref $ do
      tbl_n2 <- LSM.duplicate tbl_n1
      LSM.updates is tbl_n2
      pure tbl_n2

    -- 4. sync: send the updates and new table
    timeLatency tref $  do
      let delta' :: Map K (LSM.Update V B)
          !delta' = Map.fromList (V.toList is)
      putMVar syncTblOut (tbl_n2, delta')

    return tbl_n2
  where
    applyUpdates :: Map K (LSM.Update V a)
                 -> V.Vector (K, LSM.LookupResult V b)
                 -> V.Vector (K, LSM.LookupResult V ())
    applyUpdates m lrs =
        flip V.map lrs $ \(k, lr) ->
          case Map.lookup k m of
            Nothing -> (k, fmap (const ()) lr)
            Just u  -> (k, updateToLookupResult u)

pipelinedIterations :: LatencyHandle
                    -> (Int -> LookupResults -> IO ())
                    -> Int -> Int -> Int -> Word64
                    -> LSM.Table IO K V B
                    -> IO ()
pipelinedIterations h output !initialSize !batchSize !batchCount !seed tbl_0 = do
    createGnuplotExampleFilePipelined
    hPutHeaderPipelined h
    n <- getNumCapabilities
    printf "INFO: the pipelined benchmark is running with %d capabilities.\n" n

    syncTblA2B <- newEmptyMVar
    syncTblB2A <- newEmptyMVar
    syncRngA2B <- newEmptyMVar
    syncRngB2A <- newEmptyMVar

    let g0 = initGen initialSize batchSize batchCount seed

    tbl_1 <- LSM.duplicate tbl_0
    let prelude = do
          let (g1, ls0, is0) = generateBatch initialSize batchSize g0 0
          lrs0 <- LSM.lookups ls0 tbl_0
          output 0 $! V.zip ls0 (fmap (fmap (const ())) lrs0)
          LSM.updates is0 tbl_1
          let !delta = Map.fromList (V.toList is0)
          putMVar syncTblA2B (tbl_1, delta)
          putMVar syncRngA2B g1

        threadA =
          forFoldM_ tbl_1 [ 2, 4 .. batchCount - 1 ] $ \b tbl_n ->
            pipelinedIteration h output initialSize batchSize
                               syncTblB2A syncTblA2B -- in, out
                               syncRngB2A syncRngA2B -- in, out
                               tbl_n b

        threadB =
          forFoldM_ tbl_0 [ 1, 3 .. batchCount - 1 ] $ \b tbl_n ->
            pipelinedIteration h output initialSize batchSize
                               syncTblA2B syncTblB2A -- in, out
                               syncRngA2B syncRngB2A -- in, out
                               tbl_n b

    -- We do batch 0 as a special prelude to get the pipeline started...
    prelude
    -- Run the pipeline: batches 2,4,6... concurrently with batches 1,3,5...
    -- If run with +RTS -N2 then we'll put each thread on a separate core.
    withAsyncOn 0 threadA $ \ta ->
      withAsyncOn 1 threadB $ \tb ->
        waitBoth ta tb >> return ()

-------------------------------------------------------------------------------
-- Testing
-------------------------------------------------------------------------------

pureReference :: Int -> Int -> Int -> Word64 -> [V.Vector (K, LSM.LookupResult V ())]
pureReference !initialSize !batchSize !batchCount !seed =
    generate g0 Map.empty 0
  where
    g0 = initGen initialSize batchSize batchCount seed

    generate !_ !_ !b | b == batchCount = []
    generate !g !m !b = results : generate g' m' (b+1)
      where
        (g', lookups, inserts) = generateBatch initialSize batchSize g b
        !results = V.map (lookup m) lookups
        !m'      = Fold.foldl' (flip (uncurry Map.insert)) m inserts

    lookup m k =
      case Map.lookup k m of
        Nothing -> (,) k LSM.NotFound
        Just u  -> (,) k $! updateToLookupResult u

updateToLookupResult :: LSM.Update v blob -> LSM.LookupResult v ()
updateToLookupResult (LSM.Insert v Nothing)  = LSM.Found v
updateToLookupResult (LSM.Insert v (Just _)) = LSM.FoundWithBlob v ()
updateToLookupResult  LSM.Delete             = LSM.NotFound

-- | Return the adjacent batches where there is overlap between one batch's
-- inserts and the next batch's lookups. Testing the pipelined version needs
-- some overlap to get proper coverage. So this function is used as a coverage
-- check.
--
batchOverlaps :: Int -> Int -> Int -> Word64 -> [IS.IntSet]
batchOverlaps initialSize batchSize batchCount seed =
    let xs = generate g0 0

     in filter (not . IS.null)
      $ map (\((_, is),(ls, _)) -> IS.intersection (intSetFromVector is)
                                                   (intSetFromVector ls))
      $ zip xs (tail xs)
  where
    generate _ b | b == batchCount = []
    generate g b = (lookups, inserts) : generate g' (b+1)
      where
        (g', lookups, inserts) = generateBatch' initialSize batchSize g b

    g0 = initGen initialSize batchSize batchCount seed

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
    (gopts, cmd) <- O.customExecParser prefs cliP
    print gopts
    print cmd
    case cmd of
        CmdSetup opts  -> doSetup gopts opts
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

intSetFromVector :: V.Vector Word64 -> IS.IntSet
intSetFromVector = V.foldl' (\acc x -> IS.insert (fromIntegral x) acc) IS.empty

-------------------------------------------------------------------------------
-- unused for now
-------------------------------------------------------------------------------

_unused :: ()
_unused = const ()
    timed
