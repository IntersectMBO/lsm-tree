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

TODO 2024-04-29 consider alternative methods of implementing key generation
-}
module Main (main) where

import           Control.Applicative ((<**>))
import           Control.Concurrent (getNumCapabilities)
import           Control.Concurrent.Async
import           Control.Concurrent.MVar
import           Control.DeepSeq (force)
import           Control.Exception (evaluate, assert)
import           Control.Monad (forM_, unless, void, when)
import           Control.Monad.Trans.State.Strict (runState, state)
import           Control.Monad.ST (ST, RealWorld, stToIO)
import           Control.Tracer
import qualified Data.ByteString.Short as BS
import qualified Data.Foldable as Fold
import qualified Data.IntSet as IS
import           Data.IORef (modifyIORef', newIORef, readIORef, writeIORef)
import qualified Data.List.NonEmpty as NE
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import qualified Data.Primitive as P
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Primitive as VP
import qualified Data.Vector.Primitive.Mutable as VPM
import           Data.Void (Void)
import           Data.Word (Word32, Word64)
import qualified GHC.Stats as GHC
import qualified Options.Applicative as O
import           Prelude hiding (lookup)
import qualified System.Clock as Clock
import qualified System.FS.API as FS
import qualified System.FS.BlockIO.API as FS
import qualified System.FS.BlockIO.IO as FsIO
import qualified System.FS.IO as FsIO
import           System.IO
import           System.Mem (performMajorGC)
import           System.Random as Random
import           System.Random.SplitMix as Random.SM
import           Text.Printf (printf)
import           Text.Show.Pretty

import           Database.LSMTree.Extras

-- We should be able to write this benchmark
-- using only use public lsm-tree interface
import qualified Database.LSMTree.Normal as LSM
import qualified Database.LSMTree.Internal.RawBytes as RB
import           Database.LSMTree.Extras.Orphans ()

-------------------------------------------------------------------------------
-- Keys and values
-------------------------------------------------------------------------------

type K = RB.RawBytes
type V = BS.ShortByteString
type B = Void

instance LSM.Labellable (K, V, B) where
  makeSnapshotLabel _ = "K V B"

-- We generate keys by hashing a word64 and adding two "random" bytes.
-- This way we can ensure that keys are distinct.
--
-- I think this approach of generating keys should match UTxO quite well.
-- This is purely CPU bound operation, and we should be able to push IO
-- when doing these in between.
makeKey :: Int -> K
makeKey seed =
    case P.runPrimArray $ do
           v <- P.newPrimArray 4
           let g0 = mkStdGen seed
           let (!w0, !g1) = Random.genWord64 g0
           P.writePrimArray v 0 w0
           let (!w1, !g2) = Random.genWord64 g1
           P.writePrimArray v 1 w1
           let (!w2, !g3) = Random.genWord64 g2
           P.writePrimArray v 2 w2
           let (!w3, _g4) = Random.genWord64 g3
           P.writePrimArray v 3 w3
           return v
 
      of (P.PrimArray ba :: P.PrimArray Word64) ->
           RB.RawBytes (VP.Vector 0 32 (P.ByteArray ba))

{-
makeKey :: Word64 -> K
makeKey w64 = BS.toShort (SHA256.hashlazy (B.encode w64) <> "==")
-}

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
    , seed       :: !Int
    , pipelined  :: !Bool
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
                  (makeKey i, theValue, Nothing)
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

    let g0 = Random.mkStdGen (seed opts)

    keysRef <- newIORef $
        if check opts
        then IS.fromList [ 0 .. (initialSize gopts) - 1 ]
        else IS.empty
    duplicateRef <- newIORef (0 :: Int)

    void $ forFoldM_ g0 [ 0 .. batchCount opts - 1 ] $ \b g -> do
        let lookups :: VU.Vector Int
            inserts :: VU.Vector Int
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
                                          (batchCount opts) (mkStdGen (seed opts)))
         in putStrLn $ "Any adjacent batches with overlap: " ++ show anyOverlap

-------------------------------------------------------------------------------
-- Batch generation
-------------------------------------------------------------------------------

generateBatch
    :: Int       -- ^ initial size of the collection
    -> Int       -- ^ batch size
    -> StdGen   -- ^ generator
    -> Int       -- ^ batch number
    -> (StdGen, V.Vector K, V.Vector (K, LSM.Update V B))
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
    -> StdGen    -- ^ generator
    -> Int       -- ^ batch number
    -> (StdGen, VU.Vector Int, VU.Vector Int)
generateBatch' initialSize batchSize g b = (g'', lookups, inserts)
  where
    maxK :: Int
    maxK = initialSize + batchSize * b

    lookups :: VU.Vector Int
    (lookups, !g'') =
       runState (VU.replicateM batchSize (state (Random.uniformR (0, maxK)))) g

    inserts :: VU.Vector Int
    inserts = VU.enumFromTo maxK (maxK + batchSize - 1)

-- | Generate operation inputs
--TODO: 82% of benchmark allocations are from this function (makeKey)
{-# INLINE toOperations #-}
toOperations :: VU.Vector Int -> VU.Vector Int -> (V.Vector K, V.Vector (K, LSM.Update V B))
toOperations lookups inserts = (batch1, batch2)
  where
    batch1 :: V.Vector K
    batch1 = V.map makeKey (V.convert lookups)

    batch2 :: V.Vector (K, LSM.Update V B)
    batch2 = V.map (\k -> (k, LSM.Delete)) batch1
        V.++ V.map (\k -> (makeKey k, LSM.Insert theValue Nothing))
                          (V.convert inserts)

-- | Manage creation of batches of LSM operations for the benchmark.
--
-- The 'withKeyBatch' has a special rule which is that the vectors passed to
-- the action /must not escape/ the body of the action.
--
-- The implementation is not kosher. We construct mutable string buffers
-- ('kbLookupKeyData' and 'kbInsertKeyData'), we arrange for the pure vectors
-- of operations to use the mutable strings. Then we mutate the strings for
-- each batch, and then hand out the modified vectors of operations.
data MOpBatches = MOpBatches {
                    kbInitialSize   :: !Int,
                    kbBatchSize     :: !Int,
                    kbLookupKeyData :: !(VP.MVector RealWorld Word64),
                    kbInsertKeyData :: !(VP.MVector RealWorld Word64),
                    kbLookupKeys    :: !(V.Vector K),
                    kbUpdateOps     :: !(V.Vector (K, LSM.Update V B))
                  }

initOpBatches :: Int -> Int -> IO MOpBatches
initOpBatches = \kbInitialSize kbBatchSize -> do
    kbLookupKeyData <- VPM.new (4 * kbBatchSize)
    kbInsertKeyData <- VPM.new (4 * kbBatchSize)
    VP.Vector _ _ lba <- VP.unsafeFreeze kbLookupKeyData
    let !kbLookupKeys = V.generate kbBatchSize $ \i ->
                          RB.RawBytes (VP.Vector (i*32) 32 lba)
    VP.Vector _ _ iba <- VP.unsafeFreeze kbInsertKeyData
    let !kbInsertKeys = V.generate kbBatchSize $ \i ->
                          RB.RawBytes (VP.Vector (i*32) 32 iba)
        !kbUpdateOps  = V.map (\k -> (k, LSM.Delete)) kbLookupKeys
                   V.++ V.map (\k -> (k, insertTheValue)) kbInsertKeys


    return $! MOpBatches{..}
  where
    insertTheValue = LSM.Insert theValue Nothing

{-# INLINE withOpBatch #-}
withOpBatch :: MOpBatches
            -> StdGen    -- ^ generator
            -> Int       -- ^ batch number
            -> (StdGen -> V.Vector K
                       -> V.Vector (K, LSM.Update V B)
                       -> IO a)
            -> IO a
withOpBatch MOpBatches{..} g0 b action = do
    gn <- genLookups 0 g0
    genInserts 0 maxK
    action gn kbLookupKeys kbUpdateOps
  where
    maxK :: Int
    !maxK = kbInitialSize + kbBatchSize * b

    genLookups !i !g | i == kbBatchSize = return g
    genLookups !i !g = do
      let (!ki, !g') = Random.uniformR (0, maxK) g
      stToIO $ makeKeyM ki (VPM.slice (4 * i) 4 kbLookupKeyData)
      genLookups (i+1) g'

    genInserts !i !_  | i == kbBatchSize = return ()
    genInserts !i !ki = do
      stToIO $ makeKeyM ki (VPM.slice (4 * i) 4 kbInsertKeyData)
      genInserts (i+1) (ki+1)

{-# NOINLINE makeKeyM #-} 
makeKeyM :: Int -> VP.MVector s Word64 -> ST s ()
makeKeyM !seed !v = assert (VPM.length v == 4) $ do
      let !g0 = Random.SM.mkSMGen (fromIntegral seed)
      let (!w0, !g1) = Random.SM.nextWord64 g0
      VPM.write v 0 w0
      let (!w1, !g2) = Random.SM.nextWord64 g1
      VPM.write v 1 w1
      let (!w2, !g3) = Random.SM.nextWord64 g2
      VPM.write v 2 w2
      let (!w3, _g4) = Random.SM.nextWord64 g3
      VPM.write v 3 w3
      return ()

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

    LSM.withSession (mkTracer gopts) hasFS hasBlockIO (FS.mkFsPath []) $ \session -> do
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
                                 (batchCount opts) (mkStdGen (seed opts))
        let fcheck | not (check opts) = \_ _ -> return ()
                  | otherwise = \b y -> do
              (x:xs) <- readIORef checkvar
              unless (x == y) $
                fail $ "lookup result mismatch in batch " ++ show b
              writeIORef checkvar xs

        let benchmarkIterations
              | pipelined opts = pipelinedIterations
              | otherwise      = sequentialIterations
            !progressInterval  = max 1 ((batchCount opts) `div` 100)
            madeProgress b     = b `mod` progressInterval == 0
        (time, _, _) <- timed_ $ do
          benchmarkIterations
            (\b y -> fcheck b y >> when (madeProgress b) (putChar '.'))
            (initialSize gopts)
            (batchSize opts)
            (batchCount opts)
            (mkStdGen (seed opts))
            tbl
          putStrLn ""

        let ops = batchCount opts * batchSize opts
        printf "Operations per second: %7.01f ops/sec\n" (fromIntegral ops / time)

-------------------------------------------------------------------------------
-- sequential
-------------------------------------------------------------------------------

type LookupResults = V.Vector (K, LSM.LookupResult V ())

{-# INLINE sequentialIteration #-}
sequentialIteration :: (Int -> LookupResults -> IO ())
                    -> MOpBatches
                    -> LSM.TableHandle IO K V B
                    -> Int
                    -> StdGen
                    -> IO StdGen
sequentialIteration output keybatches !tbl !b !g =
  withOpBatch keybatches g b $ \g' ls is -> do

    -- lookups
    results <- LSM.lookups ls tbl
    output b (V.zip ls (fmap (fmap (const ())) results))

    -- deletes and inserts
    --LSM.updates is tbl
    
    -- continue to next batch
    return g'

sequentialIterations :: (Int -> LookupResults -> IO ())
                     -> Int -> Int -> Int -> StdGen
                     -> LSM.TableHandle IO K V B
                     -> IO ()
sequentialIterations output !initialSize !batchSize !batchCount !g0 !tbl = do
    keybatches <- initOpBatches initialSize batchSize
    void $ forFoldM_ g0 [ 0 .. batchCount - 1 ] $ \b g ->
      sequentialIteration output keybatches tbl b g

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
pipelinedIteration :: (Int -> LookupResults -> IO ())
                   -> MOpBatches
                   -> MVar (LSM.TableHandle IO K V B, Map K (LSM.Update V B))
                   -> MVar (LSM.TableHandle IO K V B, Map K (LSM.Update V B))
                   -> MVar StdGen
                   -> MVar StdGen
                   -> LSM.TableHandle IO K V B
                   -> Int
                   -> IO (LSM.TableHandle IO K V B)
pipelinedIteration output opbatches
                   !syncTblIn !syncTblOut
                   !syncRngIn !syncRngOut
                   !tbl_n !b = do
  g <- takeMVar syncRngIn
  withOpBatch opbatches g b $ \ !g' !ls !is -> do

    -- 1: perform the lookups
    lrs <- LSM.lookups ls tbl_n

    -- 2. sync: receive updates and new table handle
    putMVar syncRngOut g'
    (tbl_n1, delta) <- takeMVar syncTblIn

    -- At this point, after syncing, our peer is guaranteed to no longer be
    -- using tbl_n. They used it to generate tbl_n+1 (which they gave us).
    LSM.close tbl_n
    output b $! applyUpdates delta (V.zip ls lrs)

    -- 3. perform the inserts and report outputs (in any order)
    tbl_n2 <- LSM.duplicate tbl_n1
    LSM.updates is tbl_n2

    -- 4. sync: send the updates and new table handle
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

pipelinedIterations :: (Int -> LookupResults -> IO ())
                    -> Int -> Int -> Int -> StdGen
                    -> LSM.TableHandle IO K V B
                    -> IO ()
pipelinedIterations output !initialSize !batchSize !batchCount !g0 tbl_0 = do
    n <- getNumCapabilities
    printf "INFO: the pipelined benchmark is running with %d capabilities.\n" n

    opbatches <- initOpBatches initialSize batchSize

    syncTblA2B <- newEmptyMVar
    syncTblB2A <- newEmptyMVar
    syncRngA2B <- newEmptyMVar
    syncRngB2A <- newEmptyMVar

    tbl_1 <- LSM.duplicate tbl_0
    let prelude =
          withOpBatch opbatches g0 0 $ \ !g1 !ls0 !is0 -> do
            lrs0 <- LSM.lookups ls0 tbl_0
            output 0 $! V.zip ls0 (fmap (fmap (const ())) lrs0)
            LSM.updates is0 tbl_1
            let !delta = Map.fromList (V.toList is0)
            putMVar syncTblA2B (tbl_1, delta)
            putMVar syncRngA2B g1

        threadA =
          forFoldM_ tbl_1 [ 2, 4 .. batchCount - 1 ] $ \b tbl_n ->
            pipelinedIteration output opbatches
                               syncTblB2A syncTblA2B -- in, out
                               syncRngB2A syncRngA2B -- in, out
                               tbl_n b

        threadB =
          forFoldM_ tbl_0 [ 1, 3 .. batchCount - 1 ] $ \b tbl_n ->
            pipelinedIteration output opbatches
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

pureReference :: Int -> Int -> Int -> StdGen -> [V.Vector (K, LSM.LookupResult V ())]
pureReference !initialSize !batchSize !batchCount !g0 =
    generate g0 Map.empty 0
  where
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
batchOverlaps :: Int -> Int -> Int -> StdGen -> [IS.IntSet]
batchOverlaps initialSize batchSize batchCount g0 =
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

intSetFromVector :: VU.Vector Int -> IS.IntSet
intSetFromVector = VU.foldl' (\acc x -> IS.insert x acc) IS.empty

-------------------------------------------------------------------------------
-- unused for now
-------------------------------------------------------------------------------

_unused :: ()
_unused = const ()
    timed
