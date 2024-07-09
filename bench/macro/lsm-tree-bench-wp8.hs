{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE NoFieldSelectors      #-}
{-# LANGUAGE OverloadedRecordDot   #-}
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
TODO 2024-07-05 pipelined mode needs the 'duplicate' operation. It has been
                tested for correctness with the model implementation.
-}
module Main (main) where

import           Control.Applicative ((<**>))
import           Control.Concurrent.Async
import           Control.Concurrent.MVar
import           Control.DeepSeq (force)
import           Control.Exception (evaluate)
import           Control.Monad (forM_, unless, void, when)
import qualified Crypto.Hash.SHA256 as SHA256
import qualified Data.Binary as B
import qualified Data.ByteString.Short as BS
import qualified Data.IntSet as IS
import           Data.IORef (modifyIORef', newIORef, readIORef, writeIORef)
import           Data.List (foldl')
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import           Data.Traversable (mapAccumL)
import           Data.Tuple (swap)
import qualified Data.Vector as V
import           Data.Void (Void)
import           Data.Word (Word64)
import qualified MCG
import qualified Options.Applicative as O
import           Prelude hiding (lookup)
import qualified System.Clock as Clock
import qualified System.FS.API as FS
import qualified System.FS.BlockIO.API as FS
import qualified System.FS.BlockIO.IO as FsIO
import qualified System.FS.IO as FsIO
import           Text.Printf (printf)

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

-- We generate keys by hashing a word64 and adding two "random" bytes.
-- This way we can ensure that keys are distinct.
--
-- I think this approach of generating keys should match UTxO quite well.
-- This is purely CPU bound operation, and we should be able to push IO
-- when doing these in between.
makeKey :: Word64 -> K
makeKey w64 = BS.toShort (SHA256.hashlazy (B.encode w64) <> "==")

-- We use constant value. This shouldn't affect anything.
theValue :: V
theValue = BS.replicate 60 120 -- 'x'
{-# NOINLINE theValue #-}

-------------------------------------------------------------------------------
-- Options and commands
-------------------------------------------------------------------------------

data GlobalOpts = GlobalOpts
    { rootDir     :: !FilePath  -- ^ session directory.
    , initialSize :: !Int
    }
  deriving Show

data SetupOpts = SetupOpts
  deriving Show

data RunOpts = RunOpts
    { batchCount :: !Int
    , batchSize  :: !Int
    , check      :: !Bool
    , seed       :: !Word64
    , pipelined  :: !Bool
    }
  deriving Show

data Cmd
    -- | Setup benchmark: generate initial LSM tree etc.
    = CmdSetup SetupOpts

    -- | Make a dry run, measure the overhead.
    | CmdDryRun RunOpts

    -- | Run the actual benchmark
    | CmdRun RunOpts
  deriving Show

-------------------------------------------------------------------------------
-- command line interface
-------------------------------------------------------------------------------

globalOptsP :: O.Parser GlobalOpts
globalOptsP = pure GlobalOpts
    <*> pure "_bench_session"
    <*> O.option O.auto (O.long "initial-size" <> O.value 100_000_000 <> O.showDefault <> O.help "Initial LSM tree size")

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

runOptsP :: O.Parser RunOpts
runOptsP = pure RunOpts
    <*> O.option O.auto (O.long "batch-count" <> O.value 200 <> O.showDefault <> O.help "Batch count")
    <*> O.option O.auto (O.long "batch-size" <> O.value 256 <> O.showDefault <> O.help "Batch size")
    <*> O.switch (O.long "check" <> O.help "Check generated key distribution")
    <*> O.option O.auto (O.long "seed" <> O.value 1337 <> O.showDefault <> O.help "Random seed")
    <*> O.switch (O.long "pipelined" <> O.help "Use pipelined mode")

-------------------------------------------------------------------------------
-- clock
-------------------------------------------------------------------------------

timed :: IO a -> IO (a, Double)
timed action = do
    t1 <- Clock.getTime Clock.Monotonic
    x  <- action
    t2 <- Clock.getTime Clock.Monotonic
    let !t = fromIntegral (Clock.toNanoSecs (Clock.diffTimeSpec t2 t1)) * 1e-9
    return (x, t)

timed_ :: IO () -> IO Double
timed_ action = do
    t1 <- Clock.getTime Clock.Monotonic
    action
    t2 <- Clock.getTime Clock.Monotonic
    return $! fromIntegral (Clock.toNanoSecs (Clock.diffTimeSpec t2 t1)) * 1e-9

-------------------------------------------------------------------------------
-- setup
-------------------------------------------------------------------------------

-- https://input-output-hk.github.io/fs-sim
doSetup :: GlobalOpts -> SetupOpts -> IO ()
doSetup gopts _opts = do
    let mountPoint :: FS.MountPoint
        mountPoint = FS.MountPoint gopts.rootDir

    let hasFS :: FS.HasFS IO FsIO.HandleIO
        hasFS = FsIO.ioHasFS mountPoint

    hasBlockIO <- FsIO.ioHasBlockIO hasFS FS.defaultIOCtxParams

    name <- maybe (fail "invalid snapshot name") return $
        LSM.mkSnapshotName "bench"

    LSM.withSession hasFS hasBlockIO (FS.mkFsPath []) $ \session -> do
        tbh <- LSM.new @IO @K @V @B session LSM.defaultTableConfig

        forM_ [ 0 .. gopts.initialSize ] $ \ (fromIntegral -> i) -> do
            -- TODO: this procedure simply inserts all the keys into initial lsm tree
            -- We might want to do deletes, so there would be delete-insert pairs
            -- Let's do that when we can actually test that benchmark works.

            let k = makeKey i
            let v = theValue

            -- TODO: LSM.inserts has annoying order
            flip LSM.inserts tbh $
              V.singleton (k, v, Nothing)

        LSM.snapshot name tbh

-------------------------------------------------------------------------------
-- dry-run
-------------------------------------------------------------------------------

doDryRun :: GlobalOpts -> RunOpts -> IO ()
doDryRun gopts opts = do
    time <- timed_ $ doDryRun' gopts opts
    printf "Batch generation: %.03f sec\n" time

doDryRun' :: GlobalOpts -> RunOpts -> IO ()
doDryRun' gopts opts = do
    -- calculated some expected statistics for generated batches
    id $ do
        -- we generate n random numbers in range of [ 1 .. d ]
        -- what is the chance they are all distinct
        let n = fromIntegral (opts.batchCount * opts.batchSize) :: Double
        let d = fromIntegral gopts.initialSize :: Double
        -- this is birthday problem.
        let p = 1 - exp (negate $  (n * (n - 1)) / (2 * d))

        -- number of people with a shared birthday
        -- https://en.wikipedia.org/wiki/Birthday_problem#Number_of_people_with_a_shared_birthday
        let q = n * (1 - ((d - 1) / d) ** (n - 1))

        printf "Probability of a duplicate:                          %5f\n" p
        printf "Expected number of duplicates (extreme upper bound): %5f out of %f\n" q n

    -- TODO: open session to measure that as well.
    let g0 = initGen gopts.initialSize opts.batchSize opts.batchCount opts.seed

    keysRef <- newIORef $
        if opts.check
        then IS.fromList [ 0 .. gopts.initialSize - 1 ]
        else IS.empty
    duplicateRef <- newIORef (0 :: Int)

    void $ forFoldM_ g0 [ 0 .. opts.batchCount - 1 ] $ \b g -> do
        let lookups :: V.Vector Word64
            inserts :: V.Vector Word64
            (!g', lookups, inserts) = generateBatch' gopts.initialSize opts.batchSize g b

        when opts.check $ do
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

    when opts.check $ do
        duplicates <- readIORef duplicateRef
        printf "True duplicates: %d\n" duplicates

    -- See batchOverlaps for explanation of this check.
    when opts.check $
        let anyOverlap = (not . null)
                           (batchOverlaps gopts.initialSize opts.batchSize
                                          opts.batchCount opts.seed)
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

{- | Implement generation of unbounded sequence of insert/delete operations

matching UTxO style from spec: interleaved batches insert and lookup
configurable batch sizes
1 insert, 1 delete, 1 lookup per key.

Current approach is probabilistic, but uses very little state.
We could also make it exact, but then we'll need to carry some state around
(at least the difference).

-}
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
    (!g'', lookups) = mapAccumL (\g' _ -> swap (MCG.reject maxK g'))
                                g (V.enumFromTo 1 batchSize)

    inserts :: V.Vector Word64
    inserts = V.enumFromTo maxK (maxK + fromIntegral batchSize - 1)

-- | Generate operation inputs
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
        mountPoint = FS.MountPoint gopts.rootDir

    let hasFS :: FS.HasFS IO FsIO.HandleIO
        hasFS = FsIO.ioHasFS mountPoint

    hasBlockIO <- FsIO.ioHasBlockIO hasFS FS.defaultIOCtxParams

    name <- maybe (fail "invalid snapshot name") return $
        LSM.mkSnapshotName "bench"

    LSM.withSession hasFS hasBlockIO (FS.mkFsPath []) $ \session -> do
        -- open snapshot
        -- In checking mode we start with an empty table, since our pure
        -- reference version starts with empty (as it's not practical or
        -- necessary for testing to load the whole snapshot).
        tbl <- if opts.check
                 then LSM.new  @IO @K @V @B session LSM.defaultTableConfig
                 else LSM.open @IO @K @V @B session name

        -- In checking mode, compare each output against a pure reference.
        checkvar <- newIORef $ pureReference
                                 gopts.initialSize opts.batchSize
                                 opts.batchCount opts.seed
        let check | not opts.check = \_ _ -> return ()
                  | otherwise = \b y -> do
              (x:xs) <- readIORef checkvar
              unless (x == y) $
                fail $ "lookup result mismatch in batch " ++ show b
              writeIORef checkvar xs

        let benchmarkIterations
              | opts.pipelined = pipelinedIterations
              | otherwise      = sequentialIterations
        time <- timed_ $
          benchmarkIterations
            check
            gopts.initialSize
            opts.batchSize
            opts.batchCount
            opts.seed
            tbl

        printf "Proper run:            %.03f sec\n" time
        let ops = opts.batchCount * opts.batchSize
        printf "Operations per second: %7.01f ops/sec\n" (fromIntegral ops / time)
        -- TODO: collect more statistic, save them in dry-run,
        -- TODO: make the results human comprehensible.


-------------------------------------------------------------------------------
-- sequential
-------------------------------------------------------------------------------

type LookupResults = V.Vector (K, LSM.LookupResult V ())

{-# INLINE sequentialIteration #-}
sequentialIteration :: (Int -> LookupResults -> IO ())
                    -> Int
                    -> Int
                    -> LSM.TableHandle IO K V B
                    -> Int
                    -> MCG.MCG
                    -> IO MCG.MCG
sequentialIteration output !initialSize !batchSize !tbl !b !g = do
    let (!g', ls, is) = generateBatch initialSize batchSize g b

    -- lookups
    results <- LSM.lookups ls tbl
    output b (V.zip ls (fmap (fmap (const ())) results))

    -- deletes and inserts
    LSM.updates is tbl

    -- continue to the next batch
    return g'

sequentialIterations :: (Int -> LookupResults -> IO ())
                     -> Int -> Int -> Int -> Word64
                     -> LSM.TableHandle IO K V B
                     -> IO ()
sequentialIterations output !initialSize !batchSize !batchCount !seed !tbl =
    void $ forFoldM_ g0 [ 0 .. batchCount - 1 ] $ \b g ->
      sequentialIteration output initialSize batchSize tbl b g
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
pipelinedIteration :: (Int -> LookupResults -> IO ())
                   -> Int
                   -> Int
                   -> MVar (LSM.TableHandle IO K V B, Map K (LSM.Update V B))
                   -> MVar (LSM.TableHandle IO K V B, Map K (LSM.Update V B))
                   -> MVar MCG.MCG
                   -> MVar MCG.MCG
                   -> LSM.TableHandle IO K V B
                   -> Int
                   -> IO (LSM.TableHandle IO K V B)
pipelinedIteration output !initialSize !batchSize
                   !syncTblIn !syncTblOut
                   !syncRngIn !syncRngOut
                   !tbl_n !b = do
    g <- takeMVar syncRngIn
    let (!g', !ls, !is) = generateBatch initialSize batchSize g b

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
                    -> Int -> Int -> Int -> Word64
                    -> LSM.TableHandle IO K V B
                    -> IO ()
pipelinedIterations output !initialSize !batchSize !batchCount !seed tbl_0 = do
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
            pipelinedIteration output initialSize batchSize
                               syncTblB2A syncTblA2B -- in, out
                               syncRngB2A syncRngA2B -- in, out
                               tbl_n b

        threadB =
          forFoldM_ tbl_0 [ 1, 3 .. batchCount - 1 ] $ \b tbl_n ->
            pipelinedIteration output initialSize batchSize
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
        !m'      = foldl' (flip (uncurry Map.insert)) m inserts

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
-- main
-------------------------------------------------------------------------------

main :: IO ()
main = do
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
forFoldM_ !s []     _ = return s
forFoldM_ !s (x:xs) f = do
    !s' <- f x s
    forFoldM_ s' xs f

intSetFromVector :: V.Vector Word64 -> IS.IntSet
intSetFromVector = V.foldl' (\acc x -> IS.insert (fromIntegral x) acc) IS.empty

-------------------------------------------------------------------------------
-- unused for now
-------------------------------------------------------------------------------

_unused :: ()
_unused = const ()
    timed
