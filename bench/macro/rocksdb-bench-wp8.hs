{-# LANGUAGE DuplicateRecordFields    #-}
{-# LANGUAGE NondecreasingIndentation #-}
{-# LANGUAGE OverloadedStrings        #-}

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
TODO 2024-04-29 pipelined mode is not implemented.

-}
module Main (main) where

import           Control.Applicative ((<**>))
import           Control.DeepSeq (force)
import           Control.Exception (evaluate)
import           Control.Monad (forM, forM_, void, when)
import qualified Crypto.Hash.SHA256 as SHA256
import qualified Data.Binary as B
import qualified Data.ByteString as BS
import qualified Data.IntSet as IS
import           Data.IORef (modifyIORef', newIORef, readIORef, writeIORef)
import           Data.List.Split (chunksOf)
import           Data.Traversable (mapAccumL)
import           Data.Tuple (swap)
import           Data.Word (Word64)
import qualified MCG
import qualified Options.Applicative as O
import qualified System.Clock as Clock
import           System.Directory (removePathForcibly)
import           Text.Printf (printf)

import qualified RocksDB

-------------------------------------------------------------------------------
-- Keys and values
-------------------------------------------------------------------------------

type K = BS.ByteString
type V = BS.ByteString

-- We generate keys by hashing a word64 and adding two "random" bytes.
-- This way we can ensure that keys are distinct.
--
-- I think this approach of generating keys should match UTxO quite well.
-- This is purely CPU bound operation, and we should be able to push IO
-- when doing these in between.
makeKey :: Word64 -> K
makeKey w64 = SHA256.hashlazy (B.encode w64) <> "=="

makeValue :: K -> V
makeValue k = k <> BS.replicate (60 - 34) 120 -- 'x'

-------------------------------------------------------------------------------
-- Options and commands
-------------------------------------------------------------------------------

data GlobalOpts = GlobalOpts
    { rootDir     :: !FilePath  -- ^ session directory.
    , initialSize :: !Int
    }
  deriving stock Show

data SetupOpts = SetupOpts
  deriving stock Show

data RunOpts = RunOpts
    { batchCount :: !Int
    , batchSize  :: !Int
    , check      :: !Bool
    , seed       :: !Word64
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

-------------------------------------------------------------------------------
-- command line interface
-------------------------------------------------------------------------------

globalOptsP :: O.Parser GlobalOpts
globalOptsP = pure GlobalOpts
    <*> pure "_bench_rocksdb"
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
    <*> O.option O.auto (O.long "batch-count" <> O.value 1000 <> O.showDefault <> O.help "Batch count")
    <*> O.option O.auto (O.long "batch-size" <> O.value 256 <> O.showDefault <> O.help "Batch size")
    <*> O.switch (O.long "check" <> O.help "Check generated key distribution")
    <*> O.option O.auto (O.long "seed" <> O.value 1337 <> O.showDefault <> O.help "Random seed")

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

{-
rocksdb::BlockBasedTableOptions table_options;
  table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
  my_cf_options.table_factory.reset(
      rocksdb::NewBlockBasedTableFactory(table_options));
-}

rocksDbWithOptions :: (RocksDB.Options -> IO r) -> IO r
rocksDbWithOptions kont =
    RocksDB.withOptions $ \options ->
    RocksDB.withFilterPolicyBloom 10 $ \filterPolicy ->
    RocksDB.withBlockTableOptions $ \topts -> do
        RocksDB.blockBasedOptionsSetFilterPolicy topts filterPolicy

        RocksDB.optionsSetCreateIfMissing options True
        RocksDB.optionsIncreaseParallelism options 4 -- the benchmark spec says to use one core though.
        RocksDB.optionsOptimizeLevelStyleCompaction options 0x200_00000 -- 32*1024*1024; default is 512*1024*1024?
        -- RocksDB.optionsSetCompression options 0 -- no compression
        RocksDB.optionsSetCompression options 4 -- lz4
        -- RocksDB.optionsSetCompression options 7 -- zstd
        RocksDB.optionsSetMaxOpenFiles options 512

        RocksDB.optionsSetBlockBasedTableFactory options topts

        kont options

rocksDbWithWriteOptions :: (RocksDB.WriteOptions -> IO r) -> IO r
rocksDbWithWriteOptions kont = RocksDB.withWriteOptions $ \options -> do
    -- lsm-tree doesn't have WAL, so we don't use with RocksDB to be fair.
    RocksDB.writeOptionsDisableWAL options True

    kont options

--
--
-- It's useful to run
--
-- sst_dump --file=_bench_rocksdb --command=identity --show_properties
--
-- after setup
doSetup :: GlobalOpts -> SetupOpts -> IO ()
doSetup gopts opts = do
    time <- timed_ $ doSetup' gopts opts
    printf "Setup %.03f sec\n" time

doSetup' :: GlobalOpts -> SetupOpts -> IO ()
doSetup' gopts _opts =
    rocksDbWithOptions $ \options ->
    RocksDB.withRocksDB options (rootDir gopts) $ \db ->
    rocksDbWithWriteOptions $ \wopts ->
    forM_ (chunksOf 256 [ 0 .. initialSize gopts ]) $ \chunk ->
    RocksDB.withWriteBatch $ \batch -> do
        forM_ chunk $ \ (fromIntegral -> i) -> do
            when (mod i (fromIntegral (div (initialSize gopts) 50)) == 0) $ do
                printf "%3.0f%%\n" (100 * fromIntegral i / fromIntegral (initialSize gopts) :: Double)
            let k = makeKey i
            let v = makeValue k
            RocksDB.writeBatchPut batch k v

        RocksDB.write db wopts batch

-------------------------------------------------------------------------------
-- dry-run
-------------------------------------------------------------------------------

doDryRun :: GlobalOpts -> RunOpts -> IO ()
doDryRun gopts opts = do
    time <- timed_ $ doDryRun' gopts opts
    printf "Batch generation: %.03f sec\n" time

doDryRun' :: GlobalOpts -> RunOpts -> IO ()
doDryRun' gopts opts = do
    keysRef <- newIORef $
        if (check opts)
        then IS.fromList [ 0 .. initialSize gopts - 1 ]
        else IS.empty
    duplicateRef <- newIORef (0 :: Int)

    void $ forFoldM_ initGen [ 0 .. batchCount opts - 1 ] $ \b g -> do
        let lookups :: [Word64]
            inserts :: [Word64]
            (!nextG, lookups, inserts) = generateBatch (initialSize gopts) (batchSize opts) g b

        when (check opts) $ do
            keys <- readIORef keysRef
            let new  = IS.fromList $ map fromIntegral lookups
            let diff = IS.difference new keys
            -- when (IS.notNull diff) $ printf "missing in batch %d %s\n" b (show diff)
            modifyIORef' duplicateRef $ \n -> n + IS.size diff
            writeIORef keysRef $! IS.union
                (IS.difference keys new)
                (IS.fromList $ map fromIntegral inserts)

        -- lookups
        forM_ lookups $ \k -> evaluate (makeKey k)

        -- deletes & inserts; deletes done above.
        forM_ inserts $ \k' -> do
            let k = makeKey k'
            evaluate k >> evaluate (makeValue k)

        return nextG

    when (check opts) $ do
        duplicates <- readIORef duplicateRef
        printf "True duplicates: %d\n" duplicates
  where
    initGen = MCG.make
        (fromIntegral $ initialSize gopts + batchSize opts * batchCount opts)
        (seed opts)

-------------------------------------------------------------------------------
-- Batch generation
-------------------------------------------------------------------------------

{- | Implement generation of unbounded sequence of insert/delete operations

matching UTxO style from spec: interleaved batches insert and lookup
configurable batch sizes
1 insert, 1 delete, 1 lookup per key.

Current approach is probabilistic, but uses very little state.
We could also make it exact, but then we'll need to carry some state around
(at least the difference).

-}
generateBatch
    :: Int       -- ^ initial size of the collection
    -> Int       -- ^ batch size
    -> MCG.MCG   -- ^ generator
    -> Int       -- ^ batch number
    -> (MCG.MCG, [Word64], [Word64])
generateBatch initialSize batchSize g b = (nextG, lookups, inserts)
  where
    maxK :: Word64
    maxK = fromIntegral $ initialSize + batchSize * b

    lookups :: [Word64]
    (!nextG, lookups) = mapAccumL (\g' _ -> swap (MCG.reject maxK g')) g [1 .. batchSize]

    inserts :: [Word64]
    inserts = [ maxK .. maxK + fromIntegral batchSize - 1 ]

-------------------------------------------------------------------------------
-- run
-------------------------------------------------------------------------------

doRun :: GlobalOpts -> RunOpts -> IO ()
doRun gopts opts = do
    removePathForcibly $ rootDir gopts ++ "_cp"
    makeCheckpoint gopts

    time <- timed_ $ doRun' gopts { rootDir = rootDir gopts ++ "_cp" } opts
    -- TODO: collect more statistic, save them in dry-run,
    -- TODO: make the results human comprehensible.
    printf "Proper run:            %7.03f sec\n" time
    let ops = batchCount opts * batchSize opts
    printf "Operations per second: %7.01f ops/sec\n" (fromIntegral ops / time)

makeCheckpoint :: GlobalOpts -> IO ()
makeCheckpoint gopts =
    rocksDbWithOptions $ \options ->
    RocksDB.withRocksDB options (rootDir gopts) $ \db ->
    RocksDB.checkpoint db $ rootDir gopts ++ "_cp"

doRun' :: GlobalOpts -> RunOpts -> IO ()
doRun' gopts opts =
    rocksDbWithOptions $ \options ->
    RocksDB.withRocksDB options (rootDir gopts) $ \db ->
    rocksDbWithWriteOptions $ \wopts ->
    RocksDB.withReadOptions $ \ropts ->
        void $ forFoldM_ initGen [ 0 .. batchCount opts - 1 ] $ \b g -> do
            let lookups :: [Word64]
                inserts :: [Word64]
                (!nextG, lookups, inserts) = generateBatch (initialSize gopts) (batchSize opts) g b

            -- lookups
            let ks = makeKey <$> lookups

            vs' <-
                -- multi get or not.
                if True
                then RocksDB.multiGet db ropts ks
                else forM ks (RocksDB.get db ropts)

            vs <- evaluate (force vs')

            -- check that we get values we expect
            when (check opts) $ do
                let expected = map (Just . makeValue) ks
                when (vs /= expected) $ do
                    printf "Value mismatch in batch %d\n" b
                    print vs
                    print expected

             -- deletes and inserts
            RocksDB.withWriteBatch $ \batch -> do
                forM_ ks $ \k -> do
                    RocksDB.writeBatchDelete batch k

                forM_ inserts $ \k' -> do
                    let k = makeKey k'
                    let v = makeValue k
                    RocksDB.writeBatchPut batch k v

                RocksDB.write db wopts batch

            return nextG
  where
    initGen = MCG.make
        (fromIntegral $ initialSize gopts + batchSize opts * batchCount opts)
        (seed opts)

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

-------------------------------------------------------------------------------
-- unused for now
-------------------------------------------------------------------------------

_unused :: ()
_unused = const ()
    timed
