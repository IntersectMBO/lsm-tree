-- | Tests for the concurrency guarantees documented in the "Concurrency"
-- section of "Database.LSMTree.Simple":
--
-- * it is not a race to read and read the same table concurrently
--   ('prop_concurrentLookups', 'prop_concurrentReadOperations');
--
-- * it is not a race to read or write separate tables concurrently
--   ('prop_concurrentWritesSeparateTables',
--   'prop_readWhileWritingSeparateTable');
--
-- * a duplicate is a stable view of a table that can be read while the
--   original table is written to ('prop_readDuplicateWhileWriting');
--
-- * a cursor is a stable view of a table that can be read while the original
--   table is written to ('prop_readCursorWhileWriting'), and concurrent reads
--   on the same cursor block each other ('prop_concurrentCursorReads');
--
-- * a session can be used from multiple threads concurrently
--   ('prop_concurrentSessionUse').
--
-- The property bodies are polymorphic in the monad, so the same code runs in:
--
-- * 'IO', against the real file system, with the scheduling of the GHC RTS;
--
-- * 'IOSim', against the mock file system, with a deterministic schedule, so a
--   failure can be reproduced from the QuickCheck seed alone;
--
-- * IOSimPOR, which explores alternative schedules by reversing races
--   between threads.
--
-- 'IOSim' switches between threads when an STM transaction commits (which
-- includes the 'MVar's, and the mock file system used by these tests), at
-- blocking operations, and at a few other points such as 'yield' and thread
-- exit. Operations on 'MutVar's and 'PrimVar's (lifted 'ST' actions) never
-- cause a thread switch, and IOSimPOR does not see races on them.
module Test.Database.LSMTree.Concurrency (tests) where

import           Control.Monad (forM)
import           Control.Monad.Class.MonadAsync (concurrently, forConcurrently,
                     mapConcurrently, replicateConcurrently)
import           Control.Monad.Class.MonadTest (exploreRaces)
import           Control.Monad.IOSim (IOSim, SimTrace, exploreSimTrace,
                     runSimTrace, traceResult, withScheduleBound)
import           Control.Tracer (nullTracer)
import           Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as BSC
import           Data.List (sort, sortOn)
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import           Data.String (fromString)
import qualified Data.Vector as V
import qualified System.FS.API as FS
import qualified System.FS.Sim.MockFS as MockFS

import           Database.LSMTree as R
import           Test.QuickCheck
import           Test.Tasty (TestTree, localOption, testGroup)
import           Test.Tasty.QuickCheck (QuickCheckMaxSize (..), testProperty)
import           Test.Util.FS (propNoOpenHandles, withSimHasBlockIO,
                     withTempIOHasBlockIO)
import           Text.Printf (printf)

tests :: TestTree
tests =
    testGroup "Test.Database.LSMTree.Concurrency" [
        testGroup "IO" [
            testProperty "prop_concurrentLookups" $ \n ->
              runInIO "prop_concurrentLookups" $
                prop_concurrentLookups testTableConfig n
          , testProperty "prop_concurrentReadOperations" $ \n ->
              runInIO "prop_concurrentReadOperations" $
                prop_concurrentReadOperations testTableConfig n
          , testProperty "prop_concurrentWritesSeparateTables" $ \n ->
              runInIO "prop_concurrentWritesSeparateTables" $
                prop_concurrentWritesSeparateTables testTableConfig n
          , testProperty "prop_readDuplicateWhileWriting" $ \n ->
              runInIO "prop_readDuplicateWhileWriting" $
                prop_readDuplicateWhileWriting testTableConfig n
          , testProperty "prop_readCursorWhileWriting" $ \n ->
              runInIO "prop_readCursorWhileWriting" $
                prop_readCursorWhileWriting testTableConfig n
          , testProperty "prop_concurrentCursorReads" $
              forAll (choose (2, 4)) $ \k n ->
              runInIO "prop_concurrentCursorReads" $
                prop_concurrentCursorReads testTableConfig k n
          , testProperty "prop_readWhileWritingSeparateTable" $ \n ->
              runInIO "prop_readWhileWritingSeparateTable" $
                prop_readWhileWritingSeparateTable testTableConfig n
          , testProperty "prop_concurrentSessionUse" $ \n ->
              runInIO "prop_concurrentSessionUse" $
                prop_concurrentSessionUse testTableConfig n
          ]
        -- IOSim is much slower than IO, so keep the inputs smaller.
      , localOption (QuickCheckMaxSize 30) $ testGroup "IOSim" [
            testProperty "prop_concurrentLookups" $ \n ->
              runInIOSim $ prop_concurrentLookups testTableConfig n
            -- Seven threads of reads, several of them over all keys, so keep
            -- the inputs smaller still.
          , localOption (QuickCheckMaxSize 10) $
            testProperty "prop_concurrentReadOperations" $ \n ->
              runInIOSim $ prop_concurrentReadOperations testTableConfig n
          , testProperty "prop_concurrentWritesSeparateTables" $ \n ->
              runInIOSim $ prop_concurrentWritesSeparateTables testTableConfig n
          , testProperty "prop_readDuplicateWhileWriting" $ \n ->
              runInIOSim $ prop_readDuplicateWhileWriting testTableConfig n
          , testProperty "prop_readCursorWhileWriting" $ \n ->
              runInIOSim $ prop_readCursorWhileWriting testTableConfig n
          , testProperty "prop_concurrentCursorReads" $
              forAll (choose (2, 4)) $ \k n ->
              runInIOSim $ prop_concurrentCursorReads testTableConfig k n
            -- A lookup, a range lookup and a write per key, so keep the inputs
            -- smaller still.
          , localOption (QuickCheckMaxSize 15) $
            testProperty "prop_readWhileWritingSeparateTable" $ \n ->
              runInIOSim $ prop_readWhileWritingSeparateTable testTableConfig n
            -- Every thread saves and opens a snapshot, which is slow in
            -- 'IOSim', so keep the inputs even smaller.
          , localOption (QuickCheckMaxSize 10) $
            testProperty "prop_concurrentSessionUse" $ \n ->
              runInIOSim $ prop_concurrentSessionUse testTableConfig n
          ]
        -- IOSimPOR runs the simulation once for every schedule it explores, so
        -- only run one small input of the properties that share the most
        -- state between threads: the runs and merges of a table and its
        -- duplicate or cursor, and the position of a cursor.
        -- 'prop_concurrentCursorReads' reads the cursor from two threads, not
        -- up to four as above: with four threads, 20 schedules are not enough
        -- to reach the ones in which the reads would interleave if 'R.next'
        -- did not lock the cursor.
        -- 'prop_readCursorWhileWriting' does two writes per key, so it gets
        -- one key fewer, to keep its run time similar.
        -- 'prop_concurrentReadOperations' and 'prop_concurrentSessionUse' are
        -- not run here: they have too many threads, or too much work per
        -- thread, for even a small input to finish in reasonable time.
      , testGroup "IOSimPOR" [
            testProperty "prop_readDuplicateWhileWriting" $
              once $
              runInIOSimPOR 20 $
                prop_readDuplicateWhileWriting
                  testTableConfig
                  (Positive (Small 4))
          , testProperty "prop_readCursorWhileWriting" $
              once $
              runInIOSimPOR 20 $
                prop_readCursorWhileWriting
                  testTableConfig
                  (Positive (Small 3))
          , testProperty "prop_concurrentCursorReads" $
              once $
              runInIOSimPOR 20 $
                prop_concurrentCursorReads
                  testTableConfig
                  2
                  (Positive (Small 4))
          ]
      ]

testSalt :: R.Salt
testSalt = 4

-- | A table configuration with a tiny write buffer and merge batch size, so
-- that even the small inputs that QuickCheck generates flush the write buffer
-- to disk and run merges, which are the parts of a table that are shared
-- between a table and its duplicates. With the default write buffer (20,000
-- entries) every entry would stay in memory and no run would reach disk.
testTableConfig :: TableConfig
testTableConfig = defaultTableConfig {
      confWriteBufferAlloc = AllocNumEntries 3
    , confMergeBatchSize   = MergeBatchSize 1
    }

{-------------------------------------------------------------------------------
  Running property bodies
-------------------------------------------------------------------------------}

-- | Run a property body in 'IO', with a fresh session in a temporary
-- directory.
runInIO :: String -> (Session IO -> IO Property) -> Property
runInIO name body =
    ioProperty $
    withTempIOHasBlockIO name $ \hfs hbio ->
    withOpenSession nullTracer hfs hbio testSalt (FS.mkFsPath []) body

-- | The simulated version of 'runInIO': a fresh session on an empty mock file
-- system. After the session is closed, there must be no open file handles.
simSession :: (Session (IOSim s) -> IOSim s Property) -> IOSim s Property
simSession body =
    withSimHasBlockIO propNoOpenHandles MockFS.empty $ \hfs hbio _fsVar ->
    withOpenSession nullTracer hfs hbio testSalt (FS.mkFsPath []) body

-- | Run a property body in 'IOSim', with IOSim's deterministic schedule.
runInIOSim :: (forall s. Session (IOSim s) -> IOSim s Property) -> Property
runInIOSim body = simTraceProperty (runSimTrace (simSession body))

-- | Run a property body with IOSimPOR, exploring at most @bound@ schedules
-- that reverse races found in previous schedules.
runInIOSimPOR ::
     Int
  -> (forall s. Session (IOSim s) -> IOSim s Property)
  -> Property
runInIOSimPOR bound body =
    exploreSimTrace
      (withScheduleBound bound)
      (exploreRaces >> simSession body)
      (\_prevTrace trace -> simTraceProperty trace)

-- | The simulation must finish with a passing property, and without
-- deadlocks, uncaught exceptions or threads that are still running.
simTraceProperty :: SimTrace Property -> Property
simTraceProperty trace = case traceResult True trace of
    Left failure -> counterexample ("Simulation failed: " ++ show failure) False
    Right p      -> p

{-------------------------------------------------------------------------------
  Utilities
-------------------------------------------------------------------------------}

type Key = ByteString
type Value = ResolveAsFirst ByteString
type Blob = ByteString

-- | Keys are zero-padded, so that the order of the keys as 'ByteString's (the
-- order in which range lookups and cursors return them) matches the order of
-- the numbers.
mkKey :: Int -> Key
mkKey = BSC.pack . printf "%06d"

mkValue :: Int -> Value
mkValue = ResolveAsFirst . BSC.pack . show

insertsN ::
     IOLike m
  => Table m Key Value Blob
  -> [Int]
  -> (Int -> Value)
  -> m ()
insertsN table is f =
    inserts table $ V.fromList [ (mkKey i, f i, Nothing) | i <- is ]

lookupValues ::
     IOLike m
  => Table m Key Value Blob
  -> [Int]
  -> m (V.Vector (Maybe Value))
lookupValues table is =
    fmap getValue <$> lookups table (V.fromList (map mkKey is))

-- | The tests insert no blobs, so an entry is a key and a value. Blob
-- references are dropped, so that the result can be compared.
entryPair :: Entry k v b -> (k, v)
entryPair e = (getEntryKey e, getEntryValue e)

rangeLookupPairs ::
     IOLike m
  => Table m Key Value Blob
  -> Range Key
  -> m [(Key, Value)]
rangeLookupPairs table range =
    map entryPair . V.toList <$> rangeLookup table range

-- | Read the remaining entries of a cursor one at a time with 'R.next', until
-- the cursor is exhausted.
readCursorAll :: IOLike m => Cursor m Key Value Blob -> m [(Key, Value)]
readCursorAll cursor = go []
  where
    go acc = R.next cursor >>= \case
      Nothing -> pure (reverse acc)
      Just e  -> go (entryPair e : acc)

-- | Read the remaining entries of a cursor two at a time with 'R.take', so
-- that reading takes several steps.
readCursorTwoAtATime ::
     IOLike m
  => Cursor m Key Value Blob
  -> m [(Key, Value)]
readCursorTwoAtATime cursor = do
    es <- R.take 2 cursor
    if V.null es
      then pure []
      else (map entryPair (V.toList es) ++) <$> readCursorTwoAtATime cursor

testLabel :: SnapshotLabel
testLabel = fromString "Key Value Blob"

-- | The expected contents of a table.
type Model = Map Key Value

modelOf :: [Int] -> (Int -> Value) -> Model
modelOf is f = Map.fromList [ (mkKey i, f i) | i <- is ]

-- | The expected result of 'lookupValues'.
modelLookups :: Model -> [Int] -> V.Vector (Maybe Value)
modelLookups model is = V.fromList [ Map.lookup (mkKey i) model | i <- is ]

-- | The expected result of 'rangeLookupPairs'.
modelRange :: Range Key -> Model -> [(Key, Value)]
modelRange range = filter (inRange . fst) . Map.toList
  where
    inRange k = case range of
      FromToExcluding lo hi -> lo <= k && k <  hi
      FromToIncluding lo hi -> lo <= k && k <= hi

{-------------------------------------------------------------------------------
  Tables
-------------------------------------------------------------------------------}

-- | Concurrent lookups on the same table are not a race: every thread sees the
-- values that were inserted before the threads started.
prop_concurrentLookups ::
     IOLike m
  => TableConfig
  -> Positive (Small Int)
  -> Session m
  -> m Property
prop_concurrentLookups conf (Positive (Small n)) sess = do
    table <- newTableWith conf sess
    insertsN table [0 .. n-1] mkValue
    results <- forConcurrently [0 .. n-1] $ \i -> do
      res <- lookupValues table [i]
      pure (res === V.singleton (Just (mkValue i)))
    pure (conjoin results)

-- | Concurrent reads of the same table are not a race, for all of the read
-- operations. From the "Concurrency" section of "Database.LSMTree.Simple":
--
-- > It is not a race to read and read the same table concurrently.
--
-- > The read operations are 'lookup', 'rangeLookup', 'duplicate', `union`,
-- > 'saveSnapshot', 'newCursor', and their variants.
--
-- Each thread runs a different read operation on the same table, and checks
-- the result against a model of the table. Operations that create a new table
-- or cursor from the table ('duplicate', 'union', 'newCursor') read the new
-- table or cursor in the same thread. Afterwards, the table must still have
-- the same contents.
--
-- 'saveSnapshot' is left out because running it concurrently with 'union' on
-- the same table can currently fail: see the TODO on
-- 'Database.LSMTree.Internal.MergingRun.snapshot'.
prop_concurrentReadOperations ::
     IOLike m
  => TableConfig
  -> Positive (Small Int)
  -> Session m
  -> m Property
prop_concurrentReadOperations conf (Positive (Small n)) sess = do
    table <- newTableWith conf sess
    other <- newTableWith conf sess
    insertsN table is mkValue
    insertsN other os mkValue
    results <- mapConcurrently id [
        check "lookups" (modelLookups model (is ++ os)) $
          lookupValues table (is ++ os)
      , check "rangeLookup" (modelRange range model) $
          rangeLookupPairs table range
      , check "duplicate" (modelLookups model is) $
          withDuplicate table $ \dup ->
            lookupValues dup is
      , check "cursor" (Map.toList model) $
          withCursor table readCursorTwoAtATime
      , check "cursor at offset"
          (Map.toList (Map.dropWhileAntitone (< offset) model)) $
          withCursorAtOffset table offset readCursorTwoAtATime
      , check "union with itself" (modelLookups model is) $
          withUnion table table $ \u ->
            lookupValues u is
      , check "union with another table"
          (modelLookups (Map.union model otherModel) (is ++ os)) $
          withUnion table other $ \u ->
            lookupValues u (is ++ os)
      ]
    afterwards <- lookupValues table (is ++ os)
    pure $
           conjoin results
      .&&. counterexample "table afterwards"
             (afterwards === modelLookups model (is ++ os))
  where
    is = [0 .. n-1]
    os = [n .. 2*n - 1]
    model = modelOf is mkValue
    otherModel = modelOf os mkValue
    range = FromToIncluding (mkKey 0) (mkKey (n `div` 2))
    offset = mkKey (n `div` 2)

    check name expected action =
      counterexample name . (expected ===) <$> action

-- | Writing to separate tables of the same session concurrently is not a race:
-- each table ends up containing exactly its own writes.
prop_concurrentWritesSeparateTables ::
     IOLike m
  => TableConfig
  -> Positive (Small Int)
  -> Session m
  -> m Property
prop_concurrentWritesSeparateTables conf (Positive (Small n)) sess = do
    table1 <- newTableWith conf sess
    table2 <- newTableWith conf sess
    _ <- concurrently
           (mapM_ (\i -> insertsN table1 [i] mkValue) is)
           (mapM_ (\i -> insertsN table2 [i] (mkValue . (+ n))) is)
    res1 <- lookupValues table1 is
    res2 <- lookupValues table2 is
    pure $
           res1 === V.fromList [ Just (mkValue i)       | i <- is ]
      .&&. res2 === V.fromList [ Just (mkValue (i + n)) | i <- is ]
  where
    is = [0 .. n-1]

-- | A duplicate is a stable view of a table: reading the duplicate while the
-- original table is concurrently written to observes exactly the contents at
-- the time of duplication, and the writes are visible in the original table
-- afterwards.
--
-- Note: it is a race to run 'duplicate' concurrently with writes to the
-- original table, so 'duplicate' happens before the concurrent part of the
-- test, not during it.
prop_readDuplicateWhileWriting ::
     IOLike m
  => TableConfig
  -> Positive (Small Int)
  -> Session m
  -> m Property
prop_readDuplicateWhileWriting conf (Positive (Small n)) sess = do
    table <- newTableWith conf sess
    insertsN table before mkValue
    dup <- duplicate table
    (_, dupRes) <- concurrently
      (mapM_ (\i -> insertsN table [i] mkValue) after)
      (lookupValues dup (before ++ after))
    origRes <- lookupValues table (before ++ after)
    pure $
           counterexample "duplicate"
             (dupRes === V.fromList (  map (Just . mkValue) before
                                    ++ map (const Nothing) after ))
      .&&. counterexample "original"
             (origRes === V.fromList (map (Just . mkValue) (before ++ after)))
  where
    before = [0 .. n-1]
    after  = [n .. 2*n - 1]

-- | Reading one table while another table of the same session is written to
-- is not a race. From the "Concurrency" section of "Database.LSMTree.Simple":
--
-- > It is not a race to read or write separate tables concurrently.
--
-- One thread does lookups and range lookups on table A, while another thread
-- inserts into and then deletes from table B. Every read of A sees the
-- contents of A, and afterwards B contains exactly its writes.
prop_readWhileWritingSeparateTable ::
     IOLike m
  => TableConfig
  -> Positive (Small Int)
  -> Session m
  -> m Property
prop_readWhileWritingSeparateTable conf (Positive (Small n)) sess = do
    tableA <- newTableWith conf sess
    tableB <- newTableWith conf sess
    insertsN tableA is mkValue
    (readsA, ()) <- concurrently
      (forM is $ \i -> do
         res <- lookupValues tableA [i]
         rng <- rangeLookupPairs tableA (range i)
         pure $ counterexample ("reads of table A, i = " ++ show i) $
                res === modelLookups modelA [i]
           .&&. rng === modelRange (range i) modelA)
      (do mapM_ (\i -> insertsN tableB [i] (mkValue . (+ n))) is
          mapM_ (\i -> deletes tableB (V.singleton (mkKey i))) deleted)
    resA <- lookupValues tableA is
    resB <- lookupValues tableB is
    pure $
           conjoin readsA
      .&&. counterexample "table A afterwards"
             (resA === modelLookups modelA is)
      .&&. counterexample "table B afterwards"
             (resB === modelLookups modelB is)
  where
    is = [0 .. n-1]
    deleted = filter (\i -> i `mod` 3 == 0) is
    modelA = modelOf is mkValue
    modelB = modelOf (filter (`notElem` deleted) is) (mkValue . (+ n))
    range i = FromToIncluding (mkKey 0) (mkKey i)

{-------------------------------------------------------------------------------
  Cursors
-------------------------------------------------------------------------------}

-- | A cursor is a stable view of a table: reading the whole cursor while the
-- table is concurrently written to returns exactly the entries of the table at
-- the time the cursor was created, in key order, and the writes are visible
-- in the table afterwards. From the "Concurrency" section of
-- "Database.LSMTree.Simple":
--
-- > A 'Cursor' creates a stable view of a table and can safely be read while
-- > modifying the original table.
--
-- The writes insert new keys, delete half of the keys that the cursor sees,
-- and overwrite the other half, one key at a time, so that they interleave
-- with the reads, flush the write buffer and run merges.
--
-- Note: the cursor is created before the concurrent part of the test, as
-- 'newCursor' is a read operation and it is a race to read and write the same
-- table concurrently.
prop_readCursorWhileWriting ::
     IOLike m
  => TableConfig
  -> Positive (Small Int)
  -> Session m
  -> m Property
prop_readCursorWhileWriting conf (Positive (Small n)) sess = do
    table <- newTableWith conf sess
    insertsN table before mkValue
    cursorRes <- withCursor table $ \cursor ->
      snd <$> concurrently (mapM_ (write table) writes)
                           (readCursorAll cursor)
    tableRes <- lookupValues table (before ++ after)
    pure $
           counterexample "cursor"
             (cursorRes === [ (mkKey i, mkValue i) | i <- before ])
      .&&. counterexample "table"
             (tableRes === V.fromList (  map expectedAfterWrites before
                                      ++ map (Just . mkValue) after ))
  where
    before = [0 .. n-1]
    after  = [n .. 2*n - 1]

    -- Interleave the deletes and overwrites of the old keys with the inserts
    -- of the new keys.
    writes = concat [ [Left i, Right j] | (i, j) <- zip before after ]

    write table (Left i)
      | even i    = deletes table (V.singleton (mkKey i))
      | otherwise = insertsN table [i] newValue
    write table (Right i) = insertsN table [i] mkValue

    expectedAfterWrites i
      | even i    = Nothing
      | otherwise = Just (newValue i)

    newValue i = ResolveAsFirst (BSC.pack ("new" ++ show i))

-- | Concurrent reads on the same cursor are not a race: they block each other,
-- so that every entry is read by exactly one thread, and each thread reads its
-- entries in key order. From the "Concurrency" section of
-- "Database.LSMTree.Simple":
--
-- > However, reading the 'next' key/value pair from a cursor locks the view,
-- > so concurrent reads on the same cursor block. This is because 'next'
-- > updates the cursor's current position.
--
-- The 'Int' argument is the number of threads that read the cursor.
--
-- Note: which thread reads which entry depends on the schedule, so the test
-- does not check that. It checks that together the threads read every entry
-- exactly once, and that each thread reads its entries in key order.
prop_concurrentCursorReads ::
     IOLike m
  => TableConfig
  -> Int
  -> Positive (Small Int)
  -> Session m
  -> m Property
prop_concurrentCursorReads conf numThreads (Positive (Small n)) sess = do
    table <- newTableWith conf sess
    insertsN table is mkValue
    results <- withCursor table $ \cursor ->
      replicateConcurrently numThreads (readCursorAll cursor)
    pure $
           counterexample "every entry is read exactly once"
             (sortOn fst (concat results)
                === [ (mkKey i, mkValue i) | i <- is ])
      .&&. counterexample "each thread reads in key order"
             (conjoin [ counterexample (show res) (isIncreasing (map fst res))
                      | res <- results ])
  where
    is = [0 .. n-1]

    isIncreasing ks = and (zipWith (<) ks (drop 1 ks))

{-------------------------------------------------------------------------------
  Sessions
-------------------------------------------------------------------------------}

-- | Session handles may be used concurrently from multiple threads. From the
-- "Concurrency" section of "Database.LSMTree.Simple":
--
-- > Session handles may be used concurrently from multiple Haskell threads,
-- > but concurrent use of read and write operations may introduce races.
-- > Specifically, it is a race to use `listSnapshots` and `deleteSnapshots`
-- > with the same session handle concurrently.
--
-- Every thread uses the same session to create its own table, write to it,
-- save it as a snapshot under its own name, and open a new table from that
-- snapshot. Each table opened from a snapshot contains exactly the writes of
-- its own thread. All threads write the same keys, with different values, so
-- that a mix-up between tables or snapshots shows up as a wrong value.
--
-- Note: 'listSnapshots' is only called after all threads have finished, and
-- the test never deletes snapshots, so it stays clear of the race that the
-- documentation describes.
prop_concurrentSessionUse ::
     IOLike m
  => TableConfig
  -> Positive (Small Int)
  -> Session m
  -> m Property
prop_concurrentSessionUse conf (Positive (Small n)) sess = do
    results <- forConcurrently threads $ \t -> do
      table <- newTableWith conf sess
      insertsN table is (threadValue t)
      saveSnapshot (snapName t) testLabel table
      table' <- openTableFromSnapshot sess (snapName t) testLabel
      res <- lookupValues table' is
      pure $ counterexample ("thread " ++ show t) $
        res === V.fromList [ Just (threadValue t i) | i <- is ]
    snaps <- listSnapshots sess
    pure $
           conjoin results
      .&&. counterexample "listSnapshots"
             (sort snaps === sort (map snapName threads))
  where
    -- Use at least 2 threads, so that the session is always shared. Saving a
    -- snapshot syncs its files to disk, which is slow in 'IO', so use at most
    -- 8 threads.
    threads = [0 .. max 2 (min 8 n) - 1]
    is = [0 .. n-1]
    threadValue t i = mkValue (t * n + i)
    snapName t = toSnapshotName ("snap" ++ show t)
