-- | Tests for the concurrency guarantees documented in the "Concurrency"
-- section of "Database.LSMTree.Simple": it is not a race to read and read the
-- same table concurrently, it is not a race to read or write separate tables
-- concurrently, and a duplicate is a stable view of a table that can be read
-- while the original table is written to.
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

import           Control.Monad.Class.MonadAsync (concurrently, forConcurrently)
import           Control.Monad.Class.MonadTest (exploreRaces)
import           Control.Monad.IOSim (IOSim, SimTrace, exploreSimTrace,
                     runSimTrace, traceResult, withScheduleBound)
import           Control.Tracer (nullTracer)
import           Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as BSC
import qualified Data.Vector as V
import qualified System.FS.API as FS
import qualified System.FS.Sim.MockFS as MockFS

import           Database.LSMTree as R
import           Test.QuickCheck
import           Test.Tasty (TestTree, localOption, testGroup)
import           Test.Tasty.QuickCheck (QuickCheckMaxSize (..), testProperty)
import           Test.Util.FS (propNoOpenHandles, withSimHasBlockIO,
                     withTempIOHasBlockIO)

tests :: TestTree
tests =
    testGroup "Test.Database.LSMTree.Concurrency" [
        testGroup "IO" [
            testProperty "prop_concurrentLookups" $ \n ->
              runInIO "prop_concurrentLookups" $
                prop_concurrentLookups testTableConfig n
          , testProperty "prop_concurrentWritesSeparateTables" $ \n ->
              runInIO "prop_concurrentWritesSeparateTables" $
                prop_concurrentWritesSeparateTables testTableConfig n
          , testProperty "prop_readDuplicateWhileWriting" $ \n ->
              runInIO "prop_readDuplicateWhileWriting" $
                prop_readDuplicateWhileWriting testTableConfig n
          ]
        -- IOSim is much slower than IO, so keep the inputs smaller.
      , localOption (QuickCheckMaxSize 30) $ testGroup "IOSim" [
            testProperty "prop_concurrentLookups" $ \n ->
              runInIOSim $ prop_concurrentLookups testTableConfig n
          , testProperty "prop_concurrentWritesSeparateTables" $ \n ->
              runInIOSim $ prop_concurrentWritesSeparateTables testTableConfig n
          , testProperty "prop_readDuplicateWhileWriting" $ \n ->
              runInIOSim $ prop_readDuplicateWhileWriting testTableConfig n
          ]
        -- IOSimPOR runs the simulation once for every schedule it explores, so
        -- only run one small input of the property that shares the most state
        -- between tables: the runs and merges of a table and its duplicate.
      , testGroup "IOSimPOR" [
            testProperty "prop_readDuplicateWhileWriting" $
              once $
              runInIOSimPOR 20 $
                prop_readDuplicateWhileWriting
                  testTableConfig
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

mkKey :: Int -> Key
mkKey = BSC.pack . show

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

{-------------------------------------------------------------------------------
  Properties
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
