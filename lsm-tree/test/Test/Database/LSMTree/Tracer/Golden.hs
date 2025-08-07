{-# LANGUAGE OverloadedLists   #-}
{-# LANGUAGE OverloadedStrings #-}

module Test.Database.LSMTree.Tracer.Golden (tests) where

import           Control.Exception (mask_)
import           Control.Monad (void)
import           Control.Tracer
import qualified Data.List.NonEmpty as NE
import           Data.Monoid (Sum (..))
import           Data.Word
import           Database.LSMTree as R
import           Paths_lsm_tree
import           Prelude hiding (lookup)
import           System.FilePath
import qualified System.FS.API as FS
import qualified System.IO as IO
import           System.IO.Unsafe
import           Test.Tasty (TestTree, localOption, testGroup)
import           Test.Tasty.Golden
import           Test.Util.FS (withTempIOHasBlockIO)


tests :: TestTree
tests =
    localOption OnPass $
    testGroup "Test.Database.LSMTree.Tracer.Golden" [
        goldenVsFile
          "golden_traceMessages"
          goldenDataFile
          dataFile
          (golden_traceMessages dataFile)
      ]

goldenDataDirectory :: FilePath
goldenDataDirectory = unsafePerformIO getDataDir </> "tracer"

-- | Path to the /golden/ file for 'golden_traceMessages'
goldenDataFile :: FilePath
goldenDataFile = dataFile <.> "golden"

-- | Path to the output file for 'golden_traceMessages'
dataFile :: FilePath
dataFile = goldenDataDirectory </> "golden_traceMessages"

-- | A monadic action that traces messages emitted by the public API of
-- @lsm-tree@ to a file.
--
-- The idea is to cover as much of the public API as possible to trace most (if
-- not all) unique trace messages at least once. The trace output is useful in
-- two ways:
--
-- * By using golden tests based on the trace output we can help to ensure that
--   trace datatypes and their printing do not change unexpectedly.
--
-- * The trace output serves as an example that can be manually inspected to see
--   whether the trace messages are printed correctly and whether they are
--   useful.
golden_traceMessages :: FilePath -> IO ()
golden_traceMessages file =
    withTempIOHasBlockIO "golden_traceMessages" $ \hfs hbio ->
    withBinaryFileTracer file $ \(contramap show -> tr) -> do

      let sessionPath = FS.mkFsPath ["session-dir"]
      FS.createDirectory hfs sessionPath

      -- Open and close a session
      withOpenSession tr hfs hbio 4 sessionPath $ \s -> do
        -- Open and close a table
        withTable s $ \(t1 :: Table IO K V B) -> do
          -- Try out all the update variants
          update t1 (K 0)  (Insert (V 0) Nothing)
          insert t1 (K 1) (V 1) (Just (B 1))
          delete t1 (K 2)
          upsert t1 (K 3) (V 3)

          -- Perform a lookup and its member variant
          FoundWithBlob (V 1) bref <- lookup t1 (K 1)
          void $ member t1 (K 2)

          void $ rangeLookup t1 (FromToExcluding (K 0) (K 3))

          -- Open and close a cursor
          withCursorAtOffset t1 (K 1) $ \c -> do
            -- Read from a cursor
            [EntryWithBlob (K 1) (V 1) _] <- R.takeWhile 2 (< K 3) c
            void $ R.next c
            -- Retrieve blobs (we didn't try this before this part)
            B 1 <- retrieveBlob s bref
            pure ()

          let snapA = "snapshot_a"
              snapB = "snapshot_b"

          -- Try out a few snapshot operations, and keep one saved snapshot for
          -- later
          saveSnapshot snapA snapLabel t1
          saveSnapshot snapB snapLabel t1
          void $ listSnapshots s
          deleteSnapshot s snapB
          void $ doesSnapshotExist s snapA

          -- Open a table from a snapshot and close it again
          withTableFromSnapshot s snapA snapLabel $ \ t2 -> do

            -- Create a table duplicate and close it again
            withDuplicate t1 $ \t3 -> do

              let unionInputs = NE.fromList [t1, t2, t3]

              -- One-shot union
              withUnions unionInputs $ \_ -> pure ()

              -- Incremental union
              withIncrementalUnions unionInputs $ \t4 -> do
                UnionDebt d <- remainingUnionDebt t4
                void $ supplyUnionCredits t4 (UnionCredits (d - 1))
                UnionDebt d' <- remainingUnionDebt t4
                void $ supplyUnionCredits t4 (UnionCredits (d' + 1))
                void $ remainingUnionDebt t4

        -- Open a table and cursor, but close them automatically as part of
        -- closing the session instead of closing them manually.
        (t :: Table IO K V B) <- mask_ $ newTable s
        _c <- mask_ $ newCursor t

        pure ()

-- | A tracer that emits trace messages to a file handle
fileHandleTracer :: IO.Handle -> Tracer IO String
fileHandleTracer h = Tracer $ emit (IO.hPutStrLn h)


-- | A tracer that emits trace messages to a binary file
withBinaryFileTracer :: FilePath -> (Tracer IO String -> IO a) -> IO a
withBinaryFileTracer fp k =
    IO.withBinaryFile fp IO.WriteMode $ \h ->
      k (fileHandleTracer h)

{-------------------------------------------------------------------------------
  Key and value types
-------------------------------------------------------------------------------}

newtype K = K Word64
  deriving stock (Show, Eq, Ord)
  deriving newtype (SerialiseKey)

newtype V = V Word64
  deriving stock (Show, Eq, Ord)
  deriving newtype (SerialiseValue)

deriving via Sum Word64 instance ResolveValue V

newtype B  = B Word64
  deriving stock (Show, Eq, Ord)
  deriving newtype (SerialiseValue)

snapLabel :: SnapshotLabel
snapLabel = SnapshotLabel "KVBs"
