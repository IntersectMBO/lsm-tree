{-# LANGUAGE CPP               #-}
{-# LANGUAGE OverloadedLists   #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -Wno-orphans #-}
{-# OPTIONS_GHC -Wno-unused-do-bind #-}

{- HLINT ignore "Redundant pure" -}

module Database.LSMTree.Demo (demo) where

import           Control.Exception (SomeException, try)
import           Control.Monad (when)
import           Control.Monad.Class.MonadST (MonadST (..))
import qualified Control.Monad.IOSim as IOSim
import           Control.Monad.Primitive (RealWorld)
import           Control.Monad.ST.Unsafe (unsafeIOToST)
import           Control.Tracer (nullTracer)
import           Data.Functor (void)
import           Data.Primitive.PrimVar (PrimVar, newPrimVar, readPrimVar,
                     writePrimVar)
import           Data.Typeable (Typeable)
import qualified Data.Vector as V
import           Data.Word (Word64)
import           Database.LSMTree as LSMT
import qualified System.Directory as IO (createDirectoryIfMissing,
                     doesDirectoryExist, removeDirectoryRecursive)
import qualified System.FS.API as FS
import qualified System.FS.BlockIO.API as FS
import qualified System.FS.BlockIO.IO as FS
import qualified System.FS.BlockIO.Sim as FSSim
import qualified System.FS.Sim.MockFS as FSSim
import           System.IO.Unsafe (unsafePerformIO)

-- | Interactive demo showing functional requiremens for the @lsm-tree@ library
-- are met.
--
-- The functional requirements are discussed in this document: "Storing the
-- Cardano ledger state on disk: final report for high-performance backend"
--
-- Sections of the demo code are headed by the number of the corresponding
-- functional requirement.
demo :: IO ()
demo = do
  freshDirectory "_demo"
  withOpenSessionIO tracer "_demo" $ \session -> do
    withTableWith config session  $ \(table :: Table IO K V B) -> do
      pause -- [0]

      -- 2. basic key-value store operations

      inserts table $ V.fromList [ (K i, V i, Just (B i)) | i <- [1 .. 10_000] ]
      as <- lookups table $ V.fromList [ K 1, K 2, K 3, K 4 ]
      print (fmap getValue as)
      pause -- [1]

      deletes table $ V.fromList [ K i | i <- [1 .. 10_000], even i ]
      bs <- lookups table $ V.fromList [ K 1, K 2, K 3, K 4 ]
      print (fmap getValue bs)
      pause -- [2]

      -- 2. Intermezzo: blob retrieval

      cs <- try @SomeException $ retrieveBlobs session $ V.mapMaybe getBlob as
      print cs
      pause -- [3]

      ds <- try @SomeException $ retrieveBlobs session $ V.mapMaybe getBlob bs
      print ds
      pause -- [4]

      -- 3. range lookups and cursors

      es <- rangeLookup table $ FromToIncluding (K 1) (K 4)
      print (fmap getEntryValue es)
      pause -- [5]

      withCursorAtOffset table (K 1) $ \cursor -> do
        fs <- LSMT.take 2 cursor
        print (fmap getEntryValue fs)
        pause -- [6]

      -- 4. upserts (or monoidal updates)

      -- better than lookup followed by insert
      upserts table $ V.fromList [ (K i, V 1) | i <- [1 .. 10_000] ]
      gs <- lookups table $ V.fromList [ K 1, K 2, K 3, K 4 ]
      print (fmap getValue gs)
      pause -- [7]

      -- 5. multiple independently writable references

      withDuplicate table $ \dupliTable -> do
        inserts dupliTable $ V.fromList [ (K i, V 1, Nothing) | i <- [1 .. 10_000] ]
        hs <- lookups dupliTable $ V.fromList [ K 1, K 2, K 3, K 4 ]
        print (fmap getValue hs)
        pause -- [8]

        is <- lookups table $ V.fromList [ K 1, K 2, K 3, K 4]
        print (fmap getValue is)
        pause -- [9]

        -- 6. snapshots

        saveSnapshot "odds_evens" label table
        saveSnapshot "all_ones" label dupliTable
        js <- listSnapshots session
        print js
        pause -- [10]

    -- 6. snapshots continued

    withTableFromSnapshot session "odds_evens" label $ \(table :: Table IO K V B) -> do
      withTableFromSnapshot session "all_ones" label $ \(dupliTable :: Table IO K V B) -> do
        pause -- [11]

        -- 7. table unions

        withUnion table dupliTable $ \uniTable -> do
          ks <- lookups uniTable $ V.fromList [ K 1, K 2, K 3, K 4]
          print (fmap getValue ks)
          pause -- [12]

        withIncrementalUnion table dupliTable $ \uniTable -> do
          ls <- lookups uniTable $ V.fromList [ K 1, K 2, K 3, K 4]
          print (fmap getValue ls)
          pause -- [13]

          m@(UnionDebt m') <- remainingUnionDebt uniTable
          supplyUnionCredits uniTable (UnionCredits (m' `div` 2))
          print m
          pause -- [14]

          ns <- lookups uniTable $ V.fromList [ K 1, K 2, K 3, K 4]
          print (fmap getValue ns)
          pause -- [15]

  -- 8. simulation

  let
    simpleAction ::
         (LSMT.IOLike m, Typeable h)
      => FS.HasFS m h -> FS.HasBlockIO m h -> m ()
    simpleAction hasFS hasBlockIO = do
      let sessionDir = FS.mkFsPath ["_demo"]
      FS.createDirectoryIfMissing hasFS False sessionDir
      withOpenSession tracer hasFS hasBlockIO 17 sessionDir $ \session -> do
        withTableWith config session  $ \(table :: Table m K V B) -> do
          inserts table $ V.fromList [ (K i, V i, Just (B i)) | i <- [1 .. 10_000] ]
          os <- lookups table $ V.fromList [ K 1, K 2, K 3, K 4 ]
          print' (fmap getValue os)

  do
    FS.withIOHasBlockIO (FS.MountPoint "") FS.defaultIOCtxParams $ \hasFS hasBlockIO -> do
      simpleAction hasFS hasBlockIO
      pause -- [16]

  do
    pure $! IOSim.runSimOrThrow $ do
      (hasFS, hasBlockIO) <- FSSim.simHasBlockIO' FSSim.empty
      simpleAction hasFS hasBlockIO
    pause -- [17]

{-------------------------------------------------------------------------------
  Types
-------------------------------------------------------------------------------}

newtype K = K Word64
  deriving stock (Show, Eq)
  deriving newtype SerialiseKey

newtype V = V Word64
  deriving stock (Show, Eq)
  deriving newtype (Num, SerialiseValue)
instance ResolveValue V where
  resolve = (+)

newtype B = B Word64
  deriving stock (Show, Eq)
  deriving newtype (Num, SerialiseValue)

config :: TableConfig
config = defaultTableConfig {
      confWriteBufferAlloc = AllocNumEntries 172
    }

tracer :: Monad m => Tracer m LSMTreeTrace
tracer = nullTracer

label :: SnapshotLabel
label = "KVB"

{-------------------------------------------------------------------------------
  Utils
-------------------------------------------------------------------------------}

{-# NOINLINE pauseRef #-}
pauseRef :: PrimVar RealWorld Int
pauseRef = unsafePerformIO $ newPrimVar 0

incrPauseRef :: IO Int
incrPauseRef = do
    x <- readPrimVar pauseRef
    writePrimVar pauseRef $! x + 1
    pure x

pause :: IO ()
pause = do
  x <- incrPauseRef
  putStr ("[" <> show x <> "] " <> "press ENTER to continue...")
  void $ getLine

freshDirectory :: FilePath -> IO ()
freshDirectory path = do
    b <- IO.doesDirectoryExist path
    when b $ IO.removeDirectoryRecursive path
    IO.createDirectoryIfMissing False path

print' :: (Show a, MonadST m) => a -> m ()
print' x = stToIO $ unsafeIOToST $ print x
