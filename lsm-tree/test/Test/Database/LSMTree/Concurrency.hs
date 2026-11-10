module Test.Database.LSMTree.Concurrency (tests) where

import           Control.Monad.Class.MonadAsync (concurrently, forConcurrently)
import           Control.Tracer (nullTracer)
import           Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as BSC
import qualified Data.Vector as V
import qualified System.FS.API as FS

import           Database.LSMTree as R
import           Test.QuickCheck
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.QuickCheck (testProperty)
import           Test.Util.FS (withTempIOHasBlockIO)

-- | Tests for the concurrency guarantees documented in the "Concurrency"
-- section of "Database.LSMTree.Simple": it is not a race to read and read the
-- same table concurrently, it is not a race to read or write separate tables
-- concurrently, and a duplicate is a stable view of a table that can be read
-- while the original table is written to.

tests :: TestTree
tests =
    testGroup "Test.Database.LSMTree.Concurrency"
      [ testProperty "prop_concurrentLookups" prop_concurrentLookups
      , testProperty "prop_concurrentWritesSeparateTables"
          prop_concurrentWritesSeparateTables
      , testProperty "prop_readDuplicateWhileWriting"
          prop_readDuplicateWhileWriting
       ]

testSalt :: R.Salt
testSalt = 4

type Key =   ByteString
type Value = ResolveAsFirst ByteString
type Blob = ByteString

mkKey :: Int -> Key
mkKey = BSC.pack . show

mkValue :: Int -> Value
mkValue = ResolveAsFirst . BSC.pack . show

insertsN :: Table IO  Key Value Blob -> [Int] -> (Int -> Value) -> IO ()
insertsN table is f =
    inserts table $ V.fromList [ (mkKey i, f i, Nothing) | i <- is ]

lookupValues :: Table IO Key Value Blob -> [Int] -> IO (V.Vector (Maybe Value))
lookupValues table is =
     fmap getValue <$> lookups table (V.fromList (map mkKey is))

-- | Concurrent lookups on the same table are not a race: every thread sees the
-- values that were inserted before the threads started.
prop_concurrentLookups ::Positive (Small Int) -> Property
prop_concurrentLookups (Positive (Small n)) =
    ioProperty $
    withTempIOHasBlockIO "prop_concurrentLookups" $ \hfs hbio ->
    withOpenSession nullTracer hfs hbio testSalt (FS.mkFsPath []) $ \sess -> do
      table <- newTable sess
      insertsN table [0 .. n-1] mkValue
      results <- forConcurrently [0 .. n-1] $ \i -> do
        res <- lookupValues table [i]
        pure (res === V.singleton (Just (mkValue i)))
      pure (conjoin results)

-- | Writing to separate tables of the same session concurrently is not a race:
-- each table ends up containing exactly its own writes.
prop_concurrentWritesSeparateTables :: Positive (Small Int) -> Property
prop_concurrentWritesSeparateTables (Positive (Small n)) =
    ioProperty $
    withTempIOHasBlockIO "prop_concurrentWritesSeparateTables" $ \hfs hbio ->
    withOpenSession nullTracer hfs hbio testSalt (FS.mkFsPath []) $ \sess -> do
      table1 <- newTable sess
      table2 <- newTable sess
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
-- Note: 'duplicate' is itself a write operation, so it happens before the
-- concurrent part of the test, not during it.
prop_readDuplicateWhileWriting :: Positive (Small Int) -> Property
prop_readDuplicateWhileWriting (Positive (Small n)) =
    ioProperty $
    withTempIOHasBlockIO "prop_readDuplicateWhileWriting" $ \hfs hbio ->
    withOpenSession nullTracer hfs hbio testSalt (FS.mkFsPath []) $ \sess -> do
       table <- newTable sess
       insertsN table before mkValue
       dup <- duplicate table
       (_, dupRes) <- concurrently
          (mapM_ (\i -> insertsN table [i] mkValue) after)
          (lookupValues dup (before ++ after))
       origRes <- lookupValues table (before ++ after)
       pure $
              counterexample "duplicate" (
                  dupRes === V.fromList ( map (Just . mkValue) before
                                       ++ map (const Nothing) after ) )
            .&&. counterexample "original" (
                    origRes === V.fromList (map (Just . mkValue) (before ++ after)) )
    where
      before = [0 .. n-1]
      after  = [n .. 2*n - 1]
