module Test.Database.LSMTree.Internal.RunReader (
    -- * Main test tree
    tests,
    -- * Utilities
    readKOps,
) where

import           Data.Bifoldable (bifoldMap)
import           Data.Coerce (coerce)
import qualified Data.Map as Map
import           Database.LSMTree.Extras (showPowersOf10)
import           Database.LSMTree.Extras.Generators (KeyForIndexCompact (..),
                     TypedWriteBuffer (..))
import           Database.LSMTree.Internal.BlobRef (readBlob)
import           Database.LSMTree.Internal.Entry (Entry)
import           Database.LSMTree.Internal.PageAcc (entryWouldFitInPage)
import qualified Database.LSMTree.Internal.Paths as Paths
import           Database.LSMTree.Internal.Run (Run)
import qualified Database.LSMTree.Internal.Run as Run
import           Database.LSMTree.Internal.RunNumber
import qualified Database.LSMTree.Internal.RunReader as Reader
import           Database.LSMTree.Internal.Serialise
import qualified System.FS.API as FS
import qualified System.FS.BlockIO.API as FS
import           Test.Database.LSMTree.Internal.Run (mkRunFromSerialisedKOps)
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.QuickCheck
import           Test.Util.FS (noOpenHandles, withSimHasBlockIO,
                     withTempIOHasBlockIO)
import           Test.Util.Orphans ()

tests :: TestTree
tests = testGroup "Database.LSMTree.Internal.RunReader"
    [ testGroup "MockFS"
        [ testProperty "prop_read" $ \wb ->
            ioProperty $ withSimHasBlockIO noOpenHandles $ \hfs hbio -> do
              prop_readAtOffset hfs hbio wb Nothing
        , testProperty "prop_readAtOffset" $ \wb offset ->
            ioProperty $ withSimHasBlockIO noOpenHandles $ \hfs hbio -> do
              prop_readAtOffset hfs hbio wb (Just offset)
        , testProperty "prop_readAtOffsetExisting" $ \wb i ->
            ioProperty $ withSimHasBlockIO noOpenHandles $ \hfs hbio -> do
              prop_readAtOffsetExisting hfs hbio wb i
        , testProperty "prop_readAtOffsetIdempotence" $ \wb i ->
            ioProperty $ withSimHasBlockIO noOpenHandles $ \hfs hbio -> do
              prop_readAtOffsetIdempotence hfs hbio wb i
        , testProperty "prop_readAtOffsetReadHead" $ \wb ->
            ioProperty $ withSimHasBlockIO noOpenHandles $ \hfs hbio -> do
              prop_readAtOffsetReadHead hfs hbio wb
        ]
    , testGroup "RealFS"
        [ testProperty "prop_read" $ \wb ->
            ioProperty $ withTempIOHasBlockIO "tmp_RunReader" $ \hfs hbio -> do
              prop_readAtOffset hfs hbio wb Nothing
        , testProperty "prop_readAtOffset" $ \wb offset ->
            ioProperty $ withTempIOHasBlockIO "tmp_RunReader" $ \hfs hbio -> do
              prop_readAtOffset hfs hbio wb (Just offset)
        , testProperty "prop_readAtOffsetExisting" $ \wb i ->
            ioProperty $ withTempIOHasBlockIO "tmp_RunReader" $ \hfs hbio -> do
              prop_readAtOffsetExisting hfs hbio wb i
        , testProperty "prop_readAtOffsetIdempotence" $ \wb i ->
            ioProperty $ withTempIOHasBlockIO "tmp_RunReader" $ \hfs hbio -> do
              prop_readAtOffsetIdempotence hfs hbio wb i
        , testProperty "prop_readAtOffsetReadHead" $ \wb ->
            ioProperty $ withTempIOHasBlockIO "tmp_RunReader" $ \hfs hbio -> do
              prop_readAtOffsetReadHead hfs hbio wb
        ]
    ]

-- | Creating a run from a write buffer and reading from the run yields the
-- original elements.
--
-- @id === read . new . flush@
--
-- If there is an offset:
--
-- @dropWhile ((< offset) . key) === read . newAtOffset offset . flush@
--
prop_readAtOffset ::
     FS.HasFS IO h
  -> FS.HasBlockIO IO h
  -> TypedWriteBuffer KeyForIndexCompact SerialisedValue SerialisedBlob
  -> Maybe KeyForIndexCompact
  -> IO Property
prop_readAtOffset fs hbio (TypedWriteBuffer wb) offsetKey = do
    run <- flush (RunNumber 42) wb
    rhs <- readKOps fs hbio (coerce offsetKey) run

    -- make sure run gets closed again
    Run.removeReference run

    return . genStats kops $
      counterexample ("entries expected: " <> show (length lhs)) $
      counterexample ("entries found: " <> show (length rhs)) $
        lhs === rhs
  where
    kops = Map.toList wb
    lhs = case offsetKey of
        Nothing -> kops
        Just k  -> dropWhile ((< coerce k) . fst) kops

    flush n = mkRunFromSerialisedKOps fs hbio
                (Paths.RunFsPaths (FS.mkFsPath []) n)

-- | A version of 'prop_readAtOffset' where the offset key is always one
-- of the keys that exist in the run.
prop_readAtOffsetExisting ::
     FS.HasFS IO h
  -> FS.HasBlockIO IO h
  -> TypedWriteBuffer KeyForIndexCompact SerialisedValue SerialisedBlob
  -> NonNegative Int
  -> IO Property
prop_readAtOffsetExisting fs hbio wb (NonNegative index)
  | null kops = pure discard
  | otherwise =
      prop_readAtOffset fs hbio wb (Just (keys !! (index `mod` length keys)))
  where
    keys :: [KeyForIndexCompact]
    keys = coerce (fst <$> kops)
    kops = Map.toList (unTypedWriteBuffer wb)

-- | Idempotence of 'readAtOffset'.
-- Reading at an offset should not perform any stateful effects which alter
-- the result of a subsequent read at the same offset.
--
-- @readAtOffset offset *> readAtOffset offset === readAtOffset offset@
--
prop_readAtOffsetIdempotence ::
     FS.HasFS IO h
  -> FS.HasBlockIO IO h
  -> TypedWriteBuffer KeyForIndexCompact SerialisedValue SerialisedBlob
  -> Maybe KeyForIndexCompact
  -> IO Property
prop_readAtOffsetIdempotence fs hbio (TypedWriteBuffer wb) offsetKey = do
    run <- flush (RunNumber 42) wb
    lhs <- readKOps fs hbio (coerce offsetKey) run
    rhs <- readKOps fs hbio (coerce offsetKey) run

    -- make sure run gets closed again
    Run.removeReference run

    return . genStats kops $
      counterexample ("entries expected: " <> show (length lhs)) $
      counterexample ("entries found: " <> show (length rhs)) $
        lhs === rhs
  where
    kops = Map.toList wb
    flush n = mkRunFromSerialisedKOps fs hbio
                (Paths.RunFsPaths (FS.mkFsPath []) n)

-- | Head of 'read' equals 'readAtOffset' of head of 'read'.
-- Reading the first key from the run initialized without an offset
-- should be the same as reading from a run initialized at the first key.
-- the result of a subsequent read at the same offset.
--
-- @read === readAtOffset (head read)@
--
prop_readAtOffsetReadHead ::
     FS.HasFS IO h
  -> FS.HasBlockIO IO h
  -> TypedWriteBuffer KeyForIndexCompact SerialisedValue SerialisedBlob
  -> IO Property
prop_readAtOffsetReadHead fs hbio (TypedWriteBuffer wb) = do
    run <- flush (RunNumber 42) wb
    lhs <- readKOps fs hbio Nothing run
    rhs <- case lhs of
      []        -> return []
      (key,_):_ -> readKOps fs hbio (Just key) run

    -- make sure run gets closed again
    Run.removeReference run

    return . genStats kops $
      counterexample ("entries expected: " <> show (length lhs)) $
      counterexample ("entries found: " <> show (length rhs)) $
        lhs === rhs
  where
    kops = Map.toList wb
    flush n = mkRunFromSerialisedKOps fs hbio
                (Paths.RunFsPaths (FS.mkFsPath []) n)

{-------------------------------------------------------------------------------
  Utilities
-------------------------------------------------------------------------------}

type SerialisedEntry = Entry SerialisedValue SerialisedBlob
type SerialisedKOp = (SerialisedKey, SerialisedEntry)

readKOps ::
     FS.HasFS IO h -> FS.HasBlockIO IO h
  -> Maybe (SerialisedKey)  -- ^ offset
  -> Run IO (FS.Handle h)
  -> IO [SerialisedKOp]
readKOps fs hbio offset run = do
    reader <- Reader.new fs hbio offset run
    go reader
  where
    go reader = do
      Reader.next fs hbio reader >>= \case
        Reader.Empty -> return []
        Reader.ReadEntry key e -> do
          e' <- traverse (readBlob fs) $ Reader.toFullEntry e
          ((key, e') :) <$> go reader

genStats :: (Testable a, Foldable t) => t SerialisedKOp -> a -> Property
genStats kops = tabulate "value size" size . label note
  where
    size = map (showPowersOf10 . sizeofValue) vals
    vals = concatMap (bifoldMap pure mempty . snd) kops
    note
      | any (uncurry entryWouldFitInPage) kops = "has large k/op"
      | otherwise = "no large k/op"
