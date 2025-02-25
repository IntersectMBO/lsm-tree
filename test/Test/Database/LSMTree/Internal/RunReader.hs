module Test.Database.LSMTree.Internal.RunReader (
    -- * Main test tree
    tests,
    -- * Utilities
    readKOps,
) where

import           Control.RefCount
import           Data.Coerce (coerce)
import qualified Data.Map as Map
import           Database.LSMTree.Extras.Generators
                     (BiasedKeyForIndexCompact (..))
import           Database.LSMTree.Extras.RunData
import           Database.LSMTree.Internal.BlobRef
import           Database.LSMTree.Internal.Entry (Entry)
import qualified Database.LSMTree.Internal.Index as Index (IndexType (Compact))
import           Database.LSMTree.Internal.Run (Run)
import qualified Database.LSMTree.Internal.RunReader as Reader
import           Database.LSMTree.Internal.Serialise
import qualified System.FS.API as FS
import qualified System.FS.BlockIO.API as FS
import qualified System.FS.Sim.MockFS as MockFS
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.QuickCheck
import           Test.Util.FS (propNoOpenHandles, withSimHasBlockIO,
                     withTempIOHasBlockIO)
import           Test.Util.Orphans ()

tests :: TestTree
tests = testGroup "Database.LSMTree.Internal.RunReader"
    [ testGroup "MockFS"
        [ testProperty "prop_read" $ \wb ->
            ioProperty $ withSimHasBlockIO propNoOpenHandles MockFS.empty $ \hfs hbio _ -> do
              prop_readAtOffset hfs hbio wb Nothing
        , testProperty "prop_readAtOffset" $ \wb offset ->
            ioProperty $ withSimHasBlockIO propNoOpenHandles MockFS.empty $ \hfs hbio _ -> do
              prop_readAtOffset hfs hbio wb (Just offset)
        , testProperty "prop_readAtOffsetExisting" $ \wb i ->
            ioProperty $ withSimHasBlockIO propNoOpenHandles MockFS.empty $ \hfs hbio _ -> do
              prop_readAtOffsetExisting hfs hbio wb i
        , testProperty "prop_readAtOffsetIdempotence" $ \wb i ->
            ioProperty $ withSimHasBlockIO propNoOpenHandles MockFS.empty $ \hfs hbio _ -> do
              prop_readAtOffsetIdempotence hfs hbio wb i
        , testProperty "prop_readAtOffsetReadHead" $ \wb ->
            ioProperty $ withSimHasBlockIO propNoOpenHandles MockFS.empty $ \hfs hbio _ -> do
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
  -> RunData BiasedKeyForIndexCompact SerialisedValue SerialisedBlob
  -> Maybe BiasedKeyForIndexCompact
  -> IO Property
prop_readAtOffset fs hbio rd offsetKey =
    withRunAt fs hbio Index.Compact (simplePath 42) rd' $ \run -> do
      rhs <- readKOps (coerce offsetKey) run

      return . labelRunData rd' $
        counterexample ("entries expected: " <> show (length lhs)) $
        counterexample ("entries found: " <> show (length rhs)) $
          lhs === rhs
  where
    rd' = serialiseRunData rd
    kops = Map.toList (unRunData rd')
    lhs = case offsetKey of
        Nothing -> kops
        Just k  -> dropWhile ((< coerce k) . fst) kops

-- | A version of 'prop_readAtOffset' where the offset key is always one
-- of the keys that exist in the run.
prop_readAtOffsetExisting ::
     FS.HasFS IO h
  -> FS.HasBlockIO IO h
  -> RunData BiasedKeyForIndexCompact SerialisedValue SerialisedBlob
  -> NonNegative Int
  -> IO Property
prop_readAtOffsetExisting fs hbio rd (NonNegative index)
  | null kops = pure discard
  | otherwise =
      prop_readAtOffset fs hbio rd (Just (keys !! (index `mod` length keys)))
  where
    keys :: [BiasedKeyForIndexCompact]
    keys = coerce (fst <$> kops)
    kops = Map.toList (unRunData rd)

-- | Idempotence of 'readAtOffset'.
-- Reading at an offset should not perform any stateful effects which alter
-- the result of a subsequent read at the same offset.
--
-- @readAtOffset offset *> readAtOffset offset === readAtOffset offset@
--
prop_readAtOffsetIdempotence ::
     FS.HasFS IO h
  -> FS.HasBlockIO IO h
  -> RunData BiasedKeyForIndexCompact SerialisedValue SerialisedBlob
  -> Maybe BiasedKeyForIndexCompact
  -> IO Property
prop_readAtOffsetIdempotence fs hbio rd offsetKey =
    withRunAt fs hbio Index.Compact (simplePath 42) rd' $ \run -> do
    lhs <- readKOps (coerce offsetKey) run
    rhs <- readKOps (coerce offsetKey) run

    return . labelRunData rd' $
      counterexample ("entries expected: " <> show (length lhs)) $
      counterexample ("entries found: " <> show (length rhs)) $
        lhs === rhs
  where
    rd' = serialiseRunData rd

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
  -> RunData BiasedKeyForIndexCompact SerialisedValue SerialisedBlob
  -> IO Property
prop_readAtOffsetReadHead fs hbio rd =
    withRunAt fs hbio Index.Compact (simplePath 42) rd' $ \run -> do
      lhs <- readKOps Nothing run
      rhs <- case lhs of
        []        -> return []
        (key,_):_ -> readKOps (Just key) run

      return . labelRunData rd' $
        counterexample ("entries expected: " <> show (length lhs)) $
        counterexample ("entries found: " <> show (length rhs)) $
          lhs === rhs
  where
    rd' = serialiseRunData rd

{-------------------------------------------------------------------------------
  Utilities
-------------------------------------------------------------------------------}

type SerialisedEntry = Entry SerialisedValue SerialisedBlob
type SerialisedKOp = (SerialisedKey, SerialisedEntry)

readKOps ::
     Maybe (SerialisedKey)  -- ^ offset
  -> Ref (Run IO h)
  -> IO [SerialisedKOp]
readKOps offset run = do
    reader <- Reader.new offsetKey run
    go reader
  where
    offsetKey = maybe Reader.NoOffsetKey (Reader.OffsetKey . coerce) offset

    go reader = do
      Reader.next reader >>= \case
        Reader.Empty -> return []
        Reader.ReadEntry key e -> do
          let fs = Reader.readerHasFS reader
          e' <- traverse (readRawBlobRef fs) $ Reader.toFullEntry e
          ((key, e') :) <$> go reader

