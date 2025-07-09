{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module Test.Database.LSMTree.Internal (tests) where

import           Control.RefCount
import           Control.Tracer
import           Data.Coerce (coerce)
import qualified Data.Map.Strict as Map
import           Data.Maybe (isJust, mapMaybe)
import qualified Data.Vector as V
import           Database.LSMTree.Extras.Generators ()
import           Database.LSMTree.Internal.BlobRef
import qualified Database.LSMTree.Internal.BloomFilter as Bloom
import           Database.LSMTree.Internal.Config
import           Database.LSMTree.Internal.Entry
import           Database.LSMTree.Internal.Serialise
import           Database.LSMTree.Internal.Unsafe
import qualified System.FS.API as FS
import           Test.QuickCheck
import           Test.Tasty
import           Test.Tasty.QuickCheck
import           Test.Util.FS

tests :: TestTree
tests = testGroup "Test.Database.LSMTree.Internal" [
      testGroup "Cursor" [
          testProperty "prop_roundtripCursor" $ withMaxSuccess 500 $
            prop_roundtripCursor
        ]
    ]


testSalt :: Bloom.Salt
testSalt = 4

testTableConfig :: TableConfig
testTableConfig = defaultTableConfig {
      -- Write buffer size is small on purpose, so that the test actually
      -- flushes and merges.
      confWriteBufferAlloc = AllocNumEntries 3
    }

-- | Check that reading from a cursor returns exactly the entries that have
-- been inserted into the table. Roughly:
--
-- @
--  readCursor . newCursor . inserts == id
-- @
--
-- If lower and upper bound are provided:
--
-- @
--  readCursorWhile (<= ub) . newCursorAt lb . inserts
--    == takeWhile ((<= ub) . key) . dropWhile ((< lb) . key)
-- @
prop_roundtripCursor ::
     Maybe SerialisedKey  -- ^ Inclusive lower bound
  -> Maybe SerialisedKey  -- ^ Inclusive upper bound
  -> V.Vector (SerialisedKey, Entry SerialisedValue SerialisedBlob)
  -> Property
prop_roundtripCursor lb ub kops = ioProperty $ withRefCtx $ \refCtx ->
    withTempIOHasBlockIO "prop_roundtripCursor" $ \hfs hbio -> do
      withOpenSession nullTracer hfs hbio testSalt (FS.mkFsPath []) $ \sesh -> do
        withTable sesh conf $ \t -> do
          updates resolve (coerce kops) t
          fromCursor <- withCursor resolve (toOffsetKey lb) t $ \c ->
            fetchBlobs hfs refCtx =<< readCursorUntil ub c
          pure $
            tabulate "duplicates" (show <$> Map.elems duplicates) $
            tabulate "any blobs" [show (any (isJust . snd . snd) fromCursor)] $
            expected === fromCursor
  where
    conf = testTableConfig

    fetchBlobs :: FS.HasFS IO h -> RefCtx
             ->     V.Vector (k, (v, Maybe (WeakBlobRef IO h)))
             -> IO (V.Vector (k, (v, Maybe SerialisedBlob)))
    fetchBlobs hfs refCtx = traverse (traverse (traverse (traverse (readWeakBlobRef hfs refCtx))))

    toOffsetKey = maybe NoOffsetKey (OffsetKey . coerce)

    expected =
      V.fromList . mapMaybe (traverse entryToValue) $
        maybe id (\k -> takeWhile ((<= k) . fst)) ub $
          maybe id (\k -> dropWhile ((< k) . fst)) lb $
            Map.assocs . Map.fromListWith (combine resolve) $
              V.toList kops

    entryToValue :: Entry v b -> Maybe (v, Maybe b)
    entryToValue = \case
      Insert v           -> Just (v, Nothing)
      InsertWithBlob v b -> Just (v, Just b)
      Upsert v          -> Just (v, Nothing)
      Delete             -> Nothing

    duplicates :: Map.Map SerialisedKey Int
    duplicates =
      Map.filter (> 1) $
        Map.fromListWith (+) . map (\(k, _) -> (k, 1)) $
          V.toList kops

readCursorUntil ::
     Maybe SerialisedKey  -- Inclusive upper bound
  -> Cursor IO h
  -> IO (V.Vector (SerialisedKey,
                   (SerialisedValue,
                    Maybe (WeakBlobRef IO h))))
readCursorUntil ub cursor = go V.empty
  where
    chunkSize = 50
    toResult k v b = (coerce k, (v, b))

    go !acc = do
      res <- case ub of
        Nothing -> readCursor resolve chunkSize cursor toResult
        Just k  -> readCursorWhile resolve (<= coerce k) chunkSize cursor toResult
      if V.length res < chunkSize then pure (acc <> res)
                                  else go (acc <> res)

resolve :: ResolveSerialisedValue
resolve (SerialisedValue x) (SerialisedValue y) = SerialisedValue (x <> y)
