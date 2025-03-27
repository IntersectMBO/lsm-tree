{-# LANGUAGE LambdaCase      #-}
{-# LANGUAGE RecordWildCards #-}
{-# OPTIONS_GHC -Wno-orphans #-}
{- HLINT ignore "Eta reduce" -}

module Test.Database.LSMTree.Internal.Index.Compact (tests) where

import           Control.DeepSeq (deepseq)
import           Control.Monad (foldM)
import           Control.Monad.ST (runST)
import           Control.Monad.State.Strict (MonadState (..), State, evalState,
                     get, put)
import           Data.Bit (Bit (..))
import qualified Data.Bit as BV
import qualified Data.ByteString.Lazy as LBS
import qualified Data.ByteString.Short as SBS
import           Data.Coerce (coerce)
import           Data.Foldable (Foldable (..))
import           Data.List.Split (chunksOf)
import qualified Data.Map.Merge.Strict as Merge
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import           Data.Primitive.ByteArray (ByteArray (..), byteArrayFromList,
                     sizeofByteArray)
import qualified Data.Vector.Primitive as VP
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Base as VU
import           Data.Word
import           Database.LSMTree.Extras
import           Database.LSMTree.Extras.Generators (BiasedKeyForIndexCompact,
                     ChunkSize (..), LogicalPageSummaries,
                     LogicalPageSummary (..), Pages (..), genRawBytes,
                     isKeyForIndexCompact, labelPages, toAppends)
import           Database.LSMTree.Extras.Index (Append (..), appendToCompact)
import           Database.LSMTree.Internal.BitMath
import           Database.LSMTree.Internal.Chunk as Chunk (toByteString)
import           Database.LSMTree.Internal.Entry (NumEntries (..))
import           Database.LSMTree.Internal.Index.Compact
import           Database.LSMTree.Internal.Index.CompactAcc
import           Database.LSMTree.Internal.Page (PageNo (PageNo), PageSpan,
                     multiPage, singlePage)
import           Database.LSMTree.Internal.RawBytes (RawBytes (..))
import qualified Database.LSMTree.Internal.RawBytes as RB
import           Database.LSMTree.Internal.Serialise
import           Numeric (showHex)
import           Prelude hiding (max, min, pi)
import qualified Test.QuickCheck as QC
import           Test.QuickCheck
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.HUnit (assertEqual, testCase)
import           Test.Tasty.QuickCheck (testProperty)
import           Test.Util.Arbitrary (noTags,
                     prop_arbitraryAndShrinkPreserveInvariant)
import           Test.Util.Orphans ()
import           Text.Printf (printf)

tests :: TestTree
tests = testGroup "Test.Database.LSMTree.Internal.Index.Compact" [
    testGroup "TestKey" $
      prop_arbitraryAndShrinkPreserveInvariant @TestKey noTags isTestKey
  , testProperty "prop_distribution @BiasedKeyForIndexCompact" $
      prop_distribution @BiasedKeyForIndexCompact
  , testProperty "prop_searchMinMaxKeysAfterConstruction" $
      prop_searchMinMaxKeysAfterConstruction @BiasedKeyForIndexCompact 100
  , testProperty "prop_differentChunkSizesSameResults" $
      prop_differentChunkSizesSameResults @BiasedKeyForIndexCompact
  , testProperty "prop_singlesEquivMulti" $
      prop_singlesEquivMulti @BiasedKeyForIndexCompact
  , testGroup "(De)serialisation" [
        testGroup "Chunks generator" $
          prop_arbitraryAndShrinkPreserveInvariant noTags chunksInvariant

      , testCase "index-2-clash" $ do
          let k1 = SerialisedKey' (VP.replicate 16 0x00)
          let k2 = SerialisedKey' (VP.replicate 16 0x11)
          let k3 = SerialisedKey' (VP.replicate 15 0x11 <> VP.replicate 1 0x12)
          let (chunks, index) = runST $ do
                ica <- new 16
                ch1 <- flip appendToCompact ica $ AppendSinglePage k1 k2
                ch2 <- flip appendToCompact ica $ AppendSinglePage k3 k3
                (mCh3, idx) <- unsafeEnd ica
                return (ch1 <> ch2 <> toList mCh3, idx)

          let expectedVersion :: [Word8]
              expectedVersion = word32toBytesLE 0x0000_0001 <> word32toBytesLE 0x0000_0000
          let expectedPrimary :: [Word8]
              expectedPrimary = foldMap word64toBytesLE
                  -- 1. primary array: two pages (64 bits LE)
                [ 0x0000_0000_0000_0000,  0x1111_1111_1111_1111
                ]
          let expectedRest :: [Word8]
              expectedRest = foldMap word32toBytesLE
                [ -- 3. clash indicator: two pages, second one has bit
                  0x0000_0002, 0
                  -- 4. larger-than-page: two pages, no bits
                , 0, 0
                  -- 5. clash map: maps k3 to page 1
                , 1, 0 {- size = 1 (64 bit LE) -}
                , 1, 16 {- page 1, key size 16 byte -}
                , 0x1111_1111, 0x1111_1111 {- k3 -}
                , 0x1111_1111, 0x1211_1111
                  -- 6.1 number of pages in the primary array (64 bits LE)
                , 2, 0
                  -- 6.2 number of keys (64bit LE)
                , 7, 0
                ]

          let header = LBS.unpack headerLBS
          let primary = LBS.unpack $
                        LBS.fromChunks (map Chunk.toByteString chunks)
          let rest = LBS.unpack (finalLBS (NumEntries 7) index)

          let comparison msg xs ys = unlines $
                  (msg <> ":")
                : zipWith (\x y -> x <> "  |  " <> y)
                    (showBytes xs <> repeat (replicate 17 '.'))
                    (showBytes ys)

          assertEqual (comparison "header" expectedVersion header)
            expectedVersion header
          assertEqual (comparison "primary" expectedPrimary primary)
            expectedPrimary primary
          assertEqual (comparison "rest" expectedRest rest)
            expectedRest rest

      , testProperty "prop_roundtrip_chunks" $
          prop_roundtrip_chunks
      , testProperty "prop_roundtrip" $
          prop_roundtrip @BiasedKeyForIndexCompact
      , testProperty "prop_total_deserialisation" $ withMaxSuccess 10000
          prop_total_deserialisation
      , testProperty "prop_total_deserialisation_whitebox" $ withMaxSuccess 10000
          prop_total_deserialisation_whitebox
      ]
  ]

{-------------------------------------------------------------------------------
  Test key
-------------------------------------------------------------------------------}

-- | Key type for compact index tests
--
-- Tests outside this module don't have to worry about generating clashing keys.
-- We can assume that the compact index handles clashes correctly, because we
-- test this extensively in this module already.
newtype TestKey = TestKey RawBytes
  deriving stock (Show, Eq, Ord)
  deriving newtype SerialiseKey

-- | Generate keys with a non-neglible probability of clashes. This generates
-- sliced keys too.
--
-- Note: recall that keys /clash/ only if their primary bits (first 8 bytes)
-- match. It does not matter whether the other bytes do not match.
instance Arbitrary TestKey where
  arbitrary = do
      -- Generate primary bits from a relatively small distribution. This
      -- ensures that we get clashes between keys with a non-negligible
      -- probability.
      primBits <- do
        lastPrefixByte <- QC.getSmall <$> arbitrary
        pure $ RB.pack ([0,0,0,0,0,0,0] <> [lastPrefixByte])
      -- The rest of the bits after the primary bits can be anything
      restBits <- genRawBytes
      -- The compact index should store keys without retaining unused memory.
      -- Therefore, we generate slices of keys too.
      prefix <- elements [RB.pack [], RB.pack [0]]
      suffix <- elements [RB.pack [], RB.pack [0]]
      -- Combine the bytes and make sure to take out only the slice we need.
      let bytes = prefix <> primBits <> restBits <> suffix
          n = RB.size primBits + RB.size restBits
          bytes' = RB.take n $ RB.drop (RB.size prefix) bytes
      pure $ TestKey bytes'

  -- Shrink keys extensively: most failures will occur in small counterexamples,
  -- so we don't have to limit the number of shrinks as much.
  shrink (TestKey bytes) = [
        TestKey bytes'
      | let RawBytes vec = bytes
      , vec' <- VP.fromList <$> shrink (VP.toList vec)
      , let bytes' = RawBytes vec'
      , isKeyForIndexCompact bytes'
      ]

isTestKey :: TestKey -> Bool
isTestKey (TestKey bytes) = isKeyForIndexCompact bytes

{-------------------------------------------------------------------------------
  Properties
-------------------------------------------------------------------------------}

--
-- Search
--

type CounterM a = State Int a

evalCounterM :: CounterM a -> Int -> a
evalCounterM = evalState

incrCounter :: CounterM Int
incrCounter = get >>= \c -> put (c+1) >> pure c

plusCounter :: Int -> CounterM Int
plusCounter n = get >>= \c -> put (c+n) >> pure c

-- | After construction, searching for the minimum/maximum key of every page
-- @pageNo@ returns the @pageNo@.
prop_searchMinMaxKeysAfterConstruction ::
     forall k. (SerialiseKey k, Show k, Ord k)
  => ChunkSize
  -> LogicalPageSummaries k
  -> Property
prop_searchMinMaxKeysAfterConstruction csize ps = eqMapProp real model
  where
    model = evalCounterM (foldM modelSearch Map.empty $ getPages ps) 0

    modelSearch :: Map k PageSpan -> LogicalPageSummary k -> CounterM (Map k PageSpan)
    modelSearch m = \case
        OnePageOneKey k       -> do
          c <- incrCounter
          pure $ Map.insert k (singlePage (PageNo c)) m
        OnePageManyKeys k1 k2 -> do
          c <- incrCounter
          pure $ Map.insert k1 (singlePage (PageNo c)) $ Map.insert k2 (singlePage (PageNo c)) m
        MultiPageOneKey k n -> do
          let incr = 1 + fromIntegral n
          c <- plusCounter incr
          pure $ if incr == 1 then Map.insert k (singlePage (PageNo c)) m
                              else Map.insert k (multiPage (PageNo c) (PageNo $ c + fromIntegral n)) m

    real = foldMap' realSearch (getPages ps)

    ic = fromPageSummaries (coerce csize) ps

    realSearch :: LogicalPageSummary k -> Map k PageSpan
    realSearch = \case
        OnePageOneKey k           -> Map.singleton k (search (serialiseKey k) ic)
        OnePageManyKeys k1 k2     -> Map.fromList [ (k1, search (serialiseKey k1) ic)
                                                  , (k2, search (serialiseKey k2) ic)]
        MultiPageOneKey k _       -> Map.singleton k (search (serialiseKey k) ic)

--
-- Construction
--

prop_differentChunkSizesSameResults ::
     SerialiseKey k
  => ChunkSize
  -> ChunkSize
  -> LogicalPageSummaries k
  -> Property
prop_differentChunkSizesSameResults csize1 csize2 ps =
    fromPageSummaries csize1 ps === fromPageSummaries csize2 ps

-- | Constructing an index using only 'appendSingle' is equivalent to using a
-- mix of 'appendSingle' and 'appendMulti'.
prop_singlesEquivMulti ::
     SerialiseKey k
  => ChunkSize
  -> ChunkSize
  -> LogicalPageSummaries k
  -> Property
prop_singlesEquivMulti csize1 csize2 ps = ic1 === ic2
  where
    apps1 = toAppends ps
    apps2 = concatMap toSingles apps1
    ic1 = fromList (coerce csize1) apps1
    ic2 = fromListSingles (coerce csize2) apps2

    toSingles :: Append -> [(SerialisedKey, SerialisedKey)]
    toSingles (AppendSinglePage k1 k2) = [(k1, k2)]
    toSingles (AppendMultiPage k n)    = replicate (fromIntegral n + 1) (k, k)

-- | Distribution of generated test data
prop_distribution :: SerialiseKey k => LogicalPageSummaries k -> Property
prop_distribution ps =
    labelPages ps $ labelIndex (fromPageSummaries 100 ps) $ property True

-- Generate and serialise chunks directly.
-- This gives more direct shrinking of individual parts and has fewer invariants
-- (covering a larger space).
prop_roundtrip_chunks :: Chunks -> NumEntries -> Property
prop_roundtrip_chunks (Chunks chunks index) numEntries =
    counterexample (show (SBS.length sbs) <> " bytes") $
    counterexample ("header:\n" <> showBS bsVersion) $
    counterexample ("primary:\n" <> showBS bsPrimary) $
    counterexample ("rest:\n" <> showBS bsRest) $
      Right (numEntries, index) === fromSBS sbs
  where
    bsVersion = headerLBS
    bsPrimary = LBS.fromChunks $
                map (Chunk.toByteString . word64VectorToChunk) chunks
    bsRest = finalLBS numEntries index
    sbs = SBS.toShort (LBS.toStrict (bsVersion <> bsPrimary <> bsRest))

    showBS = unlines . showBytes . LBS.unpack

-- Generate the compact index from logical pages.
prop_roundtrip :: SerialiseKey k => ChunkSize -> LogicalPageSummaries k -> NumEntries -> Property
prop_roundtrip csize ps numEntries =
    counterexample (show (SBS.length sbs) <> " bytes") $
    counterexample ("header:\n" <> showBS bsVersion) $
    counterexample ("primary:\n" <> showBS bsPrimary) $
    counterexample ("rest:\n" <> showBS bsRest) $
      Right (numEntries, index) === fromSBS sbs
  where
    index = fromPageSummaries csize ps

    (bsVersion, bsPrimary, bsRest) = writeIndexCompact numEntries csize ps
    sbs = SBS.toShort (LBS.toStrict (bsVersion <> bsPrimary <> bsRest))

    showBS = unlines . showBytes . LBS.unpack

prop_total_deserialisation :: [Word32] -> Property
prop_total_deserialisation word32s =
    let !(ByteArray ba) = byteArrayFromList word32s
    in case fromSBS (SBS.SBS ba) of
      Left err -> label err $ property True
      Right (numEntries, ic) -> label "parsed successfully" $ property $
        -- Just forcing the index is not enough. The underlying vectors might
        -- point to outside of the byte array, so we check they are valid.
        (numEntries, ic) `deepseq`
             vec64IsValid (icPrimary ic)
          && bitVecIsValid (icClashes ic)
          && bitVecIsValid (icLargerThanPage ic)
  where
    vec64IsValid (VU.V_Word64 (VP.Vector off len ba)) =
      off >= 0 && len >= 0 && mul8 (off + len) <= sizeofByteArray ba
    bitVecIsValid (BV.BitVec off len ba) =
      off >= 0 && len >= 0 && ceilDiv8 (off + len) <= sizeofByteArray ba

prop_total_deserialisation_whitebox :: Small Word16 -> Small Word16 -> [Word32] -> Property
prop_total_deserialisation_whitebox numEntries numPages word32s =
    prop_total_deserialisation $
         [1]  -- version
      <> word32s  -- primary array, clash bits, LTP bits, clash map
      <> [0 | even (length word32s)]  -- padding
      <> [ fromIntegral numPages, 0
         , fromIntegral numEntries, 0
         ]

{-------------------------------------------------------------------------------
  Util
-------------------------------------------------------------------------------}

writeIndexCompact :: SerialiseKey k => NumEntries -> ChunkSize -> LogicalPageSummaries k -> (LBS.ByteString, LBS.ByteString, LBS.ByteString)
writeIndexCompact numEntries (ChunkSize csize) ps = runST $ do
    ica <- new csize
    cs <- mapM (`appendToCompact` ica) (toAppends ps)
    (c, index) <- unsafeEnd ica
    return
      ( headerLBS
      , LBS.fromChunks $
        foldMap (map Chunk.toByteString) $ cs <> pure (toList c)
      , finalLBS numEntries index
      )

fromPageSummaries :: SerialiseKey k => ChunkSize -> LogicalPageSummaries k -> IndexCompact
fromPageSummaries (ChunkSize csize) ps =
    fromList csize (toAppends ps)

fromList :: Int -> [Append] -> IndexCompact
fromList maxcsize apps = runST $ do
    ica <- new maxcsize
    mapM_ (`appendToCompact` ica) apps
    (_, index) <- unsafeEnd ica
    pure index

-- | One-shot construction using only 'appendSingle'.
fromListSingles :: Int -> [(SerialisedKey, SerialisedKey)] -> IndexCompact
fromListSingles maxcsize apps = runST $ do
    ica <- new maxcsize
    mapM_ (`appendSingle` ica) apps
    (_, index) <- unsafeEnd ica
    pure index

labelIndex :: IndexCompact -> (Property -> Property)
labelIndex ic =
      QC.tabulate "# Clashes" [showPowersOf10 nclashes]
    . QC.tabulate "# Contiguous clash runs" [showPowersOf10 (length nscontig)]
    . QC.tabulate "Length of contiguous clash runs" (fmap (showPowersOf10 . snd) nscontig)
    . QC.tabulate "Contiguous clashes contain multi-page values" (fmap (show . fst) nscontig)
    . QC.classify (multiPageValuesClash ic) "Has clashing multi-page values"
  where nclashes       = countClashes ic
        nscontig       = countContiguousClashes ic

multiPageValuesClash :: IndexCompact -> Bool
multiPageValuesClash ic
    | VU.length (icClashes ic) < 3 = False
    | otherwise                   = VU.any p $ VU.zip4 v1 v2 v3 v4
  where
    -- note: @i = j - 1@ and @k = j + 1@. This gives us a local view of a
    -- triplet of contiguous LTP bits, and a C bit corresponding to the middle
    -- of the triplet.
    p (cj, ltpi, ltpj, ltpk) =
          -- two multi-page values border eachother
         unBit ltpi && not (unBit ltpj) && unBit ltpk
         -- and they clash
      && unBit cj
    v1 = VU.tail (icClashes ic)
    v2 = icLargerThanPage ic
    v3 = VU.tail v2
    v4 = VU.tail v3

-- Returns the number of entries and whether any multi-page values were in the
-- contiguous clashes
countContiguousClashes :: IndexCompact -> [(Bool, Int)]
countContiguousClashes ic = actualContigClashes
  where
    -- filtered is a list of maximal sub-vectors that have only only contiguous
    -- clashes
    zipped    = VU.zip (icClashes ic) (icLargerThanPage ic)
    grouped   = VU.groupBy (\x y -> fst x == fst y) zipped
    filtered  = filter (VU.all (\(c, _ltp) -> c == Bit True)) grouped
    -- clashes that are part of a multi-page value shouldn't be counted towards
    -- the total number of /actual/ clashes. We only care about /actual/ clashes
    -- if they total more than 1 (otherwise it's just a single clash)
    actualContigClashes = filter (\(_, n) -> n > 1) $
                          fmap (\v -> (countLTP v > 0, countC v - countLTP v)) filtered
    countC              = VU.length
    countLTP            = BV.countBits . VU.map snd

-- | Point-wise equality test for two maps, returning counterexamples for each
-- mismatch.
eqMapProp :: (Ord k, Eq v, Show k, Show v) => Map k v -> Map k v -> Property
eqMapProp m1 m2 = conjoin . Map.elems $
    Merge.merge
      (Merge.mapMissing onlyLeft)
      (Merge.mapMissing onlyRight)
      (Merge.zipWithMatched both)
      m1
      m2
  where
    onlyLeft k x = flip counterexample (property False) $
      printf "Key-value pair only on the left, (k, x) = (%s, %s)" (show k) (show x)
    onlyRight k y = flip counterexample (property False) $
      printf "Key-value pair only on the right, (k, y) = (%s, %s)" (show k) (show y)
    both k x y = flip counterexample (property (x == y)) $
      printf "Mismatch between x and y, (k, x, y) = (%s, %s, %s)" (show k) (show x) (show y)

showBytes :: [Word8] -> [String]
showBytes = map (unwords . map (foldMap showByte) . chunksOf 4) . chunksOf 8

showByte :: Word8 -> String
showByte b = let str = showHex b "" in replicate (2 - length str) '0' <> str

word32toBytesLE :: Word32 -> [Word8]
word32toBytesLE = take 4 . map fromIntegral . iterate (`div` 256)

word64toBytesLE :: Word64 -> [Word8]
word64toBytesLE = take 8 . map fromIntegral . iterate (`div` 256)

data Chunks = Chunks [VU.Vector Word64] IndexCompact
  deriving stock (Show)

-- | The concatenated chunks must correspond to the primary array of the index.
-- Apart from that, the only invariant we make sure to uphold is that the length
-- of the vectors match each other, as this is required for correct
-- deserialisation. These invariants do not guarantee that the 'IndexCompact' is
-- valid in other ways (e.g. can successfully be queried).
chunksInvariant :: Chunks -> Bool
chunksInvariant (Chunks chunks IndexCompact {..}) =
       VU.length icPrimary == sum (map VU.length chunks)
    && VU.length icClashes == VU.length icPrimary
    && VU.length icLargerThanPage == VU.length icPrimary

instance Arbitrary Chunks where
  arbitrary = do
    chunks <- map VU.fromList <$> arbitrary
    let icPrimary = mconcat chunks
    let numPages = VU.length icPrimary

    icClashes <- VU.fromList . map Bit <$> vector numPages
    icLargerThanPage <- VU.fromList . map Bit <$> vector numPages
    icTieBreaker <- arbitrary
    return (Chunks chunks IndexCompact {..})

  shrink (Chunks chunks index) =
    -- shrink number of pages
    [ Chunks chunks' index
        { icPrimary = primary'
        , icClashes = VU.slice 0 numPages' (icClashes index)
        , icLargerThanPage = VU.slice 0 numPages' (icLargerThanPage index)
        }
    | chunks' <- shrink chunks
    , let primary' = mconcat chunks'
    , let numPages' = VU.length primary'
    ] ++
    -- shrink tie breaker
    [ Chunks chunks index
        { icTieBreaker = tieBreaker'
        }
    | tieBreaker' <- shrink (icTieBreaker index)
    ]
