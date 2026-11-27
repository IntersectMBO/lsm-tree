module Test.Database.LSMTree.Internal.Index.Prefix (tests) where

import           Control.DeepSeq (force)
import           Control.Exception (evaluate)
import           Control.Monad.ST.Strict (runST, stToIO)
import           Data.Bits ((.&.))
import qualified Data.ByteString as ByteString (pack, unpack)
import qualified Data.ByteString.Lazy as LazyByteString (fromChunks, fromStrict,
                     length, toStrict, unpack)
import           Data.ByteString.Short (ShortByteString)
import qualified Data.ByteString.Short as ShortByteString (fromShort, length,
                     toShort)
import           Data.Either (isLeft)
import           Data.List (find, inits, isPrefixOf)
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map (fromList, lookup)
import           Data.Maybe (listToMaybe, maybeToList)
import qualified Data.Vector.Primitive as VP
import           Data.Word (Word32, Word64, Word8, byteSwap32)
import           Database.LSMTree.Extras.Generators (ChunkSize (ChunkSize),
                     LogicalPageSummaries, labelPages, toAppends)
import           Database.LSMTree.Extras.Index
                     (Append (AppendMultiPage, AppendSinglePage),
                     appendToOrdinary, appendToPrefix)
import           Database.LSMTree.Extras.NoThunks (propNoThunks)
import           Database.LSMTree.Internal.Chunk (Chunk)
import qualified Database.LSMTree.Internal.Chunk as Chunk (toByteString,
                     toByteVector)
import           Database.LSMTree.Internal.Entry (NumEntries (NumEntries))
import           Database.LSMTree.Internal.Index.Ordinary (IndexOrdinary)
import qualified Database.LSMTree.Internal.Index.Ordinary as Ordinary (search,
                     sizeInPages)
import qualified Database.LSMTree.Internal.Index.OrdinaryAcc as OrdinaryAcc
                     (new, unsafeEnd)
import           Database.LSMTree.Internal.Index.Prefix
                     (IndexPrefix (IndexPrefix), finalLBS, fromSBS, headerLBS,
                     restartInterval, search, sizeInPages)
import qualified Database.LSMTree.Internal.Index.PrefixAcc as PrefixAcc (new,
                     newWithDefaults, unsafeEnd)
import           Database.LSMTree.Internal.Page (NumPages (NumPages),
                     PageNo (PageNo), PageSpan (PageSpan))
import qualified Database.LSMTree.Internal.RawBytes as RB (pack, unpack)
import           Database.LSMTree.Internal.Serialise
                     (SerialisedKey (SerialisedKey))
import           Database.LSMTree.Internal.Vector (byteVectorFromPrim)
import           Test.QuickCheck (Arbitrary (arbitrary, shrink),
                     Args (chatty, maxSuccess), Gen, Property, Result (output),
                     chooseInt, conjoin, counterexample, cover, elements,
                     frequency, ioProperty, isSuccess, listOf, once,
                     quickCheckWithResult, shrinkList, stdArgs, tabulate,
                     vectorOf, (.&&.), (===), (==>))
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.HUnit (Assertion, assertBool, assertEqual, testCase)
import           Test.Tasty.QuickCheck (testProperty)

-- * Tests

tests :: TestTree
tests = testGroup "Test.Database.LSMTree.Internal.Index.Prefix" $
        [
            testGroup "Search" $
            [
                testProperty "Present keys agree with ordinary index \
                             \(random keys)" $
                    prop_presentKeysAgreeWithOrdinary @SerialisedKey,
                testProperty "Present keys agree with ordinary index \
                             \(overlapping keys)" $
                    prop_presentKeysAgreeWithOrdinary @OverlapKey,
                testProperty "Result brackets the key (random keys)" $
                    prop_resultBracketsKey @SerialisedKey,
                testProperty "Result brackets the key (overlapping keys)" $
                    prop_resultBracketsKey @OverlapKey,
                testProperty "Result is ordinary result or its predecessor \
                             \(random keys)" $
                    prop_resultIsOrdinaryOrPredecessor @SerialisedKey,
                testProperty "Result is ordinary result or its predecessor \
                             \(overlapping keys)" $
                    prop_resultIsOrdinaryOrPredecessor @OverlapKey,
                testProperty "Run reader seek finds first key at least \
                             \the given one (random keys)" $
                    prop_seekFindsFirstKeyAtLeast @SerialisedKey,
                testProperty "Run reader seek finds first key at least \
                             \the given one (overlapping keys)" $
                    prop_seekFindsFirstKeyAtLeast @OverlapKey
            ],
            testGroup "Construction" $
            [
                testProperty "Separators are as specified \
                             \(random keys)" $
                    prop_separatorsAsSpecified @SerialisedKey,
                testProperty "Separators are as specified \
                             \(overlapping keys)" $
                    prop_separatorsAsSpecified @OverlapKey,
                testProperty "Number of pages is correct" $
                    prop_sizeInPages @OverlapKey,
                testProperty "Chunks make up the entries" $
                    prop_chunksMakeUpEntries @OverlapKey,
                testProperty "Chunk size does not matter" $
                    prop_chunkSizeDoesNotMatter @OverlapKey,
                testProperty "No thunks in new accumulator"
                    prop_noThunks_new,
                testProperty "No thunks after appends" $
                    prop_noThunks_appends @OverlapKey
            ],
            testGroup "(De)serialisation" $
            [
                testProperty "Header is type and version plus padding" $
                    once prop_header,
                testProperty "Round trip (random keys)" $
                    prop_roundTrip @SerialisedKey,
                testProperty "Round trip (overlapping keys)" $
                    prop_roundTrip @OverlapKey,
                testProperty "Wrong type or version is rejected" $
                    prop_wrongTypeAndVersionRejected,
                testProperty "Swapped endianness is rejected" $
                    prop_swappedEndiannessRejected,
                testProperty "Corrupted input is rejected or canonical" $
                    prop_corruptedInputRejectedOrCanonical,
                testProperty "No thunks after deserialisation" $
                    prop_noThunks_fromSBS @OverlapKey
            ],
            testGroup "Unit tests" $
            [
                testCase "Empty index" unit_emptyIndex,
                testCase "Single page" unit_singlePage,
                testCase "Serialised format" unit_serialisedFormat,
                testCase "Empty key and keys that are prefixes of each other"
                    unit_prefixKeys,
                testCase "Long shared prefix" unit_longSharedPrefix,
                testCase "Overflow pages" unit_overflowPages,
                testCase "Zero overflow pages same as single page"
                    unit_zeroOverflowPages,
                testCase "Restart boundaries" unit_restartBoundaries,
                testCase "Numbers not in shortest form are rejected"
                    unit_nonShortestNumbersRejected
            ]
        ]

-- * Tested type and version

-- | The type and version of the index to which these tests refer.
testedTypeAndVersion :: Word32
testedTypeAndVersion = 0x0201

-- * Keys

-- | Key types that test inputs can be generated for.
class (Arbitrary k, Ord k, Show k) => TestKey k where

    toSerialisedKey :: k -> SerialisedKey

instance TestKey SerialisedKey where

    toSerialisedKey = id

{-|
    Keys with a lot of overlap: a few common prefixes, followed by a few bytes
    from a small alphabet. Generated keys are often empty, prefixes of each
    other, or share long prefixes.

    The 'Ord' instance on byte lists is lexicographic, like the one on
    'SerialisedKey'.
-}
newtype OverlapKey = OverlapKey [Word8]
    deriving stock (Eq, Ord, Show)

instance Arbitrary OverlapKey where

    arbitrary = do
        prefix <- elements $
                  [
                      [],
                      [0x61],
                      bytesOf "account/",
                      bytesOf "account/0000",
                      replicate 40 0x78,
                      [0x00, 0x00, 0x00]
                  ]
        rest <- listOf' (elements [0x00, 0x01, 0x30, 0x31, 0x39, 0x7F, 0xFF])
        pure (OverlapKey (prefix ++ rest))
        where

        listOf' :: Gen a -> Gen [a]
        listOf' gen = chooseInt (0, 5) >>= flip vectorOf gen

    shrink (OverlapKey bytes) = OverlapKey <$> shrinkList shrink bytes

instance TestKey OverlapKey where

    toSerialisedKey (OverlapKey bytes) = keyFromBytes bytes

bytesOf :: String -> [Word8]
bytesOf = map (fromIntegral . fromEnum)

keyFromBytes :: [Word8] -> SerialisedKey
keyFromBytes = SerialisedKey . RB.pack

bytesFromKey :: SerialisedKey -> [Word8]
bytesFromKey (SerialisedKey rawBytes) = RB.unpack rawBytes

-- * Model

-- | A group of pages: the first and the last key and the number of pages.
data Group = Group !SerialisedKey !SerialisedKey !Int
    deriving stock Show

-- | Yields the group that an append operation adds.
groupFromAppend :: Append -> Group
groupFromAppend (AppendSinglePage firstKey lastKey)
    = Group firstKey lastKey 1
groupFromAppend (AppendMultiPage key overflowPageCount)
    = Group key key (succ (fromIntegral overflowPageCount))

-- | Yields the groups together with their page spans.
groupSpans :: [Append] -> [(Group, PageSpan)]
groupSpans appends = zip groups spans where

    groups :: [Group]
    groups = map groupFromAppend appends

    spans :: [PageSpan]
    spans = zipWith (\ start (Group _ _ pageCount) ->
                         PageSpan (PageNo start)
                                  (PageNo (start + pageCount - 1)))
                    (scanl (+) 0 [pageCount | Group _ _ pageCount <- groups])
                    groups

-- | Yields the specified separators.
expectedSeparators :: [Append] -> [[Word8]]
expectedSeparators appends
    = case map groupFromAppend appends of
          []              -> []
          groups@(_ : others)
              -> [] : zipWith separatorOf groups others
    where

    separatorOf :: Group -> Group -> [Word8]
    separatorOf (Group _ lesser _) (Group greater _ _)
        = case [ candidate
               | candidate <- drop 1 (inits (bytesFromKey greater)),
                 keyFromBytes candidate > lesser ] of
              candidate : _ -> candidate
              []            -> error "separatorOf: keys are not increasing"


{-|
    Decodes the entries of an index independently of the module under test,
    yielding the separator and the number of pages of each group.
-}
decodeEntries :: IndexPrefix -> [([Word8], Int)]
decodeEntries (IndexPrefix entries _ entryCount _)
    = go entryCount (VP.toList entries) []
    where

    go :: Int -> [Word8] -> [Word8] -> [([Word8], Int)]
    go 0 _ _ = []
    go remaining bytes previous = (current, pageCount) :
                                  go (pred remaining) rest current
        where

        (header, bytes1) = varint bytes
        (suffixLength, bytes2) = varint bytes1
        (pageCount, bytes3)
            | header .&. 1 == 0 = (1, bytes2)
            | otherwise         = let

                                      (overflowPageCount, bytes')
                                          = varint bytes2

                                  in (succ overflowPageCount, bytes')
        (suffix, rest) = splitAt suffixLength bytes3
        current = take (header `div` 2) previous ++ suffix

    varint :: [Word8] -> (Int, [Word8])
    varint (byte : others)
        | byte < 0x80 = (fromIntegral byte, others)
        | otherwise   = let

                            (high, others') = varint others

                        in (fromIntegral (byte - 0x80) + 0x80 * high, others')
    varint [] = error "varint: unexpected end of input"

{-|
    Encodes separators and page counts independently of the module under test,
    in the only way that the format allows.
-}
encodeEntries :: [([Word8], Int)] -> [Word8]
encodeEntries groups
    = concat (zipWith3 encode [0 ..] ([] : map fst groups) groups)
    where

    encode :: Int -> [Word8] -> ([Word8], Int) -> [Word8]
    encode index previous (current, pageCount)
        = varint (2 * shared + fromEnum (pageCount > 1)) ++
          varint (length suffix)                         ++
          concat [ varint (pred pageCount) | pageCount > 1 ] ++
          suffix
        where

        shared :: Int
        shared | index `rem` restartInterval == 0
                   = 0
               | otherwise
                   = length (takeWhile id (zipWith (==) previous current))

        suffix :: [Word8]
        suffix = drop shared current

    varint :: Int -> [Word8]
    varint value
        | value < 0x80 = [fromIntegral value]
        | otherwise    = fromIntegral (value `mod` 0x80 + 0x80) :
                         varint (value `div` 0x80)

{-|
    Searches the decoded entries in the simplest possible way: the result is
    the span of the last group whose separator is less than or equal to the
    key.
-}
referenceSearch :: SerialisedKey -> [([Word8], Int)] -> PageSpan
referenceSearch key groups
    = case [ pageSpan | ((separator, _), pageSpan) <- zip groups spans,
                        keyFromBytes separator <= key ] of
          []      -> PageSpan (PageNo 0) (PageNo 0)
          matches -> last matches
    where

    spans :: [PageSpan]
    spans = zipWith (\ start pageCount ->
                         PageSpan (PageNo start)
                                  (PageNo (start + pageCount - 1)))
                    (scanl (+) 0 (map snd groups))
                    (map snd groups)

{-|
    Yields keys to search for: all keys of the groups, all prefixes of these,
    and keys just above them, which are often between the first and the last
    key of a page.
-}
probeKeys :: [Append] -> [SerialisedKey]
probeKeys = probeKeysWith 0

{-|
    Like 'probeKeys', but only with those prefixes of keys that have at least
    the given length.
-}
probeKeysWith :: Int -> [Append] -> [SerialisedKey]
probeKeysWith minPrefixLength appends
    = map keyFromBytes $
      [] : concat [ drop minPrefixLength (inits bytes) ++
                    [bytes ++ [0x00], bytes ++ [0xFF]]
                  | Group firstKey lastKey _ <- groups,
                    key <- [firstKey, lastKey],
                    let bytes = bytesFromKey key ]
    where

    groups :: [Group]
    groups = map groupFromAppend appends

-- | Yields the probe keys for some appends plus some extra keys.
probeKeysPlus :: TestKey k => [k] -> [Append] -> [SerialisedKey]
probeKeysPlus extraKeys appends
    = map toSerialisedKey extraKeys ++ probeKeys appends

-- * Construction utilities

-- | Constructs an index incrementally, yielding all chunks and the index.
construct :: Int -> [Append] -> ([Chunk], IndexPrefix)
construct minChunkSize appends = runST $ do
    acc <- PrefixAcc.new minChunkSize
    commonChunks <- concat <$> mapM (`appendToPrefix` acc) appends
    (remnant, index) <- PrefixAcc.unsafeEnd acc
    pure (commonChunks ++ maybeToList remnant, index)

-- | Constructs an index with a typical chunk size.
constructIndex :: [Append] -> IndexPrefix
constructIndex = snd . construct 4096

-- | Constructs an ordinary index for comparison.
constructOrdinary :: [Append] -> IndexOrdinary
constructOrdinary appends = runST $ do
    acc <- OrdinaryAcc.new 0x100 0x1000
    mapM_ (`appendToOrdinary` acc) appends
    snd <$> OrdinaryAcc.unsafeEnd acc

-- | Constructs a complete serialised index and the in-memory index.
serialise :: NumEntries -> Int -> [Append] -> (ShortByteString, IndexPrefix)
serialise entryCount minChunkSize appends = (serialised, index) where

    chunks :: [Chunk]
    index :: IndexPrefix
    (chunks, index) = construct minChunkSize appends

    serialised :: ShortByteString
    serialised = ShortByteString.toShort                              $
                 LazyByteString.toStrict                              $
                 headerLBS                                         <>
                 LazyByteString.fromChunks (map Chunk.toByteString chunks) <>
                 finalLBS entryCount index

-- | Yields the appends for some pages.
appendsOf :: TestKey k => LogicalPageSummaries k -> [Append]
appendsOf = toAppends . fmap toSerialisedKey

-- | Yields the entries of an index.
entriesOf :: IndexPrefix -> VP.Vector Word8
entriesOf (IndexPrefix entries _ _ _) = entries

-- | Yields the restart table of an index.
restartsOf :: IndexPrefix -> VP.Vector Word32
restartsOf (IndexPrefix _ restarts _ _) = restarts

-- * Checks shared by properties and unit tests

{-|
    For every key of every group, the search result is the group's page span,
    which is also what the ordinary index yields.
-}
checkPresentKeys :: [Append] -> Property
checkPresentKeys appends
    = conjoin [ counterexample ("Key: " ++ show key) $
                (search key index, Ordinary.search key ordinary)
                ===
                (pageSpan, pageSpan)
              | (Group firstKey lastKey _, pageSpan) <- groupSpans appends,
                key <- [firstKey, lastKey] ]
    where

    index :: IndexPrefix
    index = constructIndex appends

    ordinary :: IndexOrdinary
    ordinary = constructOrdinary appends

{-|
    Yields the groups with their page spans, indexed by group number, and a
    function that yields the number of the group with a given page span, if
    there is such a group.
-}
groupTable :: [Append]
           -> (Map Int (Group, PageSpan), PageSpan -> Maybe Int)
groupTable appends = (byNumber, groupNumber) where

    spans :: [(Group, PageSpan)]
    spans = groupSpans appends

    byNumber :: Map Int (Group, PageSpan)
    byNumber = Map.fromList (zip [0 ..] spans)

    byStart :: Map PageNo (Int, PageSpan)
    byStart = Map.fromList [ (start, (number, pageSpan))
                           | (number, (_, pageSpan@(PageSpan start _)))
                                 <- zip [0 ..] spans ]

    groupNumber :: PageSpan -> Maybe Int
    groupNumber pageSpan@(PageSpan start _)
        = case Map.lookup start byStart of
              Just (number, pageSpan') | pageSpan' == pageSpan -> Just number
              _                                                -> Nothing

{-|
    Combines the results of checking many keys into a single property, which
    lists the failing keys.
-}
checkKeys :: [(SerialisedKey, Maybe String)] -> Property
checkKeys results
    = counterexample (unlines [ "Key " ++ show key ++ ": " ++ msg
                              | (key, Just msg) <- failures ]) $
      null failures
    where

    failures :: [(SerialisedKey, Maybe String)]
    failures = take 10 [ result | result@(_, Just _) <- results ]

{-|
    For every probe key @k@, the search result is the page span of a group such
    that all groups before it only contain keys less than @k@ and all groups
    after it only contain keys greater than @k@.

    Since the keys of the groups are increasing, it is enough to check the
    last key of the preceding group and the first key of the following group.
-}
checkBrackets :: [SerialisedKey] -> [Append] -> Property
checkBrackets probes appends
    = checkKeys [ (key, verdict key (search key index))
                | not (null appends),
                  key <- probes ]
    where

    index :: IndexPrefix
    index = constructIndex appends

    byNumber :: Map Int (Group, PageSpan)
    groupNumber :: PageSpan -> Maybe Int
    (byNumber, groupNumber) = groupTable appends

    verdict :: SerialisedKey -> PageSpan -> Maybe String
    verdict key result = case groupNumber result of
        Nothing
            -> Just ("result " ++ show result ++ " is not a group's span")
        Just number
            | Just (Group _ lastKey _, _) <- Map.lookup (pred number) byNumber,
              lastKey >= key
                -> Just ("result " ++ show result ++
                         ", but preceding group ends at " ++ show lastKey)
            | Just (Group firstKey _ _, _) <- Map.lookup (succ number) byNumber,
              firstKey <= key
                -> Just ("result " ++ show result ++
                         ", but following group starts at " ++ show firstKey)
            | otherwise
                -> Nothing

{-|
    For every probe key, the search result is the same as for the ordinary
    index, or it is the group immediately before and only contains keys less
    than the key.
-}
checkOrdinaryOrPredecessor :: [SerialisedKey] -> [Append] -> Property
checkOrdinaryOrPredecessor probes appends
    = tabulate "Result" (map fst verdicts) $
      checkKeys [ (key, failure) | (_, (key, failure)) <- verdicts ]
    where

    verdicts :: [(String, (SerialisedKey, Maybe String))]
    verdicts = [ classify key (search key index) (Ordinary.search key ordinary)
               | not (null appends),
                 key <- probes ]

    index :: IndexPrefix
    index = constructIndex appends

    ordinary :: IndexOrdinary
    ordinary = constructOrdinary appends

    byNumber :: Map Int (Group, PageSpan)
    groupNumber :: PageSpan -> Maybe Int
    (byNumber, groupNumber) = groupTable appends

    classify :: SerialisedKey
             -> PageSpan
             -> PageSpan
             -> (String, (SerialisedKey, Maybe String))
    classify key prefixResult ordinaryResult
        | prefixResult == ordinaryResult
            = ("same", (key, Nothing))
        | Just prefixNumber <- groupNumber prefixResult,
          Just ordinaryNumber <- groupNumber ordinaryResult,
          prefixNumber == pred ordinaryNumber,
          Just (Group _ lastKey _, _) <- Map.lookup prefixNumber byNumber,
          lastKey < key
            = ("predecessor", (key, Nothing))
        | otherwise
            = ("neither",
               (key, Just ("prefix " ++ show prefixResult ++
                           ", ordinary " ++ show ordinaryResult)))

-- | The search agrees with a search over independently decoded entries.
checkAgainstReference :: [SerialisedKey] -> [Append] -> Property
checkAgainstReference probes appends
    = conjoin [ counterexample ("Key: " ++ show key) $
                search key index === referenceSearch key decoded
              | key <- probes ]
    where

    index :: IndexPrefix
    index = constructIndex appends

    decoded :: [([Word8], Int)]
    decoded = decodeEntries index

-- | Deserialisation yields the original index and the number of entries.
checkRoundTrip :: [SerialisedKey] -> NumEntries -> [Append] -> Property
checkRoundTrip probes entryCount appends
    = counterexample ("Serialised: " ++ show serialised) $
      ShortByteString.length serialised `rem` 8 === 0
      .&&.
      fromSBS serialised === Right (entryCount, index)
      .&&.
      case fromSBS serialised of
          Left _                -> counterexample "Unexpected Left" False
          Right (_, decoded) -> conjoin [ search key decoded
                                          ===
                                          search key index
                                        | key <- probes ]
    where

    serialised :: ShortByteString
    index :: IndexPrefix
    (serialised, index) = serialise entryCount 4096 appends

{-|
    For every probe key @k@, simulating how
    'Database.LSMTree.Internal.RunReader.new' seeks to an offset key finds the
    first key of the run that is greater than or equal to @k@, if there is
    one.

    The run reader reads the first page of the search result and looks for
    the first key in it that is greater than or equal to @k@. If there is no
    such key, it continues with the first key of the page after the search
    result. Since the appends only determine the first and the last key of
    each page, we take these to be the keys of the run.

    Unlike the other checks, this one relies on what the run reader needs,
    which is more than what the contract of
    'Database.LSMTree.Internal.Index.search' guarantees for absent keys.
-}
checkSeek :: [SerialisedKey] -> [Append] -> Property
checkSeek probes appends
    = checkKeys [ (key, verdict key) | key <- probes ]
    where

    index :: IndexPrefix
    index = constructIndex appends

    groups :: [(Group, PageSpan)]
    groups = groupSpans appends

    -- The keys of the group that starts at a given page.
    keysByStart :: Map PageNo [SerialisedKey]
    keysByStart = Map.fromList [ (start, [firstKey, lastKey])
                               | (Group firstKey lastKey _, PageSpan start _)
                                     <- groups ]

    runKeys :: [SerialisedKey]
    runKeys = concat [ [firstKey, lastKey] | (Group firstKey lastKey _, _)
                                                 <- groups ]

    seek :: SerialisedKey -> Either String (Maybe SerialisedKey)
    seek key
        | null appends
            = Right Nothing
        | otherwise
            = case Map.lookup start keysByStart of
                  Nothing
                      -> Left ("result " ++ show result ++
                               " does not start a group")
                  Just keys
                      | Just found <- find (>= key) keys
                          -> Right (Just found)
                      | otherwise
                          -> Right (Map.lookup (PageNo (succ end)) keysByStart
                                    >>= listToMaybe)
        where

        result :: PageSpan
        start :: PageNo
        end :: Int
        result@(PageSpan start (PageNo end)) = search key index

    verdict :: SerialisedKey -> Maybe String
    verdict key = case seek key of
        Left msg
            -> Just msg
        Right found
            | found == find (>= key) runKeys
                -> Nothing
            | otherwise
                -> Just ("seek finds " ++ show found ++
                         ", but expected " ++ show (find (>= key) runKeys))

-- | Runs all checks for a list of appends.
checkAll :: [Append] -> Property
checkAll appends = checkAllWith (probeKeys appends) appends

-- | Runs all checks for a list of appends with the given probe keys.
checkAllWith :: [SerialisedKey] -> [Append] -> Property
checkAllWith probes appends
    = conjoin $
      [
          checkPresentKeys appends,
          checkBrackets probes appends,
          checkOrdinaryOrPredecessor probes appends,
          checkSeek probes appends,
          checkAgainstReference probes appends,
          checkRoundTrip probes (NumEntries 42) appends,
          map (keyFromBytes . fst) (decodeEntries index) ===
          map keyFromBytes (expectedSeparators appends),
          encodeEntries (decodeEntries index) === VP.toList (entriesOf index)
      ]
    where

    index :: IndexPrefix
    index = constructIndex appends

{-|
    Labels a property with the shape of the given pages and reports whether
    interesting cases are covered sufficiently.
-}
labelAndCover :: LogicalPageSummaries k -> [Append] -> Property -> Property
labelAndCover pages appends
    = labelPages pages                                           .
      cover 20 (length appends > restartInterval)
               "more than one restart block"                     .
      cover 20 (any isMulti appends)
               "overflow pages"                                  .
      cover 0.5 (any isMulti (everyRestartEntry (drop 1 appends)))
                "overflow pages at a restart entry other than the first"
    where

    isMulti :: Append -> Bool
    isMulti (AppendSinglePage _ _) = False
    isMulti (AppendMultiPage _ _)  = True

    -- The appends of the restart entries, given that the first one is dropped.
    everyRestartEntry :: [Append] -> [Append]
    everyRestartEntry rest = case drop (pred restartInterval) rest of
        []              -> []
        restart : rest' -> restart : everyRestartEntry rest'

-- | Checks a property without randomness by running it once.
assertProperty :: Property -> Assertion
assertProperty property = do
    result <- quickCheckWithResult stdArgs { maxSuccess = 1, chatty = False }
                                   property
    assertBool (output result) (isSuccess result)

-- * Properties

prop_presentKeysAgreeWithOrdinary :: TestKey k
                                  => LogicalPageSummaries k
                                  -> Property
prop_presentKeysAgreeWithOrdinary pages
    = labelAndCover pages appends (checkPresentKeys appends)
    where

    appends :: [Append]
    appends = appendsOf pages

prop_resultBracketsKey :: TestKey k => [k] -> LogicalPageSummaries k -> Property
prop_resultBracketsKey extraKeys pages
    = checkBrackets (probeKeysPlus extraKeys appends) appends
    where

    appends :: [Append]
    appends = appendsOf pages

prop_resultIsOrdinaryOrPredecessor :: TestKey k
                                   => [k]
                                   -> LogicalPageSummaries k
                                   -> Property
prop_resultIsOrdinaryOrPredecessor extraKeys pages
    = checkOrdinaryOrPredecessor (probeKeysPlus extraKeys appends) appends
    where

    appends :: [Append]
    appends = appendsOf pages

prop_seekFindsFirstKeyAtLeast :: TestKey k
                              => [k]
                              -> LogicalPageSummaries k
                              -> Property
prop_seekFindsFirstKeyAtLeast extraKeys pages
    = checkSeek (probeKeysPlus extraKeys appends) appends
    where

    appends :: [Append]
    appends = appendsOf pages

prop_separatorsAsSpecified :: TestKey k => LogicalPageSummaries k -> Property
prop_separatorsAsSpecified pages
    = map fst (decodeEntries index) === expectedSeparators appends
      .&&.
      checkAgainstReference (probeKeys appends) appends
    where

    appends :: [Append]
    appends = appendsOf pages

    index :: IndexPrefix
    index = constructIndex appends

prop_sizeInPages :: TestKey k => LogicalPageSummaries k -> Property
prop_sizeInPages pages
    = (sizeInPages index, sizeInPages index)
      ===
      (NumPages (fromIntegral expected), Ordinary.sizeInPages ordinary)
    where

    appends :: [Append]
    appends = appendsOf pages

    index :: IndexPrefix
    index = constructIndex appends

    ordinary :: IndexOrdinary
    ordinary = constructOrdinary appends

    expected :: Int
    expected = sum [ pageCount
                   | Group _ _ pageCount <- map groupFromAppend appends ]

prop_chunksMakeUpEntries :: TestKey k
                         => ChunkSize
                         -> LogicalPageSummaries k
                         -> Property
prop_chunksMakeUpEntries (ChunkSize minChunkSize) pages
    = VP.concat (map Chunk.toByteVector chunks) === entriesOf index
    where

    chunks :: [Chunk]
    index :: IndexPrefix
    (chunks, index) = construct minChunkSize (appendsOf pages)

prop_chunkSizeDoesNotMatter :: TestKey k
                            => ChunkSize
                            -> LogicalPageSummaries k
                            -> Property
prop_chunkSizeDoesNotMatter (ChunkSize minChunkSize) pages
    = snd (construct minChunkSize appends) === constructIndex appends
    where

    appends :: [Append]
    appends = appendsOf pages

prop_header :: Property
prop_header = LazyByteString.unpack headerLBS
              ===
              VP.toList (byteVectorFromPrim testedTypeAndVersion) ++
              replicate 4 0

prop_roundTrip :: TestKey k => NumEntries -> LogicalPageSummaries k -> Property
prop_roundTrip entryCount pages
    = labelAndCover pages appends $
      checkRoundTrip (probeKeys appends) entryCount appends
    where

    appends :: [Append]
    appends = appendsOf pages

{-
    The accumulator is evaluated before checking it for thunks, as it would be
    when it is stored in a strict field, so that only thunks in its fields are
    found.
-}

prop_noThunks_new :: Property
prop_noThunks_new = once $ ioProperty $ do
    acc <- evaluate =<< stToIO PrefixAcc.newWithDefaults
    propNoThunks acc

prop_noThunks_appends :: TestKey k => LogicalPageSummaries k -> Property
prop_noThunks_appends pages = ioProperty $ do
    acc <- evaluate =<< stToIO PrefixAcc.newWithDefaults
    mapM_ (stToIO . (`appendToPrefix` acc)) (appendsOf pages)
    accResult <- propNoThunks acc
    (_, index) <- stToIO (PrefixAcc.unsafeEnd acc)
    indexResult <- propNoThunks index
    pure (accResult .&&. indexResult)

prop_noThunks_fromSBS :: TestKey k => LogicalPageSummaries k -> Property
prop_noThunks_fromSBS pages
    = case fromSBS (fst (serialise (NumEntries 0) 4096 (appendsOf pages))) of
          Left msg         -> counterexample msg False
          Right (_, index) -> ioProperty (propNoThunks index)

-- | Replaces the type–version indicator of a serialised index.
replaceTypeAndVersion :: Word32 -> ShortByteString -> ShortByteString
replaceTypeAndVersion typeAndVersion serialised
    = ShortByteString.toShort $
      ByteString.pack $
      VP.toList (byteVectorFromPrim typeAndVersion) ++
      drop 4 (ByteString.unpack (ShortByteString.fromShort serialised))

prop_wrongTypeAndVersionRejected :: Word32
                                 -> LogicalPageSummaries OverlapKey
                                 -> Property
prop_wrongTypeAndVersionRejected typeAndVersion pages
    = typeAndVersion /= testedTypeAndVersion ==>
      isLeft (fromSBS (replaceTypeAndVersion typeAndVersion serialised))
    where

    serialised :: ShortByteString
    serialised = fst (serialise (NumEntries 0) 4096 (appendsOf pages))

prop_swappedEndiannessRejected :: LogicalPageSummaries OverlapKey -> Property
prop_swappedEndiannessRejected pages
    = fromSBS (replaceTypeAndVersion (byteSwap32 testedTypeAndVersion)
                                     serialised)
      ===
      Left "Non-matching endianness"
    where

    serialised :: ShortByteString
    serialised = fst (serialise (NumEntries 0) 4096 (appendsOf pages))

-- | A way of corrupting a serialised index.
data Corruption
    = ReplaceByte Int Word8 -- ^ Replace the byte at a position (modulo length)
    | Truncate Int          -- ^ Keep only a prefix of a length (modulo length)
    | AppendBytes [Word8]   -- ^ Append bytes
    deriving stock Show

instance Arbitrary Corruption where

    arbitrary = frequency $
                [
                    (8, ReplaceByte <$> arbitrary <*> arbitrary),
                    (1, Truncate <$> arbitrary),
                    (1, AppendBytes <$> listOf arbitrary)
                ]

    shrink (ReplaceByte position byte)
        = [ ReplaceByte position' byte'
          | (position', byte') <- shrink (position, byte) ]
    shrink (Truncate len)
        = Truncate <$> shrink len
    shrink (AppendBytes bytes)
        = AppendBytes <$> shrink bytes

corrupt :: Corruption -> [Word8] -> [Word8]
corrupt _ [] = []
corrupt (ReplaceByte position byte) bytes
    = before ++ byte : drop 1 after
    where

    (before, after) = splitAt (position `mod` length bytes) bytes
corrupt (Truncate len) bytes
    = take (len `mod` length bytes) bytes
corrupt (AppendBytes extra) bytes
    = bytes ++ extra

{-|
    Deserialising a corrupted index either fails, or the result is a valid index
    whose serialisation is exactly the corrupted input, whose entries are the
    only possible encoding of their separators, and whose search agrees with a
    search over independently decoded entries. This shows that deserialisation
    checks enough for 'search' to be safe.
-}
prop_corruptedInputRejectedOrCanonical :: Corruption
                                       -> LogicalPageSummaries OverlapKey
                                       -> Property
prop_corruptedInputRejectedOrCanonical corruption pages
    = ioProperty $ do
          result <- evaluate (force (fromSBS corrupted))
          pure $ case result of
              Left msg
                  -> tabulate "Result" ["Left: " ++ msg] True
              Right (entryCount, index)
                  -> tabulate "Result" ["Right"] $
                     conjoin $
                     [
                         LazyByteString.toStrict
                             (headerLBS <>
                              LazyByteString.fromStrict
                                  (ByteString.pack
                                       (VP.toList (entriesOf index))) <>
                              finalLBS entryCount index)
                         ===
                         ShortByteString.fromShort corrupted,
                         encodeEntries (decodeEntries index)
                         ===
                         VP.toList (entriesOf index)
                     ] ++
                     [
                         search key index === referenceSearch key decoded
                     | let decoded = decodeEntries index,
                       key <- probeKeys appends
                     ]
    where

    appends :: [Append]
    appends = appendsOf pages

    corrupted :: ShortByteString
    corrupted = ShortByteString.toShort $
                ByteString.pack $
                corrupt corruption $
                ByteString.unpack $
                ShortByteString.fromShort $
                fst (serialise (NumEntries 7) 4096 appends)

-- * Unit tests

-- | Shorthand for a single page with the given first and last key.
single :: String -> String -> Append
single firstKey lastKey = AppendSinglePage (keyOf firstKey) (keyOf lastKey)

-- | Shorthand for a key–value pair with the given number of overflow pages.
multi :: String -> Word32 -> Append
multi key = AppendMultiPage (keyOf key)

keyOf :: String -> SerialisedKey
keyOf = keyFromBytes . bytesOf

unit_emptyIndex :: Assertion
unit_emptyIndex = do
    let

        index :: IndexPrefix
        index = constructIndex []

    assertEqual "search"
                (PageSpan (PageNo 0) (PageNo 0))
                (search (keyOf "a") index)
    assertEqual "sizeInPages" (NumPages 0) (sizeInPages index)
    assertEqual "fromSBS"
                (Right (NumEntries 0, index))
                (fromSBS (fst (serialise (NumEntries 0) 4096 [])))
    assertEqual "serialised size"
                40
                (ShortByteString.length
                     (fst (serialise (NumEntries 0) 4096 [])))

unit_singlePage :: Assertion
unit_singlePage = do
    let

        appends :: [Append]
        appends = [single "b" "d"]

        index :: IndexPrefix
        index = constructIndex appends

    mapM_ (\ key -> assertEqual ("search " ++ show key)
                                (PageSpan (PageNo 0) (PageNo 0))
                                (search (keyOf key) index))
          ["", "a", "b", "c", "d", "e"]
    assertEqual "entries" [0x00, 0x00] (VP.toList (entriesOf index))
    assertEqual "restarts" [0, 0] (VP.toList (restartsOf index))
    assertProperty (checkAll appends)

{-|
    Pins the serialised format, as documented in @doc/format-run.md@. Numbers
    in the header and footer are in host endianness.
-}
unit_serialisedFormat :: Assertion
unit_serialisedFormat = do
    let

        appends :: [Append]
        appends = [
                      single "aa" "ab",
                      single "ac" "ad",
                      multi "b" 2,
                      single "ba" "bb"
                  ]

        serialised :: ShortByteString
        index :: IndexPrefix
        (serialised, index) = serialise (NumEntries 7) 4096 appends

        word32 :: Word32 -> [Word8]
        word32 = VP.toList . byteVectorFromPrim

        word64 :: Word64 -> [Word8]
        word64 = VP.toList . byteVectorFromPrim

        expectedEntries :: [Word8]
        expectedEntries = concat
            [
                [0x00, 0x00],                   -- "",   shared 0
                [0x00, 0x02] ++ bytesOf "ac",   -- "ac", shared 0
                [0x01, 0x01, 0x02] ++ bytesOf "b", -- "b", 2 overflow pages
                [0x02, 0x01] ++ bytesOf "a"     -- "ba", shared 1
            ]

        expected :: [Word8]
        expected = word32 0x0201 ++ word32 0 ++      -- header
                   expectedEntries ++                -- 13 bytes of entries
                   replicate 3 0 ++                  -- padding
                   word32 0 ++ word32 0 ++           -- restart table
                   word64 13 ++                      -- size of entries
                   word64 4 ++                       -- number of entries
                   word64 6 ++                       -- number of pages
                   word64 7                          -- number of keys

    assertEqual "serialised"
                expected
                (ByteString.unpack (ShortByteString.fromShort serialised))
    assertEqual "header length" 8 (LazyByteString.length headerLBS)
    mapM_ (\ (key, start, end) ->
               assertEqual ("search " ++ show key)
                           (PageSpan (PageNo start) (PageNo end))
                           (search (keyOf key) index))
          [
              ("",      0, 0),
              ("ab",    0, 0),
              ("ab\xFF", 0, 0),
              ("ac",    1, 1),
              ("az",    1, 1),
              ("b",     2, 4),
              ("b\x00", 2, 4),
              ("ba",    5, 5),
              ("zz",    5, 5)
          ]
    assertProperty (checkAll appends)

unit_prefixKeys :: Assertion
unit_prefixKeys = assertProperty $ checkAll $
                  [
                      single "" "a",
                      single "aa" "aa",
                      single "aaa" "aab",
                      multi "aab\x00" 1,
                      single "aab\x00\x00" "ab",
                      single "b" "b",
                      single "b\x00" "b\x00\x00"
                  ]

unit_longSharedPrefix :: Assertion
unit_longSharedPrefix = do
    let

        prefix :: String
        prefix = replicate 200 'x'

        key :: Int -> String
        key n = prefix ++ reverse (take 4 (reverse ("000" ++ show n)))

        appends :: [Append]
        appends = [single (key (2 * n)) (key (2 * n + 1)) | n <- [0 .. 39]]

        index :: IndexPrefix
        index = constructIndex appends

    -- An ordinary index would need 40 * (2 + 204) = 8240 bytes.
    assertBool ("entries are small: " ++ show (VP.length (entriesOf index)))
               (VP.length (entriesOf index) < 1000)
    -- Probing all prefixes of all keys would be slow, so we only probe the
    -- prefixes that are at least as long as the shared prefix.
    assertProperty (checkAllWith (probeKeysWith 200 appends) appends)

unit_overflowPages :: Assertion
unit_overflowPages = do
    assertProperty $ checkAll [multi "a" 3]
    assertProperty $ checkAll [multi "a" 1, single "b" "c", multi "d" 5,
                             multi "e" 1, single "f" "g", multi "h" 2]
    assertProperty $ checkAll [multi (replicate n 'a') 1 | n <- [0 .. 40]]

unit_zeroOverflowPages :: Assertion
unit_zeroOverflowPages
    = assertEqual "index"
                  (constructIndex [single "a" "a", single "b" "b"])
                  (constructIndex [multi "a" 0, multi "b" 0])

unit_restartBoundaries :: Assertion
unit_restartBoundaries
    = mapM_ check $
      [ (entryCount, withMulti)
      | entryCount <- [1, 2, 15, 16, 17, 18, 31, 32, 33, 48, 49],
        withMulti <- [False, True] ]
    where

    check :: (Int, Bool) -> Assertion
    check (entryCount, withMulti) = do
        let

            -- Keys for entry numbers, in increasing order.
            key :: Int -> String
            key entryNumber = "key/" ++ show (100 + entryNumber)

            {-
                With overflow pages, the groups of restart entries and of the
                entries immediately before and after them have overflow pages.
            -}
            append :: Int -> Append
            append entryNumber
                | withMulti && entryNumber `rem` restartInterval `elem`
                               [0, 1, pred restartInterval]
                    = multi (key entryNumber) 2
                | otherwise
                    = single (key entryNumber) (key entryNumber ++ "/z")

            appends :: [Append]
            appends = map append [0 .. pred entryCount]

            index :: IndexPrefix
            index = constructIndex appends

        assertEqual ("restart table size for " ++ show entryCount)
                    (2 * restartCount)
                    (VP.length (restartsOf index))
        assertBool "first restart is at the start"
                   ([0, 0] `isPrefixOf` VP.toList (restartsOf index))
        assertEqual ("restart pages for " ++ show entryCount)
                    [ fromIntegral (restartPage block)
                    | block <- [0 .. pred restartCount] ]
                    (everySecond (drop 1 (VP.toList (restartsOf index))))
        assertProperty (checkAll appends)
        where

        -- With overflow pages, groups 0, 1 and 15 of each block of 16 have
        -- 3 pages.
        restartPage :: Int -> Int
        restartPage block
            | withMulti = block * (restartInterval + 3 * 2)
            | otherwise = block * restartInterval

        everySecond :: [a] -> [a]
        everySecond (x : _ : xs) = x : everySecond xs
        everySecond xs           = xs

        restartCount :: Int
        restartCount = (entryCount + restartInterval - 1) `div`
                       restartInterval

{-|
    Builds a serialised index from entries, a restart table and footer values,
    with the correct padding.
-}
rawSerialised :: [Word8] -> [Word32] -> Word64 -> Word64 -> ShortByteString
rawSerialised entries restarts entryCount pageCount
    = ShortByteString.toShort $
      ByteString.pack $
      word32 testedTypeAndVersion ++ word32 0           ++
      entries                                          ++
      replicate (negate (8 + length entries) `mod` 8) 0 ++
      concatMap word32 restarts                        ++
      word64 (fromIntegral (length entries))           ++
      word64 entryCount                                ++
      word64 pageCount                                 ++
      word64 0
    where

    word32 :: Word32 -> [Word8]
    word32 = VP.toList . byteVectorFromPrim

    word64 :: Word64 -> [Word8]
    word64 = VP.toList . byteVectorFromPrim

unit_nonShortestNumbersRejected :: Assertion
unit_nonShortestNumbersRejected = do
    assertEqual "shortest form"
                (Right (NumEntries 0, constructIndex [single "a" "b"]))
                (fromSBS (rawSerialised [0x00, 0x00] [0, 0] 1 1))
    assertEqual "header not in shortest form"
                (Left "Number is not in shortest form")
                (fromSBS (rawSerialised [0x80, 0x00, 0x00] [0, 0] 1 1))
    assertEqual "suffix length not in shortest form"
                (Left "Number is not in shortest form")
                (fromSBS (rawSerialised [0x00, 0x81, 0x00, 0x61] [0, 0] 1 1))
    assertEqual "overflow page count not in shortest form"
                (Left "Number is not in shortest form")
                (fromSBS (rawSerialised [0x01, 0x00, 0x82, 0x00] [0, 0] 1 3))
