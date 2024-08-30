{-# LANGUAGE MagicHash #-}

{-# OPTIONS_GHC -Wno-orphans #-}

{- HLINT ignore "Avoid restricted alias" -}

module Test.Database.LSMTree.Internal.IndexOrdinary (tests) where

import           Prelude hiding (all, head, last, length, notElem, splitAt,
                     tail, takeWhile)

import           Control.Arrow (first, (>>>))
import           Control.Monad.ST.Strict (runST)
import           Data.ByteString.Short (ShortByteString (SBS))
import qualified Data.ByteString.Short as ShortByteString (length, pack)
import           Data.Either (isLeft)
import           Data.List (genericReplicate)
import qualified Data.List as List (tail)
import           Data.Maybe (catMaybes)
import           Data.Primitive.ByteArray (ByteArray (ByteArray), ByteArray#)
import           Data.Vector (Vector, all, fromList, head, last, length,
                     notElem, splitAt, tail, takeWhile, toList, (!))
import qualified Data.Vector.Primitive as Primitive (Vector (Vector), concat,
                     force, length, singleton)
import           Data.Word (Word16, Word32, Word64, Word8)
import           Database.LSMTree.Extras.Generators (LogicalPageSummaries,
                     toAppends)
import           Database.LSMTree.Internal.Chunk (fromChunk)
import           Database.LSMTree.Internal.Entry (NumEntries (NumEntries))
import           Database.LSMTree.Internal.IndexCompact (PageNo (PageNo),
                     PageSpan (PageSpan))
import           Database.LSMTree.Internal.IndexCompactAcc
                     (Append (AppendMultiPage, AppendSinglePage))
import           Database.LSMTree.Internal.IndexOrdinary
                     (IndexOrdinary (IndexOrdinary), fromSBS, search,
                     toLastKeys)
import           Database.LSMTree.Internal.IndexOrdinaryAcc (append, new,
                     unsafeEnd)
import           Database.LSMTree.Internal.Serialise
                     (SerialisedKey (SerialisedKey'))
import           Database.LSMTree.Internal.Vector (byteVectorFromPrim)
import           Test.Database.LSMTree.Internal.Chunk ()
import           Test.QuickCheck (Arbitrary (arbitrary, shrink), Gen,
                     NonNegative (NonNegative), Property,
                     Small (Small, getSmall), Testable, chooseInt,
                     counterexample, frequency, getPositive, getSorted,
                     shrinkMap, suchThat, vector, (.&&.), (===), (==>))
import           Test.QuickCheck.Instances.ByteString ()
                     -- for @Arbitrary ShortByteString@
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.QuickCheck (testProperty)

-- * Tests

tests :: TestTree
tests = testGroup "Test.Database.LSMTree.Internal.IndexOrdinary" $
        [
            testGroup "Search" $
            [
                testProperty
                    "Search for mentioned key works"
                    prop_searchForMentionedKeyWorks,
                testProperty
                    "Search for unmentioned key in range works"
                    prop_searchForUnmentionedKeyInRangeWorks,
                testProperty
                    "Search for unmentioned key beyond range works"
                    prop_searchForUnmentionedKeyBeyondRangeWorks
            ],
            testGroup "Deserialisation" $
            [
                testProperty
                    "Number of entries from serialised index works"
                    prop_numberOfEntriesFromSerialisedIndexWorks,
                testProperty
                    "Index from serialised index works"
                    prop_indexFromSerialisedIndexWorks,
                testProperty
                    "Too short input makes deserialisation fail"
                    prop_tooShortInputMakesDeserialisationFail,
                testProperty
                    "Type-and-version error makes deserialisation fail"
                    prop_typeAndVersionErrorMakesDeserialisationFail,
                testProperty
                    "Partial key size block makes deserialisation fail"
                    prop_partialKeySizeBlockMakesDeserialisationFail,
                testProperty
                    "Partial key block makes deserialisation fail"
                    prop_partialKeyBlockMakesDeserialisationFail
            ],
            testGroup "Incremental construction" $
            [
                testProperty
                    "Incremental index construction works"
                    prop_incrementalIndexConstructionWorks,
                testProperty
                    "Incremental serialised key list construction works"
                    prop_incrementalSerialisedKeyListConstructionWorks
            ]
        ]

-- * Tested type and version

-- The type and version of the index to which these tests refer.
testedTypeAndVersion :: Word32
testedTypeAndVersion = 0x0101

-- * Utilities

-- ** Simple index construction

{-
    Constructs an index from a list of keys. The keys have the same meaning as
    in the vector given to the 'IndexOrdinary' data constructor.
-}
indexFromLastKeyList :: [SerialisedKey] -> IndexOrdinary
indexFromLastKeyList = IndexOrdinary . fromList

-- ** Partitioning according to search

{-
    Invokes the 'search' function to obtain a page span and partitions the key
    list of the index into a prefix, selection, and suffix part, which
    correspond to the pages before, within, and after the page span,
    respectively.
-}
searchPartitioning
    :: SerialisedKey
    -> IndexOrdinary
    -> (Vector SerialisedKey, Vector SerialisedKey, Vector SerialisedKey)
searchPartitioning key index@(IndexOrdinary lastKeys)
    = (prefix, selection, suffix)
    where

    start, end :: Int
    PageSpan (PageNo start) (PageNo end) = search key index

    prefix, selection, suffix :: Vector SerialisedKey
    ((prefix, selection), suffix) = first (splitAt start) $
                                    splitAt (succ end)    $
                                    lastKeys

-- Adds a search partitioning to a counterexample.
searchPartitioningCounterexample
    :: Testable prop
    => Vector SerialisedKey
    -> Vector SerialisedKey
    -> Vector SerialisedKey
    -> prop
    -> Property
searchPartitioningCounterexample prefix selection suffix
    = counterexample $ "Prefix: "    ++ show prefix    ++ "; " ++
                       "Selection: " ++ show selection ++ "; " ++
                       "Suffix: "    ++ show suffix

-- ** Construction of serialised indexes, possibly with errors

{-
    Constructs a potential serialised index from a block for representing the
    type and the version of the index, a list of blocks for representing the
    keys list of the index, and a block for representing the number of entries
    of the run. Since all blocks can be arbitrary sequences of bytes, it is
    possible to construct an incorrect serialised index.
-}
potentialSerialisedIndex :: Primitive.Vector Word8
                         -> [Primitive.Vector Word8]
                         -> Primitive.Vector Word8
                         -> ShortByteString
potentialSerialisedIndex typeAndVersionBlock
                         potentialLastKeysBlocks
                         potentialEntryCountBlock
    = SBS unliftedByteArray
    where

    primitiveVector :: Primitive.Vector Word8
    primitiveVector = Primitive.concat $ [typeAndVersionBlock]      ++
                                         potentialLastKeysBlocks    ++
                                         [potentialEntryCountBlock]

    unliftedByteArray :: ByteArray#
    !(Primitive.Vector _ _ (ByteArray unliftedByteArray))
        = Primitive.force primitiveVector

{-
    The serialisation of the type and version of the index to which these tests
    refer.
-}
testedTypeAndVersionBlock :: Primitive.Vector Word8
testedTypeAndVersionBlock = byteVectorFromPrim testedTypeAndVersion

{-
    Constructs blocks that constitute the serialisation of the key list of an
    index.
-}
lastKeysBlocks :: [SerialisedKey] -> [Primitive.Vector Word8]
lastKeysBlocks lastKeys = concatMap lastKeyBlocks lastKeys where

    lastKeyBlocks :: SerialisedKey -> [Primitive.Vector Word8]
    lastKeyBlocks (SerialisedKey' lastKeyBlock)
        = [lastKeySizeBlock, lastKeyBlock]
        where

        lastKeySizeBlock :: Primitive.Vector Word8
        lastKeySizeBlock = byteVectorFromPrim lastKeySizeAsWord16 where

            lastKeySizeAsWord16 :: Word16
            lastKeySizeAsWord16 = fromIntegral $
                                  Primitive.length lastKeyBlock

-- Constructs the serialisation of the number of entries of a run.
entryCountBlock :: NumEntries -> Primitive.Vector Word8
entryCountBlock (NumEntries entryCount)
    = byteVectorFromPrim entryCountAsWord64
    where

    entryCountAsWord64 :: Word64
    entryCountAsWord64 = fromIntegral entryCount

-- Constructs a correct serialised index.
serialisedIndex :: NumEntries -> [SerialisedKey] -> ShortByteString
serialisedIndex entryCount lastKeys
    = potentialSerialisedIndex testedTypeAndVersionBlock
                               (lastKeysBlocks lastKeys)
                               (entryCountBlock entryCount)

-- ** Construction via appending

-- Yields the number of keys that an append operation adds to an index.
appendedKeysCount :: Append -> Int
appendedKeysCount (AppendSinglePage _ _)
    = 1
appendedKeysCount (AppendMultiPage _ overflowPageCount)
    = succ (fromIntegral overflowPageCount)

-- Yields the keys that an append operation adds to an index.
appendedKeys :: Append -> [SerialisedKey]
appendedKeys (AppendSinglePage _ lastKey)
    = [lastKey]
appendedKeys (AppendMultiPage key overflowPageCount)
    = key : genericReplicate overflowPageCount key

{-
    Yields the index that results from performing a sequence of append
    operations, starting with no keys.
-}
indexFromAppends :: [Append] -> IndexOrdinary
indexFromAppends appends = indexFromLastKeyList (concatMap appendedKeys appends)

{-
    Yields the serialized key list that results from performing a sequence of
    append operations, starting with no keys.
-}
lastKeysBlockFromAppends :: [Append] -> Primitive.Vector Word8
lastKeysBlockFromAppends appends = lastKeysBlock where

    lastKeys :: [SerialisedKey]
    lastKeys = concatMap appendedKeys appends

    lastKeysBlock :: Primitive.Vector Word8
    lastKeysBlock = Primitive.concat (lastKeysBlocks lastKeys)

{-
    Incrementally constructs an index, using the functions 'new', 'append', and
    'unsafeEnd'.
-}
incrementalConstruction :: [Append] -> (IndexOrdinary, Primitive.Vector Word8)
incrementalConstruction appends = runST $ do
    acc <- new keyCount minChunkSize
    commonChunks <- mapM (flip append acc) appends
    (remnant, unserialised) <- unsafeEnd acc
    let

        serialised :: Primitive.Vector Word8
        serialised = Primitive.concat $
                     fromChunk <$> catMaybes (commonChunks ++ [remnant])

    return (unserialised, serialised)
    where

    keyCount :: Int
    keyCount = sum (map appendedKeysCount appends)

    {-
        We do not need to vary the minimum chunk size, since we are not testing
        chunk building here.
    -}
    minChunkSize :: Int
    minChunkSize = 0x1000

-- * Properties to test

-- ** Search

prop_searchForMentionedKeyWorks :: NonNegative (Small Int)
                                -> SearchableIndex
                                -> Property
prop_searchForMentionedKeyWorks (NonNegative (Small pageNo))
                                (SearchableIndex index)
    = searchPartitioningCounterexample prefix selection suffix $
      pageNo < length lastKeys ==> all (<  key) prefix    .&&.
                                   all (== key) selection .&&.
                                   all (>  key) suffix
    where

    lastKeys :: Vector SerialisedKey
    lastKeys = toLastKeys index

    key :: SerialisedKey
    key = lastKeys ! pageNo

    prefix, selection, suffix :: Vector SerialisedKey
    (prefix, selection, suffix) = searchPartitioning key index

prop_searchForUnmentionedKeyInRangeWorks :: SerialisedKey
                                         -> SearchableIndex
                                         -> Property
prop_searchForUnmentionedKeyInRangeWorks key (SearchableIndex index)
    = searchPartitioningCounterexample prefix selection suffix $
      key `notElem` lastKeys && key <= last lastKeys ==>
      key < selectionHead                     .&&.
      all (<  key)           prefix           .&&.
      all (== selectionHead) (tail selection) .&&.
      all (>  selectionHead) suffix
    where

    lastKeys :: Vector SerialisedKey
    lastKeys = toLastKeys index

    prefix, selection, suffix :: Vector SerialisedKey
    (prefix, selection, suffix) = searchPartitioning key index

    selectionHead :: SerialisedKey
    selectionHead = head selection

prop_searchForUnmentionedKeyBeyondRangeWorks :: SerialisedKey
                                             -> SearchableIndex
                                             -> Property
prop_searchForUnmentionedKeyBeyondRangeWorks key (SearchableIndex index)
    = searchPartitioningCounterexample prefix selection suffix $
      not (null lesserLastKeys) ==> all (<  selectionHead) prefix           .&&.
                                    all (== selectionHead) (tail selection) .&&.
                                    all (>  selectionHead) suffix
    where

    lastKeys :: Vector SerialisedKey
    lastKeys = toLastKeys index

    lesserLastKeys :: Vector SerialisedKey
    lesserLastKeys = takeWhile (< key) lastKeys

    prefix, selection, suffix :: Vector SerialisedKey
    (prefix, selection, suffix) = searchPartitioning key $
                                  IndexOrdinary lesserLastKeys

    selectionHead :: SerialisedKey
    selectionHead = head selection

-- ** Deserialisation

prop_numberOfEntriesFromSerialisedIndexWorks :: NumEntries
                                             -> [SerialisedKey]
                                             -> Property
prop_numberOfEntriesFromSerialisedIndexWorks entryCount lastKeys
    = errorMsgOrEntryCount === noErrorMsgButCorrectEntryCount
    where

    errorMsgOrEntryCount :: Either String NumEntries
    errorMsgOrEntryCount = fst <$> fromSBS (serialisedIndex entryCount lastKeys)

    noErrorMsgButCorrectEntryCount :: Either String NumEntries
    noErrorMsgButCorrectEntryCount = Right entryCount

prop_indexFromSerialisedIndexWorks :: NumEntries -> [SerialisedKey] -> Property
prop_indexFromSerialisedIndexWorks entryCount lastKeys
    = errorMsgOrIndex === noErrorMsgButCorrectIndex
    where

    errorMsgOrIndex :: Either String IndexOrdinary
    errorMsgOrIndex = snd <$> fromSBS (serialisedIndex entryCount lastKeys)

    noErrorMsgButCorrectIndex :: Either String IndexOrdinary
    noErrorMsgButCorrectIndex = Right (indexFromLastKeyList lastKeys)

prop_tooShortInputMakesDeserialisationFail :: TooShortByteString -> Bool
prop_tooShortInputMakesDeserialisationFail
    = isLeft . fromSBS . fromTooShortByteString

prop_typeAndVersionErrorMakesDeserialisationFail :: Word32
                                                 -> [SerialisedKey]
                                                 -> NumEntries
                                                 -> Property
prop_typeAndVersionErrorMakesDeserialisationFail typeAndVersion
                                                 lastKeys
                                                 entryCount
    = typeAndVersion /= testedTypeAndVersion ==> isLeft errorMsgOrResult
    where

    errorMsgOrResult :: Either String (NumEntries, IndexOrdinary)
    errorMsgOrResult
        = fromSBS $
          potentialSerialisedIndex (byteVectorFromPrim typeAndVersion)
                                   (lastKeysBlocks lastKeys)
                                   (entryCountBlock entryCount)

prop_partialKeySizeBlockMakesDeserialisationFail :: [SerialisedKey]
                                                 -> Word8
                                                 -> NumEntries
                                                 -> Bool
prop_partialKeySizeBlockMakesDeserialisationFail lastKeys
                                                 partialKeySizeByte
                                                 entryCount
    = isLeft $
      fromSBS $
      potentialSerialisedIndex
          testedTypeAndVersionBlock
          (lastKeysBlocks lastKeys ++ [Primitive.singleton partialKeySizeByte])
          (entryCountBlock entryCount)

prop_partialKeyBlockMakesDeserialisationFail :: [SerialisedKey]
                                             -> Small Word16
                                             -> Primitive.Vector Word8
                                             -> NumEntries
                                             -> Property
prop_partialKeyBlockMakesDeserialisationFail lastKeys
                                             (Small statedSize)
                                             partialKeyBlock
                                             entryCount
    = fromIntegral statedSize > Primitive.length partialKeyBlock ==>
      isLeft (fromSBS input)
    where

    statedSizeBlock :: Primitive.Vector Word8
    statedSizeBlock = byteVectorFromPrim statedSize

    input :: ShortByteString
    input = potentialSerialisedIndex
                testedTypeAndVersionBlock
                (lastKeysBlocks lastKeys ++ [statedSizeBlock, partialKeyBlock])
                (entryCountBlock entryCount)

-- ** Incremental construction

prop_incrementalIndexConstructionWorks
    :: LogicalPageSummaries SerialisedKey -> Property
prop_incrementalIndexConstructionWorks logicalPageSummaries
    = fst (incrementalConstruction appends) === indexFromAppends appends
    where

    appends :: [Append]
    appends = toAppends logicalPageSummaries

prop_incrementalSerialisedKeyListConstructionWorks
    :: LogicalPageSummaries SerialisedKey -> Property
prop_incrementalSerialisedKeyListConstructionWorks logicalPageSummaries
    = snd (incrementalConstruction appends) === lastKeysBlockFromAppends appends
    where

    appends :: [Append]
    appends = toAppends logicalPageSummaries

-- * Test case generation and shrinking

{-
    For 'NumEntries' and 'SerialisedKey', we use the 'Arbitrary' instantiations
    from "Database.LSMTree.Extras.Generators". For the correctness of the above
    tests, we need to assume the following properties of said instantiations,
    which are not strictly guaranteed:

      * Generated 'NumEntry' values are smaller than @2 ^ 64@.
      * The lengths of generated 'SerialisedKey' values are smaller than
        @2 ^ 16@.
-}

instance Arbitrary IndexOrdinary where

    arbitrary = IndexOrdinary <$> fromList <$> arbitrary

    shrink = shrinkMap (IndexOrdinary . fromList) (toList . toLastKeys)

newtype SearchableIndex = SearchableIndex IndexOrdinary deriving stock Show

instance Arbitrary SearchableIndex where

    arbitrary = do
        availableLastKeys <- (getSorted <$> arbitrary) `suchThat` (not . null)
        lastKeys <- concat <$>
                    mapM ((<$> genKeyCount) . flip replicate) availableLastKeys
        return $ SearchableIndex (indexFromLastKeyList lastKeys)
        where

        genKeyCount :: Gen Int
        genKeyCount = frequency $
                      [(4, pure 1), (1, getSmall <$> getPositive <$> arbitrary)]

    shrink (SearchableIndex (IndexOrdinary lastKeysVec))
        = [
              SearchableIndex (indexFromLastKeyList lastKeys') |
                  lastKeys' <- shrink (toList lastKeysVec),
                  not (null lastKeys'),
                  and (zipWith (<=) lastKeys' (List.tail lastKeys'))
          ]

-- A byte string that is too short to be a serialised index.
newtype TooShortByteString = TooShortByteString ShortByteString
    deriving stock Show

fromTooShortByteString :: TooShortByteString -> ShortByteString
fromTooShortByteString (TooShortByteString shortByteString) = shortByteString

instance Arbitrary TooShortByteString where

    arbitrary = do
        len <- chooseInt (0, 11)
        TooShortByteString <$> ShortByteString.pack <$> vector len

    shrink = fromTooShortByteString                      >>>
             shrink                                      >>>
             filter (ShortByteString.length >>> (<= 11)) >>>
             map TooShortByteString
