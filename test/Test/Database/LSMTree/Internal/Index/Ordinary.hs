{-# LANGUAGE MagicHash #-}

{-# OPTIONS_GHC -Wno-orphans #-}

{- HLINT ignore "Avoid restricted alias" -}

module Test.Database.LSMTree.Internal.Index.Ordinary (tests) where

import           Prelude hiding (all, head, last, length, notElem, splitAt,
                     tail, takeWhile)

import           Control.Arrow (first, (>>>))
import           Control.Monad.ST.Strict (runST)
import qualified Data.ByteString.Lazy as LazyByteString (unpack)
import           Data.ByteString.Short (ShortByteString (SBS))
import qualified Data.ByteString.Short as ShortByteString (length, pack)
import           Data.Either (isLeft)
import           Data.List (genericReplicate)
import qualified Data.List as List (tail)
import           Data.Maybe (maybeToList)
import           Data.Primitive.ByteArray (ByteArray (ByteArray), ByteArray#)
import           Data.Vector (Vector, all, fromList, head, last, length,
                     notElem, splitAt, tail, takeWhile, toList, (!))
import qualified Data.Vector.Primitive as Primitive (Vector (Vector), concat,
                     force, length, singleton, toList)
import           Data.Word (Word16, Word32, Word64, Word8)
import           Database.LSMTree.Extras.Generators (LogicalPageSummaries,
                     toAppends)
import           Database.LSMTree.Extras.Index
                     (Append (AppendMultiPage, AppendSinglePage),
                     appendToOrdinary)
import qualified Database.LSMTree.Internal.Chunk as Chunk (toByteVector)
import           Database.LSMTree.Internal.Entry (NumEntries (NumEntries))
import           Database.LSMTree.Internal.Index.Ordinary
                     (IndexOrdinary (IndexOrdinary), finalLBS, fromSBS,
                     headerLBS, search, toUnslicedLastKeys)
import           Database.LSMTree.Internal.Index.OrdinaryAcc (new, unsafeEnd)
import           Database.LSMTree.Internal.Page (PageNo (PageNo),
                     PageSpan (PageSpan))
import           Database.LSMTree.Internal.Serialise
                     (SerialisedKey (SerialisedKey'))
import           Database.LSMTree.Internal.Unsliced (Unsliced, fromUnslicedKey,
                     makeUnslicedKey)
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
tests = testGroup "Test.Database.LSMTree.Internal.Index.Ordinary" $
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
            testGroup "Header and footer construction" $
            [
                testProperty
                    "Header construction works"
                    prop_headerConstructionWorks,
                testProperty
                    "Footer construction works"
                    prop_footerConstructionWorks
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

-- | The type and version of the index to which these tests refer.
testedTypeAndVersion :: Word32
testedTypeAndVersion = 0x0101

-- * Utilities

-- ** Simple index construction

{-|
    Constructs an index from a list of unsliced keys. The keys have the same
    meaning as in the vector given to the 'IndexOrdinary' data constructor.
-}
indexFromUnslicedLastKeyList :: [Unsliced SerialisedKey] -> IndexOrdinary
indexFromUnslicedLastKeyList = IndexOrdinary . fromList

-- ** Partitioning according to search

{-|
    Invokes the 'search' function to obtain a page span and partitions the key
    list of the index into a prefix, selection, and suffix part, which
    correspond to the pages before, within, and after the page span,
    respectively.
-}
searchPartitioning ::
       Unsliced SerialisedKey
    -> IndexOrdinary
    -> (,,) (Vector (Unsliced SerialisedKey))
            (Vector (Unsliced SerialisedKey))
            (Vector (Unsliced SerialisedKey))
searchPartitioning unslicedKey index@(IndexOrdinary unslicedLastKeys)
    = (prefix, selection, suffix)
    where

    start, end :: Int
    PageSpan (PageNo start) (PageNo end) = search (fromUnslicedKey unslicedKey)
                                                  index

    prefix, selection, suffix :: Vector (Unsliced SerialisedKey)
    ((prefix, selection), suffix) = first (splitAt start) $
                                    splitAt (succ end)    $
                                    unslicedLastKeys

-- | Adds a search partitioning to a counterexample.
searchPartitioningCounterexample ::
       Testable prop
    => Vector (Unsliced SerialisedKey)
    -> Vector (Unsliced SerialisedKey)
    -> Vector (Unsliced SerialisedKey)
    -> prop
    -> Property
searchPartitioningCounterexample prefix selection suffix
    = counterexample $ "Prefix: "    ++ show prefix    ++ "; " ++
                       "Selection: " ++ show selection ++ "; " ++
                       "Suffix: "    ++ show suffix

-- ** Construction of serialised indexes, possibly with errors

{-|
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

{-|
    The serialisation of the type and version of the index to which these tests
    refer.
-}
testedTypeAndVersionBlock :: Primitive.Vector Word8
testedTypeAndVersionBlock = byteVectorFromPrim testedTypeAndVersion

{-|
    Constructs blocks that constitute the serialisation of the key list of an
    index.
-}
lastKeysBlocks :: [Unsliced SerialisedKey] -> [Primitive.Vector Word8]
lastKeysBlocks unslicedLastKeys = concatMap lastKeyBlocks unslicedLastKeys where

    lastKeyBlocks :: Unsliced SerialisedKey -> [Primitive.Vector Word8]
    lastKeyBlocks unslicedLastKey = [lastKeySizeBlock, lastKeyBlock] where

        SerialisedKey' lastKeyBlock = fromUnslicedKey unslicedLastKey

        lastKeySizeBlock :: Primitive.Vector Word8
        lastKeySizeBlock = byteVectorFromPrim lastKeySizeAsWord16 where

            lastKeySizeAsWord16 :: Word16
            lastKeySizeAsWord16 = fromIntegral $
                                  Primitive.length lastKeyBlock

-- | Constructs the serialisation of the number of entries of a run.
entryCountBlock :: NumEntries -> Primitive.Vector Word8
entryCountBlock (NumEntries entryCount)
    = byteVectorFromPrim entryCountAsWord64
    where

    entryCountAsWord64 :: Word64
    entryCountAsWord64 = fromIntegral entryCount

-- | Constructs a correct serialised index.
serialisedIndex :: NumEntries -> [Unsliced SerialisedKey] -> ShortByteString
serialisedIndex entryCount unslicedLastKeys
    = potentialSerialisedIndex testedTypeAndVersionBlock
                               (lastKeysBlocks unslicedLastKeys)
                               (entryCountBlock entryCount)

-- ** Construction via appending

-- | Yields the keys that an append operation adds to an index.
appendedKeys :: Append -> [Unsliced SerialisedKey]
appendedKeys (AppendSinglePage _ lastKey)
    = [makeUnslicedKey lastKey]
appendedKeys (AppendMultiPage key overflowPageCount)
    = unslicedKey : genericReplicate overflowPageCount unslicedKey
    where

    unslicedKey :: Unsliced SerialisedKey
    unslicedKey = makeUnslicedKey key

{-|
    Yields the index that results from performing a sequence of append
    operations, starting with no keys.
-}
indexFromAppends :: [Append] -> IndexOrdinary
indexFromAppends appends = indexFromUnslicedLastKeyList $
                           concatMap appendedKeys appends

{-|
    Yields the serialized key list that results from performing a sequence of
    append operations, starting with no keys.
-}
lastKeysBlockFromAppends :: [Append] -> Primitive.Vector Word8
lastKeysBlockFromAppends appends = lastKeysBlock where

    unslicedLastKeys :: [Unsliced SerialisedKey]
    unslicedLastKeys = concatMap appendedKeys appends

    lastKeysBlock :: Primitive.Vector Word8
    lastKeysBlock = Primitive.concat (lastKeysBlocks unslicedLastKeys)

{-|
    Incrementally constructs an index, using the functions 'new', 'append', and
    'unsafeEnd'.
-}
incrementalConstruction :: [Append] -> (IndexOrdinary, Primitive.Vector Word8)
incrementalConstruction appends = runST $ do
    acc <- new initialKeyBufferSize minChunkSize
    commonChunks <- concat <$> mapM (flip appendToOrdinary acc) appends
    (remnant, unserialised) <- unsafeEnd acc
    let

        serialised :: Primitive.Vector Word8
        serialised = Primitive.concat                    $
                     map Chunk.toByteVector              $
                     commonChunks ++ maybeToList remnant

    return (unserialised, serialised)
    where

    {-
        We do not need to vary the initial key buffer size, since we are not
        testing growing vectors here.
    -}
    initialKeyBufferSize :: Int
    initialKeyBufferSize = 0x100

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
      pageNo < length unslicedLastKeys ==> all (<  unslicedKey) prefix    .&&.
                                           all (== unslicedKey) selection .&&.
                                           all (>  unslicedKey) suffix
    where

    unslicedLastKeys :: Vector (Unsliced SerialisedKey)
    unslicedLastKeys = toUnslicedLastKeys index

    unslicedKey :: Unsliced SerialisedKey
    unslicedKey = unslicedLastKeys ! pageNo

    prefix, selection, suffix :: Vector (Unsliced SerialisedKey)
    (prefix, selection, suffix) = searchPartitioning unslicedKey index

prop_searchForUnmentionedKeyInRangeWorks :: Unsliced SerialisedKey
                                         -> SearchableIndex
                                         -> Property
prop_searchForUnmentionedKeyInRangeWorks unslicedKey (SearchableIndex index)
    = searchPartitioningCounterexample prefix selection suffix $
      unslicedKey `notElem` unslicedLastKeys &&
      unslicedKey <= last unslicedLastKeys   ==>
      unslicedKey < selectionHead             .&&.
      all (<  unslicedKey)   prefix           .&&.
      all (== selectionHead) (tail selection) .&&.
      all (>  selectionHead) suffix
    where

    unslicedLastKeys :: Vector (Unsliced SerialisedKey)
    unslicedLastKeys = toUnslicedLastKeys index

    prefix, selection, suffix :: Vector (Unsliced SerialisedKey)
    (prefix, selection, suffix) = searchPartitioning unslicedKey index

    selectionHead :: Unsliced SerialisedKey
    selectionHead = head selection

prop_searchForUnmentionedKeyBeyondRangeWorks :: Unsliced SerialisedKey
                                             -> SearchableIndex
                                             -> Property
prop_searchForUnmentionedKeyBeyondRangeWorks unslicedKey (SearchableIndex index)
    = searchPartitioningCounterexample prefix selection suffix $
      not (null lesserUnslicedLastKeys) ==>
      all (<  selectionHead) prefix           .&&.
      all (== selectionHead) (tail selection) .&&.
      all (>  selectionHead) suffix
    where

    unslicedLastKeys :: Vector (Unsliced SerialisedKey)
    unslicedLastKeys = toUnslicedLastKeys index

    lesserUnslicedLastKeys :: Vector (Unsliced SerialisedKey)
    lesserUnslicedLastKeys = takeWhile (< unslicedKey) unslicedLastKeys

    prefix, selection, suffix :: Vector (Unsliced SerialisedKey)
    (prefix, selection, suffix) = searchPartitioning unslicedKey $
                                  IndexOrdinary lesserUnslicedLastKeys

    selectionHead :: Unsliced SerialisedKey
    selectionHead = head selection

-- ** Header and footer construction

prop_headerConstructionWorks :: Property
prop_headerConstructionWorks
    = LazyByteString.unpack headerLBS
      ===
      Primitive.toList testedTypeAndVersionBlock

prop_footerConstructionWorks :: NumEntries -> IndexOrdinary -> Property
prop_footerConstructionWorks entryCount index
    = LazyByteString.unpack (finalLBS entryCount index)
      ===
      Primitive.toList (entryCountBlock entryCount)

-- ** Deserialisation

prop_numberOfEntriesFromSerialisedIndexWorks :: NumEntries
                                             -> [Unsliced SerialisedKey]
                                             -> Property
prop_numberOfEntriesFromSerialisedIndexWorks entryCount unslicedLastKeys
    = errorMsgOrEntryCount === noErrorMsgButCorrectEntryCount
    where

    errorMsgOrEntryCount :: Either String NumEntries
    errorMsgOrEntryCount = fst <$>
                           fromSBS (serialisedIndex entryCount unslicedLastKeys)

    noErrorMsgButCorrectEntryCount :: Either String NumEntries
    noErrorMsgButCorrectEntryCount = Right entryCount

prop_indexFromSerialisedIndexWorks :: NumEntries
                                   -> [Unsliced SerialisedKey]
                                   -> Property
prop_indexFromSerialisedIndexWorks entryCount unslicedLastKeys
    = errorMsgOrIndex === noErrorMsgButCorrectIndex
    where

    errorMsgOrIndex :: Either String IndexOrdinary
    errorMsgOrIndex = snd <$>
                      fromSBS (serialisedIndex entryCount unslicedLastKeys)

    noErrorMsgButCorrectIndex :: Either String IndexOrdinary
    noErrorMsgButCorrectIndex = Right $
                                indexFromUnslicedLastKeyList unslicedLastKeys

prop_tooShortInputMakesDeserialisationFail :: TooShortByteString -> Bool
prop_tooShortInputMakesDeserialisationFail
    = isLeft . fromSBS . fromTooShortByteString

prop_typeAndVersionErrorMakesDeserialisationFail :: Word32
                                                 -> [Unsliced SerialisedKey]
                                                 -> NumEntries
                                                 -> Property
prop_typeAndVersionErrorMakesDeserialisationFail typeAndVersion
                                                 unslicedLastKeys
                                                 entryCount
    = typeAndVersion /= testedTypeAndVersion ==> isLeft errorMsgOrResult
    where

    errorMsgOrResult :: Either String (NumEntries, IndexOrdinary)
    errorMsgOrResult
        = fromSBS $
          potentialSerialisedIndex (byteVectorFromPrim typeAndVersion)
                                   (lastKeysBlocks unslicedLastKeys)
                                   (entryCountBlock entryCount)

prop_partialKeySizeBlockMakesDeserialisationFail :: [Unsliced SerialisedKey]
                                                 -> Word8
                                                 -> NumEntries
                                                 -> Bool
prop_partialKeySizeBlockMakesDeserialisationFail unslicedLastKeys
                                                 partialKeySizeByte
                                                 entryCount
    = isLeft $
      fromSBS $
      potentialSerialisedIndex
          testedTypeAndVersionBlock
          (correctBlocks ++ [Primitive.singleton partialKeySizeByte])
          (entryCountBlock entryCount)
    where

    correctBlocks :: [Primitive.Vector Word8]
    correctBlocks = lastKeysBlocks unslicedLastKeys

prop_partialKeyBlockMakesDeserialisationFail :: [Unsliced SerialisedKey]
                                             -> Small Word16
                                             -> Primitive.Vector Word8
                                             -> NumEntries
                                             -> Property
prop_partialKeyBlockMakesDeserialisationFail unslicedLastKeys
                                             (Small statedSize)
                                             partialKeyBlock
                                             entryCount
    = fromIntegral statedSize > Primitive.length partialKeyBlock ==>
      isLeft (fromSBS input)
    where

    correctBlocks :: [Primitive.Vector Word8]
    correctBlocks = lastKeysBlocks unslicedLastKeys

    statedSizeBlock :: Primitive.Vector Word8
    statedSizeBlock = byteVectorFromPrim statedSize

    input :: ShortByteString
    input = potentialSerialisedIndex
                testedTypeAndVersionBlock
                (correctBlocks ++ [statedSizeBlock, partialKeyBlock])
                (entryCountBlock entryCount)

-- ** Incremental construction

prop_incrementalIndexConstructionWorks ::
     LogicalPageSummaries SerialisedKey -> Property
prop_incrementalIndexConstructionWorks logicalPageSummaries
    = fst (incrementalConstruction appends) === indexFromAppends appends
    where

    appends :: [Append]
    appends = toAppends logicalPageSummaries

prop_incrementalSerialisedKeyListConstructionWorks ::
     LogicalPageSummaries SerialisedKey -> Property
prop_incrementalSerialisedKeyListConstructionWorks logicalPageSummaries
    = snd (incrementalConstruction appends) === lastKeysBlockFromAppends appends
    where

    appends :: [Append]
    appends = toAppends logicalPageSummaries

-- * Test case generation and shrinking

{-
    For 'NumEntries' and 'Unsliced SerialisedKey', we use the 'Arbitrary'
    instantiations from "Database.LSMTree.Extras.Generators". For the
    correctness of the above tests, we need to assume the following properties
    of said instantiations, which are not strictly guaranteed:

      * Generated 'NumEntry' values are smaller than @2 ^ 64@.
      * The lengths of generated 'Unsliced SerialisedKey' values are smaller
        than @2 ^ 16@.
-}

instance Arbitrary IndexOrdinary where

    arbitrary = IndexOrdinary <$> fromList <$> arbitrary

    shrink = shrinkMap (IndexOrdinary . fromList) (toList . toUnslicedLastKeys)

newtype SearchableIndex = SearchableIndex IndexOrdinary deriving stock Show

instance Arbitrary SearchableIndex where

    arbitrary = do
        availableUnslicedLastKeys
            <- (getSorted <$> arbitrary) `suchThat` (not . null)
        unslicedLastKeys
            <- concat <$>
               mapM ((<$> genKeyCount) . flip replicate)
                    availableUnslicedLastKeys
        return $ SearchableIndex (indexFromUnslicedLastKeyList unslicedLastKeys)
        where

        genKeyCount :: Gen Int
        genKeyCount = frequency $
                      [(4, pure 1), (1, getSmall <$> getPositive <$> arbitrary)]

    shrink (SearchableIndex (IndexOrdinary unslicedLastKeysVec))
        = [
              SearchableIndex (indexFromUnslicedLastKeyList unslicedLastKeys') |
                  unslicedLastKeys' <- shrink (toList unslicedLastKeysVec),
                  not (null unslicedLastKeys'),
                  and $
                  zipWith (<=) unslicedLastKeys' (List.tail unslicedLastKeys')
          ]

-- | A byte string that is too short to be a serialised index.
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
