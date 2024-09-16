{- HLINT ignore "Avoid restricted alias" -}

-- | A general-purpose fence pointer index.
module Database.LSMTree.Internal.IndexOrdinary
(
    IndexOrdinary (IndexOrdinary),

    -- * Search
    search,

    -- * Deserialisation
    fromSBS
)
where

import           Prelude hiding (drop, length, takeWhile)

import           Control.Exception (assert)
import           Control.Monad (when)
import           Data.ByteString.Short (ShortByteString (SBS))
import qualified Data.ByteString.Short as ShortByteString (length)
import           Data.Primitive.ByteArray (ByteArray (ByteArray),
                     indexByteArray)
import           Data.Vector (Vector, drop, findIndexR, fromList, length,
                     takeWhile, (!))
import qualified Data.Vector.Primitive as Primitive (Vector (Vector), drop,
                     force, length, null, splitAt, take)
import           Data.Word (Word16, Word32, Word64, Word8, byteSwap32)
import           Database.LSMTree.Internal.Entry (NumEntries (NumEntries))
import           Database.LSMTree.Internal.IndexCompact (PageNo (PageNo),
                     PageSpan (PageSpan))
import           Database.LSMTree.Internal.Serialise
                     (SerialisedKey (SerialisedKey'))
import           Database.LSMTree.Internal.Vector (binarySearchL, mkPrimVector)

{-|
    The version of the index and its serialisation format that is supported by
    this module.
-}
supportedVersion :: Word32
supportedVersion = 1

{-|
    A general-purpose fence pointer index.

    An index is represented by a vector that maps the number of each page to the
    key stored last in this page or, if the page is an overflow page, to the key
    of the corresponding key–value pair. The vector must have the following
    properties:

      * It is non-empty.

      * Its elements are non-decreasing.

    This restriction follows from the fact that a run must contain keys in
    ascending order and must comprise at least one page for 'search' to be able
    to return a valid page span.
-}
newtype IndexOrdinary = IndexOrdinary (Vector SerialisedKey)

-- * Search

{-|
    Searches for a page span that contains a key–value pair with the given key.
    If there is indeed such a pair, the result is the corresponding page span;
    if there is no such pair, the result is an arbitrary but valid page span.
-}
search :: SerialisedKey -> IndexOrdinary -> PageSpan
search key (IndexOrdinary lastKeys)
    = assert (length lastKeys > 0) $ PageSpan (PageNo start) (PageNo end)
    where

    start :: Int
    start | protoStart < pageCount = protoStart
          | otherwise              = maybe 0 succ                              $
                                     findIndexR (/= lastKeys ! pred pageCount) $
                                     lastKeys

    end :: Int
    end | protoStart < pageCount = start + overflowPageCount
        | otherwise              = pred pageCount
        where

        overflowPageCount :: Int
        overflowPageCount = length                          $
                            takeWhile (== lastKeys ! start) $
                            drop (succ start) lastKeys

    protoStart :: Int
    protoStart = binarySearchL lastKeys key

    pageCount :: Int
    pageCount = length lastKeys

-- * Deserialisation

{-|
    Reads an index along with the number of entries of the respective run from a
    byte string.

    The byte string must contain the serialised index exactly, with no leading
    or trailing space.
-}
fromSBS :: ShortByteString -> Either String (NumEntries, IndexOrdinary)
fromSBS shortByteString@(SBS unliftedByteArray)
    | fullSize < 12
        = Left "Doesn't contain header and footer"
    | version == byteSwap32 supportedVersion
        = Left "Non-matching endianness"
    | version /= supportedVersion
        = Left "Unsupported version"
    | otherwise
        = (,) <$> entryCount <*> index
    where

    fullSize :: Int
    fullSize = ShortByteString.length shortByteString

    byteArray :: ByteArray
    byteArray = ByteArray unliftedByteArray

    fullBytes :: Primitive.Vector Word8
    fullBytes = mkPrimVector 0 fullSize byteArray

    version :: Word32
    version = indexByteArray byteArray 0

    postVersionBytes :: Primitive.Vector Word8
    postVersionBytes = Primitive.drop 4 fullBytes

    lastKeysBytes, entryCountBytes :: Primitive.Vector Word8
    (lastKeysBytes, entryCountBytes)
        = Primitive.splitAt (fullSize - 12) postVersionBytes

    entryCount :: Either String NumEntries
    entryCount
        | (fromIntegral asWord64 :: Integer) > fromIntegral (maxBound :: Int)
            = Left "Number of entries not representable as Int"
        | otherwise
            = Right (NumEntries (fromIntegral asWord64))
        where

        asWord64 :: Word64
        asWord64 = indexByteArray entryCountRep 0

        entryCountRep :: ByteArray
        Primitive.Vector _ _ entryCountRep = Primitive.force entryCountBytes

    index :: Either String IndexOrdinary
    index = IndexOrdinary <$> fromList <$> lastKeys lastKeysBytes

    lastKeys :: Primitive.Vector Word8 -> Either String [SerialisedKey]
    lastKeys bytes
        | Primitive.null bytes
            = Right []
        | otherwise
            = do
                  when (Primitive.length bytes < 2)
                       (Left "Too few bytes for key size")
                  let

                      firstSizeRep :: ByteArray
                      Primitive.Vector _ _ firstSizeRep
                          = Primitive.force (Primitive.take 2 bytes)

                      firstSize :: Int
                      firstSize = fromIntegral $
                                  (indexByteArray firstSizeRep 0 :: Word16)

                      postFirstSizeBytes :: Primitive.Vector Word8
                      postFirstSizeBytes = Primitive.drop 2 bytes

                  when (Primitive.length postFirstSizeBytes < firstSize)
                       (Left "Too few bytes for key")
                  let

                      firstBytes, othersBytes :: Primitive.Vector Word8
                      (firstBytes, othersBytes)
                          = Primitive.splitAt firstSize postFirstSizeBytes

                      first :: SerialisedKey
                      first = SerialisedKey' (Primitive.force firstBytes)

                  others <- lastKeys othersBytes
                  return (first : others)
