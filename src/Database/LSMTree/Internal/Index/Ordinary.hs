{-# LANGUAGE MagicHash #-}

{- HLINT ignore "Avoid restricted alias" -}

-- | A general-purpose fence pointer index.
module Database.LSMTree.Internal.Index.Ordinary
(
    IndexOrdinary (IndexOrdinary),
    toLastKeys,
    search,
    sizeInPages,
    headerLBS,
    finalLBS,
    fromSBS
)
where

import           Prelude hiding (drop, last, length)

import           Control.DeepSeq (NFData (rnf))
import           Control.Exception (assert)
import           Control.Monad (when)
import           Data.ByteString.Builder (toLazyByteString)
import           Data.ByteString.Builder.Extra (word32Host, word64Host)
import           Data.ByteString.Lazy (LazyByteString)
import           Data.ByteString.Short (ShortByteString (SBS))
import qualified Data.ByteString.Short as ShortByteString (length)
import           Data.Primitive.ByteArray (ByteArray (ByteArray),
                     indexByteArray)
import           Data.Vector (Vector, drop, findIndex, findIndexR, fromList,
                     last, length, (!))
import qualified Data.Vector.Primitive as Primitive (Vector (Vector), drop,
                     force, length, null, splitAt, take)
import           Data.Word (Word16, Word32, Word64, Word8, byteSwap32)
import           Database.LSMTree.Internal.Entry (NumEntries (NumEntries),
                     unNumEntries)
import           Database.LSMTree.Internal.Index
                     (Index (finalLBS, search, sizeInPages))
import           Database.LSMTree.Internal.Page (NumPages (NumPages),
                     PageNo (PageNo), PageSpan (PageSpan))
import           Database.LSMTree.Internal.Serialise
                     (SerialisedKey (SerialisedKey'))
import           Database.LSMTree.Internal.Vector (binarySearchL, mkPrimVector)

{-|
    The type–version indicator for the ordinary index and its serialisation
    format as supported by this module.
-}
supportedTypeAndVersion :: Word32
supportedTypeAndVersion = 0x0101

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
    deriving stock (Eq, Show)

instance NFData IndexOrdinary where

    rnf (IndexOrdinary lastKeys) = rnf lastKeys

toLastKeys :: IndexOrdinary -> Vector SerialisedKey
toLastKeys (IndexOrdinary lastKeys) = lastKeys

instance Index IndexOrdinary where

    search :: SerialisedKey -> IndexOrdinary -> PageSpan
    search key (IndexOrdinary lastKeys) = assert (pageCount > 0) result where

        protoStart :: Int
        !protoStart = binarySearchL lastKeys key

        pageCount :: Int
        !pageCount = length lastKeys

        result :: PageSpan
        !result | protoStart < pageCount
                    = let

                          end :: Int
                          !end = maybe (pred pageCount) (+ protoStart) $
                                 findIndex (/= lastKeys ! protoStart)  $
                                 drop (succ protoStart) lastKeys

                      in PageSpan (PageNo $ protoStart)
                                  (PageNo $ end)
                | otherwise
                    = let

                          start :: Int
                          !start = maybe 0 succ                  $
                                   findIndexR (/= last lastKeys) $
                                   lastKeys

                      in PageSpan (PageNo $ start)
                                  (PageNo $ pred pageCount)

    sizeInPages :: IndexOrdinary -> NumPages
    sizeInPages (IndexOrdinary lastKeys)
        = NumPages $ fromIntegral (length lastKeys)

    finalLBS :: NumEntries -> IndexOrdinary -> LazyByteString
    finalLBS entryCount _ = toLazyByteString $
                            word64Host       $
                            fromIntegral     $
                            unNumEntries     $
                            entryCount

{-|
    Yields the header of the serialised form of an ordinary index.

    See the documentation of the 'Index' class for how to generate a complete
    serialised index.
-}
headerLBS :: LazyByteString
headerLBS = toLazyByteString        $
            word32Host              $
            supportedTypeAndVersion

{-|
    Reads an ordinary index along with the number of entries of the respective
    run from a byte string.

    The byte string must contain the serialised index exactly, with no leading
    or trailing space.

    For deserialising numbers, the endianness of the host system is used. If
    serialisation has been done with a different endianness, this mismatch is
    detected by looking at the type–version indicator.
-}
fromSBS :: ShortByteString -> Either String (NumEntries, IndexOrdinary)
fromSBS shortByteString@(SBS unliftedByteArray)
    | fullSize < 12
        = Left "Doesn't contain header and footer"
    | typeAndVersion == byteSwap32 supportedTypeAndVersion
        = Left "Non-matching endianness"
    | typeAndVersion /= supportedTypeAndVersion
        = Left "Unsupported type or version"
    | otherwise
        = (,) <$> entryCount <*> index
    where

    fullSize :: Int
    fullSize = ShortByteString.length shortByteString

    byteArray :: ByteArray
    byteArray = ByteArray unliftedByteArray

    fullBytes :: Primitive.Vector Word8
    fullBytes = mkPrimVector 0 fullSize byteArray

    typeAndVersion :: Word32
    typeAndVersion = indexByteArray byteArray 0

    postTypeAndVersionBytes :: Primitive.Vector Word8
    postTypeAndVersionBytes = Primitive.drop 4 fullBytes

    lastKeysBytes, entryCountBytes :: Primitive.Vector Word8
    (lastKeysBytes, entryCountBytes)
        = Primitive.splitAt (fullSize - 12) postTypeAndVersionBytes

    entryCount :: Either String NumEntries
    entryCount
        | toInteger asWord64 > toInteger (maxBound :: Int)
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
