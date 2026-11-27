{-# OPTIONS_HADDOCK not-home #-}

{-|
    A fence pointer index that benefits from keys sharing prefixes.

    For each page, or each group of pages that holds a single key–value pair
    whose value overflows, the index stores a /separator/. The separator of the
    first group is the empty string. The separator of every other group is the
    shortest prefix of the group's first key that is greater than the last key
    of the preceding group. Searching for a key yields the group with the
    largest separator that is less than or equal to the key.

    Separators are /front-coded/: an entry only stores the number of bytes that
    its separator shares with the separator of the preceding entry, plus the
    remaining bytes. Every 'restartInterval' entries, there is a /restart
    entry/, which stores its separator in full. A restart table records where
    each restart entry starts and which page it refers to, so that a search can
    do a binary search over the restart entries and then a short linear scan.

    Typically, a separator only needs a few bytes more than the prefix that all
    keys of a run share, and most of that prefix is not repeated for each
    page, which keeps the index small for keys with a lot of overlap, such as
    keys of the form @account\/0000123\/balance@. The index places no
    constraints on keys.

    See @doc/format-run.md@ for the serialised format.
-}
module Database.LSMTree.Internal.Index.Prefix
(
    IndexPrefix (IndexPrefix),
    search,
    sizeInPages,
    headerLBS,
    finalLBS,
    fromSBS,

    -- * Building blocks shared with the accumulator
    restartInterval,
    commonPrefixLength,
    separator,
    varintBytes
)
where

import           Control.DeepSeq (NFData (rnf))
import           Control.Exception (assert)
import           Control.Monad (unless, when)
import           Data.Bits (shiftL, unsafeShiftR, (.&.), (.|.))
import           Data.ByteString.Builder (toLazyByteString, word8)
import           Data.ByteString.Builder.Extra (word32Host, word64Host)
import           Data.ByteString.Lazy (LazyByteString)
import           Data.ByteString.Short (ShortByteString (SBS))
import           Data.Primitive.ByteArray (ByteArray (ByteArray),
                     compareByteArrays, indexByteArray, sizeofByteArray)
import qualified Data.Vector.Primitive as VP
import           Data.Word (Word32, Word64, Word8, byteSwap32)
import           Database.LSMTree.Internal.Entry (NumEntries (NumEntries))
import           Database.LSMTree.Internal.Page (NumPages (NumPages),
                     PageNo (PageNo), PageSpan (PageSpan))
import           Database.LSMTree.Internal.Serialise
                     (SerialisedKey (SerialisedKey'))
import           Database.LSMTree.Internal.Vector (mkPrimVector)

{-|
    The type–version indicator for the prefix index and its serialisation
    format as supported by this module.
-}
supportedTypeAndVersion :: Word32
supportedTypeAndVersion = 0x0201

{-|
    A fence pointer index that benefits from keys sharing prefixes.

    A value @IndexPrefix entries restarts entryCount pageCount@ consists of the
    following components:

      * @entries@ is the sequence of front-coded entries, one per group of
        pages, in the format described in @doc/format-run.md@.

      * @restarts@ is the restart table. For each restart entry, it contains
        the offset of the entry in @entries@, followed by the number of the
        first page of the entry's group.

      * @entryCount@ is the number of entries, which is the number of groups.

      * @pageCount@ is the number of pages.

    The following properties must hold, and 'fromSBS' checks them:

      * The separator of entry 0, if any, is empty.

      * The separators are strictly increasing.

      * Entry @i@ is a restart entry exactly if @i@ is a multiple of
        'restartInterval'. A restart entry shares no bytes with the preceding
        separator. Any other entry shares exactly the longest common prefix of
        its separator and the preceding separator.

      * The restart table has an offset–page pair for each restart entry, and
        the page counts of all entries add up to @pageCount@.

    The vectors may be slices of the serialised index (see 'fromSBS').
-}
data IndexPrefix = IndexPrefix
                       !(VP.Vector Word8)  -- Entries
                       !(VP.Vector Word32) -- Restart table
                       !Int                -- Number of entries
                       !Int                -- Number of pages
    deriving stock (Eq, Show)

instance NFData IndexPrefix where

    rnf (IndexPrefix entries restarts entryCount pageCount)
        = rnf entries    `seq`
          rnf restarts   `seq`
          rnf entryCount `seq`
          rnf pageCount

-- * Building blocks

-- | The number of entries per restart block.
restartInterval :: Int
restartInterval = 16

-- | Yields the length of the longest common prefix of two byte vectors.
commonPrefixLength :: VP.Vector Word8 -> VP.Vector Word8 -> Int
commonPrefixLength xs ys = go 0 where

    limit :: Int
    !limit = min (VP.length xs) (VP.length ys)

    go :: Int -> Int
    go !i | i < limit && VP.unsafeIndex xs i == VP.unsafeIndex ys i = go (i + 1)
          | otherwise                                             = i
{-# INLINE commonPrefixLength #-}

{-|
    Yields the shortest prefix of the second key that is greater than the first
    key.

    The first key must be less than the second one. Then their longest common
    prefix is shorter than the second key, and the prefix that is one byte
    longer than it is greater than the first key.
-}
separator :: VP.Vector Word8 -> VP.Vector Word8 -> VP.Vector Word8
separator lesser greater = assert (lesser < greater) $
                           VP.take (succ (commonPrefixLength lesser greater))
                                   greater

{-|
    The largest number that the serialised format can store in a variable-length
    number, which has at most 5 bytes.

    This needs 'Int' to have more than 35 bits, which holds because @lsm-tree@
    only supports 64-bit systems.
-}
maxVarintValue :: Int
maxVarintValue = 2 ^ (35 :: Int) - 1

{-|
    Yields the variable-length encoding of a number: 7 bits per byte, least
    significant group first, with the most significant bit of each byte set
    exactly if more bytes follow.

    The number must be between 0 and @maxVarintValue@. Values that are
    serialised this way are lengths of keys and numbers of overflow pages,
    which are far below this bound in practice; a violation is a bug, so we
    raise an error.
-}
varintBytes :: Int -> VP.Vector Word8
varintBytes value
    | value < 0 || value > maxVarintValue
        = error "Database.LSMTree.Internal.Index.Prefix.varintBytes: \
                \value out of range"
    | otherwise
        = VP.fromList (go value)
    where

    go :: Int -> [Word8]
    go rest | rest < 0x80 = [fromIntegral rest]
            | otherwise   = (fromIntegral (rest .&. 0x7F) .|. 0x80) :
                            go (rest `unsafeShiftR` 7)

-- * Decoding of entries

{-|
    A decoded entry: the number of shared bytes, the offset and length of the
    suffix, the number of pages of the group, and the offset of the next entry.
-}
data DecodedEntry = DecodedEntry !Int !Int !Int !Int !Int

{-|
    Decodes a variable-length number starting at a given offset, without any
    checks. Yields the number and the offset after it.
-}
unsafeVarintAt :: VP.Vector Word8 -> Int -> (Int, Int)
unsafeVarintAt bytes offset0
    | byte0 < 0x80 = (fromIntegral byte0, succ offset0)
    | otherwise    = go 0 0 offset0
    where

    -- Most numbers fit into a single byte, so we handle this case separately.
    byte0 :: Word8
    !byte0 = VP.unsafeIndex bytes offset0

    go :: Int -> Int -> Int -> (Int, Int)
    go !acc !shift !offset
        | byte < 0x80 = (acc', succ offset)
        | otherwise   = go acc' (shift + 7) (succ offset)
        where

        byte :: Word8
        !byte = VP.unsafeIndex bytes offset

        acc' :: Int
        !acc' = acc .|. (fromIntegral (byte .&. 0x7F) `shiftL` shift)
{-# INLINE unsafeVarintAt #-}

-- | Decodes the entry starting at a given offset, without any checks.
unsafeEntryAt :: VP.Vector Word8 -> Int -> DecodedEntry
unsafeEntryAt entries offset = DecodedEntry shared
                                            suffixOffset
                                            suffixLength
                                            pageCount
                                            (suffixOffset + suffixLength)
    where

    header, suffixLength, suffixOffset, pageCount, offset1, offset2 :: Int
    (!header, !offset1) = unsafeVarintAt entries offset
    (!suffixLength, !offset2) = unsafeVarintAt entries offset1
    (!pageCount, !suffixOffset)
        | header .&. 1 == 0 = (1, offset2)
        | otherwise         = let

                                  overflowPageCount, offset3 :: Int
                                  (!overflowPageCount, !offset3)
                                      = unsafeVarintAt entries offset2

                              in (succ overflowPageCount, offset3)

    shared :: Int
    !shared = header `unsafeShiftR` 1
{-# INLINE unsafeEntryAt #-}

-- * Search

{-|
    For a specification of this operation, see the documentation of [its
    type-agnostic version]('Database.LSMTree.Internal.Index.search').

    More precisely, the result is the page span of the group with the largest
    separator that is less than or equal to the given key. For a key @k@, this
    has the following consequences:

      * Every key stored in a page before the page span is less than @k@.

      * Every key stored in a page after the page span is greater than @k@.

    As a result, if @k@ is in the run, the result is the page span of the
    key–value pair with key @k@. If @k@ is not in the run, the result is either
    the page span that 'Database.LSMTree.Internal.Index.Ordinary.search' yields
    or the page span immediately before it, and in the latter case all keys of
    the page span are less than @k@.

    Like the ordinary index, an index without pages yields @PageSpan 0 0@.
-}
search :: SerialisedKey -> IndexPrefix -> PageSpan
search (SerialisedKey' key) (IndexPrefix entries restarts entryCount _)
    | entryCount <= 0 = PageSpan (PageNo 0) (PageNo 0)
    | otherwise       = scanBlock (findBlock 1 restartCount)
    where

    entriesOffset, keyOffset :: Int
    entriesArray, keyArray :: ByteArray
    VP.Vector entriesOffset _ entriesArray = entries
    VP.Vector keyOffset     _ keyArray     = key

    keyLength :: Int
    !keyLength = VP.length key

    restartCount :: Int
    !restartCount = VP.length restarts `unsafeShiftR` 1

    restartOffset, restartPage :: Int -> Int
    restartOffset block = fromIntegral (VP.unsafeIndex restarts (2 * block))
    restartPage   block = fromIntegral (VP.unsafeIndex restarts (2 * block + 1))

    {-
        Yields the length of the longest common prefix of the bytes of
        @entries@ at offsets from @offset@ to @offset + len@ (exclusive) and the
        part of the key starting at @start@.
    -}
    suffixCommonPrefixLength :: Int -> Int -> Int -> Int
    suffixCommonPrefixLength offset len start = go 0 where

        go :: Int -> Int
        go !i | i < len                                &&
                start + i < keyLength                  &&
                VP.unsafeIndex entries (offset + i) ==
                VP.unsafeIndex key (start + i)
                  = go (succ i)
              | otherwise
                  = i

    {-
        Checks whether the bytes of @entries@ at offsets from @offset@ to
        @offset + len@ (exclusive) are less than or equal to the part of the
        key starting at @start@, given that @common@ is the length of their
        longest common prefix.
    -}
    suffixLE :: Int -> Int -> Int -> Int -> Bool
    suffixLE offset len start common
        = common == len                       ||
          (start + common < keyLength         &&
           VP.unsafeIndex entries (offset + common) <
           VP.unsafeIndex key (start + common))

    {-
        Checks whether the bytes of @entries@ at offsets from @offset@ to
        @offset + len@ (exclusive) are less than or equal to the key. This uses
        @memcmp@, which is faster than comparing byte by byte, as done by
        'suffixCommonPrefixLength'.
    -}
    bytesLE :: Int -> Int -> Bool
    bytesLE offset len
        = case compareByteArrays entriesArray
                                 (entriesOffset + offset)
                                 keyArray
                                 keyOffset
                                 (min len keyLength) of
              LT -> True
              EQ -> len <= keyLength
              GT -> False

    {-
        Yields the largest block number whose restart separator is less than or
        equal to the key, given that this holds for all block numbers below
        @low@ and does not hold for any block number from @high@ on. The
        separator of block 0 is empty, so block 0 always qualifies.
    -}
    findBlock :: Int -> Int -> Int
    findBlock !low !high
        | low >= high = pred low
        | otherwise   = let

                            middle :: Int
                            !middle = (low + high) `unsafeShiftR` 1

                            DecodedEntry _ offset len _ _
                                = unsafeEntryAt entries (restartOffset middle)

                        in if bytesLE offset len
                               then findBlock (succ middle) high
                               else findBlock low middle

    {-
        Scans the entries of a block. The invariant of the loop is that the
        current entry's separator is less than or equal to the key and that
        @common@ is the length of their longest common prefix.
    -}
    scanBlock :: Int -> PageSpan
    scanBlock block = go (succ first)
                         next0
                         (restartPage block)
                         pageCount0
                         common0
        where

        first, end :: Int
        !first = block * restartInterval
        !end   = min entryCount (first + restartInterval)

        DecodedEntry _ offset0 len0 pageCount0 next0
            = unsafeEntryAt entries (restartOffset block)

        common0 :: Int
        !common0 = suffixCommonPrefixLength offset0 len0 0

        go :: Int -> Int -> Int -> Int -> Int -> PageSpan
        go !index !offset !page !pageCount !common
            | index >= end    = result
            | shared > common
                -- The next separator agrees with the current one at the first
                -- position where the current one differs from the key, so it
                -- is less than the key too, with the same common prefix.
                = go (succ index) next (page + pageCount) pageCount' common
            | shared < common
                -- The next separator is greater than the current one at a
                -- position where the current one agrees with the key.
                = result
            | suffixLE suffixOffset suffixLength common common'
                = go (succ index)
                     next
                     (page + pageCount)
                     pageCount'
                     (common + common')
            | otherwise
                = result
            where

            DecodedEntry shared suffixOffset suffixLength pageCount' next
                = unsafeEntryAt entries offset

            common' :: Int
            common' = suffixCommonPrefixLength suffixOffset suffixLength common

            result :: PageSpan
            result = PageSpan (PageNo page) (PageNo (page + pageCount - 1))

{-|
    For a specification of this operation, see the documentation of [its
    type-agnostic version]('Database.LSMTree.Internal.Index.sizeInPages').
-}
sizeInPages :: IndexPrefix -> NumPages
sizeInPages (IndexPrefix _ _ _ pageCount) = NumPages (fromIntegral pageCount)

-- * Serialisation

-- | The size of the header in bytes.
headerSize :: Int
headerSize = 8

-- | The size of the footer in bytes, excluding padding and the restart table.
footerSize :: Int
footerSize = 32

-- | Yields the number of padding bytes needed after the entries.
paddingSize :: Int -> Int
paddingSize entriesSize = negate (headerSize + entriesSize) .&. 7

{-|
    For a specification of this operation, see the documentation of [its
    type-agnostic version]('Database.LSMTree.Internal.Index.headerLBS').

    The type–version indicator is padded to 64 bits.
-}
headerLBS :: LazyByteString
headerLBS = toLazyByteString $
            word32Host supportedTypeAndVersion <> word32Host 0

{-|
    For a specification of this operation, see the documentation of [its
    type-agnostic version]('Database.LSMTree.Internal.Index.finalLBS').

    The footer consists of padding to 64 bits, the restart table, the size of
    the entries in bytes, the number of entries, the number of pages, and the
    number of key–value pairs of the run.
-}
finalLBS :: NumEntries -> IndexPrefix -> LazyByteString
finalLBS (NumEntries keyCount)
         (IndexPrefix entries restarts entryCount pageCount)
    = toLazyByteString $
      mconcat (replicate (paddingSize entriesSize) (word8 0)) <>
      VP.foldMap word32Host restarts                         <>
      word64Host (fromIntegral entriesSize)                  <>
      word64Host (fromIntegral entryCount)                   <>
      word64Host (fromIntegral pageCount)                    <>
      word64Host (fromIntegral keyCount)
    where

    entriesSize :: Int
    !entriesSize = VP.length entries

-- * Deserialisation

{-|
    For a specification of this operation, see the documentation of [its
    type-agnostic version]('Database.LSMTree.Internal.Index.fromSBS').

    The resulting index uses the given byte string as its backing memory. All
    entries are checked, so that 'search' can access the entries without bounds
    checks.
-}
fromSBS :: ShortByteString -> Either String (NumEntries, IndexPrefix)
fromSBS (SBS unliftedByteArray) = do
    when (fullSize `rem` 8 /= 0)
         (Left "Length is not a multiple of 64 bits")
    when (fullSize < headerSize + footerSize)
         (Left "Doesn't contain header and footer")
    when (typeAndVersion == byteSwap32 supportedTypeAndVersion)
         (Left "Non-matching endianness")
    when (typeAndVersion /= supportedTypeAndVersion)
         (Left "Unsupported type or version")
    when ((indexByteArray byteArray 1 :: Word32) /= 0)
         (Left "Non-zero header padding")
    entriesSize <- footerInt 4
    entryCount  <- footerInt 3
    pageCount   <- footerInt 2
    keyCount    <- footerInt 1
    let

        bodySize :: Int
        !bodySize = fullSize - headerSize - footerSize

        restartCount :: Int
        !restartCount = (entryCount + pred restartInterval) `quot`
                        restartInterval

    when (entriesSize > bodySize || entryCount > bodySize)
         (Left "Size information is too large")
    let

        padding :: Int
        !padding = paddingSize entriesSize

        restartTableOffset :: Int
        !restartTableOffset = headerSize + entriesSize + padding

    when (restartTableOffset + 8 * restartCount + footerSize /= fullSize)
         (Left "Size information does not match the length")
    unless (all ((== (0 :: Word8)) . indexByteArray byteArray)
                [headerSize + entriesSize .. pred restartTableOffset])
           (Left "Non-zero padding after the entries")
    let

        entries :: VP.Vector Word8
        !entries = mkPrimVector headerSize entriesSize byteArray

        restarts :: VP.Vector Word32
        !restarts = mkPrimVector (restartTableOffset `quot` 4)
                                 (2 * restartCount)
                                 byteArray

    checkEntries entries restarts entryCount pageCount
    pure (NumEntries keyCount,
          IndexPrefix entries restarts entryCount pageCount)
    where

    byteArray :: ByteArray
    byteArray = ByteArray unliftedByteArray

    fullSize :: Int
    fullSize = sizeofByteArray byteArray

    typeAndVersion :: Word32
    typeAndVersion = indexByteArray byteArray 0

    -- Reads the given 64-bit word, counting from the end, as an 'Int'.
    footerInt :: Int -> Either String Int
    footerInt indexFromEnd
        | toInteger word > toInteger (maxBound :: Int)
            = Left "Size information not representable as Int"
        | otherwise
            = Right (fromIntegral word)
        where

        word :: Word64
        word = indexByteArray byteArray (fullSize `quot` 8 - indexFromEnd)

{-|
    Checks that entries and a restart table fulfil the properties stated in the
    documentation of 'IndexPrefix' for the given numbers of entries and pages.
-}
checkEntries :: VP.Vector Word8
             -> VP.Vector Word32
             -> Int
             -> Int
             -> Either String ()
checkEntries entries restarts entryCount pageCount = go 0 0 0 VP.empty where

    entriesSize :: Int
    !entriesSize = VP.length entries

    go :: Int -> Int -> Int -> VP.Vector Word8 -> Either String ()
    go !index !offset !page !previous
        | index == entryCount
            = do
                  when (offset /= entriesSize)
                       (Left "Entries are followed by extra bytes")
                  when (page /= pageCount)
                       (Left "Page counts do not add up to the number of pages")
        | otherwise
            = do
                  let

                      isRestart :: Bool
                      !isRestart = index `rem` restartInterval == 0

                      restartIndex :: Int
                      !restartIndex = 2 * (index `quot` restartInterval)

                  when (isRestart &&
                        (toInteger (restarts VP.! restartIndex) /=
                         toInteger offset ||
                         toInteger (restarts VP.! succ restartIndex) /=
                         toInteger page))
                       (Left "Restart table does not match the entries")
                  (header, offset1) <- varintAt offset
                  (suffixLength, offset2) <- varintAt offset1
                  (groupPageCount, suffixOffset)
                      <- if header .&. 1 == 0
                             then pure (1, offset2)
                             else do
                                      (overflowPageCount, offset3)
                                          <- varintAt offset2
                                      when (overflowPageCount == 0 ||
                                            overflowPageCount >
                                            fromIntegral (maxBound :: Word32))
                                           (Left "Invalid overflow page count")
                                      pure (succ overflowPageCount, offset3)
                  let

                      shared :: Int
                      !shared = header `unsafeShiftR` 1

                  when (suffixLength > entriesSize - suffixOffset)
                       (Left "Suffix is out of bounds")
                  when (shared > VP.length previous)
                       (Left "Shared prefix is too long")
                  when (isRestart && shared /= 0)
                       (Left "Restart entry with shared prefix")
                  let

                      suffix :: VP.Vector Word8
                      !suffix = VP.slice suffixOffset suffixLength entries

                      current :: VP.Vector Word8
                      !current = VP.force (VP.take shared previous <> suffix)

                  when (index == 0 && not (VP.null current))
                       (Left "Non-empty first separator")
                  when (index > 0 && current <= previous)
                       (Left "Separators are not strictly increasing")
                  when (not isRestart                  &&
                        shared < VP.length previous    &&
                        suffixLength > 0               &&
                        VP.head suffix == previous VP.! shared)
                       (Left "Shared prefix is not maximal")
                  when (groupPageCount > pageCount - page)
                       (Left "Page counts exceed the number of pages")
                  go (succ index)
                     (suffixOffset + suffixLength)
                     (page + groupPageCount)
                     current

    -- Decodes a variable-length number with bounds checks.
    varintAt :: Int -> Either String (Int, Int)
    varintAt = loop 0 0 where

        loop :: Int -> Int -> Int -> Either String (Int, Int)
        loop !acc !shift !offset
            | offset >= entriesSize = Left "Number is out of bounds"
            | shift >= 35           = Left "Number is too long"
            | byte == 0 && shift > 0
                -- A final zero byte could be left out, so the encoding would
                -- not be the shortest one, which the format requires.
                                    = Left "Number is not in shortest form"
            | byte < 0x80           = Right (acc', succ offset)
            | otherwise             = loop acc' (shift + 7) (succ offset)
            where

            byte :: Word8
            !byte = VP.unsafeIndex entries offset

            acc' :: Int
            !acc' = acc .|. (fromIntegral (byte .&. 0x7F) `shiftL` shift)
