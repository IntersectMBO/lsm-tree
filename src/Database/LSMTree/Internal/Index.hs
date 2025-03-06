{-|
    Provides support for working with fence pointer indexes of different types
    and their accumulators.

    Keys used with an index are subject to the key size constraints of the
    concrete type of the index. These constraints are stated in the descriptions
    of the modules "Database.LSMTree.Internal.Index.Compact" and
    "Database.LSMTree.Internal.Index.Ordinary", respectively.

    Part of the functionality that this module provides is the construction of
    serialised indexes in a mostly incremental fashion. The incremental part of
    serialisation is provided through index accumulators, while the
    non-incremental bits are provided through the index operations 'headerLBS'
    and 'finalLBS'. To completely serialise an index interleaved with its
    construction, proceed as follows:

     1. Use 'headerLBS' to generate the header of the serialised index.

     2. Incrementally construct the index using the operations of 'IndexAcc',
        and assemble the body of the serialised index from the generated chunks.

     3. Use 'finalLBS' to generate the footer of the serialised index.
-}
module Database.LSMTree.Internal.Index
(
    -- * Index types
    IndexType (Compact, Ordinary),

    -- * Indexes
    Index (CompactIndex, OrdinaryIndex),
    search,
    sizeInPages,
    headerLBS,
    finalLBS,
    fromSBS,

    -- * Index accumulators
    IndexAcc (CompactIndexAcc, OrdinaryIndexAcc),
    newWithDefaults,
    appendSingle,
    appendMulti,
    unsafeEnd
)
where

import           Control.Arrow (second)
import           Control.DeepSeq (NFData (rnf))
import           Control.Monad.ST.Strict (ST)
import           Data.ByteString.Lazy (LazyByteString)
import           Data.ByteString.Short (ShortByteString)
import           Data.Word (Word32)
import           Database.LSMTree.Internal.Chunk (Chunk)
import           Database.LSMTree.Internal.Entry (NumEntries)
import           Database.LSMTree.Internal.Index.Compact (IndexCompact)
import qualified Database.LSMTree.Internal.Index.Compact as Compact (finalLBS,
                     fromSBS, headerLBS, search, sizeInPages)
import           Database.LSMTree.Internal.Index.CompactAcc (IndexCompactAcc)
import qualified Database.LSMTree.Internal.Index.CompactAcc as Compact
                     (appendMulti, appendSingle, newWithDefaults, unsafeEnd)
import           Database.LSMTree.Internal.Index.Ordinary (IndexOrdinary)
import qualified Database.LSMTree.Internal.Index.Ordinary as Ordinary (finalLBS,
                     fromSBS, headerLBS, search, sizeInPages)
import           Database.LSMTree.Internal.Index.OrdinaryAcc (IndexOrdinaryAcc)
import qualified Database.LSMTree.Internal.Index.OrdinaryAcc as Ordinary
                     (appendMulti, appendSingle, newWithDefaults, unsafeEnd)
import           Database.LSMTree.Internal.Page (NumPages, PageSpan)
import           Database.LSMTree.Internal.Serialise (SerialisedKey)

-- * Index types

-- | The type of supported index types.
data IndexType = Compact | Ordinary
    deriving stock (Eq, Show)

-- * Indexes

-- | The type of supported indexes.
data Index
    = CompactIndex  !IndexCompact
    | OrdinaryIndex !IndexOrdinary
    deriving stock (Eq, Show)

instance NFData Index where

    rnf (CompactIndex  index) = rnf index
    rnf (OrdinaryIndex index) = rnf index

{-|
    Searches for a page span that contains a key–value pair with the given key.
    If there is indeed such a pair, the result is the corresponding page span;
    if there is no such pair, the result is an arbitrary but valid page span.
-}
search :: SerialisedKey -> Index -> PageSpan
search key (CompactIndex  index) = Compact.search  key index
search key (OrdinaryIndex index) = Ordinary.search key index

-- | Yields the number of pages covered by an index.
sizeInPages :: Index -> NumPages
sizeInPages (CompactIndex  index) = Compact.sizeInPages  index
sizeInPages (OrdinaryIndex index) = Ordinary.sizeInPages index

{-|
    Yields the header of the serialised form of an index.

    See [the module documentation]("Database.LSMTree.Internal.Index") for how to
    generate a complete serialised index.
-}
headerLBS :: IndexType -> LazyByteString
headerLBS Compact  = Compact.headerLBS
headerLBS Ordinary = Ordinary.headerLBS

{-|
    Yields the footer of the serialised form of an index.

    See [the module documentation]("Database.LSMTree.Internal.Index") for how to
    generate a complete serialised index.
-}
finalLBS :: NumEntries -> Index -> LazyByteString
finalLBS entryCount (CompactIndex  index) = Compact.finalLBS  entryCount index
finalLBS entryCount (OrdinaryIndex index) = Ordinary.finalLBS entryCount index

{-|
    Reads an index along with the number of entries of the respective run from a
    byte string.

    The byte string must contain the serialised index exactly, with no leading
    or trailing space. Furthermore, its contents must be stored 64-bit-aligned.

    The contents of the byte string may be directly used as the backing memory
    for the constructed index. Currently, this is done for compact indexes.

    For deserialising numbers, the endianness of the host system is used. If
    serialisation has been done with a different endianness, this mismatch is
    detected by looking at the type–version indicator.
-}
fromSBS :: IndexType -> ShortByteString -> Either String (NumEntries, Index)
fromSBS Compact input  = second CompactIndex  <$> Compact.fromSBS  input
fromSBS Ordinary input = second OrdinaryIndex <$> Ordinary.fromSBS input

-- * Index accumulators

{-|
    The type of supported index accumulators, where an index accumulator denotes
    an index under incremental construction.

    Incremental index construction is only guaranteed to work correctly when the
    supplied key ranges do not overlap and are given in ascending order.
-}
data IndexAcc s = CompactIndexAcc  (IndexCompactAcc  s)
                | OrdinaryIndexAcc (IndexOrdinaryAcc s)

-- | Create a new index accumulator, using a default configuration.
newWithDefaults :: IndexType -> ST s (IndexAcc s)
newWithDefaults Compact  = CompactIndexAcc  <$> Compact.newWithDefaults
newWithDefaults Ordinary = OrdinaryIndexAcc <$> Ordinary.newWithDefaults

{-|
    Adds information about a single page that fully comprises one or more
    key–value pairs to an index and outputs newly available chunks.

    See the documentation of the 'IndexAcc' type for constraints to adhere to.
-}
appendSingle :: (SerialisedKey, SerialisedKey)
             -> IndexAcc s
             -> ST s (Maybe Chunk)
appendSingle pageInfo (CompactIndexAcc  indexAcc) = Compact.appendSingle
                                                        pageInfo
                                                        indexAcc
appendSingle pageInfo (OrdinaryIndexAcc indexAcc) = Ordinary.appendSingle
                                                        pageInfo
                                                        indexAcc

{-|
    Adds information about multiple pages that together comprise a single
    key–value pair to an index and outputs newly available chunks.

    The provided 'Word32' value denotes the number of /overflow/ pages, so that
    the number of pages that comprise the key–value pair is the successor of
    that number.

    See the documentation of the 'IndexAcc' type for constraints to adhere to.
-}
appendMulti :: (SerialisedKey, Word32) -> IndexAcc s -> ST s [Chunk]
appendMulti pagesInfo (CompactIndexAcc  indexAcc) = Compact.appendMulti
                                                        pagesInfo
                                                        indexAcc
appendMulti pagesInfo (OrdinaryIndexAcc indexAcc) = Ordinary.appendMulti
                                                        pagesInfo
                                                        indexAcc

{-|
    Returns the constructed index, along with a final chunk in case the
    serialised key list has not been fully output yet, thereby invalidating the
    index under construction. Executing @unsafeEnd index@ is only safe when
    @index@ is not used afterwards.
-}
unsafeEnd :: IndexAcc s -> ST s (Maybe Chunk, Index)
unsafeEnd (CompactIndexAcc  indexAcc) = second CompactIndex         <$>
                                        Compact.unsafeEnd  indexAcc
unsafeEnd (OrdinaryIndexAcc indexAcc) = second OrdinaryIndex        <$>
                                        Ordinary.unsafeEnd indexAcc
