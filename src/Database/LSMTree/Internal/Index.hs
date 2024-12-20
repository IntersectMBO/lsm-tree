{-# LANGUAGE MagicHash    #-}
{-# LANGUAGE TypeFamilies #-}

{-|
    Provides a common interface to different types of fence pointer indexes and
    their accumulators.
-}
module Database.LSMTree.Internal.Index
(
    Index (search, sizeInPages, finalLBS),
    IndexAcc (ResultingIndex, appendSingle, appendMulti, unsafeEnd)
)
where

import           Control.DeepSeq (NFData (..))
import           Control.Monad.ST.Strict (ST)
import           Data.ByteString.Lazy (LazyByteString)
import           Data.Word (Word32)
import           Database.LSMTree.Internal.Chunk (Chunk)
import           Database.LSMTree.Internal.Entry (NumEntries)
import           Database.LSMTree.Internal.Page (NumPages, PageSpan)
import           Database.LSMTree.Internal.Serialise (SerialisedKey)

{-|
    The class of index types.

    This class contains also methods for the non-incremental parts of otherwise
    incremental serialisation. To completely serialise an index interleaved with
    its construction, proceed as follows:

     1. Use the 'headerLBS' operation of the respective index type to generate
        the header of the serialised index.

     2. Incrementally construct the index using the methods of 'IndexAcc', and
        assemble the body of the serialised index from the generated chunks.

     3. Use 'finalLBS' to generate the footer of the serialised index.
-}
class Index i where

    {-|
        Searches for a page span that contains a key–value pair with the given
        key. If there is indeed such a pair, the result is the corresponding
        page span; if there is no such pair, the result is an arbitrary but
        valid page span.
    -}
    search :: SerialisedKey -> i -> PageSpan

    -- | Yields the number of pages covered by an index.
    sizeInPages :: i -> NumPages

    {-|
        Yields the footer of the serialised form of an index.

        See the documentation of the 'Index' class for how to generate a
        complete serialised index.
    -}
    finalLBS :: NumEntries -> i -> LazyByteString

{-|
    The class of index accumulator types, where an index accumulator denotes an
    index under incremental construction.

    Incremental index construction is only guaranteed to work correctly when the
    following conditions are met:

      * The supplied key ranges do not overlap and are given in ascending order.

      * Each supplied key is at least 8 and at most 65535 bytes long.
        (Currently, construction of compact indexes needs the former and
        construction of ordinary indexes needs the latter bound.)
-}
class (Index (ResultingIndex j), NFData (ResultingIndex j)) => IndexAcc j where

    -- | The type of indexes constructed by accumulators of a certain type
    type ResultingIndex j

    {-|
        Adds information about a single page that fully comprises one or more
        key–value pairs to an index and outputs newly available chunks.

        See the documentation of the 'IndexAcc' class for constraints to adhere
        to.
    -}
    appendSingle :: (SerialisedKey, SerialisedKey) -> j s -> ST s (Maybe Chunk)

    {-|
        Adds information about multiple pages that together comprise a single
        key–value pair to an index and outputs newly available chunks.

        The provided 'Word32' value denotes the number of /overflow/ pages, so
        that the number of pages that comprise the key–value pair is the
        successor of that number.

        See the documentation of the 'IndexAcc' class for constraints to adhere
        to.
    -}
    appendMulti :: (SerialisedKey, Word32) -> j s -> ST s [Chunk]

    {-|
        Returns the constructed index, along with a final chunk in case the
        serialised key list has not been fully output yet, thereby invalidating
        the index under construction. Executing @unsafeEnd index@ is only safe
        when @index@ is not used afterwards.
    -}
    unsafeEnd :: j s -> ST s (Maybe Chunk, ResultingIndex j)
