{-# LANGUAGE TypeFamilies #-}

-- | Provides support for working with fence pointer indexes of unknown type.
module Database.LSMTree.Internal.Index.Some
(
    IndexType (CompactIndex, OrdinaryIndex),
    SomeIndex (SomeIndex),
    headerLBS,
    fromSBS,
    SomeIndexAcc (SomeIndexAcc),
    newWithDefaults
)
where

import           Control.Arrow (second)
import           Control.DeepSeq (NFData (..))
import           Control.Monad.ST.Strict (ST)
import           Data.ByteString.Lazy (LazyByteString)
import           Data.ByteString.Short (ShortByteString)
import           Data.Word (Word32)
import           Database.LSMTree.Internal.Chunk (Chunk)
import           Database.LSMTree.Internal.Entry (NumEntries)
import           Database.LSMTree.Internal.Index (Index (..), IndexAcc (..))
import qualified Database.LSMTree.Internal.Index.Compact as IndexCompact
                     (fromSBS, headerLBS)
import qualified Database.LSMTree.Internal.Index.CompactAcc as IndexCompact
                     (new)
import qualified Database.LSMTree.Internal.Index.Ordinary as IndexOrdinary
                     (fromSBS, headerLBS)
import qualified Database.LSMTree.Internal.Index.OrdinaryAcc as IndexOrdinary
                     (new)
import           Database.LSMTree.Internal.Page (NumPages, PageSpan)
import           Database.LSMTree.Internal.Serialise (SerialisedKey)

-- | The type of concrete index types specifically supported by this module.
data IndexType
    {-|
        The type of compact indexes.

        Compact indexes are designed to work with keys that are large (for
        example, 32 bytes long) cryptographic hashes.

        When using a compact index, it is vital that the
        'Database.LSMTree.Internal.Serialise.Class.serialiseKey' function
        satisfies the following law:

        [Minimal size] @'Database.LSMTree.Internal.RawBytes.size'
          ('Database.LSMTree.Internal.Serialise.Class.serialiseKey' x) >= 8@

        Use 'Database.LSMTree.Internal.Serialise.Class.serialiseKeyMinimalSize'
        to test this law.
    -}
    = CompactIndex
      {-|
          The type of ordinary indexes.

          Ordinary indexes do not have any constraints on keys other than that
          their serialised forms may not be 64 KiB or more in size.
      -}
    | OrdinaryIndex
    deriving stock (Show, Eq)

instance NFData IndexType where

    rnf CompactIndex  = ()
    rnf OrdinaryIndex = ()

-- | The type of indexes.
data SomeIndex = forall i . (Index i, NFData i) => SomeIndex i

instance NFData SomeIndex where

    rnf (SomeIndex index) = rnf index

instance Index SomeIndex where

    search :: SerialisedKey -> SomeIndex -> PageSpan
    search key (SomeIndex index) = search key index

    sizeInPages  :: SomeIndex -> NumPages
    sizeInPages (SomeIndex index) = sizeInPages index

    finalLBS :: NumEntries -> SomeIndex -> LazyByteString
    finalLBS entryCount (SomeIndex index) = finalLBS entryCount index

-- | Yields the header of the serialised form of an index of a given type.
headerLBS :: IndexType -> LazyByteString
headerLBS CompactIndex  = IndexCompact.headerLBS
headerLBS OrdinaryIndex = IndexOrdinary.headerLBS

{-|
    Reads an index of a given type along with the number of entries of the
    respective run from a byte string.

    The byte string must contain the serialised index exactly, with no leading
    or trailing space. In the case of a compact index, the contents of the byte
    string must be stored 64-bit-aligned.

    For deserialising numbers, the endianness of the host system is used. If
    serialisation has been done with a different endianness, this mismatch is
    detected by looking at the type–version indicator.
-}
fromSBS :: IndexType -> ShortByteString -> Either String (NumEntries, SomeIndex)
fromSBS CompactIndex  = fmap (second SomeIndex) . IndexCompact.fromSBS
fromSBS OrdinaryIndex = fmap (second SomeIndex) . IndexOrdinary.fromSBS

-- | The type of index accumulators.
data SomeIndexAcc s = forall j . IndexAcc j => SomeIndexAcc (j s)

instance IndexAcc SomeIndexAcc where

    type ResultingIndex SomeIndexAcc = SomeIndex

    appendSingle :: (SerialisedKey, SerialisedKey)
                 -> SomeIndexAcc s
                 -> ST s (Maybe Chunk)
    appendSingle pageInfo (SomeIndexAcc indexAcc)
        = appendSingle pageInfo indexAcc

    appendMulti :: (SerialisedKey, Word32) -> SomeIndexAcc s -> ST s [Chunk]
    appendMulti pagesInfo (SomeIndexAcc indexAcc)
        = appendMulti pagesInfo indexAcc

    unsafeEnd :: SomeIndexAcc s -> ST s (Maybe Chunk, SomeIndex)
    unsafeEnd (SomeIndexAcc indexAcc) = second SomeIndex <$> unsafeEnd indexAcc

{-|
    Create a new accumulator for an index of a given type, using a default
    configuration.
-}
newWithDefaults :: IndexType -> ST s (SomeIndexAcc s)
newWithDefaults CompactIndex  = SomeIndexAcc <$> IndexCompact.new 1024
newWithDefaults OrdinaryIndex = SomeIndexAcc <$> IndexOrdinary.new 1024 4096
