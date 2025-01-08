{-# LANGUAGE TypeFamilies #-}

-- | Provides support for working with fence pointer indexes of unknown type.
module Database.LSMTree.Internal.Index.Some
(
    IndexType (Compact, Ordinary),
    SomeIndex (SomeIndex),
    headerLBS,
    fromSBS,
    SomeIndexAcc (SomeIndexAcc),
    newWithDefaults
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
data IndexType = Compact | Ordinary deriving stock (Show, Eq)

-- | The type of indexes.
data SomeIndex = forall i . Index i => SomeIndex i

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
headerLBS Compact  = IndexCompact.headerLBS
headerLBS Ordinary = IndexOrdinary.headerLBS

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
fromSBS Compact  = fmap (second SomeIndex) . IndexCompact.fromSBS
fromSBS Ordinary = fmap (second SomeIndex) . IndexOrdinary.fromSBS

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
newWithDefaults Compact  = SomeIndexAcc <$> IndexCompact.new 1024
newWithDefaults Ordinary = SomeIndexAcc <$> IndexOrdinary.new 1024 4096
