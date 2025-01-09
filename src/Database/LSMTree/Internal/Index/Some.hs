{-# LANGUAGE MagicHash    #-}

-- | Provides support for working with fence pointer indexes of unknown type.
module Database.LSMTree.Internal.Index.Some
(
    SomeIndex (SomeIndex),
    search,
    sizeInPages,
    finalLBS,
    fromSBS,
    SomeIndexAcc (SomeIndexAcc),
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
import           Database.LSMTree.Internal.Index (Index, IndexAcc)
import qualified Database.LSMTree.Internal.Index as Index (appendMulti,
                     appendSingle, finalLBS, fromSBS, newWithDefaults, search,
                     sizeInPages, unsafeEnd)
import           Database.LSMTree.Internal.Page (NumPages, PageSpan)
import           Database.LSMTree.Internal.Serialise (SerialisedKey)
import           GHC.Exts (Proxy#)

-- | The type of indexes, where an index can have any concrete type.
data SomeIndex = forall i . Index i => SomeIndex i

instance NFData SomeIndex where

    rnf (SomeIndex index) = rnf index

-- | A version of 'Index.search' that works with indexes of unknown type.
search :: SerialisedKey -> SomeIndex -> PageSpan
search key (SomeIndex index) = Index.search key index

-- | A version of 'Index.sizeInPages' that works with indexes of unknown type.
sizeInPages  :: SomeIndex -> NumPages
sizeInPages (SomeIndex index) = Index.sizeInPages index

-- | A version of 'Index.finalLBS' that works with indexes of unknown type.
finalLBS :: NumEntries -> SomeIndex -> LazyByteString
finalLBS entryCount (SomeIndex index) = Index.finalLBS entryCount index

-- | A version of 'Index.fromSBS' that works with indexes of unknown type.
fromSBS :: Index i
        => Proxy# i
        -> ShortByteString
        -> Either String (NumEntries, SomeIndex)
fromSBS indexTypeProxy shortByteString
    = second SomeIndex <$> Index.fromSBS indexTypeProxy shortByteString

{-|
    The type of index accumulators, where an index accumulator can have any
    concrete type.
-}
data SomeIndexAcc s = forall j . IndexAcc j => SomeIndexAcc (j s)

{-|
    A version of 'Index.newWithDefaults' that works with indexes of unknown
    type.
-}
newWithDefaults :: IndexAcc j => Proxy# j -> ST s (SomeIndexAcc s)
newWithDefaults indexAccTypeProxy
    = SomeIndexAcc <$> Index.newWithDefaults indexAccTypeProxy

-- | A version of 'Index.appendSingle' that works with indexes of unknown type.
appendSingle :: (SerialisedKey, SerialisedKey)
             -> SomeIndexAcc s
             -> ST s (Maybe Chunk)
appendSingle pageInfo (SomeIndexAcc indexAcc)
    = Index.appendSingle pageInfo indexAcc

-- | A version of 'Index.appendMulti' that works with indexes of unknown type.
appendMulti :: (SerialisedKey, Word32) -> SomeIndexAcc s -> ST s [Chunk]
appendMulti pagesInfo (SomeIndexAcc indexAcc)
    = Index.appendMulti pagesInfo indexAcc

-- | A version of 'Index.unsafeEnd' that works with indexes of unknown type.
unsafeEnd :: SomeIndexAcc s -> ST s (Maybe Chunk, SomeIndex)
unsafeEnd (SomeIndexAcc indexAcc)
    = second SomeIndex <$> Index.unsafeEnd indexAcc
