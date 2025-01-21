{-|
    Provides additional support for working with fence pointer indexes and their
    accumulators.
-}
module Database.LSMTree.Extras.Index
(
    Append (AppendSinglePage, AppendMultiPage),
    appendToCompact,
    appendToOrdinary,
    append
)
where

import           Control.DeepSeq (NFData (rnf))
import           Control.Monad.ST.Strict (ST)
import           Data.Foldable (toList)
import           Data.Word (Word32)
import           Database.LSMTree.Internal.Chunk (Chunk)
import           Database.LSMTree.Internal.Index (IndexAcc)
import qualified Database.LSMTree.Internal.Index as Index (appendMulti,
                     appendSingle)
import           Database.LSMTree.Internal.Index.CompactAcc (IndexCompactAcc)
import qualified Database.LSMTree.Internal.Index.CompactAcc as IndexCompact
                     (appendMulti, appendSingle)
import           Database.LSMTree.Internal.Index.OrdinaryAcc (IndexOrdinaryAcc)
import qualified Database.LSMTree.Internal.Index.OrdinaryAcc as IndexOrdinary
                     (appendMulti, appendSingle)
import           Database.LSMTree.Internal.Serialise (SerialisedKey)

-- | Instruction for appending pages, to be used in conjunction with indexes.
data Append
    = {-|
          Append a single page that fully comprises one or more key–value pairs.
      -}
      AppendSinglePage
          SerialisedKey -- ^ Minimum key
          SerialisedKey -- ^ Maximum key
    | {-|
          Append multiple pages that together comprise a single key–value pair.
      -}
      AppendMultiPage
          SerialisedKey -- ^ Sole key
          Word32        -- ^ Number of overflow pages

instance NFData Append where

    rnf (AppendSinglePage minKey maxKey)
        = rnf minKey `seq` rnf maxKey
    rnf (AppendMultiPage key overflowPageCount)
        = rnf key `seq` rnf overflowPageCount

{-|
    Adds information about appended pages to an index and outputs newly
    available chunks, using primitives specific to the type of the index.

    See the documentation of the 'IndexAcc' type for constraints to adhere to.
-}
appendWith :: ((SerialisedKey, SerialisedKey) -> j s -> ST s (Maybe Chunk))
           -> ((SerialisedKey, Word32)        -> j s -> ST s [Chunk])
           -> Append
           -> j s
           -> ST s [Chunk]
appendWith appendSingle appendMulti instruction indexAcc = case instruction of
    AppendSinglePage minKey maxKey
        -> toList <$> appendSingle (minKey, maxKey) indexAcc
    AppendMultiPage key overflowPageCount
        -> appendMulti (key, overflowPageCount) indexAcc
{-# INLINABLE appendWith #-}

{-|
    Adds information about appended pages to a compact index and outputs newly
    available chunks.

    See the documentation of the 'IndexAcc' type for constraints to adhere to.
-}
appendToCompact :: Append -> IndexCompactAcc s -> ST s [Chunk]
appendToCompact = appendWith IndexCompact.appendSingle
                             IndexCompact.appendMulti
{-# INLINE appendToCompact #-}

{-|
    Adds information about appended pages to an ordinary index and outputs newly
    available chunks.

    See the documentation of the 'IndexAcc' type for constraints to adhere to.
-}
appendToOrdinary :: Append -> IndexOrdinaryAcc s -> ST s [Chunk]
appendToOrdinary = appendWith IndexOrdinary.appendSingle
                              IndexOrdinary.appendMulti
{-# INLINE appendToOrdinary #-}

{-|
    Adds information about appended pages to an index and outputs newly
    available chunks.

    See the documentation of the 'IndexAcc' type for constraints to adhere to.
-}
append :: Append -> IndexAcc s -> ST s [Chunk]
append = appendWith Index.appendSingle
                    Index.appendMulti
{-# INLINE append #-}
