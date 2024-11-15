module Database.LSMTree.Extras.Index
(
    Append (AppendSinglePage, AppendMultiPage),
    append,
    append'
)
where

import           Control.DeepSeq (NFData (rnf))
import           Control.Monad.ST.Strict (ST)
import           Data.Foldable (toList)
import           Data.Word (Word32)
import           Database.LSMTree.Internal.Chunk (Chunk)
import           Database.LSMTree.Internal.IndexCompactAcc (IndexCompactAcc)
import qualified Database.LSMTree.Internal.IndexCompactAcc as IndexCompact
                     (appendMulti, appendSingle)
import           Database.LSMTree.Internal.IndexOrdinaryAcc (IndexOrdinaryAcc)
import qualified Database.LSMTree.Internal.IndexOrdinaryAcc as IndexOrdinary
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
    Add information about appended pages to an index under incremental
    construction.

    Internally, 'append' uses 'IndexCompact.appendSingle' and
    'IndexCompact.appendMulti', and the usage restrictions of those functions
    apply also here.
-}
append :: Append -> IndexCompactAcc s -> ST s [Chunk]
append instruction indexAcc = case instruction of
    AppendSinglePage minKey maxKey
        -> toList <$> IndexCompact.appendSingle (minKey, maxKey) indexAcc
    AppendMultiPage key overflowPageCount
        -> IndexCompact.appendMulti (key, overflowPageCount) indexAcc

{-|
    A variant of 'append' for ordinary indexes, which is only used temporarily
    until there is a type class of index types.

    Internally, 'append'' uses 'IndexOrdinary.appendSingle' and
    'IndexOrdinary.appendMulti', and the usage restrictions of those functions
    apply also here.
-}
append' :: Append -> IndexOrdinaryAcc s -> ST s [Chunk]
append' instruction indexAcc = case instruction of
    AppendSinglePage minKey maxKey
        -> toList <$> IndexOrdinary.appendSingle (minKey, maxKey) indexAcc
    AppendMultiPage key overflowPageCount
        -> IndexOrdinary.appendMulti (key, overflowPageCount) indexAcc
