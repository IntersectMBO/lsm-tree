{-|
    Provides additional support for working with fence pointer indexes and their
    accumulators.
-}
module Database.LSMTree.Extras.Index
(
    Append (AppendSinglePage, AppendMultiPage),
    append
)
where

import           Control.DeepSeq (NFData (rnf))
import           Control.Monad.ST.Strict (ST)
import           Data.Foldable (toList)
import           Data.Word (Word32)
import           Database.LSMTree.Internal.Chunk (Chunk)
import           Database.LSMTree.Internal.Index (IndexAcc, appendMulti,
                     appendSingle)
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
    available chunks.

    See the documentation of the 'IndexAcc' class for constraints to adhere to.
-}
append :: IndexAcc j => Append -> j s -> ST s [Chunk]
append instruction indexAcc = case instruction of
    AppendSinglePage minKey maxKey
        -> toList <$> appendSingle (minKey, maxKey) indexAcc
    AppendMultiPage key overflowPageCount
        -> appendMulti (key, overflowPageCount) indexAcc
{-# INLINABLE append #-}
