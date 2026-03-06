{-# OPTIONS_HADDOCK not-home #-}

module Database.LSMTree.Internal.Config (
    LevelNo (..)
    -- * Table configuration
  , TableConfig (..)
  , defaultTableConfig
  , RunLevelNo (..)
  , runParamsForLevel
    -- * Merge policy
  , MergePolicy (..)
    -- * Size ratio
  , SizeRatio (..)
  , sizeRatioInt
    -- * Write buffer allocation
  , WriteBufferAlloc (..)
    -- * Bloom filter allocation
  , BloomFilterAlloc (..)
  , bloomFilterAllocForLevel
    -- * Fence pointer index
  , FencePointerIndexType (..)
  , indexTypeForRun
    -- * Disk cache policy
  , DiskCachePolicy (..)
  , diskCachePolicyForLevel
    -- * Merge schedule
  , MergeSchedule (..)
    -- * Merge batch size
  , MergeBatchSize (..)
  , creditThresholdForLevel
  ) where

import           Control.DeepSeq (NFData (..))
import           Database.LSMTree.Internal.Index (IndexType)
import qualified Database.LSMTree.Internal.Index as Index
                     (IndexType (Compact, Ordinary))
import qualified Database.LSMTree.Internal.MergingRun as MR
import           Database.LSMTree.Internal.Run (RunDataCaching (..))
import           Database.LSMTree.Internal.RunAcc (RunBloomFilterAlloc (..))
import           Database.LSMTree.Internal.RunBuilder (RunParams (..))

newtype LevelNo = LevelNo Int
  deriving stock (Show, Eq, Ord)
  deriving newtype (Enum, NFData)

{-------------------------------------------------------------------------------
  Table configuration
-------------------------------------------------------------------------------}

{- |
A collection of configuration parameters for tables, which can be used to tune the performance of the table.
To construct a 'TableConfig', modify the 'defaultTableConfig', which defines reasonable defaults for all parameters.

For a detailed discussion of fine-tuning the table configuration, see [Fine-tuning Table Configuration](../#fine_tuning).

[@confMergePolicy :: t'MergePolicy'@]
    The /merge policy/ balances the performance of lookups against the performance of updates.
    Levelling favours lookups.
    Tiering favours updates.
    Lazy levelling strikes a middle ground between levelling and tiering, and moderately favours updates.
    This parameter is explicitly referenced in the documentation of those operations it affects.

[@confSizeRatio :: t'SizeRatio'@]
    The /size ratio/ pushes the effects of the merge policy to the extreme.
    If the size ratio is higher, levelling favours lookups more, and tiering and lazy levelling favour updates more.
    This parameter is referred to as \(T\) in the disk I\/O cost of operations.

[@confWriteBufferAlloc :: t'WriteBufferAlloc'@]
    The /write buffer capacity/ balances the performance of lookups and updates against the in-memory size of the database.
    If the write buffer is larger, it takes up more memory, but lookups and updates are more efficient.
    This parameter is referred to as \(B\) in the disk I\/O cost of operations.
    Irrespective of this parameter, the write buffer size cannot exceed 4GiB.

[@confMergeSchedule :: t'MergeSchedule'@]
    The /merge schedule/ balances the performance of lookups and updates against the consistency of updates.
    With the one-shot merge schedule, lookups and updates are more efficient overall, but some updates may take much longer than others.
    With the incremental merge schedule, lookups and updates are less efficient overall, but each update does a similar amount of work.
    This parameter is explicitly referenced in the documentation of those operations it affects.
    The merge schedule does not affect the way that table unions are computed.
    However, any table union must complete all outstanding incremental updates.

[@confBloomFilterAlloc :: t'BloomFilterAlloc'@]
    The Bloom filter size balances the performance of lookups against the in-memory size of the database.
    If the Bloom filters are larger, they take up more memory, but lookup operations are more efficient.

[@confFencePointerIndex :: t'FencePointerIndexType'@]
    The /fence-pointer index type/ supports two types of indexes.
    The /ordinary/ indexes are designed to work with any key.
    The /compact/ indexes are optimised for the case where the keys in the database are uniformly distributed, e.g., when the keys are hashes.

[@confDiskCachePolicy :: t'DiskCachePolicy'@]
    The /disk cache policy/ supports caching lookup operations using the OS page cache.
    Caching may improve the performance of lookups and updates if database access follows certain patterns.

[@confMergeBatchSize :: t'MergeBatchSize'@]
    The merge batch size balances the maximum latency of individual update
    operations, versus the latency of a sequence of update operations. Bigger
    batches improves overall performance but some updates will take a lot
    longer than others. The default is to use a large batch size.
-}
data TableConfig = TableConfig {
    confMergePolicy       :: !MergePolicy
  , confMergeSchedule     :: !MergeSchedule
  , confSizeRatio         :: !SizeRatio
  , confWriteBufferAlloc  :: !WriteBufferAlloc
  , confBloomFilterAlloc  :: !BloomFilterAlloc
  , confFencePointerIndex :: !FencePointerIndexType
  , confDiskCachePolicy   :: !DiskCachePolicy
  , confMergeBatchSize    :: !MergeBatchSize
  }
  deriving stock (Show, Eq)

instance NFData TableConfig where
  rnf (TableConfig a b c d e f g h) =
      rnf a `seq` rnf b `seq` rnf c `seq` rnf d `seq`
      rnf e `seq` rnf f `seq` rnf g `seq` rnf h

-- | The 'defaultTableConfig' defines reasonable defaults for all 'TableConfig' parameters.
--
-- >>> confMergePolicy defaultTableConfig
-- LazyLevelling
-- >>> confMergeSchedule defaultTableConfig
-- Incremental
-- >>> confSizeRatio defaultTableConfig
-- Four
-- >>> confWriteBufferAlloc defaultTableConfig
-- AllocNumEntries 20000
-- >>> confBloomFilterAlloc defaultTableConfig
-- AllocRequestFPR 1.0e-3
-- >>> confFencePointerIndex defaultTableConfig
-- OrdinaryIndex
-- >>> confDiskCachePolicy defaultTableConfig
-- DiskCacheAll
-- >>> confMergeBatchSize defaultTableConfig
-- MergeBatchSize 20000
--
defaultTableConfig :: TableConfig
defaultTableConfig =
    TableConfig
      { confMergePolicy       = LazyLevelling
      , confMergeSchedule     = Incremental
      , confSizeRatio         = Four
      , confWriteBufferAlloc  = AllocNumEntries 20_000
      , confBloomFilterAlloc  = AllocRequestFPR 1.0e-3
      , confFencePointerIndex = OrdinaryIndex
      , confDiskCachePolicy   = DiskCacheAll
      , confMergeBatchSize    = MergeBatchSize 20_000 -- same as write buffer
      }

data RunLevelNo = RegularLevel LevelNo | UnionLevel

runParamsForLevel :: TableConfig -> RunLevelNo -> RunParams
runParamsForLevel conf@TableConfig {..} levelNo =
    RunParams
      { runParamCaching = diskCachePolicyForLevel confDiskCachePolicy levelNo
      , runParamAlloc   = bloomFilterAllocForLevel conf levelNo
      , runParamIndex   = indexTypeForRun confFencePointerIndex
      }

{-------------------------------------------------------------------------------
  Merge policy
-------------------------------------------------------------------------------}

{- |
The /merge policy/ balances the performance of lookups against the performance of updates.
Levelling favours lookups.
Tiering favours updates.
Lazy levelling strikes a middle ground between levelling and tiering, and moderately favours updates.
This parameter is explicitly referenced in the documentation of those operations it affects.

__NOTE:__ This package only supports lazy levelling.

For a detailed discussion of the merge policy, see [Fine-tuning: Merge Policy, Size Ratio, and Write Buffer Size](../#fine_tuning_data_layout).
-}
data MergePolicy =
    LazyLevelling
  deriving stock (Eq, Show)

instance NFData MergePolicy where
  rnf LazyLevelling = ()

{-------------------------------------------------------------------------------
  Size ratio
-------------------------------------------------------------------------------}

{- |
The /size ratio/ pushes the effects of the merge policy to the extreme.
If the size ratio is higher, levelling favours lookups more, and tiering and lazy levelling favour updates more.
This parameter is referred to as \(T\) in the disk I\/O cost of operations.

__NOTE:__ This package only supports a size ratio of four.

For a detailed discussion of the size ratio, see [Fine-tuning: Merge Policy, Size Ratio, and Write Buffer Size](../#fine_tuning_data_layout).
-}
data SizeRatio = Four
  deriving stock (Eq, Show)

instance NFData SizeRatio where
  rnf Four = ()

sizeRatioInt :: SizeRatio -> Int
sizeRatioInt = \case Four -> 4

{-------------------------------------------------------------------------------
  Write buffer allocation
-------------------------------------------------------------------------------}

-- TODO: "If the sizes of values vary greatly, this can lead to unevenly sized runs on disk and unpredictable performance."

{- |
The /write buffer capacity/ balances the performance of lookups and updates against the in-memory size of the table.
If the write buffer is larger, it takes up more memory, but lookups and updates are more efficient.
Irrespective of this parameter, the write buffer size cannot exceed 4GiB.

For a detailed discussion of the size ratio, see [Fine-tuning: Merge Policy, Size Ratio, and Write Buffer Size](../#fine_tuning_data_layout).
-}
data WriteBufferAlloc =
    {- |
    Allocate space for the in-memory write buffer to fit the requested number of entries.
    This parameter is referred to as \(B\) in the disk I\/O cost of operations.
    -}
    AllocNumEntries !Int
  deriving stock (Show, Eq)

instance NFData WriteBufferAlloc where
  rnf (AllocNumEntries n) = rnf n

{-------------------------------------------------------------------------------
  Merge schedule
-------------------------------------------------------------------------------}

{- |
The /merge schedule/ balances the performance of lookups and updates against the consistency of updates.
The merge schedule does not affect the performance of table unions.
With the one-shot merge schedule, lookups and updates are more efficient overall, but some updates may take much longer than others.
With the incremental merge schedule, lookups and updates are less efficient overall, but each update does a similar amount of work.
This parameter is explicitly referenced in the documentation of those operations it affects.

For a detailed discussion of the effect of the merge schedule, see [Fine-tuning: Merge Schedule](../#fine_tuning_merge_schedule).
-}
data MergeSchedule =
    {- |
    The 'OneShot' merge schedule causes the merging algorithm to complete merges immediately.
    This is more efficient than the 'Incremental' merge schedule, but has an inconsistent workload.
    Using the 'OneShot' merge schedule, the worst-case disk I\/O complexity of the update operations is /linear/ in the size of the table.
    For real-time systems and other use cases where unresponsiveness is unacceptable, use the 'Incremental' merge schedule.
    -}
    OneShot
    {- |
    The 'Incremental' merge schedule spreads out the merging work over time.
    This is less efficient than the 'OneShot' merge schedule, but has a consistent workload.
    Using the 'Incremental' merge schedule, the worst-case disk I\/O complexity of the update operations is /logarithmic/ in the size of the table.
    This 'Incremental' merge schedule still uses batching to improve performance.
    The batch size can be controlled using the 'MergeBatchSize'.
    -}
  | Incremental
  deriving stock (Eq, Show)

instance NFData MergeSchedule where
  rnf OneShot     = ()
  rnf Incremental = ()

{-------------------------------------------------------------------------------
  Bloom filter allocation
-------------------------------------------------------------------------------}

{- |
The Bloom filter size balances the performance of lookups against the in-memory size of the table.
If the Bloom filters are larger, they take up more memory, but lookup operations are more efficient.

For a detailed discussion of the Bloom filter size, see [Fine-tuning: Bloom Filter Size](../#fine_tuning_bloom_filter_size).
-}
data BloomFilterAlloc =
    {- |
    Allocate the requested number of bits per entry in the table.

    The value must strictly positive, but fractional values are permitted.
    The recommended range is \([2, 24]\).
    -}
    AllocFixed !Double
  | {- |
    Allocate the required number of bits per entry to get the requested false-positive rate.

    The value must be in the range \((0, 1)\).
    The recommended range is \([1\mathrm{e}{ -5 },1\mathrm{e}{ -2 }]\).
    -}
    AllocRequestFPR !Double
  deriving stock (Show, Eq)

instance NFData BloomFilterAlloc where
  rnf (AllocFixed n)        = rnf n
  rnf (AllocRequestFPR fpr) = rnf fpr

bloomFilterAllocForLevel :: TableConfig -> RunLevelNo -> RunBloomFilterAlloc
bloomFilterAllocForLevel conf _levelNo =
    case confBloomFilterAlloc conf of
      AllocFixed n        -> RunAllocFixed n
      AllocRequestFPR fpr -> RunAllocRequestFPR fpr

{-------------------------------------------------------------------------------
  Fence pointer index
-------------------------------------------------------------------------------}

{- |
The /fence-pointer index type/ supports two types of indexes.
The /ordinary/ indexes are designed to work with any key.
The /compact/ indexes are optimised for the case where the keys in the database are uniformly distributed, e.g., when the keys are hashes.

For a detailed discussion the fence-pointer index types, see [Fine-tuning: Fence-Pointer Index Type](../#fine_tuning_fence_pointer_index_type).
-}
data FencePointerIndexType =
    {- |
    Ordinary indexes are designed to work with any key.

    When using an ordinary index, the 'Database.LSMTree.Internal.Serialise.Class.serialiseKey' function cannot produce output larger than 64KiB.
    -}
    OrdinaryIndex
  | {- |
    Compact indexes are designed  for the case where the keys in the database are uniformly distributed, e.g., when the keys are hashes.

    When using a compact index, some requirements apply to serialised keys:

    * keys must be uniformly distributed;
    * keys can be of variable length;
    * keys less than 8 bytes (64bits) are padded with zeros (in LSB position).
    -}
    CompactIndex
  deriving stock (Eq, Show)

instance NFData FencePointerIndexType where
  rnf CompactIndex  = ()
  rnf OrdinaryIndex = ()

indexTypeForRun :: FencePointerIndexType -> IndexType
indexTypeForRun CompactIndex  = Index.Compact
indexTypeForRun OrdinaryIndex = Index.Ordinary

{-------------------------------------------------------------------------------
  Disk cache policy
-------------------------------------------------------------------------------}

{- |
The /disk cache policy/ determines if lookup operations use the OS page cache.
Caching may improve the performance of lookups if database access follows certain patterns.

For a detailed discussion the disk cache policy, see [Fine-tuning: Disk Cache Policy](../#fine_tuning_disk_cache_policy).
-}
data DiskCachePolicy =
    {- |
    Cache all data in the table.

    Use this policy if the database's access pattern has either good spatial locality or both good spatial and temporal locality.
    -}
    DiskCacheAll

  | {- |
    Cache the data in the freshest @l@ levels.

    Use this policy if the database's access pattern only has good temporal locality.

    The variable @l@ determines the number of levels that are cached.
    For a description of levels, see [Merge Policy, Size Ratio, and Write Buffer Size](#fine_tuning_data_layout).
    With this setting, the database can be expected to cache up to \(\frac{k}{P}\) pages of memory,
    where \(k\) refers to the number of entries that fit in levels \([1,l]\) and is defined as \(\sum_{i=1}^{l}BT^{i}\).
    -}
    -- TODO: Add a policy for caching based on size in bytes, rather than exposing internal details such as levels.
    --       For instance, a policy that states "cache the freshest 10MiB"
    DiskCacheLevelOneTo !Int

  | {- |
    Do not cache any table data.

    Use this policy if the database's access pattern has does not have good spatial or temporal locality.
    For instance, if the access pattern is uniformly random.
    -}
    DiskCacheNone
  deriving stock (Show, Eq)

instance NFData DiskCachePolicy where
  rnf DiskCacheAll            = ()
  rnf (DiskCacheLevelOneTo l) = rnf l
  rnf DiskCacheNone           = ()

-- | Interpret the 'DiskCachePolicy' for a level: should we cache data in runs
-- at this level.
--
diskCachePolicyForLevel :: DiskCachePolicy -> RunLevelNo -> RunDataCaching
diskCachePolicyForLevel policy levelNo =
  case policy of
    DiskCacheAll          -> CacheRunData
    DiskCacheNone         -> NoCacheRunData
    DiskCacheLevelOneTo n ->
      case levelNo of
        RegularLevel l | l <= LevelNo n -> CacheRunData
                       | otherwise      -> NoCacheRunData
        UnionLevel                      -> NoCacheRunData

{-------------------------------------------------------------------------------
  Merge batch size
-------------------------------------------------------------------------------}

{- |
The /merge batch size/ is a micro-tuning parameter, and in most cases you do
need to think about it and can leave it at its default.

When using the 'Incremental' merge schedule, merging is done in batches. This
is a trade-off: larger batches tends to mean better overall performance but the
downside is that while most updates (inserts, deletes, upserts) are fast, some
are slower (when a batch of merging work has to be done).

If you care most about the maximum latency of updates, then use a small batch
size. If you don't care about latency of individual operations, just the
latency of the overall sequence of operations then use a large batch size. The
default is to use a large batch size, the same size as the write buffer itself.
The minimum batch size is 1. The maximum batch size is the size of the write
buffer 'confWriteBufferAlloc'.

Note that the actual batch size is the minimum of this configuration
parameter and the size of the batch of operations performed (e.g. 'inserts').
So if you consistently use large batches, you can use a batch size of 1 and
the merge batch size will always be determined by the operation batch size.

A further reason why it may be preferable to use minimal batch sizes is to get
good parallel work balance, when using parallelism.
-}
newtype MergeBatchSize = MergeBatchSize Int
  deriving stock (Show, Eq, Ord)
  deriving newtype (NFData)

-- TODO: the thresholds for doing merge work should be different for each level,
-- and ideally all-pairs co-prime.
creditThresholdForLevel :: TableConfig -> LevelNo -> MR.CreditThreshold
creditThresholdForLevel TableConfig {
                           confMergeBatchSize   = MergeBatchSize mergeBatchSz,
                           confWriteBufferAlloc = AllocNumEntries writeBufferSz
                         }
                        (LevelNo _i) =
    MR.CreditThreshold
  . MR.UnspentCredits
  . MR.MergeCredits
  . max 1
  . min writeBufferSz
  $ mergeBatchSz
