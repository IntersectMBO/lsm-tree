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
  , serialiseKeyMinimalSize
    -- * Disk cache policy
  , DiskCachePolicy (..)
  , diskCachePolicyForLevel
    -- * Merge schedule
  , MergeSchedule (..)
  ) where

import           Control.DeepSeq (NFData (..))
import           Database.LSMTree.Internal.Index (IndexType)
import qualified Database.LSMTree.Internal.Index as Index
                     (IndexType (Compact, Ordinary))
import qualified Database.LSMTree.Internal.RawBytes as RB
import           Database.LSMTree.Internal.Run (RunDataCaching (..))
import           Database.LSMTree.Internal.RunAcc (RunBloomFilterAlloc (..))
import           Database.LSMTree.Internal.RunBuilder (RunParams (..))
import           Database.LSMTree.Internal.Serialise.Class (SerialiseKey (..))

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
    The merge schedule does not affect the performance of table unions.
    With the one-shot merge schedule, lookups and updates are more efficient overall, but some updates may take much longer than others.
    With the incremental merge schedule, lookups and updates are less efficient overall, but each update does a similar amount of work.
    This parameter is explicitly referenced in the documentation of those operations it affects.

[@confBloomFilterAlloc :: t'BloomFilterAlloc'@]
    The Bloom filter size balances the performance of lookups against the in-memory size of the database.
    If the Bloom filters are larger, they take up more memory, but lookup operations are more efficient.

[@confFencePointerIndex :: t'FencePointerIndexType'@]
    The /fence-pointer index type/ supports two types of indexes.
    The /ordinary/ indexes are designed to work with any key.
    The /compact/ indexes are optimised for the case where the keys in the database are uniformly distributed, e.g., when the keys are hashes.

[@confDiskCachePolicy :: t'DiskCachePolicy'@]
    The /disk cache policy/ supports caching lookup operations using the OS page cache.
    Caching may improve the performance of lookups if database access follows certain patterns.
-}
data TableConfig = TableConfig {
    confMergePolicy       :: !MergePolicy
  , confMergeSchedule     :: !MergeSchedule
  , confSizeRatio         :: !SizeRatio
  , confWriteBufferAlloc  :: !WriteBufferAlloc
  , confBloomFilterAlloc  :: !BloomFilterAlloc
  , confFencePointerIndex :: !FencePointerIndexType
  , confDiskCachePolicy   :: !DiskCachePolicy
  }
  deriving stock (Show, Eq)

instance NFData TableConfig where
  rnf (TableConfig a b c d e f g) =
      rnf a `seq` rnf b `seq` rnf c `seq` rnf d `seq` rnf e `seq` rnf f `seq` rnf g

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
    -}
  | Incremental
  | Greedy
  deriving stock (Eq, Show)

instance NFData MergeSchedule where
  rnf OneShot     = ()
  rnf Incremental = ()
  rnf Greedy      = ()

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

    When using a compact index, the 'Database.LSMTree.Internal.Serialise.Class.serialiseKey' function must satisfy the following additional law:

    [Minimal size]
      @'Database.LSMTree.Internal.RawBytes.size' ('Database.LSMTree.Internal.Serialise.Class.serialiseKey' x) >= 8@

    Use 'serialiseKeyMinimalSize' to test this law.
    -}
    CompactIndex
  deriving stock (Eq, Show)

instance NFData FencePointerIndexType where
  rnf CompactIndex  = ()
  rnf OrdinaryIndex = ()

indexTypeForRun :: FencePointerIndexType -> IndexType
indexTypeForRun CompactIndex  = Index.Compact
indexTypeForRun OrdinaryIndex = Index.Ordinary

-- | Test the __Minimal size__ law for the 'CompactIndex' option.
serialiseKeyMinimalSize :: SerialiseKey k => k -> Bool
serialiseKeyMinimalSize x = RB.size (serialiseKey x) >= 8

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
