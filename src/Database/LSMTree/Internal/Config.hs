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
    The merge policy determines when a table sorts and deduplicates its data.
    This affects the disk I\/O complexity of many table operations.
    This parameter is explicitly referenced in the documentation of those operations it affects.
[@confMergeSchedule :: t'MergeSchedule'@]
    The merge schedule determines how a table sorts and deduplicates its data.
    This affects the worst-case disk I\/O complexity of update operations.
    This parameter is explicitly referenced in the documentation of those operations it affects.
[@confSizeRatio :: t'SizeRatio'@]
    The size ratio determines how a table organises its data.
    This affects the disk I\/O complexity of many table operations.
    This parameter is referred to as \(T\) in the disk I\/O cost of operations.
[@confWriteBufferAlloc :: t'WriteBufferAlloc'@]
    The write buffer allocation strategy determines the maximum size of the in-memory write buffer.
    This affects the disk I\/O cost of many table operations.
    This parameter is referred to as \(B\) in the disk I\/O cost of operations.
    Irrespective of this parameter, the write buffer size cannot exceed 4GiB.
[@confBloomFilterAlloc :: t'BloomFilterAlloc'@]
    The Bloom filter allocation strategy determines the number of bits per physical entry allocated for the Bloom filters.
    This affects the in-memory size of tables.
    See [In-memory size of tables](../#performance_size).
[@confFencePointerIndex :: t'FencePointerIndexType'@]
    The fence pointer index type determines the type of the fence pointer indexes.
    This affects the in-memory size of tables.
    See [In-memory size of tables](../#performance_size).
    Some index types impose additional constraints on the type of table keys.
[@confDiskCachePolicy :: t'DiskCachePolicy'@]
    The disk cache policy determines the policy for caching data from disk in memory.
    This may affect the performance of lookup operations.
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
-- AllocFixed 10
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
      , confBloomFilterAlloc  = AllocFixed 10
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
The merge policy determines when a table sorts and deduplicates its data.
This affects how much of the data that is kept sorted and deduplicated.
More sorting favours lookups, less sorting favours updates.

Currently, this package only supports the 'LazyLevelling' merge policy.

For a detailed discussion of the effect of the merge policy, see [Fine-tuning Table Configuration](../#fine_tuning).
-}
data MergePolicy =
    LazyLevelling
  deriving stock (Eq, Show)

instance NFData MergePolicy where
  rnf LazyLevelling = ()

{-------------------------------------------------------------------------------
  Merge schedule
-------------------------------------------------------------------------------}

{- |
The merge schedule determines how a table sorts and deduplicates its data.
This affects the performance of lookup and update operations.
The merge schedule does not affect the performance of table unions.

For a detailed discussion of the effect of the merge schedule, see [Fine-tuning Table Configuration](../#fine_tuning).
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
  deriving stock (Eq, Show)

instance NFData MergeSchedule where
  rnf OneShot     = ()
  rnf Incremental = ()

{-------------------------------------------------------------------------------
  Size ratio
-------------------------------------------------------------------------------}

{- |
The size ratio determines how a table organises its data.
This affects the disk I\/O complexity of many table operations.
This parameter is referred to as \(T\) in the disk I\/O cost of operations.

For a detailed discussion of the effect of the size ratio, see [Fine-tuning Table Configuration](../#fine_tuning).
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
The write buffer allocation strategy determines the maximum size of the in-memory write buffer.
This affects the disk I\/O cost of many table operations.
Irrespective of this parameter, the write buffer size cannot exceed 4GiB.
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
  Bloom filter allocation
-------------------------------------------------------------------------------}

{- |
The Bloom filter allocation strategy determines the number of bits per physical entry allocated for the Bloom filters.
There is a trade-off between Bloom filter memory size and the false-positive rate.
A higher false positive rate (FPR) leads to more unnecessary I\/O.
The false-positive rate scales exponentially with the number of bits per entry:

+---------------------------+---------------------+
| False-positive rate       | Bits per entry      |
+===========================+=====================+
| \(1\text{ in }10\)        | \(\approx  4.77 \)  |
+---------------------------+---------------------+
| \(1\text{ in }100\)       | \(\approx  9.85 \)  |
+---------------------------+---------------------+
| \(1\text{ in }1{,}000\)   | \(\approx 15.79 \)  |
+---------------------------+---------------------+
| \(1\text{ in }10{,}000\)  | \(\approx 22.58 \)  |
+---------------------------+---------------------+
| \(1\text{ in }100{,}000\) | \(\approx 30.22 \)  |
+---------------------------+---------------------+

The policy can be specified by fixing a number of bits per entry or by fixing the desired FPR.

For a detailed discussion of the in-memory size of Bloom filters, see [In-memory size of tables](../#performance_size).
-}
data BloomFilterAlloc =
    {- |
    Allocate the requested number of bits per key–operation pair in the table.

    The value must strictly positive.
    Non-integer values are permitted.
    The recommended range is \([2, 24]\).
    -}
    AllocFixed !Double
  | {- |
    Allocate the required number of bits per physical entry to get the requested false-positive rate.

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
The fence pointer index type determines the type of the fence pointer indexes.
This affects the in-memory size of tables.
Some index types impose additional constraints on the type of table keys.

For a detailed discussion of the in-memory size of indexes, see [In-memory size of tables](../#performance_size).
-}
data FencePointerIndexType =
    {- |
    Ordinary indexes are designed to work with any key.

    When using an ordinary index, the 'Database.LSMTree.Internal.Serialise.Class.serialiseKey' function cannot produce output larger than 64KiB.
    -}
    OrdinaryIndex
  | {- |
    Compact indexes are designed to work with keys that are large cryptographic hashes.

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
The disk cache policy determines the policy for caching data from disk in memory.
This may affect the performance of lookup operations.

If the access pattern has good spatial or temporal locality,
then caching data in memory improves performance.
On the other hand, if the access pattern has poor locality,
then caching is detrimental to performance.

This package is designed to have good performance without caching any table data.
However, some use cases may benefit from the caching of table data.

This package relies on the OS page cache and only caches disk pages, as opposed to individual lookup results.
-}
-- TODO: @jdral "good spatial or temporal locality" <-- What is this is simple words?
-- TODO: @jdral "if the overall data size fits within memory" <-- What overall data size?
data DiskCachePolicy =
    {- |
    Cache all data in the table.

    Use this policy if the expected access pattern for the table has good spatial or temporal locality.
    -}
    DiskCacheAll

  | {- |
    Cache the data in the freshest @n@ levels.

    For instance, @'DiskCacheLevelsAtOrBelow' 4@ means the table caches data for all levels between @1@ and @4@.
    Use this policy if the expected access pattern for the table has good spatial or temporal locality for fresh data.

    For a detailed discussion of levels, see [Fine-tuning Table Configuration](../#fine_tuning).
    -}
    -- TODO: Add a policy for caching based on size in bytes, rather than exposing internal details such as levels.
    --       For instance, a policy that states "cache the freshest 10MiB"
    DiskCacheLevelsAtOrBelow !Int

  | {- |
    Do not cache any table data.

    Use this policy if the expected access pattern for the table has poor spatial or temporal locality, such as uniform random access.
    -}
    DiskCacheNone
  deriving stock (Show, Eq)

instance NFData DiskCachePolicy where
  rnf DiskCacheAll                 = ()
  rnf (DiskCacheLevelsAtOrBelow l) = rnf l
  rnf DiskCacheNone                = ()

-- | Interpret the 'DiskCachePolicy' for a level: should we cache data in runs
-- at this level.
--
diskCachePolicyForLevel :: DiskCachePolicy -> RunLevelNo -> RunDataCaching
diskCachePolicyForLevel policy levelNo =
  case policy of
    DiskCacheAll  -> CacheRunData
    DiskCacheNone -> NoCacheRunData
    DiskCacheLevelsAtOrBelow n ->
      case levelNo of
        RegularLevel l | l <= LevelNo n -> CacheRunData
                       | otherwise      -> NoCacheRunData
        UnionLevel                      -> NoCacheRunData
