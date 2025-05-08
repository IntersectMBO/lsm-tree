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
import           Data.Word (Word64)
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

Each parameter is associated with its own type.
Detailed discussion of the use of each parameter can be found in the documentation for its associated type.
For an overview, see the [Performance](../#performance) section in the package description.

The 'defaultTableConfig' defines reasonable defaults for all parameters.
-}
data TableConfig = TableConfig {
    -- | The merge policy determines how the table manages its data,
    --   which affects the disk I\/O cost of some operations.
    confMergePolicy       :: !MergePolicy
    -- | The size ratio determines how the table manages its data,
    --   and is the parameter \(T\) in the disk I\/O cost of operations.
  , confSizeRatio         :: !SizeRatio
    -- | The write buffer allocation strategy determines the maximum size of the in-memory write buffer,
    --   and is the parameter \(B\) in the disk I\/O cost of operations.
    --   Irrespective of this parameter, the write buffer size cannot exceed 4GiB.
  , confWriteBufferAlloc  :: !WriteBufferAlloc
    -- | The Bloom filter allocation strategy determines the number of bits per physical entry allocated for the Bloom filters.
  , confBloomFilterAlloc  :: !BloomFilterAlloc
    -- | The fence pointer index type determines the type of indexes,
    --   which affects the in-memory size of the table and may constrain the table keys.
  , confFencePointerIndex :: !FencePointerIndexType
    -- | The disk cache policy determines the policy for caching data from disk in memory,
    --   which may affect the performance of lookup operations.
  , confDiskCachePolicy   :: !DiskCachePolicy
    -- | The merge schedule determines how the table manages its data,
    --   which affects the disk I\/O cost of some operations.
  , confMergeSchedule     :: !MergeSchedule
  }
  deriving stock (Show, Eq)

instance NFData TableConfig where
  rnf (TableConfig a b c d e f g) =
      rnf a `seq` rnf b `seq` rnf c `seq` rnf d `seq` rnf e `seq` rnf f `seq` rnf g

-- | The 'defaultTableConfig' defines reasonable defaults for all 'TableConfig' parameters.
--
-- >>> :{
-- defaultTableConfig ==
--     TableConfig
--       { confMergePolicy       = LazyLevelling
--       , confSizeRatio         = Four
--       , confWriteBufferAlloc  = AllocNumEntries 20_000
--       , confBloomFilterAlloc  = AllocFixed 10
--       , confFencePointerIndex = OrdinaryIndex
--       , confDiskCachePolicy   = DiskCacheAll
--       , confMergeSchedule     = Incremental
--       }
-- :}
-- True
--
defaultTableConfig :: TableConfig
defaultTableConfig =
    TableConfig
      { confMergePolicy       = LazyLevelling
      , confSizeRatio         = Four
      , confWriteBufferAlloc  = AllocNumEntries 20_000
      , confBloomFilterAlloc  = AllocFixed 10
      , confFencePointerIndex = OrdinaryIndex
      , confDiskCachePolicy   = DiskCacheAll
      , confMergeSchedule     = Incremental
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

data MergePolicy =
    -- | Use tiering on intermediate levels, and levelling on the last level.
    -- This makes it easier for delete operations to disappear on the last
    -- level.
    LazyLevelling
    -- TODO: add other merge policies, like tiering and levelling.
  deriving stock (Eq, Show)

instance NFData MergePolicy where
  rnf LazyLevelling = ()

{-------------------------------------------------------------------------------
  Size ratio
-------------------------------------------------------------------------------}

data SizeRatio = Four
  deriving stock (Eq, Show)

instance NFData SizeRatio where
  rnf Four = ()

sizeRatioInt :: SizeRatio -> Int
sizeRatioInt = \case Four -> 4

{-------------------------------------------------------------------------------
  Write buffer allocation
-------------------------------------------------------------------------------}

-- | Allocation method for the write buffer.
data WriteBufferAlloc =
    -- | Total number of key\/value pairs that can be present in the write
    -- buffer before flushing the write buffer to disk.
    --
    -- NOTE: if the sizes of values vary greatly, this can lead to wonky runs on
    -- disk, and therefore unpredictable performance.
    AllocNumEntries !Int
  deriving stock (Show, Eq)

instance NFData WriteBufferAlloc where
  rnf (AllocNumEntries n) = rnf n

{-------------------------------------------------------------------------------
  Bloom filter allocation
-------------------------------------------------------------------------------}

-- | Allocation method for bloom filters.
--
-- NOTE: a __physical__ database entry is a key\/operation pair that exists in a
-- file, i.e., a run. Multiple physical entries that have the same key
-- constitute a __logical__ database entry.
data BloomFilterAlloc =
    -- | Allocate a fixed number of bits per physical entry in each bloom
    -- filter.
    AllocFixed
      !Word64 -- ^ Bits per physical entry.
  | -- | Allocate as many bits as required per physical entry to get the requested
    -- false-positive rate. Do this for each bloom filter.
    AllocRequestFPR
      !Double -- ^ Requested FPR.
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

-- | Configure the type of fence pointer index.
data FencePointerIndexType =
    -- | Use a compact fence pointer index.
    --
    -- Compact indexes are designed to work with keys that are large (for
    -- example, 32 bytes long) cryptographic hashes.
    --
    -- When using a compact index, it is vital that the
    -- 'Database.LSMTree.Internal.Serialise.Class.serialiseKey' function
    -- satisfies the following law:
    --
    -- [Minimal size] @'Database.LSMTree.Internal.RawBytes.size'
    -- ('Database.LSMTree.Internal.Serialise.Class.serialiseKey' x) >= 8@
    --
    -- Use 'serialiseKeyMinimalSize' to test this law.
    CompactIndex
    -- | Use an ordinary fence pointer index
    --
    -- Ordinary indexes do not have any constraints on keys other than that
    -- their serialised forms may not be 64 KiB or more in size.
  | OrdinaryIndex
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

-- | The policy for caching data from disk in memory (using the OS page cache).
--
-- Caching data in memory can improve performance if the access pattern has
-- good access locality or if the overall data size fits within memory. On the
-- other hand, caching is detrimental to performance and wastes memory if the
-- access pattern has poor spatial or temporal locality.
--
-- This implementation is designed to have good performance using a cacheless
-- policy, where main memory is used only to cache Bloom filters and indexes,
-- but none of the key\/value data itself. Nevertheless, some use cases will be
-- faster if some or all of the key\/value data is also cached in memory. This
-- implementation does not do any custom caching of key\/value data, relying
-- simply on the OS page cache. Thus caching is done in units of 4kb disk pages
-- (as opposed to individual key\/value pairs for example).
--
data DiskCachePolicy =

       -- | Use the OS page cache to cache any\/all key\/value data in the
       -- table.
       --
       -- Use this policy if the expected access pattern for the table
       -- has a good spatial or temporal locality.
       DiskCacheAll

       -- | Use the OS page cache to cache data in all LSMT levels at or below
       -- a given level number. For example, use 1 to cache the first level.
       -- (The write buffer is considered to be level 0.)
       --
       -- Use this policy if the expected access pattern for the table
       -- has good temporal locality for recently inserted keys.
     | DiskCacheLevelsAtOrBelow !Int

       --TODO: Add a policy based on size in bytes rather than internal details
       -- like levels. An easy use policy would be to say: "cache the first 10
       -- Mb" and have everything worked out from that.

       -- | Do not cache any key\/value data in any level (except the write
       -- buffer).
       --
       -- Use this policy if expected access pattern for the table has poor
       -- spatial or temporal locality, such as uniform random access.
     | DiskCacheNone
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

{-------------------------------------------------------------------------------
  Merge schedule
-------------------------------------------------------------------------------}

-- | A configuration option that determines how merges are stepped to
-- completion. This does not affect the amount of work that is done by merges,
-- only how the work is spread out over time.
data MergeSchedule =
    -- | Complete merges immediately when started.
    --
    -- The 'OneShot' option will make the merging algorithm perform /big/ batches
    -- of work in one go, so intermittent slow-downs can be expected. For use
    -- cases where unresponsiveness is unacceptable, e.g. in real-time systems,
    -- use 'Incremental' instead.
    OneShot
    -- | Schedule merges for incremental construction, and step the merge when
    -- updates are performed on a table.
    --
    -- The 'Incremental' option spreads out merging work over time. More
    -- specifically, updates to a table can cause a /small/ batch of merge work
    -- to be performed. The scheduling of these batches is designed such that
    -- merges are fully completed in time for when new merges are started on the
    -- same level.
  | Incremental
  deriving stock (Eq, Show)

instance NFData MergeSchedule where
  rnf OneShot     = ()
  rnf Incremental = ()
