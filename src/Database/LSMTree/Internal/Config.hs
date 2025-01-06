module Database.LSMTree.Internal.Config (
    LevelNo (..)
    -- * Table configuration
  , TableConfig (..)
  , defaultTableConfig
    -- * Table configuration override
  , TableConfigOverride
  , applyOverride
  , configNoOverride
  , configOverrideDiskCachePolicy
    -- * Merge policy
  , MergePolicy (..)
    -- * Size ratio
  , SizeRatio (..)
  , sizeRatioInt
    -- * Write buffer allocation
  , WriteBufferAlloc (..)
    -- * Bloom filter allocation
  , BloomFilterAlloc (..)
  , defaultBloomFilterAlloc
  , bloomFilterAllocForLevel
    -- * Fence pointer index
  , FencePointerIndex (..)
  , indexTypeForRun
    -- * Disk cache policy
  , DiskCachePolicy (..)
  , diskCachePolicyForLevel
    -- * Merge schedule
  , MergeSchedule (..)
  , defaultMergeSchedule
  ) where

import           Control.DeepSeq (NFData (..))
import           Data.Maybe (fromMaybe)
import           Data.Monoid (Last (..))
import           Data.Word (Word64)
import           Database.LSMTree.Internal.Assertions (assert,
                     fromIntegralChecked)
import           Database.LSMTree.Internal.Entry (NumEntries (..))
import           Database.LSMTree.Internal.Index.Some (IndexType (..))
import           Database.LSMTree.Internal.Run (RunDataCaching (..))
import           Database.LSMTree.Internal.RunAcc (RunBloomFilterAlloc (..))
import qualified Monkey

newtype LevelNo = LevelNo Int
  deriving stock (Show, Eq)
  deriving newtype Enum

{-------------------------------------------------------------------------------
  Table configuration
-------------------------------------------------------------------------------}

-- | Table configuration parameters, including LSM tree tuning parameters.
--
-- Some config options are fixed (for now):
--
-- * Merge policy: Tiering
--
-- * Size ratio: 4
data TableConfig = TableConfig {
    confMergePolicy       :: !MergePolicy
    -- Size ratio between the capacities of adjacent levels.
  , confSizeRatio         :: !SizeRatio
    -- | Total number of bytes that the write buffer can use.
    --
    -- The maximum is 4GiB, which should be more than enough for realistic
    -- applications.
  , confWriteBufferAlloc  :: !WriteBufferAlloc
  , confBloomFilterAlloc  :: !BloomFilterAlloc
  , confFencePointerIndex :: !FencePointerIndex
    -- | The policy for caching key\/value data from disk in memory.
  , confDiskCachePolicy   :: !DiskCachePolicy
  , confMergeSchedule     :: !MergeSchedule
  }
  deriving stock (Show, Eq)

instance NFData TableConfig where
  rnf (TableConfig a b c d e f g) =
      rnf a `seq` rnf b `seq` rnf c `seq` rnf d `seq` rnf e `seq` rnf f `seq` rnf g

-- | A reasonable default 'TableConfig'.
--
-- This uses a write buffer with up to 20,000 elements and a generous amount of
-- memory for Bloom filters (FPR of 2%).
--
defaultTableConfig :: TableConfig
defaultTableConfig =
    TableConfig
      { confMergePolicy       = MergePolicyLazyLevelling
      , confSizeRatio         = Four
      , confWriteBufferAlloc  = AllocNumEntries (NumEntries 20_000)
      , confBloomFilterAlloc  = defaultBloomFilterAlloc
      , confFencePointerIndex = CompactIndex
      , confDiskCachePolicy   = DiskCacheAll
      , confMergeSchedule     = defaultMergeSchedule
      }

{-------------------------------------------------------------------------------
  Table configuration override
-------------------------------------------------------------------------------}

-- | Override configuration options in 'TableConfig' that can be changed dynamically.
--
-- Some parts of the 'TableConfig' are considered fixed after a table is
-- created. That is, these options should (i) should stay the same over the
-- lifetime of a table, and (ii) these options should not be changed when a
-- snapshot is created or loaded. Other options can be changed dynamically
-- without sacrificing correctness.
--
-- This type has 'Semigroup' and 'Monoid' instances for composing override
-- options.
data TableConfigOverride = TableConfigOverride {
      -- | Override for 'confDiskCachePolicy'
      confOverrideDiskCachePolicy  :: Last DiskCachePolicy
    }
    deriving stock Show

-- | Behaves like a point-wise 'Last' instance
instance Semigroup TableConfigOverride where
  override1 <> override2 = TableConfigOverride {
        confOverrideDiskCachePolicy =
          confOverrideDiskCachePolicy override1 <>
          confOverrideDiskCachePolicy override2
      }

-- | Behaves like a point-wise 'Last' instance
instance Monoid TableConfigOverride where
  mempty = configNoOverride

applyOverride :: TableConfigOverride -> TableConfig -> TableConfig
applyOverride TableConfigOverride{..} conf = conf {
      confDiskCachePolicy =
        fromMaybe (confDiskCachePolicy conf) (getLast confOverrideDiskCachePolicy)
    }

configNoOverride :: TableConfigOverride
configNoOverride = TableConfigOverride {
      confOverrideDiskCachePolicy = Last Nothing
    }

configOverrideDiskCachePolicy :: DiskCachePolicy -> TableConfigOverride
configOverrideDiskCachePolicy pol = TableConfigOverride {
      confOverrideDiskCachePolicy = Last (Just pol)
    }

{-------------------------------------------------------------------------------
  Merge policy
-------------------------------------------------------------------------------}

data MergePolicy =
    -- | Use tiering on intermediate levels, and levelling on the last level.
    -- This makes it easier for delete operations to disappear on the last
    -- level.
    MergePolicyLazyLevelling
{- TODO: disabled for now. Would we ever want to provide pure tiering?
  | MergePolicyTiering
-}
{- TODO: disabled for now
  | MergePolicyLevelling
-}
  deriving stock (Show, Eq)

instance NFData MergePolicy where
  rnf MergePolicyLazyLevelling = ()

{-------------------------------------------------------------------------------
  Size ratio
-------------------------------------------------------------------------------}

data SizeRatio = Four
  deriving stock (Show, Eq)

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
    AllocNumEntries !NumEntries
{- TODO: disabled for now
  | -- | Total number of bytes that the write buffer can use.
    --
    -- The maximum is 4GiB, which should be more than enough for realistic
    -- applications.
    AllocTotalBytes !Word32
-}
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
  | -- | Allocate bits amongst all bloom filters according to the Monkey algorithm.
    --
    -- The allocation algorithm will never go over the memory budget. If more
    -- levels are added that the algorithm did not account for, then bloom
    -- filters on those levels will be empty. This can happen for a number of
    -- reasons:
    --
    -- * The number of budgeted physical entries is exceeded
    -- * Underfull runs causes levels to be underfull, which causes entries to
    --   reside in larger levels
    --
    -- To combat this, make sure to budget for a generous number of physical
    -- entries.
    AllocMonkey
      !Word64 -- ^ Total number of bytes that bloom filters can use collectively.
      !NumEntries -- ^ Total number of /physical/ entries expected to be in the database.
  deriving stock (Show, Eq)

instance NFData BloomFilterAlloc where
  rnf (AllocFixed n)        = rnf n
  rnf (AllocRequestFPR fpr) = rnf fpr
  rnf (AllocMonkey a b)     = rnf a `seq` rnf b

defaultBloomFilterAlloc :: BloomFilterAlloc
defaultBloomFilterAlloc = AllocFixed 10

bloomFilterAllocForLevel :: TableConfig -> LevelNo -> RunBloomFilterAlloc
bloomFilterAllocForLevel conf (LevelNo l) =
    assert (l > 0) $
    case confBloomFilterAlloc conf of
      AllocFixed n -> RunAllocFixed n
      AllocRequestFPR fpr -> RunAllocRequestFPR fpr
      AllocMonkey totalBits (NumEntries n) ->
        let !sr = sizeRatioInt (confSizeRatio conf)
            !m = case confWriteBufferAlloc conf of
                    AllocNumEntries (NumEntries x) -> x
            !levelCount = Monkey.numLevels (fromIntegral n) (fromIntegral m) (fromIntegral sr)
            !allocPerLevel = Monkey.monkeyBits
                                (fromIntegralChecked totalBits)
                                (fromIntegralChecked n)
                                (fromIntegralChecked sr)
                                levelCount
        in  -- TODO: monkey-style allocation does not currently work as
            -- expected, so it is disabled for now.
            error "boomFilterAllocForLevel: monkey allocation temporarily disabled" $
            case allocPerLevel !? (l - 1) of
              -- Default to an empty bloom filter in case the level wasn't
              -- accounted for. See 'AllocMonkey'.
              Nothing     -> RunAllocMonkey 0
              Just (_, x) -> RunAllocMonkey (fromIntegralChecked x)
  where
    -- Copied from "Data.List"
    {-# INLINABLE (!?) #-}
    (!?) :: [a] -> Int -> Maybe a
    xs !? n
      | n < 0     = Nothing
      | otherwise = foldr (\x r k -> case k of
                                      0 -> Just x
                                      _ -> r (k-1)) (const Nothing) xs n

{-------------------------------------------------------------------------------
  Fence pointer index
-------------------------------------------------------------------------------}

-- | Configure the type of fence pointer index.
data FencePointerIndex =
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
    -- Use 'Database.LSMTree.Internal.Serialise.Class.serialiseKeyMinimalSize'
    -- to test this law.
    CompactIndex
    -- | Use an ordinary fence pointer index
    --
    -- Ordinary indexes do not have any constraints on keys other than that
    -- their serialised forms may not be 64 KiB or more in size.
  | OrdinaryIndex
  deriving stock (Show, Eq)

instance NFData FencePointerIndex where
  rnf CompactIndex  = ()
  rnf OrdinaryIndex = ()

indexTypeForRun :: FencePointerIndex -> IndexType
indexTypeForRun CompactIndex  = Compact
indexTypeForRun OrdinaryIndex = Ordinary

{-------------------------------------------------------------------------------
  Disk cache policy
-------------------------------------------------------------------------------}

-- | The policy for caching data from disk in memory (using the OS page cache).
--
-- Caching data in memory can improve performance if the access pattern has
-- good access locality or if the overall data size fits within memory. On the
-- other hand, caching is determental to performance and wastes memory if the
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
  deriving stock (Eq, Show)

instance NFData DiskCachePolicy where
  rnf DiskCacheAll                 = ()
  rnf (DiskCacheLevelsAtOrBelow l) = rnf l
  rnf DiskCacheNone                = ()

-- | Interpret the 'DiskCachePolicy' for a level: should we cache data in runs
-- at this level.
--
diskCachePolicyForLevel :: DiskCachePolicy -> LevelNo -> RunDataCaching
diskCachePolicyForLevel policy (LevelNo ln) =
  case policy of
    DiskCacheAll               -> CacheRunData
    DiskCacheNone              -> NoCacheRunData
    DiskCacheLevelsAtOrBelow n
      | ln <= n                -> CacheRunData
      | otherwise              -> NoCacheRunData


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

-- | The default 'MergeSchedule'.
--
-- >>> defaultMergeSchedule
-- Incremental
defaultMergeSchedule :: MergeSchedule
defaultMergeSchedule = Incremental
