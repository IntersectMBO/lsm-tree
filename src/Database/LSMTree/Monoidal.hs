-- TODO: remove once the API is implemented.
{-# OPTIONS_GHC -Wno-redundant-constraints #-}

-- | On disk key-value tables, implemented as Log Structured Merge (LSM) trees.
--
-- This module is the API for \"monoidal\" tables, as opposed to \"normal\"
-- tables (that do not support monoidal updates and unions).
--
-- Key features:
--
-- * Basic key\/value operations: lookup, insert, delete
-- * Monoidal operations: mupsert
-- * Merging of tables
-- * Range lookups by key or key prefix
-- * On-disk durability is /only/ via named snapshots: ordinary operations
--   are otherwise not durable
-- * Opening tables from previous named snapshots
-- * Full persistent data structure by cheap table duplication: all duplicate
--   tables can be both accessed and modified
-- * High performance lookups on SSDs by I\/O batching and concurrency
--
-- This module is intended to be imported qualified.
--
-- > import qualified Database.LSMTree.Monoidal as LSMT
--
module Database.LSMTree.Monoidal (
    -- * Exceptions
    Common.LSMTreeError (..)

    -- * Table sessions
  , Session
  , withSession
  , openSession
  , closeSession

    -- * Table handles
  , TableHandle
  , Internal.TableConfig (..)
  , Internal.defaultTableConfig
  , Internal.SizeRatio (..)
  , Internal.MergePolicy (..)
  , Internal.WriteBufferAlloc (..)
  , Internal.NumEntries (..)
  , Internal.BloomFilterAlloc (..)
  , Internal.defaultBloomFilterAlloc
  , Internal.FencePointerIndex (..)
  , Internal.DiskCachePolicy (..)
  , withTable
  , new
  , close
    -- ** Resource management
    -- $resource-management

    -- ** Exception safety
    -- $exception-safety

    -- * Table queries and updates
    -- ** Queries
  , lookups
  , LookupResult (..)
  , rangeLookup
  , Range (..)
  , RangeLookupResult (..)
    -- ** Updates
  , inserts
  , deletes
  , mupserts
  , updates
  , Update (..)

    -- * Durability (snapshots)
  , SnapshotName
  , Common.mkSnapshotName
  , Common.SnapshotLabel
  , Common.Labellable (..)
  , snapshot
  , open
  , Internal.TableConfigOverride
  , Internal.configNoOverride
  , Internal.configOverrideDiskCachePolicy
  , deleteSnapshot
  , listSnapshots

    -- * Persistence
  , duplicate

    -- * Merging tables
  , merge

    -- * Concurrency
    -- $concurrency

    -- * Serialisation
  , SerialiseKey
  , SerialiseValue

    -- * Monoidal value resolution
  , ResolveValue (..)
  , resolveDeserialised
    -- ** Properties
  , resolveValueValidOutput
  , resolveValueTotality
  , resolveValueAssociativity

    -- * Utility types
  , IOLike
  ) where

import           Control.DeepSeq
import           Control.Monad (void, (<$!>))
import           Data.Bifunctor (Bifunctor (..))
import           Data.Coerce (coerce)
import           Data.Kind (Type)
import           Data.Typeable (Proxy (Proxy), Typeable)
import qualified Data.Vector as V
import           Data.Word (Word64)
import           Database.LSMTree.Common (IOLike, Range (..), SerialiseKey,
                     SerialiseValue (..), Session (..), SnapshotName,
                     closeSession, deleteSnapshot, listSnapshots, openSession,
                     withSession)
import qualified Database.LSMTree.Common as Common
import qualified Database.LSMTree.Internal as Internal
import qualified Database.LSMTree.Internal.Entry as Entry
import           Database.LSMTree.Internal.Monoidal
import           Database.LSMTree.Internal.RawBytes (RawBytes)
import qualified Database.LSMTree.Internal.Serialise as Internal
import qualified Database.LSMTree.Internal.Vector as V

-- $resource-management
-- See "Database.LSMTree.Normal#g:resource"

-- $exception-safety
-- See "Database.LSMTree.Normal#g:exception"

-- $concurrency
-- See "Database.LSMTree.Normal#g:concurrency"

{-------------------------------------------------------------------------------
  Tables
-------------------------------------------------------------------------------}

-- | A handle to an on-disk key\/value table.
--
-- An LSMT table is an individual key value mapping with in-memory and on-disk
-- parts. A table handle is the object\/reference by which an in-use LSM table
-- will be operated upon. In this API it identifies a single mutable instance of
-- an LSM table. The multiple-handles feature allows for there to may be many
-- such instances in use at once.
type TableHandle :: (Type -> Type) -> Type -> Type -> Type
data TableHandle m k v = forall h. Typeable h =>
    TableHandle !(Internal.TableHandle m h)

instance NFData (TableHandle m k v) where
  rnf (TableHandle th) = rnf th

{-# SPECIALISE withTable :: Session IO -> Internal.TableConfig -> (TableHandle IO k v -> IO a) -> IO a #-}
-- | (Asynchronous) exception-safe, bracketed opening and closing of a table.
--
-- If possible, it is recommended to use this function instead of 'new' and
-- 'close'.
withTable ::
     IOLike m
  => Session m
  -> Internal.TableConfig
  -> (TableHandle m k v -> m a)
  -> m a
withTable (Session sesh) conf action =
    Internal.withTable sesh conf $
      action . TableHandle

{-# SPECIALISE new :: Session IO -> Internal.TableConfig -> IO (TableHandle IO k v) #-}
-- | Create a new empty table, returning a fresh table handle.
--
-- NOTE: table handles hold open resources (such as open files) and should be
-- closed using 'close' as soon as they are no longer used.
--
new ::
     IOLike m
  => Session m
  -> Internal.TableConfig
  -> m (TableHandle m k v)
new (Session sesh) conf = TableHandle <$> Internal.new sesh conf

{-# SPECIALISE close :: TableHandle IO k v -> IO () #-}
-- | Close a table handle. 'close' is idempotent. All operations on a closed
-- handle will throw an exception.
--
-- Any on-disk files and in-memory data that are no longer referenced after
-- closing the table handle are lost forever. Use 'Snapshot's to ensure data is
-- not lost.
close ::
     IOLike m
  => TableHandle m k v
  -> m ()
close (TableHandle th) = Internal.close th

{-------------------------------------------------------------------------------
  Table querying and updates
-------------------------------------------------------------------------------}

{-# SPECIALISE lookups :: (SerialiseKey k, SerialiseValue v, ResolveValue v) => V.Vector k -> TableHandle IO k v -> IO (V.Vector (LookupResult v)) #-}
-- | Perform a batch of lookups.
--
-- Lookups can be performed concurrently from multiple Haskell threads.
lookups ::
     forall m k v. (IOLike m, SerialiseKey k, SerialiseValue v, ResolveValue v)
  => V.Vector k
  -> TableHandle m k v
  -> m (V.Vector (LookupResult v))
lookups ks (TableHandle th) =
    V.mapStrict (fmap Internal.deserialiseValue) <$!>
    Internal.lookups
      (resolve @v Proxy)
      (V.map Internal.serialiseKey ks)
      th
      toMonoidalLookupResult

toMonoidalLookupResult :: Maybe (Entry.Entry v b) -> LookupResult v
toMonoidalLookupResult = \case
    Just e -> case e of
      Entry.Insert v           -> Found v
      Entry.InsertWithBlob _ _ -> error "toMonoidalLookupResult: InsertWithBlob unexpected"
      Entry.Mupdate v          -> Found v
      Entry.Delete             -> NotFound
    Nothing -> NotFound

{-# SPECIALISE rangeLookup :: (SerialiseKey k, SerialiseValue v, ResolveValue v) => Range k -> TableHandle IO k v -> IO (V.Vector (RangeLookupResult k v)) #-}
-- | Perform a range lookup.
--
-- Range lookups can be performed concurrently from multiple Haskell threads.
rangeLookup ::
     (IOLike m, SerialiseKey k, SerialiseValue v, ResolveValue v)
  => Range k
  -> TableHandle m k v
  -> m (V.Vector (RangeLookupResult k v))
rangeLookup = undefined

{-# SPECIALISE updates :: (SerialiseKey k, SerialiseValue v, ResolveValue v) => V.Vector (k, Update v) -> TableHandle IO k v -> IO () #-}
-- | Perform a mixed batch of inserts, deletes and monoidal upserts.
--
-- If there are duplicate keys in the same batch, then keys nearer to the front
-- of the vector take precedence.
--
-- Updates can be performed concurrently from multiple Haskell threads.
updates ::
     forall m k v. (IOLike m, SerialiseKey k, SerialiseValue v, ResolveValue v)
  => V.Vector (k, Update v)
  -> TableHandle m k v
  -> m ()
updates es (TableHandle th) = do
    Internal.updates
      (resolve @v Proxy)
      (V.mapStrict serialiseEntry es)
      th
  where
    serialiseEntry = bimap Internal.serialiseKey serialiseOp
    serialiseOp = first Internal.serialiseValue . Entry.updateToEntryMonoidal

{-# SPECIALISE inserts :: (SerialiseKey k, SerialiseValue v, ResolveValue v) => V.Vector (k, v) -> TableHandle IO k v -> IO () #-}
-- | Perform a batch of inserts.
--
-- Inserts can be performed concurrently from multiple Haskell threads.
inserts ::
     (IOLike m, SerialiseKey k, SerialiseValue v, ResolveValue v)
  => V.Vector (k, v)
  -> TableHandle m k v
  -> m ()
inserts = updates . fmap (second Insert)

{-# SPECIALISE deletes :: (SerialiseKey k, SerialiseValue v, ResolveValue v) => V.Vector k -> TableHandle IO k v -> IO () #-}
-- | Perform a batch of deletes.
--
-- Deletes can be performed concurrently from multiple Haskell threads.
deletes ::
     (IOLike m, SerialiseKey k, SerialiseValue v, ResolveValue v)
  => V.Vector k
  -> TableHandle m k v
  -> m ()
deletes = updates . fmap (,Delete)

{-# SPECIALISE mupserts :: (SerialiseKey k, SerialiseValue v, ResolveValue v) => V.Vector (k, v) -> TableHandle IO k v -> IO () #-}
-- | Perform a batch of monoidal upserts.
--
-- Monoidal upserts can be performed concurrently from multiple Haskell threads.
mupserts ::
     (IOLike m, SerialiseKey k, SerialiseValue v, ResolveValue v)
  => V.Vector (k, v)
  -> TableHandle m k v
  -> m ()
mupserts = updates . fmap (second Mupsert)

{-------------------------------------------------------------------------------
  Snapshots
-------------------------------------------------------------------------------}

{-# SPECIALISE snapshot :: (SerialiseKey k, SerialiseValue v, ResolveValue v, Common.Labellable (k, v)) => SnapshotName -> TableHandle IO k v -> IO () #-}
-- | Make the current value of a table durable on-disk by taking a snapshot and
-- giving the snapshot a name. This is the __only__ mechanism to make a table
-- durable -- ordinary insert\/delete operations are otherwise not preserved.
--
-- Snapshots have names and the table may be opened later using 'open' via that
-- name. Names are strings and the management of the names is up to the user of
-- the library.
--
-- The names correspond to disk files, which imposes some constraints on length
-- and what characters can be used.
--
-- Snapshotting does not close the table handle.
--
-- Taking a snapshot is /relatively/ cheap, but it is not so cheap that one can
-- use it after every operation. In the implementation, it must at least flush
-- the write buffer to disk.
--
-- Concurrency:
--
-- * It is safe to concurrently make snapshots from any table, provided that
--   the snapshot names are distinct (otherwise this would be a race).
--
snapshot ::
     forall m k v. ( IOLike m
     , SerialiseKey k, SerialiseValue v, ResolveValue v
     , Common.Labellable (k, v)
     )
  => SnapshotName
  -> TableHandle m k v
  -> m ()
snapshot snap (TableHandle th) =
    void $ Internal.snapshot (resolve @v Proxy) snap label th
  where
    -- to ensure we don't open a monoidal table as normal later
    label = Common.makeSnapshotLabel (Proxy @(k, v)) <> " (monoidal)"

{-# SPECIALISE open :: (SerialiseKey k, SerialiseValue v, Common.Labellable (k, v)) => Session IO -> Internal.TableConfigOverride -> SnapshotName -> IO (TableHandle IO k v) #-}
-- | Open a table from a named snapshot, returning a new table handle.
--
-- NOTE: close table handles using 'close' as soon as they are
-- unused.
--
-- Exceptions:
--
-- * Opening a non-existent snapshot is an error.
--
-- * Opening a snapshot but expecting the wrong type of table is an error. e.g.,
--   the following will fail:
--
-- @
-- example session = do
--   th <- 'new' \@IO \@Int \@Int \@Int session _
--   'snapshot' "intTable" th
--   'open' \@IO \@Bool \@Bool \@Bool session "intTable"
-- @
--
-- TOREMOVE: before snapshots are implemented, the snapshot name should be ignored.
-- Instead, this function should open a table handle from files that exist in
-- the session's directory.
open ::
     forall m k v. ( IOLike m
     , SerialiseKey k, SerialiseValue v
     , Common.Labellable (k, v)
     )
  => Session m
  -> Internal.TableConfigOverride -- ^ Optional config override
  -> SnapshotName
  -> m (TableHandle m k v)
open (Session sesh) override snap =
    TableHandle <$> Internal.open sesh label override snap
  where
    -- to ensure that the table is really a monoidal table
    label = Common.makeSnapshotLabel (Proxy @(k, v)) <> " (monoidal)"

{-------------------------------------------------------------------------------
  Mutiple writable table handles
-------------------------------------------------------------------------------}

{-# SPECIALISE duplicate :: TableHandle IO k v -> IO (TableHandle IO k v) #-}
-- | Create a logically independent duplicate of a table handle. This returns a
-- new table handle.
--
-- A table handle and its duplicate are logically independent: changes to one
-- are not visible to the other. However, in-memory and on-disk data are
-- shared internally.
--
-- This operation enables /fully persistent/ use of tables by duplicating the
-- table prior to a batch of mutating operations. The duplicate retains the
-- original table value, and can still be modified independently.
--
-- This is persistence in the sense of persistent data structures (not of on-disk
-- persistence). The usual definition of a persistent data structure is one in
-- which each operation preserves the previous version of the structure when
-- the structure is modified. Full persistence is if every version can be both
-- accessed and modified. This API does not directly fit the definition because
-- the update operations do mutate the table value, however full persistence
-- can be emulated by duplicating the table prior to a mutating operation.
--
-- Duplication itself is cheap. In particular it requires no disk I\/O, and
-- requires little additional memory. Just as with normal persistent data
-- structures, making use of multiple tables will have corresponding costs in
-- terms of memory and disk space. Initially the two tables will share
-- everything (both in memory and on disk) but as more and more update
-- operations are performed on each, the sharing will decrease. Ultimately the
-- memory and disk cost will be the same as if each table were entirely
-- independent.
--
-- NOTE: duplication create a new table handle, which should be closed when no
-- longer needed.
--
duplicate ::
     IOLike m
  => TableHandle m k v
  -> m (TableHandle m k v)
duplicate (TableHandle th) = TableHandle <$> Internal.duplicate th

{-------------------------------------------------------------------------------
  Merging tables
-------------------------------------------------------------------------------}

{-# SPECIALISE merge :: ResolveValue v => TableHandle IO k v -> TableHandle IO k v -> IO (TableHandle IO k v) #-}
-- | Merge full tables, creating a new table handle.
--
-- Multiple tables of the same type but with different configuration parameters
-- can live in the same session. However, 'merge' only works for tables that
-- have the same key\/value types and configuration parameters.
--
-- NOTE: merging table handles creates a new table handle, but does not close
-- the table handles that were used as inputs.
merge ::
     (IOLike m, ResolveValue v)
  => TableHandle m k v
  -> TableHandle m k v
  -> m (TableHandle m k v)
merge = undefined

{-------------------------------------------------------------------------------
  Monoidal value resolution
-------------------------------------------------------------------------------}

-- | A class to specify how to resolve/merge values when using monoidal updates
-- (mupserts). This is required for merging entries during compaction and also
-- for doing lookups, to resolve multiple entries of the same key on the fly.
-- The class has some laws, which should be tested (e.g. with QuickCheck).
--
-- Prerequisites:
--
-- * [Valid Output] The result of resolution should always be deserialisable.
--   See 'resolveValueValidOutput'.
-- * [Associativity] Resolving values should be associative.
--   See 'resolveValueAssociativity'.
-- * [Totality] For any input 'RawBytes', resolution should successfully provide
--   a result. This is a pretty strong requirement. Usually it is sufficient to
--   handle input produced by 'serialiseValue' and 'resolveValue' (which are
--   are required to be deserialisable by 'deserialiseValue'),
--   but this makes sure no error occurs in the middle of compaction, which
--   could lead to corruption.
--   See 'resolveValueTotality'.
--
-- TODO: Revisit Totality. How are errors handled during run merging?
--
-- Future opportunities for optimisations:
--
-- * Include a function that determines whether it is safe to remove an 'Update'
--   from the last level of an LSM tree.
--
-- * Include a function @v -> RawBytes -> RawBytes@, which can then be used when
--   inserting into the write buffer. Currently, using 'resolveDeserialised'
--   means that the inserted value is serialised and (if there is another value
--   with the same key in the write buffer) immediately deserialised again.
--
-- TODO: Should this go into @Internal.Monoidal@ or @Internal.ResolveValue@?
-- TODO: The laws depend on 'SerialiseValue', should we make it a superclass?
class ResolveValue v where
  resolveValue :: Proxy v -> RawBytes -> RawBytes -> RawBytes

-- | Test the __Valid Output__ law for the 'ResolveValue' class
resolveValueValidOutput ::
     forall v. (SerialiseValue v, ResolveValue v, NFData v)
  => v -> v -> Bool
resolveValueValidOutput (serialiseValue -> x) (serialiseValue -> y) =
    (deserialiseValue (resolveValue (Proxy @v) x y) :: v) `deepseq` True

-- | Test the __Associativity__ law for the 'ResolveValue' class
resolveValueAssociativity ::
     forall v. (SerialiseValue v, ResolveValue v)
  => v -> v -> v -> Bool
resolveValueAssociativity (serialiseValue -> x) (serialiseValue -> y) (serialiseValue -> z) =
    x <+> (y <+> z) == (x <+> y) <+> z
  where
    (<+>) = resolveValue (Proxy @v)

-- | Test the __Totality__ law for the 'ResolveValue' class
resolveValueTotality ::
     forall v. ResolveValue v
  => Proxy v -> RawBytes -> RawBytes -> Bool
resolveValueTotality _ x y =
    resolveValue (Proxy @v) x y `deepseq` True

-- | A helper function to implement 'resolveValue' by operating on the
-- deserialised representation. Note that especially for simple types it
-- should be possible to provide a more efficient implementation by directly
-- operating on the 'RawBytes'.
--
-- This function could potentially be used to provide a default implementation
-- for 'resolveValue', but it's probably best to be explicit about instances.
--
-- To satisfy the prerequisites of 'ResolveValue', the function provided to
-- 'resolveDeserialised' should itself satisfy some properties.
--
-- Prerequisites:
--
-- * [Associativity] The provided function should be associative.
-- * [Total Resolution] The provided function should be total.
-- * [Total Deserialisation] 'deserialiseValue' for @v@ should handle any input
--   'RawBytes'.
resolveDeserialised ::
     SerialiseValue v
  => (v -> v -> v) -> Proxy v -> RawBytes -> RawBytes -> RawBytes
resolveDeserialised f _ x y =
    serialiseValue (f (deserialiseValue x) (deserialiseValue y))

resolve :: ResolveValue v => Proxy v -> Internal.ResolveSerialisedValue
resolve = coerce . resolveValue

-- | Mostly to give an example instance (plus the property tests for it).
instance ResolveValue Word64 where
  resolveValue = resolveDeserialised (+)
