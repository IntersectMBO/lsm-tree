{-# LANGUAGE GADTs                    #-}
{-# LANGUAGE StandaloneDeriving       #-}
{-# LANGUAGE StandaloneKindSignatures #-}
{-# LANGUAGE TupleSections            #-}

-- TODO: remove once the API is implemented.
{-# OPTIONS_GHC -Wno-redundant-constraints #-}
{-# OPTIONS_GHC -Wno-unused-top-binds #-}

-- | On disk key-value tables, implemented as Log Structured Merge (LSM) trees.
--
-- This module is the API for \"normal\" tables, as opposed to \"monoidal\"
-- tables (that support monoidal updates and unions).
--
-- Key features:
--
-- * Basic key\/value operations: lookup, insert, delete
-- * Range lookups by key or key prefix
-- * Support for BLOBs: large auxilliary values associated with a key
-- * On-disk durability is /only/ via named snapshots: ordinary operations
--   are otherwise not durable
-- * Opening tables from previous named snapshots
-- * Full persistent data structure by cheap table duplication: all duplicate
--   tables can be both accessed and modified
-- * High performance lookups on SSDs by I\/O batching and concurrency
--
-- This module is intended to be imported qualified.
--
-- > import qualified Database.LSMTree.Normal as LSMT
--
module Database.LSMTree.Normal (

    -- * Table sessions
    Session
  , withSession
  , openSession
  , closeSession

    -- * Table handles
  , TableHandle
  , Internal.TableConfig (..)
  , Internal.MergePolicy (..)
  , Internal.SizeRatio (..)
  , Internal.BloomFilterAlloc (..)
  , withTable
  , new
  , close
    -- ** Resource management #resource#
    -- $resource-management

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
  , updates
  , Update (..)
    -- ** Blobs
  , BlobRef
  , retrieveBlobs

    -- * Durability (snapshots)
  , SnapshotName
  , snapshot
  , open
  , deleteSnapshot
  , listSnapshots

    -- * Persistence
  , duplicate

    -- * Concurrency #concurrency#
    -- $concurrency

    -- * Serialisation
  , SerialiseKey
  , SerialiseValue

    -- * Utility types
  , IOLike
  ) where

import           Control.Monad
import           Data.Bifunctor (Bifunctor (..))
import           Data.Kind (Type)
import           Data.Typeable (Typeable)
import qualified Data.Vector as V
import           Database.LSMTree.Common (BlobRef (BlobRef), IOLike, Range (..),
                     SerialiseKey, SerialiseValue, Session (..), SnapshotName,
                     closeSession, deleteSnapshot, listSnapshots, openSession,
                     withSession)
import qualified Database.LSMTree.Internal as Internal
import           Database.LSMTree.Internal.Normal
import qualified Database.LSMTree.Internal.Serialise as Internal
import qualified Database.LSMTree.Internal.Vector as V

-- $resource-management
-- Table handles use resources and as such need to be managed. In particular
-- handles retain memory (for indexes, Bloom filters and write buffers) and
-- hold open multiple file handles.
--
-- The resource management style that this library uses is explicit management,
-- with backup from automatic management. Explicit management is required to
-- enable prompt release of resources. Explicit management means using 'close'
-- on 'TableHandle's when they are no longer needed. The backup management
-- relies on GC finalisers and thus is not guaranteed to be prompt.
--
-- In particular, certain operations create new table handles:
--
-- * 'new'
-- * 'open'
-- * 'duplicate'
--
-- These ones must be paired with a corresponding 'close'.

-- $concurrency
-- Table handles are mutable objects and as such applications should restrict
-- their concurrent use to avoid races.
--
-- It is a reasonable mental model to think of a 'TableHandle' as being like a
-- @IORef (Map k v)@ (though without any equivalent of @atomicModifyIORef@).
--
-- The rules are:
--
-- * It is a race to read and modify the same table concurrently.
-- * It is a race to modify and modify the same table concurrently.
-- * No concurrent table operations create a /happens-before/ relation.
-- * All synchronisation needs to occur using other concurrency constructs.
--
-- * It is /not/ a race to read and read the same table concurrently.
-- * It is /not/ a race to read or modify /separate/ tables concurrently.
--
-- We can classify all table operations as \"read\" or \"modify\" for the
-- purpose of the rules above. The read operations are:
--
-- * 'lookups'
-- * 'rangeLookup'
-- * 'retrieveBlobs'
-- * 'snapshot'
-- * 'duplicate'
--
-- The write operations are:
--
-- * 'inserts'
-- * 'deletes'
-- * 'updates'
-- * 'close'
--
-- In particular it is possible to read a stable view of a table while
-- concurrently modifying it: 'duplicate' the table handle first and then
-- perform reads on the duplicate, while modifying the original handle. Note
-- however that it would still be a race to 'duplicate' concurrently with
-- modifications: the duplication must /happen before/ subsequent modifications.
--
-- It safe to read a table (using 'lookups' or 'rangeLookup') concurrently, and
-- doing so can take advantage of CPU and I\/O parallelism, and thus may
-- improve throughput.


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
type TableHandle :: (Type -> Type) -> Type -> Type -> Type -> Type
data TableHandle m k v blob = forall h. Typeable h =>
    TableHandle !(Internal.TableHandle m h)

{-# SPECIALISE withTable :: Session IO -> Internal.TableConfig -> (TableHandle IO k v blob -> IO a) -> IO a #-}
-- | (Asynchronous) exception-safe, bracketed opening and closing of a table.
--
-- If possible, it is recommended to use this function instead of 'new' and
-- 'close'.
withTable ::
     IOLike m
  => Session m
  -> Internal.TableConfig
  -> (TableHandle m k v blob -> m a)
  -> m a
withTable (Session sesh) conf action = Internal.withTable sesh conf (action . TableHandle)

{-# SPECIALISE new :: Session IO -> Internal.TableConfig -> IO (TableHandle IO k v blob) #-}
-- | Create a new empty table, returning a fresh table handle.
--
-- NOTE: table handles hold open resources (such as open files) and should be
-- closed using 'close' as soon as they are no longer used.
--
new ::
     IOLike m
  => Session m
  -> Internal.TableConfig
  -> m (TableHandle m k v blob)
new (Session sesh) conf = TableHandle <$> Internal.new sesh conf

{-# SPECIALISE close :: TableHandle IO k v blob -> IO () #-}
-- | Close a table handle. 'close' is idempotent. All operations on a closed
-- handle will throw an exception.
--
-- Any on-disk files and in-memory data that are no longer referenced after
-- closing the table handle are lost forever. Use 'Snapshot's to ensure data is
-- not lost.
close ::
     IOLike m
  => TableHandle m k v blob
  -> m ()
close (TableHandle th) = Internal.close th

{-------------------------------------------------------------------------------
  Table querying and updates
-------------------------------------------------------------------------------}

{-# SPECIALISE lookups :: (SerialiseKey k, SerialiseValue v) => V.Vector k -> TableHandle IO k v blob -> IO (V.Vector (LookupResult v (BlobRef IO blob))) #-}
-- | Perform a batch of lookups.
--
-- Lookups can be performed concurrently from multiple Haskell threads.
lookups ::
     (IOLike m, SerialiseKey k, SerialiseValue v)
  => V.Vector k
  -> TableHandle m k v blob
  -> m (V.Vector (LookupResult v (BlobRef m blob)))
lookups ks (TableHandle th) =
    V.mapStrict (bimap Internal.deserialiseValue BlobRef) <$!>
    Internal.lookups (V.map Internal.serialiseKey ks) th Internal.toNormalLookupResult

-- | Perform a range lookup.
--
-- Range lookups can be performed concurrently from multiple Haskell threads.
rangeLookup ::
     (IOLike m, SerialiseKey k, SerialiseValue v)
  => Range k
  -> TableHandle m k v blob
  -> m (V.Vector (RangeLookupResult k v (BlobRef m blob)))
rangeLookup = undefined

-- | Perform a mixed batch of inserts and deletes.
--
-- Updates can be performed concurrently from multiple Haskell threads.
updates ::
     (IOLike m, SerialiseKey k, SerialiseValue v, SerialiseValue blob)
  => V.Vector (k, Update v blob)
  -> TableHandle m k v blob
  -> m ()
updates es (TableHandle th) =
    Internal.updatesNormal (V.map serialiseEntry es) th
  where
    serialiseEntry =
      bimap
        Internal.serialiseKey
        (bimap Internal.serialiseValue Internal.serialiseBlob)

-- | Perform a batch of inserts.
--
-- Inserts can be performed concurrently from multiple Haskell threads.
inserts ::
     (IOLike m, SerialiseKey k, SerialiseValue v, SerialiseValue blob)
  => V.Vector (k, v, Maybe blob)
  -> TableHandle m k v blob
  -> m ()
inserts = updates . fmap (\(k, v, blob) -> (k, Insert v blob))

-- | Perform a batch of deletes.
--
-- Deletes can be performed concurrently from multiple Haskell threads.
deletes ::
     (IOLike m, SerialiseKey k, SerialiseValue v, SerialiseValue blob)
  => V.Vector k
  -> TableHandle m k v blob
  -> m ()
deletes = updates . fmap (,Delete)

-- | Perform a batch of blob retrievals.
--
-- This is a separate step from 'lookups' and 'rangeLookups'. The result of a
-- lookup can include a 'BlobRef', which can be used to retrieve the actual
-- 'Blob'.
--
-- Blob lookups can be performed concurrently from multiple Haskell threads.
retrieveBlobs ::
     (IOLike m, SerialiseValue blob)
  => Session m
  -> V.Vector (BlobRef m blob)
  -> m (V.Vector blob)
retrieveBlobs = undefined

{-------------------------------------------------------------------------------
  Snapshots
-------------------------------------------------------------------------------}

{-# SPECIALISE snapshot :: (SerialiseKey k, SerialiseValue v, SerialiseValue blob) => SnapshotName -> TableHandle IO k v blob -> IO () #-}
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
-- TODO: this function currently has a temporary implementation until we have
-- proper snapshots. The temporary snapshot consists of a small file in the
-- snapshots directory that lists: (i) the runs that are used by a specific
-- snapshot, and (ii) its table configuration. We don't have to copy run files
-- as part of the snapshot because run files aren't (yet) deleted when runs are
-- closed. The write buffer is also persisted to disk as part of the snapshot,
-- but the original table remains unchanged.
snapshot ::
     (IOLike m, SerialiseKey k, SerialiseValue v, SerialiseValue blob)
  => SnapshotName
  -> TableHandle m k v blob
  -> m ()
snapshot snap (TableHandle th) = void $ Internal.snapshot snap th

{-# SPECIALISE open :: (SerialiseKey k, SerialiseValue v, SerialiseValue blob) => Session IO -> SnapshotName -> IO (TableHandle IO k v blob ) #-}
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
-- TODO: this function currently has a temporary implementation until we have
-- proper snapshots. See 'snapshot'.
open ::
     (IOLike m, SerialiseKey k, SerialiseValue v, SerialiseValue blob)
  => Session m
  -> SnapshotName
  -> m (TableHandle m k v blob)
open (Session sesh) snap = TableHandle <$> Internal.open sesh snap

{-------------------------------------------------------------------------------
  Mutiple writable table handles
-------------------------------------------------------------------------------}

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
  => TableHandle m k v blob
  -> m (TableHandle m k v blob)
duplicate = undefined
