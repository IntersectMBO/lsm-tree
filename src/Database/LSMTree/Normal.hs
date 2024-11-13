{-# LANGUAGE OverloadedStrings #-}

-- TODO: remove once the API is implemented.
{-# OPTIONS_GHC -Wno-redundant-constraints #-}

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
    -- * Exceptions
    Common.LSMTreeError (..)

    -- * Tracing
  , Common.LSMTreeTrace (..)
  , Common.TableTrace (..)
  , Common.MergeTrace (..)

    -- * Table sessions
  , Session
  , withSession
  , openSession
  , closeSession

    -- * Table
  , Table
  , Common.TableConfig (..)
  , Common.defaultTableConfig
  , Common.SizeRatio (..)
  , Common.MergePolicy (..)
  , Common.WriteBufferAlloc (..)
  , Common.NumEntries (..)
  , Common.BloomFilterAlloc (..)
  , Common.defaultBloomFilterAlloc
  , Common.FencePointerIndex (..)
  , Common.DiskCachePolicy (..)
  , Common.MergeSchedule (..)
  , Common.defaultMergeSchedule
  , withTable
  , new
  , close
    -- ** Resource management #resource#
    -- $resource-management

    -- ** Exception safety #exception#
    -- $exception-safety

    -- * Table queries and updates
    -- ** Queries
  , lookups
  , LookupResult (..)
  , rangeLookup
  , Range (..)
  , QueryResult (..)
    -- ** Cursor
  , Cursor
  , withCursor
  , withCursorAtOffset
  , newCursor
  , newCursorAtOffset
  , closeCursor
  , readCursor
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
  , Common.mkSnapshotName
  , Common.Labellable (..)
  , createSnapshot
  , openSnapshot
  , Common.TableConfigOverride
  , Common.configNoOverride
  , Common.configOverrideDiskCachePolicy
  , deleteSnapshot
  , listSnapshots

    -- * Persistence
  , duplicate

    -- * Table union
  , union

    -- * Concurrency #concurrency#
    -- $concurrency

    -- * Serialisation
  , SerialiseKey
  , SerialiseValue

    -- * Utility types
  , IOLike
  ) where

import           Control.DeepSeq
import           Control.Exception (throw)
import           Control.Monad
import           Data.Bifunctor (Bifunctor (..))
import           Data.Kind (Type)
import           Data.Typeable (Proxy (..), eqT, type (:~:) (Refl))
import qualified Data.Vector as V
import           Database.LSMTree.Common (BlobRef (BlobRef), IOLike, Range (..),
                     SerialiseKey, SerialiseValue, Session, SnapshotName,
                     closeSession, deleteSnapshot, listSnapshots, openSession,
                     withSession)
import qualified Database.LSMTree.Common as Common
import qualified Database.LSMTree.Internal as Internal
import qualified Database.LSMTree.Internal.BlobRef as Internal
import qualified Database.LSMTree.Internal.Entry as Entry
import qualified Database.LSMTree.Internal.Serialise as Internal
import qualified Database.LSMTree.Internal.Snapshot as Internal
import qualified Database.LSMTree.Internal.Vector as V

-- $resource-management
-- Sessions, tables and cursors use resources and as such need to be
-- managed. In particular they retain memory (for indexes, Bloom filters and
-- write buffers) and hold open multiple file handles.
--
-- The resource management style that this library uses is explicit management,
-- with backup from automatic management. Explicit management is required to
-- enable prompt release of resources. Explicit management means using 'close'
-- on 'Table's when they are no longer needed, for example.
-- The backup management relies on GC finalisers and thus is not guaranteed to
-- be prompt.
--
-- In particular, certain operations create new resources:
--
-- * 'openSession'
-- * 'new'
-- * 'openSnapshot'
-- * 'duplicate'
-- * 'newCursor'
--
-- These ones must be paired with a corresponding 'closeSesstion', 'close' or
-- 'closeCursor'.
--

-- $exception-safety
--
-- To prevent resource/memory leaks in the presence of asynchronous exceptions,
-- it is recommended to:
--
-- 1. Run resource-allocating functions with asynchronous exceptions masked.
-- 2. Pair a resource-allocating function with a masked cleanup function, for
-- example using 'bracket'.
--
-- These function pairs include:
--
-- * 'openSession', paired with 'closeSession'
-- * 'new', paired with 'close'
-- * 'openSnapshot', paired with 'close'
-- * 'duplicate', paired with 'close'
-- * 'newCursor', paired with 'closeCursor'
--
-- Bracket-style @with*@ functions are also provided by the library, such as
-- 'withTable'.

-- $concurrency
-- Table are mutable objects and as such applications should restrict their
-- concurrent use to avoid races.
--
-- It is a reasonable mental model to think of a 'Table' as being like a
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
-- * 'createSnapshot'
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
-- concurrently modifying it: 'duplicate' the table first and then perform reads
-- on the duplicate, while modifying the original table. Note however that it
-- would still be a race to 'duplicate' concurrently with modifications: the
-- duplication must /happen before/ subsequent modifications.
--
-- Similarly, a cursor constitutes a stable view of a table and can safely be
-- read while modifying the original table.
-- However, reading from a cursor will take a lock, so concurrent reads on the
-- same cursor will block until the first one completes. This is due to the
-- cursor position being updated as entries are read.
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
-- parts. A table is the object\/reference by which an in-use LSM table will be
-- operated upon. In this API it identifies a single mutable instance of an LSM
-- table. The duplicate tables feature allows for there to may be many such
-- instances in use at once.
type Table = Internal.NormalTable

{-# SPECIALISE withTable ::
     Session IO
  -> Common.TableConfig
  -> (Table IO k v blob -> IO a)
  -> IO a #-}
-- | (Asynchronous) exception-safe, bracketed opening and closing of a table.
--
-- If possible, it is recommended to use this function instead of 'new' and
-- 'close'.
withTable ::
     IOLike m
  => Session m
  -> Common.TableConfig
  -> (Table m k v blob -> m a)
  -> m a
withTable (Internal.Session' sesh) conf action =
    Internal.withTable sesh conf (action . Internal.NormalTable)

{-# SPECIALISE new ::
     Session IO
  -> Common.TableConfig
  -> IO (Table IO k v blob) #-}
-- | Create a new empty table, returning a fresh table.
--
-- NOTE: tables hold open resources (such as open files) and should be
-- closed using 'close' as soon as they are no longer used.
--
new ::
     IOLike m
  => Session m
  -> Common.TableConfig
  -> m (Table m k v blob)
new (Internal.Session' sesh) conf = Internal.NormalTable <$> Internal.new sesh conf

{-# SPECIALISE close ::
     Table IO k v blob
  -> IO () #-}
-- | Close a table. 'close' is idempotent. All operations on a closed
-- handle will throw an exception.
--
-- Any on-disk files and in-memory data that are no longer referenced after
-- closing the table are lost forever. Use 'Snapshot's to ensure data is
-- not lost.
close ::
     IOLike m
  => Table m k v blob
  -> m ()
close (Internal.NormalTable t) = Internal.close t

{-------------------------------------------------------------------------------
  Table queries
-------------------------------------------------------------------------------}

-- | Result of a single point lookup.
data LookupResult v blobref =
    NotFound
  | Found         !v
  | FoundWithBlob !v !blobref
  deriving stock (Eq, Show, Functor, Foldable, Traversable)

instance Bifunctor LookupResult where
  first f = \case
      NotFound          -> NotFound
      Found v           -> Found (f v)
      FoundWithBlob v b -> FoundWithBlob (f v) b

  second g = \case
      NotFound          -> NotFound
      Found v           -> Found v
      FoundWithBlob v b -> FoundWithBlob v (g b)

{-# SPECIALISE lookups ::
     (SerialiseKey k, SerialiseValue v)
  => Table IO k v blob
  -> V.Vector k
  -> IO (V.Vector (LookupResult v (BlobRef IO blob))) #-}
{-# INLINEABLE lookups #-}
-- | Perform a batch of lookups.
--
-- Lookups can be performed concurrently from multiple Haskell threads.
lookups ::
     ( IOLike m
     , SerialiseKey k
     , SerialiseValue v
     )
  => Table m k v blob
  -> V.Vector k
  -> m (V.Vector (LookupResult v (BlobRef m blob)))
lookups (Internal.NormalTable t) ks =
    V.map toLookupResult <$>
    Internal.lookups const (V.map Internal.serialiseKey ks) t
  where
    toLookupResult (Just e) = case e of
      Entry.Insert v            -> Found (Internal.deserialiseValue v)
      Entry.InsertWithBlob v br -> FoundWithBlob (Internal.deserialiseValue v)
                                                 (BlobRef br)
      Entry.Mupdate _           -> error "Normal.lookups: unexpected Mupdate"
      Entry.Delete              -> NotFound
    toLookupResult Nothing = NotFound

-- | A result for one point in a cursor read or range lookup.
data QueryResult k v blobref =
    FoundInQuery         !k !v
  | FoundInQueryWithBlob !k !v !blobref
  deriving stock (Eq, Show, Functor, Foldable, Traversable)

instance Bifunctor (QueryResult k) where
  bimap f g = \case
      FoundInQuery k v           -> FoundInQuery k (f v)
      FoundInQueryWithBlob k v b -> FoundInQueryWithBlob k (f v) (g b)

{-# SPECIALISE rangeLookup ::
     (SerialiseKey k, SerialiseValue v)
  => Table IO k v blob
  -> Range k
  -> IO (V.Vector (QueryResult k v (BlobRef IO blob))) #-}
-- | Perform a range lookup.
--
-- Range lookups can be performed concurrently from multiple Haskell threads.
rangeLookup ::
     ( IOLike m
     , SerialiseKey k
     , SerialiseValue v
     )
  => Table m k v blob
  -> Range k
  -> m (V.Vector (QueryResult k v (BlobRef m blob)))
rangeLookup (Internal.NormalTable t) range =
    Internal.rangeLookup const (Internal.serialiseKey <$> range) t $ \k v mblob ->
      toNormalQueryResult
        (Internal.deserialiseKey k)
        (Internal.deserialiseValue v)
        (BlobRef <$> mblob)

{-------------------------------------------------------------------------------
  Cursor
-------------------------------------------------------------------------------}

-- | A read-only view into a table.
--
-- A cursor allows reading from a table sequentially (according to serialised
-- key ordering) in an incremental fashion. For example, this allows doing a
-- table scan in small chunks.
--
-- Once a cursor has been created, updates to the referenced table don't affect
-- the cursor.
type Cursor :: (Type -> Type) -> Type -> Type -> Type -> Type
type Cursor = Internal.NormalCursor

{-# SPECIALISE withCursor ::
     Table IO k v blob
  -> (Cursor IO k v blob -> IO a)
  -> IO a #-}
-- | (Asynchronous) exception-safe, bracketed opening and closing of a cursor.
--
-- If possible, it is recommended to use this function instead of 'newCursor'
-- and 'closeCursor'.
withCursor ::
     IOLike m
  => Table m k v blob
  -> (Cursor m k v blob -> m a)
  -> m a
withCursor (Internal.NormalTable t) action =
    Internal.withCursor Internal.NoOffsetKey t (action . Internal.NormalCursor)

{-# SPECIALISE withCursorAtOffset ::
     SerialiseKey k
  => k
  -> Table IO k v blob
  -> (Cursor IO k v blob -> IO a)
  -> IO a #-}
-- | A variant of 'withCursor' that allows initialising the cursor at an offset
-- within the table such that the first entry the cursor returns will be the
-- one with the lowest key that is greater than or equal to the given key.
-- In other words, it uses an inclusive lower bound.
--
-- NOTE: The ordering of the serialised keys will be used, which can lead to
-- unexpected results if the 'SerialiseKey' instance is not order-preserving!
withCursorAtOffset ::
     ( IOLike m
     , SerialiseKey k
     )
  => k
  -> Table m k v blob
  -> (Cursor m k v blob -> m a)
  -> m a
withCursorAtOffset offset (Internal.NormalTable t) action =
    Internal.withCursor (Internal.OffsetKey (Internal.serialiseKey offset)) t $
      action . Internal.NormalCursor

{-# SPECIALISE newCursor ::
     Table IO k v blob
  -> IO (Cursor IO k v blob) #-}
-- | Create a new cursor to read from a given table. Future updates to the table
-- will not be reflected in the cursor. The cursor starts at the beginning, i.e.
-- the minimum key of the table.
--
-- Consider using 'withCursor' instead.
--
-- NOTE: cursors hold open resources (such as open files) and should be closed
-- using 'close' as soon as they are no longer used.
newCursor ::
     IOLike m
  => Table m k v blob
  -> m (Cursor m k v blob)
newCursor (Internal.NormalTable t) =
    Internal.NormalCursor <$!> Internal.newCursor Internal.NoOffsetKey t

{-# SPECIALISE newCursorAtOffset ::
     SerialiseKey k
  => k
  -> Table IO k v blob
  -> IO (Cursor IO k v blob) #-}
-- | A variant of 'newCursor' that allows initialising the cursor at an offset
-- within the table such that the first entry the cursor returns will be the
-- one with the lowest key that is greater than or equal to the given key.
-- In other words, it uses an inclusive lower bound.
--
-- NOTE: The ordering of the serialised keys will be used, which can lead to
-- unexpected results if the 'SerialiseKey' instance is not order-preserving!
newCursorAtOffset ::
     ( IOLike m
     , SerialiseKey k
     )
  => k
  -> Table m k v blob
  -> m (Cursor m k v blob)
newCursorAtOffset offset (Internal.NormalTable t) =
    Internal.NormalCursor <$!>
      Internal.newCursor (Internal.OffsetKey (Internal.serialiseKey offset)) t

{-# SPECIALISE closeCursor ::
     Cursor IO k v blob
  -> IO () #-}
-- | Close a cursor. 'closeCursor' is idempotent. All operations on a closed
-- cursor will throw an exception.
closeCursor ::
     IOLike m
  => Cursor m k v blob
  -> m ()
closeCursor (Internal.NormalCursor c) = Internal.closeCursor c

{-# SPECIALISE readCursor ::
     (SerialiseKey k, SerialiseValue v)
  => Int
  -> Cursor IO k v blob
  -> IO (V.Vector (QueryResult k v (BlobRef IO blob))) #-}
-- | Read the next @n@ entries from the cursor. The resulting vector is shorter
-- than @n@ if the end of the table has been reached. The cursor state is
-- updated, so the next read will continue where this one ended.
--
-- The cursor gets locked for the duration of the call, preventing concurrent
-- reads.
--
-- NOTE: entries are returned in order of the serialised keys, which might not
-- agree with @Ord k@. See 'SerialiseKey' for more information.
readCursor ::
     ( IOLike m
     , SerialiseKey k
     , SerialiseValue v
     )
  => Int
  -> Cursor m k v blob
  -> m (V.Vector (QueryResult k v (BlobRef m blob)))
readCursor n (Internal.NormalCursor c) =
    Internal.readCursor const n c $ \k v mblob ->
      toNormalQueryResult
        (Internal.deserialiseKey k)
        (Internal.deserialiseValue v)
        (BlobRef <$> mblob)

toNormalQueryResult :: k -> v -> Maybe b -> QueryResult k v b
toNormalQueryResult k v = \case
    Nothing    -> FoundInQuery k v
    Just blob  -> FoundInQueryWithBlob k v blob

{-------------------------------------------------------------------------------
  Table updates
-------------------------------------------------------------------------------}

-- | Normal tables support insert and delete operations.
--
-- An __update__ is a term that groups all types of table-manipulating
-- operations, like inserts and deletes.
data Update v blob =
    Insert !v !(Maybe blob)
  | Delete
  deriving stock (Show, Eq)

instance (NFData v, NFData blob) => NFData (Update v blob) where
  rnf Delete       = ()
  rnf (Insert v b) = rnf v `seq` rnf b

{-# SPECIALISE updates ::
     (SerialiseKey k, SerialiseValue v, SerialiseValue blob)
  => Table IO k v blob
  -> V.Vector (k, Update v blob)
  -> IO () #-}
-- | Perform a mixed batch of inserts and deletes.
--
-- If there are duplicate keys in the same batch, then keys nearer to the front
-- of the vector take precedence.
--
-- Updates can be performed concurrently from multiple Haskell threads.
updates ::
     ( IOLike m
     , SerialiseKey k
     , SerialiseValue v
     , SerialiseValue blob
     )
  => Table m k v blob
  -> V.Vector (k, Update v blob)
  -> m ()
updates (Internal.NormalTable t) es = do
    Internal.updates const (V.mapStrict serialiseEntry es) t
  where
    serialiseEntry = bimap Internal.serialiseKey serialiseOp
    serialiseOp = bimap Internal.serialiseValue Internal.serialiseBlob
                . updateToEntry

    updateToEntry :: Update v blob -> Entry.Entry v blob
    updateToEntry = \case
        Insert v Nothing  -> Entry.Insert v
        Insert v (Just b) -> Entry.InsertWithBlob v b
        Delete            -> Entry.Delete

{-# SPECIALISE inserts ::
     (SerialiseKey k, SerialiseValue v, SerialiseValue blob)
  => Table IO k v blob
  -> V.Vector (k, v, Maybe blob)
  -> IO () #-}
-- | Perform a batch of inserts.
--
-- Inserts can be performed concurrently from multiple Haskell threads.
inserts ::
     ( IOLike m
     , SerialiseKey k
     , SerialiseValue v
     , SerialiseValue blob
     )
  => Table m k v blob
  -> V.Vector (k, v, Maybe blob)
  -> m ()
inserts t = updates t . fmap (\(k, v, blob) -> (k, Insert v blob))

{-# SPECIALISE deletes ::
     (SerialiseKey k, SerialiseValue v, SerialiseValue blob)
  => Table IO k v blob
  -> V.Vector k
  -> IO () #-}
-- | Perform a batch of deletes.
--
-- Deletes can be performed concurrently from multiple Haskell threads.
deletes ::
     ( IOLike m
     , SerialiseKey k
     , SerialiseValue v
     , SerialiseValue blob
     )
  => Table m k v blob
  -> V.Vector k
  -> m ()
deletes t = updates t . fmap (,Delete)

{-# SPECIALISE retrieveBlobs ::
     SerialiseValue blob
  => Session IO
  -> V.Vector (BlobRef IO blob)
  -> IO (V.Vector blob) #-}
-- | Perform a batch of blob retrievals.
--
-- This is a separate step from 'lookups' and 'rangeLookup'. The result of a
-- lookup can include a 'BlobRef', which can be used to retrieve the actual
-- 'Blob'.
--
-- Note that 'BlobRef's can become invalid if the table is modified. See
-- 'BlobRef' for the exact rules on blob reference validity.
--
-- Blob lookups can be performed concurrently from multiple Haskell threads.
retrieveBlobs ::
     ( IOLike m
     , SerialiseValue blob
     )
  => Session m
  -> V.Vector (BlobRef m blob)
  -> m (V.Vector blob)
retrieveBlobs (Internal.Session' (sesh :: Internal.Session m h)) refs =
    V.map Internal.deserialiseBlob <$>
      Internal.retrieveBlobs sesh (V.imap checkBlobRefType refs)
  where
    checkBlobRefType _ (BlobRef (ref :: Internal.WeakBlobRef m h'))
      | Just Refl <- eqT @h @h' = ref
    checkBlobRefType i _ = throw (Internal.ErrBlobRefInvalid i)

{-------------------------------------------------------------------------------
  Snapshots
-------------------------------------------------------------------------------}

{-# SPECIALISE createSnapshot ::
     ( SerialiseKey k
     , SerialiseValue v
     , SerialiseValue blob
     , Common.Labellable (k, v, blob)
     )
  => SnapshotName
  -> Table IO k v blob
  -> IO () #-}
-- | Make the current value of a table durable on-disk by taking a snapshot and
-- giving the snapshot a name. This is the __only__ mechanism to make a table
-- durable -- ordinary insert\/delete operations are otherwise not preserved.
--
-- Snapshots have names and the table may be opened later using 'openSnapshot'
-- via that name. Names are strings and the management of the names is up to
-- the user of the library.
--
-- The names correspond to disk files, which imposes some constraints on length
-- and what characters can be used.
--
-- Snapshotting does not close the table.
--
-- Taking a snapshot is /relatively/ cheap, but it is not so cheap that one can
-- use it after every operation. In the implementation, it must at least flush
-- the write buffer to disk.
--
-- Concurrency:
--
-- * It is safe to concurrently make snapshots from any table, provided that
--   the snapshot names are distinct (otherwise this would be a race).
createSnapshot :: forall m k v blob.
     ( IOLike m
     , SerialiseKey k
     , SerialiseValue v
     , SerialiseValue blob
     , Common.Labellable (k, v, blob)
     )
  => SnapshotName
  -> Table m k v blob
  -> m ()
createSnapshot snap (Internal.NormalTable t) =
    Internal.createSnapshot const snap label Internal.SnapNormalTable t
  where
    label = Internal.SnapshotLabel $ Common.makeSnapshotLabel (Proxy @(k, v, blob))

{-# SPECIALISE openSnapshot ::
     ( SerialiseKey k
     , SerialiseValue v
     , SerialiseValue blob
     , Common.Labellable (k, v, blob)
     )
  => Session IO
  -> Common.TableConfigOverride
  -> SnapshotName
  -> IO (Table IO k v blob ) #-}
-- | Open a table from a named snapshot, returning a new table.
--
-- NOTE: close tables using 'close' as soon as they are
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
--   t <- 'new' \@IO \@Int \@Int \@Int session _
--   'createSnapshot' "intTable" t
--   'openSnapshot' \@IO \@Bool \@Bool \@Bool session "intTable"
-- @
openSnapshot :: forall m k v blob.
     ( IOLike m
     , SerialiseKey k
     , SerialiseValue v
     , SerialiseValue blob
     , Common.Labellable (k, v, blob)
     )
  => Session m
  -> Common.TableConfigOverride -- ^ Optional config override
  -> SnapshotName
  -> m (Table m k v blob)
openSnapshot (Internal.Session' sesh) override snap =
    Internal.NormalTable <$!>
      Internal.openSnapshot
        sesh
        label
        Internal.SnapNormalTable
        override
        snap
        const
  where
    label = Internal.SnapshotLabel $ Common.makeSnapshotLabel (Proxy @(k, v, blob))

{-------------------------------------------------------------------------------
  Mutiple writable tables
-------------------------------------------------------------------------------}

{-# SPECIALISE duplicate ::
     Table IO k v blob
  -> IO (Table IO k v blob) #-}
-- | Create a logically independent duplicate of a table. This returns a
-- new table.
--
-- A table and its duplicate are logically independent: changes to one
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
-- NOTE: duplication creates a new table, which should be closed when no
-- longer needed.
duplicate ::
     IOLike m
  => Table m k v blob
  -> m (Table m k v blob)
duplicate (Internal.NormalTable t) = Internal.NormalTable <$!> Internal.duplicate t

{-------------------------------------------------------------------------------
  Table union
-------------------------------------------------------------------------------}

{-# SPECIALISE union ::
     Table IO k v blob
  -> Table IO k v blob
  -> IO (Table IO k v blob) #-}
-- | Union two full tables, creating a new table.
--
-- A good mental model of this operation is @'Data.Map.Lazy.union'@ on
-- @'Data.Map.Lazy.Map' k v@.
--
-- Multiple tables of the same type but with different configuration parameters
-- can live in the same session. However, 'union' only works for tables that
-- have the same key\/value types and configuration parameters.
--
-- NOTE: unioning tables creates a new table, but does not close the tables that
-- were used as inputs.
union :: forall m k v blob.
     IOLike m
  => Table m k v blob
  -> Table m k v blob
  -> m (Table m k v blob)
union = undefined
