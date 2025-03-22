{-# LANGUAGE MagicHash #-}

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
    Common.SessionDirDoesNotExistError (..)
  , Common.SessionDirLockedError (..)
  , Common.SessionDirCorruptedError (..)
  , Common.SessionClosedError (..)
  , Common.TableClosedError (..)
  , Common.TableCorruptedError (..)
  , Common.TableTooLargeError (..)
  , Common.TableUnionNotCompatibleError (..)
  , Common.SnapshotExistsError (..)
  , Common.SnapshotDoesNotExistError (..)
  , Common.SnapshotCorruptedError (..)
  , Common.SnapshotNotCompatibleError (..)
  , Common.BlobRefInvalidError (..)
  , Common.CursorClosedError (..)
  , Common.FileFormat (..)
  , Common.FileCorruptedError (..)
  , Common.InvalidSnapshotNameError (..)

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
  , Common.FencePointerIndexType (..)
  , Common.DiskCachePolicy (..)
  , Common.MergeSchedule (..)
  , Common.defaultMergeSchedule
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
  , mupserts
  , updates
  , Update (..)

    -- * Durability (snapshots)
  , Common.SnapshotName
  , Common.toSnapshotName
  , Common.isValidSnapshotName
  , Common.SnapshotLabel (..)
  , createSnapshot
  , openSnapshot
  , Common.OverrideDiskCachePolicy (..)
  , deleteSnapshot
  , listSnapshots

    -- * Persistence
  , duplicate

    -- * Table union
  , union
  , unions
  , UnionDebt (..)
  , remainingUnionDebt
  , UnionCredits (..)
  , supplyUnionCredits

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
  , resolveValueAssociativity

    -- * Utility types
  , IOLike
  ) where

import           Control.DeepSeq
import           Control.Exception (assert)
import           Control.Monad (zipWithM, (<$!>))
import           Control.Monad.Class.MonadThrow
import           Data.Bifunctor (Bifunctor (..))
import           Data.Coerce (coerce)
import           Data.Kind (Type)
import           Data.List.NonEmpty (NonEmpty (..))
import           Data.Monoid (Sum (..))
import           Data.Typeable (Proxy (..), Typeable, eqT, type (:~:) (Refl),
                     typeRep)
import qualified Data.Vector as V
import           Database.LSMTree.Common (IOLike, Range (..), SerialiseKey,
                     SerialiseValue (..), Session, UnionCredits (..),
                     UnionDebt (..), closeSession, deleteSnapshot,
                     listSnapshots, openSession, withSession)
import qualified Database.LSMTree.Common as Common
import qualified Database.LSMTree.Internal as Internal
import qualified Database.LSMTree.Internal.Entry as Entry
import           Database.LSMTree.Internal.RawBytes (RawBytes)
import qualified Database.LSMTree.Internal.Serialise as Internal
import qualified Database.LSMTree.Internal.Snapshot as Internal
import qualified Database.LSMTree.Internal.Vector as V
import           GHC.Exts (Proxy#, proxy#)

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
-- parts. A table is the object\/reference by which an in-use LSM table will be
-- operated upon. In this API it identifies a single mutable instance of an LSM
-- table. The duplicate tables feature allows for there to may be many such
-- instances in use at once.
type Table = Internal.MonoidalTable

{-# SPECIALISE withTable ::
     Session IO
  -> Common.TableConfig
  -> (Table IO k v -> IO a)
  -> IO a #-}
-- | (Asynchronous) exception-safe, bracketed opening and closing of a table.
--
-- If possible, it is recommended to use this function instead of 'new' and
-- 'close'.
withTable :: forall m k v a.
     IOLike m
  => Session m
  -> Common.TableConfig
  -> (Table m k v -> m a)
  -> m a
withTable (Internal.Session' sesh) conf action =
    Internal.withTable sesh conf $
      action . Internal.MonoidalTable

{-# SPECIALISE new ::
     Session IO
  -> Common.TableConfig
  -> IO (Table IO k v) #-}
-- | Create a new empty table, returning a fresh table.
--
-- NOTE: tables hold open resources (such as open files) and should be
-- closed using 'close' as soon as they are no longer used.
--
new :: forall m k v.
     IOLike m
  => Session m
  -> Common.TableConfig
  -> m (Table m k v)
new (Internal.Session' sesh) conf = Internal.MonoidalTable <$> Internal.new sesh conf

{-# SPECIALISE close ::
     Table IO k v
  -> IO () #-}
-- | Close a table. 'close' is idempotent. All operations on a closed
-- handle will throw an exception.
--
-- Any on-disk files and in-memory data that are no longer referenced after
-- closing the table are lost forever. Use 'Snapshot's to ensure data is
-- not lost.
close :: forall m k v.
     IOLike m
  => Table m k v
  -> m ()
close (Internal.MonoidalTable t) = Internal.close t

{-------------------------------------------------------------------------------
  Table queries
-------------------------------------------------------------------------------}

-- | Result of a single point lookup.
data LookupResult v =
    NotFound
  | Found !v
  deriving stock (Eq, Show, Functor, Foldable, Traversable)

{-# SPECIALISE lookups ::
     (SerialiseKey k, SerialiseValue v, ResolveValue v)
  => Table IO k v
  -> V.Vector k
  -> IO (V.Vector (LookupResult v)) #-}
{-# INLINEABLE lookups #-}
-- | Perform a batch of lookups.
--
-- Lookups can be performed concurrently from multiple Haskell threads.
lookups :: forall m k v.
     (IOLike m, SerialiseKey k, SerialiseValue v, ResolveValue v)
  => Table m k v
  -> V.Vector k
  -> m (V.Vector (LookupResult v))
lookups (Internal.MonoidalTable t) ks =
    V.map toLookupResult <$>
    Internal.lookups
      (resolve @v Proxy)
      (V.map Internal.serialiseKey ks)
      t
  where
    toLookupResult (Just e) = case e of
      Entry.Insert v           -> Found (Internal.deserialiseValue v)
      Entry.InsertWithBlob v _ -> Found (Internal.deserialiseValue v)
      Entry.Mupdate v          -> Found (Internal.deserialiseValue v)
      Entry.Delete             -> NotFound
    toLookupResult Nothing = NotFound

-- | A result for one point in a cursor read or range query.
data QueryResult k v =
    FoundInQuery !k !v
  deriving stock (Eq, Show, Functor, Foldable, Traversable)

{-# SPECIALISE rangeLookup ::
     (SerialiseKey k, SerialiseValue v, ResolveValue v)
  => Table IO k v
  -> Range k
  -> IO (V.Vector (QueryResult k v)) #-}
-- | Perform a range lookup.
--
-- Range lookups can be performed concurrently from multiple Haskell threads.
rangeLookup :: forall m k v.
     (IOLike m, SerialiseKey k, SerialiseValue v, ResolveValue v)
  => Table m k v
  -> Range k
  -> m (V.Vector (QueryResult k v))
rangeLookup (Internal.MonoidalTable t) range =
    Internal.rangeLookup (resolve @v Proxy)(Internal.serialiseKey <$> range) t $ \k v mblob ->
      assert (null mblob) $
        FoundInQuery (Internal.deserialiseKey k) (Internal.deserialiseValue v)

{-------------------------------------------------------------------------------
  Cursor
-------------------------------------------------------------------------------}

-- | A read-only view into a table.
--
-- A cursor allows reading from a table sequentially (according to serialised
-- key ordering) in an incremental fashion. For example, this allows doing a
-- table scan in small chunks.
-- Once a cursor has been created, updates to the referenced table don't affect
-- the cursor.
type Cursor :: (Type -> Type) -> Type -> Type -> Type
type Cursor = Internal.MonoidalCursor

{-# SPECIALISE withCursor ::
     Table IO k v
  -> (Cursor IO k v -> IO a)
  -> IO a #-}
-- | (Asynchronous) exception-safe, bracketed opening and closing of a cursor.
--
-- If possible, it is recommended to use this function instead of 'newCursor'
-- and 'closeCursor'.
withCursor :: forall m k v a.
     IOLike m
  => Table m k v
  -> (Cursor m k v -> m a)
  -> m a
withCursor (Internal.MonoidalTable t) action =
    Internal.withCursor Internal.NoOffsetKey t (action . Internal.MonoidalCursor)

{-# SPECIALISE withCursorAtOffset ::
     SerialiseKey k
  => k
  -> Table IO k v
  -> (Cursor IO k v -> IO a)
  -> IO a #-}
-- | A variant of 'withCursor' that allows initialising the cursor at an offset
-- within the table such that the first entry the cursor returns will be the
-- one with the lowest key that is greater than or equal to the given key.
-- In other words, it uses an inclusive lower bound.
--
-- NOTE: The ordering of the serialised keys will be used, which can lead to
-- unexpected results if the 'SerialiseKey' instance is not order-preserving!
withCursorAtOffset :: forall m k v a.
     (IOLike m, SerialiseKey k)
  => k
  -> Table m k v
  -> (Cursor m k v -> m a)
  -> m a
withCursorAtOffset offset (Internal.MonoidalTable t) action =
    Internal.withCursor (Internal.OffsetKey (Internal.serialiseKey offset)) t $
      action . Internal.MonoidalCursor

{-# SPECIALISE newCursor ::
     Table IO k v
  -> IO (Cursor IO k v) #-}
-- | Create a new cursor to read from a given table. Future updates to the table
-- will not be reflected in the cursor. The cursor starts at the beginning, i.e.
-- the minimum key of the table.
--
-- Consider using 'withCursor' instead.
--
-- NOTE: cursors hold open resources (such as open files) and should be closed
-- using 'close' as soon as they are no longer used.
newCursor :: forall m k v.
     IOLike m
  => Table m k v
  -> m (Cursor m k v)
newCursor (Internal.MonoidalTable t) =
    Internal.MonoidalCursor <$!> Internal.newCursor Internal.NoOffsetKey t

{-# SPECIALISE newCursorAtOffset ::
     SerialiseKey k
  => k
  -> Table IO k v
  -> IO (Cursor IO k v) #-}
-- | A variant of 'newCursor' that allows initialising the cursor at an offset
-- within the table such that the first entry the cursor returns will be the
-- one with the lowest key that is greater than or equal to the given key.
-- In other words, it uses an inclusive lower bound.
--
-- NOTE: The ordering of the serialised keys will be used, which can lead to
-- unexpected results if the 'SerialiseKey' instance is not order-preserving!
newCursorAtOffset :: forall m k v.
     (IOLike m, SerialiseKey k)
  => k
  -> Table m k v
  -> m (Cursor m k v)
newCursorAtOffset offset (Internal.MonoidalTable t) =
    Internal.MonoidalCursor <$!>
      Internal.newCursor (Internal.OffsetKey (Internal.serialiseKey offset)) t

{-# SPECIALISE closeCursor ::
     Cursor IO k v
  -> IO () #-}
-- | Close a cursor. 'closeCursor' is idempotent. All operations on a closed
-- cursor will throw an exception.
closeCursor :: forall m k v.
     IOLike m
  => Cursor m k v
  -> m ()
closeCursor (Internal.MonoidalCursor c) = Internal.closeCursor c

{-# SPECIALISE readCursor ::
     (SerialiseKey k, SerialiseValue v, ResolveValue v)
  => Int
  -> Cursor IO k v
  -> IO (V.Vector (QueryResult k v)) #-}
-- | Read the next @n@ entries from the cursor. The resulting vector is shorter
-- than @n@ if the end of the table has been reached. The cursor state is
-- updated, so the next read will continue where this one ended.
--
-- The cursor gets locked for the duration of the call, preventing concurrent
-- reads.
--
-- NOTE: entries are returned in order of the serialised keys, which might not
-- agree with @Ord k@. See 'SerialiseKey' for more information.
readCursor :: forall m k v.
     ( IOLike m
     , SerialiseKey k
     , SerialiseValue v
     , ResolveValue v
     )
  => Int
  -> Cursor m k v
  -> m (V.Vector (QueryResult k v))
readCursor n (Internal.MonoidalCursor c) =
    Internal.readCursor (resolve @v Proxy) n c $ \k v mblob ->
      assert (null mblob) $
        FoundInQuery (Internal.deserialiseKey k) (Internal.deserialiseValue v)

{-------------------------------------------------------------------------------
  Table updates
-------------------------------------------------------------------------------}

-- | Monoidal tables support insert, delete and monoidal upsert operations.
--
-- An __update__ is a term that groups all types of table-manipulating
-- operations, like inserts, deletes and mupserts.
data Update v =
    Insert !v
  | Delete
    -- | TODO: should be given a more suitable name.
  | Mupsert !v
  deriving stock (Show, Eq)

instance NFData v => NFData (Update v) where
  rnf (Insert v)  = rnf v
  rnf Delete      = ()
  rnf (Mupsert v) = rnf v

{-# SPECIALISE updates ::
     (SerialiseKey k, SerialiseValue v, ResolveValue v)
  => Table IO k v
  -> V.Vector (k, Update v)
  -> IO () #-}
-- | Perform a mixed batch of inserts, deletes and monoidal upserts.
--
-- If there are duplicate keys in the same batch, then keys nearer to the front
-- of the vector take precedence.
--
-- Updates can be performed concurrently from multiple Haskell threads.
updates :: forall m k v.
     ( IOLike m
     , SerialiseKey k
     , SerialiseValue v
     , ResolveValue v
     )
  => Table m k v
  -> V.Vector (k, Update v)
  -> m ()
updates (Internal.MonoidalTable t) es = do
    Internal.updates
      (resolve @v Proxy)
      (V.mapStrict serialiseEntry es)
      t
  where
    serialiseEntry = bimap Internal.serialiseKey serialiseOp
    serialiseOp = first Internal.serialiseValue . updateToEntry

    updateToEntry :: Update v -> Entry.Entry v b
    updateToEntry = \case
        Insert v  -> Entry.Insert v
        Mupsert v -> Entry.Mupdate v
        Delete    -> Entry.Delete

{-# SPECIALISE inserts ::
     (SerialiseKey k, SerialiseValue v, ResolveValue v)
  => Table IO k v
  -> V.Vector (k, v)
  -> IO () #-}
-- | Perform a batch of inserts.
--
-- Inserts can be performed concurrently from multiple Haskell threads.
inserts :: forall m k v.
     ( IOLike m
     , SerialiseKey k
     , SerialiseValue v
     , ResolveValue v
     )
  => Table m k v
  -> V.Vector (k, v)
  -> m ()
inserts t = updates t . fmap (second Insert)

{-# SPECIALISE deletes ::
     (SerialiseKey k, SerialiseValue v, ResolveValue v)
  => Table IO k v
  -> V.Vector k
  -> IO () #-}
-- | Perform a batch of deletes.
--
-- Deletes can be performed concurrently from multiple Haskell threads.
deletes :: forall m k v.
     ( IOLike m
     , SerialiseKey k
     , SerialiseValue v
     , ResolveValue v
     )
  => Table m k v
  -> V.Vector k
  -> m ()
deletes t = updates t . fmap (,Delete)

{-# SPECIALISE mupserts ::
     (SerialiseKey k, SerialiseValue v, ResolveValue v)
  => Table IO k v
  -> V.Vector (k, v)
  -> IO () #-}
-- | Perform a batch of monoidal upserts.
--
-- Monoidal upserts can be performed concurrently from multiple Haskell threads.
mupserts :: forall m k v.
     ( IOLike m
     , SerialiseKey k
     , SerialiseValue v
     , ResolveValue v
     )
  => Table m k v
  -> V.Vector (k, v)
  -> m ()
mupserts t = updates t . fmap (second Mupsert)

{-------------------------------------------------------------------------------
  Snapshots
-------------------------------------------------------------------------------}

{-# SPECIALISE createSnapshot ::
     Common.SnapshotLabel
  -> Common.SnapshotName
  -> Table IO k v
  -> IO () #-}
-- | Make the current value of a table durable on-disk by taking a snapshot and
-- giving the snapshot a name. This is the __only__ mechanism to make a table
-- durable -- ordinary insert\/delete operations are otherwise not preserved.
--
-- Snapshots have names and the table may be opened later using 'openSnapshot'
-- via that name. Names are strings and the management of the names is up to the
-- user of the library.
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
--
createSnapshot :: forall m k v.
     IOLike m
  => Common.SnapshotLabel
  -> Common.SnapshotName
  -> Table m k v
  -> m ()
createSnapshot label snap (Internal.MonoidalTable t) =
    Internal.createSnapshot snap label Internal.SnapMonoidalTable t

{-# SPECIALISE openSnapshot ::
     ResolveValue v
  => Session IO
  -> Common.OverrideDiskCachePolicy
  -> Common.SnapshotLabel
  -> Common.SnapshotName
  -> IO (Table IO k v) #-}
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
openSnapshot :: forall m k v.
     ( IOLike m
     , ResolveValue v
     )
  => Session m
  -> Common.OverrideDiskCachePolicy
  -> Common.SnapshotLabel
  -> Common.SnapshotName
  -> m (Table m k v)
openSnapshot (Internal.Session' sesh) policyOverride label snap =
    Internal.MonoidalTable <$>
      Internal.openSnapshot
        sesh
        policyOverride
        label
        Internal.SnapMonoidalTable
        snap
        (resolve @v Proxy)

{-------------------------------------------------------------------------------
  Multiple writable tables
-------------------------------------------------------------------------------}

{-# SPECIALISE duplicate ::
     Table IO k v
  -> IO (Table IO k v) #-}
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
-- NOTE: duplication create a new table, which should be closed when no
-- longer needed.
--
duplicate :: forall m k v.
     IOLike m
  => Table m k v
  -> m (Table m k v)
duplicate (Internal.MonoidalTable t) = Internal.MonoidalTable <$> Internal.duplicate t

{-------------------------------------------------------------------------------
  Table union
-------------------------------------------------------------------------------}

{-# SPECIALISE union ::
     Table IO k v
  -> Table IO k v
  -> IO (Table IO k v) #-}
-- | Union two full tables, creating a new table.
--
-- A good mental model of this operation is @'Data.Map.Lazy.unionWith' (<>)@ on
-- @'Data.Map.Lazy.Map' k v@.
--
-- Multiple tables of the same type but with different configuration parameters
-- can live in the same session. However, 'union' only works for tables that
-- have the same key\/value types and configuration parameters.
--
-- NOTE: unioning tables creates a new table, but does not close the tables that
-- were used as inputs.
union :: forall m k v.
     IOLike m
  => Table m k v
  -> Table m k v
  -> m (Table m k v)
union t1 t2 = unions $ t1 :| [t2]

{-# SPECIALISE unions ::
     NonEmpty (Table IO k v)
  -> IO (Table IO k v) #-}
-- | Like 'union', but for @n@ tables.
--
-- A good mental model of this operation is @'Data.Map.Lazy.unionsWith' (<>)@ on
-- @'Data.Map.Lazy.Map' k v@.
unions :: forall m k v.
     IOLike m
  => NonEmpty (Table m k v)
  -> m (Table m k v)
unions (t :| ts) =
    case t of
      Internal.MonoidalTable (t' :: Internal.Table m h) -> do
        ts' <- zipWithM (checkTableType (proxy# @h)) [1..] ts
        Internal.MonoidalTable <$> Internal.unions (t' :| ts')
  where
    checkTableType ::
         forall h. Typeable h
      => Proxy# h
      -> Int
      -> Table m k v
      -> m (Internal.Table m h)
    checkTableType _ i (Internal.MonoidalTable (t' :: Internal.Table m h'))
      | Just Refl <- eqT @h @h' = pure t'
      | otherwise = throwIO $ Common.ErrTableUnionHandleTypeMismatch 0 (typeRep $ Proxy @h) i (typeRep $ Proxy @h')

{-# SPECIALISE remainingUnionDebt :: Table IO k v -> IO UnionDebt #-}
-- | Return the current union debt. This debt can be reduced until it is paid
-- off using @supplyUnionCredits@.
remainingUnionDebt :: IOLike m => Table m k v -> m UnionDebt
remainingUnionDebt (Internal.MonoidalTable t) =
    (\(Internal.UnionDebt x) -> UnionDebt x) <$>
      Internal.remainingUnionDebt t

{-# SPECIALISE supplyUnionCredits ::
     ResolveValue v => Table IO k v -> UnionCredits -> IO UnionCredits #-}
-- | Supply union credits to reduce union debt.
--
-- Supplying union credits leads to union merging work being performed in
-- batches. This reduces the union debt returned by @remainingUnionDebt@. Union
-- debt will be reduced by /at least/ the number of supplied union credits. It
-- is therefore advisable to query @remainingUnionDebt@ every once in a while to
-- see what the current debt is.
--
-- This function returns any surplus of union credits as /leftover/ credits when
-- a union has finished. In particular, if the returned number of credits is
-- non-negative, then the union is finished.
supplyUnionCredits ::
     forall m k v. (IOLike m, ResolveValue v)
  => Table m k v
  -> UnionCredits
  -> m UnionCredits
supplyUnionCredits (Internal.MonoidalTable t) (UnionCredits credits) =
    (\(Internal.UnionCredits x) -> UnionCredits x) <$>
      Internal.supplyUnionCredits
        (resolve @v Proxy)
        t
        (Internal.UnionCredits credits)

{-------------------------------------------------------------------------------
  Monoidal value resolution
-------------------------------------------------------------------------------}

-- | A class to specify how to resolve/merge values when using monoidal updates
-- ('Mupsert'). This is required for merging entries during compaction and also
-- for doing lookups, resolving multiple entries of the same key on the fly.
-- The class has some laws, which should be tested (e.g. with QuickCheck).
--
-- It is okay to assume that the input bytes can be deserialised using
-- 'deserialiseValue', as they are produced by either 'serialiseValue' or
-- 'resolveValue' itself, which are required to produce deserialisable output.
--
-- Prerequisites:
--
-- * [Valid Output] The result of resolution should always be deserialisable.
--   See 'resolveValueValidOutput'.
-- * [Associativity] Resolving values should be associative.
--   See 'resolveValueAssociativity'.
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
-- TODO: The laws depend on 'SerialiseValue', should we make it a superclass?
-- TODO: should we additionally require Totality (for any input 'RawBytes',
--       resolution should successfully provide a result)? This could reduce the
--       risk of encountering errors during a run merge.
class ResolveValue v where
  resolveValue :: Proxy v -> RawBytes -> RawBytes -> RawBytes

-- | Test the __Valid Output__ law for the 'ResolveValue' class
resolveValueValidOutput :: forall v.
     (SerialiseValue v, ResolveValue v, NFData v)
  => v -> v -> Bool
resolveValueValidOutput (serialiseValue -> x) (serialiseValue -> y) =
    (deserialiseValue (resolveValue (Proxy @v) x y) :: v) `deepseq` True

-- | Test the __Associativity__ law for the 'ResolveValue' class
resolveValueAssociativity :: forall v.
     (SerialiseValue v, ResolveValue v)
  => v -> v -> v -> Bool
resolveValueAssociativity (serialiseValue -> x) (serialiseValue -> y) (serialiseValue -> z) =
    x <+> (y <+> z) == (x <+> y) <+> z
  where
    (<+>) = resolveValue (Proxy @v)

-- | A helper function to implement 'resolveValue' by operating on the
-- deserialised representation. Note that especially for simple types it
-- should be possible to provide a more efficient implementation by directly
-- operating on the 'RawBytes'.
--
-- This function could potentially be used to provide a default implementation
-- for 'resolveValue', but it's probably best to be explicit about instances.
--
-- To satisfy the prerequisites of 'ResolveValue', the function provided to
-- 'resolveDeserialised' should itself satisfy some properties:
--
-- * [Associativity] The provided function should be associative.
-- * [Totality] The provided function should be total.
resolveDeserialised :: forall v.
     SerialiseValue v
  => (v -> v -> v) -> Proxy v -> RawBytes -> RawBytes -> RawBytes
resolveDeserialised f _ x y =
    serialiseValue (f (deserialiseValue x) (deserialiseValue y))

resolve ::  forall v. ResolveValue v => Proxy v -> Internal.ResolveSerialisedValue
resolve = coerce . resolveValue

-- | Mostly to give an example instance (plus the property tests for it).
-- Additionally, this instance for 'Sum' provides a nice monoidal, numerical
-- aggregator.
instance (Num a, SerialiseValue a) => ResolveValue (Sum a) where
  resolveValue = resolveDeserialised (+)
