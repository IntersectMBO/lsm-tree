-- TODO: Disable this warning once LSMTreeError is split.
{-# OPTIONS_GHC -Wno-incomplete-patterns #-}

{- |
Module      : Database.LSMTree.Simple
Copyright   : (c) 2023, Input Output Global, Inc. (IOG)
              (c) 2023-2025, INTERSECT
License     : Apache-2.0
Stability   : experimental
Portability : portable
-}
module Database.LSMTree.Simple (
    -- * Example
    -- $example

    -- * Usage Notes #usage_notes#

    -- ** Resource Management #resource_management#
    -- $resource_management

    -- ** Concurrency #concurrency#
    -- $concurrency

    -- ** Sharing #sharing#
    -- $sharing

    -- * Sessions #sessions#
    Session,
    withSession,
    openSession,
    closeSession,

    -- * Tables #tables#
    Table,
    withTable,
    withTableWith,
    new,
    newWith,
    close,

    -- ** Table Lookups #table_lookups#
    lookup,
    lookups,
    rangeLookup,

    -- ** Table Updates #table_updates#
    insert,
    inserts,
    delete,
    deletes,
    update,
    updates,

    -- ** Table Duplication #table_duplication#
    withDuplicate,
    duplicate,

    -- ** Table Unions #table_unions#
    withUnion,
    withUnions,
    union,
    unions,

    -- * Cursors #cursor#
    Cursor,
    withCursor,
    withCursorAtOffset,
    newCursor,
    newCursorAtOffset,
    closeCursor,
    next,
    take,
    takeWhile,

    -- * Snapshots #snapshots#
    createSnapshot,
    withTableFromSnapshot,
    withTableFromSnapshotWith,
    openTableFromSnapshot,
    openTableFromSnapshotWith,
    deleteSnapshot,
    listSnapshots,
    SnapshotName,
    mkSnapshotName,
    SnapshotLabel (..),

    -- * Table Configuration #table_configuration#
    TableConfig (..),
    MergePolicy (MergePolicyLazyLevelling),
    SizeRatio (Four),
    WriteBufferAlloc (AllocNumEntries),
    BloomFilterAlloc (AllocFixed, AllocRequestFPR),
    FencePointerIndex (OrdinaryIndex, CompactIndex),
    DiskCachePolicy (..),
    MergeSchedule (..),

    -- ** Table Configuration Overrides #table_configuration_overrides#
    TableConfigOverride,
    configNoOverride,
    configOverrideDiskCachePolicy,

    -- * Ranges #ranges#
    Range (..),

    -- * Key\/Value Serialisation #key_value_serialisation#
    RawBytes (RawBytes),
    SerialiseKey (serialiseKey, deserialiseKey),
    SerialiseValue (serialiseValue, deserialiseValue),

    -- ** Key\/Value Serialisation Property Tests #key_value_serialisation_property_tests#
    serialiseKeyIdentity,
    serialiseKeyIdentityUpToSlicing,
    serialiseKeyMinimalSize,
    serialiseValueIdentity,
    serialiseValueIdentityUpToSlicing,

    -- * Errors #error#
    SessionDoesNotExistError (..),
    SessionLockedError (..),
    SessionCorruptedError (..),
    SessionClosedError (..),
    TableClosedError (..),
    TableCorruptedError (..),
    ByteCountDiscrepancy (..),
    SnapshotExistsError (..),
    SnapshotDoesNotExistError (..),
    SnapshotCorruptedError (..),
    SnapshotNotCompatibleError (..),
    DeserialiseFailure (..),
    SnapshotTableType (..),
    TableNotCompatibleError (..),
) where

import           Codec.CBOR.Read (DeserialiseFailure (..))
import           Control.Exception.Base (Exception, SomeException (..), bracket,
                     mapException)
import           Control.Monad (join)
import           Data.Bifunctor (Bifunctor (..))
import           Data.Kind (Type)
import           Data.List.NonEmpty (NonEmpty (..))
import           Data.Vector (Vector)
import qualified Data.Vector as V
import qualified Database.LSMTree.Internal as Internal
import           Database.LSMTree.Internal.Config
                     (BloomFilterAlloc (AllocFixed, AllocRequestFPR),
                     DiskCachePolicy (..), FencePointerIndex (..),
                     MergePolicy (..), MergeSchedule (..), SizeRatio (..),
                     TableConfig (..), TableConfigOverride,
                     WriteBufferAlloc (..), configNoOverride,
                     configOverrideDiskCachePolicy, defaultTableConfig)
import qualified Database.LSMTree.Internal.Entry as Entry
import           Database.LSMTree.Internal.Lookup (ByteCountDiscrepancy (..))
import           Database.LSMTree.Internal.Paths (SnapshotName, mkSnapshotName)
import           Database.LSMTree.Internal.Range (Range (..))
import           Database.LSMTree.Internal.RawBytes (RawBytes (..))
import qualified Database.LSMTree.Internal.Serialise as Internal
import           Database.LSMTree.Internal.Serialise.Class (SerialiseKey (..),
                     SerialiseValue (..), serialiseKeyIdentity,
                     serialiseKeyIdentityUpToSlicing, serialiseKeyMinimalSize,
                     serialiseValueIdentity, serialiseValueIdentityUpToSlicing)
import           Database.LSMTree.Internal.Snapshot (SnapshotLabel (..),
                     SnapshotTableType)
import qualified Database.LSMTree.Internal.Snapshot as Internal
import           Prelude hiding (lookup, take, takeWhile)
import           System.FS.API (MountPoint (..), mkFsPath)
import           System.FS.BlockIO.API (defaultIOCtxParams)
import           System.FS.BlockIO.IO (ioHasBlockIO, withIOHasBlockIO)
import           System.FS.IO (HandleIO, ioHasFS)

--------------------------------------------------------------------------------
-- Example
--------------------------------------------------------------------------------

{- $setup

>>> import Prelude hiding (lookup)
>>> import Data.ByteString.Short (ShortByteString)
>>> import Data.Word (Word64)
>>> import System.Directory (createDirectoryIfMissing, getTemporaryDirectory)
>>> import System.FilePath ((</>))
>>> import System.FS.IO (ioHasFS)

>>> let mkSessionDirectory (n :: Int) = getTemporaryDirectory >>= \tmp -> let dir = tmp </> "lsm-tree-doctest" </> "sessions" </> show n in createDirectoryIfMissing True dir >> pure dir
>>> mySessionDirectory <- mkSessionDirectory 0
-}

{- $example

>>> session <- openSession mySessionDirectory
>>> table <- new @Word64 @ShortByteString session
>>> insert table 0 "Hello"
>>> insert table 1 "World"
>>> lookup table 0
Just "Hello"
>>> close table
>>> closeSession session
-}

--------------------------------------------------------------------------------
-- Resource Management
--------------------------------------------------------------------------------

{- $resource_management
This package uses explicit resource mangagement. The 'Session', 'Table', and 'Cursor'
handles hold open resources, such as file handles, which must be explicitly released.
Every operation that allocates a resource is paired with another operation to releases
that resource. For each pair of allocate and release operations there is a bracketed
function that combines the two.

+------------+--------------------------+-------------------------+-------------------+
| Resource   | Bracketed #bracketed#    | Allocate #allocate#     | Release #release# |
+============+==========================+=========================+===================+
|'Session'   | 'withSession'            | 'openSession'           | 'closeSession'    |
+------------+--------------------------+-------------------------+-------------------+
| 'Table'    | 'withTable'              | 'new'                   | 'close'           |
+            +--------------------------+-------------------------+                   +
|            | 'withDuplicate'          | 'duplicate'             |                   |
+            +--------------------------+-------------------------+                   +
|            | 'withTableFromSnapshot'  | 'openTableFromSnapshot' |                   |
+------------+--------------------------+-------------------------+-------------------+
| 'Cursor'   | 'withCursor'             | 'newCursor'             | 'closeCursor'     |
+------------+--------------------------+-------------------------+-------------------+

To prevent resource and memory leaks due to asynchronous exceptions,
it is recommended to use the [bracketed](#bracketed) functions whenever
possible, and otherwise:

*   Run functions that allocate, use, and release a resource with asynchronous
    exceptions masked.
*   Pair functions that allocate a resource with a masked cleanup function,
    e.g., using 'bracket'.
-}

--------------------------------------------------------------------------------
-- Concurrency
--------------------------------------------------------------------------------

{- $concurrency
Table handles may be used concurrently from multiple Haskell threads,
and doing read operations concurrently may result in improved throughput,
as it can take advantage of CPU and I\/O parallelism. However, concurrent
use of write operations may introduces races. Specifically:

* It is a race to read and write the same table concurrently.
* It is a race to write and write the same table concurrently.
* It is /not/ a race to read and read the same table concurrently.
* It is /not/ a race to read or write /separate/ tables concurrently.

For the purposes of the above rules:

* The read operations are 'lookup', 'rangeLookup', 'duplicate', `union`, 'createSnapshot', 'newCursor', and their variants.
* The write operations are 'insert', 'delete', 'update', 'close', and their variants.

It is possible to read obtain read a stable view of a table while concurrently
writing to it by using 'duplicate' and performing the read operations on the
duplicate. However, this requires that the 'duplicate' operation /happens before/
the subsequent writes, as it is a race to duplicate concurrently with any writes.
As this package does not provide any construct for synchronisation or atomic
operations, this ordering of operations must be accomplished by other means.

An 'Cursor' creates a stable view of a table and can safely be read while
modifying the original table. However, reading the 'next' key\/value pair from
an cursor locks the view, so concurrent reads on the same cursor block.
This is because 'next' updates the cursor's current position.
-}

--------------------------------------------------------------------------------
-- Sharing
--------------------------------------------------------------------------------

{- $sharing
Tables created via 'duplicate' or 'union' will share some of their data with
the tables they were created from. Sharing is not fully preserved by snapshots.
Existing runs are shared, but ongoing merges are not.
Opening a table from a snapshot (using 'openTableFromSnapshot' or
'withTableFromSnapshot') is expensive, but creating a snapshot (using
'createSnapshot') is relatively cheap.
-}

--------------------------------------------------------------------------------
-- Sessions
--------------------------------------------------------------------------------

{- | A 'Session' stores context that is shared by multiple tables.

  Each session is associated with one session directory where the files
  containing table data are stored. Each session locks its session directory.
  There can only be one active session for each session directory at a time.
  If a database is must be accessed from multiple parts of a program,
  one session should be opened and shared between those parts of the program.
  Session directories cannot be shared between OS processes.
-}
type Session :: Type
newtype Session = Session {unSession :: Internal.Session IO HandleIO}

{- |
Throws the following exceptions:

['SessionDoesNotExistError']:
    If the session directory does not exist.
['SessionLockedError']:
    If the session directory is locked by another process.
['SessionCorruptedError']:
    If the session directory is malformed.
-}
withSession ::
    -- | The session directory.
    FilePath ->
    (Session -> IO a) ->
    IO a
withSession dir action = do
    let tracer = mempty
    let mountPoint = MountPoint dir
    let rootDir = mkFsPath []
    let hasFS = ioHasFS mountPoint
    mapException (toSessionDoesNotExistError dir) . mapException (toSessionLockedError dir) . mapException (toSessionCorruptedError dir) $
        withIOHasBlockIO hasFS defaultIOCtxParams $ \hasBlockIO ->
            Internal.withSession tracer hasFS hasBlockIO rootDir (action . Session)

{- |
Throws the following exceptions:

['SessionDoesNotExistError']:
    If the session directory does not exist.
['SessionLockedError']:
    If the session directory is locked by another process.
['SessionCorruptedError']:
    If the session directory is malformed.
-}
openSession ::
    -- | The session directory.
    FilePath ->
    IO Session
openSession dir = do
    let tracer = mempty
    let mountPoint = MountPoint dir
    let rootDir = mkFsPath []
    let hasFS = ioHasFS mountPoint
    hasBlockIO <- ioHasBlockIO hasFS defaultIOCtxParams
    mapException (toSessionDoesNotExistError dir) . mapException (toSessionLockedError dir) . mapException (toSessionCorruptedError dir) $
        Session <$> Internal.openSession tracer hasFS hasBlockIO rootDir

closeSession :: Session -> IO ()
closeSession = Internal.closeSession . unSession

--------------------------------------------------------------------------------
-- Tables
--------------------------------------------------------------------------------

{- | A 'Table' is a handle to an LSM-tree key\/value store.

  Each 'Table' is a handle to an individual key\/value store with both in-memory and on-disk parts.

  __Warning:__ Tables are emphemeral. Once you close a table, its data is lost forever. To persist tables, use [snapshots](#g:snapshots).
-}
type role Table nominal nominal

type Table :: Type -> Type -> Type
data Table k v
    = (SerialiseKey k, SerialiseValue v) =>
    Table {unTable :: {-# UNPACK #-} !(Internal.Table IO HandleIO)}

{- | Run an action with access to an empty 'Table'.

This function is exception-safe for both asynchronous and synchronous exceptions.

It is recommended to use this function instead of 'new' and 'close'.

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
-}
withTable ::
    (SerialiseKey k, SerialiseValue v) =>
    Session ->
    (Table k v -> IO a) ->
    IO a
withTable session =
    withTableWith defaultTableConfig session

-- | Variant of 'withTable' that accepts [table configuration](#g:table_configuration).
withTableWith ::
    (SerialiseKey k, SerialiseValue v) =>
    TableConfig ->
    Session ->
    (Table k v -> IO a) ->
    IO a
withTableWith tableConfig session action =
    mapException toSessionClosedError $
        Internal.withTable (unSession session) tableConfig (action . Table)

{- | Create an empty table.

__Warning:__ Tables hold open resources and must be closed using 'close'.

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
-}
new ::
    (SerialiseKey k, SerialiseValue v) =>
    Session ->
    IO (Table k v)
new session =
    newWith defaultTableConfig session

-- | Variant of 'new' that accepts [table configuration](#g:table_configuration).
newWith ::
    (SerialiseKey k, SerialiseValue v) =>
    TableConfig ->
    Session ->
    IO (Table k v)
newWith tableConfig (Session session) =
    mapException toSessionClosedError $
        Table <$> Internal.new session tableConfig

{- | Close a table.

Closing is idempotent, i.e., closing a closed table does nothing.
All other operations on a closed table will throw an exception.

  __Warning:__ Tables are emphemeral. Once you close a table, its data is lost forever. To persist tables, use [snapshots](#g:snapshots).
-}
close :: Table k v -> IO ()
close (Table table) =
    mapException toSessionClosedError $
        Internal.close table

--------------------------------------------------------------------------------
-- Lookups
--------------------------------------------------------------------------------

{- | Look up a the value associated with a key.

Lookups can be performed concurrently from multiple Haskell threads.

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
['TableClosedError']:
    If the table is closed.
['TableCorruptedError']:
    If the table data is corrupted.
-}
lookup ::
    Table k v ->
    k ->
    IO (Maybe v)
lookup table k = do
    mvs <- lookups table (V.singleton k)
    let mmv = fst <$> V.uncons mvs
    pure (join mmv)

{- | Variant of 'lookup' for batch lookups.

The batch of keys corresponds in-order to the batch of results, i.e., the following property holds in the absence of races:

prop> lookups table keys = traverse (lookup table) keys
-}
lookups ::
    Table k v ->
    Vector k ->
    IO (Vector (Maybe v))
lookups (Table table) keys = do
    mapException toSessionClosedError . mapException toTableClosedError . mapException toTableCorruptedError $ do
        maybeEntries <- Internal.lookups const (fmap Internal.serialiseKey keys) table
        pure $ (entryToMaybeValue =<<) <$> maybeEntries
  where
    entryToMaybeValue = \case
        Entry.Insert !v -> Just (Internal.deserialiseValue v)
        Entry.InsertWithBlob !v !_b -> Just (Internal.deserialiseValue v)
        Entry.Mupdate !v -> Just (Internal.deserialiseValue v)
        Entry.Delete -> Nothing

{- | Look up a batch of values associated with keys in the given range.

Range lookups can be performed concurrently from multiple Haskell threads.

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
['TableClosedError']:
    If the table is closed.
['TableCorruptedError']:
    If the table data is corrupted.
-}
rangeLookup ::
    Table k v ->
    Range k ->
    IO (Vector (k, v))
rangeLookup (Table table) range =
    mapException toSessionClosedError . mapException toTableClosedError . mapException toTableCorruptedError $
        Internal.rangeLookup const (Internal.serialiseKey <$> range) table $ \k v !_b ->
            (Internal.deserialiseKey k, Internal.deserialiseValue v)

--------------------------------------------------------------------------------
-- Updates
--------------------------------------------------------------------------------

{- |

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
['TableClosedError']:
    If the table is closed.
-}
insert ::
    Table k v ->
    k ->
    v ->
    IO ()
insert table k v =
    inserts table (V.singleton (k, v))

{- | Variant of 'insert' for batch insertions.

The following property holds in the absence of races:

prop> inserts table entries = traverse_ (uncurry $ insert table) entries
-}
inserts ::
    Table k v ->
    Vector (k, v) ->
    IO ()
inserts table entries =
    updates table (fmap (second Just) entries)

{- |

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
['TableClosedError']:
    If the table is closed.
-}
delete ::
    Table k v ->
    k ->
    IO ()
delete table k =
    deletes table (V.singleton k)

{- | Variant of 'delete' for batch deletions.

The following property holds in the absence of races:

prop> deletes table keys = traverse_ (delete table) keys
-}
deletes ::
    Table k v ->
    Vector k ->
    IO ()
deletes table entries =
    updates table (fmap (,Nothing) entries)

{- |

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
['TableClosedError']:
    If the table is closed.
-}
update ::
    Table k v ->
    k ->
    Maybe v ->
    IO ()
update table k mv =
    updates table (V.singleton (k, mv))

{- | Variant of 'update' for batch updates.

The following property holds in the absence of races:

prop> updates table entries = traverse_ (uncurry $ update table) entries
-}
updates ::
    Table k v ->
    Vector (k, Maybe v) ->
    IO ()
updates (Table table) entries =
    mapException toSessionClosedError . mapException toTableClosedError $
        Internal.updates const (serialiseUpdate <$> entries) table
  where
    serialiseUpdate (k, Just v) = (Internal.serialiseKey k, Entry.Insert (Internal.serialiseValue v))
    serialiseUpdate (k, Nothing) = (Internal.serialiseKey k, Entry.Delete)

--------------------------------------------------------------------------------
-- Duplication
--------------------------------------------------------------------------------

{- | Run an action with access to the duplicate of a table.

This function is exception-safe for both asynchronous and synchronous exceptions.

It is recommended to use this function instead of 'duplicate' and 'close'.

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
['TableClosedError']:
    If the table is closed.
-}
withDuplicate ::
    Table k v ->
    (Table k v -> IO a) ->
    IO a
withDuplicate table =
    bracket (duplicate table) close

{- | Duplicate a table.

__Warning:__ The duplicate must be independently closed using 'close'.

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
['TableClosedError']:
    If the table is closed.
-}
duplicate ::
    Table k v ->
    IO (Table k v)
duplicate (Table table) =
    mapException toSessionClosedError . mapException toTableClosedError $
        Table <$> Internal.duplicate table

--------------------------------------------------------------------------------
-- Union
--------------------------------------------------------------------------------

{- | Run an action with access to a table that contains the union of the entries of the given tables.

This function is exception-safe for both asynchronous and synchronous exceptions.

It is recommended to use this function instead of 'union' and 'close'.

__Warning:__ Both input tables must be from the same 'Session' and have the same configuration parameters.

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
['TableClosedError']:
    If the table is closed.
['TableNotCompatibleError']:
    If both tables are not from the same 'Session'.
-}
withUnion ::
    Table k v ->
    Table k v ->
    (Table k v -> IO a) ->
    IO a
withUnion table1 table2 =
    bracket (table1 `union` table2) close


-- | Variant of 'withUnions' for any number of tables.
withUnions ::
    NonEmpty (Table k v) ->
    (Table k v -> IO a) ->
    IO a
withUnions tables =
    bracket (unions tables) close

{- | Create a table that contains the union of the entries of the given tables.

__Warning:__ The new table must be independently closed using 'close'.

__Warning:__ Both input tables must be from the same 'Session' and have the same configuration parameters.

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
['TableClosedError']:
    If the table is closed.
['TableNotCompatibleError']:
    If both tables are not from the same 'Session'.
-}
union ::
    Table k v ->
    Table k v ->
    IO (Table k v)
union table1 table2 =
    unions (table1 :| table2 : [])

-- | Variant of 'union' for any number of tables.
unions ::
    NonEmpty (Table k v) ->
    IO (Table k v)
unions (Table table :| tables) =
    mapException toSessionClosedError . mapException toTableClosedError . mapException toTableNotCompatibleError $
        Table <$> Internal.unions (table :| fmap unTable tables)

--------------------------------------------------------------------------------
-- Snapshots
--------------------------------------------------------------------------------

{- |

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
['TableClosedError']:
    If the table is closed.
['SnapshotExistsError']:
    If a snapshot with the same name already exists.
-}
createSnapshot ::
    SnapshotName ->
    SnapshotLabel ->
    Table k v ->
    IO ()
createSnapshot snapName snapLabel (Table table) =
    mapException toSessionClosedError . mapException toTableClosedError . mapException toSnapshotExistsError $
        -- TODO: remove SnapshotTableType
        Internal.createSnapshot snapName snapLabel Internal.SnapSimpleTable table

{- |

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
['TableClosedError']:
    If the table is closed.
['SnapshotDoesNotExistError']
    If no snapshot with the given name exists.
['SnapshotCorruptedError']:
    If the snapshot data is corrupted.
['SnapshotNotCompatibleError']:
    If the snapshot has a different label or is a different table type.
-}
withTableFromSnapshot ::
    (SerialiseKey k, SerialiseValue v) =>
    Session ->
    SnapshotName ->
    SnapshotLabel ->
    (Table k v -> IO a) ->
    IO a
withTableFromSnapshot session snapName snapLabel =
    bracket (openTableFromSnapshot session snapName snapLabel) close

{- |

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
['TableClosedError']:
    If the table is closed.
['SnapshotDoesNotExistError']
    If no snapshot with the given name exists.
['SnapshotCorruptedError']:
    If the snapshot data is corrupted.
['SnapshotNotCompatibleError']:
    If the snapshot has a different label or is a different table type.
-}
withTableFromSnapshotWith ::
    (SerialiseKey k, SerialiseValue v) =>
    TableConfigOverride ->
    Session ->
    SnapshotName ->
    SnapshotLabel ->
    (Table k v -> IO a) ->
    IO a
withTableFromSnapshotWith tableConfigOverride session snapName snapLabel =
    bracket (openTableFromSnapshotWith session tableConfigOverride snapName snapLabel) close

{- |

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
['TableClosedError']:
    If the table is closed.
['SnapshotDoesNotExistError']
    If no snapshot with the given name exists.
['SnapshotCorruptedError']:
    If the snapshot data is corrupted.
['SnapshotNotCompatibleError']:
    If the snapshot has a different label or is a different table type.
-}
openTableFromSnapshot ::
    (SerialiseKey k, SerialiseValue v) =>
    Session ->
    SnapshotName ->
    SnapshotLabel ->
    IO (Table k v)
openTableFromSnapshot session snapName snapLabel =
    openTableFromSnapshotWith session mempty snapName snapLabel

{- |

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
['TableClosedError']:
    If the table is closed.
['SnapshotDoesNotExistError']
    If no snapshot with the given name exists.
['SnapshotCorruptedError']:
    If the snapshot data is corrupted.
['SnapshotNotCompatibleError']:
    If the snapshot has a different label or is a different table type.
-}
openTableFromSnapshotWith ::
    (SerialiseKey k, SerialiseValue v) =>
    Session ->
    TableConfigOverride ->
    SnapshotName ->
    SnapshotLabel ->
    IO (Table k v)
openTableFromSnapshotWith (Session session) tableConfigOverride snapName snapLabel =
    mapException toSessionClosedError . mapException toTableClosedError . mapException toSnapshotCorruptedError . mapException toSnapshotNotCompatibleError $
        Table <$> Internal.openSnapshot session snapLabel Internal.SnapSimpleTable tableConfigOverride snapName const

{- |

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
['SnapshotDoesNotExistError']:
    If no snapshot with the given name exists.
-}
deleteSnapshot ::
    Session ->
    SnapshotName ->
    IO ()
deleteSnapshot =
    mapException toSessionClosedError . mapException toTableClosedError . mapException toSnapshotDoesNotExistError $
        Internal.deleteSnapshot . unSession

{- |


Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
-}
listSnapshots ::
    Session ->
    IO [SnapshotName]
listSnapshots (Session session) =
    mapException toSessionClosedError . mapException toTableClosedError $
        Internal.listSnapshots session

--------------------------------------------------------------------------------
-- Cursors
--------------------------------------------------------------------------------

type role Cursor nominal nominal
type Cursor :: Type -> Type -> Type
data Cursor k v
    = (SerialiseKey k, SerialiseValue v) =>
    Cursor {unCursor :: {-# UNPACK #-} !(Internal.Cursor IO HandleIO)}

withCursor ::
    Table k v ->
    (Cursor k v -> IO a) ->
    IO a
withCursor = _withCursorMaybeAtOffset Nothing

withCursorAtOffset ::
    k ->
    Table k v ->
    (Cursor k v -> IO a) ->
    IO a
withCursorAtOffset offsetKey = _withCursorMaybeAtOffset (Just offsetKey)

-- Internal helper.
_withCursorMaybeAtOffset ::
    Maybe k ->
    Table k v ->
    (Cursor k v -> IO a) ->
    IO a
_withCursorMaybeAtOffset maybeOffsetKey (Table table) action =
    Internal.withCursor offsetKey table (action . Cursor)
  where
    offsetKey = maybe Internal.NoOffsetKey (Internal.OffsetKey . Internal.serialiseKey) maybeOffsetKey

newCursor ::
    Table k v ->
    IO (Cursor k v)
newCursor = _newCursorMaybeAtOffset Nothing

newCursorAtOffset ::
    k ->
    Table k v ->
    IO (Cursor k v)
newCursorAtOffset offsetKey = _newCursorMaybeAtOffset (Just offsetKey)

-- Internal helper.
_newCursorMaybeAtOffset ::
    Maybe k ->
    Table k v ->
    IO (Cursor k v)
_newCursorMaybeAtOffset maybeOffsetKey (Table table) =
    Cursor <$> Internal.newCursor offsetKey table
  where
    offsetKey = maybe Internal.NoOffsetKey (Internal.OffsetKey . Internal.serialiseKey) maybeOffsetKey

closeCursor ::
    Cursor k v ->
    IO ()
closeCursor = Internal.closeCursor . unCursor

next ::
    Cursor k v ->
    IO (Maybe (k, v))
next iterator = do
    entries <- take 1 iterator
    pure $ fst <$> V.uncons entries

take ::
    Int ->
    Cursor k v ->
    IO (Vector (k, v))
take n (Cursor cursor) =
    Internal.readCursor const n cursor $ \ !k !v !_b ->
        (Internal.deserialiseKey k, Internal.deserialiseValue v)

takeWhile ::
    (k -> Bool) ->
    Int ->
    Cursor k v ->
    IO (Vector (k, v))
takeWhile p n (Cursor cursor) =
    Internal.readCursorWhile const (p . Internal.deserialiseKey) n cursor $ \ !k !v !_b ->
        (Internal.deserialiseKey k, Internal.deserialiseValue v)

--------------------------------------------------------------------------------
-- Errors
--------------------------------------------------------------------------------

data SessionDoesNotExistError
    = -- | The session directory does not exist.
      ErrSessionDirDoesNotExist !FilePath
    deriving stock (Show, Eq)
    deriving anyclass (Exception)

toSessionDoesNotExistError :: FilePath -> Internal.LSMTreeError -> SomeException
toSessionDoesNotExistError dir = \case
    Internal.SessionDirDoesNotExist _fsErrorPath -> SomeException $ ErrSessionDirDoesNotExist dir
    e -> SomeException e

data SessionLockedError
    = -- | The session directory is locked by another active session.
      ErrSessionDirLocked !FilePath
    deriving stock (Show, Eq)
    deriving anyclass (Exception)

toSessionLockedError :: FilePath -> Internal.LSMTreeError -> SomeException
toSessionLockedError dir = \case
    Internal.SessionDirLocked _fsErrorPath -> SomeException $ ErrSessionDirLocked dir
    e -> SomeException e

data SessionCorruptedError
    = -- | The session directory is malformed.
      ErrSessionDirMalformed !FilePath
    deriving stock (Show, Eq)
    deriving anyclass (Exception)

toSessionCorruptedError :: FilePath -> Internal.LSMTreeError -> SomeException
toSessionCorruptedError dir = \case
    Internal.SessionDirMalformed _fsErrorPath -> SomeException $ ErrSessionDirMalformed dir
    e -> SomeException e

data SessionClosedError
    = ErrSessionClosed
    deriving stock (Show, Eq)
    deriving anyclass (Exception)

toSessionClosedError :: Internal.LSMTreeError -> SomeException
toSessionClosedError = \case
    Internal.ErrSessionClosed -> SomeException ErrSessionClosed
    e -> SomeException e

data TableClosedError
    = ErrTableClosed
    deriving stock (Show, Eq)
    deriving anyclass (Exception)

toTableClosedError :: Internal.LSMTreeError -> SomeException
toTableClosedError = \case
    Internal.ErrTableClosed -> SomeException ErrTableClosed
    e -> SomeException e

data TableCorruptedError
    = ErrTableCorrupted !ByteCountDiscrepancy
    deriving stock (Show, Eq)
    deriving anyclass (Exception)

toTableCorruptedError :: Internal.LSMTreeError -> SomeException
toTableCorruptedError = \case
    Internal.ErrLookup byteCountDiscrepancy -> SomeException $ ErrTableCorrupted byteCountDiscrepancy
    e -> SomeException e

data SnapshotExistsError
    = ErrSnapshotExists !SnapshotName
    deriving stock (Show, Eq)
    deriving anyclass (Exception)

toSnapshotExistsError :: Internal.LSMTreeError -> SomeException
toSnapshotExistsError = \case
    Internal.ErrSnapshotExists snapName -> SomeException $ ErrSnapshotExists snapName
    e -> SomeException e

data SnapshotDoesNotExistError
    = ErrSnapshotDoesNotExist !SnapshotName
    deriving stock (Show, Eq)
    deriving anyclass (Exception)

toSnapshotDoesNotExistError :: Internal.LSMTreeError -> SomeException
toSnapshotDoesNotExistError = \case
    Internal.ErrSnapshotDoesNotExist snapName -> SomeException $ ErrSnapshotDoesNotExist snapName
    e -> SomeException e

data SnapshotCorruptedError
    = ErrSnapshotDeserialiseFailure
      !DeserialiseFailure
      !SnapshotName
    deriving stock (Show, Eq)
    deriving anyclass (Exception)

toSnapshotCorruptedError :: Internal.LSMTreeError -> SomeException
toSnapshotCorruptedError = \case
    Internal.ErrSnapshotDeserialiseFailure deserialiseFailure snapName ->
        SomeException $ ErrSnapshotDeserialiseFailure deserialiseFailure snapName
    e -> SomeException e

data SnapshotNotCompatibleError
    = ErrSnapshotWrongTableType
      !SnapshotName
      !SnapshotTableType -- ^ Expected type
      !SnapshotTableType -- ^ Actual type
    | ErrSnapshotWrongLabel
      !SnapshotName
      !SnapshotLabel -- ^ Expected label
      !SnapshotLabel -- ^ Actual label
    deriving stock (Show, Eq)
    deriving anyclass (Exception)

toSnapshotNotCompatibleError :: Internal.LSMTreeError -> SomeException
toSnapshotNotCompatibleError = \case
    Internal.ErrSnapshotWrongTableType snapName expectedType actualType ->
        SomeException $ ErrSnapshotWrongTableType snapName expectedType actualType
    Internal.ErrSnapshotWrongLabel snapName expectedLabel actualLabel ->
        SomeException $ ErrSnapshotWrongLabel snapName expectedLabel actualLabel
    e -> SomeException e

data TableNotCompatibleError
  = -- | 'unions' was called on tables that are not of the same type.
    ErrTableTypeMismatch
      Int -- ^ Vector index of table @t1@ involved in the mismatch
      Int -- ^ Vector index of table @t2@ involved in the mismatch
  | -- | 'unions' was called on tables that are not in the same session.
    ErrTableSessionMismatch
      Int -- ^ Vector index of table @t1@ involved in the mismatch
      Int -- ^ Vector index of table @t2@ involved in the mismatch
    deriving stock (Show, Eq)
    deriving anyclass (Exception)

toTableNotCompatibleError :: Internal.LSMTreeError -> SomeException
toTableNotCompatibleError = \case
    Internal.ErrUnionsTableTypeMismatch index1 index2 ->
        SomeException $ ErrTableTypeMismatch index1 index2
    Internal.ErrUnionsSessionMismatch index1 index2 ->
        SomeException $ ErrTableSessionMismatch index1 index2
    e -> SomeException e
