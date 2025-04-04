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
    newTable,
    newTableWith,
    closeTable,

    -- ** Table Lookups #table_lookups#
    member,
    members,
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
    saveSnapshot,
    withTableFromSnapshot,
    withTableFromSnapshotWith,
    openTableFromSnapshot,
    openTableFromSnapshotWith,
    doesSnapshotExist,
    deleteSnapshot,
    listSnapshots,
    SnapshotName,
    isValidSnapshotName,
    toSnapshotName,
    SnapshotLabel (..),

    -- * Table Configuration #table_configuration#
    TableConfig (..),
    MergePolicy (MergePolicyLazyLevelling),
    SizeRatio (Four),
    WriteBufferAlloc (AllocNumEntries),
    BloomFilterAlloc (AllocFixed, AllocRequestFPR),
    FencePointerIndexType (OrdinaryIndex, CompactIndex),
    DiskCachePolicy (..),
    MergeSchedule (..),

    -- ** Table Configuration Overrides #table_configuration_overrides#
    OverrideDiskCachePolicy (..),

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

    -- * Errors #errors#
    SessionDirDoesNotExistError (..),
    SessionDirLockedError (..),
    SessionDirCorruptedError (..),
    SessionClosedError (..),
    TableClosedError (..),
    TableCorruptedError (..),
    TableTooLargeError (..),
    TableUnionNotCompatibleError (..),
    SnapshotExistsError (..),
    SnapshotDoesNotExistError (..),
    SnapshotCorruptedError (..),
    SnapshotNotCompatibleError (..),
    BlobRefInvalidError (..),
    CursorClosedError (..),
    InvalidSnapshotNameError (..),
) where

import           Control.ActionRegistry (mapExceptionWithActionRegistry)
import           Control.Exception.Base (Exception, SomeException (..), assert,
                     bracket, bracketOnError)
import           Control.Monad (join)
import           Data.Bifunctor (Bifunctor (..))
import           Data.Kind (Type)
import           Data.List.NonEmpty (NonEmpty (..))
import           Data.Maybe (isJust)
import           Data.Typeable (TypeRep)
import           Data.Vector (Vector)
import qualified Data.Vector as V
import           Database.LSMTree.Internal (BlobRefInvalidError (..),
                     CursorClosedError (..), SessionClosedError (..),
                     SnapshotCorruptedError (..),
                     SnapshotDoesNotExistError (..), SnapshotExistsError (..),
                     SnapshotNotCompatibleError (..), TableClosedError (..),
                     TableCorruptedError (..), TableTooLargeError (..))
import qualified Database.LSMTree.Internal as Internal
import           Database.LSMTree.Internal.Config
                     (BloomFilterAlloc (AllocFixed, AllocRequestFPR),
                     DiskCachePolicy (..), FencePointerIndexType (..),
                     MergePolicy (..), MergeSchedule (..), SizeRatio (..),
                     TableConfig (..), WriteBufferAlloc (..),
                     defaultTableConfig)
import           Database.LSMTree.Internal.Config.Override
                     (OverrideDiskCachePolicy (..))
import qualified Database.LSMTree.Internal.Entry as Entry
import           Database.LSMTree.Internal.Paths (InvalidSnapshotNameError (..),
                     SnapshotName, isValidSnapshotName, toSnapshotName)
import           Database.LSMTree.Internal.Range (Range (..))
import           Database.LSMTree.Internal.RawBytes (RawBytes (..))
import qualified Database.LSMTree.Internal.Serialise as Internal
import           Database.LSMTree.Internal.Serialise.Class (SerialiseKey (..),
                     SerialiseValue (..), serialiseKeyIdentity,
                     serialiseKeyIdentityUpToSlicing, serialiseKeyMinimalSize,
                     serialiseValueIdentity, serialiseValueIdentityUpToSlicing)
import           Database.LSMTree.Internal.Snapshot (SnapshotLabel (..))
import qualified Database.LSMTree.Internal.Snapshot as Internal
import           Prelude hiding (lookup, take, takeWhile)
import           System.FS.API (MountPoint (..), mkFsPath)
import           System.FS.BlockIO.API (HasBlockIO (..), defaultIOCtxParams)
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

>>> :{
withSession mySessionDirectory $ \session -> do
  withTable @Word64 @ShortByteString session $ \table -> do
    insert table 0 "Hello"
    insert table 1 "World"
    lookup table 0
:}
Just "Hello"
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
| 'Table'    | 'withTable'              | 'newTable'              | 'closeTable'      |
+            +--------------------------+-------------------------+                   +
|            | 'withDuplicate'          | 'duplicate'             |                   |
+            +--------------------------+-------------------------+                   +
|            | 'withUnion'              | 'union'                 |                   |
+            +--------------------------+-------------------------+                   +
|            | 'withTableFromSnapshot'  | 'openTableFromSnapshot' |                   |
+------------+--------------------------+-------------------------+-------------------+
| 'Cursor'   | 'withCursor'             | 'newCursor'             | 'closeCursor'     |
+------------+--------------------------+-------------------------+-------------------+

To prevent resource and memory leaks due to asynchronous exceptions,
it is recommended to use the [bracketed](#bracketed) functions whenever
possible, and otherwise:

*   Run functions that allocate and release a resource with asynchronous
    exceptions masked.
*   Ensure that every use allocate operation is followed by the corresponding release
    operation even in the presence of asynchronous exceptions, e.g., using 'bracket'.
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

* The read operations are 'lookup', 'rangeLookup', 'duplicate', `union`, 'saveSnapshot', 'newCursor', and their variants.
* The write operations are 'insert', 'delete', 'update', 'closeTable', and their variants.

It is possible to read from a stable view of a table while concurrently writing to
the table by using 'duplicate' and performing the read operations on the duplicate.
However, this requires that the 'duplicate' operation /happens before/ the subsequent
writes, as it is a race to duplicate concurrently with any writes.
As this package does not provide any construct for synchronisation or atomic
operations, this ordering of operations must be accomplished by the user through
other means.

A 'Cursor' creates a stable view of a table and can safely be read while
modifying the original table. However, reading the 'next' key\/value pair from
a cursor locks the view, so concurrent reads on the same cursor block.
This is because 'next' updates the cursor's current position.

Session handles may be used concurrently from multiple Haskell threads,
but concurrent use of read and write operations may introduce races.
Specifically, it is a race to use `listSnapshots` and `deleteSnapshots`
with the same session handle concurrently.
-}

--------------------------------------------------------------------------------
-- Sharing
--------------------------------------------------------------------------------

{- $sharing
Tables created via 'duplicate' or 'union' will initially share as much of their
in-memory and on-disk data as possible with the tables they were created from.
Over time as these related tables are modified, the contents of the tables will
diverge, which means that the tables will share less and less.

Sharing of in-memory data is not preserved by snapshots, but sharing of on-disk
data is partially preserved.
Existing files for runs are shared, but files for ongoing merges are not.
Opening a table from a snapshot (using 'openTableFromSnapshot' or
'withTableFromSnapshot') is expensive, but creating a snapshot (using
'saveSnapshot') is relatively cheap.
-}

--------------------------------------------------------------------------------
-- Sessions
--------------------------------------------------------------------------------

{- | A session stores context that is shared by multiple tables.

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

['SessionDirDoesNotExistError']:
    If the session directory does not exist.
['SessionDirLockedError']:
    If the session directory is locked by another process.
['SessionDirCorruptedError']:
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
    _convertSessionDirErrors dir $
        withIOHasBlockIO hasFS defaultIOCtxParams $ \hasBlockIO ->
            Internal.withSession tracer hasFS hasBlockIO rootDir (action . Session)

{- |
Throws the following exceptions:

['SessionDirDoesNotExistError']:
    If the session directory does not exist.
['SessionDirLockedError']:
    If the session directory is locked by another process.
['SessionDirCorruptedError']:
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
    _convertSessionDirErrors dir $ do
        bracketOnError (acquire hasFS) release $ \hasBlockIO ->
            Session <$> Internal.openSession tracer hasFS hasBlockIO rootDir
  where
    acquire hasFS = ioHasBlockIO hasFS defaultIOCtxParams
    release = \HasBlockIO{close} -> close

closeSession :: Session -> IO ()
closeSession = Internal.closeSession . unSession

--------------------------------------------------------------------------------
-- Tables
--------------------------------------------------------------------------------

{- | A table is a handle to an individual LSM-tree key\/value store with both in-memory and on-disk parts.

  __Warning:__ Tables are ephemeral. Once you close a table, its data is lost forever. To persist tables, use [snapshots](#g:snapshots).
-}
type role Table nominal nominal

type Table :: Type -> Type -> Type
data Table k v
    = (SerialiseKey k, SerialiseValue v) =>
    Table {unTable :: {-# UNPACK #-} !(Internal.Table IO HandleIO)}

{- | Run an action with access to an empty table.

This function is exception-safe for both synchronous and asynchronous exceptions.

It is recommended to use this function instead of 'newTable' and 'closeTable'.

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
    Internal.withTable (unSession session) tableConfig (action . Table)

{- | Create an empty table.

__Warning:__ Tables hold open resources and must be closed using 'closeTable'.

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
-}
newTable ::
    (SerialiseKey k, SerialiseValue v) =>
    Session ->
    IO (Table k v)
newTable session =
    newTableWith defaultTableConfig session

-- | Variant of 'newTable' that accepts [table configuration](#g:table_configuration).
newTableWith ::
    (SerialiseKey k, SerialiseValue v) =>
    TableConfig ->
    Session ->
    IO (Table k v)
newTableWith tableConfig (Session session) =
    Table <$> Internal.new session tableConfig

{- | Close a table.

Closing is idempotent, i.e., closing a closed table does nothing.
All other operations on a closed table will throw an exception.

  __Warning:__ Tables are ephemeral. Once you close a table, its data is lost forever. To persist tables, use [snapshots](#g:snapshots).
-}
closeTable :: Table k v -> IO ()
closeTable (Table table) =
    Internal.close table

--------------------------------------------------------------------------------
-- Lookups
--------------------------------------------------------------------------------

{- | Check if the key is a member of the table.

Membership tests can be performed concurrently from multiple Haskell threads.

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
['TableClosedError']:
    If the table is closed.
['TableCorruptedError']:
    If the table data is corrupted.
-}
member ::
    Table k v ->
    k ->
    IO Bool
member = (fmap isJust .) . lookup

{- | Variant of 'member' for batch membership tests.
The batch of keys corresponds in-order to the batch of results.

The following property holds in the absence of races:

prop> members table keys = traverse (member table) keys
-}
members ::
    Table k v ->
    Vector k ->
    IO (Vector Bool)
members = (fmap (fmap isJust) .) . lookups

{- | Look up the value associated with a key.

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
The batch of keys corresponds in-order to the batch of results.

The following property holds in the absence of races:

prop> lookups table keys = traverse (lookup table) keys
-}
lookups ::
    Table k v ->
    Vector k ->
    IO (Vector (Maybe v))
lookups (Table table) keys = do
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
    Internal.rangeLookup const (Internal.serialiseKey <$> range) table $ \ !k !v !b ->
        assert (null b) $
            (Internal.deserialiseKey k, Internal.deserialiseValue v)

--------------------------------------------------------------------------------
-- Updates
--------------------------------------------------------------------------------

{- | Insert a new key and value in the table.

If the key is already present in the table, the associated value is replaced with the given value.

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

{- | Delete a key and its value from the table.

If the key is not present in the table, the table is left unchanged.

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

{- | Update the value at a specific key:

* If the given value is 'Just', this operation acts as 'insert'.
* If the given value is 'Nothing', this operation acts as 'delete'.

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
    Internal.updates const (serialiseUpdate <$> entries) table
  where
    serialiseUpdate (k, Just v) = (Internal.serialiseKey k, Entry.Insert (Internal.serialiseValue v))
    serialiseUpdate (k, Nothing) = (Internal.serialiseKey k, Entry.Delete)

--------------------------------------------------------------------------------
-- Duplication
--------------------------------------------------------------------------------

{- | Run an action with access to the duplicate of a table.

This function is exception-safe for both synchronous and asynchronous exceptions.

It is recommended to use this function instead of 'duplicate' and 'closeTable'.

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
    bracket (duplicate table) closeTable

{- | Duplicate a table.

The duplicate is an independent copy of the given table.
The duplicate is unaffected by subsequent updates to the given table and vice versa.

__Warning:__ The duplicate must be independently closed using 'closeTable'.

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
    Table <$> Internal.duplicate table

--------------------------------------------------------------------------------
-- Union
--------------------------------------------------------------------------------

{- | Run an action with access to a table that contains the union of the entries of the given tables.

This function is exception-safe for both synchronous and asynchronous exceptions.

It is recommended to use this function instead of 'union' and 'closeTable'.

__Warning:__ Both input tables must be from the same 'Session'.

__Warning:__ This is a relatively expensive operation that may take some time to complete.

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
['TableClosedError']:
    If the table is closed.
['TableUnionNotCompatibleError']:
    If both tables are not from the same 'Session'.
-}
withUnion ::
    Table k v ->
    Table k v ->
    (Table k v -> IO a) ->
    IO a
withUnion table1 table2 =
    bracket (table1 `union` table2) closeTable

-- | Variant of 'withUnions' for any number of tables.
withUnions ::
    NonEmpty (Table k v) ->
    (Table k v -> IO a) ->
    IO a
withUnions tables =
    bracket (unions tables) closeTable

{- | Create a table that contains the union of the entries of the given tables.

The union of two tables is left-biased.

__Warning:__ The new table must be independently closed using 'closeTable'.

__Warning:__ Both input tables must be from the same 'Session'.

__Warning:__ This is a relatively expensive operation that may take some time to complete.

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
['TableClosedError']:
    If the table is closed.
['TableUnionNotCompatibleError']:
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
unions (Table table :| tables) = do
    -- TODO: add variant of 'unions' that incrementally computes unions
    bracketOnError acquireUnionOfTables Internal.close $ \unionOfTables -> do
      Internal.UnionDebt debt <- Internal.remainingUnionDebt unionOfTables
      leftovers <- Internal.supplyUnionCredits const unionOfTables (Internal.UnionCredits debt)
      assert (leftovers >= 0) $ pure ()
      pure $ Table unionOfTables
  where
    acquireUnionOfTables = Internal.unions (table :| fmap unTable tables)

--------------------------------------------------------------------------------
-- Snapshots
--------------------------------------------------------------------------------

{- | Save the current state of the table to disk as a snapshot under the given
snapshot name. This is the /only/ mechanism that persists a table. Each snapshot
must have a unique name, which may be used to restore the table from that snapshot
using 'openTableFromSnapshot'.
Saving a snapshot /does not/ close the table.

Saving a snapshot is /relatively/ cheap when compared to opening a snapshot.
However, it is not so cheap that one should use it after every operation.
At very least, saving a snapshot requires writing the in-memory buffer to disk.

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
['TableClosedError']:
    If the table is closed.
['SnapshotExistsError']:
    If a snapshot with the same name already exists.
-}
saveSnapshot ::
    SnapshotName ->
    SnapshotLabel ->
    Table k v ->
    IO ()
saveSnapshot snapName snapLabel (Table table) =
    -- TODO: remove SnapshotTableType
    Internal.createSnapshot snapName snapLabel Internal.SnapSimpleTable table

{- | Run an action with access to a table from a snapshot.

This function is exception-safe for both synchronous and asynchronous exceptions.

It is recommended to use this function instead of 'openTableFromSnapshot' and 'closeTable'.

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
    bracket (openTableFromSnapshot session snapName snapLabel) closeTable

-- | Variant of 'withTableFromSnapshot' that accepts [table configuration overrides](#g:table_configuration_overrides).
withTableFromSnapshotWith ::
    (SerialiseKey k, SerialiseValue v) =>
    OverrideDiskCachePolicy ->
    Session ->
    SnapshotName ->
    SnapshotLabel ->
    (Table k v -> IO a) ->
    IO a
withTableFromSnapshotWith tableConfigOverride session snapName snapLabel =
    bracket (openTableFromSnapshotWith session tableConfigOverride snapName snapLabel) closeTable

{- | Open a table from a named snapshot.

__Warning:__ The new table must be independently closed using 'closeTable'.

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
    openTableFromSnapshotWith session NoOverrideDiskCachePolicy snapName snapLabel

-- | Variant of 'openTableFromSnapshot' that accepts [table configuration overrides](#g:table_configuration_overrides).
openTableFromSnapshotWith ::
    (SerialiseKey k, SerialiseValue v) =>
    Session ->
    OverrideDiskCachePolicy ->
    SnapshotName ->
    SnapshotLabel ->
    IO (Table k v)
openTableFromSnapshotWith (Session session) tableConfigOverride snapName snapLabel =
    Table <$> Internal.openSnapshot session tableConfigOverride snapLabel Internal.SnapSimpleTable snapName const

{- | Delete the named snapshot.

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
deleteSnapshot (Session session) =
    Internal.deleteSnapshot session

{- | Check if the named snapshot exists.

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
-}
doesSnapshotExist ::
    Session ->
    SnapshotName ->
    IO Bool
doesSnapshotExist (Session session) =
    Internal.doesSnapshotExist session

{- | List the names of all snapshots.

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
-}
listSnapshots ::
    Session ->
    IO [SnapshotName]
listSnapshots (Session session) =
    Internal.listSnapshots session

--------------------------------------------------------------------------------
-- Cursors
--------------------------------------------------------------------------------

{- | A cursor is a stable read-only iterator for a table.

A cursor iterates over the entries in a table following the order of the
serialised keys. After the cursor is created, updates to the referenced table
do not affect the cursor.

The name of this type references [database cursors](https://en.wikipedia.org/wiki/Cursor_(databases\)), not, e.g., text editor cursors.
-}
type role Cursor nominal nominal

type Cursor :: Type -> Type -> Type
data Cursor k v
    = (SerialiseKey k, SerialiseValue v) =>
    Cursor {unCursor :: {-# UNPACK #-} !(Internal.Cursor IO HandleIO)}

{- | Run an action with access to a cursor.

This function is exception-safe for both synchronous and asynchronous exceptions.

It is recommended to use this function instead of 'newCursor' and 'closeCursor'.

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
['TableClosedError']:
    If the table is closed.
-}
withCursor ::
    Table k v ->
    (Cursor k v -> IO a) ->
    IO a
withCursor (Table table) action =
    Internal.withCursor Internal.NoOffsetKey table (action . Cursor)

-- | Variant of 'withCursor' that starts at a given key.
withCursorAtOffset ::
    Table k v ->
    k ->
    (Cursor k v -> IO a) ->
    IO a
withCursorAtOffset (Table table) offsetKey action =
    Internal.withCursor (Internal.OffsetKey $ Internal.serialiseKey offsetKey) table (action . Cursor)

{- | Create a cursor for the given table.

__Warning:__ Cursors hold open resources and must be closed using 'closeCursor'.

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
['TableClosedError']:
    If the table is closed.
-}
newCursor ::
    Table k v ->
    IO (Cursor k v)
newCursor (Table table) =
    Cursor <$> Internal.newCursor Internal.NoOffsetKey table

-- | Variant of 'newCursor' that starts at a given key.
newCursorAtOffset ::
    Table k v ->
    k ->
    IO (Cursor k v)
newCursorAtOffset (Table table) offsetKey =
    Cursor <$> Internal.newCursor (Internal.OffsetKey $ Internal.serialiseKey offsetKey) table

{- | Close a cursor.

Closing is idempotent, i.e., closing a closed cursor does nothing.
All other operations on a closed cursor will throw an exception.
-}
closeCursor ::
    Cursor k v ->
    IO ()
closeCursor = Internal.closeCursor . unCursor

{- | Read the next table entry from the cursor.

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
['CursorClosedError']:
    If the cursor is closed.
-}
next ::
    Cursor k v ->
    IO (Maybe (k, v))
next iterator = do
    entries <- take 1 iterator
    pure $ fst <$> V.uncons entries

{- | Read the next batch of table entries from the cursor.

The size of the batch is /at most/ equal to the given number, but may contain fewer entries.
In practice, this occurs only when the cursor reaches the end of the table.

The following property holds:

prop> take n cursor = catMaybes <$> replicateM n (next cursor)

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
['CursorClosedError']:
    If the cursor is closed.
-}
take ::
    Int ->
    Cursor k v ->
    IO (Vector (k, v))
take n (Cursor cursor) =
    Internal.readCursor const n cursor $ \ !k !v !b ->
        assert (null b) $
            (Internal.deserialiseKey k, Internal.deserialiseValue v)

{- | Variant of 'take' that accepts an additional predicate to determine whether or not to continue reading.

The following properties hold:

prop> takeWhile n (const True) cursor = take n cursor
prop> takeWhile n (const False) cursor = pure empty

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
['CursorClosedError']:
    If the cursor is closed.
-}
takeWhile ::
    Int ->
    (k -> Bool) ->
    Cursor k v ->
    IO (Vector (k, v))
takeWhile n p (Cursor cursor) =
    -- TODO: Rewrite to use a variant of 'readCursorWhile' that works without the maximum batch size.
    Internal.readCursorWhile const (p . Internal.deserialiseKey) n cursor $ \ !k !v !_b ->
        (Internal.deserialiseKey k, Internal.deserialiseValue v)

--------------------------------------------------------------------------------
-- Errors
--------------------------------------------------------------------------------

-- | The session directory does not exist.
data SessionDirDoesNotExistError
    = ErrSessionDirDoesNotExist !FilePath
    deriving stock (Show, Eq)
    deriving anyclass (Exception)

-- | The session directory is locked by another active session.
data SessionDirLockedError
    = ErrSessionDirLocked !FilePath
    deriving stock (Show, Eq)
    deriving anyclass (Exception)

-- | The session directory is corrupted, e.g., it misses required files or contains unexpected files.
data SessionDirCorruptedError
    = ErrSessionDirCorrupted !FilePath
    deriving stock (Show, Eq)
    deriving anyclass (Exception)

{- | Internal helper. Convert:

    * t'Internal.SessionDirDoesNotExistError' to t'SessionDirDoesNotExistError';
    * t'Internal.SessionDirLockedError' to t'SessionDirLockedError'; and
    * t'Internal.SessionDirCorruptedError' to t'SessionDirCorruptedError'.
-}
_convertSessionDirErrors :: FilePath -> IO a -> IO a
_convertSessionDirErrors sessionDir =
    mapExceptionWithActionRegistry (\(Internal.ErrSessionDirDoesNotExist _fsErrorPath) -> SomeException $ ErrSessionDirDoesNotExist sessionDir)
        . mapExceptionWithActionRegistry (\(Internal.ErrSessionDirLocked _fsErrorPath) -> SomeException $ ErrSessionDirLocked sessionDir)
        . mapExceptionWithActionRegistry (\(Internal.ErrSessionDirCorrupted _fsErrorPath) -> SomeException $ ErrSessionDirCorrupted sessionDir)

{-------------------------------------------------------------------------------
   Table union
-------------------------------------------------------------------------------}

-- | A table union was constructed with two tables that are not compatible.
data TableUnionNotCompatibleError
    = ErrTableUnionHandleTypeMismatch
        -- | The index of the first table.
        !Int
        -- | The type of the filesystem handle of the first table.
        !TypeRep
        -- | The index of the second table.
        !Int
        -- | The type of the filesystem handle of the second table.
        !TypeRep
    | ErrTableUnionSessionMismatch
        -- | The index of the first table.
        !Int
        -- | The session directory of the first table.
        !FilePath
        -- | The index of the second table.
        !Int
        -- | The session directory of the second table.
        !FilePath
    deriving stock (Show, Eq)
    deriving anyclass (Exception)

{- | Internal helper. Convert:

    * t'Internal.SessionDirDoesNotExistError' to t'SessionDirDoesNotExistError';
    * t'Internal.SessionDirLockedError' to t'SessionDirLockedError'; and
    * t'Internal.SessionDirCorruptedError' to t'SessionDirCorruptedError'.
-}
_convertTableUnionNotCompatibleError :: (Int -> FilePath) -> IO a -> IO a
_convertTableUnionNotCompatibleError sessionDirFor =
    mapExceptionWithActionRegistry $ \case
        Internal.ErrTableUnionHandleTypeMismatch i1 typeRep1 i2 typeRep2 ->
            ErrTableUnionHandleTypeMismatch i1 typeRep1 i2 typeRep2
        Internal.ErrTableUnionSessionMismatch i1 _fsErrorPath1 i2 _fsErrorPath2 ->
            ErrTableUnionSessionMismatch i1 (sessionDirFor i1) i2 (sessionDirFor i2)
