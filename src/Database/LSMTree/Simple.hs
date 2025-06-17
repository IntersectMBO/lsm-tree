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

    -- ** ACID properties #acid#
    -- $acid

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
    withIncrementalUnion,
    withIncrementalUnions,
    incrementalUnion,
    incrementalUnions,
    remainingUnionDebt,
    supplyUnionCredits,

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
    TableConfig (
        confMergePolicy,
        confSizeRatio,
        confWriteBufferAlloc,
        confBloomFilterAlloc,
        confFencePointerIndex,
        confDiskCachePolicy,
        confMergeSchedule
    ),
    MergePolicy (LazyLevelling),
    SizeRatio (Four),
    WriteBufferAlloc (AllocNumEntries),
    BloomFilterAlloc (AllocFixed, AllocRequestFPR),
    FencePointerIndexType (OrdinaryIndex, CompactIndex),
    DiskCachePolicy (..),
    MergeSchedule (..),
    MergeBatchSize (..),

    -- ** Table Configuration Overrides #table_configuration_overrides#
    OverrideDiskCachePolicy (..),

    -- * Ranges #ranges#
    Range (..),

    -- * Union Credit and Debt
    UnionCredits (..),
    UnionDebt (..),

    -- * Key\/Value Serialisation #key_value_serialisation#
    RawBytes (RawBytes),
    SerialiseKey (serialiseKey, deserialiseKey),
    SerialiseKeyOrderPreserving,
    SerialiseValue (serialiseValue, deserialiseValue),

    -- ** Key\/Value Serialisation Property Tests #key_value_serialisation_property_tests#
    serialiseKeyIdentity,
    serialiseKeyIdentityUpToSlicing,
    serialiseKeyPreservesOrdering,
    serialiseKeyMinimalSize,
    serialiseValueIdentity,
    serialiseValueIdentityUpToSlicing,
    packSlice,

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
    CursorClosedError (..),
    InvalidSnapshotNameError (..),
) where

import           Control.ActionRegistry (mapExceptionWithActionRegistry)
import           Control.Exception.Base (Exception, SomeException (..))
import           Data.Bifunctor (Bifunctor (..))
import           Data.Coerce (coerce)
import           Data.Kind (Type)
import           Data.List.NonEmpty (NonEmpty (..))
import           Data.Typeable (TypeRep)
import           Data.Vector (Vector)
import           Data.Void (Void)
import           Database.LSMTree (BloomFilterAlloc, CursorClosedError (..),
                     DiskCachePolicy, FencePointerIndexType,
                     InvalidSnapshotNameError (..), MergeBatchSize, MergePolicy,
                     MergeSchedule, OverrideDiskCachePolicy (..), Range (..),
                     RawBytes, ResolveAsFirst (..), SerialiseKey (..),
                     SerialiseKeyOrderPreserving, SerialiseValue (..),
                     SessionClosedError (..), SizeRatio,
                     SnapshotCorruptedError (..),
                     SnapshotDoesNotExistError (..), SnapshotExistsError (..),
                     SnapshotLabel (..), SnapshotName,
                     SnapshotNotCompatibleError (..), TableClosedError (..),
                     TableConfig (..), TableCorruptedError (..),
                     TableTooLargeError (..), UnionCredits (..), UnionDebt (..),
                     WriteBufferAlloc, isValidSnapshotName, packSlice,
                     serialiseKeyIdentity, serialiseKeyIdentityUpToSlicing,
                     serialiseKeyMinimalSize, serialiseKeyPreservesOrdering,
                     serialiseValueIdentity, serialiseValueIdentityUpToSlicing,
                     toSnapshotName)
import qualified Database.LSMTree as LSMT
import           Prelude hiding (lookup, take, takeWhile)

--------------------------------------------------------------------------------
-- Example
--------------------------------------------------------------------------------

{- $setup
>>> import Prelude hiding (lookup)
>>> import Data.ByteString.Short (ShortByteString)
>>> import Data.String (IsString)
>>> import Data.Word (Word64)
>>> import System.Directory (createDirectoryIfMissing, getTemporaryDirectory)
>>> import System.FilePath ((</>))

>>> :{
newtype Key = Key Word64
  deriving stock (Eq, Show)
  deriving newtype (Num, SerialiseKey)
:}

>>> :{
newtype Value = Value ShortByteString
  deriving stock (Eq, Show)
  deriving newtype (IsString, SerialiseValue)
:}

>>> :{
runExample :: (Session -> Table Key Value -> IO a) -> IO a
runExample action = do
  tmpDir <- getTemporaryDirectory
  let sessionDir = tmpDir </> "doctest_Database_LSMTree_Simple"
  createDirectoryIfMissing True sessionDir
  withSession sessionDir $ \session ->
    withTable session $ \table ->
      action session table
:}
-}

{- $example

>>> :{
runExample $ \session table -> do
  insert table 0 "Hello"
  insert table 1 "World"
  lookup table 0
:}
Just (Value "Hello")
-}

--------------------------------------------------------------------------------
-- Resource Management
--------------------------------------------------------------------------------

{- $resource_management
This package uses explicit resource management. The 'Session', 'Table', and 'Cursor'
handles hold open resources, such as file handles, which must be explicitly released.
Every operation that allocates a resource is paired with another operation to releases
that resource. For each pair of allocate and release operations there is a bracketed
function that combines the two.

+------------+--------------------------+-------------------------+-------------------+
| Resource   | Bracketed #bracketed#    | Allocate #allocate#     | Release #release# |
+============+==========================+=========================+===================+
| 'Session'  | 'withSession'            | 'openSession'           | 'closeSession'    |
+------------+--------------------------+-------------------------+-------------------+
| 'Table'    | 'withTable'              | 'newTable'              | 'closeTable'      |
+            +--------------------------+-------------------------+                   +
|            | 'withDuplicate'          | 'duplicate'             |                   |
+            +--------------------------+-------------------------+                   +
|            | 'withUnion'              | 'union'                 |                   |
+            +--------------------------+-------------------------+                   +
|            | 'withIncrementalUnion'   | 'incrementalUnion'      |                   |
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
-- ACID properties
--------------------------------------------------------------------------------

{- $acid
This text copies liberally from https://en.wikipedia.org/wiki/ACID and related wiki pages.

Atomicity, consistency, isolation, and durability (ACID) are important
properties of database transactions.
They guarantee data validity despite errors, power failures, and other mishaps.
A /transaction/ is a sequence of database operations that satisfy the ACID properties.

@lsm-tree@ does not support transactions in the typical sense that many relational databases do,
where transactions can be built from smaller components/actions,
e.g., reads and writes of individual cells.
Instead, the public API only exposes functions that individually form a transaction;
there are no smaller building blocks.
An example of such a transaction is 'updates'.

An @lsm-tree@ transaction still perform multiple database actions /internally/,
but transactions themselves are not composable into larger transactions,
so it should be expected that table contents can change between transactions in a concurrent setting.
A consistent view of a table can be created,
so that independent transactions have access to their own version of the database state (see [concurrency](#g:concurreny)).

All @lsm-tree@ transactions are designed for atomicity, consistency, and isolation (ACI),
assuming that users of the library perform proper [resource management](#g:resource-management).
Durability is only guaranteed when saving a [snapshot](#g:snapshots),
which is the only method of stopping and restarting tables.

We currently cannot guarantee consistency in the presence of synchronous and asynchronous exceptions,
eventhough major strides were made to make it so.
The safest course of action when an internal exception is encountered is to stop and restart:
close the session along with all its tables and cursors, reopen the session,
and load a previous saved table snapshot.
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

{- |
A session stores context that is shared by multiple tables.

Each session is associated with one session directory where the files
containing table data are stored. Each session locks its session directory.
There can only be one active session for each session directory at a time.
If a database is must be accessed from multiple parts of a program,
one session should be opened and shared between those parts of the program.
Session directories cannot be shared between OS processes.
-}
type Session :: Type
newtype Session = Session (LSMT.Session IO)

{- |
Run an action with access to a session opened from a session directory.

If there are no open tables or cursors when the session terminates, then the disk I\/O complexity of this operation is \(O(1)\).
Otherwise, 'closeTable' is called for each open table and 'closeCursor' is called for each open cursor.
Consequently, the worst-case disk I\/O complexity of this operation depends on the merge policy of the open tables in the session.
The following assumes all tables in the session have the same merge policy:

['LazyLevelling']:
  \(O(o \: T \log_T \frac{n}{B})\).

The variable \(o\) refers to the number of open tables and cursors in the session.

This function is exception-safe for both synchronous and asynchronous exceptions.

It is recommended to use this function instead of 'openSession' and 'closeSession'.

Throws the following exceptions:

['SessionDirDoesNotExistError']:
    If the session directory does not exist.
['SessionDirLockedError']:
    If the session directory is locked by another process.
['SessionDirCorruptedError']:
    If the session directory is malformed.
-}
withSession ::
    forall a.
    -- | The session directory.
    FilePath ->
    (Session -> IO a) ->
    IO a
withSession dir action = do
    let tracer = mempty
    _convertSessionDirErrors dir $
        LSMT.withSessionIO tracer dir (action . Session)

{- |
Open a session from a session directory.

The worst-case disk I\/O complexity of this operation is \(O(1)\).

__Warning:__ Sessions hold open resources and must be closed using 'closeSession'.

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
    _convertSessionDirErrors dir $ do
        Session <$> LSMT.openSessionIO tracer dir

{- |
Close a session.

If there are no open tables or cursors in the session, then the disk I\/O complexity of this operation is \(O(1)\).
Otherwise, 'closeTable' is called for each open table and 'closeCursor' is called for each open cursor.
Consequently, the worst-case disk I\/O complexity of this operation depends on the merge policy of the tables in the session.
The following assumes all tables in the session have the same merge policy:

['LazyLevelling']:
  \(O(o \: T \log_T \frac{n}{B})\).

The variable \(o\) refers to the number of open tables and cursors in the session.

Closing is idempotent, i.e., closing a closed session does nothing.
All other operations on a closed session will throw an exception.
-}
closeSession ::
    Session ->
    IO ()
closeSession (Session session) =
    LSMT.closeSession session

--------------------------------------------------------------------------------
-- Tables
--------------------------------------------------------------------------------

{- |
A table is a handle to an individual LSM-tree key\/value store with both in-memory and on-disk parts.

__Warning:__ Tables are ephemeral. Once you close a table, its data is lost forever. To persist tables, use [snapshots](#g:snapshots).
-}
type role Table nominal nominal

type Table :: Type -> Type -> Type
newtype Table k v = Table (LSMT.Table IO k (LSMT.ResolveAsFirst v) Void)

{- |
Run an action with access to an empty table.

The worst-case disk I\/O complexity of this operation depends on the merge policy of the table:

['LazyLevelling']:
    \(O(T \log_T \frac{n}{B})\).

This function is exception-safe for both synchronous and asynchronous exceptions.

It is recommended to use this function instead of 'newTable' and 'closeTable'.

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
-}
withTable ::
    forall k v a.
    Session ->
    (Table k v -> IO a) ->
    IO a
withTable (Session session) action =
    LSMT.withTable session (action . Table)

-- | Variant of 'withTable' that accepts [table configuration](#g:table_configuration).
withTableWith ::
    forall k v a.
    TableConfig ->
    Session ->
    (Table k v -> IO a) ->
    IO a
withTableWith tableConfig (Session session) action =
    LSMT.withTableWith tableConfig session (action . Table)

{- |
Create an empty table.

The worst-case disk I\/O complexity of this operation is \(O(1)\).

__Warning:__ Tables hold open resources and must be closed using 'closeTable'.

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
-}
newTable ::
    forall k v.
    Session ->
    IO (Table k v)
newTable (Session session) =
    Table <$> LSMT.newTable session

{- |
Variant of 'newTable' that accepts [table configuration](#g:table_configuration).
-}
newTableWith ::
    forall k v.
    TableConfig ->
    Session ->
    IO (Table k v)
newTableWith tableConfig (Session session) =
    Table <$> LSMT.newTableWith tableConfig session

{- |
Close a table.

The worst-case disk I\/O complexity of this operation depends on the merge policy of the table:

['LazyLevelling']:
    \(O(T \log_T \frac{n}{B})\).

Closing is idempotent, i.e., closing a closed table does nothing.
All other operations on a closed table will throw an exception.

__Warning:__ Tables are ephemeral. Once you close a table, its data is lost forever. To persist tables, use [snapshots](#g:snapshots).
-}
closeTable ::
    forall k v.
    Table k v ->
    IO ()
closeTable (Table table) =
    LSMT.closeTable table

--------------------------------------------------------------------------------
-- Lookups
--------------------------------------------------------------------------------

{- |
Check if the key is a member of the table.

The worst-case disk I\/O complexity of this operation depends on the merge policy of the table:

['LazyLevelling']:
    \(O(T \log_T \frac{n}{B})\).

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
    forall k v.
    (SerialiseKey k, SerialiseValue v) =>
    Table k v ->
    k ->
    IO Bool
member (Table table) =
    LSMT.member table

{- |
Variant of 'member' for batch membership tests.
The batch of keys corresponds in-order to the batch of results.

The worst-case disk I\/O complexity of this operation depends on the merge policy of the table:

['LazyLevelling']:
    \(O(b \: T \log_T \frac{n}{B})\).

The variable \(b\) refers to the length of the input vector.

The following property holds in the absence of races:

prop> members table keys = traverse (member table) keys
-}
members ::
    forall k v.
    (SerialiseKey k, SerialiseValue v) =>
    Table k v ->
    Vector k ->
    IO (Vector Bool)
members (Table table) =
    LSMT.members table

-- | Internal helper. Get the value from a 'LSMT.LookupResult' from the full API.
getValue :: LSMT.LookupResult (ResolveAsFirst v) (LSMT.BlobRef IO Void) -> Maybe v
getValue =
    fmap LSMT.unResolveAsFirst . LSMT.getValue

{- |
Look up the value associated with a key.

The worst-case disk I\/O complexity of this operation depends on the merge policy of the table:

['LazyLevelling']:
    \(O(T \log_T \frac{n}{B})\).

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
    forall k v.
    (SerialiseKey k, SerialiseValue v) =>
    Table k v ->
    k ->
    IO (Maybe v)
lookup (Table table) =
    fmap getValue . LSMT.lookup table

{- |
Variant of 'lookup' for batch lookups.
The batch of keys corresponds in-order to the batch of results.

The worst-case disk I\/O complexity of this operation depends on the merge policy of the table:

['LazyLevelling']:
    \(O(b \: T \log_T \frac{n}{B})\).

The variable \(b\) refers to the length of the input vector.

The following property holds in the absence of races:

prop> lookups table keys = traverse (lookup table) keys
-}
lookups ::
    forall k v.
    (SerialiseKey k, SerialiseValue v) =>
    Table k v ->
    Vector k ->
    IO (Vector (Maybe v))
lookups (Table table) =
    fmap (fmap getValue) . LSMT.lookups table

-- | Internal helper. Get a key\/value pair from an 'LSMT.Entry' from the full API.
getKeyValue :: LSMT.Entry k (ResolveAsFirst v) (LSMT.BlobRef IO Void) -> (k, v)
getKeyValue (LSMT.Entry k v)             = (k, LSMT.unResolveAsFirst v)
getKeyValue (LSMT.EntryWithBlob k v !_b) = (k, LSMT.unResolveAsFirst v)

{- |
Look up a batch of values associated with keys in the given range.

The worst-case disk I\/O complexity of this operation is \(O(T \log_T \frac{n}{B} + \frac{b}{P})\),
where the variable \(b\) refers to the length of the /output/ vector.

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
    forall k v.
    (SerialiseKey k, SerialiseValue v) =>
    Table k v ->
    Range k ->
    IO (Vector (k, v))
rangeLookup (Table table) =
    fmap (fmap getKeyValue) . LSMT.rangeLookup table

--------------------------------------------------------------------------------
-- Updates
--------------------------------------------------------------------------------

{- |
Insert a new key and value in the table.
If the key is already present in the table, the associated value is replaced with the given value.

The worst-case disk I\/O complexity of this operation depends on the merge policy and the merge schedule of the table:

['LazyLevelling'\/'Incremental']:
    \(O(\frac{1}{P} \: \log_T \frac{n}{B})\).
['LazyLevelling'\/'OneShot']:
    \(O(\frac{n}{P})\).

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
['TableClosedError']:
    If the table is closed.
-}
insert ::
    forall k v.
    (SerialiseKey k, SerialiseValue v) =>
    Table k v ->
    k ->
    v ->
    IO ()
insert (Table table) k v =
    LSMT.insert table k (LSMT.ResolveAsFirst v) Nothing

{- |
Variant of 'insert' for batch insertions.

The worst-case disk I\/O complexity of this operation depends on the merge policy and the merge schedule of the table:

['LazyLevelling'\/'Incremental']:
    \(O(b \: \frac{1}{P} \: \log_T \frac{n}{B})\).
['LazyLevelling'\/'OneShot']:
    \(O(\frac{b}{P} \log_T \frac{b}{B} + \frac{n}{P})\).

The variable \(b\) refers to the length of the input vector.

The following property holds in the absence of races:

prop> inserts table entries = traverse_ (uncurry $ insert table) entries
-}
inserts ::
    forall k v.
    (SerialiseKey k, SerialiseValue v) =>
    Table k v ->
    Vector (k, v) ->
    IO ()
inserts (Table table) entries =
    LSMT.inserts table (fmap (\(k, v) -> (k, LSMT.ResolveAsFirst v, Nothing)) entries)

{- |
Delete a key and its value from the table.
If the key is not present in the table, the table is left unchanged.

The worst-case disk I\/O complexity of this operation depends on the merge policy and the merge schedule of the table:

['LazyLevelling'\/'Incremental']:
    \(O(\frac{1}{P} \: \log_T \frac{n}{B})\).
['LazyLevelling'\/'OneShot']:
    \(O(\frac{n}{P})\).

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
['TableClosedError']:
    If the table is closed.
-}
delete ::
    forall k v.
    (SerialiseKey k, SerialiseValue v) =>
    Table k v ->
    k ->
    IO ()
delete (Table table) =
    LSMT.delete table

{- |
Variant of 'delete' for batch deletions.

The worst-case disk I\/O complexity of this operation depends on the merge policy and the merge schedule of the table:

['LazyLevelling'\/'Incremental']:
    \(O(b \: \frac{1}{P} \: \log_T \frac{n}{B})\).
['LazyLevelling'\/'OneShot']:
    \(O(\frac{b}{P} \log_T \frac{b}{B} + \frac{n}{P})\).

The variable \(b\) refers to the length of the input vector.

The following property holds in the absence of races:

prop> deletes table keys = traverse_ (delete table) keys
-}
deletes ::
    forall k v.
    (SerialiseKey k, SerialiseValue v) =>
    Table k v ->
    Vector k ->
    IO ()
deletes (Table table) =
    LSMT.deletes table

-- | Internal helper. Convert from @'Maybe' v@ to an 'LSMT.Update'.
maybeValueToUpdate :: Maybe v -> LSMT.Update (ResolveAsFirst v) Void
maybeValueToUpdate =
    maybe LSMT.Delete (\v -> LSMT.Insert (LSMT.ResolveAsFirst v) Nothing)

{- |
Update the value at a specific key:

* If the given value is 'Just', this operation acts as 'insert'.
* If the given value is 'Nothing', this operation acts as 'delete'.

The worst-case disk I\/O complexity of this operation depends on the merge policy and the merge schedule of the table:

['LazyLevelling'\/'Incremental']:
    \(O(\frac{1}{P} \: \log_T \frac{n}{B})\).
['LazyLevelling'\/'OneShot']:
    \(O(\frac{n}{P})\).

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
['TableClosedError']:
    If the table is closed.
-}
update ::
    forall k v.
    (SerialiseKey k, SerialiseValue v) =>
    Table k v ->
    k ->
    Maybe v ->
    IO ()
update (Table table) k =
    LSMT.update table k . maybeValueToUpdate

{- |
Variant of 'update' for batch updates.

The worst-case disk I\/O complexity of this operation depends on the merge policy and the merge schedule of the table:

['LazyLevelling'\/'Incremental']:
    \(O(b \: \frac{1}{P} \: \log_T \frac{n}{B})\).
['LazyLevelling'\/'OneShot']:
    \(O(\frac{b}{P} \log_T \frac{b}{B} + \frac{n}{P})\).

The variable \(b\) refers to the length of the input vector.

The following property holds in the absence of races:

prop> updates table entries = traverse_ (uncurry $ update table) entries
-}
updates ::
    forall k v.
    (SerialiseKey k, SerialiseValue v) =>
    Table k v ->
    Vector (k, Maybe v) ->
    IO ()
updates (Table table) =
    LSMT.updates table . fmap (second maybeValueToUpdate)

--------------------------------------------------------------------------------
-- Duplication
--------------------------------------------------------------------------------

{- |
Run an action with access to the duplicate of a table.

The duplicate is an independent copy of the given table.
The duplicate is unaffected by subsequent updates to the given table and vice versa.

The worst-case disk I\/O complexity of this operation depends on the merge policy of the table:

['LazyLevelling']:
    \(O(T \log_T \frac{n}{B})\).

This function is exception-safe for both synchronous and asynchronous exceptions.

It is recommended to use this function instead of 'duplicate' and 'closeTable'.

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
['TableClosedError']:
    If the table is closed.
-}
withDuplicate ::
    forall k v a.
    Table k v ->
    (Table k v -> IO a) ->
    IO a
withDuplicate (Table table) action =
    LSMT.withDuplicate table (action . Table)

{- |
Duplicate a table.

The duplicate is an independent copy of the given table.
The duplicate is unaffected by subsequent updates to the given table and vice versa.

The worst-case disk I\/O complexity of this operation is \(O(0)\).

__Warning:__ The duplicate must be independently closed using 'closeTable'.

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
['TableClosedError']:
    If the table is closed.
-}
duplicate ::
    forall k v.
    Table k v ->
    IO (Table k v)
duplicate (Table table) =
    Table <$> LSMT.duplicate table

--------------------------------------------------------------------------------
-- Union
--------------------------------------------------------------------------------

{- |
Run an action with access to a table that contains the union of the entries of the given tables.

The worst-case disk I\/O complexity of this operation is \(O(\frac{n}{P})\).

This function is exception-safe for both synchronous and asynchronous exceptions.

It is recommended to use this function instead of 'union' and 'closeTable'.

__Warning:__ Both input tables must be from the same 'Session'.

__Warning:__ This is a relatively expensive operation that may take some time to complete.
See 'withIncrementalUnion' for an incremental alternative.

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
['TableClosedError']:
    If the table is closed.
['TableUnionNotCompatibleError']:
    If both tables are not from the same 'Session'.
-}
withUnion ::
    forall k v a.
    Table k v ->
    Table k v ->
    (Table k v -> IO a) ->
    IO a
withUnion (Table table1) (Table table2) action =
    LSMT.withUnion table1 table2 (action . Table)

{- |
Variant of 'withUnions' that takes any number of tables.
-}
withUnions ::
    forall k v a.
    NonEmpty (Table k v) ->
    (Table k v -> IO a) ->
    IO a
withUnions tables action =
    LSMT.withUnions (coerce tables) (action . Table)

{- |
Create a table that contains the left-biased union of the entries of the given tables.

The worst-case disk I\/O complexity of this operation is \(O(\frac{n}{P})\).

__Warning:__ The new table must be independently closed using 'closeTable'.

__Warning:__ Both input tables must be from the same 'Session'.

__Warning:__ This is a relatively expensive operation that may take some time to complete.
See 'incrementalUnion' for an incremental alternative.

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
['TableClosedError']:
    If the table is closed.
['TableUnionNotCompatibleError']:
    If both tables are not from the same 'Session'.
-}
union ::
    forall k v.
    Table k v ->
    Table k v ->
    IO (Table k v)
union (Table table1) (Table table2) =
    Table <$> LSMT.union table1 table2

{- |
Variant of 'union' that takes any number of tables.
-}
unions ::
    forall k v.
    NonEmpty (Table k v) ->
    IO (Table k v)
unions tables =
    Table <$> LSMT.unions (coerce tables)

{- |
Run an action with access to a table that incrementally computes the union of the given tables.

The worst-case disk I\/O complexity of this operation depends on the merge policy of the table:

['LazyLevelling']:
    \(O(T \log_T \frac{n}{B})\).

This function is exception-safe for both synchronous and asynchronous exceptions.

It is recommended to use this function instead of 'incrementalUnion' and 'closeTable'.

The created table has a /union debt/ which represents the amount of computation that remains. See 'remainingUnionDebt'.
The union debt can be paid off by supplying /union credit/ which performs an amount of computation proportional to the amount of union credit. See 'supplyUnionCredits'.
While a table has unresolved union debt, operations may become more expensive by a factor that scales with the number of unresolved unions.

__Warning:__ Both input tables must be from the same 'Session'.

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
['TableClosedError']:
    If the table is closed.
['TableUnionNotCompatibleError']:
    If both tables are not from the same 'Session'.
-}
withIncrementalUnion ::
    forall k v a.
    Table k v ->
    Table k v ->
    (Table k v -> IO a) ->
    IO a
withIncrementalUnion (Table table1) (Table table2) action =
    LSMT.withIncrementalUnion table1 table2 (action . Table)

{- |
Variant of 'withIncrementalUnion' that takes any number of tables.

The worst-case disk I\/O complexity of this operation depends on the merge policy of the table:

['LazyLevelling']:
    \(O(T \log_T \frac{n}{B} + b)\).

The variable \(b\) refers to the number of input tables.
-}
withIncrementalUnions ::
    forall k v a.
    NonEmpty (Table k v) ->
    (Table k v -> IO a) ->
    IO a
withIncrementalUnions tables action =
    LSMT.withIncrementalUnions (coerce tables) (action . Table)

{- |
Create a table that incrementally computes the union of the given tables.

The worst-case disk I\/O complexity of this operation is \(O(1)\).

The created table has a /union debt/ which represents the amount of computation that remains. See 'remainingUnionDebt'.
The union debt can be paid off by supplying /union credit/ which performs an amount of computation proportional to the amount of union credit. See 'supplyUnionCredits'.
While a table has unresolved union debt, operations may become more expensive by a factor that scales with the number of unresolved unions.

__Warning:__ The new table must be independently closed using 'closeTable'.

__Warning:__ Both input tables must be from the same 'Session'.

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
['TableClosedError']:
    If the table is closed.
['TableUnionNotCompatibleError']:
    If both tables are not from the same 'Session'.
-}
incrementalUnion ::
    forall k v.
    Table k v ->
    Table k v ->
    IO (Table k v)
incrementalUnion (Table table1) (Table table2) = do
    Table <$> LSMT.incrementalUnion table1 table2

{- |
Variant of 'incrementalUnion' for any number of tables.

The worst-case disk I\/O complexity of this operation is \(O(b)\),
where the variable \(b\) refers to the number of input tables.
-}
incrementalUnions ::
    forall k v.
    NonEmpty (Table k v) ->
    IO (Table k v)
incrementalUnions tables = do
    Table <$> LSMT.unions (coerce tables)

{- |
Get the amount of remaining union debt.
This includes the union debt of any table that was part of the union's input.

The worst-case disk I\/O complexity of this operation is \(O(0)\).
-}
remainingUnionDebt ::
    forall k v.
    Table k v ->
    IO UnionDebt
remainingUnionDebt (Table table) =
    LSMT.remainingUnionDebt table

{- |
Supply the given amount of union credits.

This reduces the union debt by /at least/ the number of supplied union credits.
It is therefore advisable to query 'remainingUnionDebt' every once in a while to see what the current debt is.

This function returns any surplus of union credits as /leftover/ credits when a union has finished.
In particular, if the returned number of credits is positive, then the union is finished.

The worst-case disk I\/O complexity of this operation is \(O(\frac{b}{P})\),
where the variable \(b\) refers to the amount of credits supplied.

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
['TableClosedError']:
    If the table is closed.
-}
supplyUnionCredits ::
    forall k v.
    Table k v ->
    UnionCredits ->
    IO UnionCredits
supplyUnionCredits (Table table) credits =
    LSMT.supplyUnionCredits table credits

--------------------------------------------------------------------------------
-- Cursors
--------------------------------------------------------------------------------

{- |
A cursor is a stable read-only iterator for a table.

A cursor iterates over the entries in a table following the order of the
serialised keys. After the cursor is created, updates to the referenced table
do not affect the cursor.

The name of this type references [database cursors](https://en.wikipedia.org/wiki/Cursor_(databases\)), not, e.g., text editor cursors.
-}
type role Cursor nominal nominal

type Cursor :: Type -> Type -> Type
newtype Cursor k v = Cursor (LSMT.Cursor IO k (ResolveAsFirst v) Void)

{- |
Run an action with access to a cursor.

The worst-case disk I\/O complexity of this operation depends on the merge policy of the table:

['LazyLevelling']:
    \(O(T \log_T \frac{n}{B})\).

This function is exception-safe for both synchronous and asynchronous exceptions.

It is recommended to use this function instead of 'newCursor' and 'closeCursor'.

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
['TableClosedError']:
    If the table is closed.
-}
withCursor ::
    forall k v a.
    Table k v ->
    (Cursor k v -> IO a) ->
    IO a
withCursor (Table table) action =
    LSMT.withCursor table (action . Cursor)

{- |
Variant of 'withCursor' that starts at a given key.
-}
withCursorAtOffset ::
    forall k v a.
    (SerialiseKey k) =>
    Table k v ->
    k ->
    (Cursor k v -> IO a) ->
    IO a
withCursorAtOffset (Table table) offsetKey action =
    LSMT.withCursorAtOffset table offsetKey (action . Cursor)

{- |
Create a cursor for the given table.

The worst-case disk I\/O complexity of this operation depends on the merge policy of the table:

['LazyLevelling']:
    \(O(T \log_T \frac{n}{B})\).

__Warning:__ Cursors hold open resources and must be closed using 'closeCursor'.

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
['TableClosedError']:
    If the table is closed.
-}
newCursor ::
    forall k v.
    Table k v ->
    IO (Cursor k v)
newCursor (Table table) =
    Cursor <$> LSMT.newCursor table

{- |
Variant of 'newCursor' that starts at a given key.
-}
newCursorAtOffset ::
    forall k v.
    (SerialiseKey k) =>
    Table k v ->
    k ->
    IO (Cursor k v)
newCursorAtOffset (Table table) offsetKey =
    Cursor <$> LSMT.newCursorAtOffset table offsetKey

{- |
Close a cursor.

The worst-case disk I\/O complexity of this operation depends on the merge policy of the table:

['LazyLevelling']:
    \(O(T \log_T \frac{n}{B})\).

Closing is idempotent, i.e., closing a closed cursor does nothing.
All other operations on a closed cursor will throw an exception.
-}
closeCursor ::
    forall k v.
    Cursor k v ->
    IO ()
closeCursor (Cursor cursor) =
    LSMT.closeCursor cursor

{- |
Read the next table entry from the cursor.

The worst-case disk I\/O complexity of this operation is \(O(\frac{1}{P})\).

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
['CursorClosedError']:
    If the cursor is closed.
-}
next ::
    forall k v.
    (SerialiseKey k, SerialiseValue v) =>
    Cursor k v ->
    IO (Maybe (k, v))
next (Cursor cursor) =
    fmap getKeyValue <$> LSMT.next cursor

{- |
Read the next batch of table entries from the cursor.

The worst-case disk I\/O complexity of this operation is \(O(\frac{b}{P})\),
where the variable \(b\) refers to the length of the /output/ vector,
which is /at most/ equal to the given number.
In practice, the length of the output vector is only less than the given number
once the cursor reaches the end of the table.

The following property holds:

prop> take n cursor = catMaybes <$> replicateM n (next cursor)

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
['CursorClosedError']:
    If the cursor is closed.
-}
take ::
    forall k v.
    (SerialiseKey k, SerialiseValue v) =>
    Int ->
    Cursor k v ->
    IO (Vector (k, v))
take n (Cursor cursor) =
    fmap getKeyValue <$> LSMT.take n cursor

{- |
Variant of 'take' that accepts an additional predicate to determine whether or not to continue reading.

The worst-case disk I\/O complexity of this operation is \(O(\frac{b}{P})\),
where the variable \(b\) refers to the length of the /output/ vector,
which is /at most/ equal to the given number.
In practice, the length of the output vector is only less than the given number
when the predicate returns false or the cursor reaches the end of the table.

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
    forall k v.
    (SerialiseKey k, SerialiseValue v) =>
    Int ->
    (k -> Bool) ->
    Cursor k v ->
    IO (Vector (k, v))
takeWhile n p (Cursor cursor) =
    fmap getKeyValue <$> LSMT.takeWhile n p cursor

--------------------------------------------------------------------------------
-- Snapshots
--------------------------------------------------------------------------------

{- |
Save the current state of the table to disk as a snapshot under the given
snapshot name. This is the /only/ mechanism that persists a table. Each snapshot
must have a unique name, which may be used to restore the table from that snapshot
using 'openTableFromSnapshot'.
Saving a snapshot /does not/ close the table.

Saving a snapshot is /relatively/ cheap when compared to opening a snapshot.
However, it is not so cheap that one should use it after every operation.

The worst-case disk I\/O complexity of this operation depends on the merge policy of the table:

['LazyLevelling']:
    \(O(T \log_T \frac{n}{B})\).

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
['TableClosedError']:
    If the table is closed.
['SnapshotExistsError']:
    If a snapshot with the same name already exists.
-}
saveSnapshot ::
    forall k v.
    SnapshotName ->
    SnapshotLabel ->
    Table k v ->
    IO ()
saveSnapshot snapName snapLabel (Table table) =
    LSMT.saveSnapshot snapName snapLabel table

{- |
Run an action with access to a table from a snapshot.

The worst-case disk I\/O complexity of this operation is \(O(\frac{n}{P})\).

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
    forall k v a.
    Session ->
    SnapshotName ->
    SnapshotLabel ->
    (Table k v -> IO a) ->
    IO a
withTableFromSnapshot (Session session) snapName snapLabel action =
    LSMT.withTableFromSnapshot session snapName snapLabel (action . Table)

{- |
Variant of 'withTableFromSnapshot' that accepts [table configuration overrides](#g:table_configuration_overrides).
-}
withTableFromSnapshotWith ::
    forall k v a.
    OverrideDiskCachePolicy ->
    Session ->
    SnapshotName ->
    SnapshotLabel ->
    (Table k v -> IO a) ->
    IO a
withTableFromSnapshotWith tableConfigOverride (Session session) snapName snapLabel action =
    LSMT.withTableFromSnapshotWith tableConfigOverride session snapName snapLabel (action . Table)

{- |
Open a table from a named snapshot.

The worst-case disk I\/O complexity of this operation is \(O(\frac{n}{P})\).

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
    forall k v.
    Session ->
    SnapshotName ->
    SnapshotLabel ->
    IO (Table k v)
openTableFromSnapshot (Session session) snapName snapLabel =
    Table <$> LSMT.openTableFromSnapshot @IO @k @(ResolveAsFirst v) session snapName snapLabel

{- |
Variant of 'openTableFromSnapshot' that accepts [table configuration overrides](#g:table_configuration_overrides).
-}
openTableFromSnapshotWith ::
    forall k v.
    OverrideDiskCachePolicy ->
    Session ->
    SnapshotName ->
    SnapshotLabel ->
    IO (Table k v)
openTableFromSnapshotWith tableConfigOverride (Session session) snapName snapLabel =
    Table <$> LSMT.openTableFromSnapshotWith tableConfigOverride session snapName snapLabel

{- |
Delete the named snapshot.

The worst-case disk I\/O complexity of this operation depends on the merge policy of the table:

['LazyLevelling']:
    \(O(T \log_T \frac{n}{B})\).

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
    LSMT.deleteSnapshot session

{- |
Check if the named snapshot exists.

The worst-case disk I\/O complexity of this operation is \(O(1)\).

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
-}
doesSnapshotExist ::
    Session ->
    SnapshotName ->
    IO Bool
doesSnapshotExist (Session session) =
    LSMT.doesSnapshotExist session

{- |
List the names of all snapshots.

The worst-case disk I\/O complexity of this operation is \(O(s)\),
where the variable \(s\) refers to the number of snapshots in the session.

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
-}
listSnapshots ::
    Session ->
    IO [SnapshotName]
listSnapshots (Session session) =
    LSMT.listSnapshots session

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

*   t'LSMT.SessionDirDoesNotExistError' to t'SessionDirDoesNotExistError';
*   t'LSMT.SessionDirLockedError'       to t'SessionDirLockedError'; and
*   t'LSMT.SessionDirCorruptedError'    to t'SessionDirCorruptedError'.
-}
_convertSessionDirErrors ::
    forall a.
    FilePath ->
    IO a ->
    IO a
_convertSessionDirErrors sessionDir =
    mapExceptionWithActionRegistry (\(LSMT.ErrSessionDirDoesNotExist _fsErrorPath) -> SomeException $ ErrSessionDirDoesNotExist sessionDir)
        . mapExceptionWithActionRegistry (\(LSMT.ErrSessionDirLocked _fsErrorPath) -> SomeException $ ErrSessionDirLocked sessionDir)
        . mapExceptionWithActionRegistry (\(LSMT.ErrSessionDirCorrupted _fsErrorPath) -> SomeException $ ErrSessionDirCorrupted sessionDir)

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

*   t'LSMT.TableUnionNotCompatibleError' to t'TableUnionNotCompatibleError';
-}
_convertTableUnionNotCompatibleError ::
    forall a.
    (Int -> FilePath) ->
    IO a ->
    IO a
_convertTableUnionNotCompatibleError sessionDirFor =
    mapExceptionWithActionRegistry $ \case
        LSMT.ErrTableUnionHandleTypeMismatch i1 typeRep1 i2 typeRep2 ->
            ErrTableUnionHandleTypeMismatch i1 typeRep1 i2 typeRep2
        LSMT.ErrTableUnionSessionMismatch i1 _fsErrorPath1 i2 _fsErrorPath2 ->
            ErrTableUnionSessionMismatch i1 (sessionDirFor i1) i2 (sessionDirFor i2)
