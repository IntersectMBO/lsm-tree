{- |
Module      : Database.LSMTree
Copyright   : (c) 2023, Input Output Global, Inc. (IOG)
              (c) 2023-2025, INTERSECT
License     : Apache-2.0
Stability   : experimental
Portability : portable
-}
module Database.LSMTree (
  -- * Usage Notes
  -- $usage_notes

  -- ** Real and Simulated IO
  -- $real_and_simulated_io
  IOLike,

  -- * Examples
  -- $setup

  -- * Sessions
  Session,
  withSession,
  withSessionIO,
  openSession,
  openSessionIO,
  closeSession,

  -- * Tables
  Table,
  withTable,
  withTableWith,
  newTable,
  newTableWith,
  closeTable,

  -- ** Table Lookups #table_lookups#
  member,
  members,
  LookupResult (..),
  getValue,
  getBlob,
  lookup,
  lookups,
  Entry (..),
  rangeLookup,

  -- ** Table Updates #table_updates#
  insert,
  inserts,
  upsert,
  upserts,
  delete,
  deletes,
  Update (..),
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

  -- * Blob References #blob_references#
  BlobRef,
  retrieveBlob,
  retrieveBlobs,

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
    confMergeSchedule,
    confMergeBatchSize
  ),
  defaultTableConfig,
  MergePolicy (LazyLevelling),
  MergeSchedule (..),
  SizeRatio (Four),
  WriteBufferAlloc (AllocNumEntries),
  BloomFilterAlloc (AllocFixed, AllocRequestFPR),
  FencePointerIndexType (OrdinaryIndex, CompactIndex),
  DiskCachePolicy (..),
  MergeBatchSize (..),

  -- ** Table Configuration Overrides #table_configuration_overrides#
  TableConfigOverride (..),
  noTableConfigOverride,

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

  -- * Monoidal Value Resolution #monoidal_value_resolution#
  ResolveValue (..),
  ResolveViaSemigroup (..),
  ResolveAsFirst (..),

  -- ** Monoidal Value Resolution Property Tests #monoidal_value_resolution_property_tests#
  resolveCompatibility,
  resolveValidOutput,
  resolveAssociativity,

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

  -- * Traces #traces#
  Tracer,
  LSMTreeTrace (..),
  TableTrace (..),
  CursorTrace (..),
  MergeTrace (..),
  CursorId (..),
  TableId (..),
  AtLevel (..),
  LevelNo (..),
  NumEntries (..),
  RunNumber (..),
  MergePolicyForLevel (..),
  LevelMergeType (..),
  RunParams (..),
  RunDataCaching (..),
  IndexType (..),
) where

import           Control.Concurrent.Class.MonadMVar.Strict (MonadMVar)
import           Control.Concurrent.Class.MonadSTM (MonadSTM (STM))
import           Control.DeepSeq (NFData (..))
import           Control.Exception.Base (assert)
import           Control.Monad.Class.MonadAsync (MonadAsync)
import           Control.Monad.Class.MonadST (MonadST)
import           Control.Monad.Class.MonadThrow (MonadCatch (..), MonadMask,
                     MonadThrow (..))
import           Control.Monad.Primitive (PrimMonad)
import           Control.Tracer (Tracer)
import           Data.Bifunctor (Bifunctor (..))
import           Data.Coerce (coerce)
import           Data.Kind (Constraint, Type)
import           Data.List.NonEmpty (NonEmpty (..))
import           Data.Maybe (fromMaybe, isJust)
import           Data.Typeable (Proxy (..), Typeable, eqT, type (:~:) (Refl),
                     typeRep)
import           Data.Vector (Vector)
import qualified Data.Vector as V
import qualified Database.LSMTree.Internal.BlobRef as Internal
import           Database.LSMTree.Internal.Config
                     (BloomFilterAlloc (AllocFixed, AllocRequestFPR),
                     DiskCachePolicy (..), FencePointerIndexType (..),
                     LevelNo (..), MergeBatchSize (..), MergePolicy (..),
                     MergeSchedule (..), SizeRatio (..), TableConfig (..),
                     WriteBufferAlloc (..), defaultTableConfig,
                     serialiseKeyMinimalSize)
import           Database.LSMTree.Internal.Config.Override
                     (TableConfigOverride (..), noTableConfigOverride)
import           Database.LSMTree.Internal.Entry (NumEntries (..))
import qualified Database.LSMTree.Internal.Entry as Entry
import           Database.LSMTree.Internal.Merge (LevelMergeType (..))
import           Database.LSMTree.Internal.MergeSchedule (AtLevel (..),
                     MergePolicyForLevel (..), MergeTrace (..))
import           Database.LSMTree.Internal.Paths (SnapshotName,
                     isValidSnapshotName, toSnapshotName)
import           Database.LSMTree.Internal.Range (Range (..))
import           Database.LSMTree.Internal.RawBytes (RawBytes (..))
import           Database.LSMTree.Internal.RunBuilder (IndexType (..),
                     RunDataCaching (..), RunParams (..))
import           Database.LSMTree.Internal.RunNumber (CursorId (..),
                     RunNumber (..), TableId (..))
import qualified Database.LSMTree.Internal.Serialise as Internal
import           Database.LSMTree.Internal.Serialise.Class (SerialiseKey (..),
                     SerialiseKeyOrderPreserving, SerialiseValue (..),
                     packSlice, serialiseKeyIdentity,
                     serialiseKeyIdentityUpToSlicing,
                     serialiseKeyPreservesOrdering, serialiseValueIdentity,
                     serialiseValueIdentityUpToSlicing)
import           Database.LSMTree.Internal.Snapshot (SnapshotLabel (..))
import           Database.LSMTree.Internal.Types (BlobRef (..), Cursor (..),
                     ResolveAsFirst (..), ResolveValue (..),
                     ResolveViaSemigroup (..), Session (..), Table (..),
                     resolveAssociativity, resolveCompatibility,
                     resolveValidOutput)
import           Database.LSMTree.Internal.Unsafe (BlobRefInvalidError (..),
                     CursorClosedError (..), CursorTrace,
                     InvalidSnapshotNameError (..), LSMTreeTrace (..),
                     ResolveSerialisedValue, SessionClosedError (..),
                     SessionDirCorruptedError (..),
                     SessionDirDoesNotExistError (..),
                     SessionDirLockedError (..), SnapshotCorruptedError (..),
                     SnapshotDoesNotExistError (..), SnapshotExistsError (..),
                     SnapshotNotCompatibleError (..), TableClosedError (..),
                     TableCorruptedError (..), TableTooLargeError (..),
                     TableTrace, TableUnionNotCompatibleError (..),
                     UnionCredits (..), UnionDebt (..))
import qualified Database.LSMTree.Internal.Unsafe as Internal
import           Prelude hiding (lookup, take, takeWhile)
import           System.FS.API (FsPath, HasFS (..), MountPoint (..), mkFsPath)
import           System.FS.BlockIO.API (HasBlockIO (..), defaultIOCtxParams)
import           System.FS.BlockIO.IO (ioHasBlockIO, withIOHasBlockIO)
import           System.FS.IO (HandleIO, ioHasFS)

--------------------------------------------------------------------------------
-- Usage Notes
--------------------------------------------------------------------------------

{- $usage_notes
This section focuses on the differences between the full API as defined in this module and the simple API as defined in "Database.LSMTree.Simple".
It assumes that the reader is familiar with [Usage Notes for the simple API]("Database.LSMTree.Simple#g:usage_notes"), which discusses crucial topics such as [Resource Management]("Database.LSMTree.Simple#g:resource_management"), [Concurrency]("Database.LSMTree.Simple#g:concurrency"), [ACID properties]("Database.LSMTree.Simple#g:acid"), and [Sharing]("Database.LSMTree.Simple#g:sharing").
-}

{- $real_and_simulated_io
-}

type IOLike :: (Type -> Type) -> Constraint
type IOLike m =
  ( MonadAsync m
  , MonadMVar m
  , MonadThrow m
  , MonadThrow (STM m)
  , MonadCatch m
  , MonadMask m
  , PrimMonad m
  , MonadST m
  )

--------------------------------------------------------------------------------
-- Example
--------------------------------------------------------------------------------

{- $setup

The examples in this module use the preamble described in this section, which does three things:

1.  It imports this module qualified, as intended, as well as any other relevant modules.
2.  It defines types for keys, values, and BLOBs.
3.  It defines a helper function that runs examples with access to an open session and fresh table.

=== Importing "Database.LSMTree"

This module is intended to be imported qualified, to avoid name clashes with Prelude functions.

>>> import           Database.LSMTree (BlobRef, Cursor, RawBytes, ResolveValue (..), SerialiseKey (..), SerialiseValue (..), Session, Table)
>>> import qualified Database.LSMTree as LSMT

=== Defining key, value, and BLOB types

The examples in this module use the types @Key@, @Value@, and @Blob@ for keys, values and BLOBs.

>>> import Data.ByteString (ByteString)
>>> import Data.ByteString.Short (ShortByteString)
>>> import Data.Proxy (Proxy)
>>> import Data.String (IsString)
>>> import Data.Word (Word64)

The type @Key@ is a newtype wrapper around 'Data.Word.Word64'.
The required instance of 'SerialiseKey' is derived by @GeneralisedNewtypeDeriving@ from the preexisting instance for 'Data.Word.Word64'.

>>> :{
newtype Key = Key Word64
  deriving stock (Eq, Ord, Show)
  deriving newtype (Num, SerialiseKey)
:}

The type @Value@ is a newtype wrapper around 'Data.ByteString.Short.ShortByteString'.
The required instance of 'SerialiseValue' is derived by @GeneralisedNewtypeDeriving@ from the preexisting instance for 'Data.ByteString.Short.ShortByteString'.

>>> :{
newtype Value = Value ShortByteString
  deriving stock (Eq, Show)
  deriving newtype (IsString, SerialiseValue)
:}

The type @Value@ has an instance of @ResolveValue@ which appends the new value to the old value separated by a space.
It is sufficient to define either 'resolve' or 'resolveSerialised',
as each can be defined in terms of the other and 'serialiseValue'\/'deserialiseValue'.
For optimal performance, you should /always/ define 'resolveSerialised' manually.

__NOTE__:
The /first/ argument of 'resolve' and 'resolveSerialised' is the /new/ value and the /second/ argument is the /old/ value.

>>> :{
instance ResolveValue Value where
  resolve :: Value -> Value -> Value
  resolve (Value new) (Value old) = Value (new <> " " <> old)
  resolveSerialised :: Proxy Value -> RawBytes -> RawBytes -> RawBytes
  resolveSerialised _ new old = new <> " " <> old
:}

The type @Blob@ is a newtype wrapper around 'Data.ByteString.ByteString',
The required instance of 'SerialiseValue' is derived by @GeneralisedNewtypeDeriving@ from the preexisting instance for 'Data.ByteString.ByteString'.

>>> :{
newtype Blob = Blob ByteString
  deriving stock (Eq, Show)
  deriving newtype (IsString, SerialiseValue)
:}

=== Defining a helper function to run examples

The examples in this module are wrapped in a call to @runExample@,
which creates a temporary session directory and
runs the example with access to an open 'Session' and a fresh 'Table'.

>>> import           Control.Exception (bracket, bracket_)
>>> import           Data.Foldable (traverse_)
>>> import qualified System.Directory as Dir
>>> import           System.FilePath ((</>))
>>> import           System.Process (getCurrentPid)
>>> :{
runExample :: (Session IO -> Table IO Key Value Blob -> IO a) -> IO a
runExample action = do
  tmpDir <- Dir.getTemporaryDirectory
  pid <- getCurrentPid
  let sessionDir = tmpDir </> "doctest_Database_LSMTree" </> show pid
  let createSessionDir = Dir.createDirectoryIfMissing True sessionDir
  let removeSessionDir = Dir.removeDirectoryRecursive sessionDir
  bracket_ createSessionDir removeSessionDir $ do
    LSMT.withSessionIO mempty sessionDir $ \session -> do
      LSMT.withTable session $ \table ->
        action session table
:}
-}

--------------------------------------------------------------------------------
-- Sessions
--------------------------------------------------------------------------------

-- NOTE: 'Session' is defined in 'Database.LSMTree.Internal.Types'

{- |
Run an action with access to a session opened from a session directory.

If the session directory is empty, a new session is created.
Otherwise, the session directory is opened as an existing session.

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
{-# SPECIALISE
  withSession ::
    Tracer IO LSMTreeTrace ->
    HasFS IO HandleIO ->
    HasBlockIO IO HandleIO ->
    FsPath ->
    (Session IO -> IO a) ->
    IO a
  #-}
withSession ::
  forall m h a.
  (IOLike m, Typeable h) =>
  Tracer m LSMTreeTrace ->
  HasFS m h ->
  HasBlockIO m h ->
  -- | The session directory.
  FsPath ->
  (Session m -> m a) ->
  m a
withSession tracer hasFS hasBlockIO sessionDir action = do
  Internal.withSession tracer hasFS hasBlockIO sessionDir (action . Session)

-- | Variant of 'withSession' that is specialised to 'IO' using the real filesystem.
withSessionIO ::
  Tracer IO LSMTreeTrace ->
  FilePath ->
  (Session IO -> IO a) ->
  IO a
withSessionIO tracer sessionDir action = do
  let mountPoint = MountPoint sessionDir
  let sessionDirFsPath = mkFsPath []
  let hasFS = ioHasFS mountPoint
  withIOHasBlockIO hasFS defaultIOCtxParams $ \hasBlockIO ->
    withSession tracer hasFS hasBlockIO sessionDirFsPath action

{- |
Open a session from a session directory.

If the session directory is empty, a new session is created.
Otherwise, the session directory is opened as an existing session.

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
{-# SPECIALISE
  openSession ::
    Tracer IO LSMTreeTrace ->
    HasFS IO HandleIO ->
    HasBlockIO IO HandleIO ->
    -- \| The session directory.
    FsPath ->
    IO (Session IO)
  #-}
openSession ::
  forall m h.
  (IOLike m, Typeable h) =>
  Tracer m LSMTreeTrace ->
  HasFS m h ->
  HasBlockIO m h ->
  -- | The session directory.
  FsPath ->
  m (Session m)
openSession tracer hasFS hasBlockIO sessionDir =
  Session <$> Internal.openSession tracer hasFS hasBlockIO sessionDir

-- | Variant of 'openSession' that is specialised to 'IO' using the real filesystem.
openSessionIO ::
  Tracer IO LSMTreeTrace ->
  -- \| The session directory.
  FilePath ->
  IO (Session IO)
openSessionIO tracer sessionDir = do
  let mountPoint = MountPoint sessionDir
  let sessionDirFsPath = mkFsPath []
  let hasFS = ioHasFS mountPoint
  let acquireHasBlockIO = ioHasBlockIO hasFS defaultIOCtxParams
  let releaseHasBlockIO HasBlockIO{close} = close
  bracketOnError acquireHasBlockIO releaseHasBlockIO $ \hasBlockIO ->
    openSession tracer hasFS hasBlockIO sessionDirFsPath

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
{-# SPECIALISE
  closeSession ::
    Session IO ->
    IO ()
  #-}
closeSession ::
  forall m.
  (IOLike m) =>
  Session m ->
  m ()
closeSession (Session session) =
  Internal.closeSession session

--------------------------------------------------------------------------------
-- Tables
--------------------------------------------------------------------------------

-- NOTE: 'Table' is defined in 'Database.LSMTree.Internal.Types'

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
{-# SPECIALISE
  withTable ::
    Session IO ->
    (Table IO k v b -> IO a) ->
    IO a
  #-}
withTable ::
  forall m k v b a.
  (IOLike m) =>
  Session m ->
  (Table m k v b -> m a) ->
  m a
withTable session =
  withTableWith defaultTableConfig session

-- | Variant of 'withTable' that accepts [table configuration](#g:table_configuration).
{-# SPECIALISE
  withTableWith ::
    TableConfig ->
    Session IO ->
    (Table IO k v b -> IO a) ->
    IO a
  #-}
withTableWith ::
  forall m k v b a.
  (IOLike m) =>
  TableConfig ->
  Session m ->
  (Table m k v b -> m a) ->
  m a
withTableWith tableConfig (Session session) action =
  Internal.withTable session tableConfig (action . Table)

{- |
Create an empty table.

The worst-case disk I\/O complexity of this operation is \(O(1)\).

__Warning:__ Tables hold open resources and must be closed using 'closeTable'.

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
-}
{-# SPECIALISE
  newTable ::
    Session IO ->
    IO (Table IO k v b)
  #-}
newTable ::
  forall m k v b.
  (IOLike m) =>
  Session m ->
  m (Table m k v b)
newTable session =
  newTableWith defaultTableConfig session

{- |
Variant of 'newTable' that accepts [table configuration](#g:table_configuration).
-}
{-# SPECIALISE
  newTableWith ::
    TableConfig ->
    Session IO ->
    IO (Table IO k v b)
  #-}
newTableWith ::
  forall m k v b.
  (IOLike m) =>
  TableConfig ->
  Session m ->
  m (Table m k v b)
newTableWith tableConfig (Session session) =
  Table <$> Internal.new session tableConfig

{- |
Close a table.

The worst-case disk I\/O complexity of this operation depends on the merge policy of the table:

['LazyLevelling']:
    \(O(T \log_T \frac{n}{B})\).

Closing is idempotent, i.e., closing a closed table does nothing.
All other operations on a closed table will throw an exception.

__Warning:__ Tables are ephemeral. Once you close a table, its data is lost forever. To persist tables, use [snapshots](#g:snapshots).
-}
{-# SPECIALISE
  closeTable ::
    Table IO k v b ->
    IO ()
  #-}
closeTable ::
  forall m k v b.
  (IOLike m) =>
  Table m k v b ->
  m ()
closeTable (Table table) =
  Internal.close table

--------------------------------------------------------------------------------
-- Lookups
--------------------------------------------------------------------------------

{- |
Check if the key is a member of the table.

>>> :{
runExample $ \session table -> do
  LSMT.insert table 0 "Hello" Nothing
  LSMT.insert table 1 "World" Nothing
  print =<< LSMT.member table 0
:}
True

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
{-# SPECIALISE
  member ::
    (SerialiseKey k, SerialiseValue v, ResolveValue v) =>
    Table IO k v b ->
    k ->
    IO Bool
  #-}
member ::
  forall m k v b.
  (IOLike m) =>
  (SerialiseKey k, SerialiseValue v, ResolveValue v) =>
  Table m k v b ->
  k ->
  m Bool
member =
  -- Technically, this does not need the 'SerialiseValue' constraint.
  (fmap (isJust . getValue) .) . lookup

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
{-# SPECIALISE
  members ::
    (SerialiseKey k, SerialiseValue v, ResolveValue v) =>
    Table IO k v b ->
    Vector k ->
    IO (Vector Bool)
  #-}
members ::
  forall m k v b.
  (IOLike m) =>
  (SerialiseKey k, SerialiseValue v, ResolveValue v) =>
  Table m k v b ->
  Vector k ->
  m (Vector Bool)
members =
  -- Technically, this does not need the 'SerialiseValue' constraint.
  (fmap (fmap (isJust . getValue)) .) . lookups

data LookupResult v b
  = NotFound
  | Found !v
  | FoundWithBlob !v !b
  deriving stock (Eq, Show, Functor, Foldable, Traversable)

{- |
Get the field of type @v@ from a @'LookupResult' v b@, if any.
-}
getValue :: LookupResult v b -> Maybe v
getValue = \case
  NotFound -> Nothing
  Found !v -> Just v
  FoundWithBlob !v !_b -> Just v

{- |
Get the field of type @b@ from a @'LookupResult' v b@, if any.

The following property holds:

prop> isJust (getBlob result) <= isJust (getValue result)
-}
getBlob :: LookupResult v b -> Maybe b
getBlob = \case
  NotFound -> Nothing
  Found !_v -> Nothing
  FoundWithBlob !_v !b -> Just b

instance (NFData v, NFData b) => NFData (LookupResult v b) where
  rnf :: LookupResult v b -> ()
  rnf = \case
    NotFound -> ()
    Found v -> rnf v
    FoundWithBlob v b -> rnf v `seq` rnf b

instance Bifunctor LookupResult where
  bimap :: (v -> v') -> (b -> b') -> LookupResult v b -> LookupResult v' b'
  bimap f g = \case
    NotFound -> NotFound
    Found v -> Found (f v)
    FoundWithBlob v b -> FoundWithBlob (f v) (g b)

{- |
Look up the value associated with a key.

>>> :{
runExample $ \session table -> do
  LSMT.insert table 0 "Hello" Nothing
  LSMT.insert table 1 "World" Nothing
  print =<< LSMT.lookup table 0
:}
Found (Value "Hello")

If the key is not associated with any value, 'lookup' returns 'NotFound'.

>>> :{
runExample $ \session table -> do
  LSMT.lookup table 0
:}
NotFound

If the key has an associated BLOB, the result contains a 'BlobRef'.
The full BLOB can be retrieved by passing that 'BlobRef' to 'retrieveBlob'.

>>> :{
runExample $ \session table -> do
  LSMT.insert table 0 "Hello" (Just "World")
  print
    =<< traverse (LSMT.retrieveBlob session)
    =<< LSMT.lookup table 0
:}
FoundWithBlob (Value "Hello") (Blob "World")

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
{-# SPECIALISE
  lookup ::
    (SerialiseKey k, SerialiseValue v, ResolveValue v) =>
    Table IO k v b ->
    k ->
    IO (LookupResult v (BlobRef IO b))
  #-}
lookup ::
  forall m k v b.
  (IOLike m) =>
  (SerialiseKey k, SerialiseValue v, ResolveValue v) =>
  Table m k v b ->
  k ->
  m (LookupResult v (BlobRef m b))
lookup table k = do
  mvs <- lookups table (V.singleton k)
  let mmv = fst <$> V.uncons mvs
  pure $ fromMaybe NotFound mmv

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
{-# SPECIALISE
  lookups ::
    (SerialiseKey k, SerialiseValue v, ResolveValue v) =>
    Table IO k v b ->
    Vector k ->
    IO (Vector (LookupResult v (BlobRef IO b)))
  #-}
lookups ::
  forall m k v b.
  (IOLike m) =>
  (SerialiseKey k, SerialiseValue v, ResolveValue v) =>
  Table m k v b ->
  Vector k ->
  m (Vector (LookupResult v (BlobRef m b)))
lookups (Table table :: Table m k v b) keys = do
  maybeEntries <- Internal.lookups (_getResolveSerialisedValue (Proxy @v)) (fmap Internal.serialiseKey keys) table
  pure $ maybe NotFound entryToLookupResult <$> maybeEntries
 where
  entryToLookupResult = \case
    Entry.Insert !v -> Found (Internal.deserialiseValue v)
    Entry.InsertWithBlob !v !b -> FoundWithBlob (Internal.deserialiseValue v) (BlobRef b)
    Entry.Upsert !v -> Found (Internal.deserialiseValue v)
    Entry.Delete -> NotFound

data Entry k v b
  = Entry !k !v
  | EntryWithBlob !k !v !b
  deriving stock (Eq, Show, Functor, Foldable, Traversable)

instance (NFData k, NFData v, NFData b) => NFData (Entry k v b) where
  rnf :: Entry k v b -> ()
  rnf = \case
    Entry k v -> rnf k `seq` rnf v
    EntryWithBlob k v b -> rnf k `seq` rnf v `seq` rnf b

instance Bifunctor (Entry k) where
  bimap :: (v -> v') -> (b -> b') -> Entry k v b -> Entry k v' b'
  bimap f g = \case
    Entry k v -> Entry k (f v)
    EntryWithBlob k v b -> EntryWithBlob k (f v) (g b)

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
{-# SPECIALISE
  rangeLookup ::
    (SerialiseKey k, SerialiseValue v, ResolveValue v) =>
    Table IO k v b ->
    Range k ->
    IO (Vector (Entry k v (BlobRef IO b)))
  #-}
rangeLookup ::
  forall m k v b.
  (IOLike m) =>
  (SerialiseKey k, SerialiseValue v, ResolveValue v) =>
  Table m k v b ->
  Range k ->
  m (Vector (Entry k v (BlobRef m b)))
rangeLookup (Table table :: Table m k v b) range =
  Internal.rangeLookup (_getResolveSerialisedValue (Proxy @v)) (Internal.serialiseKey <$> range) table $ \ !k !v -> \case
    Just !b -> EntryWithBlob (Internal.deserialiseKey k) (Internal.deserialiseValue v) (BlobRef b)
    Nothing -> Entry (Internal.deserialiseKey k) (Internal.deserialiseValue v)

--------------------------------------------------------------------------------
-- Updates
--------------------------------------------------------------------------------

{- |
Insert associates the given value and BLOB with the given key in the table.

>>> :{
runExample $ \session table -> do
  LSMT.insert table 0 "Hello" Nothing
  LSMT.insert table 1 "World" Nothing
  print =<< LSMT.lookup table 0
:}
Found (Value "Hello")

Insert may optionally associate a BLOB value with the given key.

>>> :{
runExample $ \session table -> do
  LSMT.insert table 0 "Hello" (Just "World")
  print
    =<< traverse (retrieveBlob session)
    =<< LSMT.lookup table 0
:}
FoundWithBlob (Value "Hello") (Blob "World")

Insert overwrites any value and BLOB previously associated with the given key,
even if the given BLOB is 'Nothing'.

>>> :{
runExample $ \session table -> do
  LSMT.insert table 0 "Hello" (Just "World")
  LSMT.insert table 0 "Goodbye" Nothing
  print
    =<< traverse (retrieveBlob session)
    =<< LSMT.lookup table 0
:}
Found (Value "Goodbye")

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
{-# SPECIALISE
  insert ::
    (SerialiseKey k, SerialiseValue v, ResolveValue v, SerialiseValue b) =>
    Table IO k v b ->
    k ->
    v ->
    Maybe b ->
    IO ()
  #-}
insert ::
  forall m k v b.
  (IOLike m) =>
  (SerialiseKey k, SerialiseValue v, ResolveValue v, SerialiseValue b) =>
  Table m k v b ->
  k ->
  v ->
  Maybe b ->
  m ()
insert table k v b =
  inserts table (V.singleton (k, v, b))

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
{-# SPECIALISE
  inserts ::
    (SerialiseKey k, SerialiseValue v, ResolveValue v, SerialiseValue b) =>
    Table IO k v b ->
    Vector (k, v, Maybe b) ->
    IO ()
  #-}
inserts ::
  forall m k v b.
  (IOLike m) =>
  (SerialiseKey k, SerialiseValue v, ResolveValue v, SerialiseValue b) =>
  Table m k v b ->
  Vector (k, v, Maybe b) ->
  m ()
inserts table entries =
  updates table (fmap (\(k, v, mb) -> (k, Insert v mb)) entries)

{- |
If the given key is not a member of the table, 'upsert' associates the given value with the given key in the table.
Otherwise, 'upsert' updates the value associated with the given key by combining it with the given value using 'resolve'.

>>> :{
runExample $ \session table -> do
  LSMT.upsert table 0 "Hello"
  LSMT.upsert table 0 "Goodbye"
  print =<< LSMT.lookup table 0
:}
Found (Value "Goodbye Hello")

__Warning:__
Upsert deletes any BLOB previously associated with the given key.

>>> :{
runExample $ \session table -> do
  LSMT.insert table 0 "Hello" (Just "World")
  LSMT.upsert table 0 "Goodbye"
  print
    =<< traverse (LSMT.retrieveBlob session)
    =<< LSMT.lookup table 0
:}
Found (Value "Goodbye Hello")

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

The following property holds in the absence of races:

@
upsert table k v = do
  r <- lookup table k
  let v' = maybe v (resolve v) (getValue r)
  insert table k v' Nothing
@
-}
{-# SPECIALISE
  upsert ::
    (SerialiseKey k, SerialiseValue v, ResolveValue v, SerialiseValue b) =>
    Table IO k v b ->
    k ->
    v ->
    IO ()
  #-}
upsert ::
  forall m k v b.
  (IOLike m) =>
  (SerialiseKey k, SerialiseValue v, ResolveValue v, SerialiseValue b) =>
  Table m k v b ->
  k ->
  v ->
  m ()
upsert table k v =
  upserts table (V.singleton (k, v))

{- |
Variant of 'upsert' for batch insertions.

The worst-case disk I\/O complexity of this operation depends on the merge policy and the merge schedule of the table:

['LazyLevelling'\/'Incremental']:
    \(O(b \: \frac{1}{P} \: \log_T \frac{n}{B})\).
['LazyLevelling'\/'OneShot']:
    \(O(\frac{b}{P} \log_T \frac{b}{B} + \frac{n}{P})\).

The variable \(b\) refers to the length of the input vector.

The following property holds in the absence of races:

prop> upserts table entries = traverse_ (uncurry $ upsert table) entries
-}
{-# SPECIALISE
  upserts ::
    (SerialiseKey k, SerialiseValue v, ResolveValue v, SerialiseValue b) =>
    Table IO k v b ->
    Vector (k, v) ->
    IO ()
  #-}
upserts ::
  forall m k v b.
  (IOLike m) =>
  (SerialiseKey k, SerialiseValue v, ResolveValue v, SerialiseValue b) =>
  Table m k v b ->
  Vector (k, v) ->
  m ()
upserts table entries =
  updates table (second Upsert <$> entries)

{- |
Delete a key from the table.

>>> :{
runExample $ \session table -> do
  LSMT.insert table 0 "Hello" Nothing
  LSMT.delete table 0
  print =<< LSMT.lookup table 0
:}
NotFound

If the key is not a member of the table, the table is left unchanged.

>>> :{
runExample $ \session table -> do
  LSMT.insert table 0 "Hello" Nothing
  LSMT.delete table 1
  print =<< LSMT.lookup table 0
:}
Found (Value "Hello")

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
{-# SPECIALISE
  delete ::
    (SerialiseKey k, SerialiseValue v, ResolveValue v, SerialiseValue b) =>
    Table IO k v b ->
    k ->
    IO ()
  #-}
delete ::
  forall m k v b.
  (IOLike m) =>
  (SerialiseKey k, SerialiseValue v, ResolveValue v, SerialiseValue b) =>
  Table m k v b ->
  k ->
  m ()
delete table k =
  deletes table (V.singleton k)

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
{-# SPECIALISE
  deletes ::
    (SerialiseKey k, SerialiseValue v, ResolveValue v, SerialiseValue b) =>
    Table IO k v b ->
    Vector k ->
    IO ()
  #-}
deletes ::
  forall m k v b.
  (IOLike m) =>
  (SerialiseKey k, SerialiseValue v, ResolveValue v, SerialiseValue b) =>
  Table m k v b ->
  Vector k ->
  m ()
deletes table entries =
  updates table (fmap (,Delete) entries)

type Update :: Type -> Type -> Type
data Update v b
  = Insert !v !(Maybe b)
  | Delete
  | Upsert !v
  deriving stock (Show, Eq)

instance (NFData v, NFData b) => NFData (Update v b) where
  rnf :: Update v b -> ()
  rnf = \case
    Insert v mb -> rnf v `seq` rnf mb
    Delete -> ()
    Upsert v -> rnf v

{- |
Update generalises 'insert', 'delete', and 'upsert'.

The worst-case disk I\/O complexity of this operation depends on the merge policy and the merge schedule of the table:

['LazyLevelling'\/'Incremental']:
    \(O(\frac{1}{P} \: \log_T \frac{n}{B})\).
['LazyLevelling'\/'OneShot']:
    \(O(\frac{n}{P})\).

The following properties hold:

prop> update table k (Insert v mb) = insert table k v mb
prop> update table k Delete = delete table k
prop> update table k (Upsert v) = upsert table k v

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
['TableClosedError']:
    If the table is closed.
-}
{-# SPECIALISE
  update ::
    (SerialiseKey k, SerialiseValue v, ResolveValue v, SerialiseValue b) =>
    Table IO k v b ->
    k ->
    Update v b ->
    IO ()
  #-}
update ::
  forall m k v b.
  (IOLike m) =>
  (SerialiseKey k, SerialiseValue v, ResolveValue v, SerialiseValue b) =>
  Table m k v b ->
  k ->
  Update v b ->
  m ()
update table k mv =
  updates table (V.singleton (k, mv))

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
{-# SPECIALISE
  updates ::
    (SerialiseKey k, SerialiseValue v, ResolveValue v, SerialiseValue b) =>
    Table IO k v b ->
    Vector (k, Update v b) ->
    IO ()
  #-}
updates ::
  forall m k v b.
  (IOLike m) =>
  (SerialiseKey k, SerialiseValue v, ResolveValue v, SerialiseValue b) =>
  Table m k v b ->
  Vector (k, Update v b) ->
  m ()
updates (Table table :: Table m k v b) entries =
  Internal.updates (_getResolveSerialisedValue (Proxy @v)) (serialiseEntry <$> entries) table
 where
  serialiseEntry (k, u) = (Internal.serialiseKey k, serialiseUpdate u)
  serialiseUpdate = \case
    Insert v (Just b) -> Entry.InsertWithBlob (Internal.serialiseValue v) (Internal.serialiseBlob b)
    Insert v Nothing -> Entry.Insert (Internal.serialiseValue v)
    Delete -> Entry.Delete
    Upsert v -> Entry.Upsert (Internal.serialiseValue v)

--------------------------------------------------------------------------------
-- Duplication
--------------------------------------------------------------------------------

{- |
Run an action with access to the duplicate of a table.

The duplicate is an independent copy of the given table.
Subsequent updates to the original table do not affect the duplicate, and vice versa.

>>> :{
runExample $ \session table -> do
  LSMT.insert table 0 "Hello" Nothing
  LSMT.withDuplicate table $ \table' -> do
    print =<< LSMT.lookup table' 0
    LSMT.insert table' 0 "Goodbye" Nothing
    print =<< LSMT.lookup table' 0
  LSMT.lookup table 0
  print =<< LSMT.lookup table 0
:}
Found (Value "Hello")
Found (Value "Goodbye")
Found (Value "Hello")

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
{-# SPECIALISE
  withDuplicate ::
    Table IO k v b ->
    (Table IO k v b -> IO a) ->
    IO a
  #-}
withDuplicate ::
  forall m k v b a.
  (IOLike m) =>
  Table m k v b ->
  (Table m k v b -> m a) ->
  m a
withDuplicate table =
  bracket (duplicate table) closeTable

{- |
Duplicate a table.

The duplicate is an independent copy of the given table.
Subsequent updates to the original table do not affect the duplicate, and vice versa.

>>> :{
runExample $ \session table -> do
  LSMT.insert table 0 "Hello" Nothing
  bracket (LSMT.duplicate table) LSMT.closeTable $ \table' -> do
    print =<< LSMT.lookup table' 0
    LSMT.insert table' 0 "Goodbye" Nothing
    print =<< LSMT.lookup table' 0
  LSMT.lookup table 0
  print =<< LSMT.lookup table 0
:}
Found (Value "Hello")
Found (Value "Goodbye")
Found (Value "Hello")

The worst-case disk I\/O complexity of this operation is \(O(0)\).

__Warning:__ The duplicate must be independently closed using 'closeTable'.

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
['TableClosedError']:
    If the table is closed.
-}
{-# SPECIALISE
  duplicate ::
    Table IO k v b ->
    IO (Table IO k v b)
  #-}
duplicate ::
  forall m k v b.
  (IOLike m) =>
  Table m k v b ->
  m (Table m k v b)
duplicate (Table table) =
  Table <$> Internal.duplicate table

--------------------------------------------------------------------------------
-- Union
--------------------------------------------------------------------------------

{- |
Run an action with access to a table that contains the union of the entries of the given tables.

>>> :{
runExample $ \session table1 -> do
  LSMT.insert table1 0 "Hello" Nothing
  LSMT.withTable session $ \table2 -> do
    LSMT.insert table2 0 "World" Nothing
    LSMT.insert table2 1 "Goodbye" Nothing
    LSMT.withUnion table1 table2 $ \table3 -> do
      print =<< LSMT.lookup table3 0
      print =<< LSMT.lookup table3 1
    print =<< LSMT.lookup table1 0
    print =<< LSMT.lookup table2 0
:}
Found (Value "Hello World")
Found (Value "Goodbye")
Found (Value "Hello")
Found (Value "World")

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
{-# SPECIALISE
  withUnion ::
    (ResolveValue v) =>
    Table IO k v b ->
    Table IO k v b ->
    (Table IO k v b -> IO a) ->
    IO a
  #-}
withUnion ::
  forall m k v b a.
  (IOLike m) =>
  (ResolveValue v) =>
  Table m k v b ->
  Table m k v b ->
  (Table m k v b -> m a) ->
  m a
withUnion table1 table2 =
  bracket (table1 `union` table2) closeTable

{- |
Variant of 'withUnions' that takes any number of tables.
-}
{-# SPECIALISE
  withUnions ::
    (ResolveValue v) =>
    NonEmpty (Table IO k v b) ->
    (Table IO k v b -> IO a) ->
    IO a
  #-}
withUnions ::
  forall m k v b a.
  (IOLike m) =>
  (ResolveValue v) =>
  NonEmpty (Table m k v b) ->
  (Table m k v b -> m a) ->
  m a
withUnions tables =
  bracket (unions tables) closeTable

{- |
Create a table that contains the union of the entries of the given tables.

If the given key is a member of a single input table, then the same key and value occur in the output table.
Otherwise, the values for duplicate keys are combined using 'resolve' from left to right.
If the 'resolve' function behaves like 'const', then this computes a left-biased union.

>>> :{
runExample $ \session table1 -> do
  LSMT.insert table1 0 "Hello" Nothing
  LSMT.withTable session $ \table2 -> do
    LSMT.insert table2 0 "World" Nothing
    LSMT.insert table2 1 "Goodbye" Nothing
    bracket (LSMT.union table1 table2) LSMT.closeTable $ \table3 -> do
      print =<< LSMT.lookup table3 0
      print =<< LSMT.lookup table3 1
    print =<< LSMT.lookup table1 0
    print =<< LSMT.lookup table2 0
:}
Found (Value "Hello World")
Found (Value "Goodbye")
Found (Value "Hello")
Found (Value "World")

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
{-# SPECIALISE
  union ::
    (ResolveValue v) =>
    Table IO k v b ->
    Table IO k v b ->
    IO (Table IO k v b)
  #-}
union ::
  forall m k v b.
  (IOLike m) =>
  (ResolveValue v) =>
  Table m k v b ->
  Table m k v b ->
  m (Table m k v b)
union table1 table2 =
  unions (table1 :| table2 : [])

{- |
Variant of 'union' that takes any number of tables.
-}
{-# SPECIALISE
  unions ::
    (ResolveValue v) =>
    NonEmpty (Table IO k v b) ->
    IO (Table IO k v b)
  #-}
unions ::
  forall m k v b.
  (IOLike m) =>
  (ResolveValue v) =>
  NonEmpty (Table m k v b) ->
  m (Table m k v b)
unions tables = do
  bracketOnError (incrementalUnions tables) closeTable $ \table -> do
    UnionDebt debt <- remainingUnionDebt table
    UnionCredits leftovers <- supplyUnionCredits table (UnionCredits debt)
    assert (leftovers >= 0) $ pure ()
    pure table

{- |
Run an action with access to a table that incrementally computes the union of the given tables.

>>> :{
runExample $ \session table1 -> do
  LSMT.insert table1 0 "Hello" Nothing
  LSMT.withTable session $ \table2 -> do
    LSMT.insert table2 0 "World" Nothing
    LSMT.insert table2 1 "Goodbye" Nothing
    LSMT.withIncrementalUnion table1 table2 $ \table3 -> do
      print =<< LSMT.lookup table3 0
      print =<< LSMT.lookup table3 1
    print =<< LSMT.lookup table1 0
    print =<< LSMT.lookup table2 0
:}
Found (Value "Hello World")
Found (Value "Goodbye")
Found (Value "Hello")
Found (Value "World")

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
{-# SPECIALISE
  withIncrementalUnion ::
    Table IO k v b ->
    Table IO k v b ->
    (Table IO k v b -> IO a) ->
    IO a
  #-}
withIncrementalUnion ::
  forall m k v b a.
  (IOLike m) =>
  Table m k v b ->
  Table m k v b ->
  (Table m k v b -> m a) ->
  m a
withIncrementalUnion table1 table2 =
  bracket (incrementalUnion table1 table2) closeTable

{- |
Variant of 'withIncrementalUnion' that takes any number of tables.

The worst-case disk I\/O complexity of this operation depends on the merge policy of the table:

['LazyLevelling']:
    \(O(T \log_T \frac{n}{B} + b)\).

The variable \(b\) refers to the number of input tables.
-}
{-# SPECIALISE
  withIncrementalUnions ::
    NonEmpty (Table IO k v b) ->
    (Table IO k v b -> IO a) ->
    IO a
  #-}
withIncrementalUnions ::
  forall m k v b a.
  (IOLike m) =>
  NonEmpty (Table m k v b) ->
  (Table m k v b -> m a) ->
  m a
withIncrementalUnions tables =
  bracket (incrementalUnions tables) closeTable

{- |
Create a table that incrementally computes the union of the given tables.

>>> :{
runExample $ \session table1 -> do
  LSMT.insert table1 0 "Hello" Nothing
  LSMT.withTable session $ \table2 -> do
    LSMT.insert table2 0 "World" Nothing
    LSMT.insert table2 1 "Goodbye" Nothing
    bracket (LSMT.incrementalUnion table1 table2) LSMT.closeTable $ \table3 -> do
      print =<< LSMT.lookup table3 0
      print =<< LSMT.lookup table3 1
    print =<< LSMT.lookup table1 0
    print =<< LSMT.lookup table2 0
:}
Found (Value "Hello World")
Found (Value "Goodbye")
Found (Value "Hello")
Found (Value "World")

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
{-# SPECIALISE
  incrementalUnion ::
    Table IO k v b ->
    Table IO k v b ->
    IO (Table IO k v b)
  #-}
incrementalUnion ::
  forall m k v b.
  (IOLike m) =>
  Table m k v b ->
  Table m k v b ->
  m (Table m k v b)
incrementalUnion table1 table2 = do
  incrementalUnions (table1 :| table2 : [])

{- |
Variant of 'incrementalUnion' for any number of tables.

The worst-case disk I\/O complexity of this operation is \(O(b)\),
where the variable \(b\) refers to the number of input tables.
-}
{-# SPECIALISE
  incrementalUnions ::
    NonEmpty (Table IO k v b) ->
    IO (Table IO k v b)
  #-}
incrementalUnions ::
  forall m k v b.
  (IOLike m) =>
  NonEmpty (Table m k v b) ->
  m (Table m k v b)
incrementalUnions tables@(Table _ :| _) =
  _withInternalTables tables (fmap Table . Internal.unions)

-- | Internal helper. Run an action with access to the underlying tables.
{-# SPECIALISE
  _withInternalTables ::
    NonEmpty (Table IO k v b) ->
    (forall h. (Typeable h) => NonEmpty (Internal.Table IO h) -> IO a) ->
    IO a
  #-}
_withInternalTables ::
  forall m k v b a.
  (IOLike m) =>
  NonEmpty (Table m k v b) ->
  (forall h. (Typeable h) => NonEmpty (Internal.Table m h) -> m a) ->
  m a
_withInternalTables (Table (table :: Internal.Table m h) :| tables) action =
  action . (table :|) =<< traverse assertTableType (zip [1 ..] tables)
 where
  assertTableType :: (Int, Table m k v b) -> m (Internal.Table m h)
  assertTableType (i, Table (table' :: Internal.Table m h'))
    | Just Refl <- eqT @h @h' = pure table'
    | otherwise = throwIO $ ErrTableUnionHandleTypeMismatch 0 (typeRep $ Proxy @h) i (typeRep $ Proxy @h')

{- |
Get an /upper bound/ for the amount of remaining union debt.
This includes the union debt of any table that was part of the union's input.

>>> :{
runExample $ \session table1 -> do
  LSMT.insert table1 0 "Hello" Nothing
  LSMT.withTable session $ \table2 -> do
    LSMT.insert table2 0 "World" Nothing
    LSMT.insert table2 1 "Goodbye" Nothing
    bracket (LSMT.incrementalUnion table1 table2) LSMT.closeTable $ \table3 -> do
      putStrLn . ("UnionDebt: "<>) . show =<< LSMT.remainingUnionDebt table3
:}
UnionDebt: 4

The worst-case disk I\/O complexity of this operation is \(O(0)\).

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
['TableClosedError']:
    If the table is closed.
-}
{-# SPECIALISE
  remainingUnionDebt ::
    Table IO k v b ->
    IO UnionDebt
  #-}
remainingUnionDebt ::
  forall m k v b.
  (IOLike m) =>
  Table m k v b ->
  m UnionDebt
remainingUnionDebt (Table table) =
  Internal.remainingUnionDebt table

{- |
Supply the given amount of union credits.

This reduces the union debt by /at least/ the number of supplied union credits.
It is therefore advisable to query 'remainingUnionDebt' every once in a while to get an upper bound on the current debt.

This function returns any surplus of union credits as /leftover/ credits when a union has finished.
In particular, if the returned number of credits is positive, then the union is finished.

>>> :{
runExample $ \session table1 -> do
  LSMT.insert table1 0 "Hello" Nothing
  LSMT.withTable session $ \table2 -> do
    LSMT.insert table2 0 "World" Nothing
    LSMT.insert table2 1 "Goodbye" Nothing
    bracket (LSMT.incrementalUnion table1 table2) LSMT.closeTable $ \table3 -> do
      putStrLn . ("UnionDebt: "<>) . show =<< LSMT.remainingUnionDebt table3
      putStrLn . ("Leftovers: "<>) . show =<< LSMT.supplyUnionCredits table3 2
      putStrLn . ("UnionDebt: "<>) . show =<< LSMT.remainingUnionDebt table3
      putStrLn . ("Leftovers: "<>) . show =<< LSMT.supplyUnionCredits table3 4
:}
UnionDebt: 4
Leftovers: 0
UnionDebt: 2
Leftovers: 3

__NOTE:__
The 'remainingUnionDebt' functions gets an /upper bound/ for the amount of remaning union debt.
In the example above, the second call to 'remainingUnionDebt' reports @2@, but the union debt is @1@.
Therefore, the second call to 'supplyUnionCredits' returns more leftovers than expected.

The worst-case disk I\/O complexity of this operation is \(O(\frac{b}{P})\),
where the variable \(b\) refers to the amount of credits supplied.

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
['TableClosedError']:
    If the table is closed.
-}
{-# SPECIALISE
  supplyUnionCredits ::
    (ResolveValue v) =>
    Table IO k v b ->
    UnionCredits ->
    IO UnionCredits
  #-}
supplyUnionCredits ::
  forall m k v b.
  (IOLike m) =>
  (ResolveValue v) =>
  Table m k v b ->
  UnionCredits ->
  m UnionCredits
supplyUnionCredits (Table table :: Table m k v b) credits =
  Internal.supplyUnionCredits (_getResolveSerialisedValue (Proxy @v)) table credits

--------------------------------------------------------------------------------
-- Blob References
--------------------------------------------------------------------------------

-- NOTE: 'BlobRef' is defined in 'Database.LSMTree.Internal.Types'

{- |
Retrieve the blob value from a blob reference.

>>> :{
runExample $ \session table -> do
  LSMT.insert table 0 "Hello" (Just "World")
  print
    =<< traverse (LSMT.retrieveBlob session)
    =<< LSMT.lookup table 0
:}
FoundWithBlob (Value "Hello") (Blob "World")

The worst-case disk I\/O complexity of this operation is \(O(1)\).

__Warning:__ A blob reference is /not stable/. Any operation that modifies the table,
cursor, or session that corresponds to a blob reference may cause it to be invalidated.

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
['BlobRefInvalidError']:
    If the blob reference has been invalidated.
-}
{-# SPECIALISE
  retrieveBlob ::
    (SerialiseValue b) =>
    Session IO ->
    BlobRef IO b ->
    IO b
  #-}
retrieveBlob ::
  forall m b.
  (IOLike m, SerialiseValue b) =>
  Session m ->
  BlobRef m b ->
  m b
retrieveBlob session blobRef = do
  blobs <- retrieveBlobs session (V.singleton blobRef)
  pure $ V.head blobs

{- |
Variant of 'retrieveBlob' for batch retrieval.
The batch of blob references corresponds in-order to the batch of results.

The worst-case disk I\/O complexity of this operation is \(O(b)\),
where the variable \(b\) refers to the length of the input vector.

The following property holds in the absence of races:

prop> retrieveBlobs session blobRefs = traverse (retrieveBlob session) blobRefs
-}
{-# SPECIALISE
  retrieveBlobs ::
    (SerialiseValue b) =>
    Session IO ->
    Vector (BlobRef IO b) ->
    IO (Vector b)
  #-}
retrieveBlobs ::
  forall m b.
  (IOLike m, SerialiseValue b) =>
  Session m ->
  Vector (BlobRef m b) ->
  m (Vector b)
retrieveBlobs (Session (session :: Internal.Session m h)) blobRefs = do
  let numBlobRefs = V.length blobRefs
  let blobRefNums = V.enumFromTo 0 (numBlobRefs - 1)
  weakBlobRefs <- traverse assertBlobRefHandleType (V.zip blobRefNums blobRefs)
  serialisedBlobs <- Internal.retrieveBlobs session weakBlobRefs
  pure $ Internal.deserialiseBlob <$> serialisedBlobs
 where
  assertBlobRefHandleType :: (Int, BlobRef m b) -> m (Internal.WeakBlobRef m h)
  assertBlobRefHandleType (i, BlobRef (weakBlobRef :: Internal.WeakBlobRef m h'))
    | Just Refl <- eqT @h @h' = pure weakBlobRef
    | otherwise = throwIO $ ErrBlobRefInvalid i

--------------------------------------------------------------------------------
-- Cursors
--------------------------------------------------------------------------------

-- NOTE: 'Cursor' is defined in 'Database.LSMTree.Internal.Types'

{- |
Run an action with access to a cursor.

>>> :{
runExample $ \session table -> do
  LSMT.insert table 0 "Hello" Nothing
  LSMT.insert table 1 "World" Nothing
  LSMT.withCursor table $ \cursor -> do
    traverse_ print
      =<< LSMT.take 32 cursor
:}
Entry (Key 0) (Value "Hello")
Entry (Key 1) (Value "World")

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
{-# SPECIALISE
  withCursor ::
    (ResolveValue v) =>
    Table IO k v b ->
    (Cursor IO k v b -> IO a) ->
    IO a
  #-}
withCursor ::
  forall m k v b a.
  (IOLike m) =>
  (ResolveValue v) =>
  Table m k v b ->
  (Cursor m k v b -> m a) ->
  m a
withCursor (Table table) action =
  Internal.withCursor (_getResolveSerialisedValue (Proxy @v)) Internal.NoOffsetKey table (action . Cursor)

{- |
Variant of 'withCursor' that starts at a given key.

>>> :{
runExample $ \session table -> do
  LSMT.insert table 0 "Hello" Nothing
  LSMT.insert table 1 "World" Nothing
  LSMT.withCursorAtOffset table 1 $ \cursor -> do
    traverse_ print
      =<< LSMT.take 32 cursor
:}
Entry (Key 1) (Value "World")
-}
{-# SPECIALISE
  withCursorAtOffset ::
    (SerialiseKey k, ResolveValue v) =>
    Table IO k v b ->
    k ->
    (Cursor IO k v b -> IO a) ->
    IO a
  #-}
withCursorAtOffset ::
  forall m k v b a.
  (IOLike m) =>
  (SerialiseKey k, ResolveValue v) =>
  Table m k v b ->
  k ->
  (Cursor m k v b -> m a) ->
  m a
withCursorAtOffset (Table table) offsetKey action =
  Internal.withCursor (_getResolveSerialisedValue (Proxy @v)) (Internal.OffsetKey $ Internal.serialiseKey offsetKey) table (action . Cursor)

{- |
Create a cursor for the given table.

>>> :{
runExample $ \session table -> do
  LSMT.insert table 0 "Hello" Nothing
  LSMT.insert table 1 "World" Nothing
  bracket (LSMT.newCursor table) LSMT.closeCursor $ \cursor -> do
    traverse_ print
      =<< LSMT.take 32 cursor
:}
Entry (Key 0) (Value "Hello")
Entry (Key 1) (Value "World")

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
{-# SPECIALISE
  newCursor ::
    (ResolveValue v) =>
    Table IO k v b ->
    IO (Cursor IO k v b)
  #-}
newCursor ::
  forall m k v b.
  (IOLike m) =>
  (ResolveValue v) =>
  Table m k v b ->
  m (Cursor m k v b)
newCursor (Table table) =
  Cursor <$> Internal.newCursor (_getResolveSerialisedValue (Proxy @v)) Internal.NoOffsetKey table

{- |
Variant of 'newCursor' that starts at a given key.

>>> :{
runExample $ \session table -> do
  LSMT.insert table 0 "Hello" Nothing
  LSMT.insert table 1 "World" Nothing
  bracket (LSMT.newCursorAtOffset table 1) LSMT.closeCursor $ \cursor -> do
    traverse_ print
      =<< LSMT.take 32 cursor
:}
Entry (Key 1) (Value "World")
-}
{-# SPECIALISE
  newCursorAtOffset ::
    (SerialiseKey k, ResolveValue v) =>
    Table IO k v b ->
    k ->
    IO (Cursor IO k v b)
  #-}
newCursorAtOffset ::
  forall m k v b.
  (IOLike m) =>
  (SerialiseKey k, ResolveValue v) =>
  Table m k v b ->
  k ->
  m (Cursor m k v b)
newCursorAtOffset (Table table) offsetKey =
  Cursor <$> Internal.newCursor (_getResolveSerialisedValue (Proxy @v)) (Internal.OffsetKey $ Internal.serialiseKey offsetKey) table

{- |
Close a cursor.

The worst-case disk I\/O complexity of this operation depends on the merge policy of the table:

['LazyLevelling']:
    \(O(T \log_T \frac{n}{B})\).

Closing is idempotent, i.e., closing a closed cursor does nothing.
All other operations on a closed cursor will throw an exception.
-}
{-# SPECIALISE
  closeCursor ::
    Cursor IO k v b ->
    IO ()
  #-}
closeCursor ::
  forall m k v b.
  (IOLike m) =>
  Cursor m k v b ->
  m ()
closeCursor (Cursor cursor) =
  Internal.closeCursor cursor

{- |
Read the next table entry from the cursor.

>>> :{
runExample $ \session table -> do
  LSMT.insert table 0 "Hello" Nothing
  LSMT.insert table 1 "World" Nothing
  LSMT.withCursor table $ \cursor -> do
    print =<< LSMT.next cursor
    print =<< LSMT.next cursor
    print =<< LSMT.next cursor
:}
Just (Entry (Key 0) (Value "Hello"))
Just (Entry (Key 1) (Value "World"))
Nothing

The worst-case disk I\/O complexity of this operation is \(O(\frac{1}{P})\).

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
['CursorClosedError']:
    If the cursor is closed.
-}
{-# SPECIALISE
  next ::
    (SerialiseKey k, SerialiseValue v, ResolveValue v) =>
    Cursor IO k v b ->
    IO (Maybe (Entry k v (BlobRef IO b)))
  #-}
next ::
  forall m k v b.
  (IOLike m) =>
  (SerialiseKey k, SerialiseValue v, ResolveValue v) =>
  Cursor m k v b ->
  m (Maybe (Entry k v (BlobRef m b)))
next iterator = do
  -- TODO: implement this function in terms of 'readEntry'
  entries <- take 1 iterator
  pure $ fst <$> V.uncons entries

{- |
Read the next batch of table entries from the cursor.

>>> :{
runExample $ \session table -> do
  LSMT.insert table 0 "Hello" Nothing
  LSMT.insert table 1 "World" Nothing
  LSMT.withCursor table $ \cursor -> do
    traverse_ print
      =<< LSMT.take 32 cursor
:}
Entry (Key 0) (Value "Hello")
Entry (Key 1) (Value "World")

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
{-# SPECIALISE
  take ::
    (SerialiseKey k, SerialiseValue v, ResolveValue v) =>
    Int ->
    Cursor IO k v b ->
    IO (Vector (Entry k v (BlobRef IO b)))
  #-}
take ::
  forall m k v b.
  (IOLike m) =>
  (SerialiseKey k, SerialiseValue v, ResolveValue v) =>
  Int ->
  Cursor m k v b ->
  m (Vector (Entry k v (BlobRef m b)))
take n (Cursor cursor :: Cursor m k v b) =
  Internal.readCursor (_getResolveSerialisedValue (Proxy @v)) n cursor $ \ !k !v -> \case
    Just !b -> EntryWithBlob (Internal.deserialiseKey k) (Internal.deserialiseValue v) (BlobRef b)
    Nothing -> Entry (Internal.deserialiseKey k) (Internal.deserialiseValue v)

{- |
Variant of 'take' that accepts an additional predicate to determine whether or not to continue reading.

>>> :{
runExample $ \session table -> do
  LSMT.insert table 0 "Hello" Nothing
  LSMT.insert table 1 "World" Nothing
  LSMT.withCursor table $ \cursor -> do
    traverse_ print
      =<< LSMT.takeWhile 32 (<1) cursor
:}
Entry (Key 0) (Value "Hello")

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
{-# SPECIALISE
  takeWhile ::
    (SerialiseKey k, SerialiseValue v, ResolveValue v) =>
    Int ->
    (k -> Bool) ->
    Cursor IO k v b ->
    IO (Vector (Entry k v (BlobRef IO b)))
  #-}
takeWhile ::
  forall m k v b.
  (IOLike m) =>
  (SerialiseKey k, SerialiseValue v, ResolveValue v) =>
  Int ->
  (k -> Bool) ->
  Cursor m k v b ->
  m (Vector (Entry k v (BlobRef m b)))
takeWhile n p (Cursor cursor :: Cursor m k v b) =
  -- TODO: implement this function using a variant of 'readCursorWhile' that does not take the maximum batch size
  Internal.readCursorWhile (_getResolveSerialisedValue (Proxy @v)) (p . Internal.deserialiseKey) n cursor $  \ !k !v -> \case
    Just !b -> EntryWithBlob (Internal.deserialiseKey k) (Internal.deserialiseValue v) (BlobRef b)
    Nothing -> Entry (Internal.deserialiseKey k) (Internal.deserialiseValue v)

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

>>> :{
runExample $ \session table -> do
  LSMT.insert table 0 "Hello" Nothing
  LSMT.insert table 1 "World" Nothing
  LSMT.saveSnapshot "example" "Key Value Blob" table
:}

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
{-# SPECIALISE
  saveSnapshot ::
    SnapshotName ->
    SnapshotLabel ->
    Table IO k v b ->
    IO ()
  #-}
saveSnapshot ::
  forall m k v b.
  (IOLike m) =>
  SnapshotName ->
  SnapshotLabel ->
  Table m k v b ->
  m ()
saveSnapshot snapName snapLabel (Table table) =
  Internal.saveSnapshot snapName snapLabel table

{- |
Run an action with access to a table from a snapshot.

>>> :{
runExample $ \session table -> do
  -- Save snapshot
  LSMT.insert table 0 "Hello" Nothing
  LSMT.insert table 1 "World" Nothing
  LSMT.saveSnapshot "example" "Key Value Blob" table
  -- Open snapshot
  LSMT.withTableFromSnapshot @_ @Key @Value @Blob session "example" "Key Value Blob" $ \table' -> do
      LSMT.withCursor table' $ \cursor ->
        traverse_ print
          =<< LSMT.take 32 cursor
:}
Entry (Key 0) (Value "Hello")
Entry (Key 1) (Value "World")

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
{-# SPECIALISE
  withTableFromSnapshot ::
    (ResolveValue v) =>
    Session IO ->
    SnapshotName ->
    SnapshotLabel ->
    (Table IO k v b -> IO a) ->
    IO a
  #-}
withTableFromSnapshot ::
  forall m k v b a.
  (IOLike m) =>
  (ResolveValue v) =>
  Session m ->
  SnapshotName ->
  SnapshotLabel ->
  (Table m k v b -> m a) ->
  m a
withTableFromSnapshot session snapName snapLabel =
  bracket (openTableFromSnapshot session snapName snapLabel) closeTable

{- |
Variant of 'withTableFromSnapshot' that accepts [table configuration overrides](#g:table_configuration_overrides).
-}
{-# SPECIALISE
  withTableFromSnapshotWith ::
    forall k v b a.
    (ResolveValue v) =>
    TableConfigOverride ->
    Session IO ->
    SnapshotName ->
    SnapshotLabel ->
    (Table IO k v b -> IO a) ->
    IO a
  #-}
withTableFromSnapshotWith ::
  forall m k v b a.
  (IOLike m) =>
  (ResolveValue v) =>
  TableConfigOverride ->
  Session m ->
  SnapshotName ->
  SnapshotLabel ->
  (Table m k v b -> m a) ->
  m a
withTableFromSnapshotWith tableConfigOverride session snapName snapLabel =
  bracket (openTableFromSnapshotWith tableConfigOverride session snapName snapLabel) closeTable

{- |
Open a table from a named snapshot.

>>> :{
runExample $ \session table -> do
  -- Save snapshot
  LSMT.insert table 0 "Hello" Nothing
  LSMT.insert table 1 "World" Nothing
  LSMT.saveSnapshot "example" "Key Value Blob" table
  -- Open snapshot
  bracket
    (LSMT.openTableFromSnapshot @_ @Key @Value @Blob session "example" "Key Value Blob")
    LSMT.closeTable $ \table' -> do
      LSMT.withCursor table' $ \cursor ->
        traverse_ print
          =<< LSMT.take 32 cursor
:}
Entry (Key 0) (Value "Hello")
Entry (Key 1) (Value "World")

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
{-# SPECIALISE
  openTableFromSnapshot ::
    forall k v b.
    (ResolveValue v) =>
    Session IO ->
    SnapshotName ->
    SnapshotLabel ->
    IO (Table IO k v b)
  #-}
openTableFromSnapshot ::
  forall m k v b.
  (IOLike m) =>
  (ResolveValue v) =>
  Session m ->
  SnapshotName ->
  SnapshotLabel ->
  m (Table m k v b)
openTableFromSnapshot session snapName snapLabel =
  openTableFromSnapshotWith noTableConfigOverride session snapName snapLabel

{- |
Variant of 'openTableFromSnapshot' that accepts [table configuration overrides](#g:table_configuration_overrides).
-}
{-# SPECIALISE
  openTableFromSnapshotWith ::
    forall k v b.
    (ResolveValue v) =>
    TableConfigOverride ->
    Session IO ->
    SnapshotName ->
    SnapshotLabel ->
    IO (Table IO k v b)
  #-}
openTableFromSnapshotWith ::
  forall m k v b.
  (IOLike m) =>
  (ResolveValue v) =>
  TableConfigOverride ->
  Session m ->
  SnapshotName ->
  SnapshotLabel ->
  m (Table m k v b)
openTableFromSnapshotWith tableConfigOverride (Session session) snapName snapLabel =
  Table <$> Internal.openTableFromSnapshot tableConfigOverride session snapName snapLabel (_getResolveSerialisedValue (Proxy @v))

{- |
Delete the named snapshot.

>>> :{
runExample $ \session table -> do
  -- Save snapshot
  LSMT.insert table 0 "Hello" Nothing
  LSMT.insert table 1 "World" Nothing
  LSMT.saveSnapshot "example" "Key Value Blob" table
  -- Delete snapshot
  LSMT.deleteSnapshot session "example"
:}

The worst-case disk I\/O complexity of this operation depends on the merge policy of the table:

['LazyLevelling']:
    \(O(T \log_T \frac{n}{B})\).

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
['SnapshotDoesNotExistError']:
    If no snapshot with the given name exists.
-}
{-# SPECIALISE
  deleteSnapshot ::
    Session IO ->
    SnapshotName ->
    IO ()
  #-}
deleteSnapshot ::
  forall m.
  (IOLike m) =>
  Session m ->
  SnapshotName ->
  m ()
deleteSnapshot (Session session) =
  Internal.deleteSnapshot session

{- |
Check if the named snapshot exists.

>>> :{
runExample $ \session table -> do
  -- Save snapshot
  LSMT.insert table 0 "Hello" Nothing
  LSMT.insert table 1 "World" Nothing
  LSMT.saveSnapshot "example" "Key Value Blob" table
  -- Check snapshots
  print =<< doesSnapshotExist session "example"
  print =<< doesSnapshotExist session "this_snapshot_does_not_exist"
:}
True
False

The worst-case disk I\/O complexity of this operation is \(O(1)\).

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
-}
{-# SPECIALISE
  doesSnapshotExist ::
    Session IO ->
    SnapshotName ->
    IO Bool
  #-}
doesSnapshotExist ::
  forall m.
  (IOLike m) =>
  Session m ->
  SnapshotName ->
  m Bool
doesSnapshotExist (Session session) =
  Internal.doesSnapshotExist session

{- |
List the names of all snapshots.

>>> :{
runExample $ \session table -> do
  -- Save snapshot
  LSMT.insert table 0 "Hello" Nothing
  LSMT.insert table 1 "World" Nothing
  LSMT.saveSnapshot "example" "Key Value Blob" table
  -- List snapshots
  traverse_ print
    =<< listSnapshots session
:}
"example"

The worst-case disk I\/O complexity of this operation is \(O(s)\),
where \(s\) refers to the number of snapshots in the session.

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
-}
{-# SPECIALISE
  listSnapshots ::
    Session IO ->
    IO [SnapshotName]
  #-}
listSnapshots ::
  forall m.
  (IOLike m) =>
  Session m ->
  m [SnapshotName]
listSnapshots (Session session) =
  Internal.listSnapshots session

-- | Internal helper. Get 'resolveSerialised' at type 'ResolveSerialisedValue'.
_getResolveSerialisedValue ::
  forall v.
  (ResolveValue v) =>
  Proxy v ->
  ResolveSerialisedValue
_getResolveSerialisedValue = coerce . resolveSerialised
