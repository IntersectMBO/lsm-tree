{-# LANGUAGE MagicHash       #-}
{-# LANGUAGE PatternSynonyms #-}

{- |
Module      : Database.LSMTree
Copyright   : (c) 2023, Input Output Global, Inc. (IOG)
              (c) 2023-2025, INTERSECT
License     : Apache-2.0
Stability   : experimental
Portability : portable
-}
module Database.LSMTree (
  -- * Comparison with Simple API
  -- $comparison_with_simple_api
  IOLike,

  -- * Example
  -- $example

  -- * Sessions
  Session,
  withSession,
  openSession,
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
  lookup,
  lookups,
  Entry (..),
  rangeLookup,

  -- ** Table Updates #table_updates#
  insert,
  inserts,
  mupsert,
  mupserts,
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
  TableConfig (..),
  defaultTableConfig,
  MergePolicy (MergePolicyLazyLevelling),
  SizeRatio (Four),
  WriteBufferAlloc (AllocNumEntries),
  NumEntries (..),
  BloomFilterAlloc (AllocFixed, AllocRequestFPR),
  defaultBloomFilterAlloc,
  FencePointerIndexType (OrdinaryIndex, CompactIndex),
  DiskCachePolicy (..),
  MergeSchedule (..),

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
  SerialiseValue (serialiseValue, deserialiseValue),

  -- ** Key\/Value Serialisation Property Tests #key_value_serialisation_property_tests#
  serialiseKeyIdentity,
  serialiseKeyIdentityUpToSlicing,
  serialiseKeyMinimalSize,
  serialiseValueIdentity,
  serialiseValueIdentityUpToSlicing,

  -- * Monoidal Value Resolution #monoidal_value_resolution#
  ResolveValue (..),
  ResolveViaSemigroup (..),
  ResolveAsFirst (..),

  -- ** Monoidal Value Resolution Property Tests #monoidal_value_resolution_property_tests#
  resolveCompatibility,
  resolveValidOutput,
  resolveAssociativity,

  -- * Tracer
  Tracer,
  LSMTreeTrace (..),
  TableTrace (..),
  CursorTrace (..),

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
import           Data.Maybe (fromMaybe)
import           Data.Typeable (Proxy (..), Typeable, eqT, type (:~:) (Refl),
                     typeRep)
import           Data.Vector (Vector)
import qualified Data.Vector as V
import qualified Database.LSMTree.Internal.BlobRef as Internal
import           Database.LSMTree.Internal.Config
                     (BloomFilterAlloc (AllocFixed, AllocRequestFPR),
                     DiskCachePolicy (..), FencePointerIndexType (..),
                     MergePolicy (..), MergeSchedule (..), SizeRatio (..),
                     TableConfig (..), WriteBufferAlloc (..),
                     defaultBloomFilterAlloc, defaultTableConfig)
import           Database.LSMTree.Internal.Config.Override
                     (OverrideDiskCachePolicy (..))
import           Database.LSMTree.Internal.Entry (NumEntries (..))
import qualified Database.LSMTree.Internal.Entry as Entry
import           Database.LSMTree.Internal.Paths (SnapshotName,
                     isValidSnapshotName, toSnapshotName)
import           Database.LSMTree.Internal.Range (Range (..))
import           Database.LSMTree.Internal.RawBytes (RawBytes (..))
import qualified Database.LSMTree.Internal.Serialise as Internal
import           Database.LSMTree.Internal.Serialise.Class (SerialiseKey (..),
                     SerialiseValue (..), serialiseKeyIdentity,
                     serialiseKeyIdentityUpToSlicing, serialiseKeyMinimalSize,
                     serialiseValueIdentity, serialiseValueIdentityUpToSlicing)
import           Database.LSMTree.Internal.Snapshot (SnapshotLabel (..))
import qualified Database.LSMTree.Internal.Snapshot as Internal
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
import           System.FS.API (FsPath, HasFS (..))
import           System.FS.BlockIO.API (HasBlockIO (..))
import           System.FS.IO (HandleIO)

--------------------------------------------------------------------------------
-- Comparison with Simple API
--------------------------------------------------------------------------------

{- $comparison_with_simple_api
  TODO: comparison with "Database.LSMTree.Simple"
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

>>> import           Prelude hiding (lookup)
>>> import           System.FS.Sim.MockFS (MockFS)
>>> import qualified System.FS.Sim.MockFS as Mock
>>> import           System.FS.Sim.STM (simHasFS)
>>> import qualified System.FS.BlockIO.Sim as SimHasBlockIO (fromHasFS)
>>> import           Data.ByteString.Short (ShortByteString)
>>> import           Data.Word (Word64)

>>> mockFSVar <- newTMVar Mock.empty
>>> let hasFS = simHasFS mockFSVar
>>> let hasBlockIO = SimHasBlockIO.fromHasFS hasFS
-}

{- $example

:{
withSession mySessionDirectory $ \session -> do
  withTable @Word64 @ShortByteString session $ \table -> do
    insert table 0 "Hello"
    insert table 1 "World"
    lookup table 0
:}
Just "Hello"
-}

--------------------------------------------------------------------------------
-- Sessions
--------------------------------------------------------------------------------

-- NOTE: 'Session' is defined in 'Database.LSMTree.Internal.Types'

{- |
Run an action with access to a session opened from a session directory.

The worst-case disk I\/O complexity of this operation is \(O(1)\).

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
{-# SPECIALIZE
  withSession ::
    Tracer IO LSMTreeTrace ->
    HasFS IO HandleIO ->
    HasBlockIO IO HandleIO ->
    FsPath ->
    (Session IO -> IO a) ->
    IO a #-}
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
{-# SPECIALIZE
  openSession ::
    Tracer IO LSMTreeTrace ->
    HasFS IO HandleIO ->
    HasBlockIO IO HandleIO ->
    -- \| The session directory.
    FsPath ->
    IO (Session IO) #-}
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

{- |
Close a session.

The worst-case disk I\/O complexity of this operation is \(O(t \log_T n)\),
where the variable \(t\) refers to the number of tables in the session.

If the session has any open tables, then 'closeTable' is called for each open table.
Otherwise, this operation takes constant time.

Closing is idempotent, i.e., closing a closed session does nothing.
All other operations on a closed session will throw an exception.
-}
{-# SPECIALIZE
  closeSession ::
    Session IO ->
    IO () #-}
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

The worst-case disk I\/O complexity of this operation is \(O(1)\).

This function is exception-safe for both synchronous and asynchronous exceptions.

It is recommended to use this function instead of 'newTable' and 'closeTable'.

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
-}
{-# SPECIALIZE
  withTable ::
    Session IO ->
    (Table IO k v b -> IO a) ->
    IO a #-}
withTable ::
  forall m k v b a.
  (IOLike m) =>
  Session m ->
  (Table m k v b -> m a) ->
  m a
withTable session =
  withTableWith defaultTableConfig session

-- | Variant of 'withTable' that accepts [table configuration](#g:table_configuration).
{-# SPECIALIZE
  withTableWith ::
    TableConfig ->
    Session IO ->
    (Table IO k v b -> IO a) ->
    IO a #-}
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
{-# SPECIALIZE
  newTable ::
    Session IO ->
    IO (Table IO k v b) #-}
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
{-# SPECIALIZE
  newTableWith ::
    TableConfig ->
    Session IO ->
    IO (Table IO k v b) #-}
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

The worst-case disk I\/O complexity of this operation is \(O(\log_T n)\).

Closing is idempotent, i.e., closing a closed table does nothing.
All other operations on a closed table will throw an exception.

__Warning:__ Tables are ephemeral. Once you close a table, its data is lost forever. To persist tables, use [snapshots](#g:snapshots).
-}
{-# SPECIALIZE
  closeTable ::
    Table IO k v b ->
    IO () #-}
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

The worst-case disk I\/O complexity of this operation depends on the merge policy of the table:

['MergePolicyLazyLevelling']:
    \(O(\log_T n)\).

Membership tests can be performed concurrently from multiple Haskell threads.

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
['TableClosedError']:
    If the table is closed.
['TableCorruptedError']:
    If the table data is corrupted.
-}
{-# SPECIALIZE
  member ::
    (SerialiseKey k, ResolveValue v) =>
    Table IO k v b ->
    k ->
    IO Bool #-}
member ::
  forall m k v b.
  (IOLike m) =>
  (SerialiseKey k, ResolveValue v) =>
  Table m k v b ->
  k ->
  m Bool
member = (fmap _isFound .) . lookup

{- |
Variant of 'member' for batch membership tests.
The batch of keys corresponds in-order to the batch of results.

The worst-case disk I\/O complexity of this operation depends on the merge policy of the table:

['MergePolicyLazyLevelling']:
    \(O(b \log_T n)\).

The variable \(b\) refers to the length of the input vector.

The following property holds in the absence of races:

prop> members table keys = traverse (member table) keys
-}
{-# SPECIALIZE
  members ::
    (SerialiseKey k, ResolveValue v) =>
    Table IO k v b ->
    Vector k ->
    IO (Vector Bool) #-}
members ::
  forall m k v b.
  (IOLike m) =>
  (SerialiseKey k, ResolveValue v) =>
  Table m k v b ->
  Vector k ->
  m (Vector Bool)
members = (fmap (fmap _isFound) .) . lookups

data LookupResult v b
  = NotFound
  | Found !v
  | FoundWithBlob !v !b
  deriving stock (Eq, Show, Functor, Foldable, Traversable)

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

-- | Internal helper. Check whether a 'LookupResult' contains a value.
_isFound :: LookupResult v b -> Bool
_isFound = \case
  NotFound -> False
  _        -> True

{- |
Look up the value associated with a key.

The worst-case disk I\/O complexity of this operation depends on the merge policy of the table:

['MergePolicyLazyLevelling']:
    \(O(\log_T n)\).

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
    (SerialiseKey k, ResolveValue v) =>
    Table IO k v b ->
    k ->
    IO (LookupResult v (BlobRef IO b)) #-}
lookup ::
  forall m k v b.
  (IOLike m) =>
  (SerialiseKey k, ResolveValue v) =>
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

['MergePolicyLazyLevelling']:
    \(O(b \log_T n)\).

The variable \(b\) refers to the length of the input vector.

The following property holds in the absence of races:

prop> lookups table keys = traverse (lookup table) keys
-}
{-# SPECIALISE
  lookups ::
    (SerialiseKey k, ResolveValue v) =>
    Table IO k v b ->
    Vector k ->
    IO (Vector (LookupResult v (BlobRef IO b))) #-}
lookups ::
  forall m k v b.
  (IOLike m) =>
  (SerialiseKey k, ResolveValue v) =>
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
    Entry.Mupdate !v -> Found (Internal.deserialiseValue v)
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

The worst-case disk I\/O complexity of this operation is \(O(\log_T n + b)\),
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
-- The worst-case time complexity is \(O(b \log_T n)\).
-- The amortised time complexity is \(\Theta(b \log\log_T n)\).
{-# SPECIALISE
  rangeLookup ::
    (SerialiseKey k, ResolveValue v) =>
    Table IO k v b ->
    Range k ->
    IO (Vector (Entry k v (BlobRef IO b))) #-}
rangeLookup ::
  forall m k v b.
  (IOLike m) =>
  (SerialiseKey k, ResolveValue v) =>
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
Insert a new key and value in the table.
If the key is already present in the table, the associated value is replaced with the given value.

The worst-case disk I\/O complexity of this operation depends on the merge policy of the table:

['MergePolicyLazyLevelling']:
    \(O(\log_T n)\).

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
['TableClosedError']:
    If the table is closed.
-}
{-# SPECIALISE
  insert ::
    (SerialiseKey k, ResolveValue v, SerialiseValue b) =>
    Table IO k v b ->
    k ->
    v ->
    Maybe b ->
    IO () #-}
insert ::
  forall m k v b.
  (IOLike m) =>
  (SerialiseKey k, ResolveValue v, SerialiseValue b) =>
  Table m k v b ->
  k ->
  v ->
  Maybe b ->
  m ()
insert table k v b =
  inserts table (V.singleton (k, v, b))

{- |
Variant of 'insert' for batch insertions.

The worst-case disk I\/O complexity of this operation depends on the merge policy of the table:

['MergePolicyLazyLevelling']:
    \(O(b \log_T n)\).

The variable \(b\) refers to the length of the input vector.

The following property holds in the absence of races:

prop> inserts table entries = traverse_ (uncurry $ insert table) entries
-}
{-# SPECIALISE
  inserts ::
    (SerialiseKey k, ResolveValue v, SerialiseValue b) =>
    Table IO k v b ->
    Vector (k, v, Maybe b) ->
    IO () #-}
inserts ::
  forall m k v b.
  (IOLike m) =>
  (SerialiseKey k, ResolveValue v, SerialiseValue b) =>
  Table m k v b ->
  Vector (k, v, Maybe b) ->
  m ()
inserts table entries =
  updates table (fmap (\(k, v, mb) -> (k, Insert v mb)) entries)

{- |
Insert a new key and value in the table.
If the key is already present in the table, the associated value is replaced with the given value.

The worst-case disk I\/O complexity of this operation depends on the merge policy of the table:

['MergePolicyLazyLevelling']:
    \(O(\log_T n)\).

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
['TableClosedError']:
    If the table is closed.
-}
{-# SPECIALISE
  mupsert ::
    (SerialiseKey k, ResolveValue v, SerialiseValue b) =>
    Table IO k v b ->
    k ->
    v ->
    IO () #-}
mupsert ::
  forall m k v b.
  (IOLike m) =>
  (SerialiseKey k, ResolveValue v, SerialiseValue b) =>
  Table m k v b ->
  k ->
  v ->
  m ()
mupsert table k v =
  mupserts table (V.singleton (k, v))

{- |
Variant of 'mupsert' for batch insertions.

The worst-case disk I\/O complexity of this operation depends on the merge policy of the table:

['MergePolicyLazyLevelling']:
    \(O(b \log_T n)\).

The variable \(b\) refers to the length of the input vector.

The following property holds in the absence of races:

prop> mupserts table entries = traverse_ (uncurry $ mupsert table) entries
-}
{-# SPECIALISE
  mupserts ::
    (SerialiseKey k, ResolveValue v, SerialiseValue b) =>
    Table IO k v b ->
    Vector (k, v) ->
    IO () #-}
mupserts ::
  forall m k v b.
  (IOLike m) =>
  (SerialiseKey k, ResolveValue v, SerialiseValue b) =>
  Table m k v b ->
  Vector (k, v) ->
  m ()
mupserts table entries =
  updates table (second Mupsert <$> entries)

{- |
Delete a key and its value from the table.
If the key is not present in the table, the table is left unchanged.

The worst-case disk I\/O complexity of this operation depends on the merge policy of the table:

['MergePolicyLazyLevelling']:
    \(O(\log_T n)\).

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
['TableClosedError']:
    If the table is closed.
-}
{-# SPECIALISE
  delete ::
    (SerialiseKey k, ResolveValue v, SerialiseValue b) =>
    Table IO k v b ->
    k ->
    IO () #-}
delete ::
  forall m k v b.
  (IOLike m) =>
  (SerialiseKey k, ResolveValue v, SerialiseValue b) =>
  Table m k v b ->
  k ->
  m ()
delete table k =
  deletes table (V.singleton k)

{- |
Variant of 'delete' for batch deletions.

The worst-case disk I\/O complexity of this operation depends on the merge policy of the table:

['MergePolicyLazyLevelling']:
    \(O(b \log_T n)\).

The variable \(b\) refers to the length of the input vector.

The following property holds in the absence of races:

prop> deletes table keys = traverse_ (delete table) keys
-}
{-# SPECIALISE
  deletes ::
    (SerialiseKey k, ResolveValue v, SerialiseValue b) =>
    Table IO k v b ->
    Vector k ->
    IO () #-}
deletes ::
  forall m k v b.
  (IOLike m) =>
  (SerialiseKey k, ResolveValue v, SerialiseValue b) =>
  Table m k v b ->
  Vector k ->
  m ()
deletes table entries =
  updates table (fmap (,Delete) entries)

type Update :: Type -> Type -> Type
data Update v b
  = Insert !v !(Maybe b)
  | Delete
  | Mupsert !v
  deriving stock (Show, Eq)

instance (NFData v, NFData b) => NFData (Update v b) where
  rnf :: Update v b -> ()
  rnf = \case
    Insert v mb -> rnf v `seq` rnf mb
    Delete -> ()
    Mupsert v -> rnf v

{- |
Update the value at a specific key:

* If the given value is 'Just', this operation acts as 'insert'.
* If the given value is 'Nothing', this operation acts as 'delete'.

The worst-case disk I\/O complexity of this operation depends on the merge policy of the table:

['MergePolicyLazyLevelling']:
    \(O(\log_T n)\).

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
['TableClosedError']:
    If the table is closed.
-}
{-# SPECIALISE
  update ::
    (SerialiseKey k, ResolveValue v, SerialiseValue b) =>
    Table IO k v b ->
    k ->
    Update v b ->
    IO () #-}
update ::
  forall m k v b.
  (IOLike m) =>
  (SerialiseKey k, ResolveValue v, SerialiseValue b) =>
  Table m k v b ->
  k ->
  Update v b ->
  m ()
update table k mv =
  updates table (V.singleton (k, mv))

{- |
Variant of 'update' for batch updates.

The worst-case disk I\/O complexity of this operation depends on the merge policy of the table:

['MergePolicyLazyLevelling']:
    \(O(b \log_T n)\).

The variable \(b\) refers to the length of the input vector.

The following property holds in the absence of races:

prop> updates table entries = traverse_ (uncurry $ update table) entries
-}
{-# SPECIALISE
  updates ::
    (SerialiseKey k, ResolveValue v, SerialiseValue b) =>
    Table IO k v b ->
    Vector (k, Update v b) ->
    IO () #-}
updates ::
  forall m k v b.
  (IOLike m) =>
  (SerialiseKey k, ResolveValue v, SerialiseValue b) =>
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
    Mupsert v -> Entry.Mupdate (Internal.serialiseValue v)

--------------------------------------------------------------------------------
-- Duplication
--------------------------------------------------------------------------------

{- |
Run an action with access to the duplicate of a table.

The duplicate is an independent copy of the given table.
The duplicate is unaffected by subsequent updates to the given table and vice versa.

The worst-case disk I\/O complexity of this operation is \(O(1)\).

This function is exception-safe for both synchronous and asynchronous exceptions.

It is recommended to use this function instead of 'duplicate' and 'closeTable'.

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
['TableClosedError']:
    If the table is closed.
-}
-- The worst-case time complexity is \(O(\log_T n)\).
{-# SPECIALISE
  withDuplicate ::
    Table IO k v b ->
    (Table IO k v b -> IO a) ->
    IO a #-}
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
The duplicate is unaffected by subsequent updates to the given table and vice versa.

The worst-case disk I\/O complexity of this operation is \(O(1)\).

__Warning:__ The duplicate must be independently closed using 'closeTable'.

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
['TableClosedError']:
    If the table is closed.
-}
-- The worst-case time complexity is \(O(\log_T n)\).
{-# SPECIALISE
  duplicate ::
    Table IO k v b ->
    IO (Table IO k v b) #-}
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

The worst-case disk I\/O complexity of this operation is \(O(n)\).

This function is exception-safe for both synchronous and asynchronous exceptions.

It is recommended to use this function instead of 'union' and 'closeTable'.

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
  withUnion ::
    (ResolveValue v) =>
    Table IO k v b ->
    Table IO k v b ->
    (Table IO k v b -> IO a) ->
    IO a #-}
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
    IO a #-}
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
Create a table that contains the left-biased union of the entries of the given tables.

The worst-case disk I\/O complexity of this operation is \(O(n)\).

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
    IO (Table IO k v b) #-}
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
    IO (Table IO k v b) #-}
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

The worst-case disk I\/O complexity of this operation is \(O(1)\).

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
-- The worst-case time complexity is \(O(\log_T n)\).
{-# SPECIALISE
  withIncrementalUnion ::
    Table IO k v b ->
    Table IO k v b ->
    (Table IO k v b -> IO a) ->
    IO a #-}
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

The worst-case disk I\/O complexity of this operation is \(O(b)\),
where the variable \(b\) refers to the number of input tables.
-}
-- The worst-case time complexity is \(O(b \log_T n)\).
{-# SPECIALISE
  withIncrementalUnions ::
    NonEmpty (Table IO k v b) ->
    (Table IO k v b -> IO a) ->
    IO a #-}
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
-- The worst-case time complexity is \(O(\log_T n)\).
{-# SPECIALISE
  incrementalUnion ::
    Table IO k v b ->
    Table IO k v b ->
    IO (Table IO k v b) #-}
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
-- The worst-case time complexity is \(O(\log_T n)\).
{-# SPECIALISE
  incrementalUnions ::
    NonEmpty (Table IO k v b) ->
    IO (Table IO k v b) #-}
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
    IO a #-}
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
Get the amount of remaining union debt.
This includes the union debt of any table that was part of the union's input.

The worst-case disk I\/O complexity of this operation is \(O\(1)\).
-}
{-# SPECIALISE
  remainingUnionDebt ::
    Table IO k v b ->
    IO UnionDebt #-}
remainingUnionDebt ::
  forall m k v b.
  (IOLike m) =>
  Table m k v b ->
  m UnionDebt
remainingUnionDebt (Table table) =
  Internal.remainingUnionDebt table

{- |
Supply the given amount of union credits.

The worst-case disk I\/O complexity of this operation is \(O(b)\),
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
    IO UnionCredits #-}
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

The worst-case disk I\/O complexity of this operation is \(O(1)\).

__Warning__: A blob reference is /not stable/. Any operation that modifies the table,
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
    IO b #-}
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
    IO (Vector b) #-}
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

The worst-case disk I\/O complexity of this operation is \(O(\log_T n)\).

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
    Table IO k v b ->
    (Cursor IO k v b -> IO a) ->
    IO a #-}
withCursor ::
  forall m k v b a.
  (IOLike m) =>
  Table m k v b ->
  (Cursor m k v b -> m a) ->
  m a
withCursor (Table table) action =
  Internal.withCursor Internal.NoOffsetKey table (action . Cursor)

{- |
Variant of 'withCursor' that starts at a given key.
-}
{-# SPECIALISE
  withCursorAtOffset ::
    (SerialiseKey k) =>
    Table IO k v b ->
    k ->
    (Cursor IO k v b -> IO a) ->
    IO a #-}
withCursorAtOffset ::
  forall m k v b a.
  (IOLike m) =>
  (SerialiseKey k) =>
  Table m k v b ->
  k ->
  (Cursor m k v b -> m a) ->
  m a
withCursorAtOffset (Table table) offsetKey action =
  Internal.withCursor (Internal.OffsetKey $ Internal.serialiseKey offsetKey) table (action . Cursor)

{- |
Create a cursor for the given table.

The worst-case disk I\/O complexity of this operation is \(O(\log_T n)\).

__Warning:__ Cursors hold open resources and must be closed using 'closeCursor'.

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
['TableClosedError']:
    If the table is closed.
-}
{-# SPECIALISE
  newCursor ::
    Table IO k v b ->
    IO (Cursor IO k v b) #-}
newCursor ::
  forall m k v b.
  (IOLike m) =>
  Table m k v b ->
  m (Cursor m k v b)
newCursor (Table table) =
  Cursor <$> Internal.newCursor Internal.NoOffsetKey table

{- |
Variant of 'newCursor' that starts at a given key.
-}
{-# SPECIALISE
  newCursorAtOffset ::
    (SerialiseKey k) =>
    Table IO k v b ->
    k ->
    IO (Cursor IO k v b) #-}
newCursorAtOffset ::
  forall m k v b.
  (IOLike m) =>
  (SerialiseKey k) =>
  Table m k v b ->
  k ->
  m (Cursor m k v b)
newCursorAtOffset (Table table) offsetKey =
  Cursor <$> Internal.newCursor (Internal.OffsetKey $ Internal.serialiseKey offsetKey) table

{- |
Close a cursor.

The worst-case disk I\/O complexity of this operation is \(O(\log_T n)\).

Closing is idempotent, i.e., closing a closed cursor does nothing.
All other operations on a closed cursor will throw an exception.
-}
{-# SPECIALISE
  closeCursor ::
    Cursor IO k v b ->
    IO () #-}
closeCursor ::
  forall m k v b.
  (IOLike m) =>
  Cursor m k v b ->
  m ()
closeCursor (Cursor cursor) =
  Internal.closeCursor cursor

{- |
Read the next table entry from the cursor.

The worst-case disk I\/O complexity of this operation is \(O(1)\).

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
['CursorClosedError']:
    If the cursor is closed.
-}
-- The worst-case time complexity is \(O(\log_T n)\)
-- The amortised time complexity is \(\Theta(\log\log_T n)\).
-- TODO: implement this function in terms of 'readEntry'
{-# SPECIALISE
  next ::
    (SerialiseKey k, ResolveValue v) =>
    Cursor IO k v b ->
    IO (Maybe (Entry k v (BlobRef IO b))) #-}
next ::
  forall m k v b.
  (IOLike m) =>
  (SerialiseKey k, ResolveValue v) =>
  Cursor m k v b ->
  m (Maybe (Entry k v (BlobRef m b)))
next iterator = do
  entries <- take 1 iterator
  pure $ fst <$> V.uncons entries

{- |
Read the next batch of table entries from the cursor.

The worst-case disk I\/O complexity of this operation is \(O(b)\),
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
-- The worst-case time complexity is \(O(b \log_T n)\).
-- The amortised time complexity is \(\Theta(b \log\log_T n)\).
{-# SPECIALISE
  take ::
    (SerialiseKey k, ResolveValue v) =>
    Int ->
    Cursor IO k v b ->
    IO (Vector (Entry k v (BlobRef IO b))) #-}
take ::
  forall m k v b.
  (IOLike m) =>
  (SerialiseKey k, ResolveValue v) =>
  Int ->
  Cursor m k v b ->
  m (Vector (Entry k v (BlobRef m b)))
take n (Cursor cursor :: Cursor m k v b) =
  Internal.readCursor (_getResolveSerialisedValue (Proxy @v)) n cursor $ \ !k !v -> \case
    Just !b -> EntryWithBlob (Internal.deserialiseKey k) (Internal.deserialiseValue v) (BlobRef b)
    Nothing -> Entry (Internal.deserialiseKey k) (Internal.deserialiseValue v)

{- |
Variant of 'take' that accepts an additional predicate to determine whether or not to continue reading.

The worst-case disk I\/O complexity of this operation is \(O(b)\),
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
-- The worst-case time complexity is \(O(b \log_T n)\).
-- The amortised time complexity is \(\Theta(b \log\log_T n)\).
-- TODO: implement this function using a variant of 'readCursorWhile' that does not take the maximum batch size
{-# SPECIALISE
  takeWhile ::
    (SerialiseKey k, ResolveValue v) =>
    Int ->
    (k -> Bool) ->
    Cursor IO k v b ->
    IO (Vector (Entry k v (BlobRef IO b))) #-}
takeWhile ::
  forall m k v b.
  (IOLike m) =>
  (SerialiseKey k, ResolveValue v) =>
  Int ->
  (k -> Bool) ->
  Cursor m k v b ->
  m (Vector (Entry k v (BlobRef m b)))
takeWhile n p (Cursor cursor :: Cursor m k v b) =
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

The worst-case disk I\/O complexity of this operation is \(O(\log_T n)\).

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
    IO () #-}
saveSnapshot ::
  forall m k v b.
  (IOLike m) =>
  SnapshotName ->
  SnapshotLabel ->
  Table m k v b ->
  m ()
saveSnapshot snapName snapLabel (Table table) =
  -- TODO: remove SnapshotTableType
  Internal.createSnapshot snapName snapLabel Internal.SnapSimpleTable table

{- |
Run an action with access to a table from a snapshot.

The worst-case disk I\/O complexity of this operation is \(O(n)\).

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
    IO a #-}
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
    OverrideDiskCachePolicy ->
    Session IO ->
    SnapshotName ->
    SnapshotLabel ->
    (Table IO k v b -> IO a) ->
    IO a #-}
withTableFromSnapshotWith ::
  forall m k v b a.
  (IOLike m) =>
  (ResolveValue v) =>
  OverrideDiskCachePolicy ->
  Session m ->
  SnapshotName ->
  SnapshotLabel ->
  (Table m k v b -> m a) ->
  m a
withTableFromSnapshotWith tableConfigOverride session snapName snapLabel =
  bracket (openTableFromSnapshotWith tableConfigOverride session snapName snapLabel) closeTable

{- |
Open a table from a named snapshot.

The worst-case disk I\/O complexity of this operation is \(O(n)\).

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
    IO (Table IO k v b) #-}
openTableFromSnapshot ::
  forall m k v b.
  (IOLike m) =>
  (ResolveValue v) =>
  Session m ->
  SnapshotName ->
  SnapshotLabel ->
  m (Table m k v b)
openTableFromSnapshot session snapName snapLabel =
  openTableFromSnapshotWith NoOverrideDiskCachePolicy session snapName snapLabel

{- |
Variant of 'openTableFromSnapshot' that accepts [table configuration overrides](#g:table_configuration_overrides).
-}
{-# SPECIALISE
  openTableFromSnapshotWith ::
    forall k v b.
    (ResolveValue v) =>
    OverrideDiskCachePolicy ->
    Session IO ->
    SnapshotName ->
    SnapshotLabel ->
    IO (Table IO k v b) #-}
openTableFromSnapshotWith ::
  forall m k v b.
  (IOLike m) =>
  (ResolveValue v) =>
  OverrideDiskCachePolicy ->
  Session m ->
  SnapshotName ->
  SnapshotLabel ->
  m (Table m k v b)
openTableFromSnapshotWith tableConfigOverride (Session session) snapName snapLabel =
  Table <$> Internal.openSnapshot session tableConfigOverride snapLabel Internal.SnapFullTable snapName (_getResolveSerialisedValue (Proxy @v))

{- |
Delete the named snapshot.

The worst-case disk I\/O complexity of this operation is \(O(\log_T n)\).

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
    IO () #-}
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

The worst-case disk I\/O complexity of this operation is \(O(1)\).

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
-}
{-# SPECIALISE
  doesSnapshotExist ::
    Session IO ->
    SnapshotName ->
    IO Bool #-}
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

The worst-case disk I\/O complexity of this operation is \(O(s)\),
where \(s\) refers to the number of snapshots in the session.

Throws the following exceptions:

['SessionClosedError']:
    If the session is closed.
-}
{-# SPECIALISE
  listSnapshots ::
    Session IO ->
    IO [SnapshotName] #-}
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
