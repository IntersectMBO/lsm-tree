{-# LANGUAGE DataKinds #-}

-- | This module brings together the internal parts to provide an API in terms
-- of untyped serialised keys, values and blobs. It makes no distinction between
-- normal and monoidal tables, accepting both blobs and mupserts.
-- The typed [normal]("Database.LSMTree.Normal") and
-- [monoidal]("Database.LSMTree.Monoidal") APIs then provide more type-safe
-- wrappers and handle serialisation.
--
-- Apart from defining the API, this module mainly deals with concurrency- and
-- exception-safe opening and closing of resources. Any other non-trivial logic
-- should live somewhere else.
--
module Database.LSMTree.Internal (
    -- * Existentials
    Session' (..)
  , Table' (..)
  , Cursor' (..)
  , NormalTable (..)
  , NormalCursor (..)
  , MonoidalTable (..)
  , MonoidalCursor (..)
    -- * Exceptions
  , LSMTreeError (..)
    -- * Tracing
  , LSMTreeTrace (..)
  , TableTrace (..)
    -- * Session
  , Session (..)
  , SessionState (..)
  , SessionEnv (..)
  , withOpenSession
    -- ** Implementation of public API
  , withSession
  , openSession
  , closeSession
    -- * Table
  , Table (..)
  , TableState (..)
  , TableEnv (..)
  , withOpenTable
    -- ** Implementation of public API
  , ResolveSerialisedValue
  , withTable
  , new
  , close
  , lookups
  , rangeLookup
  , updates
  , retrieveBlobs
    -- ** Cursor API
  , Cursor (..)
  , CursorState (..)
  , CursorEnv (..)
  , OffsetKey (..)
  , withCursor
  , newCursor
  , closeCursor
  , readCursor
  , readCursorWhile
    -- * Snapshots
  , SnapshotLabel
  , createSnapshot
  , openSnapshot
  , deleteSnapshot
  , listSnapshots
    -- * Mutiple writable tables
  , duplicate
    -- * Table union
  , unions
  , UnionDebt (..)
  , remainingUnionDebt
  , UnionCredits (..)
  , supplyUnionCredits
  ) where

import           Codec.CBOR.Read
import           Control.ActionRegistry
import           Control.Concurrent.Class.MonadMVar.Strict
import           Control.Concurrent.Class.MonadSTM (MonadSTM (..))
import           Control.Concurrent.Class.MonadSTM.RWVar (RWVar)
import qualified Control.Concurrent.Class.MonadSTM.RWVar as RW
import           Control.DeepSeq
import           Control.Monad (forM, unless, void)
import           Control.Monad.Class.MonadAsync as Async
import           Control.Monad.Class.MonadST (MonadST (..))
import           Control.Monad.Class.MonadThrow
import           Control.Monad.Primitive
import           Control.RefCount
import           Control.Tracer
import           Data.Arena (ArenaManager, newArenaManager)
import           Data.Either (fromRight)
import           Data.Foldable
import           Data.Kind
import           Data.List.NonEmpty (NonEmpty (..))
import qualified Data.List.NonEmpty as NE
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import           Data.Maybe (catMaybes, maybeToList)
import qualified Data.Set as Set
import           Data.Typeable
import qualified Data.Vector as V
import           Database.LSMTree.Internal.BlobRef (WeakBlobRef (..))
import qualified Database.LSMTree.Internal.BlobRef as BlobRef
import           Database.LSMTree.Internal.Config
import qualified Database.LSMTree.Internal.Cursor as Cursor
import           Database.LSMTree.Internal.Entry (Entry)
import           Database.LSMTree.Internal.Lookup (ByteCountDiscrepancy,
                     ResolveSerialisedValue, lookupsIO,
                     lookupsIOWithoutWriteBuffer)
import           Database.LSMTree.Internal.MergeSchedule
import qualified Database.LSMTree.Internal.MergingRun as MR
import           Database.LSMTree.Internal.MergingTree
import qualified Database.LSMTree.Internal.MergingTree.Lookup as MT
import           Database.LSMTree.Internal.Paths (SessionRoot (..),
                     SnapshotMetaDataChecksumFile (..),
                     SnapshotMetaDataFile (..), SnapshotName)
import qualified Database.LSMTree.Internal.Paths as Paths
import           Database.LSMTree.Internal.Range (Range (..))
import           Database.LSMTree.Internal.Run (Run)
import qualified Database.LSMTree.Internal.Run as Run
import           Database.LSMTree.Internal.RunNumber
import           Database.LSMTree.Internal.RunReaders (OffsetKey (..))
import qualified Database.LSMTree.Internal.RunReaders as Readers
import           Database.LSMTree.Internal.Serialise (SerialisedBlob (..),
                     SerialisedKey, SerialisedValue)
import           Database.LSMTree.Internal.Snapshot
import           Database.LSMTree.Internal.Snapshot.Codec
import           Database.LSMTree.Internal.UniqCounter
import qualified Database.LSMTree.Internal.Vector as V
import qualified Database.LSMTree.Internal.WriteBuffer as WB
import qualified Database.LSMTree.Internal.WriteBufferBlobs as WBB
import qualified System.FS.API as FS
import           System.FS.API (FsError, FsErrorPath (..), FsPath, HasFS)
import qualified System.FS.BlockIO.API as FS
import           System.FS.BlockIO.API (HasBlockIO)

{-------------------------------------------------------------------------------
  Existentials
-------------------------------------------------------------------------------}

type Session' :: (Type -> Type) -> Type
data Session' m = forall h. Typeable h => Session' !(Session m h)

instance NFData (Session' m) where
  rnf (Session' s) = rnf s

type Table' :: (Type -> Type) -> Type -> Type -> Type -> Type
data Table' m k v b = forall h. Typeable h => Table' (Table m h)

instance NFData (Table' m k v b) where
  rnf (Table' t) = rnf t

type Cursor' :: (Type -> Type) -> Type -> Type -> Type -> Type
data Cursor' m k v b = forall h. Typeable h => Cursor' (Cursor m h)

instance NFData (Cursor' m k v b) where
  rnf (Cursor' t) = rnf t

type NormalTable :: (Type -> Type) -> Type -> Type -> Type -> Type
data NormalTable m k v b = forall h. Typeable h =>
    NormalTable !(Table m h)

instance NFData (NormalTable m k v b) where
  rnf (NormalTable t) = rnf t

type NormalCursor :: (Type -> Type) -> Type -> Type -> Type -> Type
data NormalCursor m k v b = forall h. Typeable h =>
    NormalCursor !(Cursor m h)

instance NFData (NormalCursor m k v b) where
  rnf (NormalCursor c) = rnf c

type MonoidalTable :: (Type -> Type) -> Type -> Type -> Type
data MonoidalTable m k v = forall h. Typeable h =>
    MonoidalTable !(Table m h)

instance NFData (MonoidalTable m k v) where
  rnf (MonoidalTable t) = rnf t

type MonoidalCursor :: (Type -> Type) -> Type -> Type -> Type
data MonoidalCursor m k v = forall h. Typeable h =>
    MonoidalCursor !(Cursor m h)

instance NFData (MonoidalCursor m k v) where
  rnf (MonoidalCursor c) = rnf c

{-------------------------------------------------------------------------------
  Exceptions
-------------------------------------------------------------------------------}

-- TODO: give this a nicer Show instance.
data LSMTreeError =
    SessionDirDoesNotExist FsErrorPath
    -- | The session directory is already locked
  | SessionDirLocked FsErrorPath
    -- | The session directory is malformed: the layout of the session directory
    -- contains unexpected files and/or directories.
  | SessionDirMalformed FsErrorPath
    -- | All operations on a closed session as well as tables or cursors within
    -- a closed session will throw this exception. There is one exception (pun
    -- intended) to this rule: the idempotent operation
    -- 'Database.LSMTree.Common.closeSession'.
  | ErrSessionClosed
    -- | All operations on a closed table will throw this exception, except for
    -- the idempotent operation 'Database.LSMTree.Common.close'.
  | ErrTableClosed
    -- | All operations on a closed cursor will throw this exception, except for
    -- the idempotent operation 'Database.LSMTree.Common.closeCursor'.
  | ErrCursorClosed
  | ErrSnapshotExists SnapshotName
  | ErrSnapshotNotExists SnapshotName
  | ErrSnapshotDeserialiseFailure DeserialiseFailure SnapshotName
  | ErrSnapshotWrongTableType
      SnapshotName
      SnapshotTableType -- ^ Expected type
      SnapshotTableType -- ^ Actual type
  | ErrSnapshotWrongLabel
      SnapshotName
      SnapshotLabel -- ^ Expected label
      SnapshotLabel -- ^ Actual label
    -- | Something went wrong during batch lookups.
  | ErrLookup ByteCountDiscrepancy
    -- | A 'BlobRef' used with 'retrieveBlobs' was invalid.
    --
    -- 'BlobRef's are obtained from lookups in a 'Table', but they may be
    -- invalidated by subsequent changes in that 'Table'. In general the
    -- reliable way to retrieve blobs is not to change the 'Table' before
    -- retrieving the blobs. To allow later retrievals, duplicate the table
    -- before making modifications and keep the table open until all blob
    -- retrievals are complete.
    --
    -- The 'Int' index indicates which 'BlobRef' was invalid. Many may be
    -- invalid but only the first is reported.
  | ErrBlobRefInvalid Int
    -- | 'unions' was called on tables that are not of the same type.
  | ErrUnionsTableTypeMismatch
      Int -- ^ Vector index of table @t1@ involved in the mismatch
      Int -- ^ Vector index of table @t2@ involved in the mismatch
    -- | 'unions' was called on tables that are not in the same session.
  | ErrUnionsSessionMismatch
      Int -- ^ Vector index of table @t1@ involved in the mismatch
      Int -- ^ Vector index of table @t2@ involved in the mismatch
    -- | 'unions' was called on tables that do not have the same configuration.
  deriving stock (Show, Eq)
  deriving anyclass (Exception)

{-------------------------------------------------------------------------------
  Traces
-------------------------------------------------------------------------------}

data LSMTreeTrace =
    -- Session
    TraceOpenSession FsPath
  | TraceNewSession
  | TraceRestoreSession
  | TraceCloseSession
    -- Table
  | TraceNewTable
  | TraceOpenSnapshot SnapshotName TableConfigOverride
  | TraceTable TableId TableTrace
  | TraceDeleteSnapshot SnapshotName
  | TraceListSnapshots
    -- Cursor
  | TraceCursor CursorId CursorTrace
    -- Unions
  | TraceUnions (NonEmpty TableId)
  deriving stock Show

data TableTrace =
    -- | A table is created with the specified config.
    --
    -- This message is traced in addition to messages like 'TraceNewTable' and
    -- 'TraceDuplicate'.
    TraceCreateTable TableConfig
  | TraceCloseTable
    -- Lookups
  | TraceLookups Int
  | TraceRangeLookup (Range SerialisedKey)
    -- Updates
  | TraceUpdates Int
  | TraceMerge (AtLevel MergeTrace)
    -- Snapshot
  | TraceSnapshot SnapshotName
    -- Duplicate
  | TraceDuplicate
    -- Unions
  | TraceRemainingUnionDebt
  | TraceSupplyUnionCredits UnionCredits
  deriving stock Show

data CursorTrace =
    TraceCreateCursor TableId
  | TraceCloseCursor
  | TraceReadCursor Int
  deriving stock Show

{-------------------------------------------------------------------------------
  Session
-------------------------------------------------------------------------------}

-- | A session provides context that is shared across multiple tables.
--
-- For more information, see 'Database.LSMTree.Common.Session'.
data Session m h = Session {
      -- | The primary purpose of this 'RWVar' is to ensure consistent views of
      -- the open-/closedness of a session when multiple threads require access
      -- to the session's fields (see 'withOpenSession'). We use more
      -- fine-grained synchronisation for various mutable parts of an open
      -- session.
      --
      -- INVARIANT: once the session state is changed from 'SessionOpen' to
      -- 'SessionClosed', it is never changed back to 'SessionOpen' again.
      sessionState  :: !(RWVar m (SessionState m h))
    , sessionTracer :: !(Tracer m LSMTreeTrace)
    }

instance NFData (Session m h) where
  rnf (Session a b) = rnf a `seq` rwhnf b

data SessionState m h =
    SessionOpen !(SessionEnv m h)
  | SessionClosed

data SessionEnv m h = SessionEnv {
    -- | The path to the directory in which this sesion is live. This is a path
    -- relative to root of the 'HasFS' instance.
    --
    -- INVARIANT: the session root is never changed during the lifetime of a
    -- session.
    sessionRoot        :: !SessionRoot
  , sessionHasFS       :: !(HasFS m h)
  , sessionHasBlockIO  :: !(HasBlockIO m h)
  , sessionLockFile    :: !(FS.LockFileHandle m)
    -- | A session-wide shared, atomic counter that is used to produce unique
    -- names, for example: run names, table IDs.
  , sessionUniqCounter :: !(UniqCounter m)
    -- | Open tables are tracked here so they can be closed once the session is
    -- closed. Tables also become untracked when they are closed manually.
    --
    -- Tables are assigned unique identifiers using 'sessionUniqCounter' to
    -- ensure that modifications to the set of known tables are independent.
    -- Each identifier is added only once in 'new', 'openSnapshot', 'duplicate',
    -- 'union', or 'unions', and is deleted only once in 'close' or
    -- 'closeSession'.
    --
    -- * A new table may only insert its own identifier when it has acquired the
    --   'sessionState' read-lock. This is to prevent races with 'closeSession'.
    --
    -- * A table 'close' may delete its own identifier from the set of open
    --   tables without restrictions, even concurrently with 'closeSession'.
    --   This is safe because 'close' is idempotent'.
  , sessionOpenTables  :: !(StrictMVar m (Map TableId (Table m h)))
    -- | Similarly to tables, open cursors are tracked so they can be closed
    -- once the session is closed. See 'sessionOpenTables'.
  , sessionOpenCursors :: !(StrictMVar m (Map CursorId (Cursor m h)))
  }

{-# INLINE withOpenSession #-}
{-# SPECIALISE withOpenSession ::
     Session IO h
  -> (SessionEnv IO h -> IO a)
  -> IO a #-}
-- | 'withOpenSession' ensures that the session stays open for the duration of the
-- provided continuation.
--
-- NOTE: any operation except 'sessionClose' can use this function.
withOpenSession ::
     (MonadSTM m, MonadThrow m)
  => Session m h
  -> (SessionEnv m h -> m a)
  -> m a
withOpenSession sesh action = RW.withReadAccess (sessionState sesh) $ \case
    SessionClosed -> throwIO ErrSessionClosed
    SessionOpen seshEnv -> action seshEnv

--
-- Implementation of public API
--

{-# SPECIALISE withSession ::
     Tracer IO LSMTreeTrace
  -> HasFS IO h
  -> HasBlockIO IO h
  -> FsPath
  -> (Session IO h -> IO a)
  -> IO a #-}
-- | See 'Database.LSMTree.Common.withSession'.
withSession ::
     (MonadMask m, MonadSTM m, MonadMVar m, PrimMonad m)
  => Tracer m LSMTreeTrace
  -> HasFS m h
  -> HasBlockIO m h
  -> FsPath
  -> (Session m h -> m a)
  -> m a
withSession tr hfs hbio dir = bracket (openSession tr hfs hbio dir) closeSession

{-# SPECIALISE openSession ::
     Tracer IO LSMTreeTrace
  -> HasFS IO h
  -> HasBlockIO IO h
  -> FsPath
  -> IO (Session IO h) #-}
-- | See 'Database.LSMTree.Common.openSession'.
openSession ::
     forall m h.
     (MonadSTM m, MonadMVar m, PrimMonad m, MonadMask m)
  => Tracer m LSMTreeTrace
  -> HasFS m h
  -> HasBlockIO m h -- TODO: could we prevent the user from having to pass this in?
  -> FsPath -- ^ Path to the session directory
  -> m (Session m h)
openSession tr hfs hbio dir =
    -- We can not use modifyWithActionRegistry here, since there is no in-memory
    -- state to modify. We use withActionRegistry instead, which may have a tiny
    -- chance of leaking resources if openSession is not called in a masked
    -- state.
    withActionRegistry $ \reg -> do
      traceWith tr (TraceOpenSession dir)
      dirExists <- FS.doesDirectoryExist hfs dir
      unless dirExists $
        throwIO (SessionDirDoesNotExist (FS.mkFsErrorPath hfs dir))
      -- List directory contents /before/ trying to acquire a file lock, so that
      -- that the lock file does not show up in the listed contents.
      dirContents <- FS.listDirectory hfs dir
      -- Try to acquire the session file lock as soon as possible to reduce the
      -- risk of race conditions.
      --
      -- The lock is only released when an exception is raised, otherwise the lock
      -- is included in the returned Session.
      elock <-
        withRollbackFun reg
          (fromRight Nothing)
          acquireLock
          releaseLock

      case elock of
        Left e
          | FS.FsResourceAlreadyInUse <- FS.fsErrorType e
          , fsep@(FsErrorPath _ fsp) <- FS.fsErrorPath e
          , fsp == lockFilePath
          -> throwIO (SessionDirLocked fsep)
        Left  e -> throwIO e -- rethrow unexpected errors
        Right Nothing -> throwIO (SessionDirLocked (FS.mkFsErrorPath hfs lockFilePath))
        Right (Just sessionFileLock) ->
          if Set.null dirContents then newSession reg sessionFileLock
                                  else restoreSession reg sessionFileLock
  where
    root             = Paths.SessionRoot dir
    lockFilePath     = Paths.lockFile root
    activeDirPath    = Paths.getActiveDir (Paths.activeDir root)
    snapshotsDirPath = Paths.snapshotsDir root

    acquireLock = try @m @FsError $ FS.tryLockFile hbio lockFilePath FS.ExclusiveLock

    releaseLock = FS.hUnlock

    mkSession lockFile = do
        counterVar <- newUniqCounter 0
        openTablesVar <- newMVar Map.empty
        openCursorsVar <- newMVar Map.empty
        sessionVar <- RW.new $ SessionOpen $ SessionEnv {
            sessionRoot = root
          , sessionHasFS = hfs
          , sessionHasBlockIO = hbio
          , sessionLockFile = lockFile
          , sessionUniqCounter = counterVar
          , sessionOpenTables = openTablesVar
          , sessionOpenCursors = openCursorsVar
          }
        pure $! Session sessionVar tr

    newSession reg sessionFileLock = do
        traceWith tr TraceNewSession
        withRollback_ reg
          (FS.createDirectory hfs activeDirPath)
          (FS.removeDirectoryRecursive hfs activeDirPath)
        withRollback_ reg
          (FS.createDirectory hfs snapshotsDirPath)
          (FS.removeDirectoryRecursive hfs snapshotsDirPath)
        mkSession sessionFileLock

    restoreSession _reg sessionFileLock = do
        traceWith tr TraceRestoreSession
        -- If the layouts are wrong, we throw an exception
        checkTopLevelDirLayout

        -- Clear the active directory by removing the directory and recreating
        -- it again.
        FS.removeDirectoryRecursive hfs activeDirPath
          `finally` FS.createDirectoryIfMissing hfs False activeDirPath

        checkActiveDirLayout
        checkSnapshotsDirLayout
        mkSession sessionFileLock

    -- Check that the active directory and snapshots directory exist. We assume
    -- the lock file already exists at this point.
    --
    -- This checks only that the /expected/ files and directories exist.
    -- Unexpected files in the top-level directory are ignored for the layout
    -- check.
    checkTopLevelDirLayout = do
      FS.doesDirectoryExist hfs activeDirPath >>= \b ->
        unless b $ throwIO (SessionDirMalformed (FS.mkFsErrorPath hfs activeDirPath))
      FS.doesDirectoryExist hfs snapshotsDirPath >>= \b ->
        unless b $ throwIO (SessionDirMalformed (FS.mkFsErrorPath hfs snapshotsDirPath))

    -- The active directory should be empty
    checkActiveDirLayout = do
        contents <- FS.listDirectory hfs activeDirPath
        unless (Set.null contents) $ throwIO (SessionDirMalformed (FS.mkFsErrorPath hfs activeDirPath))

    -- Nothing to check: snapshots are verified when they are loaded, not when a
    -- session is restored.
    checkSnapshotsDirLayout = pure ()

{-# SPECIALISE closeSession :: Session IO h -> IO () #-}
-- | See 'Database.LSMTree.Common.closeSession'.
--
-- A session's global resources will only be released once it is sure that no
-- tables or cursors are open anymore.
closeSession ::
     (MonadMask m, MonadSTM m, MonadMVar m, PrimMonad m)
  => Session m h
  -> m ()
closeSession Session{sessionState, sessionTracer} = do
    traceWith sessionTracer TraceCloseSession
    modifyWithActionRegistry_
      (RW.unsafeAcquireWriteAccess sessionState)
      (atomically . RW.unsafeReleaseWriteAccess sessionState)
      $ \reg -> \case
        SessionClosed -> pure SessionClosed
        SessionOpen seshEnv -> do
          -- Close tables and cursors first, so that we know none are open when we
          -- release session-wide resources.
          --
          -- If any tables or cursors have been closed already by a different
          -- thread, then the idempotent close functions will act like a no-op,
          -- and so we are not in trouble.
          --
          -- Since we have a write lock on the session state, we know that no
          -- tables or cursors will be added while we are closing the session
          -- (see sessionOpenTables), and that we are the only thread currently
          -- closing the session. .
          --
          -- We technically don't have to overwrite this with an empty Map, but
          -- why not.

          -- close cursors
          cursors <-
            withRollback reg
              (swapMVar (sessionOpenCursors seshEnv) Map.empty)
              (void . swapMVar (sessionOpenCursors seshEnv))
          mapM_ (delayedCommit reg . closeCursor) cursors

          -- close tables
          tables <-
            withRollback reg
              (swapMVar (sessionOpenTables seshEnv) Map.empty)
              (void . swapMVar (sessionOpenTables seshEnv))
          mapM_ (delayedCommit reg . close) tables

          delayedCommit reg $ FS.close (sessionHasBlockIO seshEnv)
          delayedCommit reg $ FS.hUnlock (sessionLockFile seshEnv)

          pure SessionClosed

{-------------------------------------------------------------------------------
  Table
-------------------------------------------------------------------------------}

-- | A handle to an on-disk key\/value table.
--
-- For more information, see 'Database.LSMTree.Normal.Table'.
data Table m h = Table {
      tableConfig       :: !TableConfig
      -- | The primary purpose of this 'RWVar' is to ensure consistent views of
      -- the open-/closedness of a table when multiple threads require access to
      -- the table's fields (see 'withOpenTable'). We use more fine-grained
      -- synchronisation for various mutable parts of an open table.
    , tableState        :: !(RWVar m (TableState m h))
    , tableArenaManager :: !(ArenaManager (PrimState m))
    , tableTracer       :: !(Tracer m TableTrace)
      -- | Session-unique identifier for this table.
      --
      -- INVARIANT: a table's identifier is never changed during the lifetime of
      -- the table.
    , tableId           :: !TableId

      -- === Session-inherited

      -- | The session that this table belongs to.
      --
      -- INVARIANT: a table only ever belongs to one session, and can't be
      -- transferred to a different session.
    , tableSession      :: !(Session m h)
    }

instance NFData (Table m h) where
  rnf (Table a b c d e f) =
    rnf a `seq` rnf b `seq` rnf c `seq` rwhnf d `seq` rnf e`seq` rwhnf f

-- | A table may assume that its corresponding session is still open as
-- long as the table is open. A session's global resources, and therefore
-- resources that are inherited by the table, will only be released once the
-- session is sure that no tables are open anymore.
data TableState m h =
    TableOpen !(TableEnv m h)
  | TableClosed

data TableEnv m h = TableEnv {
    -- === Session-inherited

    -- | Use this instead of 'tableSession' for easy access. An open table may
    -- assume that its session is open.
    tableSessionEnv :: !(SessionEnv m h)

    -- === Table-specific

    -- | All of the state being in a single 'StrictMVar' is a relatively simple
    -- solution, but there could be more concurrency. For example, while inserts
    -- are in progress, lookups could still look at the old state without
    -- waiting for the MVar.
    --
    -- TODO: switch to more fine-grained synchronisation approach
  , tableContent    :: !(RWVar m (TableContent m h))
  }

{-# INLINE tableSessionRoot #-}
 -- | Inherited from session for ease of access.
tableSessionRoot :: TableEnv m h -> SessionRoot
tableSessionRoot = sessionRoot . tableSessionEnv

{-# INLINE tableHasFS #-}
-- | Inherited from session for ease of access.
tableHasFS :: TableEnv m h -> HasFS m h
tableHasFS = sessionHasFS . tableSessionEnv

{-# INLINE tableHasBlockIO #-}
-- | Inherited from session for ease of access.
tableHasBlockIO :: TableEnv m h -> HasBlockIO m h
tableHasBlockIO = sessionHasBlockIO . tableSessionEnv

{-# INLINE tableSessionUniqCounter #-}
-- | Inherited from session for ease of access.
tableSessionUniqCounter :: TableEnv m h -> UniqCounter m
tableSessionUniqCounter = sessionUniqCounter . tableSessionEnv

{-# INLINE tableSessionUntrackTable #-}
{-# SPECIALISE tableSessionUntrackTable :: TableId -> TableEnv IO h -> IO () #-}
-- | Open tables are tracked in the corresponding session, so when a table is
-- closed it should become untracked (forgotten).
tableSessionUntrackTable :: MonadMVar m => TableId -> TableEnv m h -> m ()
tableSessionUntrackTable tableId tEnv =
    modifyMVar_ (sessionOpenTables (tableSessionEnv tEnv)) $ pure . Map.delete tableId

-- | 'withOpenTable' ensures that the table stays open for the duration of the
-- provided continuation.
--
-- NOTE: any operation except 'close' can use this function.
{-# INLINE withOpenTable #-}
{-# SPECIALISE withOpenTable ::
     Table IO h
  -> (TableEnv IO h -> IO a)
  -> IO a #-}
withOpenTable ::
     (MonadSTM m, MonadThrow m)
  => Table m h
  -> (TableEnv m h -> m a)
  -> m a
withOpenTable t action = RW.withReadAccess (tableState t) $ \case
    TableClosed -> throwIO ErrTableClosed
    TableOpen tEnv -> action tEnv

--
-- Implementation of public API
--

{-# SPECIALISE withTable ::
     Session IO h
  -> TableConfig
  -> (Table IO h -> IO a)
  -> IO a #-}
-- | See 'Database.LSMTree.Normal.withTable'.
withTable ::
     (MonadMask m, MonadSTM m, MonadMVar m, PrimMonad m)
  => Session m h
  -> TableConfig
  -> (Table m h -> m a)
  -> m a
withTable sesh conf = bracket (new sesh conf) close

{-# SPECIALISE new ::
     Session IO h
  -> TableConfig
  -> IO (Table IO h) #-}
-- | See 'Database.LSMTree.Normal.new'.
new ::
     (MonadSTM m, MonadMVar m, PrimMonad m, MonadMask m)
  => Session m h
  -> TableConfig
  -> m (Table m h)
new sesh conf = do
    traceWith (sessionTracer sesh) TraceNewTable
    withOpenSession sesh $ \seshEnv ->
      withActionRegistry $ \reg -> do
        am <- newArenaManager
        tc <- newEmptyTableContent seshEnv reg
        newWith reg sesh seshEnv conf am tc

{-# SPECIALISE newEmptyTableContent ::
     SessionEnv IO h
  -> ActionRegistry IO
  -> IO (TableContent IO h) #-}
newEmptyTableContent ::
     (PrimMonad m, MonadMask m, MonadMVar m)
  => SessionEnv m h
  -> ActionRegistry m
  -> m (TableContent m h)
newEmptyTableContent seshEnv reg = do
    blobpath <- Paths.tableBlobPath (sessionRoot seshEnv) <$>
                  incrUniqCounter (sessionUniqCounter seshEnv)
    let tableWriteBuffer = WB.empty
    tableWriteBufferBlobs
      <- withRollback reg
           (WBB.new (sessionHasFS seshEnv) blobpath)
           releaseRef
    let tableLevels = V.empty
    tableCache <- mkLevelsCache reg tableLevels
    pure TableContent {
      tableWriteBuffer
    , tableWriteBufferBlobs
    , tableLevels
    , tableCache
    , tableUnionLevel = NoUnion
    }


{-# SPECIALISE newWith ::
     ActionRegistry IO
  -> Session IO h
  -> SessionEnv IO h
  -> TableConfig
  -> ArenaManager RealWorld
  -> TableContent IO h
  -> IO (Table IO h) #-}
newWith ::
     (MonadSTM m, MonadMVar m, PrimMonad m)
  => ActionRegistry m
  -> Session m h
  -> SessionEnv m h
  -> TableConfig
  -> ArenaManager (PrimState m)
  -> TableContent m h
  -> m (Table m h)
newWith reg sesh seshEnv conf !am !tc = do
    tableId <- uniqueToTableId <$> incrUniqCounter (sessionUniqCounter seshEnv)
    let tr = TraceTable tableId `contramap` sessionTracer sesh
    traceWith tr $ TraceCreateTable conf
    -- The session is kept open until we've updated the session's set of tracked
    -- tables. If 'closeSession' is called by another thread while this code
    -- block is being executed, that thread will block until it reads the
    -- /updated/ set of tracked tables.
    contentVar <- RW.new $ tc
    tableVar <- RW.new $ TableOpen $ TableEnv {
          tableSessionEnv = seshEnv
        , tableContent = contentVar
        }
    let !t = Table conf tableVar am tr tableId sesh
    -- Track the current table
    delayedCommit reg $
      modifyMVar_ (sessionOpenTables seshEnv) $
        pure . Map.insert tableId t
    pure $! t

{-# SPECIALISE close :: Table IO h -> IO () #-}
-- | See 'Database.LSMTree.Normal.close'.
close ::
     (MonadMask m, MonadSTM m, MonadMVar m, PrimMonad m)
  => Table m h
  -> m ()
close t = do
    traceWith (tableTracer t) TraceCloseTable
    modifyWithActionRegistry_
      (RW.unsafeAcquireWriteAccess (tableState t))
      (atomically . RW.unsafeReleaseWriteAccess (tableState t)) $ \reg -> \case
      TableClosed -> pure TableClosed
      TableOpen tEnv -> do
        -- Since we have a write lock on the table state, we know that we are the
        -- only thread currently closing the table. We can safely make the session
        -- forget about this table.
        delayedCommit reg (tableSessionUntrackTable (tableId t) tEnv)
        RW.withWriteAccess_ (tableContent tEnv) $ \tc -> do
          releaseTableContent reg tc
          pure tc
        pure TableClosed

{-# SPECIALISE lookups ::
     ResolveSerialisedValue
  -> V.Vector SerialisedKey
  -> Table IO h
  -> IO (V.Vector (Maybe (Entry SerialisedValue (WeakBlobRef IO h)))) #-}
-- | See 'Database.LSMTree.Normal.lookups'.
lookups ::
     (MonadAsync m, MonadMask m, MonadMVar m, MonadST m)
  => ResolveSerialisedValue
  -> V.Vector SerialisedKey
  -> Table m h
  -> m (V.Vector (Maybe (Entry SerialisedValue (WeakBlobRef m h))))
lookups resolve ks t = do
    traceWith (tableTracer t) $ TraceLookups (V.length ks)
    withOpenTable t $ \tEnv ->
      RW.withReadAccess (tableContent tEnv) $ \tableContent -> do
        case tableUnionLevel tableContent of
          NoUnion -> regularLevelLookups tEnv tableContent
          Union tree -> do
            isStructurallyEmpty tree >>= \case
              True  -> regularLevelLookups tEnv tableContent
              False ->
                -- TODO: the blob refs returned from the tree can be invalidated
                -- by supplyUnionCredits or other operations on any table that
                -- shares merging runs or trees. We need to keep open the runs!
                -- This could be achieved by storing the LookupTree and only
                -- calling MT.releaseLookupTree later, when we are okay with
                -- invalidating the blob refs (similar to the LevelsCache).
                -- Lookups then use the cached tree, but when should we rebuild
                -- the tree? On each call to supplyUnionCredits?
                withActionRegistry $ \reg -> do
                  regularResult <-
                    -- asynchronously, so tree lookup batches can already be
                    -- submitted without waiting for the result.
                    Async.async $ regularLevelLookups tEnv tableContent
                  treeBatches <- MT.buildLookupTree reg tree
                  treeResults <- forM treeBatches $ \runs ->
                    Async.async $ treeBatchLookups tEnv runs
                  -- TODO: if regular levels are empty, don't add them to tree
                  res <- MT.foldLookupTree resolve $
                    MT.mkLookupNode MR.MergeLevel $ V.fromList
                      [ MT.LookupBatch regularResult
                      , treeResults
                      ]
                  MT.releaseLookupTree reg treeBatches
                  return res
  where
    regularLevelLookups tEnv tableContent = do
        let !cache = tableCache tableContent
        lookupsIO
          (tableHasBlockIO tEnv)
          (tableArenaManager t)
          resolve
          (tableWriteBuffer tableContent)
          (tableWriteBufferBlobs tableContent)
          (cachedRuns cache)
          (cachedFilters cache)
          (cachedIndexes cache)
          (cachedKOpsFiles cache)
          ks

    treeBatchLookups tEnv runs =
        lookupsIOWithoutWriteBuffer
          (tableHasBlockIO tEnv)
          (tableArenaManager t)
          resolve
          runs
          (V.mapStrict (\(DeRef r) -> Run.runFilter   r) runs)
          (V.mapStrict (\(DeRef r) -> Run.runIndex    r) runs)
          (V.mapStrict (\(DeRef r) -> Run.runKOpsFile r) runs)
          ks

{-# SPECIALISE rangeLookup ::
     ResolveSerialisedValue
  -> Range SerialisedKey
  -> Table IO h
  -> (SerialisedKey -> SerialisedValue -> Maybe (WeakBlobRef IO h) -> res)
  -> IO (V.Vector res) #-}
-- | See 'Database.LSMTree.Normal.rangeLookup'.
rangeLookup ::
     (MonadMask m, MonadMVar m, MonadST m, MonadSTM m)
  => ResolveSerialisedValue
  -> Range SerialisedKey
  -> Table m h
  -> (SerialisedKey -> SerialisedValue -> Maybe (WeakBlobRef m h) -> res)
     -- ^ How to map to a query result, different for normal/monoidal
  -> m (V.Vector res)
rangeLookup resolve range t fromEntry = do
    traceWith (tableTracer t) $ TraceRangeLookup range
    case range of
      FromToExcluding lb ub ->
        withCursor (OffsetKey lb) t $ \cursor ->
          go cursor (< ub) []
      FromToIncluding lb ub ->
        withCursor (OffsetKey lb) t $ \cursor ->
          go cursor (<= ub) []
  where
    -- TODO: tune!
    -- Also, such a high number means that many tests never cover the case
    -- of having multiple chunks. Expose through the public API as config?
    chunkSize = 500

    go cursor isInUpperBound !chunks = do
      chunk <- readCursorWhile resolve isInUpperBound chunkSize cursor fromEntry
      let !n = V.length chunk
      if n >= chunkSize
        then go cursor isInUpperBound (chunk : chunks)
             -- This requires an extra copy. If we had a size hint, we could
             -- directly write everything into the result vector.
             -- TODO(optimise): revisit
        else return (V.concat (reverse (V.slice 0 n chunk : chunks)))

{-# SPECIALISE updates ::
     ResolveSerialisedValue
  -> V.Vector (SerialisedKey, Entry SerialisedValue SerialisedBlob)
  -> Table IO h
  -> IO () #-}
-- | See 'Database.LSMTree.Normal.updates'.
--
-- Does not enforce that mupsert and blobs should not occur in the same table.
updates ::
     (MonadMask m, MonadMVar m, MonadST m, MonadSTM m)
  => ResolveSerialisedValue
  -> V.Vector (SerialisedKey, Entry SerialisedValue SerialisedBlob)
  -> Table m h
  -> m ()
updates resolve es t = do
    traceWith (tableTracer t) $ TraceUpdates (V.length es)
    let conf = tableConfig t
    withOpenTable t $ \tEnv -> do
      let hfs = tableHasFS tEnv
      modifyWithActionRegistry_
        (RW.unsafeAcquireWriteAccess (tableContent tEnv))
        (atomically . RW.unsafeReleaseWriteAccess (tableContent tEnv)) $ \reg -> do
          updatesWithInterleavedFlushes
            (TraceMerge `contramap` tableTracer t)
            conf
            resolve
            hfs
            (tableHasBlockIO tEnv)
            (tableSessionRoot tEnv)
            (tableSessionUniqCounter tEnv)
            es
            reg

{-------------------------------------------------------------------------------
  Blobs
-------------------------------------------------------------------------------}

{-# SPECIALISE retrieveBlobs ::
     Session IO h
  -> V.Vector (WeakBlobRef IO h)
  -> IO (V.Vector SerialisedBlob) #-}
retrieveBlobs ::
     (MonadMask m, MonadST m, MonadSTM m)
  => Session m h
  -> V.Vector (WeakBlobRef m h)
  -> m (V.Vector SerialisedBlob)
retrieveBlobs sesh wrefs =
    withOpenSession sesh $ \seshEnv ->
      let hbio = sessionHasBlockIO seshEnv in
      handle (\(BlobRef.WeakBlobRefInvalid i) ->
                throwIO (ErrBlobRefInvalid i)) $
      BlobRef.readWeakBlobRefs hbio wrefs

{-------------------------------------------------------------------------------
  Cursors
-------------------------------------------------------------------------------}

-- | A read-only view into the table state at the time of cursor creation.
--
-- For more information, see 'Database.LSMTree.Normal.Cursor'.
--
-- The representation of a cursor is similar to that of a 'Table', but
-- simpler, as it is read-only.
data Cursor m h = Cursor {
      -- | Mutual exclusion, only a single thread can read from a cursor at a
      -- given time.
      cursorState  :: !(StrictMVar m (CursorState m h))
    , cursorTracer :: !(Tracer m CursorTrace)
    }

instance NFData (Cursor m h) where
  rnf (Cursor a b) = rwhnf a `seq` rwhnf b

data CursorState m h =
    CursorOpen !(CursorEnv m h)
  | CursorClosed  -- ^ Calls to a closed cursor raise an exception.

data CursorEnv m h = CursorEnv {
    -- === Session-inherited

    -- | The session that this cursor belongs to.
    --
    -- NOTE: Consider using the 'cursorSessionEnv' field instead of acquiring
    -- the session lock.
    cursorSession    :: !(Session m h)
    -- | Use this instead of 'cursorSession' for easy access. An open cursor may
    -- assume that its session is open. A session's global resources, and
    -- therefore resources that are inherited by the cursor, will only be
    -- released once the session is sure that no cursors are open anymore.
  , cursorSessionEnv :: !(SessionEnv m h)

    -- === Cursor-specific

    -- | Session-unique identifier for this cursor.
  , cursorId         :: !CursorId
    -- | Readers are immediately discarded once they are 'Readers.Drained',
    -- so if there is a 'Just', we can assume that it has further entries.
    -- Note that, while the readers do retain references on the blob files
    -- while they are active, once they are drained they do not. This could
    -- invalidate any 'BlobRef's previously handed out. To avoid this, we
    -- explicitly retain references on the runs and write buffer blofs and
    -- only release them when the cursor is closed (see cursorRuns and
    -- cursorWBB below).
  , cursorReaders    :: !(Maybe (Readers.Readers m h))

    --TODO: the cursorRuns and cursorWBB could be replaced by just retaining
    -- the BlobFile from the runs and WBB, so that we retain less. Since we
    -- only retain these to keep BlobRefs valid until the cursor is closed.
    -- Alternatively: the Readers could be modified to keep the BlobFiles even
    -- once the readers are drained, and only release them when the Readers is
    -- itself closed.

    -- | The runs held open by the cursor. We must release these references
    -- when the cursor gets closed.
  , cursorRuns       :: !(V.Vector (Ref (Run m h)))

    -- | The write buffer blobs, which like the runs, we have to keep open
    -- untile the cursor is closed.
  , cursorWBB        :: !(Ref (WBB.WriteBufferBlobs m h))
  }

{-# SPECIALISE withCursor ::
     OffsetKey
  -> Table IO h
  -> (Cursor IO h -> IO a)
  -> IO a #-}
-- | See 'Database.LSMTree.Normal.withCursor'.
withCursor ::
     (MonadMask m, MonadMVar m, MonadST m, MonadSTM m)
  => OffsetKey
  -> Table m h
  -> (Cursor m h -> m a)
  -> m a
withCursor offsetKey t = bracket (newCursor offsetKey t) closeCursor

{-# SPECIALISE newCursor ::
     OffsetKey
  -> Table IO h
  -> IO (Cursor IO h) #-}
-- | See 'Database.LSMTree.Normal.newCursor'.
newCursor ::
     (MonadMask m, MonadMVar m, MonadST m, MonadSTM m)
  => OffsetKey
  -> Table m h
  -> m (Cursor m h)
newCursor !offsetKey t = withOpenTable t $ \tEnv -> do
    let cursorSession = tableSession t
    let cursorSessionEnv = tableSessionEnv tEnv
    cursorId <- uniqueToCursorId <$>
      incrUniqCounter (sessionUniqCounter cursorSessionEnv)
    let cursorTracer = TraceCursor cursorId `contramap` sessionTracer cursorSession
    traceWith cursorTracer $ TraceCreateCursor (tableId t)

    -- We acquire a read-lock on the session open-state to prevent races, see
    -- 'sessionOpenTables'.
    withOpenSession cursorSession $ \_ -> do
      withActionRegistry $ \reg -> do
        (wb, wbblobs, cursorRuns) <- dupTableContent reg (tableContent tEnv)
        cursorReaders <-
          withRollbackMaybe reg
            (Readers.new offsetKey (Just (wb, wbblobs)) cursorRuns)
            Readers.close
        let cursorWBB = wbblobs
        cursorState <- newMVar (CursorOpen CursorEnv {..})
        let !cursor = Cursor {cursorState, cursorTracer}
        -- Track cursor, but careful: If now an exception is raised, all
        -- resources get freed by the registry, so if the session still
        -- tracks 'cursor' (which is 'CursorOpen'), it later double frees.
        -- Therefore, we only track the cursor if 'withActionRegistry' exits
        -- successfully, i.e. using 'delayedCommit'.
        delayedCommit reg $
          modifyMVar_ (sessionOpenCursors cursorSessionEnv) $
            pure . Map.insert cursorId cursor
        pure $! cursor
  where
    -- The table contents escape the read access, but we just duplicate
    -- references to each run, so it is safe.
    dupTableContent reg contentVar = do
        RW.withReadAccess contentVar $ \content -> do
          let !wb      = tableWriteBuffer content
              !wbblobs = tableWriteBufferBlobs content
          wbblobs' <- withRollback reg (dupRef wbblobs) releaseRef
          let runs = cachedRuns (tableCache content)
          runs' <- V.forM runs $ \r ->
                     withRollback reg (dupRef r) releaseRef
          pure (wb, wbblobs', runs')

{-# SPECIALISE closeCursor :: Cursor IO h -> IO () #-}
-- | See 'Database.LSMTree.Normal.closeCursor'.
closeCursor ::
     (MonadMask m, MonadMVar m, MonadSTM m, PrimMonad m)
  => Cursor m h
  -> m ()
closeCursor Cursor {..} = do
    traceWith cursorTracer $ TraceCloseCursor
    modifyWithActionRegistry_ (takeMVar cursorState) (putMVar cursorState) $ \reg -> \case
      CursorClosed -> return CursorClosed
      CursorOpen CursorEnv {..} -> do
        -- This should be safe-ish, but it's still not ideal, because it doesn't
        -- rule out sync exceptions in the cleanup operations.
        -- In that case, the cursor ends up closed, but resources might not have
        -- been freed. Probably better than the other way around, though.
        delayedCommit reg $
          modifyMVar_ (sessionOpenCursors cursorSessionEnv) $
            pure . Map.delete cursorId

        forM_ cursorReaders $ delayedCommit reg . Readers.close
        V.forM_ cursorRuns $ delayedCommit reg . releaseRef
        delayedCommit reg (releaseRef cursorWBB)
        return CursorClosed

{-# SPECIALISE readCursor ::
     ResolveSerialisedValue
  -> Int
  -> Cursor IO h
  -> (SerialisedKey -> SerialisedValue -> Maybe (WeakBlobRef IO h) -> res)
  -> IO (V.Vector res) #-}
-- | See 'Database.LSMTree.Normal.readCursor'.
readCursor ::
     forall m h res.
     (MonadMask m, MonadMVar m, MonadST m, MonadSTM m)
  => ResolveSerialisedValue
  -> Int  -- ^ Maximum number of entries to read
  -> Cursor m h
  -> (SerialisedKey -> SerialisedValue -> Maybe (WeakBlobRef m h) -> res)
     -- ^ How to map to a query result, different for normal/monoidal
  -> m (V.Vector res)
readCursor resolve n cursor fromEntry =
    readCursorWhile resolve (const True) n cursor fromEntry

{-# SPECIALISE readCursorWhile ::
     ResolveSerialisedValue
  -> (SerialisedKey -> Bool)
  -> Int
  -> Cursor IO h
  -> (SerialisedKey -> SerialisedValue -> Maybe (WeakBlobRef IO h) -> res)
  -> IO (V.Vector res) #-}
-- | @readCursorWhile _ p n cursor _@ reads elements until either:
--
--    * @n@ elements have been read already
--    * @p@ returns @False@ for the key of an entry to be read
--    * the cursor is drained
--
-- Consequently, once a call returned fewer than @n@ elements, any subsequent
-- calls with the same predicate @p@ will return an empty vector.
readCursorWhile ::
     forall m h res.
     (MonadMask m, MonadMVar m, MonadST m, MonadSTM m)
  => ResolveSerialisedValue
  -> (SerialisedKey -> Bool)  -- ^ Only read as long as this predicate holds
  -> Int  -- ^ Maximum number of entries to read
  -> Cursor m h
  -> (SerialisedKey -> SerialisedValue -> Maybe (WeakBlobRef m h) -> res)
     -- ^ How to map to a query result, different for normal/monoidal
  -> m (V.Vector res)
readCursorWhile resolve keyIsWanted n Cursor {..} fromEntry = do
    traceWith cursorTracer $ TraceReadCursor n
    modifyMVar cursorState $ \case
      CursorClosed -> throwIO ErrCursorClosed
      state@(CursorOpen cursorEnv) -> do
        case cursorReaders cursorEnv of
          Nothing ->
            -- a drained cursor will just return an empty vector
            return (state, V.empty)
          Just readers -> do
            (vec, hasMore) <- Cursor.readEntriesWhile resolve keyIsWanted fromEntry readers n
            -- if we drained the readers, remove them from the state
            let !state' = case hasMore of
                  Readers.HasMore -> state
                  Readers.Drained -> CursorOpen (cursorEnv {cursorReaders = Nothing})
            return (state', vec)

{-------------------------------------------------------------------------------
  Snapshots
-------------------------------------------------------------------------------}

{-# SPECIALISE createSnapshot ::
     SnapshotName
  -> SnapshotLabel
  -> SnapshotTableType
  -> Table IO h
  -> IO () #-}
-- |  See 'Database.LSMTree.Normal.createSnapshot''.
createSnapshot ::
     (MonadMask m, MonadMVar m, MonadST m, MonadSTM m)
  => SnapshotName
  -> SnapshotLabel
  -> SnapshotTableType
  -> Table m h
  -> m ()
createSnapshot snap label tableType t = do
    traceWith (tableTracer t) $ TraceSnapshot snap
    withOpenTable t $ \tEnv ->
      withActionRegistry $ \reg -> do -- TODO: use the action registry for all side effects
        let hfs  = tableHasFS tEnv
            hbio = tableHasBlockIO tEnv
            activeUc = tableSessionUniqCounter tEnv

        -- Guard that the snapshot does not exist already
        let snapDir = Paths.namedSnapshotDir (tableSessionRoot tEnv) snap
        doesSnapshotExist <-
          FS.doesDirectoryExist (tableHasFS tEnv) (Paths.getNamedSnapshotDir snapDir)
        if doesSnapshotExist then
          throwIO (ErrSnapshotExists snap)
        else
          -- we assume the snapshots directory already exists, so we just have
          -- to create the directory for this specific snapshot.
          withRollback_ reg
            (FS.createDirectory hfs (Paths.getNamedSnapshotDir snapDir))
            (FS.removeDirectoryRecursive hfs (Paths.getNamedSnapshotDir snapDir))

        -- Duplicate references to the table content, so that resources do not disappear
        -- from under our feet while taking a snapshot. These references are released
        -- again after the snapshot files/directories are written.
        content <- RW.withReadAccess (tableContent tEnv) (duplicateTableContent reg)

        -- Fresh UniqCounter so that we start numbering from 0 in the named
        -- snapshot directory
        snapUc <- newUniqCounter 0

        -- Snapshot the write buffer.
        let activeDir = Paths.activeDir (tableSessionRoot tEnv)
        let wb = tableWriteBuffer content
        let wbb = tableWriteBufferBlobs content
        snapWriteBufferNumber <- Paths.writeBufferNumber <$>
            snapshotWriteBuffer reg hfs hbio activeUc snapUc activeDir snapDir wb wbb

        -- Convert to snapshot format
        snapLevels <- toSnapLevels (tableLevels content)

        -- Hard link runs into the named snapshot directory
        snapLevels' <- snapshotRuns reg snapUc snapDir snapLevels

        -- Release the table content
        releaseTableContent reg content

        let snapMetaData = SnapshotMetaData label tableType (tableConfig t) snapWriteBufferNumber snapLevels'
            SnapshotMetaDataFile contentPath = Paths.snapshotMetaDataFile snapDir
            SnapshotMetaDataChecksumFile checksumPath = Paths.snapshotMetaDataChecksumFile snapDir
        writeFileSnapshotMetaData hfs contentPath checksumPath snapMetaData

        -- Make the directory and its contents durable.
        FS.synchroniseDirectoryRecursive hfs hbio (Paths.getNamedSnapshotDir snapDir)

{-# SPECIALISE openSnapshot ::
     Session IO h
  -> SnapshotLabel
  -> SnapshotTableType
  -> TableConfigOverride
  -> SnapshotName
  -> ResolveSerialisedValue
  -> IO (Table IO h) #-}
-- |  See 'Database.LSMTree.Normal.openSnapshot'.
openSnapshot ::
     (MonadMask m, MonadMVar m, MonadST m, MonadSTM m)
  => Session m h
  -> SnapshotLabel -- ^ Expected label
  -> SnapshotTableType -- ^ Expected table type
  -> TableConfigOverride -- ^ Optional config override
  -> SnapshotName
  -> ResolveSerialisedValue
  -> m (Table m h)
openSnapshot sesh label tableType override snap resolve = do
    traceWith (sessionTracer sesh) $ TraceOpenSnapshot snap override
    withOpenSession sesh $ \seshEnv -> do
      withActionRegistry $ \reg -> do
        let hfs     = sessionHasFS seshEnv
            hbio    = sessionHasBlockIO seshEnv
            uc      = sessionUniqCounter seshEnv

        -- Guard that the snapshot exists
        let snapDir = Paths.namedSnapshotDir (sessionRoot seshEnv) snap
        FS.doesDirectoryExist hfs (Paths.getNamedSnapshotDir snapDir) >>= \b ->
          unless b $ throwIO (ErrSnapshotNotExists snap)

        let SnapshotMetaDataFile contentPath = Paths.snapshotMetaDataFile snapDir
            SnapshotMetaDataChecksumFile checksumPath = Paths.snapshotMetaDataChecksumFile snapDir
        snapMetaData <- readFileSnapshotMetaData hfs contentPath checksumPath >>= \case
          Left e  -> throwIO (ErrSnapshotDeserialiseFailure e snap)
          Right x -> pure x

        let SnapshotMetaData label' tableType' conf snapWriteBuffer snapLevels = snapMetaData

        unless (tableType == tableType') $
          throwIO (ErrSnapshotWrongTableType snap tableType tableType')

        unless (label == label') $
          throwIO (ErrSnapshotWrongLabel snap label label')

        let conf' = applyOverride override conf
        am <- newArenaManager

        let activeDir = Paths.activeDir (sessionRoot seshEnv)

        -- Read write buffer
        let snapWriteBufferPaths = Paths.WriteBufferFsPaths (Paths.getNamedSnapshotDir snapDir) snapWriteBuffer
        (tableWriteBuffer, tableWriteBufferBlobs) <- openWriteBuffer reg resolve hfs hbio uc activeDir snapWriteBufferPaths

        -- Hard link runs into the active directory,
        snapLevels' <- openRuns reg hfs hbio (sessionUniqCounter seshEnv) snapDir activeDir snapLevels

        -- Convert from the snapshot format, restoring merge progress in the process
        tableLevels <- fromSnapLevels reg hfs hbio conf (sessionUniqCounter seshEnv) resolve activeDir snapLevels'
        releaseRuns reg snapLevels'

        tableCache <- mkLevelsCache reg tableLevels
        newWith reg sesh seshEnv conf' am $! TableContent {
            tableWriteBuffer
          , tableWriteBufferBlobs
          , tableLevels
          , tableCache
          , tableUnionLevel = NoUnion  -- TODO: at some point also load union level from snapshot
          }

{-# SPECIALISE deleteSnapshot ::
     Session IO h
  -> SnapshotName
  -> IO () #-}
-- |  See 'Database.LSMTree.Common.deleteSnapshot'.
deleteSnapshot ::
     (MonadMask m, MonadSTM m)
  => Session m h
  -> SnapshotName
  -> m ()
deleteSnapshot sesh snap = do
    traceWith (sessionTracer sesh) $ TraceDeleteSnapshot snap
    withOpenSession sesh $ \seshEnv -> do
      let hfs = sessionHasFS seshEnv

      let snapDir = Paths.namedSnapshotDir (sessionRoot seshEnv) snap
      doesSnapshotExist <-
        FS.doesDirectoryExist (sessionHasFS seshEnv) (Paths.getNamedSnapshotDir snapDir)
      unless doesSnapshotExist $ throwIO (ErrSnapshotNotExists snap)
      FS.removeDirectoryRecursive hfs (Paths.getNamedSnapshotDir snapDir)

{-# SPECIALISE listSnapshots :: Session IO h -> IO [SnapshotName] #-}
-- |  See 'Database.LSMTree.Common.listSnapshots'.
listSnapshots ::
     (MonadMask m, MonadSTM m)
  => Session m h
  -> m [SnapshotName]
listSnapshots sesh = do
    traceWith (sessionTracer sesh) TraceListSnapshots
    withOpenSession sesh $ \seshEnv -> do
      let hfs = sessionHasFS seshEnv
          root = sessionRoot seshEnv
      contents <- FS.listDirectory hfs (Paths.snapshotsDir (sessionRoot seshEnv))
      snaps <- mapM (checkSnapshot hfs root) $ Set.toList contents
      pure $ catMaybes snaps
  where
    checkSnapshot hfs root s =
      case Paths.mkSnapshotName s of
        Nothing   -> pure Nothing
        Just snap -> do
          -- check that it is a directory
          b <- FS.doesDirectoryExist hfs
                (Paths.getNamedSnapshotDir $ Paths.namedSnapshotDir root snap)
          if b then pure $ Just snap
               else pure $ Nothing

{-------------------------------------------------------------------------------
  Mutiple writable tables
-------------------------------------------------------------------------------}

{-# SPECIALISE duplicate :: Table IO h -> IO (Table IO h) #-}
-- | See 'Database.LSMTree.Normal.duplicate'.
duplicate ::
     (MonadMask m, MonadMVar m, MonadST m, MonadSTM m)
  => Table m h
  -> m (Table m h)
duplicate t@Table{..} = do
    traceWith tableTracer TraceDuplicate
    withOpenTable t $ \TableEnv{..} -> do
      -- We acquire a read-lock on the session open-state to prevent races, see
      -- 'sessionOpenTables'.
      withOpenSession tableSession $ \_ -> do
        withActionRegistry $ \reg -> do
          -- The table contents escape the read access, but we just added references
          -- to each run so it is safe.
          content <- RW.withReadAccess tableContent (duplicateTableContent reg)
          newWith
            reg
            tableSession
            tableSessionEnv
            tableConfig
            tableArenaManager
            content


{-------------------------------------------------------------------------------
   Table union
-------------------------------------------------------------------------------}

{-# SPECIALISE unions :: NonEmpty (Table IO h) -> IO (Table IO h) #-}
-- | See 'Database.LSMTree.Normal.unions'.
unions ::
     (MonadMask m, MonadMVar m, MonadST m, MonadSTM m)
  => NonEmpty (Table m h)
  -> m (Table m h)
unions ts = do
    sesh <-
      matchSessions ts >>= \case
        Left (i, j) -> throwIO $ ErrUnionsSessionMismatch i j
        Right sesh  -> pure sesh

    traceWith (sessionTracer sesh) $ TraceUnions (NE.map tableId ts)

    -- The TableConfig for the new table is taken from the last table in the
    -- union. This corresponds to the "Data.Map.union updates baseMap" order,
    -- where we take the config from the base map, not the updates.
    --
    -- This could be modified to take the new config as an input. It works to
    -- pick any config because the new table is almost completely fresh. It
    -- will have an empty write buffer and no runs in the normal levels. All
    -- the existing runs get squashed down into a single run before rejoining
    -- as a last level.
    let conf = tableConfig (NE.last ts)

    -- We acquire a read-lock on the session open-state to prevent races, see
    -- 'sessionOpenTables'.
    modifyWithActionRegistry
      (atomically $ RW.unsafeAcquireReadAccess (sessionState sesh))
      (\_ -> atomically $ RW.unsafeReleaseReadAccess (sessionState sesh)) $
      \reg -> \case
        SessionClosed -> throwIO ErrSessionClosed
        seshState@(SessionOpen seshEnv) -> do
          t <- unionsInOpenSession reg sesh seshEnv conf ts
          pure (seshState, t)

{-# SPECIALISE unionsInOpenSession ::
     ActionRegistry IO
  -> Session IO h
  -> SessionEnv IO h
  -> TableConfig
  -> NonEmpty (Table IO h)
  -> IO (Table IO h) #-}
unionsInOpenSession ::
     (MonadSTM m, MonadMask m, MonadMVar m, MonadST m)
  => ActionRegistry m
  -> Session m h
  -> SessionEnv m h
  -> TableConfig
  -> NonEmpty (Table m h)
  -> m (Table m h)
unionsInOpenSession reg sesh seshEnv conf ts = do

    mts <- forM (NE.toList ts) $ \t ->
      withOpenTable t $ \tEnv ->
        RW.withReadAccess (tableContent tEnv) $ \tc ->
          -- tableContentToMergingTree duplicates all runs and merges
          -- so the ones from the tableContent here do not escape
          -- the read access.
          withRollback reg
            (tableContentToMergingTree seshEnv conf tc)
            releaseRef
    mt <- withRollback reg (newPendingUnionMerge mts) releaseRef

    -- The mts here is a temporary value, since newPendingUnionMerge
    -- will make its own references, so release mts at the end of
    -- the action registry bracket
    forM_ mts (delayedCommit reg . releaseRef)

    empty <- newEmptyTableContent seshEnv reg
    let content = empty { tableUnionLevel = Union mt }

        -- Pick the arena manager to optimise the case of:
        -- someUpdates <> bigTableWithLotsOfLookups
        -- by reusing the arena manager from the last one.
        am = tableArenaManager (NE.last ts)

    newWith reg sesh seshEnv conf am content

{-# SPECIALISE tableContentToMergingTree ::
     SessionEnv IO h
  -> TableConfig
  -> TableContent IO h
  -> IO (Ref (MergingTree IO h)) #-}
tableContentToMergingTree ::
     forall m h.
     (MonadMask m, MonadMVar m, MonadST m, MonadSTM m)
  => SessionEnv m h
  -> TableConfig
  -> TableContent m h
  -> m (Ref (MergingTree m h))
tableContentToMergingTree seshEnv conf
                          tc@TableContent {
                            tableLevels,
                            tableUnionLevel
                          } =
    bracket (writeBufferToNewRun seshEnv conf tc)
            (mapM_ releaseRef) $ \mwbr ->
      let runs :: [PreExistingRun m h]
          runs = maybeToList (fmap PreExistingRun mwbr)
              ++ concatMap levelToPreExistingRuns (V.toList tableLevels)
          -- any pre-existing union in the input table:
          unionmt = case tableUnionLevel of
                    NoUnion  -> Nothing
                    Union mt -> Just mt
       in newPendingLevelMerge runs unionmt
  where
    levelToPreExistingRuns :: Level m h -> [PreExistingRun m h]
    levelToPreExistingRuns Level{incomingRun, residentRuns} =
        case incomingRun of
          Single        r  -> PreExistingRun r
          Merging _ _ _ mr -> PreExistingMergingRun mr
      : map PreExistingRun (V.toList residentRuns)

--TODO: can we share this or move it to MergeSchedule?
{-# SPECIALISE writeBufferToNewRun ::
     SessionEnv IO h
  -> TableConfig
  -> TableContent IO h
  -> IO (Maybe (Ref (Run IO h))) #-}
writeBufferToNewRun ::
     (MonadMask m, MonadST m, MonadSTM m)
  => SessionEnv m h
  -> TableConfig
  -> TableContent m h
  -> m (Maybe (Ref (Run m h)))
writeBufferToNewRun SessionEnv {
                      sessionRoot        = root,
                      sessionHasFS       = hfs,
                      sessionHasBlockIO  = hbio,
                      sessionUniqCounter = uc
                    }
                    conf
                    TableContent{
                      tableWriteBuffer,
                      tableWriteBufferBlobs
                    }
  | WB.null tableWriteBuffer = pure Nothing
  | otherwise                = Just <$> do
    !uniq <- incrUniqCounter uc
    let (!runParams, !runPaths) = mergingRunParamsForLevel
                                   (Paths.activeDir root) conf uniq (LevelNo 1)
    Run.fromWriteBuffer
      hfs hbio
      runParams runPaths
      tableWriteBuffer
      tableWriteBufferBlobs

-- | Check that all tables in the session match. If so, return the matched
-- session. If there is a mismatch, return the list indices of the mismatching
-- tables.
--
-- TODO: compare LockFileHandle instead of SessionRoot (?). We can write an Eq
-- instance for LockFileHandle based on pointer equality, just like base does
-- for Handle.
matchSessions ::
     (MonadSTM m, MonadThrow m)
  => NonEmpty (Table m h)
  -> m (Either (Int, Int) (Session m h))
matchSessions = \(t :| ts) ->
    withSessionRoot t $ \root -> do
      eith <- go root 1 ts
      pure $ case eith of
        Left i   -> Left (0, i)
        Right () -> Right (tableSession t)
  where
    -- Check that the session roots for all tables are the same. There can only
    -- be one *open/active* session per directory because of cooperative file
    -- locks, so each unique *open* session has a unique session root. We check
    -- that all the table's sessions are open at the same time while comparing
    -- the session roots.
    go _ _ [] = pure (Right ())
    go root !i (t':ts') =
        withSessionRoot t' $ \root' ->
          if root == root'
            then go root (i+1) ts'
            else pure (Left i)

    withSessionRoot t k =  withOpenSession (tableSession t) $ k . sessionRoot

{-------------------------------------------------------------------------------
  Table union: debt and credit
-------------------------------------------------------------------------------}

-- | See 'Database.LSMTree.Normal.UnionDebt'.
newtype UnionDebt = UnionDebt Int
  deriving newtype (Show, Eq, Ord, Num)

{-# SPECIALISE remainingUnionDebt :: Table IO h -> IO UnionDebt #-}
-- | See 'Database.LSMTree.Normal.remainingUnionDebt'.
remainingUnionDebt :: (MonadSTM m, MonadThrow m) => Table m h -> m UnionDebt
remainingUnionDebt t = do
    traceWith (tableTracer t) TraceRemainingUnionDebt
    withOpenTable t $ \tEnv -> do
      RW.withReadAccess (tableContent tEnv) $ \_tableContent -> do
        error "remainingUnionDebt: not yet implemented"

-- | See 'Database.LSMTree.Normal.UnionCredits'.
newtype UnionCredits = UnionCredits Int
  deriving newtype (Show, Eq, Ord, Num)

{-# SPECIALISE supplyUnionCredits :: Table IO h -> UnionCredits -> IO UnionCredits #-}
-- | See 'Database.LSMTree.Normal.supplyUnionCredits'.
supplyUnionCredits :: (MonadSTM m, MonadCatch m) => Table m h -> UnionCredits -> m UnionCredits
supplyUnionCredits t credits = do
    traceWith (tableTracer t) $ TraceSupplyUnionCredits credits
    withOpenTable t $ \tEnv -> do
      -- TODO: should this be acquiring read or write access?
      RW.withWriteAccess (tableContent tEnv) $ \_tableContent -> do
        error "supplyUnionCredits: not yet implemented"
