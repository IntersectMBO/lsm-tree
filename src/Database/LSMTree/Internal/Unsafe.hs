{-# LANGUAGE CPP       #-}
{-# LANGUAGE DataKinds #-}
{-# OPTIONS_HADDOCK not-home #-}

-- | This module brings together the internal parts to provide an API in terms
-- of untyped serialised keys, values and blobs.
--
-- Apart from defining the API, this module mainly deals with concurrency- and
-- exception-safe opening and closing of resources. Any other non-trivial logic
-- should live somewhere else.
--
module Database.LSMTree.Internal.Unsafe (
    -- * Exceptions
    SessionDirDoesNotExistError (..)
  , SessionDirLockedError (..)
  , SessionDirCorruptedError (..)
  , SessionClosedError (..)
  , TableClosedError (..)
  , TableCorruptedError (..)
  , TableTooLargeError (..)
  , TableUnionNotCompatibleError (..)
  , SnapshotExistsError (..)
  , SnapshotDoesNotExistError (..)
  , SnapshotCorruptedError (..)
  , SnapshotNotCompatibleError (..)
  , BlobRefInvalidError (..)
  , CursorClosedError (..)
  , FileFormat (..)
  , FileCorruptedError (..)
  , Paths.InvalidSnapshotNameError (..)
    -- * Tracing
  , LSMTreeTrace (..)
  , TableTrace (..)
  , CursorTrace (..)
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
  , saveSnapshot
  , openTableFromSnapshot
  , deleteSnapshot
  , doesSnapshotExist
  , listSnapshots
    -- * Multiple writable tables
  , duplicate
    -- * Table union
  , unions
  , UnionDebt (..)
  , remainingUnionDebt
  , UnionCredits (..)
  , supplyUnionCredits
  ) where

import           Control.ActionRegistry
import           Control.Concurrent.Class.MonadMVar.Strict
import           Control.Concurrent.Class.MonadSTM (MonadSTM (..))
import           Control.Concurrent.Class.MonadSTM.RWVar (RWVar)
import qualified Control.Concurrent.Class.MonadSTM.RWVar as RW
import           Control.DeepSeq
import           Control.Monad (forM, unless, void, (<$!>))
import           Control.Monad.Class.MonadAsync as Async
import           Control.Monad.Class.MonadST (MonadST (..))
import           Control.Monad.Class.MonadThrow
import           Control.Monad.Primitive
import           Control.RefCount
import           Control.Tracer
import           Data.Either (fromRight)
import           Data.Foldable
import           Data.List.NonEmpty (NonEmpty (..))
import qualified Data.List.NonEmpty as NE
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import           Data.Maybe (catMaybes, maybeToList)
import qualified Data.Set as Set
import           Data.Typeable
import qualified Data.Vector as V
import           Database.LSMTree.Internal.Arena (ArenaManager, newArenaManager)
import           Database.LSMTree.Internal.BlobRef (WeakBlobRef (..))
import qualified Database.LSMTree.Internal.BlobRef as BlobRef
import           Database.LSMTree.Internal.Config
import           Database.LSMTree.Internal.Config.Override (TableConfigOverride,
                     overrideTableConfig)
import           Database.LSMTree.Internal.CRC32C (FileCorruptedError (..),
                     FileFormat (..))
import qualified Database.LSMTree.Internal.Cursor as Cursor
import           Database.LSMTree.Internal.Entry (Entry)
import           Database.LSMTree.Internal.IncomingRun (IncomingRun (..))
import           Database.LSMTree.Internal.Lookup (TableCorruptedError (..),
                     lookupsIO, lookupsIOWithWriteBuffer)
import           Database.LSMTree.Internal.MergeSchedule
import           Database.LSMTree.Internal.MergingRun (TableTooLargeError (..))
import qualified Database.LSMTree.Internal.MergingRun as MR
import           Database.LSMTree.Internal.MergingTree
import qualified Database.LSMTree.Internal.MergingTree as MT
import qualified Database.LSMTree.Internal.MergingTree.Lookup as MT
import           Database.LSMTree.Internal.Paths (SessionRoot (..),
                     SnapshotMetaDataChecksumFile (..),
                     SnapshotMetaDataFile (..), SnapshotName)
import qualified Database.LSMTree.Internal.Paths as Paths
import           Database.LSMTree.Internal.Range (Range (..))
import           Database.LSMTree.Internal.Readers (OffsetKey (..))
import qualified Database.LSMTree.Internal.Readers as Readers
import           Database.LSMTree.Internal.Run (Run)
import qualified Database.LSMTree.Internal.Run as Run
import           Database.LSMTree.Internal.RunNumber
import           Database.LSMTree.Internal.Serialise (ResolveSerialisedValue,
                     SerialisedBlob (..), SerialisedKey, SerialisedValue)
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
  | TraceOpenTableFromSnapshot SnapshotName TableConfigOverride
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
#ifdef DEBUG_TRACES
  | TraceMerge (AtLevel MergeTrace)
#endif
    -- Snapshot
  | TraceSnapshot SnapshotName
    -- Duplicate
  | TraceDuplicate
    -- Unions
  | TraceRemainingUnionDebt
  | TraceSupplyUnionCredits UnionCredits
  deriving stock Show

contramapTraceMerge :: Monad m => Tracer m TableTrace -> Tracer m (AtLevel MergeTrace)
#ifdef DEBUG_TRACES
contramapTraceMerge t = TraceMerge `contramap` t
#else
contramapTraceMerge t = traceMaybe (const Nothing) t
#endif

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
-- For more information, see 'Database.LSMTree.Internal.Types.Session'.
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
    -- | The path to the directory in which this session is live. This is a path
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
    -- Each identifier is added only once in 'new', 'openTableFromSnapshot', 'duplicate',
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

-- | The session is closed.
data SessionClosedError
    = ErrSessionClosed
    deriving stock (Show, Eq)
    deriving anyclass (Exception)

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
-- | See 'Database.LSMTree.withSession'.
withSession ::
     (MonadMask m, MonadSTM m, MonadMVar m, PrimMonad m)
  => Tracer m LSMTreeTrace
  -> HasFS m h
  -> HasBlockIO m h
  -> FsPath
  -> (Session m h -> m a)
  -> m a
withSession tr hfs hbio dir = bracket (openSession tr hfs hbio dir) closeSession

-- | The session directory does not exist.
data SessionDirDoesNotExistError
    = ErrSessionDirDoesNotExist !FsErrorPath
    deriving stock (Show, Eq)
    deriving anyclass (Exception)

-- | The session directory is locked by another active session.
data SessionDirLockedError
    = ErrSessionDirLocked !FsErrorPath
    deriving stock (Show, Eq)
    deriving anyclass (Exception)

-- | The session directory is corrupted, e.g., it misses required files or contains unexpected files.
data SessionDirCorruptedError
    = ErrSessionDirCorrupted !FsErrorPath
    deriving stock (Show, Eq)
    deriving anyclass (Exception)

{-# SPECIALISE openSession ::
     Tracer IO LSMTreeTrace
  -> HasFS IO h
  -> HasBlockIO IO h
  -> FsPath
  -> IO (Session IO h) #-}
-- | See 'Database.LSMTree.openSession'.
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
        throwIO (ErrSessionDirDoesNotExist (FS.mkFsErrorPath hfs dir))
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
          -> throwIO (ErrSessionDirLocked fsep)
        Left  e -> throwIO e -- rethrow unexpected errors
        Right Nothing -> throwIO (ErrSessionDirLocked (FS.mkFsErrorPath hfs lockFilePath))
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
        unless b $ throwIO (ErrSessionDirCorrupted (FS.mkFsErrorPath hfs activeDirPath))
      FS.doesDirectoryExist hfs snapshotsDirPath >>= \b ->
        unless b $ throwIO (ErrSessionDirCorrupted (FS.mkFsErrorPath hfs snapshotsDirPath))

    -- The active directory should be empty
    checkActiveDirLayout = do
        contents <- FS.listDirectory hfs activeDirPath
        unless (Set.null contents) $ throwIO (ErrSessionDirCorrupted (FS.mkFsErrorPath hfs activeDirPath))

    -- Nothing to check: snapshots are verified when they are loaded, not when a
    -- session is restored.
    checkSnapshotsDirLayout = pure ()

{-# SPECIALISE closeSession :: Session IO h -> IO () #-}
-- | See 'Database.LSMTree.closeSession'.
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
-- For more information, see 'Database.LSMTree.Table'.
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

-- | The table is closed.
data TableClosedError
    = ErrTableClosed
    deriving stock (Show, Eq)
    deriving anyclass (Exception)

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
-- | See 'Database.LSMTree.withTable'.
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
-- | See 'Database.LSMTree.new'.
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
-- | See 'Database.LSMTree.close'.
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
-- | See 'Database.LSMTree.lookups'.
lookups ::
     (MonadAsync m, MonadMask m, MonadMVar m, MonadST m)
  => ResolveSerialisedValue
  -> V.Vector SerialisedKey
  -> Table m h
  -> m (V.Vector (Maybe (Entry SerialisedValue (WeakBlobRef m h))))
lookups resolve ks t = do
    traceWith (tableTracer t) $ TraceLookups (V.length ks)
    withOpenTable t $ \tEnv ->
      RW.withReadAccess (tableContent tEnv) $ \tc -> do
        case tableUnionLevel tc of
          NoUnion -> lookupsRegular tEnv tc
          Union tree unionCache -> do
            isStructurallyEmpty tree >>= \case
              True  -> lookupsRegular tEnv tc
              False -> if WB.null (tableWriteBuffer tc) && V.null (tableLevels tc)
                         then lookupsUnion tEnv unionCache
                         else lookupsRegularAndUnion tEnv tc unionCache
  where
    lookupsRegular tEnv tc = do
        let !cache = tableCache tc
        lookupsIOWithWriteBuffer
          (tableHasBlockIO tEnv)
          (tableArenaManager t)
          resolve
          (tableWriteBuffer tc)
          (tableWriteBufferBlobs tc)
          (cachedRuns cache)
          (cachedFilters cache)
          (cachedIndexes cache)
          (cachedKOpsFiles cache)
          ks

    lookupsUnion tEnv unionCache = do
        treeResults <- flip MT.mapMStrict (cachedTree unionCache) $ \runs ->
          Async.async $ lookupsUnionSingleBatch tEnv runs
        MT.foldLookupTree resolve treeResults

    lookupsRegularAndUnion tEnv tc unionCache = do
        -- asynchronously, so tree lookup batches can already be submitted
        -- without waiting for the regular level result.
        regularResult <- Async.async $ lookupsRegular tEnv tc
        treeResults <- flip MT.mapMStrict (cachedTree unionCache) $ \runs ->
          Async.async $ lookupsUnionSingleBatch tEnv runs
        MT.foldLookupTree resolve $
          MT.mkLookupNode MR.MergeLevel $ V.fromList
            [ MT.LookupBatch regularResult
            , treeResults
            ]

    lookupsUnionSingleBatch tEnv runs =
        lookupsIO
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
-- | See 'Database.LSMTree.rangeLookup'.
rangeLookup ::
     (MonadMask m, MonadMVar m, MonadST m, MonadSTM m)
  => ResolveSerialisedValue
  -> Range SerialisedKey
  -> Table m h
  -> (SerialisedKey -> SerialisedValue -> Maybe (WeakBlobRef m h) -> res)
     -- ^ How to map to a query result
  -> m (V.Vector res)
rangeLookup resolve range t fromEntry = do
    traceWith (tableTracer t) $ TraceRangeLookup range
    case range of
      FromToExcluding lb ub ->
        withCursor resolve (OffsetKey lb) t $ \cursor ->
          go cursor (< ub) []
      FromToIncluding lb ub ->
        withCursor resolve (OffsetKey lb) t $ \cursor ->
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
        else pure (V.concat (reverse (V.slice 0 n chunk : chunks)))

{-# SPECIALISE updates ::
     ResolveSerialisedValue
  -> V.Vector (SerialisedKey, Entry SerialisedValue SerialisedBlob)
  -> Table IO h
  -> IO () #-}
-- | See 'Database.LSMTree.updates'.
--
-- Does not enforce that upsert and BLOBs should not occur in the same table.
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
            (contramapTraceMerge $ tableTracer t)
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

{- | A 'BlobRef' used with 'retrieveBlobs' was invalid.

'BlobRef's are obtained from lookups in a 'Table', but they may be
invalidated by subsequent changes in that 'Table'. In general the
reliable way to retrieve blobs is not to change the 'Table' before
retrieving the blobs. To allow later retrievals, duplicate the table
before making modifications and keep the table open until all blob
retrievals are complete.
-}
data BlobRefInvalidError
    = -- | The 'Int' index indicates the first invalid 'BlobRef'.
      ErrBlobRefInvalid !Int
    deriving stock (Show, Eq)
    deriving anyclass (Exception)

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
-- For more information, see 'Database.LSMTree.Cursor'.
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
    -- only release them when the cursor is closed (see 'cursorWBB' and
    -- 'cursorRuns' below).
  , cursorReaders    :: !(Maybe (Readers.Readers m h))

    -- TODO: the cursorRuns and cursorWBB could be replaced by just retaining
    -- the BlobFile from the runs and WBB, so that we retain less. Since we
    -- only retain these to keep BlobRefs valid until the cursor is closed.
    -- Alternatively: the Readers could be modified to keep the BlobFiles even
    -- once the readers are drained, and only release them when the Readers is
    -- itself closed.

    -- | The write buffer blobs, which like the runs, we have to keep open
    -- untile the cursor is closed.
  , cursorWBB        :: !(Ref (WBB.WriteBufferBlobs m h))

    -- | The runs from regular levels that are held open by the cursor. We must
    -- release these references when the cursor gets closed.
  , cursorRuns       :: !(V.Vector (Ref (Run m h)))

    -- | The runs from the union level that are held open by the cursor. We must
    -- release these references when the cursor gets closed.
  , cursorUnion      :: !(Maybe (UnionCache m h))
  }

{-# SPECIALISE withCursor ::
     ResolveSerialisedValue
  -> OffsetKey
  -> Table IO h
  -> (Cursor IO h -> IO a)
  -> IO a #-}
-- | See 'Database.LSMTree.withCursor'.
withCursor ::
     (MonadMask m, MonadMVar m, MonadST m, MonadSTM m)
  => ResolveSerialisedValue
  -> OffsetKey
  -> Table m h
  -> (Cursor m h -> m a)
  -> m a
withCursor resolve offsetKey t = bracket (newCursor resolve offsetKey t) closeCursor

{-# SPECIALISE newCursor ::
     ResolveSerialisedValue
  -> OffsetKey
  -> Table IO h
  -> IO (Cursor IO h) #-}
-- | See 'Database.LSMTree.newCursor'.
newCursor ::
     (MonadMask m, MonadMVar m, MonadST m, MonadSTM m)
  => ResolveSerialisedValue
  -> OffsetKey
  -> Table m h
  -> m (Cursor m h)
newCursor !resolve !offsetKey t = withOpenTable t $ \tEnv -> do
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
        (wb, wbblobs, cursorRuns, cursorUnion) <-
          dupTableContent reg (tableContent tEnv)
        let cursorSources =
                Readers.FromWriteBuffer wb wbblobs
              : fmap Readers.FromRun (V.toList cursorRuns)
             <> case cursorUnion of
                  Nothing -> []
                  Just (UnionCache treeCache) ->
                    [lookupTreeToReaderSource treeCache]
        cursorReaders <-
          withRollbackMaybe reg
            (Readers.new resolve offsetKey cursorSources)
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
          unionCache <- case tableUnionLevel content of
            NoUnion   -> pure Nothing
            Union _ c -> Just <$!> duplicateUnionCache reg c
          pure (wb, wbblobs', runs', unionCache)

lookupTreeToReaderSource ::
     MT.LookupTree (V.Vector (Ref (Run m h))) -> Readers.ReaderSource m h
lookupTreeToReaderSource = \case
    MT.LookupBatch v ->
      case map Readers.FromRun (V.toList v) of
        [src] -> src
        srcs  -> Readers.FromReaders Readers.MergeLevel srcs
    MT.LookupNode ty children ->
      Readers.FromReaders
        (convertMergeType ty)
        (map lookupTreeToReaderSource (V.toList children))
  where
    convertMergeType = \case
      MR.MergeUnion -> Readers.MergeUnion
      MR.MergeLevel -> Readers.MergeLevel

{-# SPECIALISE closeCursor :: Cursor IO h -> IO () #-}
-- | See 'Database.LSMTree.closeCursor'.
closeCursor ::
     (MonadMask m, MonadMVar m, MonadSTM m, PrimMonad m)
  => Cursor m h
  -> m ()
closeCursor Cursor {..} = do
    traceWith cursorTracer $ TraceCloseCursor
    modifyWithActionRegistry_ (takeMVar cursorState) (putMVar cursorState) $ \reg -> \case
      CursorClosed -> pure CursorClosed
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
        forM_ cursorUnion $ releaseUnionCache reg
        delayedCommit reg (releaseRef cursorWBB)
        pure CursorClosed

{-# SPECIALISE readCursor ::
     ResolveSerialisedValue
  -> Int
  -> Cursor IO h
  -> (SerialisedKey -> SerialisedValue -> Maybe (WeakBlobRef IO h) -> res)
  -> IO (V.Vector res) #-}
-- | See 'Database.LSMTree.readCursor'.
readCursor ::
     forall m h res.
     (MonadMask m, MonadMVar m, MonadST m, MonadSTM m)
  => ResolveSerialisedValue
  -> Int  -- ^ Maximum number of entries to read
  -> Cursor m h
  -> (SerialisedKey -> SerialisedValue -> Maybe (WeakBlobRef m h) -> res)
     -- ^ How to map to a query result
  -> m (V.Vector res)
readCursor resolve n cursor fromEntry =
    readCursorWhile resolve (const True) n cursor fromEntry

-- | The cursor is closed.
data CursorClosedError
    = ErrCursorClosed
    deriving stock (Show, Eq)
    deriving anyclass (Exception)

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
     -- ^ How to map to a query result
  -> m (V.Vector res)
readCursorWhile resolve keyIsWanted n Cursor {..} fromEntry = do
    traceWith cursorTracer $ TraceReadCursor n
    modifyMVar cursorState $ \case
      CursorClosed -> throwIO ErrCursorClosed
      state@(CursorOpen cursorEnv) -> do
        case cursorReaders cursorEnv of
          Nothing ->
            -- a drained cursor will just return an empty vector
            pure (state, V.empty)
          Just readers -> do
            (vec, hasMore) <- Cursor.readEntriesWhile resolve keyIsWanted fromEntry readers n
            -- if we drained the readers, remove them from the state
            let !state' = case hasMore of
                  Readers.HasMore -> state
                  Readers.Drained -> CursorOpen (cursorEnv {cursorReaders = Nothing})
            pure (state', vec)

{-------------------------------------------------------------------------------
  Snapshots
-------------------------------------------------------------------------------}

-- | The named snapshot already exists.
data SnapshotExistsError
    = ErrSnapshotExists !SnapshotName
    deriving stock (Show, Eq)
    deriving anyclass (Exception)

{-# SPECIALISE saveSnapshot ::
     SnapshotName
  -> SnapshotLabel
  -> Table IO h
  -> IO () #-}
-- |  See 'Database.LSMTree.saveSnapshot'.
saveSnapshot ::
     (MonadMask m, MonadMVar m, MonadST m, MonadSTM m)
  => SnapshotName
  -> SnapshotLabel
  -> Table m h
  -> m ()
saveSnapshot snap label t = do
    traceWith (tableTracer t) $ TraceSnapshot snap
    withOpenTable t $ \tEnv ->
      withActionRegistry $ \reg -> do -- TODO: use the action registry for all side effects
        let hfs  = tableHasFS tEnv
            hbio = tableHasBlockIO tEnv
            activeUc = tableSessionUniqCounter tEnv

        -- Guard that the snapshot does not exist already
        let snapDir = Paths.namedSnapshotDir (tableSessionRoot tEnv) snap
        snapshotExists <- doesSnapshotDirExist snap (tableSessionEnv tEnv)
        if snapshotExists then
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
            snapshotWriteBuffer hfs hbio activeUc snapUc reg activeDir snapDir wb wbb

        -- Convert to snapshot format
        snapLevels <- toSnapLevels (tableLevels content)

        -- Hard link runs into the named snapshot directory
        snapLevels' <- traverse (snapshotRun hfs hbio snapUc reg snapDir) snapLevels

        -- If a merging tree exists, do the same hard-linking for the runs within
        mTreeOpt <- case tableUnionLevel content of
          NoUnion -> pure Nothing
          Union mTreeRef _cache -> do
            mTree <- toSnapMergingTree mTreeRef
            Just <$> traverse (snapshotRun hfs hbio snapUc reg snapDir) mTree

        releaseTableContent reg content

        let snapMetaData = SnapshotMetaData
                label
                (tableConfig t)
                snapWriteBufferNumber
                snapLevels'
                mTreeOpt
            SnapshotMetaDataFile contentPath = Paths.snapshotMetaDataFile snapDir
            SnapshotMetaDataChecksumFile checksumPath = Paths.snapshotMetaDataChecksumFile snapDir
        writeFileSnapshotMetaData hfs contentPath checksumPath snapMetaData

        -- Make the directory and its contents durable.
        FS.synchroniseDirectoryRecursive hfs hbio (Paths.getNamedSnapshotDir snapDir)

-- | The named snapshot does not exist.
data SnapshotDoesNotExistError
    = ErrSnapshotDoesNotExist !SnapshotName
    deriving stock (Show, Eq)
    deriving anyclass (Exception)

-- | The named snapshot is corrupted.
data SnapshotCorruptedError
    = ErrSnapshotCorrupted
        !SnapshotName
        !FileCorruptedError
    deriving stock (Show, Eq)
    deriving anyclass (Exception)

-- | The named snapshot is not compatible with the expected type.
data SnapshotNotCompatibleError
    = -- | The named snapshot is not compatible with the given label.
      ErrSnapshotWrongLabel
        !SnapshotName
        -- | Expected label
        !SnapshotLabel
        -- | Actual label
        !SnapshotLabel
    deriving stock (Show, Eq)
    deriving anyclass (Exception)

{-# SPECIALISE openTableFromSnapshot ::
     TableConfigOverride
  -> Session IO h
  -> SnapshotName
  -> SnapshotLabel
  -> ResolveSerialisedValue
  -> IO (Table IO h) #-}
-- |  See 'Database.LSMTree.openTableFromSnapshot'.
openTableFromSnapshot ::
     (MonadMask m, MonadMVar m, MonadST m, MonadSTM m)
  => TableConfigOverride
  -> Session m h
  -> SnapshotName
  -> SnapshotLabel -- ^ Expected label
  -> ResolveSerialisedValue
  -> m (Table m h)
openTableFromSnapshot policyOveride sesh snap label resolve =
  wrapFileCorruptedErrorAsSnapshotCorruptedError snap $ do
    traceWith (sessionTracer sesh) $ TraceOpenTableFromSnapshot snap policyOveride
    withOpenSession sesh $ \seshEnv -> do
      withActionRegistry $ \reg -> do
        let hfs     = sessionHasFS seshEnv
            hbio    = sessionHasBlockIO seshEnv
            uc      = sessionUniqCounter seshEnv

        -- Guard that the snapshot exists
        let snapDir = Paths.namedSnapshotDir (sessionRoot seshEnv) snap
        FS.doesDirectoryExist hfs (Paths.getNamedSnapshotDir snapDir) >>= \b ->
          unless b $ throwIO (ErrSnapshotDoesNotExist snap)

        let SnapshotMetaDataFile contentPath = Paths.snapshotMetaDataFile snapDir
            SnapshotMetaDataChecksumFile checksumPath = Paths.snapshotMetaDataChecksumFile snapDir

        snapMetaData <- readFileSnapshotMetaData hfs contentPath checksumPath

        let SnapshotMetaData label' conf snapWriteBuffer snapLevels mTreeOpt
              = overrideTableConfig policyOveride snapMetaData

        unless (label == label') $
          throwIO (ErrSnapshotWrongLabel snap label label')

        am <- newArenaManager

        let activeDir = Paths.activeDir (sessionRoot seshEnv)

        -- Read write buffer
        let snapWriteBufferPaths = Paths.WriteBufferFsPaths (Paths.getNamedSnapshotDir snapDir) snapWriteBuffer
        (tableWriteBuffer, tableWriteBufferBlobs) <-
          openWriteBuffer reg resolve hfs hbio uc activeDir snapWriteBufferPaths

        -- Hard link runs into the active directory,
        snapLevels' <- traverse (openRun hfs hbio uc reg snapDir activeDir) snapLevels
        unionLevel <- case mTreeOpt of
              Nothing -> pure NoUnion
              Just mTree -> do
                snapTree <- traverse (openRun hfs hbio uc reg snapDir activeDir) mTree
                mt <- fromSnapMergingTree hfs hbio uc resolve activeDir reg snapTree
                isStructurallyEmpty mt >>= \case
                  True ->
                    pure NoUnion
                  False -> do
                    traverse_ (delayedCommit reg . releaseRef) snapTree
                    cache <- mkUnionCache reg mt
                    pure (Union mt cache)

        -- Convert from the snapshot format, restoring merge progress in the process
        tableLevels <- fromSnapLevels hfs hbio uc conf resolve reg activeDir snapLevels'
        traverse_ (delayedCommit reg . releaseRef) snapLevels'

        tableCache <- mkLevelsCache reg tableLevels
        newWith reg sesh seshEnv conf am $! TableContent {
            tableWriteBuffer
          , tableWriteBufferBlobs
          , tableLevels
          , tableCache
          , tableUnionLevel = unionLevel
          }

{-# SPECIALISE wrapFileCorruptedErrorAsSnapshotCorruptedError ::
       SnapshotName
    -> IO a
    -> IO a
    #-}
wrapFileCorruptedErrorAsSnapshotCorruptedError ::
       forall m a.
       (MonadCatch m)
    => SnapshotName
    -> m a
    -> m a
wrapFileCorruptedErrorAsSnapshotCorruptedError snapshotName =
    mapExceptionWithActionRegistry (ErrSnapshotCorrupted snapshotName)

{-# SPECIALISE doesSnapshotExist ::
     Session IO h
  -> SnapshotName
  -> IO Bool #-}
-- |  See 'Database.LSMTree.doesSnapshotExist'.
doesSnapshotExist ::
     (MonadMask m, MonadSTM m)
  => Session m h
  -> SnapshotName
  -> m Bool
doesSnapshotExist sesh snap = withOpenSession sesh (doesSnapshotDirExist snap)

-- | Internal helper: Variant of 'doesSnapshotExist' that does not take a session lock.
doesSnapshotDirExist :: SnapshotName -> SessionEnv m h -> m Bool
doesSnapshotDirExist snap seshEnv = do
  let snapDir = Paths.namedSnapshotDir (sessionRoot seshEnv) snap
  FS.doesDirectoryExist (sessionHasFS seshEnv) (Paths.getNamedSnapshotDir snapDir)

{-# SPECIALISE deleteSnapshot ::
     Session IO h
  -> SnapshotName
  -> IO () #-}
-- |  See 'Database.LSMTree.deleteSnapshot'.
deleteSnapshot ::
     (MonadMask m, MonadSTM m)
  => Session m h
  -> SnapshotName
  -> m ()
deleteSnapshot sesh snap = do
    traceWith (sessionTracer sesh) $ TraceDeleteSnapshot snap
    withOpenSession sesh $ \seshEnv -> do
      let snapDir = Paths.namedSnapshotDir (sessionRoot seshEnv) snap
      snapshotExists <- doesSnapshotDirExist snap seshEnv
      unless snapshotExists $ throwIO (ErrSnapshotDoesNotExist snap)
      FS.removeDirectoryRecursive (sessionHasFS seshEnv) (Paths.getNamedSnapshotDir snapDir)

{-# SPECIALISE listSnapshots :: Session IO h -> IO [SnapshotName] #-}
-- |  See 'Database.LSMTree.listSnapshots'.
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
    checkSnapshot hfs root s = do
      -- TODO: rethrow 'ErrInvalidSnapshotName' as 'ErrSnapshotDirCorrupted'
      let snap = Paths.toSnapshotName s
      -- check that it is a directory
      b <- FS.doesDirectoryExist hfs
            (Paths.getNamedSnapshotDir $ Paths.namedSnapshotDir root snap)
      if b then pure $ Just snap
            else pure $ Nothing

{-------------------------------------------------------------------------------
  Multiple writable tables
-------------------------------------------------------------------------------}

{-# SPECIALISE duplicate :: Table IO h -> IO (Table IO h) #-}
-- | See 'Database.LSMTree.duplicate'.
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
        !FsErrorPath
        -- | The index of the second table.
        !Int
        -- | The session directory of the second table.
        !FsErrorPath
    deriving stock (Show, Eq)
    deriving anyclass (Exception)

{-# SPECIALISE unions :: NonEmpty (Table IO h) -> IO (Table IO h) #-}
-- | See 'Database.LSMTree.unions'.
unions ::
     (MonadMask m, MonadMVar m, MonadST m, MonadSTM m)
  => NonEmpty (Table m h)
  -> m (Table m h)
unions ts = do
    sesh <- ensureSessionsMatch ts

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

    content <- MT.isStructurallyEmpty mt >>= \case
      True -> do
        -- no need to have an empty merging tree
        delayedCommit reg (releaseRef mt)
        newEmptyTableContent seshEnv reg
      False -> do
        empty <- newEmptyTableContent seshEnv reg
        cache <- mkUnionCache reg mt
        pure empty { tableUnionLevel = Union mt cache }

    -- Pick the arena manager to optimise the case of:
    -- someUpdates <> bigTableWithLotsOfLookups
    -- by reusing the arena manager from the last one.
    let am = tableArenaManager (NE.last ts)

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
                    NoUnion    -> Nothing
                    Union mt _ -> Just mt  -- we could reuse the cache, but it
                                           -- would complicate things
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

{-# SPECIALISE ensureSessionsMatch ::
     NonEmpty (Table IO h)
  -> IO (Session IO h) #-}
-- | Check if all tables have the same session.
--   If so, return the session.
--   Otherwise, throw a 'TableUnionNotCompatibleError'.
ensureSessionsMatch ::
     (MonadSTM m, MonadThrow m)
  => NonEmpty (Table m h)
  -> m (Session m h)
ensureSessionsMatch (t :| ts) = do
  let sesh = tableSession t
  withOpenSession sesh $ \seshEnv -> do
    let root = FS.mkFsErrorPath (sessionHasFS seshEnv) (getSessionRoot (sessionRoot seshEnv))
    -- Check that the session roots for all tables are the same. There can only
    -- be one *open/active* session per directory because of cooperative file
    -- locks, so each unique *open* session has a unique session root. We check
    -- that all the table's sessions are open at the same time while comparing
    -- the session roots.
    for_ (zip [1..] ts) $ \(i, t') -> do
      let sesh' = tableSession t'
      withOpenSession sesh' $ \seshEnv' -> do
        let root' = FS.mkFsErrorPath (sessionHasFS seshEnv') (getSessionRoot (sessionRoot seshEnv'))
        -- TODO: compare LockFileHandle instead of SessionRoot (?).
        -- We can write an Eq instance for LockFileHandle based on pointer equality,
        -- just like base does for Handle.
        unless (root == root') $ throwIO $ ErrTableUnionSessionMismatch 0 root i root'
    pure sesh

{-------------------------------------------------------------------------------
  Table union: debt and credit
-------------------------------------------------------------------------------}

{- |
Union debt represents the amount of computation that must be performed before an incremental union is completed.
This includes the cost of completing incremental unions that were part of a union's input.

__Warning:__ The 'UnionDebt' returned by 'Database.LSMTree.remainingUnionDebt' is an /upper bound/ on the remaining union debt, not the exact union debt.
-}
newtype UnionDebt = UnionDebt Int
  deriving newtype (Show, Eq, Ord, Num)

{-# SPECIALISE remainingUnionDebt :: Table IO h -> IO UnionDebt #-}
-- | See 'Database.LSMTree.remainingUnionDebt'.
remainingUnionDebt ::
     (MonadSTM m, MonadMVar m, MonadThrow m, PrimMonad m)
  => Table m h -> m UnionDebt
remainingUnionDebt t = do
    traceWith (tableTracer t) TraceRemainingUnionDebt
    withOpenTable t $ \tEnv -> do
      RW.withReadAccess (tableContent tEnv) $ \tableContent -> do
        case tableUnionLevel tableContent of
          NoUnion ->
            pure (UnionDebt 0)
          Union mt _ -> do
            (MergeDebt (MergeCredits c), _) <- MT.remainingMergeDebt mt
            pure (UnionDebt c)

{- |
Union credits are passed to 'Database.LSMTree.supplyUnionCredits' to perform some amount of computation to incrementally complete a union.
-}
newtype UnionCredits = UnionCredits Int
  deriving newtype (Show, Eq, Ord, Num)

{-# SPECIALISE supplyUnionCredits ::
     ResolveSerialisedValue -> Table IO h -> UnionCredits -> IO UnionCredits #-}
-- | See 'Database.LSMTree.supplyUnionCredits'.
supplyUnionCredits ::
     (MonadST m, MonadSTM m, MonadMVar m, MonadMask m)
  => ResolveSerialisedValue -> Table m h -> UnionCredits -> m UnionCredits
supplyUnionCredits resolve t credits = do
    traceWith (tableTracer t) $ TraceSupplyUnionCredits credits
    withOpenTable t $ \tEnv -> do
      -- We also want to mutate the table content to re-build the union cache,
      -- but we don't need to hold a writer lock while we work on the tree
      -- itself.
      -- TODO: revisit the locking strategy here.
      leftovers <- RW.withReadAccess (tableContent tEnv) $ \tc -> do
        case tableUnionLevel tc of
          NoUnion ->
            pure (max 0 credits)  -- all leftovers (but never negative)
          Union mt _ -> do
            let conf = tableConfig t
            let AllocNumEntries x = confWriteBufferAlloc conf
            -- We simply use the write buffer capacity as merge credit threshold, as
            -- the regular level merges also do.
            -- TODO: pick a more suitable threshold or make configurable?
            let thresh = MR.CreditThreshold (MR.UnspentCredits (MergeCredits x))
            MergeCredits leftovers <-
              MT.supplyCredits
                (tableHasFS tEnv)
                (tableHasBlockIO tEnv)
                resolve
                (runParamsForLevel conf UnionLevel)
                thresh
                (tableSessionRoot tEnv)
                (tableSessionUniqCounter tEnv)
                mt
                (let UnionCredits c = credits in MergeCredits c)
            pure (UnionCredits leftovers)
      -- TODO: don't go into this section if we know there is NoUnion
      modifyWithActionRegistry_
        (RW.unsafeAcquireWriteAccess (tableContent tEnv))
        (atomically . RW.unsafeReleaseWriteAccess (tableContent tEnv))
        $ \reg tc ->
          case tableUnionLevel tc of
            NoUnion -> pure tc
            Union mt cache -> do
              unionLevel' <- MT.isStructurallyEmpty mt >>= \case
                True  ->
                  pure NoUnion
                False -> do
                  cache' <- mkUnionCache reg mt
                  releaseUnionCache reg cache
                  pure (Union mt cache')
              pure tc { tableUnionLevel = unionLevel' }
      pure leftovers
