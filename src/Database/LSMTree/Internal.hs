{-# LANGUAGE CPP       #-}
{-# LANGUAGE DataKinds #-}

module Database.LSMTree.Internal (
    -- * Exceptions
    LSMTreeError (..)
    -- * Tracing
  , LSMTreeTrace (..)
  , TableTrace (..)
    -- * Session
  , Session (..)
  , SessionState (..)
  , SessionEnv (..)
    -- ** Implementation of public API
  , withSession
  , openSession
  , closeSession
    -- * Table handle
  , TableHandle (..)
  , TableHandleState (..)
  , TableHandleEnv (..)
  , withOpenTable
    -- ** Implementation of public API
  , ResolveSerialisedValue
  , withTable
  , new
  , close
  , lookups
  , updates
    -- ** Cursor API
  , Cursor (..)
  , CursorState (..)
  , CursorEnv (..)
  , withCursor
  , newCursor
  , closeCursor
    -- * Snapshots
  , SnapshotLabel
  , snapshot
  , open
  , deleteSnapshot
  , listSnapshots
    -- * Mutiple writable table handles
  , duplicate
  ) where

import           Control.Concurrent.Class.MonadMVar.Strict
import           Control.Concurrent.Class.MonadSTM (MonadSTM (..))
import           Control.Concurrent.Class.MonadSTM.RWVar (RWVar)
import qualified Control.Concurrent.Class.MonadSTM.RWVar as RW
import           Control.DeepSeq
import           Control.Monad (unless, void, when)
import           Control.Monad.Class.MonadThrow
import           Control.Monad.Primitive
import           Control.TempRegistry
import           Control.Tracer
import           Data.Arena (ArenaManager, newArenaManager)
import           Data.Bifunctor (Bifunctor (..))
import qualified Data.ByteString.Char8 as BSC
import           Data.Char (isNumber)
import           Data.Foldable
import           Data.Functor.Compose (Compose (..))
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import           Data.Maybe (catMaybes)
import qualified Data.Set as Set
import qualified Data.Vector as V
import           Data.Word (Word64)
import           Database.LSMTree.Internal.BlobRef
import           Database.LSMTree.Internal.Config
import           Database.LSMTree.Internal.Entry (Entry, combineMaybe)
import           Database.LSMTree.Internal.Lookup (ByteCountDiscrepancy,
                     ResolveSerialisedValue, lookupsIO)
import           Database.LSMTree.Internal.MergeSchedule
import           Database.LSMTree.Internal.Paths (RunFsPaths (..),
                     SessionRoot (..), SnapshotName)
import qualified Database.LSMTree.Internal.Paths as Paths
import           Database.LSMTree.Internal.Run (Run)
import qualified Database.LSMTree.Internal.Run as Run
import           Database.LSMTree.Internal.RunNumber
import qualified Database.LSMTree.Internal.RunReaders as Readers
import           Database.LSMTree.Internal.Serialise (SerialisedBlob,
                     SerialisedKey, SerialisedValue)
import           Database.LSMTree.Internal.UniqCounter
import qualified Database.LSMTree.Internal.Vector as V
import           Database.LSMTree.Internal.WriteBuffer (WriteBuffer)
import qualified Database.LSMTree.Internal.WriteBuffer as WB
import qualified System.FS.API as FS
import           System.FS.API (FsError, FsErrorPath (..), FsPath, Handle,
                     HasFS)
import qualified System.FS.API.Lazy as FS
import qualified System.FS.API.Strict as FS
import qualified System.FS.BlockIO.API as FS
import           System.FS.BlockIO.API (HasBlockIO)

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
  | ErrSnapshotWrongType SnapshotName
    -- | Something went wrong during batch lookups.
  | ErrLookup ByteCountDiscrepancy
  deriving stock (Show)
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
  | TraceTable
      Word64 -- ^ Table identifier
      TableTrace
  | TraceDeleteSnapshot SnapshotName
  | TraceListSnapshots
  deriving stock Show

data TableTrace =
    -- | A table handle is created with the specified config.
    --
    -- This message is traced in addition to messages like 'TraceNewTable' and
    -- 'TraceDuplicate'.
    TraceCreateTableHandle TableConfig
  | TraceCloseTable
    -- Lookups
  | TraceLookups Int
    -- Updates
  | TraceUpdates Int
  | TraceMerge (AtLevel MergeTrace)
    -- Snapshot
  | TraceSnapshot SnapshotName
    -- Duplicate
  | TraceDuplicate
  deriving stock Show

{-------------------------------------------------------------------------------
  Session
-------------------------------------------------------------------------------}

-- | A session provides context that is shared across multiple table handles.
--
-- For more information, see 'Database.LSMTree.Common.Session'.
data Session m h = Session {
      -- | The primary purpose of this 'RWVar' is to ensure consistent views of
      -- the open-/closedness of a session when multiple threads require access
      -- to the session's fields (see 'withOpenSession'). We use more
      -- fine-grained synchronisation for various mutable parts of an open
      -- session.
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
    -- Each identifier is added only once in 'new', 'open' or 'duplicate', and
    -- is deleted only once in 'close' or 'closeSession'.
    --
    -- * A new table may only insert its own identifier when it has acquired the
    --   'sessionState' read-lock. This is to prevent races with 'closeSession'.
    --
    -- * A table 'close' may delete its own identifier from the set of open
    --   tables without restrictions, even concurrently with 'closeSession'.
    --   This is safe because 'close' is idempotent'.
  , sessionOpenTables  :: !(StrictMVar m (Map Word64 (TableHandle m h)))
    -- | Similarly to tables, open cursors are tracked so they can be closed
    -- once the session is closed. See 'sessionOpenTables'.
  , sessionOpenCursors :: !(StrictMVar m (Map Word64 (Cursor m h)))
  }

-- | 'withOpenSession' ensures that the session stays open for the duration of the
-- provided continuation.
--
-- NOTE: any operation except 'sessionClose' can use this function.
{-# INLINE withOpenSession #-}
withOpenSession ::
     m ~ IO -- TODO: replace by @io-classes@ constraints for IO simulation.
  => Session m h
  -> (SessionEnv m h -> m a)
  -> m a
withOpenSession sesh action = RW.withReadAccess (sessionState sesh) $ \case
    SessionClosed -> throwIO ErrSessionClosed
    SessionOpen seshEnv -> action seshEnv

--
-- Implementation of public API
--

{-# SPECIALISE withSession :: Tracer IO LSMTreeTrace -> HasFS IO h -> HasBlockIO IO h -> FsPath -> (Session IO h -> IO a) -> IO a #-}
-- | See 'Database.LSMTree.Common.withSession'.
withSession ::
     m ~ IO -- TODO: replace by @io-classes@ constraints for IO simulation.
  => Tracer IO LSMTreeTrace
  -> HasFS m h
  -> HasBlockIO m h
  -> FsPath
  -> (Session m h -> m a)
  -> m a
withSession tr hfs hbio dir = bracket (openSession tr hfs hbio dir) closeSession

{-# SPECIALISE openSession :: Tracer IO LSMTreeTrace -> HasFS IO h -> HasBlockIO IO h -> FsPath -> IO (Session IO h) #-}
-- | See 'Database.LSMTree.Common.openSession'.
openSession ::
     forall m h. m ~ IO -- TODO: replace by @io-classes@ constraints for IO simulation.
  => Tracer m LSMTreeTrace
  -> HasFS m h
  -> HasBlockIO m h -- TODO: could we prevent the user from having to pass this in?
  -> FsPath -- ^ Path to the session directory
  -> m (Session m h)
openSession tr hfs hbio dir = do
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
    bracketOnError
      acquireLock
      releaseLock
      $ \case
          Left e
            | FS.FsResourceAlreadyInUse <- FS.fsErrorType e
            , fsep@(FsErrorPath _ fsp) <- FS.fsErrorPath e
            , fsp == lockFilePath
            -> throwIO (SessionDirLocked fsep)
          Left  e -> throwIO e -- rethrow unexpected errors
          Right Nothing -> throwIO (SessionDirLocked (FS.mkFsErrorPath hfs lockFilePath))
          Right (Just sessionFileLock) ->
            if Set.null dirContents then newSession sessionFileLock
                                    else restoreSession sessionFileLock
  where
    root             = Paths.SessionRoot dir
    lockFilePath     = Paths.lockFile root
    activeDirPath    = Paths.activeDir root
    snapshotsDirPath = Paths.snapshotsDir root

    acquireLock = try @m @FsError $ FS.tryLockFile hbio lockFilePath FS.ExclusiveLock

    releaseLock lockFile = forM_ (Compose lockFile) $ \lockFile' -> FS.hUnlock lockFile'

    mkSession lockFile x = do
        counterVar <- newUniqCounter x
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

    newSession sessionFileLock = do
        traceWith tr TraceNewSession
        FS.createDirectory hfs activeDirPath
        FS.createDirectory hfs snapshotsDirPath
        mkSession sessionFileLock 0

    restoreSession sessionFileLock = do
        traceWith tr TraceRestoreSession
        -- If the layouts are wrong, we throw an exception, and the lock file
        -- is automatically released by bracketOnError.
        checkTopLevelDirLayout
        checkActiveDirLayout
        checkSnapshotsDirLayout
        -- TODO: remove once we have proper snapshotting. Before that, we must
        -- prevent name clashes with runs that are still present in the active
        -- directory by starting the unique counter at a strictly higher number
        -- than the name of any run in the active directory. When we do
        -- snapshoting properly, then we'll hard link files into the active
        -- directory under new names/numbers, and so session counters will
        -- always be able to start at 0.
        files <- FS.listDirectory hfs activeDirPath
        let (x :: Int) | Set.null files = 0
                       -- TODO: read is not very robust, but it is only a
                       -- temporary solution
                       | otherwise = maximum [ read (takeWhile isNumber f)
                                             | f <- Set.toList files ]
        mkSession sessionFileLock (fromIntegral x)

    -- Check that the active directory and snapshots directory exist. We assume
    -- the lock file already exists at this point.
    --
    -- This does /not/ check that /only/ the expected files and directories
    -- exist. This means that unexpected files in the top-level directory are
    -- ignored for the layout check.
    checkTopLevelDirLayout = do
      FS.doesDirectoryExist hfs activeDirPath >>= \b ->
        unless b $ throwIO (SessionDirMalformed (FS.mkFsErrorPath hfs activeDirPath))
      FS.doesDirectoryExist hfs snapshotsDirPath >>= \b ->
        unless b $ throwIO (SessionDirMalformed (FS.mkFsErrorPath hfs snapshotsDirPath))

    -- Nothing to check: runs are verified when loading a table handle, not when
    -- a session is restored.
    --
    -- TODO: when we implement proper snapshotting, the files in the active
    -- directory should be ignored and cleaned up.
    checkActiveDirLayout = pure ()

    -- Nothing to check: snapshots are verified when they are loaded, not when a
    -- session is restored.
    checkSnapshotsDirLayout = pure ()

{-# SPECIALISE closeSession :: Session IO h -> IO () #-}
-- | See 'Database.LSMTree.Common.closeSession'.
--
-- A session's global resources will only be released once it is sure that no
-- tables are open anymore.
closeSession ::
     m ~ IO -- TODO: replace by @io-classes@ constraints for IO simulation.
  => Session m h
  -> m ()
closeSession Session{sessionState, sessionTracer} = do
    traceWith sessionTracer TraceCloseSession
    RW.withWriteAccess_ sessionState $ \case
      SessionClosed -> pure SessionClosed
      SessionOpen seshEnv -> do
        -- Close tables and cursors first, so that we know none are open when we
        -- release session-wide resources.
        --
        -- If any has been closed already by a different thread, the idempotent
        -- 'close' will act like a no-op, and so we are not in trouble.
        --
        -- Since we have a write lock on the session state, we know that no
        -- tables will be added while we are closing the session, and that we
        -- are the only thread currently closing the session.
        --
        -- We technically don't have to overwrite this with an empty Map, but
        -- why not.
        --
        -- TODO: use TempRegistry
        cursors <- modifyMVar (sessionOpenCursors seshEnv) (\m -> pure (Map.empty, m))
        mapM_ closeCursor cursors
        tables <- modifyMVar (sessionOpenTables seshEnv) (\m -> pure (Map.empty, m))
        mapM_ close tables
        FS.close (sessionHasBlockIO seshEnv)
        FS.hUnlock (sessionLockFile seshEnv)
        pure SessionClosed

{-------------------------------------------------------------------------------
  Table handle
-------------------------------------------------------------------------------}

-- | A handle to an on-disk key\/value table.
--
-- For more information, see 'Database.LSMTree.Normal.TableHandle'.
data TableHandle m h = TableHandle {
      tableConfig             :: !TableConfig
      -- | The primary purpose of this 'RWVar' is to ensure consistent views of
      -- the open-/closedness of a table when multiple threads require access to
      -- the table's fields (see 'withOpenTable'). We use more fine-grained
      -- synchronisation for various mutable parts of an open table.
    , tableHandleState        :: !(RWVar m (TableHandleState m h))
    , tableHandleArenaManager :: !(ArenaManager (PrimState m))
    , tableTracer             :: !(Tracer m TableTrace)
    }

instance NFData (TableHandle m h) where
  rnf (TableHandle a b c d) =
    rnf a `seq` rnf b `seq` rnf c `seq` rwhnf d

-- | A table handle may assume that its corresponding session is still open as
-- long as the table handle is open. A session's global resources, and therefore
-- resources that are inherited by the table, will only be released once the
-- session is sure that no tables are open anymore.
data TableHandleState m h =
    TableHandleOpen !(TableHandleEnv m h)
  | TableHandleClosed

data TableHandleEnv m h = TableHandleEnv {
    -- === Session-inherited

    -- | The session that this table belongs to.
    --
    -- NOTE: Consider using the 'tableSessionEnv' field and helper functions
    -- like 'tableHasFS' instead of acquiring the session lock.
    tableSession    :: !(Session m h)
    -- | Use this instead of 'tableSession' for easy access. An open table may
    -- assume that its session is open.
  , tableSessionEnv :: !(SessionEnv m h)

    -- === Table-specific

    -- | Session-unique identifier for this table.
  , tableId         :: !Word64
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
tableSessionRoot :: TableHandleEnv m h -> SessionRoot
tableSessionRoot = sessionRoot . tableSessionEnv

{-# INLINE tableHasFS #-}
-- | Inherited from session for ease of access.
tableHasFS :: TableHandleEnv m h -> HasFS m h
tableHasFS = sessionHasFS . tableSessionEnv

{-# INLINE tableHasBlockIO #-}
-- | Inherited from session for ease of access.
tableHasBlockIO :: TableHandleEnv m h -> HasBlockIO m h
tableHasBlockIO = sessionHasBlockIO . tableSessionEnv

{-# INLINE tableSessionUniqCounter #-}
-- | Inherited from session for ease of access.
tableSessionUniqCounter :: TableHandleEnv m h -> UniqCounter m
tableSessionUniqCounter = sessionUniqCounter . tableSessionEnv

{-# INLINE tableSessionUntrackTable #-}
-- | Open tables are tracked in the corresponding session, so when a table is
-- closed it should become untracked (forgotten).
tableSessionUntrackTable :: MonadMVar m => TableHandleEnv m h -> m ()
tableSessionUntrackTable thEnv =
    modifyMVar_ (sessionOpenTables (tableSessionEnv thEnv)) $ pure . Map.delete (tableId thEnv)

-- | 'withOpenTable' ensures that the table stays open for the duration of the
-- provided continuation.
--
-- NOTE: any operation except 'close' can use this function.
{-# INLINE withOpenTable #-}
withOpenTable ::
     m ~ IO -- TODO: replace by @io-classes@ constraints for IO simulation.
  => TableHandle m h
  -> (TableHandleEnv m h -> m a)
  -> m a
withOpenTable th action = RW.withReadAccess (tableHandleState th) $ \case
    TableHandleClosed -> throwIO ErrTableClosed
    TableHandleOpen thEnv -> action thEnv

--
-- Implementation of public API
--

{-# SPECIALISE withTable :: Session IO h -> TableConfig -> (TableHandle IO h -> IO a) -> IO a #-}
-- | See 'Database.LSMTree.Normal.withTable'.
withTable ::
     m ~ IO -- TODO: replace by @io-classes@ constraints for IO simulation.
  => Session m h
  -> TableConfig
  -> (TableHandle m h -> m a)
  -> m a
withTable sesh conf = bracket (new sesh conf) close

{-# SPECIALISE new :: Session IO h -> TableConfig -> IO (TableHandle IO h) #-}
-- | See 'Database.LSMTree.Normal.new'.
new ::
     m ~ IO -- TODO: replace by @io-classes@ constraints for IO simulation.
  => Session m h
  -> TableConfig
  -> m (TableHandle m h)
new sesh conf = do
    traceWith (sessionTracer sesh) TraceNewTable
    withOpenSession sesh $ \seshEnv -> do
      am <- newArenaManager
      newWith sesh seshEnv conf am WB.empty V.empty

{-# SPECIALISE newWith :: Session IO h -> SessionEnv IO h -> TableConfig -> ArenaManager RealWorld -> WriteBuffer -> Levels IO (Handle h) -> IO (TableHandle IO h) #-}
newWith ::
     m ~ IO -- TODO: replace by @io-classes@ constraints for IO simulation.
  => Session m h
  -> SessionEnv m h
  -> TableConfig
  -> ArenaManager (PrimState m)
  -> WriteBuffer
  -> Levels m (Handle h)
  -> m (TableHandle m h)
newWith sesh seshEnv conf !am !wb !levels = do
    tableId <- incrUniqCounter (sessionUniqCounter seshEnv)
    let tr = TraceTable (uniqueToWord64 tableId) `contramap` sessionTracer sesh
    traceWith tr $ TraceCreateTableHandle conf
    -- The session is kept open until we've updated the session's set of tracked
    -- tables. If 'closeSession' is called by another thread while this code
    -- block is being executed, that thread will block until it reads the
    -- /updated/ set of tracked tables.
    contentVar <- RW.new $ TableContent wb levels (mkLevelsCache levels)
    tableVar <- RW.new $ TableHandleOpen $ TableHandleEnv {
          tableSession = sesh
        , tableSessionEnv = seshEnv
        , tableId = uniqueToWord64 tableId
        , tableContent = contentVar
        }
    let !th = TableHandle conf tableVar am tr
    -- Track the current table
    modifyMVar_ (sessionOpenTables seshEnv) $ pure . Map.insert (uniqueToWord64 tableId) th
    pure $! th

{-# SPECIALISE close :: TableHandle IO h -> IO () #-}
-- | See 'Database.LSMTree.Normal.close'.
close ::
     m ~ IO  -- TODO: replace by @io-classes@ constraints for IO simulation.
  => TableHandle m h
  -> m ()
close th = do
    traceWith (tableTracer th) TraceCloseTable
    RW.withWriteAccess_ (tableHandleState th) $ \case
      TableHandleClosed -> pure TableHandleClosed
      TableHandleOpen thEnv -> do
        -- Since we have a write lock on the table state, we know that we are the
        -- only thread currently closing the table. We can safely make the session
        -- forget about this table.
        -- TODO: use TempRegistry
        tableSessionUntrackTable thEnv
        RW.withWriteAccess_ (tableContent thEnv) $ \lvls -> do
          closeLevels (tableLevels lvls)
          pure lvls
        pure TableHandleClosed

{-# SPECIALISE lookups :: ResolveSerialisedValue -> V.Vector SerialisedKey -> TableHandle IO h -> (Maybe (Entry SerialisedValue (BlobRef (Run IO (Handle h)))) -> lookupResult) -> IO (V.Vector lookupResult) #-}
-- | See 'Database.LSMTree.Normal.lookups'.
lookups ::
     m ~ IO -- TODO: replace by @io-classes@ constraints for IO simulation.
  => ResolveSerialisedValue
  -> V.Vector SerialisedKey
  -> TableHandle m h
  -> (Maybe (Entry SerialisedValue (BlobRef (Run m (Handle h)))) -> lookupResult)
     -- ^ How to map from an entry to a lookup result.
  -> m (V.Vector lookupResult)
lookups resolve ks th fromEntry = do
    traceWith (tableTracer th) $ TraceLookups (V.length ks)
    withOpenTable th $ \thEnv -> do
      let arenaManager = tableHandleArenaManager th
      RW.withReadAccess (tableContent thEnv) $ \tableContent -> do
        let !wb = tableWriteBuffer tableContent
        let !cache = tableCache tableContent
        ioRes <-
          lookupsIO
            (tableHasBlockIO thEnv)
            arenaManager
            resolve
            (cachedRuns cache)
            (cachedFilters cache)
            (cachedIndexes cache)
            (cachedKOpsFiles cache)
            ks
        pure $! V.zipWithStrict
                  (\k1 e2 -> fromEntry $ combineMaybe resolve (WB.lookup wb k1) e2)
                  ks ioRes

{-# SPECIALISE updates :: ResolveSerialisedValue -> V.Vector (SerialisedKey, Entry SerialisedValue SerialisedBlob) -> TableHandle IO h -> IO () #-}
-- | See 'Database.LSMTree.Normal.updates'.
--
-- Does not enforce that mupsert and blobs should not occur in the same table.
updates ::
     m ~ IO -- TODO: replace by @io-classes@ constraints for IO simulation.
  => ResolveSerialisedValue
  -> V.Vector (SerialisedKey, Entry SerialisedValue SerialisedBlob)
  -> TableHandle m h
  -> m ()
updates resolve es th = do
    traceWith (tableTracer th) $ TraceUpdates (V.length es)
    let conf = tableConfig th
    withOpenTable th $ \thEnv -> do
      let hfs = tableHasFS thEnv
      modifyWithTempRegistry_
        (atomically $ RW.unsafeAcquireWriteAccess (tableContent thEnv))
        (atomically . RW.unsafeReleaseWriteAccess (tableContent thEnv)) $ \tc -> do
          updatesWithInterleavedFlushes
            (TraceMerge `contramap` tableTracer th)
            conf
            resolve
            hfs
            (tableHasBlockIO thEnv)
            (tableSessionRoot thEnv)
            (tableSessionUniqCounter thEnv)
            es
            tc

{-------------------------------------------------------------------------------
  Cursors
-------------------------------------------------------------------------------}

-- | A read-only view into the table state at the time of cursor creation.
--
-- For more information, see 'Database.LSMTree.Normal.Cursor'.
--
-- The representation of a cursor is similar to that of a 'TableHandle', but
-- simpler, as it is read-only.
data Cursor m h = Cursor {
      -- | Mutual exclusion, only a single thread can read from a cursor at a
      -- given time.
      cursorState :: !(StrictMVar m (CursorState m h))
    }

data CursorState m h =
    CursorOpen !(CursorEnv m h)
  | CursorClosed  -- ^ Calls to a closed cursor raise an exception.

data CursorEnv m h = CursorEnv {
    -- === Session-inherited

    -- | The session that this cursor belongs to.
    --
    -- NOTE: Consider using the 'cursorSessionEnv' field instead of acquiring
    -- the session lock.
    cursorSession     :: !(Session m h)
    -- | Use this instead of 'cursorSession' for easy access. An open cursor may
    -- assume that its session is open. A session's global resources, and
    -- therefore resources that are inherited by the cursor, will only be
    -- released once the session is sure that no cursors are open anymore.
  , cursorSessionEnv  :: !(SessionEnv m h)

    -- === Cursor-specific

    -- | Session-unique identifier for this cursor.
  , cursorId          :: !Word64
  , cursorWriteBuffer :: !WriteBuffer
    -- | Readers are immediately discarded once they are 'Readers.Drained'.
    -- However, the reference counts to the runs only get removed when calling
    -- 'closeCursor', as there might still be 'BlobRef's that need the
    -- corresponding run to stay alive.
  , cursorReaders     :: !(Maybe (Readers.Readers (PrimState m) (Handle h)))
    -- | The runs held open by the cursor. We must remove a reference when the
    -- cursor gets closed.
  , cursorRuns        :: !(V.Vector (Run m (Handle h)))
  }

{-# SPECIALISE withCursor :: TableHandle IO h -> (Cursor IO h -> IO a) -> IO a #-}
-- | See 'Database.LSMTree.Normal.withCursor'.
withCursor ::
     m ~ IO -- TODO: replace by @io-classes@ constraints for IO simulation.
  => TableHandle m h
  -> (Cursor m h -> m a)
  -> m a
withCursor th = bracket (newCursor th) closeCursor

{-# SPECIALISE newCursor :: TableHandle IO h -> IO (Cursor IO h) #-}
-- | See 'Database.LSMTree.Normal.newCursor'.
newCursor ::
     m ~ IO -- TODO: replace by @io-classes@ constraints for IO simulation.
  => TableHandle m h
  -> m (Cursor m h)
newCursor th = withOpenTable th $ \thEnv -> do
    let cursorSession = tableSession thEnv
    let cursorSessionEnv = tableSessionEnv thEnv
    let hfs = tableHasFS thEnv
    let hbio = tableHasBlockIO thEnv
    -- We acquire a read-lock on the session open-state to prevent races, see
    -- 'sessionOpenTables'.
    withOpenSession cursorSession $ \_ -> do
      withTempRegistry $ \reg -> do
        (cursorWriteBuffer, cursorRuns) <-
          allocTableContent reg (tableContent thEnv)
        cursorReaders <-
          allocateMaybeTemp reg
            (Readers.new hfs hbio (V.toList cursorRuns))
            (Readers.close hfs hbio)
        cursorId0 <- incrUniqCounter (sessionUniqCounter cursorSessionEnv)
        let cursorId = uniqueToWord64 cursorId0
        cursor <- Cursor <$> newMVar (CursorOpen CursorEnv {..})
        -- Track cursor, but careful: If now an exception is raised, all
        -- resources get freed by the registry, so if the session still
        -- tracks 'cursor' (which is 'CursorOpen'), it later double frees.
        -- Therefore, we only track the cursor if 'withTempRegistry' exits
        -- successfully, i.e. using 'freeTemp'.
        freeTemp reg $
          modifyMVar_ (sessionOpenCursors cursorSessionEnv) $
            pure . Map.insert cursorId cursor
        pure $! cursor
  where
    -- The table contents escape the read access, but we just added
    -- references to each run, so it is safe.
    allocTableContent reg contentVar = do
        RW.withReadAccess contentVar $ \content -> do
          let runs = cachedRuns (tableCache content)
          V.forM_ runs $ \r -> do
            allocateTemp reg
              (Run.addReference r)
              (\_ -> Run.removeReference r)
          pure (tableWriteBuffer content, runs)

{-# SPECIALISE closeCursor :: Cursor IO h -> IO () #-}
-- | See 'Database.LSMTree.Normal.closeCursor'.
closeCursor ::
     m ~ IO -- TODO: replace by @io-classes@ constraints for IO simulation.
  => Cursor m h
  -> m ()
closeCursor Cursor {..} = do
    modifyWithTempRegistry_ (takeMVar cursorState) (putMVar cursorState) $ \reg -> \case
      CursorClosed -> return CursorClosed
      CursorOpen CursorEnv {..} -> do
        let hfs = sessionHasFS cursorSessionEnv
        let hbio = sessionHasBlockIO cursorSessionEnv

        -- This should be safe-ish, but it's still not ideal, because it doesn't
        -- rule out sync exceptions in the cleanup operations.
        -- In that case, the cursor ends up closed, but resources might not have
        -- been freed. Probably better than the other way around, though.
        freeTemp reg $
          modifyMVar_ (sessionOpenCursors cursorSessionEnv) $
            pure . Map.delete cursorId

        forM_ cursorReaders $ freeTemp reg . Readers.close hfs hbio
        V.forM_ cursorRuns $ freeTemp reg . Run.removeReference
        return CursorClosed


{-------------------------------------------------------------------------------
  Snapshots
-------------------------------------------------------------------------------}

type SnapshotLabel = String

{-# SPECIALISE snapshot :: ResolveSerialisedValue -> SnapshotName -> String -> TableHandle IO h -> IO Int #-}
-- |  See 'Database.LSMTree.Normal.snapshot''.
snapshot ::
     m ~ IO -- TODO: replace by @io-classes@ constraints for IO simulation.
  => ResolveSerialisedValue
  -> SnapshotName
  -> SnapshotLabel
  -> TableHandle m h
  -> m Int
snapshot resolve snap label th = do
    traceWith (tableTracer th) $ TraceSnapshot snap
    let conf = tableConfig th
    withOpenTable th $ \thEnv -> do
      -- For the temporary implementation it is okay to just flush the buffer
      -- before taking the snapshot.
      let hfs = tableHasFS thEnv
      content <- modifyWithTempRegistry
                    (atomically $ RW.unsafeAcquireWriteAccess (tableContent thEnv))
                    (atomically . RW.unsafeReleaseWriteAccess (tableContent thEnv))
                    $ \reg content -> do
        r <- flushWriteBuffer
              (TraceMerge `contramap` tableTracer th)
              conf
              resolve
              hfs
              (tableHasBlockIO thEnv)
              (tableSessionRoot thEnv)
              (tableSessionUniqCounter thEnv)
              reg
              content
        pure (r, r)
      -- At this point, we've flushed the write buffer but we haven't created the
      -- snapshot file yet. If an asynchronous exception happens beyond this
      -- point, we'll take that loss, as the inner state of the table is still
      -- consistent.
      let runNumbers = V.map (\(Level mr rs) ->
                                ( case mr of
                                    SingleRun r -> (True, runNumber (Run.runRunFsPaths r))
                                    MergingRun (CompletedMerge r) -> (False, runNumber (Run.runRunFsPaths r))
                                , V.map (runNumber . Run.runRunFsPaths) rs)) $
                          tableLevels content
          snapPath = Paths.snapshot (tableSessionRoot thEnv) snap
      FS.doesFileExist (tableHasFS thEnv) snapPath >>= \b ->
              when b $ throwIO (ErrSnapshotExists snap)
      FS.withFile
        (tableHasFS thEnv)
        snapPath
        (FS.WriteMode FS.MustBeNew) $ \h ->
          void $ FS.hPutAllStrict (tableHasFS thEnv) h
                      (BSC.pack $ show (label, runNumbers, tableConfig th))
      pure $! V.sum (V.map (\((_ :: (Bool, RunNumber)), rs) -> 1 + V.length rs) runNumbers)

{-# SPECIALISE open :: Session IO h -> SnapshotLabel -> TableConfigOverride -> SnapshotName -> IO (TableHandle IO h) #-}
-- |  See 'Database.LSMTree.Normal.open'.
open ::
     m ~ IO -- TODO: replace by @io-classes@ constraints for IO simulation.
  => Session m h
  -> SnapshotLabel -- ^ Expected label
  -> TableConfigOverride -- ^ Optional config override
  -> SnapshotName
  -> m (TableHandle m h)
open sesh label override snap = do
    traceWith (sessionTracer sesh) $ TraceOpenSnapshot snap override
    withOpenSession sesh $ \seshEnv -> do
      withTempRegistry $ \reg -> do
        let hfs      = sessionHasFS seshEnv
            hbio     = sessionHasBlockIO seshEnv
            snapPath = Paths.snapshot (sessionRoot seshEnv) snap
        FS.doesFileExist hfs snapPath >>= \b ->
          unless b $ throwIO (ErrSnapshotNotExists snap)
        bs <- FS.withFile
                hfs
                snapPath
                FS.ReadMode $ \h ->
                  FS.hGetAll (sessionHasFS seshEnv) h
        let (label', runNumbers, conf) =
                -- why we are using read for this?
                -- apparently this is a temporary solution, to be done properly in WP15
                read @(SnapshotLabel, V.Vector ((Bool, RunNumber), V.Vector RunNumber), TableConfig) $
                BSC.unpack $ BSC.toStrict $ bs

        let conf' = applyOverride override conf
        unless (label == label') $ throwIO (ErrSnapshotWrongType snap)
        let runPaths = V.map (bimap (second $ RunFsPaths (Paths.activeDir $ sessionRoot seshEnv))
                                    (V.map (RunFsPaths (Paths.activeDir $ sessionRoot seshEnv))))
                            runNumbers
        lvls <- openLevels reg hfs hbio (confDiskCachePolicy conf') runPaths
        am <- newArenaManager
        newWith sesh seshEnv conf' am WB.empty lvls

{-# SPECIALISE openLevels :: TempRegistry IO -> HasFS IO h -> HasBlockIO IO h -> DiskCachePolicy -> V.Vector ((Bool, RunFsPaths), V.Vector RunFsPaths) -> IO (Levels IO (FS.Handle h)) #-}
-- | Open multiple levels.
openLevels ::
     m ~ IO -- TODO: replace by @io-classes@ constraints for IO simulation.
  => TempRegistry m
  -> HasFS m h
  -> HasBlockIO m h
  -> DiskCachePolicy
  -> V.Vector ((Bool, RunFsPaths), V.Vector RunFsPaths)
  -> m (Levels m (Handle h))
openLevels reg hfs hbio diskCachePolicy levels =
    flip V.imapMStrict levels $ \i (mrPath, rsPaths) -> do
      let ln      = LevelNo (i+1) -- level 0 is the write buffer
          caching = diskCachePolicyForLevel diskCachePolicy ln
      !r <- allocateTemp reg
              (Run.openFromDisk hfs hbio caching (snd mrPath))
              Run.removeReference
      !rs <- flip V.mapMStrict rsPaths $ \run ->
        allocateTemp reg
          (Run.openFromDisk hfs hbio caching run)
          Run.removeReference
      let !mr = if fst mrPath then SingleRun r else MergingRun (CompletedMerge r)
      pure $! Level mr rs

{-# SPECIALISE deleteSnapshot :: Session IO h -> SnapshotName -> IO () #-}
-- |  See 'Database.LSMTree.Common.deleteSnapshot'.
deleteSnapshot ::
     m ~ IO -- TODO: replace by @io-classes@ constraints for IO simulation.
  => Session m h
  -> SnapshotName
  -> m ()
deleteSnapshot sesh snap = do
    traceWith (sessionTracer sesh) $ TraceDeleteSnapshot snap
    withOpenSession sesh $ \seshEnv -> do
      let hfs = sessionHasFS seshEnv
          snapPath = Paths.snapshot (sessionRoot seshEnv) snap
      FS.doesFileExist hfs snapPath >>= \b ->
        unless b $ throwIO (ErrSnapshotNotExists snap)
      FS.removeFile hfs snapPath

{-# SPECIALISE listSnapshots :: Session IO h -> IO [SnapshotName] #-}
-- |  See 'Database.LSMTree.Common.listSnapshots'.
listSnapshots ::
     m ~ IO -- TODO: replace by @io-classes@ constraints for IO simulation.
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
          -- check that it is a file
          b <- FS.doesFileExist hfs (Paths.snapshot root snap)
          if b then pure $ Just snap
               else pure $ Nothing

{-------------------------------------------------------------------------------
  Mutiple writable table handles
-------------------------------------------------------------------------------}

{-# SPECIALISE duplicate :: TableHandle IO h -> IO (TableHandle IO h) #-}
-- | See 'Database.LSMTree.Normal.duplicate'.
duplicate ::
     m ~ IO -- TODO: replace by @io-classes@ constraints for IO simulation.
  => TableHandle m h
  -> m (TableHandle m h)
duplicate th = do
    traceWith (tableTracer th) TraceDuplicate
    withOpenTable th $ \thEnv -> do
      -- We acquire a read-lock on the session open-state to prevent races, see
      -- 'sessionOpenTables'.
      withOpenSession (tableSession thEnv) $ \_ -> do
        withTempRegistry $ \reg -> do
          -- The table contents escape the read access, but we just added references
          -- to each run so it is safe.
          content <- RW.withReadAccess (tableContent thEnv) $ \content -> do
            V.forM_ (runsInLevels (tableLevels content)) $ \r -> do
              allocateTemp reg
                (Run.addReference r)
                (\_ -> Run.removeReference r)
            pure content
          -- TODO: Fix possible double-free! See 'newCursor'.
          -- In `newWith`, the table handle (in the open state) gets added to
          -- `sessionOpenTables', even if later an async exception occurs and
          -- the temp registry rolls back all allocations.
          newWith
            (tableSession thEnv)
            (tableSessionEnv thEnv)
            (tableConfig th)
            (tableHandleArenaManager th)
            (tableWriteBuffer content)
            (tableLevels content)
