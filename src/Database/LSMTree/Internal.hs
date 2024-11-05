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
  , snapshot
  , open
  , deleteSnapshot
  , listSnapshots
    -- * Mutiple writable tables
  , duplicate
  ) where

import           Codec.CBOR.Read
import           Control.Concurrent.Class.MonadMVar.Strict
import           Control.Concurrent.Class.MonadSTM (MonadSTM (..))
import           Control.Concurrent.Class.MonadSTM.RWVar (RWVar)
import qualified Control.Concurrent.Class.MonadSTM.RWVar as RW
import           Control.DeepSeq
import           Control.Monad (unless, void, when)
import           Control.Monad.Class.MonadST (MonadST (..))
import           Control.Monad.Class.MonadThrow
import           Control.Monad.Fix (MonadFix)
import           Control.Monad.Primitive
import           Control.TempRegistry
import           Control.Tracer
import           Data.Arena (ArenaManager, newArenaManager)
import           Data.Char (isNumber)
import           Data.Foldable
import           Data.Functor.Compose (Compose (..))
import           Data.Kind
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import           Data.Maybe (catMaybes)
import qualified Data.Primitive.ByteArray as P
import qualified Data.Set as Set
import           Data.Typeable
import qualified Data.Vector as V
import           Data.Word (Word64)
import           Database.LSMTree.Internal.BlobRef (WeakBlobRef (..))
import qualified Database.LSMTree.Internal.BlobRef as BlobRef
import           Database.LSMTree.Internal.Config
import qualified Database.LSMTree.Internal.Cursor as Cursor
import           Database.LSMTree.Internal.Entry (Entry, unNumEntries)
import           Database.LSMTree.Internal.Lookup (ByteCountDiscrepancy,
                     ResolveSerialisedValue, lookupsIO)
import           Database.LSMTree.Internal.MergeSchedule
import           Database.LSMTree.Internal.Paths (SessionRoot (..),
                     SnapshotName)
import qualified Database.LSMTree.Internal.Paths as Paths
import           Database.LSMTree.Internal.Range (Range (..))
import qualified Database.LSMTree.Internal.RawBytes as RB
import           Database.LSMTree.Internal.Run (Run)
import qualified Database.LSMTree.Internal.Run as Run
import           Database.LSMTree.Internal.RunReaders (OffsetKey (..))
import qualified Database.LSMTree.Internal.RunReaders as Readers
import           Database.LSMTree.Internal.Serialise (SerialisedBlob (..),
                     SerialisedKey, SerialisedValue)
import           Database.LSMTree.Internal.Snapshot
import           Database.LSMTree.Internal.UniqCounter
import qualified Database.LSMTree.Internal.WriteBuffer as WB
import qualified Database.LSMTree.Internal.WriteBufferBlobs as WBB
import qualified System.FS.API as FS
import           System.FS.API (FsError, FsErrorPath (..), FsPath, HasFS)
import qualified System.FS.API.Lazy as FS
import qualified System.FS.BlockIO.API as FS
import           System.FS.BlockIO.API (HasBlockIO)

{-------------------------------------------------------------------------------
  Existentials
-------------------------------------------------------------------------------}

type Session' :: (Type -> Type) -> Type
data Session' m = forall h. Typeable h => Session' !(Session m h)

instance NFData (Session' m) where
  rnf (Session' s) = rnf s

type NormalTable :: (Type -> Type) -> Type -> Type -> Type -> Type
data NormalTable m k v b = forall h. Typeable h =>
    NormalTable !(Table m h)

instance NFData (NormalTable m k v b) where
  rnf (NormalTable t) = rnf t

type NormalCursor :: (Type -> Type) -> Type -> Type -> Type -> Type
data NormalCursor m k v blob = forall h. Typeable h =>
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
  | TraceTable
      Word64 -- ^ Table identifier
      TableTrace
  | TraceDeleteSnapshot SnapshotName
  | TraceListSnapshots
    -- Cursor
  | TraceCursor
      Word64 -- ^ Cursor identifier
      CursorTrace
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
  deriving stock Show

data CursorTrace =
    TraceCreateCursor
      Word64 -- ^ Table identifier
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
  , sessionOpenTables  :: !(StrictMVar m (Map Word64 (Table m h)))
    -- | Similarly to tables, open cursors are tracked so they can be closed
    -- once the session is closed. See 'sessionOpenTables'.
  , sessionOpenCursors :: !(StrictMVar m (Map Word64 (Cursor m h)))
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
     (MonadCatch m, MonadSTM m, MonadMVar m)
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

    -- Nothing to check: runs are verified when loading a table, not when
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
     (MonadMask m, MonadSTM m, MonadMVar m, PrimMonad m)
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
    }

instance NFData (Table m h) where
  rnf (Table a b c d) =
    rnf a `seq` rnf b `seq` rnf c `seq` rwhnf d

-- | A table may assume that its corresponding session is still open as
-- long as the table is open. A session's global resources, and therefore
-- resources that are inherited by the table, will only be released once the
-- session is sure that no tables are open anymore.
data TableState m h =
    TableOpen !(TableEnv m h)
  | TableClosed

data TableEnv m h = TableEnv {
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
{-# SPECIALISE tableSessionUntrackTable :: TableEnv IO h -> IO () #-}
-- | Open tables are tracked in the corresponding session, so when a table is
-- closed it should become untracked (forgotten).
tableSessionUntrackTable :: MonadMVar m => TableEnv m h -> m ()
tableSessionUntrackTable thEnv =
    modifyMVar_ (sessionOpenTables (tableSessionEnv thEnv)) $ pure . Map.delete (tableId thEnv)

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
    TableOpen thEnv -> action thEnv

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
      withTempRegistry $ \reg -> do
        am <- newArenaManager
        blobpath <- Paths.tableBlobPath (sessionRoot seshEnv) <$>
                      incrUniqCounter (sessionUniqCounter seshEnv)
        tableWriteBufferBlobs
          <- allocateTemp reg
               (WBB.new (sessionHasFS seshEnv) blobpath)
               WBB.removeReference
        let tableWriteBuffer = WB.empty
            tableLevels = V.empty
        tableCache <- mkLevelsCache reg tableLevels
        let tc = TableContent {
                tableWriteBuffer
              , tableWriteBufferBlobs
              , tableLevels
              , tableCache
              }
        newWith reg sesh seshEnv conf am tc

{-# SPECIALISE newWith ::
     TempRegistry IO
  -> Session IO h
  -> SessionEnv IO h
  -> TableConfig
  -> ArenaManager RealWorld
  -> TableContent IO h
  -> IO (Table IO h) #-}
newWith ::
     (MonadSTM m, MonadMVar m)
  => TempRegistry m
  -> Session m h
  -> SessionEnv m h
  -> TableConfig
  -> ArenaManager (PrimState m)
  -> TableContent m h
  -> m (Table m h)
newWith reg sesh seshEnv conf !am !tc = do
    tableId <- incrUniqCounter (sessionUniqCounter seshEnv)
    let tr = TraceTable (uniqueToWord64 tableId) `contramap` sessionTracer sesh
    traceWith tr $ TraceCreateTable conf
    -- The session is kept open until we've updated the session's set of tracked
    -- tables. If 'closeSession' is called by another thread while this code
    -- block is being executed, that thread will block until it reads the
    -- /updated/ set of tracked tables.
    contentVar <- RW.new $ tc
    tableVar <- RW.new $ TableOpen $ TableEnv {
          tableSession = sesh
        , tableSessionEnv = seshEnv
        , tableId = uniqueToWord64 tableId
        , tableContent = contentVar
        }
    let !t = Table conf tableVar am tr
    -- Track the current table
    freeTemp reg $ modifyMVar_ (sessionOpenTables seshEnv)
                 $ pure . Map.insert (uniqueToWord64 tableId) t
    pure $! t

{-# SPECIALISE close :: Table IO h -> IO () #-}
-- | See 'Database.LSMTree.Normal.close'.
close ::
     (MonadMask m, MonadSTM m, MonadMVar m, PrimMonad m)
  => Table m h
  -> m ()
close t = do
    traceWith (tableTracer t) TraceCloseTable
    modifyWithTempRegistry_
      (RW.unsafeAcquireWriteAccess (tableState t))
      (atomically . RW.unsafeReleaseWriteAccess (tableState t)) $ \reg -> \case
      TableClosed -> pure TableClosed
      TableOpen thEnv -> do
        -- Since we have a write lock on the table state, we know that we are the
        -- only thread currently closing the table. We can safely make the session
        -- forget about this table.
        freeTemp reg (tableSessionUntrackTable thEnv)
        RW.withWriteAccess_ (tableContent thEnv) $ \tc -> do
          removeReferenceTableContent reg tc
          pure tc
        pure TableClosed

{-# SPECIALISE lookups ::
     ResolveSerialisedValue
  -> V.Vector SerialisedKey
  -> Table IO h
  -> IO (V.Vector (Maybe (Entry SerialisedValue (WeakBlobRef IO h)))) #-}
-- | See 'Database.LSMTree.Normal.lookups'.
lookups ::
     (MonadST m, MonadSTM m, MonadThrow m)
  => ResolveSerialisedValue
  -> V.Vector SerialisedKey
  -> Table m h
  -> m (V.Vector (Maybe (Entry SerialisedValue (WeakBlobRef m h))))
lookups resolve ks t = do
    traceWith (tableTracer t) $ TraceLookups (V.length ks)
    withOpenTable t $ \thEnv ->
      RW.withReadAccess (tableContent thEnv) $ \tableContent ->
        let !cache = tableCache tableContent in
        lookupsIO
          (tableHasBlockIO thEnv)
          (tableArenaManager t)
          resolve
          (tableWriteBuffer tableContent)
          (tableWriteBufferBlobs tableContent)
          (cachedRuns cache)
          (cachedFilters cache)
          (cachedIndexes cache)
          (cachedKOpsFiles cache)
          ks

{-# SPECIALISE rangeLookup ::
     ResolveSerialisedValue
  -> Range SerialisedKey
  -> Table IO h
  -> (SerialisedKey -> SerialisedValue -> Maybe (WeakBlobRef IO h) -> res)
  -> IO (V.Vector res) #-}
-- | See 'Database.LSMTree.Normal.rangeLookup'.
rangeLookup ::
     (MonadFix m, MonadMask m, MonadMVar m, MonadST m, MonadSTM m)
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
     (MonadFix m, MonadMask m, MonadMVar m, MonadST m, MonadSTM m)
  => ResolveSerialisedValue
  -> V.Vector (SerialisedKey, Entry SerialisedValue SerialisedBlob)
  -> Table m h
  -> m ()
updates resolve es t = do
    traceWith (tableTracer t) $ TraceUpdates (V.length es)
    let conf = tableConfig t
    withOpenTable t $ \thEnv -> do
      let hfs = tableHasFS thEnv
      modifyWithTempRegistry_
        (RW.unsafeAcquireWriteAccess (tableContent thEnv))
        (atomically . RW.unsafeReleaseWriteAccess (tableContent thEnv)) $ \reg -> do
          updatesWithInterleavedFlushes
            (TraceMerge `contramap` tableTracer t)
            conf
            resolve
            hfs
            (tableHasBlockIO thEnv)
            (tableSessionRoot thEnv)
            (tableSessionUniqCounter thEnv)
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
      handle (\(BlobRef.WeakBlobRefInvalid i) -> throwIO (ErrBlobRefInvalid i)) $
      BlobRef.withWeakBlobRefs wrefs $ \refs -> do

        -- Prepare the IOOps:
        -- We use a single large memory buffer, with appropriate offsets within
        -- the buffer.
        let bufSize :: Int
            !bufSize = V.sum (V.map BlobRef.blobRefSpanSize refs)

            {-# INLINE bufOffs #-}
            bufOffs :: V.Vector Int
            bufOffs = V.scanl (+) 0 (V.map BlobRef.blobRefSpanSize refs)
        buf <- P.newPinnedByteArray bufSize
        let ioops = V.zipWith (BlobRef.readBlobIOOp buf) bufOffs refs
            hbio  = sessionHasBlockIO seshEnv

        -- Submit the IOOps all in one go:
        _ <- FS.submitIO hbio ioops
        -- We do not need to inspect the results because IO errors are
        -- thrown as exceptions, and the result is just the read length
        -- which is already known. Short reads can't happen here.

        -- Construct the SerialisedBlobs results:
        -- This is just the different offsets within the shared buffer.
        ba <- P.unsafeFreezeByteArray buf
        pure $! V.zipWith
                  (\off len -> SerialisedBlob (RB.fromByteArray off len ba))
                  bufOffs
                  (V.map BlobRef.blobRefSpanSize refs)

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
  , cursorId         :: !Word64
    -- | Readers are immediately discarded once they are 'Readers.Drained',
    -- so if there is a 'Just', we can assume that it has further entries.
    -- However, the reference counts to the runs only get removed when calling
    -- 'closeCursor', as there might still be 'BlobRef's that need the
    -- corresponding run to stay alive.
  , cursorReaders    :: !(Maybe (Readers.Readers m h))
    -- | The runs held open by the cursor. We must remove a reference when the
    -- cursor gets closed.
  , cursorRuns       :: !(V.Vector (Run m h))

    -- | The write buffer blobs, which like the runs, we have to keep open
    -- untile the cursor is closed.
  , cursorWBB        :: WBB.WriteBufferBlobs m h
  }

{-# SPECIALISE withCursor ::
     OffsetKey
  -> Table IO h
  -> (Cursor IO h -> IO a)
  -> IO a #-}
-- | See 'Database.LSMTree.Normal.withCursor'.
withCursor ::
     (MonadFix m, MonadMask m, MonadMVar m, MonadST m, MonadSTM m)
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
     (MonadFix m, MonadMask m, MonadMVar m, MonadST m, MonadSTM m)
  => OffsetKey
  -> Table m h
  -> m (Cursor m h)
newCursor !offsetKey t = withOpenTable t $ \thEnv -> do
    let cursorSession = tableSession thEnv
    let cursorSessionEnv = tableSessionEnv thEnv
    cursorId <- uniqueToWord64 <$>
      incrUniqCounter (sessionUniqCounter cursorSessionEnv)
    let cursorTracer = TraceCursor cursorId `contramap` sessionTracer cursorSession
    traceWith cursorTracer $ TraceCreateCursor (tableId thEnv)

    -- We acquire a read-lock on the session open-state to prevent races, see
    -- 'sessionOpenTables'.
    withOpenSession cursorSession $ \_ -> do
      withTempRegistry $ \reg -> do
        (wb, wbblobs, cursorRuns) <-
          allocTableContent reg (tableContent thEnv)
        cursorReaders <-
          allocateMaybeTemp reg
            (Readers.new offsetKey (Just (wb, wbblobs)) cursorRuns)
            Readers.close
        let cursorWBB = wbblobs
        cursorState <- newMVar (CursorOpen CursorEnv {..})
        let !cursor = Cursor {cursorState, cursorTracer}
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
          let wb      = tableWriteBuffer content
              wbblobs = tableWriteBufferBlobs content
          allocateTemp reg
            (WBB.addReference wbblobs)
            (\_ -> WBB.removeReference wbblobs)
          let runs = cachedRuns (tableCache content)
          V.forM_ runs $ \r -> do
            allocateTemp reg
              (Run.addReference r)
              (\_ -> Run.removeReference r)
          pure (wb, wbblobs, runs)

{-# SPECIALISE closeCursor :: Cursor IO h -> IO () #-}
-- | See 'Database.LSMTree.Normal.closeCursor'.
closeCursor ::
     (MonadMask m, MonadMVar m, MonadSTM m, PrimMonad m)
  => Cursor m h
  -> m ()
closeCursor Cursor {..} = do
    traceWith cursorTracer $ TraceCloseCursor
    modifyWithTempRegistry_ (takeMVar cursorState) (putMVar cursorState) $ \reg -> \case
      CursorClosed -> return CursorClosed
      CursorOpen CursorEnv {..} -> do
        -- This should be safe-ish, but it's still not ideal, because it doesn't
        -- rule out sync exceptions in the cleanup operations.
        -- In that case, the cursor ends up closed, but resources might not have
        -- been freed. Probably better than the other way around, though.
        freeTemp reg $
          modifyMVar_ (sessionOpenCursors cursorSessionEnv) $
            pure . Map.delete cursorId

        forM_ cursorReaders $ freeTemp reg . Readers.close
        V.forM_ cursorRuns $ freeTemp reg . Run.removeReference
        freeTemp reg (WBB.removeReference cursorWBB)
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
     (MonadFix m, MonadMask m, MonadMVar m, MonadST m, MonadSTM m)
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
     (MonadFix m, MonadMask m, MonadMVar m, MonadST m, MonadSTM m)
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

{-# SPECIALISE snapshot ::
     ResolveSerialisedValue
  -> SnapshotName
  -> SnapshotLabel
  -> SnapshotTableType
  -> Table IO h
  -> IO Int #-}
-- |  See 'Database.LSMTree.Normal.snapshot''.
snapshot ::
     (MonadFix m, MonadMask m, MonadMVar m, MonadST m, MonadSTM m)
  => ResolveSerialisedValue
  -> SnapshotName
  -> SnapshotLabel
  -> SnapshotTableType
  -> Table m h
  -> m Int
snapshot resolve snap label tableType t = do
    traceWith (tableTracer t) $ TraceSnapshot snap
    let conf = tableConfig t
    withOpenTable t $ \thEnv -> do
      let hfs = tableHasFS thEnv
      let snapPath = Paths.snapshot (tableSessionRoot thEnv) snap
      FS.doesFileExist (tableHasFS thEnv) snapPath >>= \b ->
              when b $ throwIO (ErrSnapshotExists snap)

      -- For the temporary implementation it is okay to just flush the buffer
      -- before taking the snapshot.
      content <- modifyWithTempRegistry
                    (RW.unsafeAcquireWriteAccess (tableContent thEnv))
                    (atomically . RW.unsafeReleaseWriteAccess (tableContent thEnv))
                    $ \reg content -> do
        -- TODO: When we flush the buffer here, it might be underfull, which
        -- could mess up the scheduling. The conservative approach is to supply
        -- credits as if the buffer was full, and then flush the (possibly)
        -- underfull buffer. However, note that this bit of code
        -- here is probably going to change anyway because of #392
        supplyCredits conf (Credit $ unNumEntries $ case confWriteBufferAlloc conf of AllocNumEntries x -> x) (tableLevels content)
        content' <- flushWriteBuffer
              (TraceMerge `contramap` tableTracer t)
              conf
              resolve
              hfs
              (tableHasBlockIO thEnv)
              (tableSessionRoot thEnv)
              (tableSessionUniqCounter thEnv)
              reg
              content
        pure (content', content')
      -- At this point, we've flushed the write buffer but we haven't created the
      -- snapshot file yet. If an asynchronous exception happens beyond this
      -- point, we'll take that loss, as the inner state of the table is still
      -- consistent.

      snappedLevels <- snapLevels (tableLevels content)
      let snapContents = encodeSnapshotMetaData (SnapshotMetaData label tableType (tableConfig t) snappedLevels)

      FS.withFile
        (tableHasFS thEnv)
        snapPath
        (FS.WriteMode FS.MustBeNew) $ \h ->
          void $ FS.hPutAll (tableHasFS thEnv) h snapContents

      pure $! numSnapRuns snappedLevels

{-# SPECIALISE open ::
     Session IO h
  -> SnapshotLabel
  -> SnapshotTableType
  -> TableConfigOverride
  -> SnapshotName
  -> ResolveSerialisedValue
  -> IO (Table IO h) #-}
-- |  See 'Database.LSMTree.Normal.open'.
open ::
     (MonadFix m, MonadMask m, MonadMVar m, MonadST m, MonadSTM m)
  => Session m h
  -> SnapshotLabel -- ^ Expected label
  -> SnapshotTableType -- ^ Expected table type
  -> TableConfigOverride -- ^ Optional config override
  -> SnapshotName
  -> ResolveSerialisedValue
  -> m (Table m h)
open sesh label tableType override snap resolve = do
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

        snapMetaData <- case decodeSnapshotMetaData bs of
          Left e  -> throwIO (ErrSnapshotDeserialiseFailure e snap)
          Right x -> pure x
        let SnapshotMetaData label' tableType' conf snappedLevels = snapMetaData

        unless (tableType == tableType') $
          throwIO (ErrSnapshotWrongTableType snap tableType tableType')

        unless (label == label') $
          throwIO (ErrSnapshotWrongLabel snap label label')

        let conf' = applyOverride override conf
        am <- newArenaManager
        blobpath <- Paths.tableBlobPath (sessionRoot seshEnv) <$>
                      incrUniqCounter (sessionUniqCounter seshEnv)
        tableWriteBufferBlobs
          <- allocateTemp reg
               (WBB.new hfs blobpath)
               WBB.removeReference
        tableLevels <- openLevels reg hfs hbio conf (sessionUniqCounter seshEnv) (sessionRoot seshEnv) resolve snappedLevels
        tableCache <- mkLevelsCache reg tableLevels
        newWith reg sesh seshEnv conf' am $! TableContent {
            tableWriteBuffer = WB.empty
          , tableWriteBufferBlobs
          , tableLevels
          , tableCache
          }

{-# SPECIALISE deleteSnapshot ::
     Session IO h
  -> SnapshotName
  -> IO () #-}
-- |  See 'Database.LSMTree.Common.deleteSnapshot'.
deleteSnapshot ::
     (MonadFix m, MonadMask m, MonadSTM m)
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
     (MonadFix m, MonadMask m, MonadSTM m)
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
  Mutiple writable tables
-------------------------------------------------------------------------------}

{-# SPECIALISE duplicate :: Table IO h -> IO (Table IO h) #-}
-- | See 'Database.LSMTree.Normal.duplicate'.
duplicate ::
     (MonadFix m, MonadMask m, MonadMVar m, MonadST m, MonadSTM m)
  => Table m h
  -> m (Table m h)
duplicate t@Table{..} = do
    traceWith tableTracer TraceDuplicate
    withOpenTable t $ \TableEnv{..} -> do
      -- We acquire a read-lock on the session open-state to prevent races, see
      -- 'sessionOpenTables'.
      withOpenSession tableSession $ \_ -> do
        withTempRegistry $ \reg -> do
          -- The table contents escape the read access, but we just added references
          -- to each run so it is safe.
          content <- RW.withReadAccess tableContent $ \content -> do
            addReferenceTableContent reg content
            pure content
          newWith
            reg
            tableSession
            tableSessionEnv
            tableConfig
            tableArenaManager
            content
