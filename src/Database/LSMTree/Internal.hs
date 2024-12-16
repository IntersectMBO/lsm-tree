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
  , TableConfig' (..)
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
  ) where

import           Codec.CBOR.Read
import           Control.Concurrent.Class.MonadMVar.Strict
import           Control.Concurrent.Class.MonadSTM (MonadSTM (..))
import           Control.Concurrent.Class.MonadSTM.RWVar (RWVar)
import qualified Control.Concurrent.Class.MonadSTM.RWVar as RW
import           Control.DeepSeq
import           Control.Monad (unless)
import           Control.Monad.Class.MonadST (MonadST (..))
import           Control.Monad.Class.MonadThrow
import           Control.Monad.Primitive
import           Control.RefCount
import           Control.TempRegistry
import           Control.Tracer
import           Data.Arena (ArenaManager, newArenaManager)
import           Data.Foldable
import           Data.Functor.Compose (Compose (..))
import           Data.GADT.Compare (GEq (geq))
import           Data.Kind
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import           Data.Maybe (catMaybes)
import qualified Data.Set as Set
import           Data.Some (Some (Some), withSome)
import           Data.Typeable
import qualified Data.Vector as V
import           Data.Word (Word64)
import           Database.LSMTree.Internal.BlobRef (WeakBlobRef (..))
import qualified Database.LSMTree.Internal.BlobRef as BlobRef
import           Database.LSMTree.Internal.Config
import qualified Database.LSMTree.Internal.Cursor as Cursor
import           Database.LSMTree.Internal.Entry (Entry, unNumEntries)
import           Database.LSMTree.Internal.Index (IndexAcc (ResultingIndex))
import           Database.LSMTree.Internal.Lookup (ByteCountDiscrepancy,
                     ResolveSerialisedValue, lookupsIO)
import           Database.LSMTree.Internal.MergeSchedule
import           Database.LSMTree.Internal.Paths (SessionRoot (..),
                     SnapshotMetaDataChecksumFile (..),
                     SnapshotMetaDataFile (..), SnapshotName)
import qualified Database.LSMTree.Internal.Paths as Paths
import           Database.LSMTree.Internal.Range (Range (..))
import           Database.LSMTree.Internal.Run (Run)
import           Database.LSMTree.Internal.RunReaders (OffsetKey (..))
import qualified Database.LSMTree.Internal.RunReaders as Readers
import           Database.LSMTree.Internal.Serialise (SerialisedBlob (..),
                     SerialisedKey, SerialisedValue)
import           Database.LSMTree.Internal.Snapshot
import           Database.LSMTree.Internal.Snapshot.Codec
import           Database.LSMTree.Internal.UniqCounter
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
data Session' m = forall j h. (IndexAcc j, Typeable h) =>
    Session' !(Session j m h)

instance NFData (Session' m) where
  rnf (Session' s) = rnf s

type Table' :: (Type -> Type) -> Type -> Type -> Type -> Type
data Table' m k v b = forall j h. (IndexAcc j, Typeable h) =>
    Table' (Table j m h)

instance NFData (Table' m k v b) where
  rnf (Table' t) = rnf t

type Cursor' :: (Type -> Type) -> Type -> Type -> Type -> Type
data Cursor' m k v b = forall j h. (IndexAcc j, Typeable h) =>
    Cursor' (Cursor j m h)

instance NFData (Cursor' m k v b) where
  rnf (Cursor' t) = rnf t

type NormalTable :: (Type -> Type) -> Type -> Type -> Type -> Type
data NormalTable m k v b = forall j h. (IndexAcc j, Typeable h) =>
    NormalTable !(Table j m h)

instance NFData (NormalTable m k v b) where
  rnf (NormalTable t) = rnf t

type NormalCursor :: (Type -> Type) -> Type -> Type -> Type -> Type
data NormalCursor m k v b = forall j h. (IndexAcc j, Typeable h) =>
    NormalCursor !(Cursor j m h)

instance NFData (NormalCursor m k v b) where
  rnf (NormalCursor c) = rnf c

type MonoidalTable :: (Type -> Type) -> Type -> Type -> Type
data MonoidalTable m k v = forall j h. (IndexAcc j, Typeable h) =>
    MonoidalTable !(Table j m h)

instance NFData (MonoidalTable m k v) where
  rnf (MonoidalTable t) = rnf t

type MonoidalCursor :: (Type -> Type) -> Type -> Type -> Type
data MonoidalCursor m k v = forall j h. (IndexAcc j, Typeable h) =>
    MonoidalCursor !(Cursor j m h)

instance NFData (MonoidalCursor m k v) where
  rnf (MonoidalCursor c) = rnf c

data TableConfig' = forall j. IndexAcc j => TableConfig' !(TableConfig j)

instance NFData TableConfig' where
  rnf (TableConfig' tc) = rnf tc

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
  | ErrSnapshotWrongIndexType
      (Some FencePointerIndex) -- ^ Expected index type
      (Some FencePointerIndex) -- ^ Actual index type
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
    TraceCreateTable (Some TableConfig)
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
data Session j m h = Session {
      -- | The primary purpose of this 'RWVar' is to ensure consistent views of
      -- the open-/closedness of a session when multiple threads require access
      -- to the session's fields (see 'withOpenSession'). We use more
      -- fine-grained synchronisation for various mutable parts of an open
      -- session.
      sessionState  :: !(RWVar m (SessionState j m h))
    , sessionTracer :: !(Tracer m LSMTreeTrace)
    }

instance NFData (Session j m h) where
  rnf (Session a b) = rnf a `seq` rwhnf b

data SessionState j m h =
    SessionOpen !(SessionEnv j m h)
  | SessionClosed

data SessionEnv j m h = SessionEnv {
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
    -- Each identifier is added only once in 'new', 'openSnapshot' or
    -- 'duplicate', and is deleted only once in 'close' or 'closeSession'.
    --
    -- * A new table may only insert its own identifier when it has acquired the
    --   'sessionState' read-lock. This is to prevent races with 'closeSession'.
    --
    -- * A table 'close' may delete its own identifier from the set of open
    --   tables without restrictions, even concurrently with 'closeSession'.
    --   This is safe because 'close' is idempotent'.
  , sessionOpenTables  :: !(StrictMVar m (Map Word64 (Table j m h)))
    -- | Similarly to tables, open cursors are tracked so they can be closed
    -- once the session is closed. See 'sessionOpenTables'.
  , sessionOpenCursors :: !(StrictMVar m (Map Word64 (Cursor j m h)))
  }

{-# INLINE withOpenSession #-}
{-# SPECIALISE withOpenSession ::
     Session j IO h
  -> (SessionEnv j IO h -> IO a)
  -> IO a #-}
-- | 'withOpenSession' ensures that the session stays open for the duration of the
-- provided continuation.
--
-- NOTE: any operation except 'sessionClose' can use this function.
withOpenSession ::
     (MonadSTM m, MonadThrow m)
  => Session j m h
  -> (SessionEnv j m h -> m a)
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
  -> (Session j IO h -> IO a)
  -> IO a #-}
-- | See 'Database.LSMTree.Common.withSession'.
withSession ::
     (MonadMask m, MonadSTM m, MonadMVar m, PrimMonad m)
  => Tracer m LSMTreeTrace
  -> HasFS m h
  -> HasBlockIO m h
  -> FsPath
  -> (Session j m h -> m a)
  -> m a
withSession tr hfs hbio dir = bracket (openSession tr hfs hbio dir) closeSession

{-# SPECIALISE openSession ::
     Tracer IO LSMTreeTrace
  -> HasFS IO h
  -> HasBlockIO IO h
  -> FsPath
  -> IO (Session j IO h) #-}
-- | See 'Database.LSMTree.Common.openSession'.
openSession ::
     forall j m h.
     (MonadCatch m, MonadSTM m, MonadMVar m)
  => Tracer m LSMTreeTrace
  -> HasFS m h
  -> HasBlockIO m h -- TODO: could we prevent the user from having to pass this in?
  -> FsPath -- ^ Path to the session directory
  -> m (Session j m h)
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
    activeDirPath    = Paths.getActiveDir (Paths.activeDir root)
    snapshotsDirPath = Paths.snapshotsDir root

    acquireLock = try @m @FsError $ FS.tryLockFile hbio lockFilePath FS.ExclusiveLock

    releaseLock lockFile = forM_ (Compose lockFile) $ \lockFile' -> FS.hUnlock lockFile'

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

    newSession sessionFileLock = do
        traceWith tr TraceNewSession
        FS.createDirectory hfs activeDirPath
        FS.createDirectory hfs snapshotsDirPath
        mkSession sessionFileLock

    restoreSession sessionFileLock = do
        traceWith tr TraceRestoreSession
        -- If the layouts are wrong, we throw an exception, and the lock file
        -- is automatically released by bracketOnError.
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
    -- This does /not/ check that /only/ the expected files and directories
    -- exist. This means that unexpected files in the top-level directory are
    -- ignored for the layout check.
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

{-# SPECIALISE closeSession :: Session j IO h -> IO () #-}
-- | See 'Database.LSMTree.Common.closeSession'.
--
-- A session's global resources will only be released once it is sure that no
-- tables are open anymore.
closeSession ::
     (MonadMask m, MonadSTM m, MonadMVar m, PrimMonad m)
  => Session j m h
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
data Table j m h = Table {
      tableConfig       :: !(TableConfig j)
      -- | The primary purpose of this 'RWVar' is to ensure consistent views of
      -- the open-/closedness of a table when multiple threads require access to
      -- the table's fields (see 'withOpenTable'). We use more fine-grained
      -- synchronisation for various mutable parts of an open table.
    , tableState        :: !(RWVar m (TableState j m h))
    , tableArenaManager :: !(ArenaManager (PrimState m))
    , tableTracer       :: !(Tracer m TableTrace)
    }

instance NFData (Table j m h) where
  rnf (Table a b c d) =
    rnf a `seq` rnf b `seq` rnf c `seq` rwhnf d

-- | A table may assume that its corresponding session is still open as
-- long as the table is open. A session's global resources, and therefore
-- resources that are inherited by the table, will only be released once the
-- session is sure that no tables are open anymore.
data TableState j m h =
    TableOpen !(TableEnv j m h)
  | TableClosed

data TableEnv j m h = TableEnv {
    -- === Session-inherited

    -- | The session that this table belongs to.
    --
    -- NOTE: Consider using the 'tableSessionEnv' field and helper functions
    -- like 'tableHasFS' instead of acquiring the session lock.
    tableSession    :: !(Session j m h)
    -- | Use this instead of 'tableSession' for easy access. An open table may
    -- assume that its session is open.
  , tableSessionEnv :: !(SessionEnv j m h)

    -- === Table-specific

    -- | Session-unique identifier for this table.
  , tableId         :: !Word64
    -- | All of the state being in a single 'StrictMVar' is a relatively simple
    -- solution, but there could be more concurrency. For example, while inserts
    -- are in progress, lookups could still look at the old state without
    -- waiting for the MVar.
    --
    -- TODO: switch to more fine-grained synchronisation approach
  , tableContent    :: !(RWVar m (TableContent j m h))
  }

{-# INLINE tableSessionRoot #-}
 -- | Inherited from session for ease of access.
tableSessionRoot :: TableEnv j m h -> SessionRoot
tableSessionRoot = sessionRoot . tableSessionEnv

{-# INLINE tableHasFS #-}
-- | Inherited from session for ease of access.
tableHasFS :: TableEnv j m h -> HasFS m h
tableHasFS = sessionHasFS . tableSessionEnv

{-# INLINE tableHasBlockIO #-}
-- | Inherited from session for ease of access.
tableHasBlockIO :: TableEnv j m h -> HasBlockIO m h
tableHasBlockIO = sessionHasBlockIO . tableSessionEnv

{-# INLINE tableSessionUniqCounter #-}
-- | Inherited from session for ease of access.
tableSessionUniqCounter :: TableEnv j m h -> UniqCounter m
tableSessionUniqCounter = sessionUniqCounter . tableSessionEnv

{-# INLINE tableSessionUntrackTable #-}
{-# SPECIALISE tableSessionUntrackTable :: TableEnv j IO h -> IO () #-}
-- | Open tables are tracked in the corresponding session, so when a table is
-- closed it should become untracked (forgotten).
tableSessionUntrackTable :: MonadMVar m => TableEnv j m h -> m ()
tableSessionUntrackTable thEnv =
    modifyMVar_ (sessionOpenTables (tableSessionEnv thEnv)) $ pure . Map.delete (tableId thEnv)

-- | 'withOpenTable' ensures that the table stays open for the duration of the
-- provided continuation.
--
-- NOTE: any operation except 'close' can use this function.
{-# INLINE withOpenTable #-}
{-# SPECIALISE withOpenTable ::
     Table j IO h
  -> (TableEnv j IO h -> IO a)
  -> IO a #-}
withOpenTable ::
     (MonadSTM m, MonadThrow m)
  => Table j m h
  -> (TableEnv j m h -> m a)
  -> m a
withOpenTable t action = RW.withReadAccess (tableState t) $ \case
    TableClosed -> throwIO ErrTableClosed
    TableOpen thEnv -> action thEnv

--
-- Implementation of public API
--

{-# SPECIALISE withTable ::
     Session j IO h
  -> TableConfig j
  -> (Table j IO h -> IO a)
  -> IO a #-}
-- | See 'Database.LSMTree.Normal.withTable'.
withTable ::
     (MonadMask m, MonadSTM m, MonadMVar m, PrimMonad m)
  => Session j m h
  -> TableConfig j
  -> (Table j m h -> m a)
  -> m a
withTable sesh conf = bracket (new sesh conf) close

{-# SPECIALISE new ::
     Session j IO h
  -> TableConfig j
  -> IO (Table j IO h) #-}
-- | See 'Database.LSMTree.Normal.new'.
new ::
     forall j m h. (MonadSTM m, MonadMVar m, PrimMonad m, MonadMask m)
  => Session j m h
  -> TableConfig j
  -> m (Table j m h)
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
               releaseRef
        let tableWriteBuffer = WB.empty
            tableLevels = V.empty
        tableCache <- mkLevelsCache @j reg tableLevels
        let tc = TableContent {
                tableWriteBuffer
              , tableWriteBufferBlobs
              , tableLevels
              , tableCache
              }
        newWith reg sesh seshEnv conf am tc

{-# SPECIALISE newWith ::
     TempRegistry IO
  -> Session j IO h
  -> SessionEnv j IO h
  -> TableConfig j
  -> ArenaManager RealWorld
  -> TableContent j IO h
  -> IO (Table j IO h) #-}
newWith ::
     (MonadSTM m, MonadMVar m)
  => TempRegistry m
  -> Session j m h
  -> SessionEnv j m h
  -> TableConfig j
  -> ArenaManager (PrimState m)
  -> TableContent j m h
  -> m (Table j m h)
newWith reg sesh seshEnv conf !am !tc = do
    tableId <- incrUniqCounter (sessionUniqCounter seshEnv)
    let tr = TraceTable (uniqueToWord64 tableId) `contramap` sessionTracer sesh
    traceWith tr $ TraceCreateTable (Some conf)
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

{-# SPECIALISE close :: Table j IO h -> IO () #-}
-- | See 'Database.LSMTree.Normal.close'.
close ::
     (MonadMask m, MonadSTM m, MonadMVar m, PrimMonad m)
  => Table j m h
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
          releaseTableContent reg tc
          pure tc
        pure TableClosed

{-# SPECIALISE lookups ::
     IndexAcc j
  => ResolveSerialisedValue
  -> V.Vector SerialisedKey
  -> Table j IO h
  -> IO (V.Vector (Maybe (Entry SerialisedValue (WeakBlobRef IO h)))) #-}
-- | See 'Database.LSMTree.Normal.lookups'.
lookups ::
     (IndexAcc j, MonadST m, MonadSTM m, MonadThrow m)
  => ResolveSerialisedValue
  -> V.Vector SerialisedKey
  -> Table j m h
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
     IndexAcc j
  => ResolveSerialisedValue
  -> Range SerialisedKey
  -> Table j IO h
  -> (SerialisedKey -> SerialisedValue -> Maybe (WeakBlobRef IO h) -> res)
  -> IO (V.Vector res) #-}
-- | See 'Database.LSMTree.Normal.rangeLookup'.
rangeLookup ::
     (IndexAcc j, MonadMask m, MonadMVar m, MonadST m, MonadSTM m)
  => ResolveSerialisedValue
  -> Range SerialisedKey
  -> Table j m h
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
     IndexAcc j
  => ResolveSerialisedValue
  -> V.Vector (SerialisedKey, Entry SerialisedValue SerialisedBlob)
  -> Table j IO h
  -> IO () #-}
-- | See 'Database.LSMTree.Normal.updates'.
--
-- Does not enforce that mupsert and blobs should not occur in the same table.
updates ::
     (IndexAcc j, MonadMask m, MonadMVar m, MonadST m, MonadSTM m)
  => ResolveSerialisedValue
  -> V.Vector (SerialisedKey, Entry SerialisedValue SerialisedBlob)
  -> Table j m h
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
     Session j IO h
  -> V.Vector (WeakBlobRef IO h)
  -> IO (V.Vector SerialisedBlob) #-}
retrieveBlobs ::
     (MonadMask m, MonadST m, MonadSTM m)
  => Session j m h
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
data Cursor j m h = Cursor {
      -- | Mutual exclusion, only a single thread can read from a cursor at a
      -- given time.
      cursorState  :: !(StrictMVar m (CursorState j m h))
    , cursorTracer :: !(Tracer m CursorTrace)
    }

instance NFData (Cursor j m h) where
  rnf (Cursor a b) = rwhnf a `seq` rwhnf b

data CursorState j m h =
    CursorOpen !(CursorEnv j m h)
  | CursorClosed  -- ^ Calls to a closed cursor raise an exception.

data CursorEnv j m h = CursorEnv {
    -- === Session-inherited

    -- | The session that this cursor belongs to.
    --
    -- NOTE: Consider using the 'cursorSessionEnv' field instead of acquiring
    -- the session lock.
    cursorSession    :: !(Session j m h)
    -- | Use this instead of 'cursorSession' for easy access. An open cursor may
    -- assume that its session is open. A session's global resources, and
    -- therefore resources that are inherited by the cursor, will only be
    -- released once the session is sure that no cursors are open anymore.
  , cursorSessionEnv :: !(SessionEnv j m h)

    -- === Cursor-specific

    -- | Session-unique identifier for this cursor.
  , cursorId         :: !Word64
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
  , cursorRuns       :: !(V.Vector (Ref (Run (ResultingIndex j) m h)))

    -- | The write buffer blobs, which like the runs, we have to keep open
    -- untile the cursor is closed.
  , cursorWBB        :: !(Ref (WBB.WriteBufferBlobs m h))
  }

{-# SPECIALISE withCursor ::
     IndexAcc j
  => OffsetKey
  -> Table j IO h
  -> (Cursor j IO h -> IO a)
  -> IO a #-}
-- | See 'Database.LSMTree.Normal.withCursor'.
withCursor ::
     (IndexAcc j, MonadMask m, MonadMVar m, MonadST m, MonadSTM m)
  => OffsetKey
  -> Table j m h
  -> (Cursor j m h -> m a)
  -> m a
withCursor offsetKey t = bracket (newCursor offsetKey t) closeCursor

{-# SPECIALISE newCursor ::
     IndexAcc j
  => OffsetKey
  -> Table j IO h
  -> IO (Cursor j IO h) #-}
-- | See 'Database.LSMTree.Normal.newCursor'.
newCursor ::
     (IndexAcc j, MonadMask m, MonadMVar m, MonadST m, MonadSTM m)
  => OffsetKey
  -> Table j m h
  -> m (Cursor j m h)
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
        (wb, wbblobs, cursorRuns) <- dupTableContent reg (tableContent thEnv)
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
    -- The table contents escape the read access, but we just duplicate
    -- references to each run, so it is safe.
    dupTableContent reg contentVar = do
        RW.withReadAccess contentVar $ \content -> do
          let !wb      = tableWriteBuffer content
              !wbblobs = tableWriteBufferBlobs content
          wbblobs' <- allocateTemp reg (dupRef wbblobs) releaseRef
          let runs = cachedRuns (tableCache content)
          runs' <- V.forM runs $ \r ->
                     allocateTemp reg (dupRef r) releaseRef
          pure (wb, wbblobs', runs')

{-# SPECIALISE closeCursor :: Cursor j IO h -> IO () #-}
-- | See 'Database.LSMTree.Normal.closeCursor'.
closeCursor ::
     (MonadMask m, MonadMVar m, MonadSTM m, PrimMonad m)
  => Cursor j m h
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
        V.forM_ cursorRuns $ freeTemp reg . releaseRef
        freeTemp reg (releaseRef cursorWBB)
        return CursorClosed

{-# SPECIALISE readCursor ::
     ResolveSerialisedValue
  -> Int
  -> Cursor j IO h
  -> (SerialisedKey -> SerialisedValue -> Maybe (WeakBlobRef IO h) -> res)
  -> IO (V.Vector res) #-}
-- | See 'Database.LSMTree.Normal.readCursor'.
readCursor ::
     forall j m h res.
     (MonadMask m, MonadMVar m, MonadST m, MonadSTM m)
  => ResolveSerialisedValue
  -> Int  -- ^ Maximum number of entries to read
  -> Cursor j m h
  -> (SerialisedKey -> SerialisedValue -> Maybe (WeakBlobRef m h) -> res)
     -- ^ How to map to a query result, different for normal/monoidal
  -> m (V.Vector res)
readCursor resolve n cursor fromEntry =
    readCursorWhile resolve (const True) n cursor fromEntry

{-# SPECIALISE readCursorWhile ::
     ResolveSerialisedValue
  -> (SerialisedKey -> Bool)
  -> Int
  -> Cursor j IO h
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
     forall j m h res.
     (MonadMask m, MonadMVar m, MonadST m, MonadSTM m)
  => ResolveSerialisedValue
  -> (SerialisedKey -> Bool)  -- ^ Only read as long as this predicate holds
  -> Int  -- ^ Maximum number of entries to read
  -> Cursor j m h
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
     IndexAcc j
  => ResolveSerialisedValue
  -> SnapshotName
  -> SnapshotLabel
  -> SnapshotTableType
  -> Table j IO h
  -> IO () #-}
-- |  See 'Database.LSMTree.Normal.createSnapshot''.
createSnapshot ::
     (IndexAcc j, MonadMask m, MonadMVar m, MonadST m, MonadSTM m)
  => ResolveSerialisedValue
  -> SnapshotName
  -> SnapshotLabel
  -> SnapshotTableType
  -> Table j m h
  -> m ()
createSnapshot resolve snap label tableType t = do
    traceWith (tableTracer t) $ TraceSnapshot snap
    let conf = tableConfig t
    withOpenTable t $ \thEnv ->
      withTempRegistry $ \reg -> do -- TODO: use the temp registry for all side effects
        let hfs = tableHasFS thEnv

        -- Guard that the snapshot does not exist already
        let snapDir = Paths.namedSnapshotDir (tableSessionRoot thEnv) snap
        doesSnapshotExist <-
          FS.doesDirectoryExist (tableHasFS thEnv) (Paths.getNamedSnapshotDir snapDir)
        if doesSnapshotExist then
          throwIO (ErrSnapshotExists snap)
        else
          -- we assume the snapshots directory already exists, so we just have to
          -- create the directory for this specific snapshot.
          FS.createDirectory hfs (Paths.getNamedSnapshotDir snapDir)

        -- For the temporary implementation it is okay to just flush the buffer
        -- before taking the snapshot.
        content <- modifyWithTempRegistry
                      (RW.unsafeAcquireWriteAccess (tableContent thEnv))
                      (atomically . RW.unsafeReleaseWriteAccess (tableContent thEnv))
                      $ \innerReg content -> do
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
                innerReg
                content
          pure (content', content')
        -- At this point, we've flushed the write buffer but we haven't created the
        -- snapshot file yet. If an asynchronous exception happens beyond this
        -- point, we'll take that loss, as the inner state of the table is still
        -- consistent.

        -- Convert to snapshot format
        snapLevels <- toSnapLevels (tableLevels content)
        -- Hard link runs into the named snapshot directory
        snapLevels' <- snapshotRuns reg snapDir snapLevels

        let snapMetaData = SnapshotMetaData label tableType (Some (tableConfig t)) snapLevels'
            SnapshotMetaDataFile contentPath = Paths.snapshotMetaDataFile snapDir
            SnapshotMetaDataChecksumFile checksumPath = Paths.snapshotMetaDataChecksumFile snapDir
        writeFileSnapshotMetaData hfs contentPath checksumPath snapMetaData

{-# SPECIALISE openSnapshot ::
     IndexAcc j
  => Session j IO h
  -> SnapshotLabel
  -> SnapshotTableType
  -> FencePointerIndex j
  -> TableConfigOverride
  -> SnapshotName
  -> ResolveSerialisedValue
  -> IO (Table j IO h) #-}
-- |  See 'Database.LSMTree.Normal.openSnapshot'.
openSnapshot ::
     (IndexAcc j, MonadMask m, MonadMVar m, MonadST m, MonadSTM m)
  => Session j m h
  -> SnapshotLabel -- ^ Expected label
  -> SnapshotTableType -- ^ Expected table type
  -> FencePointerIndex j -- ^ Expected index type
  -> TableConfigOverride -- ^ Optional config override
  -> SnapshotName
  -> ResolveSerialisedValue
  -> m (Table j m h)
openSnapshot sesh label tableType indexType override snap resolve = do
    traceWith (sessionTracer sesh) $ TraceOpenSnapshot snap override
    withOpenSession sesh $ \seshEnv -> do
      withTempRegistry $ \reg -> do
        let hfs     = sessionHasFS seshEnv
            hbio    = sessionHasBlockIO seshEnv

        -- Guard that the snapshot exists
        let snapDir = Paths.namedSnapshotDir (sessionRoot seshEnv) snap
        FS.doesDirectoryExist hfs (Paths.getNamedSnapshotDir snapDir) >>= \b ->
          unless b $ throwIO (ErrSnapshotNotExists snap)

        let SnapshotMetaDataFile contentPath = Paths.snapshotMetaDataFile snapDir
            SnapshotMetaDataChecksumFile checksumPath = Paths.snapshotMetaDataChecksumFile snapDir
        snapMetaData <- readFileSnapshotMetaData hfs contentPath checksumPath >>= \case
          Left e  -> throwIO (ErrSnapshotDeserialiseFailure e snap)
          Right x -> pure x

        let SnapshotMetaData label' tableType' someConf snapLevels =
              snapMetaData

        unless (tableType == tableType') $
          throwIO (ErrSnapshotWrongTableType snap tableType tableType')

        unless (label == label') $
          throwIO (ErrSnapshotWrongLabel snap label label')

        withSome someConf $ \conf -> do
          let indexType' = confFencePointerIndex conf
          case indexType `geq` indexType' of
            Nothing ->
              throwIO $ ErrSnapshotWrongIndexType (Some indexType)
                                                  (Some indexType')
            Just Refl -> do
              let conf' = applyOverride override conf
              am <- newArenaManager
              blobpath <- Paths.tableBlobPath (sessionRoot seshEnv) <$>
                            incrUniqCounter (sessionUniqCounter seshEnv)
              tableWriteBufferBlobs <- allocateTemp reg (WBB.new hfs blobpath)
                                                        releaseRef
  
              let actDir = Paths.activeDir (sessionRoot seshEnv)
  
              -- Hard link runs into the active directory,
              snapLevels' <- openRuns reg hfs hbio conf (sessionUniqCounter seshEnv) snapDir actDir snapLevels
              -- Convert from the snapshot format, restoring merge progress in the process
              tableLevels <- fromSnapLevels reg hfs hbio conf (sessionUniqCounter seshEnv) resolve actDir snapLevels'
  
              tableCache <- mkLevelsCache reg tableLevels
              newWith reg sesh seshEnv conf' am $! TableContent {
                  tableWriteBuffer = WB.empty
                , tableWriteBufferBlobs
                , tableLevels
                , tableCache
                }

{-# SPECIALISE deleteSnapshot ::
     Session j IO h
  -> SnapshotName
  -> IO () #-}
-- |  See 'Database.LSMTree.Common.deleteSnapshot'.
deleteSnapshot ::
     (MonadMask m, MonadSTM m)
  => Session j m h
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

{-# SPECIALISE listSnapshots :: Session j IO h -> IO [SnapshotName] #-}
-- |  See 'Database.LSMTree.Common.listSnapshots'.
listSnapshots ::
     (MonadMask m, MonadSTM m)
  => Session j m h
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

{-# SPECIALISE duplicate :: Table j IO h -> IO (Table j IO h) #-}
-- | See 'Database.LSMTree.Normal.duplicate'.
duplicate ::
     (MonadMask m, MonadMVar m, MonadST m, MonadSTM m)
  => Table j m h
  -> m (Table j m h)
duplicate t@Table{..} = do
    traceWith tableTracer TraceDuplicate
    withOpenTable t $ \TableEnv{..} -> do
      -- We acquire a read-lock on the session open-state to prevent races, see
      -- 'sessionOpenTables'.
      withOpenSession tableSession $ \_ -> do
        withTempRegistry $ \reg -> do
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
