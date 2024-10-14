{-# LANGUAGE DataKinds #-}

module Database.LSMTree.Internal.Session (
    -- * Exceptions
    LSMTreeError (..)
    -- * Tracing
  , LSMTreeTrace (..)
  , TableTrace (..)
  , CursorTrace (..)
    -- * Session
  , Session (..)
  , SessionState (..)
  , SessionEnv (..)
  , Resource (..)
  , withOpenSession
    -- ** Implementation of public API
  , withSession
  , openSession
  , closeSession
  ) where

import           Control.Concurrent.Class.MonadMVar.Strict
import           Control.Concurrent.Class.MonadSTM (MonadSTM (..))
import           Control.Concurrent.Class.MonadSTM.RWVar (RWVar)
import qualified Control.Concurrent.Class.MonadSTM.RWVar as RW
import           Control.DeepSeq
import           Control.Monad (unless)
import           Control.Monad.Class.MonadThrow
import           Control.TempRegistry (Resource (..))
import           Control.Tracer
import           Data.Char (isNumber)
import           Data.Foldable
import           Data.Functor.Compose (Compose (..))
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import qualified Data.Set as Set
import           Data.Word (Word64)
import           Database.LSMTree.Internal.Config
import           Database.LSMTree.Internal.Lookup (ByteCountDiscrepancy)
import           Database.LSMTree.Internal.MergeSchedule
import           Database.LSMTree.Internal.Paths (SessionRoot (..),
                     SnapshotName)
import qualified Database.LSMTree.Internal.Paths as Paths
import           Database.LSMTree.Internal.Range (Range (..))
import           Database.LSMTree.Internal.Serialise (SerialisedKey)
import           Database.LSMTree.Internal.UniqCounter
import qualified System.FS.API as FS
import           System.FS.API (FsError, FsErrorPath (..), FsPath, HasFS)
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
    -- | A 'BlobRef' used with 'retrieveBlobs' was invalid.
    --
    -- 'BlobRef's are obtained from lookups in a 'TableHandle', but they may be
    -- invalidated by subsequent changes in that 'TableHandle'. In general the
    -- reliable way to retrieve blobs is not to change the 'TableHandle' before
    -- retrieving the blobs. To allow later retrievals, duplicate the table
    -- handle before making modifications and keep the table handle open until
    -- all blob retrievals are complete.
    --
    -- The 'Int' index indicates which 'BlobRef' was invalid. Many may be
    -- invalid but only the first is reported.
  | ErrBlobRefInvalid Int
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
    -- Cursor
  | TraceCursor
      Word64 -- ^ Cursor identifier
      CursorTrace
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
  , sessionOpenTables  :: !(StrictMVar m (Map Word64 (Resource m)))
    -- | Similarly to tables, open cursors are tracked so they can be closed
    -- once the session is closed. See 'sessionOpenTables'.
  , sessionOpenCursors :: !(StrictMVar m (Map Word64 (Resource m)))
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
     (MonadCatch m, MonadSTM m, MonadMVar m)
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
     (MonadCatch m, MonadSTM m, MonadMVar m)
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
        traverse_ resourceRelease cursors
        tables <- modifyMVar (sessionOpenTables seshEnv) (\m -> pure (Map.empty, m))
        traverse_ resourceRelease tables
        FS.close (sessionHasBlockIO seshEnv)
        FS.hUnlock (sessionLockFile seshEnv)
        pure SessionClosed
