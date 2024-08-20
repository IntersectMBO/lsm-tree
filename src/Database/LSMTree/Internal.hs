{-# LANGUAGE CPP       #-}
{-# LANGUAGE DataKinds #-}

module Database.LSMTree.Internal (
    -- * Exceptions
    LSMTreeError (..)
    -- * Tracing
  , LSMTreeTrace (..)
  , TableTrace (..)
  , MergeTrace (..)
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
  , TableContent (..)
  , Level (..)
  , withOpenTable
    -- ** Implementation of public API
  , ResolveSerialisedValue
  , withTable
  , new
  , close
  , lookups
  , updates
    -- * Snapshots
  , SnapshotLabel
  , snapshot
  , open
  , deleteSnapshot
  , listSnapshots
    -- * Mutiple writable table handles
  , duplicate
    -- * Exported for cabal-docspec
  , MergePolicyForLevel (..)
  , maxRunSize
  ) where

import           Control.Concurrent.Class.MonadMVar.Strict
import           Control.Concurrent.Class.MonadSTM (MonadSTM (..))
import           Control.Concurrent.Class.MonadSTM.RWVar (RWVar)
import qualified Control.Concurrent.Class.MonadSTM.RWVar as RW
import           Control.DeepSeq
import           Control.Monad (unless, void, when)
import           Control.Monad.Class.MonadThrow
import           Control.Monad.Primitive
import           Control.Monad.ST.Strict
import           Control.Tracer
import           Data.Arena (ArenaManager, newArenaManager)
import           Data.Bifunctor (Bifunctor (..))
import           Data.BloomFilter (Bloom)
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
import           Database.LSMTree.Internal.Assertions (assert, assertNoThunks)
import           Database.LSMTree.Internal.BlobRef
import           Database.LSMTree.Internal.Config
import           Database.LSMTree.Internal.Entry (Entry (..), NumEntries (..),
                     combineMaybe, unNumEntries)
import           Database.LSMTree.Internal.IndexCompact (IndexCompact)
import           Database.LSMTree.Internal.Lookup (ByteCountDiscrepancy,
                     ResolveSerialisedValue, lookupsIO)
import           Database.LSMTree.Internal.Managed
import qualified Database.LSMTree.Internal.Merge as Merge
import           Database.LSMTree.Internal.Paths (RunFsPaths (..),
                     SessionRoot (..), SnapshotName)
import qualified Database.LSMTree.Internal.Paths as Paths
import           Database.LSMTree.Internal.Run (Run, RunDataCaching (..))
import qualified Database.LSMTree.Internal.Run as Run
import           Database.LSMTree.Internal.RunAcc (RunBloomFilterAlloc (..))
import           Database.LSMTree.Internal.Serialise (SerialisedBlob,
                     SerialisedKey, SerialisedValue)
import           Database.LSMTree.Internal.TempRegistry
import qualified Database.LSMTree.Internal.Vector as V
import           Database.LSMTree.Internal.WriteBuffer (WriteBuffer)
import qualified Database.LSMTree.Internal.WriteBuffer as WB
import           NoThunks.Class
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
    -- | All operations on a closed session or tables within a closed session
    -- will throw this exception. There is one exception (pun intended) to this
    -- rule: the idempotent operation 'Database.LSMTree.Common.closeSession'.
  | ErrSessionClosed
    -- | All operations on a closed table will throw this exception. There is
    -- one exception (pun intended) to this rule: the idempotent operation
    -- 'Database.LSMTree.Common.close'.
  | ErrTableClosed
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

data AtLevel a = AtLevel LevelNo a
  deriving stock Show

data MergeTrace =
    TraceFlushWriteBuffer
      NumEntries -- ^ Size of the write buffer
      RunNumber
      RunDataCaching
      RunBloomFilterAlloc
  | TraceAddLevel
  | TraceAddRun
      RunNumber -- ^ newly added run
      (V.Vector RunNumber) -- ^ resident runs
  | TraceNewMerge
      (V.Vector NumEntries) -- ^ Sizes of input runs
      RunNumber
      RunDataCaching
      RunBloomFilterAlloc
      MergePolicyForLevel
      Merge.Level
  | TraceCompletedMerge
      NumEntries -- ^ Size of output run
      RunNumber
  | TraceExpectCompletedMerge
      RunNumber
  | TraceNewMergeSingleRun
      NumEntries -- ^ Size of run
      RunNumber
  | TraceExpectCompletedMergeSingleRun
      RunNumber
  deriving stock Show

newtype RunNumber = RunNumber Word64
  deriving stock Show

runFsPathsRunNumber :: RunFsPaths -> RunNumber
runFsPathsRunNumber rfsp = RunNumber (runNumber rfsp)

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

-- | An atomic counter for producing unique 'Word64' values.
newtype UniqCounter m = UniqCounter (StrictMVar m Word64)

{-# INLINE newUniqCounter #-}
newUniqCounter :: MonadMVar m => Word64 -> m (UniqCounter m)
newUniqCounter x = UniqCounter <$> newMVar x

{-# INLINE incrUniqCounter #-}
-- | Return the current state of the atomic counter, and then increment the
-- counter.
incrUniqCounter :: MonadMVar m => UniqCounter m -> m Word64
incrUniqCounter (UniqCounter uniqVar) = modifyMVar uniqVar (\x -> pure ((x+1), x))

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
        sessionVar <- RW.new $ SessionOpen $ SessionEnv {
            sessionRoot = root
          , sessionHasFS = hfs
          , sessionHasBlockIO = hbio
          , sessionLockFile = lockFile
          , sessionUniqCounter = counterVar
          , sessionOpenTables = openTablesVar
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
        -- Since we have a write lock on the session state, we know that no
        -- tables will be added while we are closing the session, and that we
        -- are the only thread currently closing the session.
        --
        -- We technically don't have to overwrite this with an empty Map, but
        -- why not
        tables <- modifyMVar (sessionOpenTables seshEnv) (\m -> pure (Map.empty, m))
        -- Close tables first, so that we know none are open when we release
        -- session-wide resources.
        --
        -- If any table from this set has been closed already by a different
        -- thread, the idempotent 'close' will act like a no-op, and so we are
        -- not in trouble.
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
  , tableContent    :: !(RWVar m (TableContent (PrimState m) h))
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

data TableContent s h = TableContent {
    tableWriteBuffer :: !WriteBuffer
    -- | A hierarchy of levels. The vector indexes double as level numbers.
  , tableLevels      :: !(Levels s (Handle h))
    -- | Cache of flattened 'levels'.
    --
    -- INVARIANT: when 'level's is modified, this cache should be updated as
    -- well, for example using 'mkLevelsCache'.
  , tableCache       :: !(LevelsCache s (Handle h))
  }

emptyTableContent :: TableContent s h
emptyTableContent = TableContent {
      tableWriteBuffer = WB.empty
    , tableLevels = V.empty
    , tableCache = mkLevelsCache V.empty
    }

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

type Levels s h = V.Vector (Level s h)

-- | Runs in order from newer to older
data Level s h = Level {
    incomingRuns :: !(MergingRun s h)
  , residentRuns :: !(V.Vector (Run s h))
  }

-- TODO: proper instance
deriving via OnlyCheckWhnfNamed "Level" (Level s h) instance NoThunks (Level s h)

-- | A merging run is either a single run, or some ongoing merge.
data MergingRun s h =
    MergingRun !(MergingRunState s h)
  | SingleRun !(Run s h)

-- | Merges are stepped to completion immediately, so there is no representation
-- for ongoing merges (yet)
--
-- TODO: this should also represent ongoing merges once we implement scheduling.
newtype MergingRunState s h = CompletedMerge (Run s h)

-- | Return all runs in the levels, ordered from newest to oldest
runsInLevels :: Levels s h -> V.Vector (Run s h)
runsInLevels levels = flip V.concatMap levels $ \(Level mr rs) ->
    case mr of
      SingleRun r                   -> r `V.cons` rs
      MergingRun (CompletedMerge r) -> r `V.cons` rs

{-# SPECIALISE closeLevels :: HasFS IO h -> HasBlockIO IO h -> Levels RealWorld (Handle h) -> IO () #-}
closeLevels ::
     m ~ IO -- TODO: replace by @io-classes@ constraints for IO simulation.
  => HasFS m h
  -> HasBlockIO m h
  -> Levels (PrimState m) (Handle h)
  -> m ()
closeLevels hfs hbio levels = V.mapM_ (Run.removeReference hfs hbio) (runsInLevels levels)

-- | Flattend cache of the runs that referenced by a table handle.
--
-- This cache includes a vector of runs, but also vectors of the runs broken
-- down into components, like bloom filters, fence pointer indexes and file
-- handles. This allows for quick access in the lookup code. Recomputing this
-- cache should be relatively rare.
--
-- Use 'mkLevelsCache' to ensure that there are no mismatches between the vector
-- of runs and the vectors of run components.
data LevelsCache s h = LevelsCache_ {
    cachedRuns      :: !(V.Vector (Run s h))
  , cachedFilters   :: !(V.Vector (Bloom SerialisedKey))
  , cachedIndexes   :: !(V.Vector IndexCompact)
  , cachedKOpsFiles :: !(V.Vector h)
  }

-- | Flatten the argument 'Level's into a single vector of runs, and use that to
-- populate the 'LevelsCache'.
mkLevelsCache :: Levels s h -> LevelsCache s h
mkLevelsCache lvls = LevelsCache_ {
      cachedRuns      = rs
    , cachedFilters   = V.map Run.runFilter rs
    , cachedIndexes   = V.map Run.runIndex rs
    , cachedKOpsFiles = V.map Run.runKOpsFile rs
    }
  where
    rs = runsInLevels lvls

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

{-# SPECIALISE newWith :: Session IO h -> SessionEnv IO h -> TableConfig -> ArenaManager RealWorld -> WriteBuffer -> Levels RealWorld (Handle h) -> IO (TableHandle IO h) #-}
newWith ::
     m ~ IO -- TODO: replace by @io-classes@ constraints for IO simulation.
  => Session m h
  -> SessionEnv m h
  -> TableConfig
  -> ArenaManager (PrimState m)
  -> WriteBuffer
  -> Levels (PrimState m) (Handle h)
  -> m (TableHandle m h)
newWith sesh seshEnv conf !am !wb !levels = do
    tableId <- incrUniqCounter (sessionUniqCounter seshEnv)
    let tr = TraceTable tableId `contramap` sessionTracer sesh
    traceWith tr $ TraceCreateTableHandle conf
    assertNoThunks levels $ pure ()
    -- The session is kept open until we've updated the session's set of tracked
    -- tables. If 'closeSession' is called by another thread while this code
    -- block is being executed, that thread will block until it reads the
    -- /updated/ set of tracked tables.
    contentVar <- RW.new $ TableContent wb levels (mkLevelsCache levels)
    tableVar <- RW.new $ TableHandleOpen $ TableHandleEnv {
          tableSession = sesh
        , tableSessionEnv = seshEnv
        , tableId = tableId
        , tableContent = contentVar
        }
    let !th = TableHandle conf tableVar am tr
    -- Track the current table
    modifyMVar_ (sessionOpenTables seshEnv) $ pure . Map.insert tableId th
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
        tableSessionUntrackTable thEnv
        RW.withWriteAccess_ (tableContent thEnv) $ \lvls -> do
          closeLevels (tableHasFS thEnv) (tableHasBlockIO thEnv) (tableLevels lvls)
          pure emptyTableContent
        pure TableHandleClosed

{-# SPECIALISE lookups :: ResolveSerialisedValue -> V.Vector SerialisedKey -> TableHandle IO h -> (Maybe (Entry SerialisedValue (BlobRef (Run RealWorld (Handle h)))) -> lookupResult) -> IO (V.Vector lookupResult) #-}
-- | See 'Database.LSMTree.Normal.lookups'.
lookups ::
     m ~ IO -- TODO: replace by @io-classes@ constraints for IO simulation.
  => ResolveSerialisedValue
  -> V.Vector SerialisedKey
  -> TableHandle m h
  -> (Maybe (Entry SerialisedValue (BlobRef (Run (PrimState m) (Handle h)))) -> lookupResult)
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
          tc' <- updatesWithInterleavedFlushes
                  (tableTracer th)
                  conf
                  resolve
                  hfs
                  (tableHasBlockIO thEnv)
                  (tableSessionRoot thEnv)
                  (tableSessionUniqCounter thEnv)
                  es
                  tc
          assertNoThunks tc' $ pure ()
          pure tc'

{-# SPECIALISE updatesWithInterleavedFlushes :: Tracer IO TableTrace -> TableConfig -> ResolveSerialisedValue -> HasFS IO h -> HasBlockIO IO h -> SessionRoot -> UniqCounter IO -> V.Vector (SerialisedKey, Entry SerialisedValue SerialisedBlob) -> TempRegistry IO -> TableContent RealWorld h -> IO (TableContent RealWorld h) #-}
-- | A single batch of updates can fill up the write buffer multiple times. We
-- flush the write buffer each time it fills up before trying to fill it up
-- again.
--
-- TODO: in practice the size of a batch will be much smaller than the maximum
-- size of the write buffer, so we should optimise for the case that small
-- batches are inserted. Ideas:
--
-- * we can allow a range of sizes to flush to disk rather than just the max size
--
-- * could do a map bulk merge rather than sequential insert, on the prefix of
--   the batch that's guaranteed to fit
--
-- * or flush the existing buffer if we would expect the next batch to cause the
--   buffer to become too large
--
-- TODO: we could also optimise for the case where the write buffer is small
-- compared to the size of the batch, but it is less critical. In particular, in
-- case the write buffer is empty, or if it fills up multiple times for a single
-- batch of updates, we might be able to skip adding entries to the write buffer
-- for a large part. When the write buffer is empty, we can sort and deduplicate
-- the vector of updates directly, slice it up into properly sized sub-vectors,
-- and write those to disk. Of course, any remainder that did not fit into a
-- whole run should then end up in a fresh write buffer.
updatesWithInterleavedFlushes ::
     forall m h. m ~ IO -- TODO: replace by @io-classes@ constraints for IO simulation.
  => Tracer m TableTrace
  -> TableConfig
  -> ResolveSerialisedValue
  -> HasFS m h
  -> HasBlockIO m h
  -> SessionRoot
  -> UniqCounter m
  -> V.Vector (SerialisedKey, Entry SerialisedValue SerialisedBlob)
  -> TempRegistry m
  -> TableContent (PrimState m) h
  -> m (TableContent (PrimState m) h)
updatesWithInterleavedFlushes tr conf resolve hfs hbio root uniqC es reg tc = do
    let wb = tableWriteBuffer tc
        (wb', es') = WB.addEntriesUpToN resolve es maxn wb
    -- never exceed the write buffer capacity
    assert (unNumEntries (WB.numEntries wb') <= maxn) $ pure ()
    let tc' = setWriteBuffer wb' tc
    -- If the new write buffer has not reached capacity yet, then it must be the
    -- cases that we have performed all the updates.
    if unNumEntries (WB.numEntries wb') < maxn then do
      assert (V.null es') $ pure ()
      pure $! tc'
    -- If the write buffer did reach capacity, then we flush.
    else do
      assert (unNumEntries (WB.numEntries wb') == maxn) $ pure ()
      tc'' <- flushWriteBuffer (TraceMerge `contramap` tr) conf resolve hfs hbio root uniqC reg tc'
      -- In the fortunate case where we have already performed all the updates,
      -- return,
      if V.null es' then
        pure $! tc''
      -- otherwise, keep going
      else
        updatesWithInterleavedFlushes tr conf resolve hfs hbio root uniqC es' reg tc''
  where
    AllocNumEntries (NumEntries maxn) = confWriteBufferAlloc conf
    setWriteBuffer :: WriteBuffer -> TableContent (PrimState m) h -> TableContent (PrimState m) h
    setWriteBuffer wbToSet tc0 = TableContent {
          tableWriteBuffer = wbToSet
        , tableLevels = tableLevels tc0
        , tableCache = tableCache tc0
        }

{-# SPECIALISE flushWriteBuffer :: Tracer IO (AtLevel MergeTrace) -> TableConfig -> ResolveSerialisedValue -> HasFS IO h -> HasBlockIO IO h -> SessionRoot -> UniqCounter IO -> TempRegistry IO -> TableContent RealWorld h -> IO (TableContent RealWorld h) #-}
-- | Flush the write buffer to disk, regardless of whether it is full or not.
--
-- The returned table content contains an updated set of levels, where the write
-- buffer is inserted into level 1.
flushWriteBuffer ::
     m ~ IO -- TODO: replace by @io-classes@ constraints for IO simulation.
  => Tracer m (AtLevel MergeTrace)
  -> TableConfig
  -> ResolveSerialisedValue
  -> HasFS m h
  -> HasBlockIO m h
  -> SessionRoot
  -> UniqCounter m
  -> TempRegistry m
  -> TableContent (PrimState m) h
  -> m (TableContent (PrimState m) h)
flushWriteBuffer tr conf@TableConfig{confDiskCachePolicy}
                 resolve hfs hbio root uniqC reg tc
  | WB.null (tableWriteBuffer tc) = pure tc
  | otherwise = do
    !n <- incrUniqCounter uniqC
    let !size  = WB.numEntries (tableWriteBuffer tc)
        !l     = LevelNo 1
        !cache = diskCachePolicyForLevel confDiskCachePolicy l
        !alloc = bloomFilterAllocForLevel conf l
        !path  = Paths.runPath root n
    traceWith tr $ AtLevel l $ TraceFlushWriteBuffer size (runFsPathsRunNumber path) cache alloc
    r <- allocateTemp reg
            (Run.fromWriteBuffer hfs hbio
              cache
              alloc
              path
              (tableWriteBuffer tc))
            (Run.removeReference hfs hbio)
    levels' <- addRunToLevels tr conf resolve hfs hbio root uniqC r reg (tableLevels tc)
    pure $! TableContent {
        tableWriteBuffer = WB.empty
      , tableLevels = levels'
      , tableCache = mkLevelsCache levels'
      }

-- | Note that the invariants rely on the fact that levelling is only used on
-- the last level.
--
_levelsInvariant :: forall s h. TableConfig -> Levels s h -> ST s Bool
_levelsInvariant conf levels =
    go (LevelNo 1) levels >>= \ !_ -> pure True
  where
    sr = confSizeRatio conf
    wba = confWriteBufferAlloc conf

    go :: LevelNo -> Levels s h -> ST s ()
    go !_ (V.uncons -> Nothing) = pure ()

    go !ln (V.uncons -> Just (Level mr rs, ls)) = do
      mrs <- case mr of
               SingleRun r                   -> pure $ CompletedMerge r
               MergingRun (CompletedMerge r) -> pure $ CompletedMerge r
      assert (length rs < sizeRatioInt sr) $ pure ()
      assert (expectedRunLengths ln rs ls) $ pure ()
      assert (expectedMergingRunLengths ln mr mrs ls) $ pure ()
      go (succ ln) ls

    -- All runs within a level "proper" (as opposed to the incoming runs
    -- being merged) should be of the correct size for the level.
    expectedRunLengths ln rs ls = do
      case mergePolicyForLevel (confMergePolicy conf) ln ls of
        -- Levels using levelling have only one run, and that single run is
        -- (almost) always involved in an ongoing merge. Thus there are no
        -- other "normal" runs. The exception is when a levelling run becomes
        -- too large and is promoted, in that case initially there's no merge,
        -- but it is still represented as a 'MergingRun', using 'SingleRun'.
        LevelLevelling -> assert (V.null rs) True
        LevelTiering   -> V.all (\r -> assert (fits LevelTiering r ln) True) rs

    -- Incoming runs being merged also need to be of the right size, but the
    -- conditions are more complicated.
    expectedMergingRunLengths ln mr mrs ls =
      case mergePolicyForLevel (confMergePolicy conf) ln ls of
        LevelLevelling ->
          case (mr, mrs) of
            -- A single incoming run (which thus didn't need merging) must be
            -- of the expected size range already
            (SingleRun r, CompletedMerge{}) -> assert (fits LevelLevelling r ln) True
                        -- A completed merge for levelling can be of almost any size at all!
            -- It can be smaller, due to deletions in the last level. But it
            -- can't be bigger than would fit into the next level.
            (_, CompletedMerge r) -> assert (fitsUB LevelLevelling r (succ ln)) True
        LevelTiering ->
          case (mr, mrs, mergeLastForLevel ls) of
            -- A single incoming run (which thus didn't need merging) must be
            -- of the expected size already
            (SingleRun r, CompletedMerge{}, _) -> assert (fits LevelTiering r ln) True

            -- A completed last level run can be of almost any smaller size due
            -- to deletions, but it can't be bigger than the next level down.
            -- Note that tiering on the last level only occurs when there is
            -- a single level only.
            (_, CompletedMerge r, Merge.LastLevel) ->
                assert (ln == LevelNo 1) $
                assert (fitsUB LevelTiering r (succ ln)) $
                True

            -- A completed mid level run is usually of the size for the
            -- level it is entering, but can also be one smaller (in which case
            -- it'll be held back and merged again).
            (_, CompletedMerge r, Merge.MidLevel) ->
                assert (fitsUB LevelTiering r ln || fitsUB LevelTiering r (succ ln)) True

    -- Check that a run fits in the current level
    fits policy r ln = fitsLB policy r ln && fitsUB policy r ln
    -- Check that a run is too large for previous levels
    fitsLB policy r ln = maxRunSize sr wba policy (pred ln) < Run.runNumEntries r
    -- Check that a run is too small for next levels
    fitsUB policy r ln = Run.runNumEntries r <= maxRunSize sr wba policy ln

{-# SPECIALISE addRunToLevels :: Tracer IO (AtLevel MergeTrace) -> TableConfig -> ResolveSerialisedValue -> HasFS IO h -> HasBlockIO IO h -> SessionRoot -> UniqCounter IO -> Run RealWorld (Handle h) -> TempRegistry IO -> Levels RealWorld (Handle h) -> IO (Levels RealWorld (Handle h)) #-}
addRunToLevels ::
     forall m h. m ~ IO -- TODO: replace by @io-classes@ constraints for IO simulation.
  => Tracer m (AtLevel MergeTrace)
  -> TableConfig
  -> ResolveSerialisedValue
  -> HasFS m h
  -> HasBlockIO m h
  -> SessionRoot
  -> UniqCounter m
  -> Run (PrimState m) (Handle h)
  -> TempRegistry m
  -> Levels (PrimState m) (Handle h)
  -> m (Levels (PrimState m) (Handle h))
addRunToLevels tr conf@TableConfig{..} resolve hfs hbio root uniqC r0 reg levels = do
    ls' <- go (LevelNo 1) (V.singleton r0) levels
#ifdef NO_IGNORE_ASSERTS
    void $ stToIO $ _levelsInvariant conf ls'
#endif
    return ls'
  where
    -- NOTE: @go@ is based on the @increment@ function from the
    -- @ScheduledMerges@ prototype.
    go !ln rs (V.uncons -> Nothing) = do
        traceWith tr $ AtLevel ln TraceAddLevel
        -- Make a new level
        let policyForLevel = mergePolicyForLevel confMergePolicy ln V.empty
        mr <- newMerge policyForLevel Merge.LastLevel ln rs
        return $ V.singleton $ Level mr V.empty
    go !ln rs' (V.uncons -> Just (Level mr rs, ls)) = do
        -- TODO: until we have proper scheduling, the merging run is actually
        -- always stepped to completion immediately, so we can see it is just a
        -- single run.
        r <- expectCompletedMerge ln mr
        case mergePolicyForLevel confMergePolicy ln ls of
          -- If r is still too small for this level then keep it and merge again
          -- with the incoming runs.
          LevelTiering | runSize r <= maxRunSize' conf LevelTiering (pred ln) -> do
            let mergelast = mergeLastForLevel ls
            mr' <- newMerge LevelTiering mergelast ln (rs' `V.snoc` r)
            pure $! Level mr' rs `V.cons` ls
          -- This tiering level is now full. We take the completed merged run
          -- (the previous incoming runs), plus all the other runs on this level
          -- as a bundle and move them down to the level below. We start a merge
          -- for the new incoming runs. This level is otherwise empty.
          LevelTiering | levelIsFull confSizeRatio rs -> do
            mr' <- newMerge LevelTiering Merge.MidLevel ln rs'
            ls' <- go (succ ln) (r `V.cons` rs) ls
            pure $! Level mr' V.empty `V.cons` ls'
          -- This tiering level is not yet full. We move the completed merged run
          -- into the level proper, and start the new merge for the incoming runs.
          LevelTiering -> do
            let mergelast = mergeLastForLevel ls
            mr' <- newMerge LevelTiering mergelast ln rs'
            traceWith tr $ AtLevel ln
                         $ TraceAddRun
                            (runFsPathsRunNumber $ Run.runRunFsPaths r)
                            (V.map (runFsPathsRunNumber . Run.runRunFsPaths) rs)
            pure $! Level mr' (r `V.cons` rs) `V.cons` ls
          -- The final level is using levelling. If the existing completed merge
          -- run is too large for this level, we promote the run to the next
          -- level and start merging the incoming runs into this (otherwise
          -- empty) level .
          LevelLevelling | runSize r > maxRunSize' conf LevelLevelling ln -> do
            assert (V.null rs && V.null ls) $ pure ()
            mr' <- newMerge LevelTiering Merge.MidLevel ln rs'
            ls' <- go (succ ln) (V.singleton r) V.empty
            pure $! Level mr' V.empty `V.cons` ls'
          -- Otherwise we start merging the incoming runs into the run.
          LevelLevelling -> do
            assert (V.null rs && V.null ls) $ pure ()
            mr' <- newMerge LevelLevelling Merge.LastLevel ln (rs' `V.snoc` r)
            pure $! Level mr' V.empty `V.cons` V.empty

    expectCompletedMerge :: LevelNo -> MergingRun (PrimState m) (Handle h) -> m (Run (PrimState m) (Handle h))
    expectCompletedMerge ln (SingleRun r)                   = do
      traceWith tr $ AtLevel ln $ TraceExpectCompletedMergeSingleRun (runFsPathsRunNumber $ Run.runRunFsPaths r)
      pure r
    expectCompletedMerge ln (MergingRun (CompletedMerge r)) = do
      traceWith tr $ AtLevel ln $ TraceExpectCompletedMerge (runFsPathsRunNumber $ Run.runRunFsPaths r)
      pure r

    -- TODO: Until we implement proper scheduling, this does not only start a
    -- merge, but it also steps it to completion.
    newMerge :: MergePolicyForLevel
             -> Merge.Level
             -> LevelNo
             -> V.Vector (Run (PrimState m) (Handle h))
             -> m (MergingRun (PrimState m) (Handle h))
    newMerge mergepolicy mergelast ln rs
      | Just (r, rest) <- V.uncons rs
      , V.null rest = do
          traceWith tr $ AtLevel ln $ TraceNewMergeSingleRun (Run.runNumEntries r) (runFsPathsRunNumber $ Run.runRunFsPaths r)
          pure (SingleRun r)
      | otherwise = do
        assert (let l = V.length rs in l >= 2 && l <= 5) $ pure ()
        !n <- incrUniqCounter uniqC
        let !caching = diskCachePolicyForLevel confDiskCachePolicy ln
            !alloc = bloomFilterAllocForLevel conf ln
            !runPaths = Paths.runPath root n
        traceWith tr $ AtLevel ln $ TraceNewMerge (V.map Run.runNumEntries rs) (runFsPathsRunNumber runPaths) caching alloc mergepolicy mergelast
        r <- allocateTemp reg
               (mergeRuns resolve hfs hbio caching alloc runPaths mergelast rs)
               (Run.removeReference hfs hbio)
        traceWith tr $ AtLevel ln $ TraceCompletedMerge (Run.runNumEntries r) (runFsPathsRunNumber $ Run.runRunFsPaths r)
        V.mapM_ (freeTemp reg . Run.removeReference hfs hbio) rs
        pure $! MergingRun (CompletedMerge r)

data MergePolicyForLevel = LevelTiering | LevelLevelling
  deriving stock Show

mergePolicyForLevel :: MergePolicy -> LevelNo -> Levels s h -> MergePolicyForLevel
mergePolicyForLevel MergePolicyLazyLevelling (LevelNo n) nextLevels
  | n == 1
  , V.null nextLevels
  = LevelTiering    -- always use tiering on first level
  | V.null nextLevels = LevelLevelling  -- levelling on last level
  | otherwise         = LevelTiering

runSize :: Run s h -> NumEntries
runSize run = Run.runNumEntries run

-- $setup
-- >>> import Database.LSMTree.Internal.Entry
-- >>> import Database.LSMTree.Internal.Config

-- | Compute the maximum size of a run for a given level.
--
-- The @size@ of a tiering run at each level is allowed to be
-- @bufferSize*sizeRatio^(level-1) < size <= bufferSize*sizeRatio^level@.
--
-- >>> unNumEntries . maxRunSize Four (AllocNumEntries (NumEntries 2)) LevelTiering . LevelNo <$> [0, 1, 2, 3, 4]
-- [0,2,8,32,128]
--
-- The @size@ of a levelling run at each level is allowed to be
-- @bufferSize*sizeRatio^(level-1) < size <= bufferSize*sizeRatio^(level+1)@. A
-- levelling run can take take up a whole level, so the maximum size of a run is
-- @sizeRatio*@ larger than the maximum size of a tiering run on the same level.
--
-- >>> unNumEntries . maxRunSize Four (AllocNumEntries (NumEntries 2)) LevelLevelling . LevelNo <$> [0, 1, 2, 3, 4]
-- [0,8,32,128,512]
maxRunSize :: SizeRatio
           -> WriteBufferAlloc
           -> MergePolicyForLevel
           -> LevelNo
           -> NumEntries
maxRunSize (sizeRatioInt -> sizeRatio) (AllocNumEntries (NumEntries bufferSize))
           policy (LevelNo ln) =
    NumEntries $ case policy of
      LevelLevelling -> runSizeTiering * sizeRatio
      LevelTiering   -> runSizeTiering
  where
    runSizeTiering
      | ln < 0 = error "maxRunSize: non-positive level number"
      | ln == 0 = 0
      | otherwise = bufferSize * sizeRatio ^ (pred ln)

maxRunSize' :: TableConfig -> MergePolicyForLevel -> LevelNo -> NumEntries
maxRunSize' config policy ln =
    maxRunSize (confSizeRatio config) (confWriteBufferAlloc config) policy ln

mergeLastForLevel :: Levels s h -> Merge.Level
mergeLastForLevel levels
 | V.null levels = Merge.LastLevel
 | otherwise     = Merge.MidLevel

levelIsFull :: SizeRatio -> V.Vector (Run s h) -> Bool
levelIsFull sr rs = V.length rs + 1 >= (sizeRatioInt sr)

{-# SPECIALISE mergeRuns :: ResolveSerialisedValue -> HasFS IO h -> HasBlockIO IO h -> RunDataCaching -> RunBloomFilterAlloc -> RunFsPaths -> Merge.Level -> V.Vector (Run RealWorld (Handle h)) -> IO (Run RealWorld (Handle h)) #-}
mergeRuns ::
     m ~ IO
  => ResolveSerialisedValue
  -> HasFS m h
  -> HasBlockIO m h
  -> RunDataCaching
  -> RunBloomFilterAlloc
  -> RunFsPaths
  -> Merge.Level
  -> V.Vector (Run (PrimState m) (Handle h))
  -> m (Run (PrimState m) (Handle h))
mergeRuns resolve hfs hbio caching alloc runPaths mergeLevel runs = do
    Merge.new hfs hbio caching alloc mergeLevel resolve runPaths (toList runs) >>= \case
      Nothing -> error "mergeRuns: no inputs"
      Just merge -> go merge
  where
    go m =
      Merge.steps hfs hbio m 1024 >>= \case
        (_, Merge.MergeInProgress)   -> go m
        (_, Merge.MergeComplete run) -> return run

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
      pure $! V.sum (V.map (\((_ :: (Bool, Word64)), rs) -> 1 + V.length rs) runNumbers)

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
      let (label', runNumbers, conf) = read . BSC.unpack . BSC.toStrict $ bs
      let conf' = applyOverride override conf
      unless (label == label') $ throwIO (ErrSnapshotWrongType snap)
      let runPaths = V.map (bimap (second $ RunFsPaths (Paths.activeDir $ sessionRoot seshEnv))
                                  (V.map (RunFsPaths (Paths.activeDir $ sessionRoot seshEnv))))
                           runNumbers
      with (openLevels hfs hbio (confDiskCachePolicy conf') runPaths) $ \lvls -> do
        am <- newArenaManager
        (newWith sesh seshEnv conf' am WB.empty lvls)

{-# SPECIALISE openLevels :: HasFS IO h -> HasBlockIO IO h -> DiskCachePolicy -> V.Vector ((Bool, RunFsPaths), V.Vector RunFsPaths) -> Managed IO (Levels RealWorld (FS.Handle h)) #-}
-- | Open multiple levels.
--
-- If an error occurs when opening multiple runs in sequence, then we have to
-- make sure that all runs that have been succesfully opened already are closed
-- again. The 'Managed' monad allows us to fold 'bracketOnError's over an @m@
-- action.
--
-- TODO: 'Managed' is actually not properly exception-safe, because an async
-- exception can be raised just after a 'bracketOnError's, but still inside the
-- 'bracketOnError' surrounding it. We don't just need folding of
-- 'bracketOnError', we need to release all runs inside the same mask! We should
-- use something like 'TempRegistry'.
openLevels ::
     m ~ IO -- TODO: replace by @io-classes@ constraints for IO simulation.
  => HasFS m h
  -> HasBlockIO m h
  -> DiskCachePolicy
  -> V.Vector ((Bool, RunFsPaths), V.Vector RunFsPaths)
  -> Managed m (Levels (PrimState m) (Handle h))
openLevels hfs hbio diskCachePolicy levels =
    flip V.imapMStrict levels $ \i (mrPath, rsPaths) -> do
      let ln      = LevelNo (i+1) -- level 0 is the write buffer
          caching = diskCachePolicyForLevel diskCachePolicy ln
      !r <- Managed $ bracketOnError
                        (Run.openFromDisk hfs hbio caching (snd mrPath))
                        (Run.removeReference hfs hbio)
      !rs <- flip V.mapMStrict rsPaths $ \run ->
        Managed $ bracketOnError
                    (Run.openFromDisk hfs hbio caching run)
                    (Run.removeReference hfs hbio)
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
                (Run.addReference (tableHasFS thEnv) r)
                (\_ -> Run.removeReference (tableHasFS thEnv) (tableHasBlockIO thEnv) r)
            pure content
          newWith
            (tableSession thEnv)
            (tableSessionEnv thEnv)
            (tableConfig th)
            (tableHandleArenaManager th)
            (tableWriteBuffer content)
            (tableLevels content)
