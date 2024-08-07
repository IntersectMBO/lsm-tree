{-# LANGUAGE DataKinds #-}
-- | TODO: this should be removed once we have proper snapshotting with proper
-- persistence of the config to disk.
{-# OPTIONS_GHC -Wno-orphans #-}

module Database.LSMTree.Internal (
    -- * Exceptions
    LSMTreeError (..)
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
  , TableConfigOverride
  , configNoOverride
  , configOverrideDiskCachePolicy
  , deleteSnapshot
  , listSnapshots
    -- * Mutiple writable table handles
  , duplicate
    -- * Configuration
  , TableConfig (..)
  , defaultTableConfig
  , MergePolicy (..)
  , SizeRatio (..)
  , WriteBufferAlloc (..)
  , NumEntries (..)
  , BloomFilterAlloc (..)
  , defaultBloomFilterAlloc
  , FencePointerIndex (..)
  , DiskCachePolicy (..)
    -- * Exported for cabal-docspec
  , MergePolicyForLevel (..)
  , maxRunSize
  , LevelNo (..)
  ) where

import           Control.Concurrent.Class.MonadMVar.Strict
import           Control.Concurrent.Class.MonadSTM (MonadSTM (..))
import           Control.Concurrent.Class.MonadSTM.RWVar (RWVar)
import qualified Control.Concurrent.Class.MonadSTM.RWVar as RW
import           Control.DeepSeq
import           Control.Monad (unless, void, when)
import           Control.Monad.Class.MonadThrow
import           Control.Monad.Primitive (PrimState (..), RealWorld)
import           Control.Monad.ST.Strict (ST, runST)
import           Data.Arena (ArenaManager, newArenaManager)
import           Data.Bifunctor (Bifunctor (..))
import           Data.BloomFilter (Bloom)
import qualified Data.ByteString.Char8 as BSC
import           Data.Char (isNumber)
import           Data.Either (fromRight)
import           Data.Foldable
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import           Data.Maybe (catMaybes, fromMaybe)
import           Data.Monoid (Last (..))
import qualified Data.Set as Set
import qualified Data.Vector as V
import           Data.Word (Word64)
import           Database.LSMTree.Internal.Assertions (assert, assertNoThunks,
                     fromIntegralChecked)
import           Database.LSMTree.Internal.BlobRef
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
import qualified GHC.IO.Handle as GHC
import qualified Monkey
import           NoThunks.Class
import qualified System.FS.API as FS
import           System.FS.API (FsErrorPath, FsPath, Handle, HasFS)
import qualified System.FS.API.Lazy as FS
import qualified System.FS.API.Strict as FS
import qualified System.FS.BlockIO.API as HasBlockIO
import           System.FS.BlockIO.API (HasBlockIO)
import qualified System.IO as System

{-------------------------------------------------------------------------------
  Exceptions
-------------------------------------------------------------------------------}

-- TODO: give this a nicer Show instance.
data LSMTreeError =
    SessionDirDoesNotExist FsErrorPath
  | SessionDirLocked FsErrorPath
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
  Session
-------------------------------------------------------------------------------}

-- | A session provides context that is shared across multiple table handles.
--
-- For more information, see 'Database.LSMTree.Common.Session'.
newtype Session m h = Session {
      -- | The primary purpose of this 'RWVar' is to ensure consistent views of
      -- the open-/closedness of a session when multiple threads require access
      -- to the session's fields (see 'withOpenSession'). We use more
      -- fine-grained synchronisation for various mutable parts of an open
      -- session.
      sessionState :: RWVar m (SessionState m h)
    }

instance NFData (Session m h) where
  rnf (Session a) = rnf a

data SessionState m h =
    SessionOpen !(SessionEnv m h)
  | SessionClosed

data SessionEnv m h = SessionEnv {
    -- | The path to the directory in which this sesion is live. This is a path
    -- relative to root of the 'HasFS' instance.
    sessionRoot        :: !SessionRoot
  , sessionHasFS       :: !(HasFS m h)
  , sessionHasBlockIO  :: !(HasBlockIO m h)
    -- TODO: add file locking to HasFS, because this one only works in IO.
  , sessionLockFile    :: !GHC.Handle
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

{-# SPECIALISE withSession :: HasFS IO h -> HasBlockIO IO h -> FsPath -> (Session IO h -> IO a) -> IO a #-}
-- | See 'Database.LSMTree.Common.withSession'.
withSession ::
     m ~ IO -- TODO: replace by @io-classes@ constraints for IO simulation.
  => HasFS m h
  -> HasBlockIO m h
  -> FsPath
  -> (Session m h -> m a)
  -> m a
withSession hfs hbio dir = bracket (openSession hfs hbio dir) closeSession

{-# SPECIALISE openSession :: HasFS IO h -> HasBlockIO IO h -> FsPath -> IO (Session IO h) #-}
-- | See 'Database.LSMTree.Common.openSession'.
openSession ::
     forall m h. m ~ IO -- TODO: replace by @io-classes@ constraints for IO simulation.
  => HasFS m h
  -> HasBlockIO m h -- TODO: could we prevent the user from having to pass this in?
  -> FsPath -- ^ Path to the session directory
  -> m (Session m h)
openSession hfs hbio dir = do
    dirExists <- FS.doesDirectoryExist hfs dir
    unless dirExists $
      throwIO (SessionDirDoesNotExist (FS.mkFsErrorPath hfs dir))
    -- List directory contents /before/ trying to acquire a file lock, so that
    -- that the lock file does not show up in the listed contents.
    dirContents <- FS.listDirectory hfs dir
    -- TODO: unsafeToFilePath will error if not in IO. Once we enable running
    -- this code in IOSim, we should also make sure we have added file locking
    -- to the HasFS API.
    lf <- FS.unsafeToFilePath hfs lockFilePath
    -- Try to acquire the session file lock as soon as possible to reduce the
    -- risk of race conditions.
    --
    -- The lock is only released when an exception is raised, otherwise the lock
    -- is included in the returned Session.
    bracketOnError
      (acquireLock lf)
      (traverse_ System.hClose) -- also releases the lock
      $ \case
          Nothing -> throwIO (SessionDirLocked (FS.mkFsErrorPath hfs lockFilePath))
          Just sessionFileLock ->
            if Set.null dirContents then newSession sessionFileLock
                                    else restoreSession sessionFileLock
  where
    root             = Paths.SessionRoot dir
    lockFilePath     = Paths.lockFile root
    activeDirPath    = Paths.activeDir root
    snapshotsDirPath = Paths.snapshotsDir root

    acquireLock path = fromRight Nothing <$>
        try @m @SomeException
          (do lockFile <- System.openFile path System.WriteMode
              success <- GHC.hTryLock lockFile GHC.ExclusiveLock
              pure $ if success then Just lockFile else Nothing
          )

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
        pure $! Session sessionVar

    newSession sessionFileLock = do
        FS.createDirectory hfs activeDirPath
        FS.createDirectory hfs snapshotsDirPath
        mkSession sessionFileLock 0

    restoreSession sessionFileLock = do
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
closeSession Session{sessionState} =
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
        HasBlockIO.close (sessionHasBlockIO seshEnv)
        System.hClose (sessionLockFile seshEnv)
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
    }

instance NFData (TableHandle m h) where
  rnf (TableHandle a b c) =
    rnf a `seq` rnf b `seq` rnf c

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
  , tableContent    :: !(RWVar m (TableContent h))
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

data TableContent h = TableContent {
    tableWriteBuffer :: !WriteBuffer
    -- | A hierarchy of levels. The vector indexes double as level numbers.
  , tableLevels      :: !(Levels (Handle h))
    -- | Cache of flattened 'levels'.
    --
    -- INVARIANT: when 'level's is modified, this cache should be updated as
    -- well, for example using 'mkLevelsCache'.
  , tableCache       :: !(LevelsCache (Handle h))
  }

emptyTableContent :: TableContent h
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

type Levels h = V.Vector (Level h)

-- | Runs in order from newer to older
data Level h = Level {
    incomingRuns :: !(MergingRun h)
  , residentRuns :: !(V.Vector (Run h))
  }

-- TODO: proper instance
deriving via OnlyCheckWhnfNamed "Level" (Level h) instance NoThunks (Level h)

newtype LevelNo = LevelNo Int
  deriving stock Eq
  deriving newtype Enum

-- | A merging run is either a single run, or some ongoing merge.
data MergingRun h =
    MergingRun !(MergingRunState h)
  | SingleRun !(Run h)

-- | Merges are stepped to completion immediately, so there is no representation
-- for ongoing merges (yet)
--
-- TODO: this should also represent ongoing merges once we implement scheduling.
newtype MergingRunState h = CompletedMerge (Run h)

-- | Return all runs in the levels, ordered from newest to oldest
runsInLevels :: Levels h -> V.Vector (Run h)
runsInLevels levels = flip V.concatMap levels $ \(Level mr rs) ->
    case mr of
      SingleRun r                   -> r `V.cons` rs
      MergingRun (CompletedMerge r) -> r `V.cons` rs

{-# SPECIALISE closeLevels :: HasFS IO h -> HasBlockIO IO h -> Levels (Handle h) -> IO () #-}
closeLevels ::
     m ~ IO -- TODO: replace by @io-classes@ constraints for IO simulation.
  => HasFS m h
  -> HasBlockIO m h
  -> Levels (Handle h)
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
data LevelsCache h = LevelsCache_ {
    cachedRuns      :: !(V.Vector (Run h))
  , cachedFilters   :: !(V.Vector (Bloom SerialisedKey))
  , cachedIndexes   :: !(V.Vector IndexCompact)
  , cachedKOpsFiles :: !(V.Vector h)
  }

-- | Flatten the argument 'Level's into a single vector of runs, and use that to
-- populate the 'LevelsCache'.
mkLevelsCache :: Levels h -> LevelsCache h
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
new sesh conf = withOpenSession sesh $ \seshEnv -> do
    am <- newArenaManager
    newWith sesh seshEnv conf am WB.empty V.empty

{-# SPECIALISE newWith :: Session IO h -> SessionEnv IO h -> TableConfig -> ArenaManager RealWorld -> WriteBuffer -> Levels (Handle h) -> IO (TableHandle IO h) #-}
newWith ::
     m ~ IO -- TODO: replace by @io-classes@ constraints for IO simulation.
  => Session m h
  -> SessionEnv m h
  -> TableConfig
  -> ArenaManager (PrimState m)
  -> WriteBuffer
  -> Levels (Handle h)
  -> m (TableHandle m h)
newWith sesh seshEnv conf !am !wb !levels = do
    assertNoThunks levels $ pure ()
    -- The session is kept open until we've updated the session's set of tracked
    -- tables. If 'closeSession' is called by another thread while this code
    -- block is being executed, that thread will block until it reads the
    -- /updated/ set of tracked tables.
    contentVar <- RW.new $ TableContent wb levels (mkLevelsCache levels)
    tableId <- incrUniqCounter (sessionUniqCounter seshEnv)
    tableVar <- RW.new $ TableHandleOpen $ TableHandleEnv {
          tableSession = sesh
        , tableSessionEnv = seshEnv
        , tableId = tableId
        , tableContent = contentVar
        }
    let !th = TableHandle conf tableVar am
    -- Track the current table
    modifyMVar_ (sessionOpenTables seshEnv) $ pure . Map.insert tableId th
    pure $! th

{-# SPECIALISE close :: TableHandle IO h -> IO () #-}
-- | See 'Database.LSMTree.Normal.close'.
close ::
     m ~ IO  -- TODO: replace by @io-classes@ constraints for IO simulation.
  => TableHandle m h
  -> m ()
close th = RW.withWriteAccess_ (tableHandleState th) $ \case
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

{-# SPECIALISE lookups :: ResolveSerialisedValue -> V.Vector SerialisedKey -> TableHandle IO h -> (Maybe (Entry SerialisedValue (BlobRef (Run (Handle h)))) -> lookupResult) -> IO (V.Vector lookupResult) #-}
-- | See 'Database.LSMTree.Normal.lookups'.
lookups ::
     m ~ IO -- TODO: replace by @io-classes@ constraints for IO simulation.
  => ResolveSerialisedValue
  -> V.Vector SerialisedKey
  -> TableHandle m h
  -> (Maybe (Entry SerialisedValue (BlobRef (Run (Handle h)))) -> lookupResult)
     -- ^ How to map from an entry to a lookup result.
  -> m (V.Vector lookupResult)
lookups resolve ks th fromEntry = withOpenTable th $ \thEnv -> do
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
    let conf = tableConfig th
    withOpenTable th $ \thEnv -> do
      let hfs = tableHasFS thEnv
      modifyWithTempRegistry_
        (atomically $ RW.unsafeAcquireWriteAccess (tableContent thEnv))
        (atomically . RW.unsafeReleaseWriteAccess (tableContent thEnv)) $ \tc -> do
          tc' <- updatesWithInterleavedFlushes
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

{-# SPECIALISE updatesWithInterleavedFlushes :: TableConfig -> ResolveSerialisedValue -> HasFS IO h -> HasBlockIO IO h -> SessionRoot -> UniqCounter IO -> V.Vector (SerialisedKey, Entry SerialisedValue SerialisedBlob) -> TempRegistry IO -> TableContent h -> IO (TableContent h) #-}
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
     m ~ IO -- TODO: replace by @io-classes@ constraints for IO simulation.
  => TableConfig
  -> ResolveSerialisedValue
  -> HasFS m h
  -> HasBlockIO m h
  -> SessionRoot
  -> UniqCounter m
  -> V.Vector (SerialisedKey, Entry SerialisedValue SerialisedBlob)
  -> TempRegistry m
  -> TableContent h
  -> m (TableContent h)
updatesWithInterleavedFlushes conf resolve hfs hbio root uniqC es reg tc = do
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
      tc'' <- flushWriteBuffer conf resolve hfs hbio root uniqC reg tc'
      -- In the fortunate case where we have already performed all the updates,
      -- return,
      if V.null es' then
        pure $! tc''
      -- otherwise, keep going
      else
        updatesWithInterleavedFlushes conf resolve hfs hbio root uniqC es' reg tc''
  where
    AllocNumEntries (NumEntries maxn) = confWriteBufferAlloc conf
    setWriteBuffer :: WriteBuffer -> TableContent h -> TableContent h
    setWriteBuffer wbToSet tc0 = TableContent {
          tableWriteBuffer = wbToSet
        , tableLevels = tableLevels tc0
        , tableCache = tableCache tc0
        }

{-# SPECIALISE flushWriteBuffer :: TableConfig -> ResolveSerialisedValue -> HasFS IO h -> HasBlockIO IO h -> SessionRoot -> UniqCounter IO -> TempRegistry IO -> TableContent h -> IO (TableContent h) #-}
-- | Flush the write buffer to disk, regardless of whether it is full or not.
--
-- The returned table content contains an updated set of levels, where the write
-- buffer is inserted into level 1.
flushWriteBuffer ::
     m ~ IO -- TODO: replace by @io-classes@ constraints for IO simulation.
  => TableConfig
  -> ResolveSerialisedValue
  -> HasFS m h
  -> HasBlockIO m h
  -> SessionRoot
  -> UniqCounter m
  -> TempRegistry m
  -> TableContent h
  -> m (TableContent h)
flushWriteBuffer conf@TableConfig{confDiskCachePolicy}
                 resolve hfs hbio root uniqC reg tc
  | WB.null (tableWriteBuffer tc) = pure tc
  | otherwise = do
    n <- incrUniqCounter uniqC
    r <- allocateTemp reg
            (Run.fromWriteBuffer hfs hbio
              (diskCachePolicyForLevel confDiskCachePolicy (LevelNo 1))
              (bloomFilterAllocForLevel conf (LevelNo 1))
              (Paths.runPath root n)
              (tableWriteBuffer tc))
            (Run.removeReference hfs hbio)
    levels' <- addRunToLevels conf resolve hfs hbio root uniqC r reg (tableLevels tc)
    pure $! TableContent {
        tableWriteBuffer = WB.empty
      , tableLevels = levels'
      , tableCache = mkLevelsCache levels'
      }

-- | Note that the invariants rely on the fact that levelling is only used on
-- the last level.
--
levelsInvariant :: forall s h. TableConfig -> Levels h -> ST s Bool
levelsInvariant conf levels =
    go (LevelNo 1) levels >>= \ !_ -> pure True
  where
    sr = confSizeRatio conf
    wba = confWriteBufferAlloc conf

    go :: LevelNo -> Levels h -> ST s ()
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

{-# SPECIALISE addRunToLevels :: TableConfig -> ResolveSerialisedValue -> HasFS IO h -> HasBlockIO IO h -> SessionRoot -> UniqCounter IO -> Run (Handle h) -> TempRegistry IO -> Levels (Handle h) -> IO (Levels (Handle h)) #-}
addRunToLevels ::
     forall m h. m ~ IO -- TODO: replace by @io-classes@ constraints for IO simulation.
  => TableConfig
  -> ResolveSerialisedValue
  -> HasFS m h
  -> HasBlockIO m h
  -> SessionRoot
  -> UniqCounter m
  -> Run (Handle h)
  -> TempRegistry m
  -> Levels (Handle h)
  -> m (Levels (Handle h))
addRunToLevels conf@TableConfig{..} resolve hfs hbio root uniqC r0 reg levels = do
    ls' <- go (LevelNo 1) (V.singleton r0) levels
    assert (runST $ levelsInvariant conf ls') $ return ls'
  where
    -- NOTE: @go@ is based on the @increment@ function from the
    -- @ScheduledMerges@ prototype.
    go !ln rs (V.uncons -> Nothing) = do
        -- Make a new level
        let policyForLevel = mergePolicyForLevel confMergePolicy ln V.empty
        mr <- newMerge policyForLevel Merge.LastLevel ln rs
        return $ V.singleton $ Level mr V.empty
    go !ln rs' (V.uncons -> Just (Level mr rs, ls)) = do
        -- TODO: until we have proper scheduling, the merging run is actually
        -- always stepped to completion immediately, so we can see it is just a
        -- single run.
        r <- expectCompletedMerge mr
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

    expectCompletedMerge :: MergingRun (Handle h) -> m (Run (Handle h))
    expectCompletedMerge (SingleRun r)                   = pure r
    expectCompletedMerge (MergingRun (CompletedMerge r)) = pure r

    -- TODO: Until we implement proper scheduling, this does not only start a
    -- merge, but it also steps it to completion.
    newMerge :: MergePolicyForLevel
             -> Merge.Level
             -> LevelNo
             -> V.Vector (Run (Handle h))
             -> m (MergingRun (Handle h))
    newMerge mergepolicy mergelast ln rs
      | Just (r, rest) <- V.uncons rs
      , V.null rest = do
          pure (SingleRun r)
      | otherwise = do
        assert (let l = V.length rs in l >= 2 && l <= 5) $ pure ()
        let caching = diskCachePolicyForLevel confDiskCachePolicy ln
            alloc = bloomFilterAllocForLevel conf ln
        r <- allocateTemp reg
               (mergeRuns resolve hfs hbio root uniqC caching alloc mergepolicy mergelast rs)
               (Run.removeReference hfs hbio)
        V.mapM_ (freeTemp reg . Run.removeReference hfs hbio) rs
        pure $! MergingRun (CompletedMerge r)

data MergePolicyForLevel = LevelTiering | LevelLevelling

mergePolicyForLevel :: MergePolicy -> LevelNo -> Levels h -> MergePolicyForLevel
mergePolicyForLevel MergePolicyLazyLevelling (LevelNo n) nextLevels
  | n == 1
  , V.null nextLevels
  = LevelTiering    -- always use tiering on first level
  | V.null nextLevels = LevelLevelling  -- levelling on last level
  | otherwise         = LevelTiering

runSize :: Run h -> NumEntries
runSize run = Run.runNumEntries run

-- $setup
-- >>> import Database.LSMTree.Internal.Entry

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

mergeLastForLevel :: Levels s -> Merge.Level
mergeLastForLevel levels
 | V.null levels = Merge.LastLevel
 | otherwise     = Merge.MidLevel

levelIsFull :: SizeRatio -> V.Vector (Run h) -> Bool
levelIsFull sr rs = V.length rs + 1 >= (sizeRatioInt sr)

{-# SPECIALISE mergeRuns :: ResolveSerialisedValue -> HasFS IO h -> HasBlockIO IO h -> SessionRoot -> UniqCounter IO -> RunDataCaching -> RunBloomFilterAlloc -> MergePolicyForLevel -> Merge.Level -> V.Vector (Run (Handle h)) -> IO (Run (Handle h)) #-}
mergeRuns ::
     m ~ IO
  => ResolveSerialisedValue
  -> HasFS m h
  -> HasBlockIO m h
  -> SessionRoot
  -> UniqCounter m
  -> RunDataCaching
  -> RunBloomFilterAlloc
  -> MergePolicyForLevel
  -> Merge.Level
  -> V.Vector (Run (Handle h))
  -> m (Run (Handle h))
mergeRuns resolve hfs hbio root uniqC caching alloc _ mergeLevel runs = do
    n <- incrUniqCounter uniqC
    let runPaths = Paths.runPath root n
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

-- | Override configuration options in 'TableConfig' that can be changed dynamically.
--
-- Some parts of the 'TableConfig' are considered fixed after a table is
-- created. That is, these options should (i) should stay the same over the
-- lifetime of a table, and (ii) these options should not be changed when a
-- snapshot is created or loaded. Other options can be changed dynamically
-- without sacrificing correctness.
--
-- This type has 'Semigroup' and 'Monoid' instances for composing override
-- options.
data TableConfigOverride = TableConfigOverride {
      -- | Override for 'confDiskCachePolicy'
      confOverrideDiskCachePolicy  :: Last DiskCachePolicy
    }

-- | Behaves like a point-wise 'Last' instance
instance Semigroup TableConfigOverride where
  override1 <> override2 = TableConfigOverride {
        confOverrideDiskCachePolicy =
          confOverrideDiskCachePolicy override1 <>
          confOverrideDiskCachePolicy override2
      }

-- | Behaves like a point-wise 'Last' instance
instance Monoid TableConfigOverride where
  mempty = configNoOverride

applyOverride :: TableConfigOverride -> TableConfig -> TableConfig
applyOverride TableConfigOverride{..} conf = conf {
      confDiskCachePolicy =
        fromMaybe (confDiskCachePolicy conf) (getLast confOverrideDiskCachePolicy)
    }

configNoOverride :: TableConfigOverride
configNoOverride = TableConfigOverride {
      confOverrideDiskCachePolicy = Last Nothing
    }

configOverrideDiskCachePolicy :: DiskCachePolicy -> TableConfigOverride
configOverrideDiskCachePolicy pol = TableConfigOverride {
      confOverrideDiskCachePolicy = Last (Just pol)
    }

{-# SPECIALISE openLevels :: HasFS IO h -> HasBlockIO IO h -> DiskCachePolicy -> V.Vector ((Bool, RunFsPaths), V.Vector RunFsPaths) -> Managed IO (Levels (FS.Handle h)) #-}
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
  -> Managed m (Levels (Handle h))
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
deleteSnapshot sesh snap =
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
listSnapshots sesh =
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
duplicate th = withOpenTable th $ \thEnv -> do
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

{-------------------------------------------------------------------------------
  Configuration
-------------------------------------------------------------------------------}

-- | Table configuration parameters, including LSM tree tuning parameters.
--
-- Some config options are fixed (for now):
--
-- * Merge policy: Tiering
--
-- * Size ratio: 4
data TableConfig = TableConfig {
    confMergePolicy       :: !MergePolicy
    -- Size ratio between the capacities of adjacent levels.
  , confSizeRatio         :: !SizeRatio
    -- | Total number of bytes that the write buffer can use.
    --
    -- The maximum is 4GiB, which should be more than enough for realistic
    -- applications.
  , confWriteBufferAlloc  :: !WriteBufferAlloc
  , confBloomFilterAlloc  :: !BloomFilterAlloc
  , confFencePointerIndex :: !FencePointerIndex
    -- | The policy for caching key\/value data from disk in memory.
  , confDiskCachePolicy   :: !DiskCachePolicy
  }
  deriving stock (Show, Eq)

instance NFData TableConfig where
  rnf (TableConfig a b c d e f) =
      rnf a `seq` rnf b `seq` rnf c `seq` rnf d `seq` rnf e `seq` rnf f

-- | TODO: this should be removed once we have proper snapshotting with proper
-- persistence of the config to disk.
deriving stock instance Read TableConfig

-- | A reasonable default 'TableConfig'.
--
-- This uses a write buffer with up to 20,000 elements and a generous amount of
-- memory for Bloom filters (FPR of 2%).
--
defaultTableConfig :: TableConfig
defaultTableConfig =
    TableConfig
      { confMergePolicy      = MergePolicyLazyLevelling
      , confSizeRatio        = Four
      , confWriteBufferAlloc = AllocNumEntries (NumEntries 20_000)
      , confBloomFilterAlloc = defaultBloomFilterAlloc
      , confFencePointerIndex = CompactIndex
      , confDiskCachePolicy  = DiskCacheAll
      }



data MergePolicy =
    -- | Use tiering on intermediate levels, and levelling on the last level.
    -- This makes it easier for delete operations to disappear on the last
    -- level.
    MergePolicyLazyLevelling
{- TODO: disabled for now. Would we ever want to provide pure tiering?
  | MergePolicyTiering
-}
{- TODO: disabled for now
  | MergePolicyLevelling
-}
  deriving stock (Show, Eq)

instance NFData MergePolicy where
  rnf MergePolicyLazyLevelling = ()

-- | TODO: this should be removed once we have proper snapshotting with proper
-- persistence of the config to disk.
deriving stock instance Read MergePolicy

data SizeRatio = Four
  deriving stock (Show, Eq)

instance NFData SizeRatio where
  rnf Four = ()

sizeRatioInt :: SizeRatio -> Int
sizeRatioInt = \case Four -> 4

-- | TODO: this should be removed once we have proper snapshotting with proper
-- persistence of the config to disk.
deriving stock instance Read SizeRatio

-- | Allocation method for the write buffer.
data WriteBufferAlloc =
    -- | Total number of key\/value pairs that can be present in the write
    -- buffer before flushing the write buffer to disk.
    --
    -- NOTE: if the sizes of values vary greatly, this can lead to wonky runs on
    -- disk, and therefore unpredictable performance.
    AllocNumEntries !NumEntries
{- TODO: disabled for now
  | -- | Total number of bytes that the write buffer can use.
    --
    -- The maximum is 4GiB, which should be more than enough for realistic
    -- applications.
    AllocTotalBytes !Word32
-}
  deriving stock (Show, Eq)

instance NFData WriteBufferAlloc where
  rnf (AllocNumEntries n) = rnf n

-- | TODO: this should be removed once we have proper snapshotting with proper
-- persistence of the config to disk.
deriving stock instance Read WriteBufferAlloc

-- | TODO: this should be removed once we have proper snapshotting with proper
-- persistence of the config to disk.
deriving stock instance Read NumEntries

-- | Allocation method for bloom filters.
--
-- NOTE: a __physical__ database entry is a key\/operation pair that exists in a
-- file, i.e., a run. Multiple physical entries that have the same key
-- constitute a __logical__ database entry.
data BloomFilterAlloc =
    -- | Allocate a fixed number of bits per physical entry in each bloom
    -- filter.
    AllocFixed
      Word64 -- ^ Bits per physical entry.
  | -- | Allocate as many bits as required per physical entry to get the requested
    -- false-positive rate. Do this for each bloom filter.
    AllocRequestFPR
      Double -- ^ Requested FPR.
  | -- | Allocate bits amongst all bloom filters according to the Monkey algorithm.
    --
    -- The allocation algorithm will never go over the memory budget. If more
    -- levels are added that the algorithm did not account for, then bloom
    -- filters on those levels will be empty. This can happen for a number of
    -- reasons:
    --
    -- * The number of budgeted physical entries is exceeded
    -- * Underfull runs causes levels to be underfull, which causes entries to
    --   reside in larger levels
    --
    -- To combat this, make sure to budget for a generous number of physical
    -- entries.
    AllocMonkey
      Word64 -- ^ Total number of bytes that bloom filters can use collectively.
      NumEntries -- ^ Total number of /physical/ entries expected to be in the database.
  deriving stock (Show, Eq)

instance NFData BloomFilterAlloc where
  rnf (AllocFixed n)        = rnf n
  rnf (AllocRequestFPR fpr) = rnf fpr
  rnf (AllocMonkey a b)     = rnf a `seq` rnf b

-- | TODO: this should be removed once we have proper snapshotting with proper
-- persistence of the config to disk.
deriving stock instance Read BloomFilterAlloc

defaultBloomFilterAlloc :: BloomFilterAlloc
defaultBloomFilterAlloc = AllocFixed 10

bloomFilterAllocForLevel :: TableConfig -> LevelNo -> RunBloomFilterAlloc
bloomFilterAllocForLevel conf (LevelNo l) =
    assert (l > 0) $
    case confBloomFilterAlloc conf of
      AllocFixed n -> RunAllocFixed n
      AllocRequestFPR fpr -> RunAllocRequestFPR fpr
      AllocMonkey totalBits (NumEntries n) ->
        let !sr = sizeRatioInt (confSizeRatio conf)
            !m = case confWriteBufferAlloc conf of
                    AllocNumEntries (NumEntries x) -> x
            !levelCount = Monkey.numLevels (fromIntegral n) (fromIntegral m) (fromIntegral sr)
            !allocPerLevel = Monkey.monkeyBits
                                (fromIntegralChecked totalBits)
                                (fromIntegralChecked n)
                                (fromIntegralChecked sr)
                                levelCount
        in  case allocPerLevel !? (l - 1) of
              -- Default to an empty bloom filter in case the level wasn't
              -- accounted for. See 'AllocMonkey'.
              Nothing     -> RunAllocMonkey 0
              Just (_, x) -> RunAllocMonkey (fromIntegralChecked x)
  where
    -- Copied from "Data.List"
    {-# INLINABLE (!?) #-}
    (!?) :: [a] -> Int -> Maybe a
    xs !? n
      | n < 0     = Nothing
      | otherwise = foldr (\x r k -> case k of
                                      0 -> Just x
                                      _ -> r (k-1)) (const Nothing) xs n

-- | Configure the type of fence pointer index.
--
-- TODO: this configuration option currently has no effect: 'CompactIndex' is
-- always used.
data FencePointerIndex =
    -- | Use a compact fence pointer index.
    --
    -- The compact index type is designed to work with keys that are large
    -- cryptographic hashes, e.g. 32 bytes.
    --
    -- When using the 'IndexCompact', additional constraints apply to the
    -- 'Database.LSMTree.Internal.Serialise.Class.serialiseKey' function. The
    -- __Minimal size__ law should be satisfied:
    --
    -- [Minimal size] @'Database.LSMTree.Internal.RawBytes.size'
    --   ('Database.LSMTree.Internal.Serialise.Class.serialiseKey' x) >= 8@
    --
    -- Use 'Database.LSMTree.Internal.Serialise.Class.serialiseKeyMinimalSize'
    -- to test this law.
    CompactIndex
    -- | Use an ordinary fence pointer index, without any constraints on
    -- serialised keys.
  | OrdinaryIndex
  deriving stock (Show, Eq)

instance NFData FencePointerIndex where
  rnf CompactIndex  = ()
  rnf OrdinaryIndex = ()

-- | TODO: this should be removed once we have proper snapshotting with proper
-- persistence of the config to disk.
deriving stock instance Read FencePointerIndex

-- | The policy for caching data from disk in memory (using the OS page cache).
--
-- Caching data in memory can improve performance if the access pattern has
-- good access locality or if the overall data size fits within memory. On the
-- other hand, caching is determental to performance and wastes memory if the
-- access pattern has poor spatial or temporal locality.
--
-- This implementation is designed to have good performance using a cacheless
-- policy, where main memory is used only to cache Bloom filters and indexes,
-- but none of the key\/value data itself. Nevertheless, some use cases will be
-- faster if some or all of the key\/value data is also cached in memory. This
-- implementation does not do any custom caching of key\/value data, relying
-- simply on the OS page cache. Thus caching is done in units of 4kb disk pages
-- (as opposed to individual key\/value pairs for example).
--
data DiskCachePolicy =

       -- | Use the OS page cache to cache any\/all key\/value data in the
       -- table.
       --
       -- Use this policy if the expected access pattern for the table
       -- has a good spatial or temporal locality.
       DiskCacheAll

       -- | Use the OS page cache to cache data in all LSMT levels at or below
       -- a given level number. For example, use 1 to cache the first level.
       -- (The write buffer is considered to be level 0.)
       --
       -- Use this policy if the expected access pattern for the table
       -- has good temporal locality for recently inserted keys.
     | DiskCacheLevelsAtOrBelow Int

       --TODO: Add a policy based on size in bytes rather than internal details
       -- like levels. An easy use policy would be to say: "cache the first 10
       -- Mb" and have everything worked out from that.

       -- | Do not cache any key\/value data in any level (except the write
       -- buffer).
       --
       -- Use this policy if expected access pattern for the table has poor
       -- spatial or temporal locality, such as uniform random access.
     | DiskCacheNone
  deriving stock (Eq, Show, Read)

instance NFData DiskCachePolicy where
  rnf DiskCacheAll                 = ()
  rnf (DiskCacheLevelsAtOrBelow l) = rnf l
  rnf DiskCacheNone                = ()

-- | Interpret the 'DiskCachePolicy' for a level: should we cache data in runs
-- at this level.
--
diskCachePolicyForLevel :: DiskCachePolicy -> LevelNo -> RunDataCaching
diskCachePolicyForLevel policy (LevelNo ln) =
  case policy of
    DiskCacheAll               -> CacheRunData
    DiskCacheNone              -> NoCacheRunData
    DiskCacheLevelsAtOrBelow n
      | ln <= n                -> CacheRunData
      | otherwise              -> NoCacheRunData
