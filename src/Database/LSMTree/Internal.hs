{-# LANGUAGE DeriveAnyClass             #-}
{-# LANGUAGE DerivingStrategies         #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE RecordWildCards            #-}

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
    -- ** Implementation of public API
  , withTable
  , new
  , close
  , lookups
  , toNormalLookupResult
  , updates
    -- * Snapshots
  , snapshot
  , open
    -- * configuration
  , TableConfig (..)
  , resolveMupsert
  , ResolveMupsert (..)
  , SizeRatio (..)
  , MergePolicy (..)
  , BloomFilterAlloc (..)
  ) where

import           Control.Concurrent.Class.MonadMVar.Strict
import           Control.Concurrent.ReadWriteVar (RWVar)
import qualified Control.Concurrent.ReadWriteVar as RW
import           Control.Monad (unless, void)
import           Control.Monad.Class.MonadThrow
import           Data.BloomFilter (Bloom)
import qualified Data.ByteString.Char8 as BSC
import           Data.Either (fromRight)
import           Data.Foldable
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import           Data.Maybe (fromMaybe)
import qualified Data.Set as Set
import qualified Data.Vector as V
import           Data.Word (Word32, Word64)
import           Database.LSMTree.Internal.Assertions (assertNoThunks)
import           Database.LSMTree.Internal.BlobRef
import           Database.LSMTree.Internal.Entry (Entry (..), combineMaybe)
import           Database.LSMTree.Internal.IndexCompact (IndexCompact)
import           Database.LSMTree.Internal.Lookup (BatchSize (..),
                     lookupsInBatches)
import           Database.LSMTree.Internal.Managed
import qualified Database.LSMTree.Internal.Merge as Merge
import qualified Database.LSMTree.Internal.Normal as Normal
import           Database.LSMTree.Internal.Paths (RunFsPaths (..),
                     SessionRoot (..), SnapshotName)
import qualified Database.LSMTree.Internal.Paths as Paths
import           Database.LSMTree.Internal.Run (Run)
import qualified Database.LSMTree.Internal.Run as Run
import           Database.LSMTree.Internal.Serialise (ResolveMupsert (..),
                     SerialisedBlob, SerialisedKey, SerialisedValue,
                     resolveConst)
import qualified Database.LSMTree.Internal.Vector as V
import           Database.LSMTree.Internal.WriteBuffer (WriteBuffer)
import qualified Database.LSMTree.Internal.WriteBuffer as WB
import qualified GHC.IO.Handle as GHC
import           NoThunks.Class
import qualified System.FS.API as FS
import           System.FS.API (FsErrorPath, FsPath, Handle, HasFS)
import qualified System.FS.API.Lazy as FS
import qualified System.FS.API.Strict as FS
import qualified System.FS.BlockIO.API as FS
import qualified System.FS.BlockIO.API as HasBlockIO
import           System.FS.BlockIO.API (HasBlockIO, IOCtxParams (..))
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
  | ErrExpectedNormalTable
  deriving (Show, Exception)

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
      --
      -- TODO: RWVars only work in IO, not in IOSim.
      --
      -- TODO: how fair is an RWVar?
      sessionState :: RWVar (SessionState m h)
    }

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
    -- NOTE: table are assigned unique identifiers using 'sessionUniqCounter' to
    -- ensure that modifications to the set of known tables are independent.
    -- Each identifier is added only once in 'new', and is deleted only once in
    -- 'close'. A table only make modifications for their own identifier. This
    -- means that modifications can be made concurrently in any order without
    -- further restrictions.
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
withOpenSession sesh action = RW.with (sessionState sesh) $ \case
    SessionClosed -> throwIO ErrSessionClosed
    SessionOpen seshEnv -> action seshEnv

-- | An atomic counter for producing unique 'Word64' values.
newtype UniqCounter m = UniqCounter (StrictMVar m Word64)

{-# INLINE newUniqCounter #-}
newUniqCounter :: MonadMVar m => m (UniqCounter m)
newUniqCounter = UniqCounter <$> newMVar 0

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

    mkSession lockFile = do
        counterVar <- newUniqCounter
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
        mkSession sessionFileLock

    restoreSession sessionFileLock = do
        -- If the layouts are wrong, we throw an exception, and the lock file
        -- is automatically released by bracketOnError.
        checkTopLevelDirLayout
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
    RW.modify_ sessionState $ \case
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
      tableConfig      :: !TableConfig
      -- | The primary purpose of this 'RWVar' is to ensure consistent views of
      -- the open-/closedness of a table when multiple threads require access to
      -- the table's fields (see 'withOpenTable'). We use more fine-grained
      -- synchronisation for various mutable parts of an open table.
      --
      -- TODO: RWVars only work in IO, not in IOSim.
      --
      -- TODO: how fair is an RWVar?
    , tableHandleState :: !(RWVar (TableHandleState m h))
    }

-- | A table handle may assume that its corresponding session is still open as
-- long as the table handle is open. A session's global resources, and therefore
-- resources that are inherited by the table, will only be released once the
-- session is sure that no tables are open anymore.
data TableHandleState m h =
    TableHandleOpen !(TableHandleEnv m h)
  | TableHandleClosed

data TableHandleEnv m h = TableHandleEnv {
    -- === Session
    -- | Inherited from session
    tableSessionRoot         :: !SessionRoot
    -- | Inherited from session
  , tableHasFS               :: !(HasFS m h)
    -- | Inherited from session
  , tableHasBlockIO          :: !(HasBlockIO m h)
    -- | Inherited from session
  , tablesSessionUniqCounter :: !(UniqCounter m)
    -- | Open tables are tracked in the corresponding session, so when a table
    -- is closed it should become untracked (forgotten).
  , tableSessionUntrackTable :: !(m ())
    -- === Table-specific
    -- | All of the state being in a single `StrictMVar` is a relatively simple
    -- solution, but there could be more concurrency. For example, while inserts
    -- are in progress, lookups could still look at the old state without
    -- waiting for the MVar.
    -- TODO: switch to more fine-grained synchronisation approach
  , tableContent             :: !(StrictMVar m (TableContent h))
  }

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
withOpenTable th action = RW.with (tableHandleState th) $ \case
    TableHandleClosed -> throwIO ErrTableClosed
    TableHandleOpen thEnv -> action thEnv

type Levels h = V.Vector (Level h)

-- | Runs in order from newer to older
newtype Level h = Level {
    residentRuns :: V.Vector (Run h)
{- TODO: this is where ongoing merges appear once we implement scheduling ,
  incomingRuns :: MergingRun
-}
  }
  deriving newtype NoThunks

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
  where rs = V.concatMap residentRuns lvls

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
new sesh conf = withOpenSession sesh $ \seshEnv -> newWithLevels seshEnv conf V.empty

{-# SPECIALISE new :: Session IO h -> TableConfig -> IO (TableHandle IO h) #-}
newWithLevels ::
     m ~ IO -- TODO: replace by @io-classes@ constraints for IO simulation.
  => SessionEnv m h
  -> TableConfig
  -> Levels (Handle h)
  -> m (TableHandle m h)
newWithLevels seshEnv conf !levels = do
    assertNoThunks levels $ pure ()
    -- The session is kept open until we've updated the session's set of tracked
    -- tables. If 'closeSession' is called by another thread while this code
    -- block is being executed, that thread will block until it reads the
    -- /updated/ set of tracked tables.
    contentVar <- newMVar $ TableContent WB.empty levels (mkLevelsCache levels)
    tableId <- incrUniqCounter (sessionUniqCounter seshEnv)
    -- Action to untrack the current table
    let forget = modifyMVar_ (sessionOpenTables seshEnv) $ pure . Map.delete tableId
    tableVar <- RW.new $ TableHandleOpen $ TableHandleEnv {
          tableSessionRoot = sessionRoot seshEnv
        , tableHasFS = sessionHasFS seshEnv
        , tableHasBlockIO = sessionHasBlockIO seshEnv
        , tablesSessionUniqCounter = sessionUniqCounter seshEnv
        , tableSessionUntrackTable = forget
        , tableContent = contentVar
        }
    let !th = TableHandle conf tableVar
    -- Track the current table
    modifyMVar_ (sessionOpenTables seshEnv) $ pure . Map.insert tableId th
    pure $! th

{-# SPECIALISE close :: TableHandle IO h -> IO () #-}
-- | See 'Database.LSMTree.Normal.close'.
close ::
     m ~ IO  -- TODO: replace by @io-classes@ constraints for IO simulation.
  => TableHandle m h
  -> m ()
close th = RW.modify_ (tableHandleState th) $ \case
    TableHandleClosed -> pure TableHandleClosed
    TableHandleOpen thEnv -> do
      -- Since we have a write lock on the table state, we know that we are the
      -- only thread currently closing the table. We can safely make the session
      -- forget about this table.
      tableSessionUntrackTable thEnv
      lvls <- tableLevels <$> readMVar (tableContent thEnv)
      V.forM_ lvls $ \Level{residentRuns} ->
        V.forM_ residentRuns $ Run.removeReference (tableHasFS thEnv)
      pure TableHandleClosed

{-# SPECIALISE lookups :: V.Vector SerialisedKey -> TableHandle IO h -> (Maybe (Entry SerialisedValue (BlobRef (Run (Handle h)))) -> lookupResult) -> IO (V.Vector lookupResult) #-}
-- | See 'Database.LSMTree.Normal.lookups'.
lookups ::
     m ~ IO -- TODO: replace by @io-classes@ constraints for IO simulation.
  => V.Vector SerialisedKey
  -> TableHandle m h
  -> (Maybe (Entry SerialisedValue (BlobRef (Run (Handle h)))) -> lookupResult)
     -- ^ How to map from an entry to a lookup result. Use
     -- 'toNormalLookupResult' or 'toMonoidalLookupResult'.
  -> m (V.Vector lookupResult)
lookups ks th fromEntry = withOpenTable th $ \thEnv -> do
    let ResolveMupsert resolve = resolveMupsert (tableConfig th)
    tableContent <- readMVar (tableContent thEnv)
    let !wb = tableWriteBuffer tableContent
    let !cache = tableCache tableContent
    ioRes <-
      lookupsInBatches
        (tableHasBlockIO thEnv)
        (BatchSize $ ioctxBatchSizeLimit (FS.getParams (tableHasBlockIO thEnv)))
        resolve
        (cachedRuns cache)
        (cachedFilters cache)
        (cachedIndexes cache)
        (cachedKOpsFiles cache)
        ks
    pure $! V.zipWithStrict
              (\k1 e2 -> fromEntry $ combineMaybe resolve (WB.lookup wb k1) e2)
              ks ioRes

toNormalLookupResult :: Maybe (Entry v b) -> Normal.LookupResult v b
toNormalLookupResult = \case
    Just e -> case e of
      Insert v            -> Normal.Found v
      InsertWithBlob v br -> Normal.FoundWithBlob v br
      Mupdate _           -> error "toNormalLookupResult: Mupdate unexpected"
      Delete              -> Normal.NotFound
    Nothing -> Normal.NotFound

{-# SPECIALISE updates :: V.Vector (SerialisedKey, Entry SerialisedValue SerialisedBlob) -> TableHandle IO h -> IO () #-}
-- | See 'Database.LSMTree.Normal.updates'.
--
-- Does not enforce that mupsert and blobs should not occur in the same table.
updates ::
     m ~ IO -- TODO: replace by @io-classes@ constraints for IO simulation.
  => V.Vector (SerialisedKey, Entry SerialisedValue SerialisedBlob)
  -> TableHandle m h
  -> m ()
updates es th = withOpenTable th $ \thEnv -> do
    modifyMVar_ (tableContent thEnv) $ \tableContent -> do
      let !wb = WB.addEntries (resolveMupsert (tableConfig th)) es $
                  tableWriteBuffer tableContent
      if WB.sizeInBytes wb <= fromIntegral (confWriteBufferAlloc (tableConfig th))
        then return tableContent { tableWriteBuffer = wb }
        else flushWriteBuffer (tableConfig th) thEnv (tableLevels tableContent) wb

{-# INLINE flushWriteBuffer #-}
flushWriteBuffer ::
     m ~ IO
  => TableConfig
  -> TableHandleEnv m h
  -> Levels (Handle h)
  -> WriteBuffer
  -> m (TableContent h)
flushWriteBuffer config thEnv levels wb = do
    n <- incrUniqCounter (tablesSessionUniqCounter thEnv)
    let runPaths = Paths.runPath (tableSessionRoot thEnv) n
    run <- Run.fromWriteBuffer (tableHasFS thEnv) runPaths wb
    levels' <- addRunToLevels config thEnv run levels
    return TableContent {
        tableWriteBuffer = WB.empty
      , tableLevels = levels'
      , tableCache = mkLevelsCache levels'
      }

addRunToLevels ::
     m ~ IO
  => TableConfig
  -> TableHandleEnv m h
  -> Run (Handle h)
  -> Levels (Handle h)
  -> m (Levels (Handle h))
addRunToLevels config@TableConfig {..} thEnv = go 1
  where
    go !_ run (V.uncons -> Nothing) =
        -- Make a new level
        return $ V.singleton $ Level $ V.singleton run

    go !n run (V.uncons -> Just (Level runs, nextLevels)) = do
        -- Add to top level. If full, merge and continue.
        let level' = Level (V.cons run runs)
        let policyForLevel = mergePolicyForLevel confMergePolicy n nextLevels

        if levelNeedsMerging confSizeRatio policyForLevel level'
          then do
            let mergeLevel = if V.null nextLevels then Merge.LastLevel
                                                  else Merge.MidLevel
            mergedRun <- mergeRuns thEnv (resolveMupsert config) mergeLevel level'
            -- The input runs are not used for this table any more.
            -- We can remove the references here (potentially closing the runs)
            -- before the new LSM structure is stored in the handle, since all
            -- other operations are blocked until we put back the MVar.
            -- TODO: Actually, this is problematic if merging fails, since we
            -- then keep the old runs in the MVar, but removed the references.
            for_ (residentRuns level') $ \r ->
              Run.removeReference (tableHasFS thEnv) r
            -- The run might still fit on this level due to compaction.
            if runSize run <= maxRunSize config policyForLevel n
              then return $ V.cons (Level (V.singleton mergedRun)) nextLevels
              else V.cons (Level V.empty) <$> go (n + 1) mergedRun nextLevels
          else do
            return $ V.cons level' nextLevels

data MergePolicyForLevel = LevelTiering | LevelLevelling

mergePolicyForLevel :: MergePolicy -> Int -> Levels h -> MergePolicyForLevel
mergePolicyForLevel MergePolicyLazyLevelling n nextLevels
  | n == 1            = LevelTiering    -- always use tiering on first level
  | V.null nextLevels = LevelLevelling  -- levelling on last level
  | otherwise         = LevelTiering

levelNeedsMerging :: SizeRatio -> MergePolicyForLevel -> Level h -> Bool
levelNeedsMerging sizeRatio policy (Level runs) =
    V.length runs >= numRunsToMerge
  where
    numRunsToMerge = case policy of
        LevelLevelling -> 2
        LevelTiering   -> sizeRatioInt sizeRatio

maxRunSize :: TableConfig -> MergePolicyForLevel -> Int -> Int
maxRunSize config policy levelNumber = case policy of
    LevelLevelling -> runSizeTiering * sizeRatio
    LevelTiering   -> runSizeTiering
  where
    sizeRatio = sizeRatioInt (confSizeRatio config)
    bufferSize = fromIntegral (confWriteBufferAlloc config)
    runSizeTiering
      | levelNumber < 1 = error "maxRunSize: non-positive level number"
      | otherwise = bufferSize * sizeRatio ^ (levelNumber - 1)

-- TODO: pass 'confBloomFilterAlloc' down to the merge and use it to initialise
-- bloom filters
mergeRuns ::
     m ~ IO
  => TableHandleEnv m h
  -> ResolveMupsert
  -> Merge.Level
  -> Level (Handle h)
  -> m (Run (Handle h))
mergeRuns thEnv resolve mergeLevel (Level runs) = do
    n <- incrUniqCounter (tablesSessionUniqCounter thEnv)
    let runPaths = Paths.runPath (tableSessionRoot thEnv) n
    Merge.new (tableHasFS thEnv) mergeLevel resolve runPaths (toList runs) >>= \case
      Nothing -> error "mergeRuns: no inputs"
      Just merge -> go merge
  where
    go m =
      Merge.steps (tableHasFS thEnv) m 1024 >>= \case
        (_, Merge.MergeInProgress)   -> go m
        (_, Merge.MergeComplete run) -> return run

{-------------------------------------------------------------------------------
  Snapshots
-------------------------------------------------------------------------------}

{-# SPECIALISE snapshot :: SnapshotName -> TableHandle IO h -> IO Int #-}
-- |  See 'Database.LSMTree.Normal.snapshot''.
snapshot ::
     m ~ IO -- TODO: replace by @io-classes@ constraints for IO simulation.
  => SnapshotName
  -> TableHandle m h
  -> m Int
snapshot snap th = do
    withOpenTable th $ \thEnv -> do
      -- For the temporary implementation it is okay to just flush the buffer
      -- before taking the snapshot.
      runNumbers <- modifyMVar (tableContent thEnv) $ \tc -> do
        tc' <- flushWriteBuffer (tableConfig th) thEnv (tableLevels tc) (tableWriteBuffer tc)
        return $ (,) tc' $
          V.map (V.map (runNumber . Run.runRunFsPaths) . residentRuns) $
            tableLevels tc'

      FS.withFile
        (tableHasFS thEnv)
        (Paths.snapshot (tableSessionRoot thEnv) snap)
        (FS.WriteMode FS.MustBeNew) $ \h ->
          void $ FS.hPutAllStrict (tableHasFS thEnv) h
                      (BSC.pack $ show (runNumbers, tableConfig th))

      pure $! V.sum (V.map V.length runNumbers)

{-# SPECIALISE open :: Session IO h -> SnapshotName -> IO (TableHandle IO h) #-}
-- |  See 'Database.LSMTree.Normal.open'.
open ::
     m ~ IO -- TODO: replace by @io-classes@ constraints for IO simulation.
  => Session m h
  -> SnapshotName
  -> m (TableHandle m h)
open sesh snap = do
    withOpenSession sesh $ \seshEnv -> do
      let hfs = sessionHasFS seshEnv
      bs <- FS.withFile
              hfs
              (Paths.snapshot (sessionRoot seshEnv) snap)
              FS.ReadMode $ \h ->
                FS.hGetAll (sessionHasFS seshEnv) h
      let (runNumbers, conf) = read . BSC.unpack . BSC.toStrict $ bs
          runPaths = V.map (V.map (RunFsPaths (Paths.activeDir $ sessionRoot seshEnv))) runNumbers
      with (openLevels hfs runPaths) $ newWithLevels seshEnv conf

{-# SPECIALISE openLevels :: HasFS IO h -> V.Vector (V.Vector RunFsPaths) -> Managed IO (Levels (FS.Handle h)) #-}
-- | Open multiple levels.
--
-- If an error occurs when opening multiple runs in sequence, then we have to
-- make sure that all runs that have been succesfully opened already are closed
-- again. The 'Managed' monad allows us to fold 'bracketOnError's over an @m@
-- action.
openLevels ::
     m ~ IO -- TODO: replace by @io-classes@ constraints for IO simulation.
  => HasFS m h
  -> V.Vector (V.Vector RunFsPaths)
  -> Managed m (Levels (Handle h))
openLevels hfs levels =
    flip V.mapMStrict levels $ \level -> fmap Level $
      flip V.mapMStrict level $ \run ->
        Managed $ bracketOnError
                    (Run.openFromDisk hfs run)
                    (Run.removeReference hfs)

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
    confMergePolicy      :: !MergePolicy
    -- Size ratio between the capacities of adjacent levels.
  , confSizeRatio        :: !SizeRatio
    -- | Total number of bytes that the write buffer can use.
    --
    -- The maximum is 4GiB, which should be more than enough for realistic
    -- applications.
  , confWriteBufferAlloc :: !Word32
  , confBloomFilterAlloc :: !BloomFilterAlloc
    -- | Function for resolving 'Mupsert' values. This should be 'Nothing' for
    -- normal tables and 'Just' for monoidal tables.
  , confResolveMupsert   :: !(Maybe ResolveMupsert)
  }
  deriving Show

-- | TODO: this should be removed once we have proper snapshotting with proper
-- persistence of the config to disk.
deriving instance Read TableConfig

resolveMupsert :: TableConfig -> ResolveMupsert
resolveMupsert conf = fromMaybe resolveConst (confResolveMupsert conf)

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
  deriving (Show, Eq)

-- | TODO: this should be removed once we have proper snapshotting with proper
-- persistence of the config to disk.
deriving instance Read MergePolicy

data SizeRatio = Four
  deriving (Show, Eq)

sizeRatioInt :: SizeRatio -> Int
sizeRatioInt = \case Four -> 4

-- This size calculation is different than for the write buffer:
-- Instead of considering only keys and values themselves, there is
-- overhead from the page format.
-- TODO: is this a good idea?
runSize :: Run h -> Int
runSize run = Run.sizeInPages run * 4096  -- assumes pageSize = 4096

-- | TODO: this should be removed once we have proper snapshotting with proper
-- persistence of the config to disk.
deriving instance Read SizeRatio

-- | Allocation method for bloom filters.
--
-- NOTE: a __physical__ database entry is a key\/operation pair that exists in a
-- file, i.e., a run. Multiple physical entries that have the same key
-- constitute a __logical__ database entry.
data BloomFilterAlloc =
    -- | Allocate a fixed number of bits per physical entry in each bloom
    -- filter.
    AllocFixed
      Word32 -- ^ Bits per physical entry.
  | -- | Allocate as many bits as required per physical entry to get the requested
    -- false-positive rate. Do this for each bloom filter.
    AllocRequestFPR
      Double -- ^ Requested FPR.
{- TODO: disabled for now
  | -- | Allocate bits amongst all bloom filters according to the Monkey algorithm.
    --
    -- TODO: add more configuration paramters that the Monkey algorithm might
    -- need once we implement it.
    AllocMonkey
      Word32 -- ^ Total number of bytes that bloom filters can use collectively.
             --
             -- The maximum is 4GiB, which should be more than enough for
             -- realistic applications.
 -}
  deriving (Show, Eq)

-- | TODO: this should be removed once we have proper snapshotting with proper
-- persistence of the config to disk.
deriving instance Read BloomFilterAlloc

-- | TODO: this should be removed once we have proper snapshotting with proper
-- persistence of the config to disk.
instance Show ResolveMupsert where show _ = "ResolveMupsert function can not be printed"
instance Read ResolveMupsert where readsPrec = error "ResolveMupsert function can not be read"
