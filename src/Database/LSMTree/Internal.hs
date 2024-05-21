{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE LambdaCase     #-}

-- TODO: remove once the top-level bindings are in use
{-# OPTIONS_GHC -Wno-unused-top-binds #-}

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
    -- ** Implementation of public API
  , new
  , close
    -- * configuration
  , TableConfig (..)
  , ResolveMupsert (..)
  , SizeRatio (..)
  , MergePolicy (..)
  , BloomFilterAlloc (..)
  ) where

import           Control.Concurrent.Class.MonadMVar.Strict
import           Control.Concurrent.ReadWriteVar (RWVar)
import qualified Control.Concurrent.ReadWriteVar as RW
import           Control.Monad (unless)
import           Control.Monad.Class.MonadThrow
import           Data.BloomFilter (Bloom)
import           Data.Either (fromRight)
import           Data.Foldable
import           Data.Set (Set)
import qualified Data.Set as Set
import qualified Data.Text as Text
import qualified Data.Vector as V
import           Data.Word (Word32)
import           Database.LSMTree.Internal.IndexCompact (IndexCompact)
import           Database.LSMTree.Internal.Run (Run)
import qualified Database.LSMTree.Internal.Run as Run
import           Database.LSMTree.Internal.Serialise (SerialisedKey,
                     SerialisedValue)
import           Database.LSMTree.Internal.WriteBuffer (WriteBuffer)
import qualified Database.LSMTree.Internal.WriteBuffer as WB
import qualified GHC.IO.Handle as GHC
import qualified System.FS.API as FS
import           System.FS.API (FsErrorPath, FsPath, Handle, HasFS)
import           System.FS.BlockIO.API (HasBlockIO)
import           System.FS.IO (HandleIO)
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
    -- will thrown this exception. There is one exception (pun intended) to this
    -- rule: the idempotent operation 'Database.LSMTree.Common.closeSession'.
  | ErrSessionClosed
  deriving (Show, Exception)

{-------------------------------------------------------------------------------
  Session
-------------------------------------------------------------------------------}

-- | A session provides context that is shared across multiple table handles.
--
-- For more information, see 'Database.LSMTree.Common.Session'.
--
-- Concurrent access to a session is mediated by a multiple-read, single-write
-- lock. This allows concurrent read access, but only sequential write access.
-- TODO: more fine-grained concurrent access than a top-level read-write lock.
newtype Session m h = Session {
      -- TODO: RWVars only work in IO, not in IOSim.
      --
      -- TODO: how fair is an RWVar?
      sessionState :: RWVar (SessionState m h)
    }

data SessionState m h =
    SessionOpen {-# UNPACK #-} !(SessionEnv m h)
  | SessionClosed

data SessionEnv m h = SessionEnv {
    -- | The path to the directory in which this sesion is live. This is a path
    -- relative to root of the 'HasFS' instance.
    sessionRoot        :: !FsPath
  , sessionHasFS       :: !(HasFS m h)
  , sessionHasBlockIO  :: !(HasBlockIO m h)
    -- TODO: add file locking to HasFS, because this one only works in IO.
  , sessionLockFile    :: !GHC.Handle
    -- | A session-wide shared, atomic counter that is used to produce unique
    -- names for run files.
  , sessionUniqCounter :: !(StrictMVar m Integer)
    -- | These tables are recorded here so they can be closed once the session
    -- is closed.
  , sessionKnownTables :: !(StrictMVar m (Set (TableHandle m h)))
  }

{-# SPECIALISE withOpenSession :: Session IO HandleIO -> (SessionEnv IO HandleIO -> IO a) -> IO a #-}
withOpenSession ::
     m ~ IO -- TODO: replace by @io-classes@ constraints for IO simulation.
  => Session m h
  -> (SessionEnv m h -> m a)
  -> m a
withOpenSession sesh action = RW.with (sessionState sesh) $ \case
    SessionClosed -> throwIO ErrSessionClosed
    SessionOpen seshEnv -> action seshEnv

--
-- Implementation of public API
--

{-# SPECIALISE withSession :: HasFS IO HandleIO -> HasBlockIO IO HandleIO -> FsPath -> (Session IO HandleIO -> IO a) -> IO a #-}
-- | See 'Database.LSMTree.Common.withSession'.
withSession ::
     m ~ IO -- TODO: replace by @io-classes@ constraints for IO simulation.
  => HasFS m h
  -> HasBlockIO m h
  -> FsPath
  -> (Session m h -> m a)
  -> m a
withSession hfs hbio dir =
    bracket
      (openSession hfs hbio dir)
      closeSession

{-# SPECIALISE openSession :: HasFS IO HandleIO -> HasBlockIO IO HandleIO -> FsPath -> IO (Session IO HandleIO) #-}
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
    -- TODO: add combinators like (</>) for FsPaths to fs-api. The current
    -- conversion to and from lists of Text is not very nice.
    lockFilePath     = FS.fsPathFromList (FS.fsPathToList dir <> [Text.pack "lock"])
    activeDirPath    = FS.fsPathFromList (FS.fsPathToList dir <> [Text.pack "active"])
    snapshotsDirPath = FS.fsPathFromList (FS.fsPathToList dir <> [Text.pack "snapshots"])

    acquireLock path = fromRight Nothing <$>
        try @m @SomeException
          (do lockFile <- System.openFile path System.WriteMode
              success <- GHC.hTryLock lockFile GHC.ExclusiveLock
              pure $ if success then Just lockFile else Nothing
          )

    mkSession lockFile = do
        counterVar <- newMVar 0
        knownTablesVar <- newMVar Set.empty
        sessionVar <- RW.new $ SessionOpen $ SessionEnv {
            sessionRoot = dir
          , sessionHasFS = hfs
          , sessionHasBlockIO = hbio
          , sessionLockFile = lockFile
          , sessionUniqCounter = counterVar
          , sessionKnownTables = knownTablesVar
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

{-# SPECIALISE closeSession :: Session IO HandleIO -> IO () #-}
-- | See 'Database.LSMTree.Common.closeSession'.
closeSession ::
     m ~ IO -- TODO: replace by @io-classes@ constraints for IO simulation.
  => Session m h
  -> m ()
closeSession Session{sessionState} =
    RW.modify_ sessionState $ \case
      SessionClosed -> pure SessionClosed
      SessionOpen SessionEnv {sessionLockFile} -> do
        -- TODO: close known table handles
        System.hClose sessionLockFile
        pure SessionClosed

{-------------------------------------------------------------------------------
  Table handle
-------------------------------------------------------------------------------}

-- | A handle to an on-disk key\/value table.
--
-- For more information, see 'Database.LSMTree.Normal.TableHandle'.
--
-- Concurrent access to a table handle is mediated by a multiple-read,
-- single-write lock. This allows concurrent read access, but only sequential
-- write access. TODO: more fine-grained concurrent access than a top-level
-- read-write lock.
newtype TableHandle m h = TableHandle {
      -- TODO: RWVars only work in IO, not in IOSim.
      --
      -- TODO: how fair is an RWVar?
      tableHandleState :: RWVar (TableHandleState m h)
    }

data TableHandleState m h =
    TableHandleOpen {-# UNPACK #-} !(TableHandleEnv m h)
  | TableHandleClosed

data TableHandleEnv m h = TableHandleEnv {
    -- === Inherited from Session
    tableSessionRoot :: !FsPath
  , tableHasFS       :: !(HasFS m h)
  , tableHasBlockIO  :: !(HasBlockIO m h)
    -- === Table-specific
  , tableConfig      :: !TableConfig
  , tableWriteBuffer :: !(StrictMVar m (WriteBuffer))
    -- | A hierarchy of levels. The vector indexes double as level numbers.
  , tableLevels      :: !(StrictMVar m (V.Vector (Level (Handle h))))
    -- | Cache of flattened 'levels'.
    --
    -- INVARIANT: when 'level's is modified, this cache should be updated as
    -- well, for example using 'mkLevelsCache'.
  , tableCache       :: !(StrictMVar m (LevelsCache (Handle h)))
  }

-- | Runs in order from newer to older
newtype Level h = Level {
    residentRuns :: V.Vector (Run h)
{- TODO: this is where ongoing merges appear once we implement scheduling ,
  incomingRuns :: MergingRun
-}
  }

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
mkLevelsCache :: V.Vector (Level h) -> LevelsCache h
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

{-# SPECIALISE new :: Session IO HandleIO -> TableConfig -> IO (TableHandle IO HandleIO) #-}
-- | See 'Database.LSMTree.Normal.new'.
new ::
     m ~ IO -- TODO: replace by @io-classes@ constraints for IO simulation.
  => Session m h
  -> TableConfig
  -> m (TableHandle m h)
new sesh conf = do
    withOpenSession sesh $ \seshEnv -> do
      writeBufVar <- newMVar (WB.empty)
      levelsVar <- newMVar V.empty
      cacheVar <- newMVar (mkLevelsCache V.empty)
      tableVar <- RW.new $ TableHandleOpen $ TableHandleEnv {
            tableSessionRoot = sessionRoot seshEnv
          , tableHasFS = sessionHasFS seshEnv
          , tableHasBlockIO = sessionHasBlockIO seshEnv
          , tableConfig = conf
          , tableWriteBuffer = writeBufVar
          , tableLevels = levelsVar
          , tableCache = cacheVar
          }
      pure $! TableHandle tableVar

{-# SPECIALISE close :: TableHandle IO HandleIO -> IO () #-}
-- | See 'Database.LSMTree.Normal.close'.
close ::
     m ~ IO  -- TODO: replace by @io-classes@ constraints for IO simulation.
  => TableHandle m h
  -> m ()
close th = RW.modify_ (tableHandleState th) $ \case
    TableHandleClosed -> pure TableHandleClosed
    TableHandleOpen thEnv -> do
      lvls <- readMVar (tableLevels thEnv)
      V.forM_ lvls $ \Level{residentRuns} ->
        V.forM_ residentRuns $ Run.removeReference (tableHasFS thEnv)
      pure TableHandleClosed

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
    -- normal tables.
  , confResolveMupsert   :: !(Maybe ResolveMupsert)
  }
  deriving Show

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

data SizeRatio = Four
  deriving (Show, Eq)

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

newtype ResolveMupsert = ResolveMupsert (SerialisedValue -> SerialisedValue -> SerialisedValue)
instance Show ResolveMupsert where show _ = "ResolveMupsert function can not be printed"
