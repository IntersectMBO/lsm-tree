module Database.LSMTree.Internal.TableHandle (
    -- * Table handle
    TableHandle (..)
  , TableHandleState (..)
  , TableHandleEnv (..)
  , withOpenTable
    -- * Helpers
  , tableSessionRoot
  , tableHasFS
  , tableHasBlockIO
  , tableSessionUniqCounter
    -- ** Implementation of public API
  , withTable
  , new
  , newWith
  , close
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
import           Data.Kind
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import           Data.Maybe (catMaybes)
import qualified Data.Primitive.ByteArray as P
import           Data.Primitive.MutVar
import qualified Data.Set as Set
import           Data.Typeable
import qualified Data.Vector as V
import           Data.Word (Word64)
import           Database.LSMTree.Internal.BlobRef (WeakBlobRef (..))
import qualified Database.LSMTree.Internal.BlobRef as BlobRef
import           Database.LSMTree.Internal.Config
import           Database.LSMTree.Internal.Entry (Entry)
import qualified Database.LSMTree.Internal.Entry as Entry
import           Database.LSMTree.Internal.Lookup (ByteCountDiscrepancy,
                     ResolveSerialisedValue, lookupsIO)
import           Database.LSMTree.Internal.MergeSchedule
import           Database.LSMTree.Internal.Paths (RunFsPaths (..),
                     SessionRoot (..), SnapshotName)
import qualified Database.LSMTree.Internal.Paths as Paths
import           Database.LSMTree.Internal.Range (Range (..))
import qualified Database.LSMTree.Internal.RawBytes as RB
import           Database.LSMTree.Internal.Run (Run)
import qualified Database.LSMTree.Internal.Run as Run
import           Database.LSMTree.Internal.RunNumber
import qualified Database.LSMTree.Internal.RunReader as Reader
import           Database.LSMTree.Internal.RunReaders (OffsetKey (..))
import qualified Database.LSMTree.Internal.RunReaders as Readers
import           Database.LSMTree.Internal.Serialise (SerialisedBlob (..),
                     SerialisedKey, SerialisedValue)
import           Database.LSMTree.Internal.Session
import           Database.LSMTree.Internal.UniqCounter
import qualified Database.LSMTree.Internal.Vector as V
import           Database.LSMTree.Internal.WriteBuffer (WriteBuffer)
import qualified Database.LSMTree.Internal.WriteBuffer as WB
import           Database.LSMTree.Internal.WriteBufferBlobs (WriteBufferBlobs)
import qualified Database.LSMTree.Internal.WriteBufferBlobs as WBB
import qualified System.FS.API as FS
import           System.FS.API (FsError, FsErrorPath (..), FsPath, Handle,
                     HasFS)
import qualified System.FS.API.Lazy as FS
import qualified System.FS.API.Strict as FS
import qualified System.FS.BlockIO.API as FS
import           System.FS.BlockIO.API (HasBlockIO)

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
      blobpath <- Paths.tableBlobPath (sessionRoot seshEnv) <$>
                    incrUniqCounter (sessionUniqCounter seshEnv)
      wbblobs  <- WBB.new (sessionHasFS seshEnv) blobpath
      newWith sesh seshEnv conf am WB.empty wbblobs V.empty

{-# SPECIALISE newWith :: Session IO h -> SessionEnv IO h -> TableConfig -> ArenaManager RealWorld -> WriteBuffer -> WriteBufferBlobs IO h -> Levels IO (Handle h) -> IO (TableHandle IO h) #-}
newWith ::
     m ~ IO -- TODO: replace by @io-classes@ constraints for IO simulation.
  => Session m h
  -> SessionEnv m h
  -> TableConfig
  -> ArenaManager (PrimState m)
  -> WriteBuffer
  -> WriteBufferBlobs m h
  -> Levels m (Handle h)
  -> m (TableHandle m h)
newWith sesh seshEnv conf !am !wb !wbblobs !levels = do
    tableId <- incrUniqCounter (sessionUniqCounter seshEnv)
    let tr = TraceTable (uniqueToWord64 tableId) `contramap` sessionTracer sesh
    traceWith tr $ TraceCreateTableHandle conf
    cache <- mkLevelsCache levels
    -- The session is kept open until we've updated the session's set of tracked
    -- tables. If 'closeSession' is called by another thread while this code
    -- block is being executed, that thread will block until it reads the
    -- /updated/ set of tracked tables.
    contentVar <- RW.new $ TableContent
        { tableWriteBuffer = wb
        , tableWriteBufferBlobs = wbblobs
        , tableLevels = levels
        , tableCache = cache
        }
    tableVar <- RW.new $ TableHandleOpen $ TableHandleEnv {
          tableSession = sesh
        , tableSessionEnv = seshEnv
        , tableId = uniqueToWord64 tableId
        , tableContent = contentVar
        }
    let !th = TableHandle conf tableVar am tr
    -- Track the current table
    modifyMVar_ (sessionOpenTables seshEnv) $
      pure . Map.insert (uniqueToWord64 tableId) (close th)
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
        RW.withWriteAccess_ (tableContent thEnv) $ \tc -> do
          forRunM_ (tableLevels tc) Run.removeReference
          WBB.removeReference (tableWriteBufferBlobs tc)
          pure tc
        pure TableHandleClosed
