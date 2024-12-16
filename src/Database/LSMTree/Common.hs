module Database.LSMTree.Common (
    -- * IOLike
    IOLike
    -- * Exceptions
  , Internal.LSMTreeError (..)
    -- * Tracing
  , Internal.LSMTreeTrace (..)
  , Internal.TableTrace (..)
  , Internal.MergeTrace (..)
    -- * Sessions
  , Session
  , withSession
  , openSession
  , closeSession
    -- * Serialisation constraints
  , SerialiseKey (..)
  , SerialiseValue (..)
    -- * Small types
  , Internal.Range (..)
    -- * Snapshots
  , SnapshotLabel (..)
  , deleteSnapshot
  , listSnapshots
    -- ** Snapshot names
  , Internal.SnapshotName
  , Internal.mkSnapshotName
    -- * Blob references
  , BlobRef (..)
    -- * Table configuration
  , TableConfig
  , Internal.defaultTableConfig
  , Internal.SizeRatio (..)
  , Internal.MergePolicy (..)
  , Internal.WriteBufferAlloc (..)
  , Internal.NumEntries (..)
  , Internal.BloomFilterAlloc (..)
  , Internal.defaultBloomFilterAlloc
  , Internal.FencePointerIndex (..)
  , Internal.DiskCachePolicy (..)
  , Internal.MergeSchedule (..)
  , Internal.defaultMergeSchedule
    -- * Table configuration override
  , Internal.TableConfigOverride
  , Internal.configNoOverride
  , Internal.configOverrideDiskCachePolicy
  ) where

import           Control.Concurrent.Class.MonadMVar.Strict
import           Control.Concurrent.Class.MonadSTM (MonadSTM, STM)
import           Control.Monad.Class.MonadST
import           Control.Monad.Class.MonadThrow
import           Control.Monad.Primitive (PrimMonad)
import           Control.Tracer (Tracer)
import           Data.Kind (Type)
import           Data.Typeable (Typeable)
import qualified Database.LSMTree.Internal as Internal
import qualified Database.LSMTree.Internal.BlobRef as Internal
import qualified Database.LSMTree.Internal.Config as Internal
import qualified Database.LSMTree.Internal.Entry as Internal
import qualified Database.LSMTree.Internal.MergeSchedule as Internal
import qualified Database.LSMTree.Internal.Paths as Internal
import qualified Database.LSMTree.Internal.Range as Internal
import           Database.LSMTree.Internal.Serialise.Class
import           Database.LSMTree.Internal.Snapshot (SnapshotLabel (..))
import           System.FS.API (FsPath, HasFS)
import           System.FS.BlockIO.API (HasBlockIO)
import           System.FS.IO (HandleIO)

{-------------------------------------------------------------------------------
  IOLike
-------------------------------------------------------------------------------}

-- | Utility class for grouping @io-classes@ constraints.
class ( MonadMVar m, MonadSTM m, MonadThrow (STM m), MonadThrow m, MonadCatch m
      , MonadMask m, PrimMonad m, MonadST m
      ) => IOLike m

instance IOLike IO

{-------------------------------------------------------------------------------
  Sessions
-------------------------------------------------------------------------------}

-- | A session provides context that is shared across multiple tables.
--
-- Sessions are needed to support sharing between multiple table instances.
-- Sharing occurs when tables are duplicated using 'duplicate', or when tables
-- are combined using 'union'. Sharing is not fully preserved by snapshots:
-- existing runs are shared, but ongoing merges are not. As such, restoring of
-- snapshots (using 'open') is expensive, but snapshotting (using 'snapshot') is
-- relatively cheap.
--
-- The \"monoidal\" table types support a 'union' operation, which has the
-- constraint that the two input tables must be from the same 'Session'.
--
-- Each session places files for table data under a given directory. It is
-- not permitted to open multiple sessions for the same directory at once.
-- Instead a session should be opened once and shared for all uses of
-- tables. This restriction implies that tables cannot be shared between OS
-- processes. The restriction is enforced using file locks.
--
-- Sessions support both related and unrelated tables. Related tables are
-- created using 'duplicate', while unrelated tables can be created using 'new'.
-- It is possible to have multiple unrelated tables with different configuration
-- and key and value types in the same session. Similarly, a session can have
-- both \"normal\" and \"monoidal\" tables. For unrelated tables (that are not
-- involved in a 'union') one has a choice between using multiple sessions or a
-- shared session. Using multiple sessions requires using separate directories,
-- while a shared session will place all files under one directory.
--
type Session :: (Type -> Type) -> Type
type Session = Internal.Session'

{-# SPECIALISE withSession ::
     Tracer IO Internal.LSMTreeTrace
  -> HasFS IO HandleIO
  -> HasBlockIO IO HandleIO
  -> FsPath
  -> (Session IO -> IO a)
  -> IO a #-}
-- | (Asynchronous) exception-safe, bracketed opening and closing of a session.
--
-- If possible, it is recommended to use this function instead of 'openSession'
-- and 'closeSession'.
withSession ::
     (IOLike m, Typeable h)
  => Tracer m Internal.LSMTreeTrace
  -> HasFS m h
  -> HasBlockIO m h
  -> FsPath
  -> (Session m -> m a)
  -> m a
withSession tr hfs hbio dir action = Internal.withSession tr hfs hbio dir (action . Internal.Session')

{-# SPECIALISE openSession ::
     Tracer IO Internal.LSMTreeTrace
  -> HasFS IO HandleIO
  -> HasBlockIO IO HandleIO
  -> FsPath
  -> IO (Session IO) #-}
-- | Create either a new empty table session or open an existing table session,
-- given the path to the session directory.
--
-- A new empty table session is created if the given directory is entirely
-- empty. Otherwise it is intended to open an existing table session.
--
-- Sessions should be closed using 'closeSession' when no longer needed.
-- Consider using 'withSession' instead.
--
-- Exceptions:
--
-- * Throws an exception if the directory does not exist (regardless of whether
--   it is empty or not).
--
-- * This can throw exceptions if the directory does not have the expected file
--   layout for a table session
--
-- * It will throw an exception if the session is already open (in the current
--   process or another OS process)
openSession ::
     forall m h. (IOLike m, Typeable h)
  => Tracer m Internal.LSMTreeTrace
  -> HasFS m h
  -> HasBlockIO m h -- TODO: could we prevent the user from having to pass this in?
  -> FsPath -- ^ Path to the session directory
  -> m (Session m)
openSession tr hfs hbio dir = Internal.Session' <$> Internal.openSession tr hfs hbio dir

{-# SPECIALISE closeSession :: Session IO -> IO () #-}
-- | Close the table session. 'closeSession' is idempotent. All subsequent
-- operations on the session or the tables within it will throw an exception.
--
-- This also closes any open tables and cursors in the session. It would
-- typically be good practice however to close all tables first rather
-- than relying on this for cleanup.
--
-- Closing a table session allows the session to be opened again elsewhere, for
-- example in a different process. Note that the session will be closed
-- automatically if the process is terminated (in particular the session file
-- lock will be released).
--
closeSession :: IOLike m => Session m -> m ()
closeSession (Internal.Session' sesh) = Internal.closeSession sesh

{-------------------------------------------------------------------------------
  Snapshots
-------------------------------------------------------------------------------}

{-# SPECIALISE deleteSnapshot ::
     Session IO
  -> Internal.SnapshotName
  -> IO () #-}
-- | Delete a named snapshot.
--
-- NOTE: has similar behaviour to 'removeDirectory'.
--
-- Exceptions:
--
-- * Deleting a snapshot that doesn't exist is an error.
deleteSnapshot ::
     IOLike m
  => Session m
  -> Internal.SnapshotName
  -> m ()
deleteSnapshot (Internal.Session' sesh) = Internal.deleteSnapshot sesh

{-# SPECIALISE listSnapshots ::
     Session IO
  -> IO [Internal.SnapshotName] #-}
-- | List snapshots by name.
listSnapshots ::
     IOLike m
  => Session m
  -> m [Internal.SnapshotName]
listSnapshots (Internal.Session' sesh) = Internal.listSnapshots sesh

{-------------------------------------------------------------------------------
  Blob references
-------------------------------------------------------------------------------}

-- | A handle-like reference to an on-disk blob. The blob can be retrieved based
-- on the reference.
--
-- Blob comes from the acronym __Binary Large OBject (BLOB)__ and in many
-- database implementations refers to binary data that is larger than usual
-- values and is handled specially. In our context we will allow optionally a
-- blob associated with each value in the table.
--
-- Though blob references are handle-like, they /do not/ keep files open. As
-- such, when a blob reference is returned by a lookup, modifying the
-- corresponding table, cursor, or session /may/ cause the blob reference
-- to be invalidated (i.e.., the blob has gone missing because the blob file was
-- removed). These operations include:
--
-- * Updates (e.g., inserts, deletes, mupserts)
-- * Closing tables
-- * Closing cursors
-- * Closing sessions
--
-- An invalidated blob reference will throw an exception when used to look up a
-- blob. Note that operations such as snapshotting, duplication and cursor reads
-- do /not/ invalidate blob references. These operations do not modify the
-- logical contents or state of a table.
--
-- [Blob reference validity] as long as the table or cursor that the blob
-- reference originated from is not updated or closed, the blob reference will
-- be valid.
--
-- Exception: currently the 'snapshot' operation /also/ invalidates 'BlobRef's,
-- but it should not do. See <https://github.com/IntersectMBO/lsm-tree/issues/392>
--
-- TODO: get rid of the @m@ parameter?
type BlobRef :: (Type -> Type) -> Type -> Type
type role BlobRef nominal nominal
data BlobRef m b where
    BlobRef :: Typeable h
            => Internal.WeakBlobRef m h
            -> BlobRef m b

instance Show (BlobRef m b) where
    showsPrec d (BlobRef b) = showsPrec d b

{-------------------------------------------------------------------------------
  Table configurations
-------------------------------------------------------------------------------}

type TableConfig = Internal.TableConfig'
