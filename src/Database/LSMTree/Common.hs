module Database.LSMTree.Common (
    -- * IOLike
    IOLike
    -- * Exceptions
  , Internal.LSMTreeError (..)
    -- * Sessions
  , Session (..)
  , withSession
  , openSession
  , closeSession
    -- * Serialisation constraints
  , SerialiseKey (..)
  , SerialiseValue (..)
    -- * Small types
  , Internal.Range (..)
    -- * Snapshots
  , Internal.SnapshotLabel
  , Labellable (..)
  , deleteSnapshot
  , listSnapshots
    -- ** Snapshot names
  , Internal.SnapshotName
  , Internal.mkSnapshotName
    -- * Blob references
  , BlobRef (..)
  ) where

import           Control.Concurrent.Class.MonadMVar.Strict
import           Control.Concurrent.Class.MonadSTM (MonadSTM, STM)
import           Control.DeepSeq
import           Control.Monad.Class.MonadThrow
import           Data.Kind (Type)
import           Data.Typeable (Proxy, Typeable)
import qualified Database.LSMTree.Internal as Internal
import qualified Database.LSMTree.Internal.BlobRef as Internal
import qualified Database.LSMTree.Internal.Paths as Internal
import qualified Database.LSMTree.Internal.Range as Internal
import qualified Database.LSMTree.Internal.Run as Internal
import           Database.LSMTree.Internal.Serialise.Class
import           System.FS.API (FsPath, HasFS)
import           System.FS.BlockIO.API (HasBlockIO)
import           System.FS.IO (HandleIO)

{-------------------------------------------------------------------------------
  IOLike
-------------------------------------------------------------------------------}

-- | Utility class for grouping @io-classes@ constraints.
class ( MonadMVar m, MonadSTM m, MonadThrow (STM m), MonadThrow m, MonadCatch m
      , m ~ IO -- TODO: temporary constraint until we add I/O fault testing.
               -- Don't forget to specialise your functions! @m ~ IO@ in a
               -- function constraint will not produce to an IO-specialised
               -- function.
      ) => IOLike m

instance IOLike IO

{-------------------------------------------------------------------------------
  Sessions
-------------------------------------------------------------------------------}

-- | A session provides context that is shared across multiple table handles.
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
data Session m = forall h. Typeable h => Session !(Internal.Session m h)

instance NFData (Session m) where
  rnf (Session s) = rnf s

{-# SPECIALISE withSession :: HasFS IO HandleIO -> HasBlockIO IO HandleIO -> FsPath -> (Session IO -> IO a) -> IO a #-}
-- | (Asynchronous) exception-safe, bracketed opening and closing of a session.
--
-- If possible, it is recommended to use this function instead of 'openSession'
-- and 'closeSession'.
withSession ::
     (IOLike m, Typeable h)
  => HasFS m h
  -> HasBlockIO m h
  -> FsPath
  -> (Session m -> m a)
  -> m a
withSession hfs hbio dir action = Internal.withSession hfs hbio dir (action . Session)

{-# SPECIALISE openSession :: HasFS IO HandleIO -> HasBlockIO IO HandleIO -> FsPath -> IO (Session IO) #-}
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
  => HasFS m h
  -> HasBlockIO m h -- TODO: could we prevent the user from having to pass this in?
  -> FsPath -- ^ Path to the session directory
  -> m (Session m)
openSession hfs hbio dir = Session <$> Internal.openSession hfs hbio dir

{-# SPECIALISE closeSession :: Session IO -> IO () #-}
-- | Close the table session. 'closeSession' is idempotent. All subsequent
-- operations on the session or the tables within it will throw an exception.
--
-- This also closes any open table handles in the session. It would typically
-- be good practice however to close all table handles first rather than
-- relying on this for cleanup.
--
-- Closing a table session allows the session to be opened again elsewhere, for
-- example in a different process. Note that the session will be closed
-- automatically if the process is terminated (in particular the session file
-- lock will be released).
--
closeSession :: IOLike m => Session m -> m ()
closeSession (Session sesh) = Internal.closeSession sesh

{-------------------------------------------------------------------------------
  Snapshots
-------------------------------------------------------------------------------}

-- TODO: we might replace this with some other form of dynamic checking of
-- snapshot types. For example, we could ask the user to produce a label/version
-- directly instead, instead of deriving the label from a type using this type
-- class.
class Labellable a where
  makeSnapshotLabel :: Proxy a -> Internal.SnapshotLabel

{-# SPECIALISE deleteSnapshot :: Session IO -> Internal.SnapshotName -> IO () #-}
-- | Delete a named snapshot.
--
-- NOTE: has similar behaviour to 'removeDirectory'.
--
-- Exceptions:
--
-- * Deleting a snapshot that doesn't exist is an error.
--
-- TODO: this function currently has a temporary implementation until we have
-- proper snapshots.
deleteSnapshot :: IOLike m => Session m -> Internal.SnapshotName -> m ()
deleteSnapshot (Session sesh) = Internal.deleteSnapshot sesh

{-# SPECIALISE listSnapshots :: Session IO -> IO [Internal.SnapshotName] #-}
-- | List snapshots by name.
--
-- TODO: this function currently has a temporary implementation until we have
-- proper snapshots.
listSnapshots :: IOLike m => Session m -> m [Internal.SnapshotName]
listSnapshots (Session sesh) = Internal.listSnapshots sesh

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
-- corresponding table handle (or session) /may/ cause the blob reference to be
-- invalidated (i.e.., the blob has gone missing because the blob file was
-- removed). These operations include:
--
-- * Updates (e.g., inserts, deletes, mupserts)
-- * Closing table handles
-- * Closing sessions
--
-- An invalidated blob reference will throw an exception when used to look up a
-- blob. Note that table operations such as snapshotting and duplication do
-- /not/ invalidate blob references. These operations do not modify the logical
-- contents or state of an existing table.
--
-- [Blob reference validity] as long as the table handle that the blob reference
-- originated from is not updated or closed, the blob reference will be valid.
--
-- TODO: get rid of the @m@ parameter?
type BlobRef :: (Type -> Type) -> Type -> Type
type role BlobRef nominal nominal
data BlobRef m blob = forall h. Typeable h => BlobRef (Internal.BlobRef (Internal.Run h))
