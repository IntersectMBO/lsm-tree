{-# LANGUAGE DeriveFunctor            #-}
{-# LANGUAGE FlexibleContexts         #-}
{-# LANGUAGE GADTs                    #-}
{-# LANGUAGE RoleAnnotations          #-}
{-# LANGUAGE StandaloneKindSignatures #-}

-- TODO: remove once the API is implemented.
{-# OPTIONS_GHC -Wno-redundant-constraints #-}
{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Database.LSMTree.Common (
    -- * IOLike
    IOLike
    -- * Sessions
  , Session
  , AnySession
  , openSession
  , closeSession
    -- * Constraints
  , SerialiseKey (..)
  , SerialiseValue (..)
  , SomeUpdateConstraint (..)
    -- * Small types
  , Internal.Range (..)
    -- * Snapshots
  , deleteSnapshot
  , listSnapshots
    -- ** Snapshot names
  , SnapshotName
  , mkSnapshotName
    -- * Blob references
  , BlobRef
  ) where

import           Control.Concurrent.Class.MonadMVar (MonadMVar)
import           Control.Concurrent.Class.MonadSTM (MonadSTM, STM)
import           Control.Monad.Class.MonadThrow (MonadCatch, MonadThrow)
import qualified Data.ByteString as BS
import           Data.Kind (Type)
import           Data.Typeable (Typeable)
import qualified Database.LSMTree.Internal.BlobRef as Internal
import qualified Database.LSMTree.Internal.Range as Internal
import qualified Database.LSMTree.Internal.Run as Internal
import           Database.LSMTree.Internal.Serialise.Class
import qualified System.FilePath.Posix
import qualified System.FilePath.Windows
import           System.FS.API (FsPath, HasBufFS, HasFS, SomeHasFS)
import           System.FS.BlockIO.API (HasBlockIO)

{-------------------------------------------------------------------------------
  IOLike
-------------------------------------------------------------------------------}

-- | Utility class for grouping @io-classes@ constraints.
class (MonadMVar m, MonadSTM m, MonadThrow (STM m), MonadThrow m, MonadCatch m) => IOLike m where
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
data Session m = forall h. Typeable h => Session (AnySession m h)

-- | Like 'Session', but exposing its @h@ type parameter
type AnySession :: (Type -> Type) -> Type -> Type
data AnySession m h = AnySession {
    anySessionRoot       :: !FsPath
  , anySessionHasFS      :: !(HasFS m h)
  , anySessionHasBufFS   :: !(HasBufFS m h)
  , anySessionHasBlockIO :: !(HasBlockIO m h)
  }

-- | Create either a new empty table session or open an existing table session,
-- given the path to the session directory.
--
-- A new empty table session is created if the given directory is entirely
-- empty. Otherwise it is intended to open an existing table session.
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
--
-- Sessions should be closed using 'closeSession' when no longer needed.
--
openSession ::
     IOLike m
  => SomeHasFS m
  -> FsPath -- ^ Path to the session directory
  -> m (Session m)
openSession = undefined

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
closeSession = undefined

{-------------------------------------------------------------------------------
  Serialisation constraints
-------------------------------------------------------------------------------}

-- | A placeholder class for constraints on 'Update's.
--
-- === TODO
--
-- This class should be replaced by the actual constraints we want to use. Some
-- prerequisites:
--
-- * Combining\/merging\/resolving 'Update's should be associative.
--
-- * Should include a function that determines whether it is safe to remove an
--   'Update' from the last level of an LSM tree.
--
class SomeUpdateConstraint a where
    mergeU :: a -> a -> a

instance SomeUpdateConstraint BS.ByteString where
    mergeU = (<>)

{-------------------------------------------------------------------------------
  Snapshots
-------------------------------------------------------------------------------}

-- | Delete a named snapshot.
--
-- NOTE: has similar behaviour to 'removeDirectory'.
--
-- Exceptions:
--
-- * Deleting a snapshot that doesn't exist is an error.
--
deleteSnapshot :: IOLike m => Session m -> SnapshotName -> m ()
deleteSnapshot = undefined

-- | List snapshots by name.
--
listSnapshots :: IOLike m => Session m -> m [SnapshotName]
listSnapshots = undefined

{-------------------------------------------------------------------------------
  Snapshot name
-------------------------------------------------------------------------------}

newtype SnapshotName = MkSnapshotName FilePath
  deriving (Eq, Ord)

instance Show SnapshotName where
  showsPrec d (MkSnapshotName p) = showsPrec d p

-- | Create snapshot name.
--
-- The name may consist of lowercase characters, digits, dashes @-@ and underscores @_@.
-- It must be non-empty and less than 65 characters long.
-- It may not be a special filepath name.
--
-- >>> mkSnapshotName "main"
-- Just "main"
--
-- >>> mkSnapshotName "temporary-123-test_"
-- Just "temporary-123-test_"
--
-- >>> map mkSnapshotName ["UPPER", "dir/dot.exe", "..", "\\", "com1", "", replicate 100 'a']
-- [Nothing,Nothing,Nothing,Nothing,Nothing,Nothing,Nothing]
--
mkSnapshotName :: String -> Maybe SnapshotName
mkSnapshotName s
  | all isValid s
  , len > 0
  , len < 65
  , System.FilePath.Posix.isValid s
  , System.FilePath.Windows.isValid s
  = Just (MkSnapshotName s)

  | otherwise
  = Nothing
  where
    len = length s
    isValid c = ('a' <= c && c <= 'z') || ('0' <= c && c <= '9' ) || c `elem` "-_"

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
data BlobRef m blob = forall h. Eq h => BlobRef (Internal.BlobRef (Internal.Run h))
