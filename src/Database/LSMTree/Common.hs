{-# LANGUAGE FlexibleContexts         #-}
{-# LANGUAGE StandaloneKindSignatures #-}

-- TODO: remove once the API is implemented.
{-# OPTIONS_GHC -Wno-redundant-constraints #-}
{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Database.LSMTree.Common (
    -- * IOLike
    IOLike
    -- * Sessions
  , Session
  , newSession
  , closeSession
    -- * Constraints
  , SomeSerialisationConstraint (..)
  , SomeUpdateConstraint (..)
    -- * Small types
  , Range (..)
    -- * Snapshot names
  , SnapshotName
  , mkSnapshotName
  ) where

import           Control.Concurrent.Class.MonadMVar (MonadMVar)
import           Control.Concurrent.Class.MonadSTM (MonadSTM, STM)
import           Control.Monad.Class.MonadThrow (MonadCatch, MonadThrow)
import           Data.Bits (shiftR, (.&.))
import qualified Data.ByteString as BS
import           Data.Kind (Type)
import           Data.Word (Word64)
import qualified System.FilePath.Posix
import qualified System.FilePath.Windows
import           System.FS.API (FsPath, SomeHasFS)

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
-- are combined using 'union'. Sharing is preserved by snapshots, using
-- 'snapshot' and 'open'.
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
-- Sessions support both related and independent tables. Related tables are
-- created using 'duplicate', while independent tables can be created using
-- 'new'. It is possible to have multiple independent tables with different
-- configuration and key and value types in the same session. Similarly,
-- a session can have both \"normal\" and \"monoidal\" tables. For independent
-- tables (that are not involved in a 'union') one has a choice between using
-- multiple sessions or a shared session. Using multiple sessions requires
-- using separate directories, while a shared session will place all files
-- under one directory.
--
type Session :: (Type -> Type) -> Type
data Session m = Session {
    sessionRoot  :: !FsPath
  , sessionHasFS :: !(SomeHasFS m)
  }

-- | Create either a new empty table session or open an existing table session,
-- given the path to the session directory.
--
-- A new empty table session is created if the given directory is entirely
-- empty. Otherwise it is intended to open an existing table session.
--
-- Exceptions:
--
-- * This can throw exceptions if the directory does not have the expected file
--   layout for a table session
-- * It will throw an exception if the session is already open (in the current
--   process or another OS process)
--
-- Sessions should be closed using 'closeSession' when no longer needed.
--
newSession ::
     IOLike m
  => SomeHasFS m
  -> FsPath -- ^ Path to the session directory
  -> m (Session m)
newSession = undefined

-- | Close the table session.
--
-- This also closes any open table handles in the session. It would typically
-- be good practice however to close all table handles first rather than
-- relying on this for cleanup.
--
-- Closing a table session allows the session to be opened again elsewhere, for
-- example in a different process. Note that the session will be closed
-- automatically if the processes is terminated (in particular the session file
-- lock will be released).
--
closeSession :: IOLike m => Session m -> m ()
closeSession = undefined

{-------------------------------------------------------------------------------
  Serialization constraints
-------------------------------------------------------------------------------}

-- | A placeholder class for (de)serialisation constraints.
--
-- TODO: Should be replaced with whatever (de)serialisation class we eventually
-- want to use. Some prerequisites:
-- *  Serialisation/deserialisation should preserve ordering.
class SomeSerialisationConstraint a where
    serialise :: a -> BS.ByteString

    -- Note: cannot fail.
    deserialise :: BS.ByteString -> a

instance SomeSerialisationConstraint BS.ByteString where
    serialise = id
    deserialise = id

-- | A placeholder class for constraints on 'Update's.
--
-- TODO: should be replaced by the actual constraints we want to use. Some
-- prerequisites:
-- * Combining/merging/resolving 'Update's should be associative.
-- * Should include a function that determines whether it is safe to remove an
--   'Update' from the last level of an LSM tree.
--
class SomeUpdateConstraint a where
    merge :: a -> a -> a

instance SomeUpdateConstraint BS.ByteString where
    merge = (<>)

-- | MSB, so order is preserved.
instance SomeSerialisationConstraint Word64 where
    -- TODO: optimize me when SomeSerialisationConstraint is replaced with its
    -- final version
    serialise w = BS.pack [b1,b2,b3,b4,b5,b6,b7,b8] where
        b8 = fromIntegral $        w    .&. 0xff
        b7 = fromIntegral $ shiftR w  8 .&. 0xff
        b6 = fromIntegral $ shiftR w 16 .&. 0xff
        b5 = fromIntegral $ shiftR w 24 .&. 0xff
        b4 = fromIntegral $ shiftR w 32 .&. 0xff
        b3 = fromIntegral $ shiftR w 40 .&. 0xff
        b2 = fromIntegral $ shiftR w 48 .&. 0xff
        b1 = fromIntegral $ shiftR w 56 .&. 0xff

    -- TODO: optimize me when SomeSerialisationConstraint is replaced with its
    -- final version
    deserialise = BS.foldl' (\acc d -> acc * 0x100 + fromIntegral d) 0

{-------------------------------------------------------------------------------
  Small auxiliary types
-------------------------------------------------------------------------------}

-- | A range of keys.
--
-- TODO: consider adding key prefixes to the range type.
data Range k =
    -- | Inclusive lower bound, exclusive upper bound
    FromToExcluding k k
    -- | Inclusive lower bound, inclusive upper bound
  | FromToIncluding k k
  deriving (Show, Eq)

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
