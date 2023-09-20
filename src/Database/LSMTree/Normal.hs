{-# LANGUAGE StandaloneDeriving       #-}
{-# LANGUAGE StandaloneKindSignatures #-}
{-# LANGUAGE TupleSections            #-}

-- TODO: remove once the API is implemented.
{-# OPTIONS_GHC -Wno-redundant-constraints #-}
{-# OPTIONS_GHC -Wno-unused-top-binds #-}

-- |
--
-- This module is intended to be imported qualified.
--
-- > import qualified Database.LSMTree.Normal as LSMT
module Database.LSMTree.Normal (
    -- * Temporary placeholder types
    SomeSerialisationConstraint
    -- * Utility types
  , IOLike
    -- * Sessions
  , Session
  , newSession
  , closeSession
    -- * Tables
  , TableHandle
  , TableConfig (..)
  , new
  , close
    -- * Table querying and updates
    -- ** Queries
  , Range (..)
  , LookupResult (..)
  , lookups
  , RangeLookupResult (..)
  , rangeLookup
    -- ** Updates
  , Update (..)
  , updates
  , inserts
  , deletes
    -- ** Blobs
  , BlobRef
  , retrieveBlobs
    -- * Snapshots
  , SnapshotName
  , snapshot
  , open
    -- * Multiple writable table handles
  , duplicate
  ) where

import           Data.Kind (Type)
import           Data.Word (Word64)
import           Database.LSMTree.Common (IOLike, Range (..), Session,
                     SnapshotName, SomeSerialisationConstraint, closeSession,
                     newSession)

{-------------------------------------------------------------------------------
  Tables
-------------------------------------------------------------------------------}

-- | A handle to a table.
--
-- An LSMT table is an individual key value mapping with in-memory and on-disk
-- parts. A table handle is the object/reference by which an in-use LSM table
-- will be operated upon. In our API it identifies a single mutable instance of
-- an LSM table. The multiple-handles feature allows for there to may be many
-- such instances in use at once.
type TableHandle :: (Type -> Type) -> Type -> Type -> Type -> Type
data TableHandle m k v blob = TableHandle {
    thSession :: !(Session m)
  , thConfig  :: !TableConfig
  }

-- | Table configuration parameters, like tuning parameters.
--
-- Some config options are fixed (for now):
-- * Merge policy: Tiering
-- * Size ratio: 4
data TableConfig = TableConfig {
    -- | Total number of bytes that the write buffer can use.
    tcMaxBufferMemory      :: Word64
    -- | Total number of bytes that bloom filters can use collectively.
  , tcMaxBloomFilterMemory :: Word64
    -- | Bit precision for the compact index
    --
    -- TODO: fill in with a realistic, non-unit type.
  , tcBitPrecision         :: ()
  }

-- | Configs should be comparable, because only tables with the same config
-- options are __compatible__.
--
-- For more information about compatibility, see 'Session'.
deriving instance Eq TableConfig
deriving instance Show TableConfig

-- | Create a new table referenced by a table handle.
--
-- NOTE: close table handles using 'close' as soon as they are
-- unused.
new ::
     IOLike m
  => Session m
  -> TableConfig
  -> m (TableHandle m k v blob)
new = undefined

-- | Close a table handle.
--
-- Any on-disk files and in-memory data that are no longer referenced after
-- closing the table handle are lost forever. Use 'Snapshot's to ensure data is
-- not lost.
close ::
     IOLike m
  => TableHandle m k v blob
  -> m ()
close = undefined

{-------------------------------------------------------------------------------
  Table querying and updates
-------------------------------------------------------------------------------}

-- | Result of a single point lookup.
data LookupResult k v blobref =
    NotFound      !k
  | Found         !k !v
  | FoundWithBlob !k !v !blobref
  deriving (Eq, Show)

-- | Perform a batch of lookups.
--
-- Lookups can be performed concurrently from multiple Haskell threads.
lookups ::
     (IOLike m, SomeSerialisationConstraint k, SomeSerialisationConstraint v)
  => [k]
  -> TableHandle m k v blob
  -> m [LookupResult k v (BlobRef blob)]
lookups = undefined

-- | A result for one point in a range lookup.
data RangeLookupResult k v blobref =
    FoundInRange         !k !v
  | FoundInRangeWithBlob !k !v !blobref
  deriving (Eq, Show)

-- | Perform a range lookup.
--
-- Range lookups can be performed concurrently from multiple Haskell threads.
rangeLookup ::
     (IOLike m, SomeSerialisationConstraint k, SomeSerialisationConstraint v)
  => Range k
  -> TableHandle m k v blob
  -> m [RangeLookupResult k v (BlobRef blob)]
rangeLookup = undefined

-- | Normal tables support insert and delete operations.
--
-- An __update__ is a term that groups all types of table-manipulating
-- operations, like inserts and deletes.
data Update v blob =
    Insert !v !(Maybe blob)
  | Delete
  deriving (Show, Eq)

-- | Perform a mixed batch of inserts and deletes.
--
-- Updates can be performed concurrently from multiple Haskell threads.
updates ::
     ( IOLike m
     , SomeSerialisationConstraint k
     , SomeSerialisationConstraint v
     , SomeSerialisationConstraint blob
     )
  => [(k, Update v blob)]
  -> TableHandle m k v blob
  -> m ()
updates = undefined

-- | Perform a batch of inserts.
--
-- Inserts can be performed concurrently from multiple Haskell threads.
inserts ::
     ( IOLike m
     , SomeSerialisationConstraint k
     , SomeSerialisationConstraint v
     , SomeSerialisationConstraint blob
     )
  => [(k, v, Maybe blob)]
  -> TableHandle m k v blob
  -> m ()
inserts = updates . fmap (\(k, v, blob) -> (k, Insert v blob))

-- | Perform a batch of deletes.
--
-- Deletes can be performed concurrently from multiple Haskell threads.
deletes ::
     ( IOLike m
     , SomeSerialisationConstraint k
     , SomeSerialisationConstraint v
     , SomeSerialisationConstraint blob
     )
  => [k]
  -> TableHandle m k v blob
  -> m ()
deletes = updates . fmap (,Delete)

-- | A reference to an on-disk blob.
--
-- The blob can be retrieved based on the reference.
--
-- Blob comes from the acronym __Binary Large OBject (BLOB)__ and in many
-- database implementations refers to binary data that is larger than usual
-- values and is handled specially. In our context we will allow optionally a
-- blob associated with each value in the table.
data BlobRef blob = BlobRef

-- | Perform a batch of blob retrievals.
--
-- This is a separate step from 'lookups' and 'rangeLookups. The result of a
-- lookup can include a 'BlobRef', which can be used to retrieve the actual
-- 'Blob'.
--
-- Blob lookups can be performed concurrently from multiple Haskell threads.
retrieveBlobs ::
     (IOLike m, SomeSerialisationConstraint blob)
  => TableHandle m k v blob
  -> [BlobRef blob]
  -> m [blob]
retrieveBlobs = undefined

{-------------------------------------------------------------------------------
  Snapshots
-------------------------------------------------------------------------------}

-- | Take a snapshot.
--
-- Snapshotting does not close the table handle.
--
-- Taking a snapshot should be relatively cheap, but it is not so cheap that one
-- can use one after every operation. It is relatively cheap because of a reason
-- similar to the reason why 'duplicate' is cheap. On-disk and in-memory data is
-- immutable, which means we can write (the relatively small) in-memory parts to
-- disk, and create hard links for existing files.
snapshot ::
     ( IOLike m
     , SomeSerialisationConstraint k
     , SomeSerialisationConstraint v
     , SomeSerialisationConstraint blob
     )
  => SnapshotName
  -> TableHandle m k v blob
  -> m ()
snapshot = undefined

-- | Open a table through a snapshot, returning a new table handle.
--
-- NOTE: close table handles using 'close' as soon as they are
-- unused.
--
-- TOREMOVE: before snapshots are implemented, the snapshot name should be ignored.
-- Instead, this function should open a table handle from files that exist in
-- the session's directory.
open ::
     ( IOLike m
     , SomeSerialisationConstraint k
     , SomeSerialisationConstraint v
     , SomeSerialisationConstraint blob
     )
  => Session m
  -> SnapshotName
  -> m (TableHandle m k v blob)
open = undefined

{-------------------------------------------------------------------------------
  Mutiple writable table handles
-------------------------------------------------------------------------------}

-- | Create a cheap, independent duplicate of a table handle. This returns a new
-- table handle.
--
-- A table handle and its duplicate are logically independent: changes to one
-- are not visible to the other.
--
-- Duplication is cheap because on-disk and in-memory data is immutable, which
-- means we can /share/ data instead of copying it. Duplication has small
-- computation, storage and memory overheads.
--
-- NOTE: close table handles using 'close' as soon as they are
-- unused.
duplicate ::
     IOLike m
  => TableHandle m k v blob
  -> m (TableHandle m k v blob)
duplicate = undefined
