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
-- > import qualified Database.LSMTree.Monoidal as LSMT
module Database.LSMTree.Monoidal (
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
  , TableConfig
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
  , mupserts
    -- * Snapshots
  , VerificationFailure
  , SnapshotName
  , snapshot
  , open
    -- * Multiple writable table handles
  , duplicate
    -- * Merging tables
  , mergeTables
  ) where

import           Data.Bifunctor (Bifunctor (second))
import           Data.Kind (Type)
import           Data.Word (Word64)
import           Database.LSMTree.Common (IOLike, Range (..), Session,
                     SomeSerialisationConstraint, SomeUpdateConstraint,
                     closeSession, newSession)

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
type TableHandle :: (Type -> Type) -> Type -> Type -> Type
data TableHandle m k v = TableHandle {
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

-- | Create a new table referenced by a table handle.
--
-- NOTE: close table handles using 'close' as soon as they are
-- unused.
new ::
     IOLike m
  => Session m
  -> TableConfig
  -> m (TableHandle m k v)
new = undefined

-- | Close a table handle.
--
-- Any on-disk files and in-memory data that are no longer referenced after
-- closing the table handle are lost forever. Use 'Snapshot's to ensure data is
-- not lost.
close ::
     IOLike m
  => TableHandle m k v
  -> m ()
close = undefined

{-------------------------------------------------------------------------------
  Table querying and updates
-------------------------------------------------------------------------------}

-- | Result of a single point lookup.
data LookupResult k v =
    NotFound      !k
  | Found         !k !v

-- | Perform a batch of lookups.
--
-- Lookups can be performed concurrently from multiple Haskell threads.
lookups ::
     ( IOLike m
     , SomeSerialisationConstraint k
     , SomeSerialisationConstraint v
     , SomeUpdateConstraint v
     )
  => [k]
  -> TableHandle m k v
  -> m [LookupResult k v]
lookups = undefined

-- | A result for one point in a range lookup.
data RangeLookupResult k v =
    FoundInRange         !k !v

-- | Perform a range lookup.
--
-- Range lookups can be performed concurrently from multiple Haskell threads.
rangeLookup ::
     ( IOLike m
     , SomeSerialisationConstraint k
     , SomeSerialisationConstraint v
     , SomeUpdateConstraint v
     )
  => Range k
  -> TableHandle m k v
  -> m [RangeLookupResult k v]
rangeLookup = undefined

-- | Monoidal tables support insert, delete and monoidal upsert operations.
--
-- An __update__ is a term that groups all types of table-manipulating
-- operations, like inserts and deletes.
data Update v =
    Insert !v
  | Delete
    -- | TODO: should be given a more suitable name.
  | Mupsert !v

-- | Perform a mixed batch of inserts, deletes and monoidal upserts.
--
-- Updates can be performed concurrently from multiple Haskell threads.
updates ::
     ( IOLike m
     , SomeSerialisationConstraint k
     , SomeSerialisationConstraint v
     , SomeUpdateConstraint v
     )
  => [(k, Update v)]
  -> TableHandle m k v
  -> m ()
updates = undefined

-- | Perform a batch of inserts.
--
-- Inserts can be performed concurrently from multiple Haskell threads.
inserts ::
     ( IOLike m
     , SomeSerialisationConstraint k
     , SomeSerialisationConstraint v
     , SomeUpdateConstraint v
     )
  => [(k, v)]
  -> TableHandle m k v
  -> m ()
inserts = updates . fmap (second Insert)

-- | Perform a batch of deletes.
--
-- Deletes can be performed concurrently from multiple Haskell threads.
deletes ::
     ( IOLike m
     , SomeSerialisationConstraint k
     , SomeSerialisationConstraint v
     , SomeUpdateConstraint v
     )
  => [k]
  -> TableHandle m k v
  -> m ()
deletes = updates . fmap (,Delete)

-- | Perform a batch of monoidal upserts.
--
-- Monoidal upserts can be performed concurrently from multiple Haskell threads.
mupserts ::
     ( IOLike m
     , SomeSerialisationConstraint k
     , SomeSerialisationConstraint v
     , SomeUpdateConstraint v
     )
  => [(k, v)]
  -> TableHandle m k v
  -> m ()
mupserts = updates . fmap (second Mupsert)

{-------------------------------------------------------------------------------
  Snapshots
-------------------------------------------------------------------------------}

data VerificationFailure

data SnapshotName

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
     )
  => SnapshotName
  -> TableHandle m k v
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
     )
  => Session m
  -> SnapshotName
  -> m (Either
          VerificationFailure
          (TableHandle m k v)
       )
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
  => TableHandle m k v
  -> m (TableHandle m k v)
duplicate = undefined


{-------------------------------------------------------------------------------
  Merging tables
-------------------------------------------------------------------------------}

-- | Merge full tables, creating a new table handle.
--
-- NOTE: close table handles using 'close' as soon as they are
-- unused.
--
-- Multiple tables of the same type but with different configuration parameters
-- can live in the same session. However, some operations, like
mergeTables ::
     (IOLike m, SomeUpdateConstraint v)
  => TableHandle m k v
  -> TableHandle m k v
  -> m (TableHandle m k v)
mergeTables = undefined
