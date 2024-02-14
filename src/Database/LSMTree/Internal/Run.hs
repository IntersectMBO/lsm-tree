{-# LANGUAGE RecordWildCards #-}

-- | Functionality related to LSM-Tree runs (sequences of LSM-Tree data).
--
-- === TODO
--
-- This is temporary module header documentation. The module will be
-- fleshed out more as we implement bits of it.
--
-- Related work packages: 5, 6
--
-- This module includes in-memory parts and I\/O parts for, amongst others,
--
-- * High-performance batch lookups
--
-- * Range lookups
--
-- * Incremental run construction
--
-- * Lookups in loaded disk pages, value resolution
--
-- * In-memory representation of a run
--
-- * Flushing a write buffer to a run
--
-- * Opening, deserialising, and verifying files for an LSM run.
--
-- * Closing runs (and removing files)
--
-- * high performance, incremental k-way merge
--
-- The above list is a sketch. Functionality may move around, and the list is
-- not exhaustive.
--
module Database.LSMTree.Internal.Run (
    -- * Paths
    module FsPaths
    -- * Run
  , RefCount
  , Run
  , addReference
  , removeReference
  , fromWriteBuffer
  ) where

import           Control.Exception (finally)
import           Control.Monad (when)
import           Data.Foldable (for_)
import           Data.IORef
import           Database.LSMTree.Internal.Run.BloomFilter (Bloom)
import           Database.LSMTree.Internal.Run.FsPaths as FsPaths
import           Database.LSMTree.Internal.Run.Index.Compact (CompactIndex)
import           Database.LSMTree.Internal.Run.Mutable
import           Database.LSMTree.Internal.Serialise
import           Database.LSMTree.Internal.WriteBuffer (WriteBuffer)
import qualified Database.LSMTree.Internal.WriteBuffer as WB
import qualified System.FS.API as FS
import           System.FS.API (HasFS)


-- | The in-memory representation of a completed LSM run.
--
data Run fhandle = Run {
      -- | The reference count for the LSM run. This counts the
      -- number of references from LSM handles to this run. When
      -- this drops to zero the open files will be closed.
      lsmRunRefCount :: !RefCount
      -- | The file system paths for all the files used by the run.
    , lsmRunFsPaths  :: !RunFsPaths
      -- | The bloom filter for the set of keys in this run.
    , lsmRunFilter   :: !(Bloom SerialisedKey)
      -- | The in-memory index mapping keys to page numbers in the
      -- Key\/Ops file. In future we may support alternative index
      -- representations.
    , lsmRunIndex    :: !CompactIndex
      -- | The file handle for the Key\/Ops file. This file is opened
      -- read-only and is accessed in a page-oriented way, i.e. only
      -- reading whole pages, at page offsets. It will be opened with
      -- 'O_DIRECT' on supported platforms.
    , lsmRunKOpsFile :: !fhandle
      -- | The file handle for the BLOBs file. This file is opened
      -- read-only and is accessed in a normal style using buffered
      -- I\/O, reading arbitrary file offset and length spans.
    , lsmRunBlobFile :: !fhandle
    }

-- | Increase the reference count by one.
addReference :: HasFS IO h -> Run (FS.Handle h) -> IO ()
addReference _ Run {..} =
    atomicModifyIORef lsmRunRefCount (\n -> (n+1, ()))

-- | Decrease the reference count by one.
-- After calling this operation, the run must not be used anymore.
-- If the reference count reaches zero, the run is closed, removing all its
-- associated files from disk.
removeReference :: HasFS IO h -> Run (FS.Handle h) -> IO ()
removeReference fs run@Run {..} = do
    count <- atomicModifyIORef' lsmRunRefCount (\n -> (n-1, n-1))
    when (count <= 0) $
      close fs run

-- | Close the files used in the run, but do not remove them from disk.
-- After calling this operation, the run must not be used anymore.
--
-- TODO: Once snapshots are implemented, files should get removed, but for now
-- we want to be able to re-open closed runs from disk.
close :: HasFS IO h -> Run (FS.Handle h) -> IO ()
close fs Run {..} = do
    FS.hClose fs lsmRunKOpsFile
      `finally`
        FS.hClose fs lsmRunBlobFile

-- | Create a run by finalising a mutable run.
fromMutable :: HasFS IO h -> MRun (FS.Handle h) -> IO (Run (FS.Handle h))
fromMutable fs mrun = do
    (lsmRunRefCount, lsmRunFsPaths, lsmRunFilter, lsmRunIndex) <-
      unsafeFinalise fs mrun
    lsmRunKOpsFile <- FS.hOpen fs (runKOpsPath lsmRunFsPaths) FS.ReadMode
    lsmRunBlobFile <- FS.hOpen fs (runBlobPath lsmRunFsPaths) FS.ReadMode
    return Run {..}


-- | Write a write buffer to disk, including the blobs it contains.
--
-- TODO: As a possible optimisation, blobs could be written to a blob file
-- immediately when they are added to the write buffer, avoiding the need to do
-- it here.
--
-- TODO: To create the compact index, we need to estimate the number of pages.
-- This is currently done very crudely based on the assumption that a k\/op pair
-- requires approximately 100 bytes of disk space.
-- To do better, we would need information about key and value size.
fromWriteBuffer :: HasFS IO h -> RunFsPaths -> WriteBuffer k v b ->
                   IO (Run (FS.Handle h))
fromWriteBuffer fs fsPaths buffer = do
    -- We just estimate the number of pages to be one, as the write buffer is
    -- expected to be small enough not to benefit from more precise tuning.
    -- More concretely, no range finder bits will be used anyways unless there
    -- are at least 2^16 pages.
    mrun <- new fs fsPaths (WB.numEntries buffer) 1
    for_ (WB.content buffer) $ \(k, e) ->
      addFullKOp fs mrun k e
    fromMutable fs mrun
