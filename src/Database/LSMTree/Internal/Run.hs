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
  Run(..),
  RefCount,
  ) where

import Database.LSMTree.Internal.Run.Index.Compact
import Database.LSMTree.Internal.Run.BloomFilter
import Database.LSMTree.Internal.Serialise

import Data.IORef


-- | The 'Run' and 'MRun' objects are reference counted.
type RefCount = IORef Int


-- | The (relative) file path locations of all the files used by the run:
--
-- 1. @active/${n}.keyops@: the sorted run of key\/operation pairs
-- 2. @active/${n}.blobs@:  the blob values associated with the key\/operations
-- 3. @active/${n}.filter@: a Bloom filter of all the keys in the run
-- 4. @active/${n}.index@:  an index from keys to disk page numbers
-- 5. @active/${n}.checksum@: a file listing the crc32c checksums of the other
--    files
--
-- The representation doesn't store the full, name, just the number @n@. Use
-- the accessor functions to get the actual names.
--
newtype RunFsPaths = RunFsPaths { runNumber :: Int }
  deriving Show


-- | The in-memory representation of a completed LSM run.
--
data Run fhandle = Run {
       -- | The reference count for the LSM run. This counts the
       -- number of references from LSM handles to this run. When
       -- this drops to zero the open files will be closed.
       lsmRunRefCount :: !RefCount,

       -- | The file system paths for all the files used by the run.
       lsmRunFsPaths :: !RunFsPaths,

       -- | The bloom filter for the set of keys in this run.
       lsmRunFilter :: !(Bloom SerialisedKey),

       -- | The in-memory index mapping keys to page numbers in the
       -- Key\/Ops file. In future we may support alternative index
       -- representations.
       lsmRunIndex :: !CompactIndex,

       -- | The file handle for the Key\/Ops file. This file is opened
       -- read-only and is accessed in a page-oriented way, i.e. only
       -- reading whole pages, at page offsets. It will be opened with
       -- 'O_DIRECT' on supported platforms.
       lsmRunKOpsFile :: !fhandle,

       -- | The file handle for the BLOBs file. This file is opened
       -- read-only and is accessed in a normal style using buffered
       -- I\/O, reading arbitrary file offset and length spans.
       lsmRunBlobFile :: !fhandle
     }


-- | The in-memory representation of an LSM run that is under construction.
-- (The \"M\" stands for mutable.) This is the output sink for two key
-- algorithms: 1. writing out the write buffer, and 2. incrementally merging
-- two or more runs.
--
-- It contains open file handles for all four files used in the disk
-- representation of a run. Each file handle is opened write-only and should be
-- written to using normal buffered I\/O.
--
data MRun fhandle = MRun {

       -- | The reference count for the LSM run. This counts the
       -- number of references from LSM handles to this run. When
       -- this drops to zero the open files will be closed.
       lsmMRunRefCount :: !RefCount,

       -- | The file system paths for all the files used by the run.
       lsmMRunFsPaths :: !RunFsPaths,

       -- | The run accumulateor. This is the representation used for the
       -- morally pure subset of the run cnstruction functionality. In
       -- particular it contains the (mutable) index, bloom filter and buffered
       -- pending output for the key\/ops file.
       --
       lsmMRunAcc :: !RunAcc,

       -- | The file handle for the Key\/Ops file.
       lsmMRunKOpsFile :: !fhandle,

       -- | The file handle for the BLOBs file.
       lsmMRunBlobFile :: !fhandle

       -- | The file handle for the bloom filter file.
       lsmMRunBoolFilterFile :: !fhandle,

       -- | The file handle for the index file.
       lsmMRunIndexFile :: !fhandle,
    }

