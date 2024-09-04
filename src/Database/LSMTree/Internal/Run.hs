{-# LANGUAGE DataKinds          #-}
{-# LANGUAGE DeriveAnyClass     #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE DerivingVia        #-}
{-# LANGUAGE RecordWildCards    #-}
{-# LANGUAGE RecursiveDo        #-}

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
    -- * Run
    Run (..)
  , RunFsPaths
  , sizeInPages
  , addReference
  , removeReference
  , mkBlobRefForRun
    -- ** Run creation
  , fromMutable
  , fromWriteBuffer
  , openFromDisk
  , RunDataCaching (..)
  ) where

import           Control.DeepSeq (NFData (..), rwhnf)
import           Control.Monad (when)
import           Control.Monad.Class.MonadThrow
import           Control.Monad.Primitive
import           Control.RefCount (RefCount (..), RefCounter)
import qualified Control.RefCount as RC
import           Data.BloomFilter (Bloom)
import qualified Data.ByteString.Short as SBS
import           Data.Foldable (for_)
import           Database.LSMTree.Internal.BlobRef (BlobRef (..), BlobSpan (..))
import           Database.LSMTree.Internal.BloomFilter (bloomFilterFromSBS)
import qualified Database.LSMTree.Internal.CRC32C as CRC
import           Database.LSMTree.Internal.Entry (NumEntries (..))
import           Database.LSMTree.Internal.IndexCompact (IndexCompact, NumPages)
import qualified Database.LSMTree.Internal.IndexCompact as Index
import           Database.LSMTree.Internal.Paths
import           Database.LSMTree.Internal.RunAcc (RunBloomFilterAlloc)
import           Database.LSMTree.Internal.RunBuilder (RunBuilder)
import qualified Database.LSMTree.Internal.RunBuilder as Builder
import           Database.LSMTree.Internal.Serialise
import           Database.LSMTree.Internal.WriteBuffer (WriteBuffer)
import qualified Database.LSMTree.Internal.WriteBuffer as WB
import qualified System.FS.API as FS
import           System.FS.API (HasFS)
import qualified System.FS.BlockIO.API as FS
import           System.FS.BlockIO.API (HasBlockIO)

-- | The in-memory representation of a completed LSM run.
--
data Run m fhandle = Run {
      runNumEntries     :: !NumEntries
      -- | The reference count for the LSM run. This counts the
      -- number of references from LSM handles to this run. When
      -- this drops to zero the open files will be closed.
    , runRefCounter     :: !(RefCounter m)
      -- | The file system paths for all the files used by the run.
    , runRunFsPaths     :: !RunFsPaths
      -- | The bloom filter for the set of keys in this run.
    , runFilter         :: !(Bloom SerialisedKey)
      -- | The in-memory index mapping keys to page numbers in the
      -- Key\/Ops file. In future we may support alternative index
      -- representations.
    , runIndex          :: !IndexCompact
      -- | The file handle for the Key\/Ops file. This file is opened
      -- read-only and is accessed in a page-oriented way, i.e. only
      -- reading whole pages, at page offsets. It will be opened with
      -- 'O_DIRECT' on supported platforms.
    , runKOpsFile       :: !fhandle
      -- | The file handle for the BLOBs file. This file is opened
      -- read-only and is accessed in a normal style using buffered
      -- I\/O, reading arbitrary file offset and length spans.
    , runBlobFile       :: !fhandle
    , runRunDataCaching :: !RunDataCaching
    }

-- | Shows only the 'runRunFsPaths' field.
instance Show (Run m fhandle) where
  showsPrec _ run = showString "Run { runRunFsPaths = " . showsPrec 0 (runRunFsPaths run) .  showString " }"

instance NFData fhandle => NFData (Run m fhandle) where
  rnf (Run a b c d e f g h) =
      rnf a `seq` rwhnf b `seq` rnf c `seq` rnf d `seq` rnf e `seq`
      rnf f `seq` rnf g `seq` rnf h

sizeInPages :: Run m fhandle -> NumPages
sizeInPages = Index.sizeInPages . runIndex

{-# SPECIALISE addReference :: Run IO h -> IO () #-}
addReference :: PrimMonad m => Run m h -> m ()
addReference r = RC.addReference (runRefCounter r)

{-# SPECIALISE removeReference :: Run IO h -> IO () #-}
removeReference :: (PrimMonad m, MonadMask m) => Run m h -> m ()
removeReference r = RC.removeReference (runRefCounter r)

-- | Helper function to make a 'BlobRef' that points into a 'Run'.
mkBlobRefForRun :: Run m h -> BlobSpan -> BlobRef m h
mkBlobRefForRun Run{runBlobFile, runRefCounter} blobRefSpan =
    BlobRef {
      blobRefFile  = runBlobFile,
      blobRefCount = runRefCounter,
      blobRefSpan
    }

-- | Close the files used in the run, but do not remove them from disk.
-- After calling this operation, the run must not be used anymore.
--
-- TODO: Once snapshots are implemented, files should get removed, but for now
-- we want to be able to re-open closed runs from disk.
close :: HasFS IO h -> HasBlockIO IO h -> Run IO (FS.Handle h) -> IO ()
close fs hbio Run {..} = do
    -- TODO: removing files should drop them from the page cache, but until we
    -- have proper snapshotting we are keeping the files around. Because of
    -- this, we instruct the OS to drop all run-related files from the page
    -- cache
    FS.hDropCacheAll hbio runKOpsFile
    FS.hDropCacheAll hbio runBlobFile

    FS.hClose fs runKOpsFile
      `finally`
        FS.hClose fs runBlobFile

-- | Should this run cache key\/ops data in memory?
data RunDataCaching = CacheRunData | NoCacheRunData
  deriving stock (Show, Eq)

instance NFData RunDataCaching where
  rnf CacheRunData   = ()
  rnf NoCacheRunData = ()

setRunDataCaching :: HasBlockIO IO h -> FS.Handle h -> RunDataCaching -> IO ()
setRunDataCaching hbio runKOpsFile CacheRunData = do
    -- disable file readahead (only applies to this file descriptor)
    FS.hAdviseAll hbio runKOpsFile FS.AdviceRandom
    -- use the page cache for disk I/O reads
    FS.hSetNoCache hbio runKOpsFile False
setRunDataCaching hbio runKOpsFile NoCacheRunData = do
    -- do not use the page cache for disk I/O reads
    FS.hSetNoCache hbio runKOpsFile True

-- | Create a run by finalising a mutable run.
fromMutable :: HasFS IO h
            -> HasBlockIO IO h
            -> RunDataCaching
            -> RefCount
            -> RunBuilder RealWorld (FS.Handle h)
            -> IO (Run IO (FS.Handle h))
fromMutable fs hbio runRunDataCaching refCount builder = do
    (runRunFsPaths, runFilter, runIndex, runNumEntries) <-
      Builder.unsafeFinalise fs hbio (runRunDataCaching == NoCacheRunData) builder
    runKOpsFile <- FS.hOpen fs (runKOpsPath runRunFsPaths) FS.ReadMode
    runBlobFile <- FS.hOpen fs (runBlobPath runRunFsPaths) FS.ReadMode
    setRunDataCaching hbio runKOpsFile runRunDataCaching
    rec runRefCounter <- RC.unsafeMkRefCounterN refCount (Just $ close fs hbio r)
        let !r = Run{..}
    pure r


-- | Write a write buffer to disk, including the blobs it contains.
-- The resulting run has a reference count of 1.
--
-- TODO: As a possible optimisation, blobs could be written to a blob file
-- immediately when they are added to the write buffer, avoiding the need to do
-- it here.
fromWriteBuffer :: HasFS IO h
                -> HasBlockIO IO h
                -> RunDataCaching
                -> RunBloomFilterAlloc
                -> RunFsPaths
                -> WriteBuffer
                -> IO (Run IO (FS.Handle h))
fromWriteBuffer fs hbio caching alloc fsPaths buffer = do
    builder <- Builder.new fs fsPaths (WB.numEntries buffer) alloc
    for_ (WB.toList buffer) $ \(k, e) ->
      Builder.addKeyOp fs builder k e
    fromMutable fs hbio caching (RefCount 1) builder

data ChecksumError = ChecksumError FS.FsPath CRC.CRC32C CRC.CRC32C
  deriving stock Show
  deriving anyclass Exception

data FileFormatError = FileFormatError FS.FsPath String
  deriving stock Show
  deriving anyclass Exception

-- | Load a previously written run from disk, checking each file's checksum
-- against the checksum file.
-- The resulting 'Run' has a reference count of 1.
--
-- Exceptions will be raised when any of the file's contents don't match their
-- checksum ('ChecksumError') or can't be parsed ('FileFormatError').
openFromDisk :: HasFS IO h
             -> HasBlockIO IO h
             -> RunDataCaching
             -> RunFsPaths
             -> IO (Run IO (FS.Handle h))
openFromDisk fs hbio runRunDataCaching runRunFsPaths = do
    expectedChecksums <-
       expectValidFile (runChecksumsPath runRunFsPaths) . fromChecksumsFile
         =<< CRC.readChecksumsFile fs (runChecksumsPath runRunFsPaths)

    -- verify checksums of files we don't read yet
    let paths = pathsForRunFiles runRunFsPaths
    checkCRC runRunDataCaching (forRunKOps expectedChecksums) (forRunKOps paths)
    checkCRC runRunDataCaching (forRunBlob expectedChecksums) (forRunBlob paths)

    -- read and try parsing files
    runFilter <-
      expectValidFile (forRunFilter paths) . bloomFilterFromSBS
        =<< readCRC (forRunFilter expectedChecksums) (forRunFilter paths)
    (runNumEntries, runIndex) <-
      expectValidFile (forRunIndex paths) . Index.fromSBS
        =<< readCRC (forRunIndex expectedChecksums) (forRunIndex paths)

    runKOpsFile <- FS.hOpen fs (runKOpsPath runRunFsPaths) FS.ReadMode
    runBlobFile <- FS.hOpen fs (runBlobPath runRunFsPaths) FS.ReadMode
    setRunDataCaching hbio runKOpsFile runRunDataCaching
    rec runRefCounter <- RC.unsafeMkRefCounterN (RefCount 1) (Just $ close fs hbio r)
        let !r = Run{..}
    pure r
  where
    -- Note: all file data for this path is evicted from the page cache /if/ the
    -- caching argument is 'NoCacheRunData'.
    checkCRC :: RunDataCaching -> CRC.CRC32C -> FS.FsPath -> IO ()
    checkCRC cache expected fp = FS.withFile fs fp FS.ReadMode $ \h -> do
        -- double the file readahead window (only applies to this file descriptor)
        FS.hAdviseAll hbio h FS.AdviceSequential
        !checksum <- CRC.hGetAllCRC32C' fs h CRC.defaultChunkSize CRC.initialCRC32C
        when (cache == NoCacheRunData) $
          -- drop the file from the OS page cache
          FS.hDropCacheAll hbio h
        expectChecksum fp expected checksum

    -- Note: all file data for this path is evicted from the page cache
    readCRC :: CRC.CRC32C -> FS.FsPath -> IO SBS.ShortByteString
    readCRC expected fp = FS.withFile fs fp FS.ReadMode $ \h -> do
        n <- FS.hGetSize fs h
        -- double the file readahead window (only applies to this file descriptor)
        FS.hAdviseAll hbio h FS.AdviceSequential
        (sbs, !checksum) <- CRC.hGetExactlyCRC32C_SBS fs h (fromIntegral n) CRC.initialCRC32C
        -- drop the file from the OS page cache
        FS.hAdviseAll hbio h FS.AdviceDontNeed
        expectChecksum fp expected checksum
        return sbs

    expectChecksum fp expected checksum =
        when (expected /= checksum) $
          throwIO $ ChecksumError fp expected checksum

    expectValidFile _  (Right x)  = pure x
    expectValidFile fp (Left err) = throwIO $ FileFormatError fp err
