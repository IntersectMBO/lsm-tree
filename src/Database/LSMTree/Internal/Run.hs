{-# LANGUAGE DataKinds          #-}
{-# LANGUAGE DeriveAnyClass     #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE DerivingVia        #-}
{-# LANGUAGE RecordWildCards    #-}

-- | Runs of sorted key\/value data.
module Database.LSMTree.Internal.Run (
    -- * Run
    Run (Run, runIndex, runHasFS, runHasBlockIO, runRunDataCaching,
         runBlobFile, runFilter, runKOpsFile, runRefCounter)
  , RunFsPaths
  , size
  , sizeInPages
  , runFsPaths
  , runFsPathsNumber
  , addReference
  , removeReference
  , mkRawBlobRef
  , mkWeakBlobRef
    -- ** Run creation
  , fromMutable
  , fromWriteBuffer
  , RunDataCaching (..)
    -- * Snapshot
  , FileFormatError (..)
  , ChecksumError (..)
  , openFromDisk
  ) where

import           Control.DeepSeq (NFData (..), rwhnf)
import           Control.Monad (when)
import           Control.Monad.Class.MonadST (MonadST)
import           Control.Monad.Class.MonadSTM (MonadSTM (..))
import           Control.Monad.Class.MonadThrow
import           Control.Monad.Primitive
import           Control.RefCount (RefCounter)
import qualified Control.RefCount as RC
import           Data.BloomFilter (Bloom)
import qualified Data.ByteString.Short as SBS
import           Data.Foldable (for_)
import           Database.LSMTree.Internal.BlobFile hiding (removeReference)
import qualified Database.LSMTree.Internal.BlobFile as BlobFile
import           Database.LSMTree.Internal.BlobRef (RawBlobRef (..),
                     WeakBlobRef (..))
import           Database.LSMTree.Internal.BloomFilter (bloomFilterFromSBS)
import qualified Database.LSMTree.Internal.CRC32C as CRC
import           Database.LSMTree.Internal.Entry (NumEntries (..))
import qualified Database.LSMTree.Internal.Index as Index (fromSBS, sizeInPages)
import           Database.LSMTree.Internal.Index.Compact (IndexCompact)
import           Database.LSMTree.Internal.Page (NumPages)
import           Database.LSMTree.Internal.Paths as Paths
import           Database.LSMTree.Internal.RunAcc (RunBloomFilterAlloc)
import           Database.LSMTree.Internal.RunBuilder (RunBuilder)
import qualified Database.LSMTree.Internal.RunBuilder as Builder
import           Database.LSMTree.Internal.RunNumber
import           Database.LSMTree.Internal.Serialise
import           Database.LSMTree.Internal.WriteBuffer (WriteBuffer)
import qualified Database.LSMTree.Internal.WriteBuffer as WB
import           Database.LSMTree.Internal.WriteBufferBlobs (WriteBufferBlobs)
import qualified Database.LSMTree.Internal.WriteBufferBlobs as WBB
import qualified System.FS.API as FS
import           System.FS.API (HasFS)
import qualified System.FS.BlockIO.API as FS
import           System.FS.BlockIO.API (HasBlockIO)

-- | The in-memory representation of a completed LSM run.
--
data Run m h = Run {
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
    , runKOpsFile       :: !(FS.Handle h)
      -- | The file handle for the BLOBs file. This file is opened
      -- read-only and is accessed in a normal style using buffered
      -- I\/O, reading arbitrary file offset and length spans.
    , runBlobFile       :: !(BlobFile m h)
    , runRunDataCaching :: !RunDataCaching
    , runHasFS          :: !(HasFS m h)
    , runHasBlockIO     :: !(HasBlockIO m h)
    }

-- | Shows only the 'runRunFsPaths' field.
instance Show (Run m h) where
  showsPrec _ run = showString "Run { runRunFsPaths = " . showsPrec 0 (runRunFsPaths run) .  showString " }"

instance NFData h => NFData (Run m h) where
  rnf (Run a b c d e f g h i j) =
      rnf a `seq` rwhnf b `seq` rnf c `seq` rnf d `seq` rnf e `seq`
      rnf f `seq` rnf g `seq` rnf h `seq` rwhnf i `seq` rwhnf j

size :: Run m h -> NumEntries
size run = runNumEntries run

sizeInPages :: Run m h -> NumPages
sizeInPages run = Index.sizeInPages (runIndex run)

runFsPaths :: Run m h -> RunFsPaths
runFsPaths r = runRunFsPaths r

runFsPathsNumber :: Run m h -> RunNumber
runFsPathsNumber = Paths.runNumber . runFsPaths

{-# SPECIALISE addReference :: Run IO h -> IO () #-}
addReference :: PrimMonad m => Run m h -> m ()
addReference r = RC.addReference (runRefCounter r)

{-# SPECIALISE removeReference :: Run IO h -> IO () #-}
removeReference :: (PrimMonad m, MonadMask m) => Run m h -> m ()
removeReference r = RC.removeReference (runRefCounter r)

-- | Helper function to make a 'WeakBlobRef' that points into a 'Run'.
mkRawBlobRef :: Run m h -> BlobSpan -> RawBlobRef m h
mkRawBlobRef Run{runBlobFile} blobspan =
    RawBlobRef {
      rawBlobRefFile = runBlobFile,
      rawBlobRefSpan = blobspan
    }

-- | Helper function to make a 'WeakBlobRef' that points into a 'Run'.
mkWeakBlobRef :: Run m h -> BlobSpan -> WeakBlobRef m h
mkWeakBlobRef Run{runBlobFile} blobspan =
    WeakBlobRef {
      weakBlobRefFile = runBlobFile,
      weakBlobRefSpan = blobspan
    }

{-# SPECIALISE finaliser ::
     HasFS IO h
  -> FS.Handle h
  -> BlobFile IO h
  -> RunFsPaths
  -> IO () #-}
-- | Close the files used in the run and remove them from disk. After calling
-- this operation, the run must not be used anymore.
--
-- TODO: exception safety
finaliser ::
     (MonadSTM m, MonadMask m, PrimMonad m)
  => HasFS m h
  -> FS.Handle h
  -> BlobFile m h
  -> RunFsPaths
  -> m ()
finaliser hfs kopsFile blobFile fsPaths = do
    FS.hClose hfs kopsFile
    BlobFile.removeReference blobFile
    FS.removeFile hfs (runKOpsPath fsPaths)
    FS.removeFile hfs (runFilterPath fsPaths)
    FS.removeFile hfs (runIndexPath fsPaths)
    FS.removeFile hfs (runChecksumsPath fsPaths)

-- | Should this run cache key\/ops data in memory?
data RunDataCaching = CacheRunData | NoCacheRunData
  deriving stock (Show, Eq)

instance NFData RunDataCaching where
  rnf CacheRunData   = ()
  rnf NoCacheRunData = ()

{-# SPECIALISE setRunDataCaching ::
     HasBlockIO IO h
  -> FS.Handle h
  -> RunDataCaching
  -> IO () #-}
setRunDataCaching ::
     MonadSTM m
  => HasBlockIO m h
  -> FS.Handle h
  -> RunDataCaching
  -> m ()
setRunDataCaching hbio runKOpsFile CacheRunData = do
    -- disable file readahead (only applies to this file descriptor)
    FS.hAdviseAll hbio runKOpsFile FS.AdviceRandom
    -- use the page cache for disk I/O reads
    FS.hSetNoCache hbio runKOpsFile False
setRunDataCaching hbio runKOpsFile NoCacheRunData = do
    -- do not use the page cache for disk I/O reads
    FS.hSetNoCache hbio runKOpsFile True

{-# SPECIALISE fromMutable ::
     RunDataCaching
  -> RunBuilder IO h
  -> IO (Run IO h) #-}
fromMutable ::
     (MonadST m, MonadSTM m, MonadMask m)
  => RunDataCaching
  -> RunBuilder m h
  -> m (Run m h)
fromMutable runRunDataCaching builder = do
    (runHasFS, runHasBlockIO, runRunFsPaths, runFilter, runIndex, runNumEntries) <-
      Builder.unsafeFinalise (runRunDataCaching == NoCacheRunData) builder
    runKOpsFile <- FS.hOpen runHasFS (runKOpsPath runRunFsPaths) FS.ReadMode
    runBlobFile <- openBlobFile runHasFS (runBlobPath runRunFsPaths) FS.ReadMode
    setRunDataCaching runHasBlockIO runKOpsFile runRunDataCaching
    runRefCounter <- RC.mkRefCounter1
                       (finaliser runHasFS runKOpsFile runBlobFile runRunFsPaths)
    return Run { .. }

{-# SPECIALISE fromWriteBuffer ::
     HasFS IO h
  -> HasBlockIO IO h
  -> RunDataCaching
  -> RunBloomFilterAlloc
  -> RunFsPaths
  -> WriteBuffer
  -> WriteBufferBlobs IO h
  -> IO (Run IO h) #-}
-- | Write a write buffer to disk, including the blobs it contains.
-- The resulting run has a reference count of 1.
--
-- TODO: As a possible optimisation, blobs could be written to a blob file
-- immediately when they are added to the write buffer, avoiding the need to do
-- it here.
fromWriteBuffer ::
     (MonadST m, MonadSTM m, MonadMask m)
  => HasFS m h
  -> HasBlockIO m h
  -> RunDataCaching
  -> RunBloomFilterAlloc
  -> RunFsPaths
  -> WriteBuffer
  -> WriteBufferBlobs m h
  -> m (Run m h)
fromWriteBuffer fs hbio caching alloc fsPaths buffer blobs = do
    builder <- Builder.new fs hbio fsPaths (WB.numEntries buffer) alloc
    for_ (WB.toList buffer) $ \(k, e) ->
      Builder.addKeyOp builder k (fmap (WBB.mkRawBlobRef blobs) e)
      --TODO: the fmap entry here reallocates even when there are no blobs
    fromMutable caching builder

{-------------------------------------------------------------------------------
  Snapshot
-------------------------------------------------------------------------------}

data ChecksumError = ChecksumError FS.FsPath CRC.CRC32C CRC.CRC32C
  deriving stock Show
  deriving anyclass Exception

data FileFormatError = FileFormatError FS.FsPath String
  deriving stock Show
  deriving anyclass Exception

{-# SPECIALISE openFromDisk ::
     HasFS IO h
  -> HasBlockIO IO h
  -> RunDataCaching
  -> RunFsPaths
  -> IO (Run IO h) #-}
-- | Load a previously written run from disk, checking each file's checksum
-- against the checksum file.
-- The resulting 'Run' has a reference count of 1.
--
-- Exceptions will be raised when any of the file's contents don't match their
-- checksum ('ChecksumError') or can't be parsed ('FileFormatError').
openFromDisk ::
     forall m h.
     (MonadSTM m, MonadMask m, PrimMonad m)
  => HasFS m h
  -> HasBlockIO m h
  -> RunDataCaching
  -> RunFsPaths
  -> m (Run m h)
openFromDisk fs hbio runRunDataCaching runRunFsPaths = do
    expectedChecksums <-
       expectValidFile (runChecksumsPath runRunFsPaths) . fromChecksumsFile
         =<< CRC.readChecksumsFile fs (runChecksumsPath runRunFsPaths)

    -- verify checksums of files we don't read yet
    let paths = pathsForRunFiles runRunFsPaths
    checkCRC runRunDataCaching (forRunKOpsRaw expectedChecksums) (forRunKOpsRaw paths)
    checkCRC runRunDataCaching (forRunBlobRaw expectedChecksums) (forRunBlobRaw paths)

    -- read and try parsing files
    runFilter <-
      expectValidFile (forRunFilterRaw paths) . bloomFilterFromSBS
        =<< readCRC (forRunFilterRaw expectedChecksums) (forRunFilterRaw paths)
    (runNumEntries, runIndex) <-
      expectValidFile (forRunIndexRaw paths) . Index.fromSBS
        =<< readCRC (forRunIndexRaw expectedChecksums) (forRunIndexRaw paths)

    runKOpsFile <- FS.hOpen fs (runKOpsPath runRunFsPaths) FS.ReadMode
    runBlobFile <- openBlobFile fs (runBlobPath runRunFsPaths) FS.ReadMode
    setRunDataCaching hbio runKOpsFile runRunDataCaching
    runRefCounter <- RC.mkRefCounter1 (finaliser fs runKOpsFile runBlobFile runRunFsPaths)
    return Run {
      runHasFS = fs
    , runHasBlockIO = hbio
    , ..
    }
  where
    -- Note: all file data for this path is evicted from the page cache /if/ the
    -- caching argument is 'NoCacheRunData'.
    checkCRC :: RunDataCaching -> CRC.CRC32C -> FS.FsPath -> m ()
    checkCRC cache expected fp = FS.withFile fs fp FS.ReadMode $ \h -> do
        -- double the file readahead window (only applies to this file descriptor)
        FS.hAdviseAll hbio h FS.AdviceSequential
        !checksum <- CRC.hGetAllCRC32C' fs h CRC.defaultChunkSize CRC.initialCRC32C
        when (cache == NoCacheRunData) $
          -- drop the file from the OS page cache
          FS.hDropCacheAll hbio h
        expectChecksum fp expected checksum

    -- Note: all file data for this path is evicted from the page cache
    readCRC :: CRC.CRC32C -> FS.FsPath -> m SBS.ShortByteString
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
