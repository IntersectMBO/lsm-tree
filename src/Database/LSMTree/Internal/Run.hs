{-# LANGUAGE DataKinds          #-}
{-# LANGUAGE DeriveAnyClass     #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE DerivingVia        #-}
{-# LANGUAGE RecordWildCards    #-}
{-# OPTIONS_HADDOCK not-home #-}

-- | Runs of sorted key\/value data.
module Database.LSMTree.Internal.Run (
    -- * Run
    Run (Run, runIndex, runHasFS, runHasBlockIO, runRunDataCaching,
         runBlobFile, runFilter, runKOpsFile)
  , RunFsPaths
  , size
  , sizeInPages
  , runFsPaths
  , runFsPathsNumber
  , runDataCaching
  , runIndexType
  , mkRawBlobRef
  , mkWeakBlobRef
    -- ** Run creation
  , newEmpty
  , fromBuilder
  , fromWriteBuffer
  , RunParams (..)
    -- * Snapshot
  , openFromDisk
  , RunDataCaching (..)
  , IndexType (..)
  ) where

import           Control.DeepSeq (NFData (..), rwhnf)
import           Control.Monad.Class.MonadST (MonadST)
import           Control.Monad.Class.MonadSTM (MonadSTM (..))
import           Control.Monad.Class.MonadThrow
import           Control.Monad.Primitive
import           Control.RefCount
import           Data.BloomFilter (Bloom)
import qualified Data.ByteString.Short as SBS
import           Data.Foldable (for_)
import           Database.LSMTree.Internal.BlobFile
import           Database.LSMTree.Internal.BlobRef hiding (mkRawBlobRef,
                     mkWeakBlobRef)
import qualified Database.LSMTree.Internal.BlobRef as BlobRef
import           Database.LSMTree.Internal.BloomFilter (bloomFilterFromFile)
import qualified Database.LSMTree.Internal.CRC32C as CRC
import           Database.LSMTree.Internal.Entry (NumEntries (..))
import           Database.LSMTree.Internal.Index (Index, IndexType (..))
import qualified Database.LSMTree.Internal.Index as Index
import           Database.LSMTree.Internal.Page (NumPages)
import           Database.LSMTree.Internal.Paths as Paths
import           Database.LSMTree.Internal.RunBuilder (RunBuilder,
                     RunDataCaching (..), RunParams (..))
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
    , runIndex          :: !Index
      -- | The file handle for the Key\/Ops file. This file is opened
      -- read-only and is accessed in a page-oriented way, i.e. only
      -- reading whole pages, at page offsets. It will be opened with
      -- 'O_DIRECT' on supported platforms.
    , runKOpsFile       :: !(FS.Handle h)
      -- | The file handle for the BLOBs file. This file is opened
      -- read-only and is accessed in a normal style using buffered
      -- I\/O, reading arbitrary file offset and length spans.
    , runBlobFile       :: !(Ref (BlobFile m h))
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

instance RefCounted m (Run m h) where
    getRefCounter = runRefCounter

size :: Ref (Run m h) -> NumEntries
size (DeRef run) = runNumEntries run

sizeInPages :: Ref (Run m h) -> NumPages
sizeInPages (DeRef run) = Index.sizeInPages (runIndex run)

runFsPaths :: Ref (Run m h) -> RunFsPaths
runFsPaths (DeRef r) = runRunFsPaths r

runFsPathsNumber :: Ref (Run m h) -> RunNumber
runFsPathsNumber = Paths.runNumber . runFsPaths

-- | See 'openFromDisk'
runIndexType :: Ref (Run m h) -> IndexType
runIndexType (DeRef r) = Index.indexToIndexType (runIndex r)

-- | See 'openFromDisk'
runDataCaching :: Ref (Run m h) -> RunDataCaching
runDataCaching (DeRef r) = runRunDataCaching r


-- | Helper function to make a 'WeakBlobRef' that points into a 'Run'.
mkRawBlobRef :: Run m h -> BlobSpan -> RawBlobRef m h
mkRawBlobRef Run{runBlobFile} blobspan =
    BlobRef.mkRawBlobRef runBlobFile blobspan

-- | Helper function to make a 'WeakBlobRef' that points into a 'Run'.
mkWeakBlobRef :: Ref (Run m h) -> BlobSpan -> WeakBlobRef m h
mkWeakBlobRef (DeRef Run{runBlobFile}) blobspan =
    BlobRef.mkWeakBlobRef runBlobFile blobspan

{-# SPECIALISE finaliser ::
     HasFS IO h
  -> FS.Handle h
  -> Ref (BlobFile IO h)
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
  -> Ref (BlobFile m h)
  -> RunFsPaths
  -> m ()
finaliser hfs kopsFile blobFile fsPaths = do
    FS.hClose hfs kopsFile
    releaseRef blobFile
    FS.removeFile hfs (runKOpsPath fsPaths)
    FS.removeFile hfs (runFilterPath fsPaths)
    FS.removeFile hfs (runIndexPath fsPaths)
    FS.removeFile hfs (runChecksumsPath fsPaths)

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

{-# SPECIALISE newEmpty ::
     HasFS IO h
  -> HasBlockIO IO h
  -> RunParams
  -> RunFsPaths
  -> IO (Ref (Run IO h)) #-}
-- | This function should be run with asynchronous exceptions masked to prevent
-- failing after internal resources have already been created.
newEmpty ::
     (MonadST m, MonadSTM m, MonadMask m)
  => HasFS m h
  -> HasBlockIO m h
  -> RunParams
  -> RunFsPaths
  -> m (Ref (Run m h))
newEmpty hfs hbio runParams runPaths = do
    builder <- Builder.new hfs hbio runParams runPaths (NumEntries 0)
    fromBuilder builder

{-# SPECIALISE fromBuilder ::
     RunBuilder IO h
  -> IO (Ref (Run IO h)) #-}
-- TODO: make exception safe
fromBuilder ::
     (MonadST m, MonadSTM m, MonadMask m)
  => RunBuilder m h
  -> m (Ref (Run m h))
fromBuilder builder = do
    (runHasFS, runHasBlockIO,
     runRunFsPaths, runFilter, runIndex,
     RunParams {runParamCaching = runRunDataCaching}, runNumEntries) <-
      Builder.unsafeFinalise builder
    runKOpsFile <- FS.hOpen runHasFS (runKOpsPath runRunFsPaths) FS.ReadMode
    -- TODO: openBlobFile should be called with exceptions masked
    runBlobFile <- openBlobFile runHasFS (runBlobPath runRunFsPaths) FS.ReadMode
    setRunDataCaching runHasBlockIO runKOpsFile runRunDataCaching
    newRef (finaliser runHasFS runKOpsFile runBlobFile runRunFsPaths)
           (\runRefCounter -> Run { .. })

{-# SPECIALISE fromWriteBuffer ::
     HasFS IO h
  -> HasBlockIO IO h
  -> RunParams
  -> RunFsPaths
  -> WriteBuffer
  -> Ref (WriteBufferBlobs IO h)
  -> IO (Ref (Run IO h)) #-}
-- | Write a write buffer to disk, including the blobs it contains.
--
-- This creates a new 'Run' which must eventually be released with 'releaseRef'.
--
-- TODO: As a possible optimisation, blobs could be written to a blob file
-- immediately when they are added to the write buffer, avoiding the need to do
-- it here.
--
-- This function should be run with asynchronous exceptions masked to prevent
-- failing after internal resources have already been created.
fromWriteBuffer ::
     (MonadST m, MonadSTM m, MonadMask m)
  => HasFS m h
  -> HasBlockIO m h
  -> RunParams
  -> RunFsPaths
  -> WriteBuffer
  -> Ref (WriteBufferBlobs m h)
  -> m (Ref (Run m h))
fromWriteBuffer fs hbio params fsPaths buffer blobs = do
    builder <- Builder.new fs hbio params fsPaths (WB.numEntries buffer)
    for_ (WB.toList buffer) $ \(k, e) ->
      Builder.addKeyOp builder k (fmap (WBB.mkRawBlobRef blobs) e)
      --TODO: the fmap entry here reallocates even when there are no blobs
    fromBuilder builder

{-------------------------------------------------------------------------------
  Snapshot
-------------------------------------------------------------------------------}

{-# SPECIALISE openFromDisk ::
     HasFS IO h
  -> HasBlockIO IO h
  -> RunDataCaching
  -> IndexType
  -> RunFsPaths
  -> IO (Ref (Run IO h)) #-}
-- | Load a previously written run from disk, checking each file's checksum
-- against the checksum file.
--
-- This creates a new 'Run' which must eventually be released with 'releaseRef'.
--
-- Exceptions will be raised when any of the file's contents don't match their
-- checksum ('ChecksumError') or can't be parsed ('FileFormatError').
--
-- The 'RunDataCaching' and 'IndexType' parameters need to be saved and
-- restored separately because these are not stored in the on-disk
-- representation. Use 'runDataCaching' and 'runIndexType' to obtain these
-- parameters from the open run before persisting to disk.
--
-- TODO: it may make more sense to persist these parameters with the run's
-- on-disk representation.
--
openFromDisk ::
     forall m h.
     (MonadSTM m, MonadMask m, PrimMonad m)
  => HasFS m h
  -> HasBlockIO m h
  -> RunDataCaching
  -> IndexType
  -> RunFsPaths
  -> m (Ref (Run m h))
-- TODO: make exception safe
openFromDisk fs hbio runRunDataCaching indexType runRunFsPaths = do
    expectedChecksums <-
       CRC.expectValidFile fs (runChecksumsPath runRunFsPaths) CRC.FormatChecksumsFile
           . fromChecksumsFile
         =<< CRC.readChecksumsFile fs (runChecksumsPath runRunFsPaths)

    -- verify checksums of files we don't read yet
    let paths = pathsForRunFiles runRunFsPaths
    checkCRC runRunDataCaching (forRunKOpsRaw expectedChecksums) (forRunKOpsRaw paths)
    checkCRC runRunDataCaching (forRunBlobRaw expectedChecksums) (forRunBlobRaw paths)

    -- read and try parsing files
    let filterPath = forRunFilterRaw paths
    checkCRC CacheRunData (forRunFilterRaw expectedChecksums) filterPath
    runFilter <- FS.withFile fs filterPath FS.ReadMode $
                   bloomFilterFromFile fs

    (runNumEntries, runIndex) <-
      CRC.expectValidFile fs (forRunIndexRaw paths) CRC.FormatIndexFile
          . Index.fromSBS indexType
        =<< readCRC (forRunIndexRaw expectedChecksums) (forRunIndexRaw paths)

    runKOpsFile <- FS.hOpen fs (runKOpsPath runRunFsPaths) FS.ReadMode
    -- TODO: openBlobFile should be called with exceptions masked
    runBlobFile <- openBlobFile fs (runBlobPath runRunFsPaths) FS.ReadMode
    setRunDataCaching hbio runKOpsFile runRunDataCaching
    newRef (finaliser fs runKOpsFile runBlobFile runRunFsPaths) $ \runRefCounter ->
      Run {
        runHasFS = fs
      , runHasBlockIO = hbio
      , ..
      }
  where
    -- Note: all file data for this path is evicted from the page cache /if/ the
    -- caching argument is 'NoCacheRunData'.
    checkCRC :: RunDataCaching -> CRC.CRC32C -> FS.FsPath -> m ()
    checkCRC cache expected fp =
      CRC.checkCRC fs hbio (cache == NoCacheRunData) expected fp

    -- Note: all file data for this path is evicted from the page cache
    readCRC :: CRC.CRC32C -> FS.FsPath -> m SBS.ShortByteString
    readCRC expected fp = FS.withFile fs fp FS.ReadMode $ \h -> do
        n <- FS.hGetSize fs h
        -- double the file readahead window (only applies to this file descriptor)
        FS.hAdviseAll hbio h FS.AdviceSequential
        (sbs, !checksum) <- CRC.hGetExactlyCRC32C_SBS fs h (fromIntegral n) CRC.initialCRC32C
        -- drop the file from the OS page cache
        FS.hAdviseAll hbio h FS.AdviceDontNeed
        CRC.expectChecksum fs fp expected checksum
        return sbs
