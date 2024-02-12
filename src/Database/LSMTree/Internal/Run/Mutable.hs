{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes        #-}
{-# LANGUAGE RecordWildCards   #-}

-- | Mutable runs ('MRun') that are under construction.
--
module Database.LSMTree.Internal.Run.Mutable (
    -- * Mutable Run
    RefCount
  , MRun
  , NumPages
  , new
  , addFullKOp
  , unsafeFinalise
  , addMRunReference
  , removeMRunReference
  ) where

import           Control.Monad (when)
import qualified Control.Monad.ST as ST
import qualified Data.ByteString.Builder as BSB
import           Data.Foldable (for_)
import           Data.IORef
import qualified Data.Map as Map
import           Data.Traversable (for)
import           Data.Word (Word64)
import           Database.LSMTree.Internal.BlobRef (BlobSpan (..))
import           Database.LSMTree.Internal.CRC32C (CRC32C)
import qualified Database.LSMTree.Internal.CRC32C as CRC
import           Database.LSMTree.Internal.Entry
import           Database.LSMTree.Internal.Run.BloomFilter (Bloom)
import           Database.LSMTree.Internal.Run.Construction (RunAcc)
import qualified Database.LSMTree.Internal.Run.Construction as Cons
import           Database.LSMTree.Internal.Run.FsPaths
import           Database.LSMTree.Internal.Run.Index.Compact (CompactIndex,
                     NumPages)
import qualified Database.LSMTree.Internal.Run.Index.Compact as Index
import           Database.LSMTree.Internal.Serialise
import qualified System.FS.API as FS
import           System.FS.API (HasFS)

-- | The 'Run' and 'MRun' objects are reference counted.
type RefCount = IORef Int

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
      lsmMRunRefCount     :: !RefCount

      -- | The file system paths for all the files used by the run.
    , lsmMRunFsPaths      :: !RunFsPaths

      -- | The run accumulator. This is the representation used for the
      -- morally pure subset of the run cnstruction functionality. In
      -- particular it contains the (mutable) index, bloom filter and buffered
      -- pending output for the key\/ops file.
    , lsmMRunAcc          :: !(RunAcc ST.RealWorld)

      -- | The byte offset within the blob file for the next blob to be written.
    , lsmMRunBlobOffset   :: !(IORef Word64)

      -- | The file handle for the Key\/Ops file.
    , lsmMRunKOpsHandle   :: !(ChecksumHandle fhandle)
      -- | The file handle for the BLOBs file.
    , lsmMRunBlobHandle   :: !(ChecksumHandle fhandle)
      -- | The file handle for the bloom filter file.
    , lsmMRunFilterHandle :: !(ChecksumHandle fhandle)
      -- | The file handle for the index file.
    , lsmMRunIndexHandle  :: !(ChecksumHandle fhandle)
    }

-- | Create an 'MRun' to start building a run.
-- The result will have an initial reference count of 1.
new ::
     HasFS IO h
  -> RunFsPaths
  -> NumEntries  -- ^ an upper bound of the number of entries to be added
  -> NumPages    -- ^ an upper bound (or estimate) of the total number of pages
                 -- in the resulting run
  -> IO (MRun (FS.Handle h))
new fs lsmMRunFsPaths numEntries estimatedNumPages = do
    lsmMRunRefCount <- newIORef 1

    lsmMRunAcc <- ST.stToIO $ Cons.new numEntries estimatedNumPages
    lsmMRunBlobOffset <- newIORef 0

    FS.createDirectoryIfMissing fs False activeRunsDir
    lsmMRunKOpsHandle   <- makeHandle fs (runKOpsPath lsmMRunFsPaths)
    lsmMRunBlobHandle   <- makeHandle fs (runBlobPath lsmMRunFsPaths)
    lsmMRunFilterHandle <- makeHandle fs (runFilterPath lsmMRunFsPaths)
    lsmMRunIndexHandle  <- makeHandle fs (runIndexPath lsmMRunFsPaths)
    return MRun {..}

-- | Add a serialised k\/op pair. Blobs will be written to disk. Use only for
-- entries that are fully in-memory.
-- To handle larger-than-page values in a chunked style during run merging,
-- one could write slightly different function based on 'Cons.addChunkedKOp'.
--
-- The k\/ops and the primary array of the index get written incrementally,
-- everything else only at the end when 'unsafeFinalise' is called.
--
-- __Note that filter and index serialisation is not implemented yet!__
addFullKOp ::
     HasFS IO h
  -> MRun (FS.Handle h)
  -> SerialisedKey
  -> Entry SerialisedValue SerialisedBlob
  -> IO ()
addFullKOp fs mrun@MRun {..} key op = do
    op' <- for op $ \blob ->
      writeBlob fs mrun blob
    mAcc <- ST.stToIO (Cons.addFullKOp lsmMRunAcc key op')
    for_ mAcc $ \(pageAcc, chunks) -> do
      writePageAcc fs mrun pageAcc
      writeIndexChunks fs mrun chunks

-- | Finish construction of the run.
-- Writes the filter and index to file and leaves all written files on disk.
--
-- __Do not use the 'MRun' after calling this function!__
--
-- TODO: Ensure proper cleanup even in presence of exceptions.
unsafeFinalise ::
     HasFS IO h
  -> MRun (FS.Handle h)
  -> IO (RefCount, RunFsPaths, Bloom SerialisedKey, CompactIndex)
unsafeFinalise fs mrun@MRun {..} = do
    -- write final bits
    (mAcc, mChunk, finalChunk, runFilter, runIndex) <-
      ST.stToIO (Cons.unsafeFinalise lsmMRunAcc)
    for_ mAcc $ \(pageAcc, chunks) -> do
      writePageAcc fs mrun pageAcc
      writeIndexChunks fs mrun chunks
    for_ mChunk $ \chunk ->
      writeIndexChunks fs mrun [chunk]
    writeIndexFinalChunk fs mrun finalChunk
    writeFilter fs mrun runFilter
    -- close all handles and write their checksums
    let named n = (,) (CRC.ChecksumsFileName n)
    checksums <- fmap Map.fromList $ sequenceA
      [ named "keyops" <$> closeHandle fs lsmMRunKOpsHandle
      , named "blobs"  <$> closeHandle fs lsmMRunBlobHandle
      , named "index"  <$> closeHandle fs lsmMRunIndexHandle
      , named "filter" <$> closeHandle fs lsmMRunFilterHandle
      ]
    CRC.writeChecksumsFile fs (runChecksumsPath lsmMRunFsPaths) checksums
    return (lsmMRunRefCount, lsmMRunFsPaths, runFilter, runIndex)

-- | Increase the reference count by one.
addMRunReference :: HasFS IO h -> MRun (FS.Handle h) -> IO ()
addMRunReference _ MRun {..} =
    atomicModifyIORef' lsmMRunRefCount (\n -> (n+1, ()))

-- | Decrease the reference count by one.
-- After calling this operation, the run must not be used anymore.
-- If the reference count reaches zero, the run is closed, removing all its
-- associated files from disk.
removeMRunReference :: HasFS IO h -> MRun (FS.Handle h) -> IO ()
removeMRunReference fs mrun@MRun {..} = do
    count <- atomicModifyIORef' lsmMRunRefCount (\n -> (n-1, n-1))
    when (count <= 0) $
      closeMRun fs mrun

-- | Close the run, removing all files associated with it from disk.
-- After calling this operation, the run must not be used anymore.
--
-- TODO: Ensure proper cleanup even in presence of exceptions.
closeMRun :: HasFS IO h -> MRun (FS.Handle h) -> IO ()
closeMRun fs MRun {..} = do
    _ <- closeHandle fs lsmMRunKOpsHandle
    _ <- closeHandle fs lsmMRunBlobHandle
    _ <- closeHandle fs lsmMRunFilterHandle
    _ <- closeHandle fs lsmMRunIndexHandle
    FS.removeFile fs (runKOpsPath lsmMRunFsPaths)
    FS.removeFile fs (runBlobPath lsmMRunFsPaths)
    FS.removeFile fs (runFilterPath lsmMRunFsPaths)
    FS.removeFile fs (runIndexPath lsmMRunFsPaths)

{-------------------------------------------------------------------------------
  Helpers
-------------------------------------------------------------------------------}

writeBlob :: HasFS IO h -> MRun (FS.Handle h) -> SerialisedBlob -> IO BlobSpan
writeBlob fs MRun{..} blob = do
    let size = sizeofBlob64 blob
    offset <- atomicModifyIORef' lsmMRunBlobOffset (\o -> (o + size, o))
    writeToHandle fs lsmMRunBlobHandle (serialisedBlob blob)
    return (BlobSpan offset (fromIntegral size))

writePageAcc :: HasFS IO h -> MRun (FS.Handle h) -> Cons.PageAcc -> IO ()
writePageAcc fs MRun {..} =
    writeToHandle fs lsmMRunKOpsHandle . Cons.pageBuilder

-- TODO: Fill in once serialisation of the index is implemented.
writeIndexFinalChunk ::
     HasFS IO h
  -> MRun (FS.Handle h)
  -> Index.FinalChunk
  -> IO ()
writeIndexFinalChunk _ _ _ = return ()

-- TODO: Fill in once serialisation of the index is implemented.
writeIndexChunks :: HasFS IO h -> MRun (FS.Handle h) -> [Index.Chunk] -> IO ()
writeIndexChunks _ _ _ = return ()

-- TODO: Fill in once serialisation of the filter is implemented.
writeFilter :: HasFS IO h -> MRun (FS.Handle h) -> Bloom SerialisedKey -> IO ()
writeFilter _ _ _ = return ()

{-------------------------------------------------------------------------------
  ChecksumHandle
-------------------------------------------------------------------------------}

-- | Tracks the checksum of a (write mode) file handle.
data ChecksumHandle fhandle = ChecksumHandle !fhandle !(IORef CRC32C)

makeHandle :: HasFS IO h -> FS.FsPath -> IO (ChecksumHandle (FS.Handle h))
makeHandle fs path =
    ChecksumHandle
      <$> FS.hOpen fs path (FS.WriteMode FS.MustBeNew)
      <*> newIORef CRC.initialCRC32C

closeHandle :: HasFS IO h -> ChecksumHandle (FS.Handle h) -> IO CRC32C
closeHandle fs (ChecksumHandle h checksum) = do
    FS.hClose fs h
    readIORef checksum

-- TODO: Revisit performance once we have a suitable benchmark.
writeToHandle ::
     HasFS IO h
  -> ChecksumHandle (FS.Handle h) -> BSB.Builder -> IO ()
writeToHandle fs (ChecksumHandle h checksum) builder = do
    crc <- readIORef checksum
    (_, crc') <- CRC.hPutAllChunksCRC32C fs h (BSB.toLazyByteString builder) crc
    writeIORef checksum crc'
