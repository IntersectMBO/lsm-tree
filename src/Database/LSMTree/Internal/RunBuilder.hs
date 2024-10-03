-- | A mutable run ('RunBuilder') that is under construction.
--
module Database.LSMTree.Internal.RunBuilder (
    RunBuilder (..)
  , new
  , addKeyOp
  , addLargeSerialisedKeyOp
  , unsafeFinalise
  , close
    -- Internal: exposed for testing
  , ChecksumHandle (..)
  ) where

import           Control.Monad (when)
import           Control.Monad.Class.MonadST (MonadST (..))
import qualified Control.Monad.Class.MonadST as ST
import           Control.Monad.Class.MonadSTM (MonadSTM (..))
import           Control.Monad.Class.MonadThrow (MonadThrow)
import           Control.Monad.Primitive
import           Data.BloomFilter (Bloom)
import qualified Data.ByteString.Lazy as BSL
import           Data.Foldable (for_, traverse_)
import           Data.Primitive
import           Data.Word (Word64)
import           Database.LSMTree.Internal.BlobRef (BlobRef, BlobSpan (..))
import qualified Database.LSMTree.Internal.BlobRef as BlobRef
import           Database.LSMTree.Internal.BloomFilter (bloomFilterToLBS)
import           Database.LSMTree.Internal.CRC32C (CRC32C)
import qualified Database.LSMTree.Internal.CRC32C as CRC
import           Database.LSMTree.Internal.Entry
import           Database.LSMTree.Internal.IndexCompact (IndexCompact)
import qualified Database.LSMTree.Internal.IndexCompact as Index
import           Database.LSMTree.Internal.Paths
import qualified Database.LSMTree.Internal.RawBytes as RB
import           Database.LSMTree.Internal.RawOverflowPage (RawOverflowPage)
import qualified Database.LSMTree.Internal.RawOverflowPage as RawOverflowPage
import           Database.LSMTree.Internal.RawPage (RawPage)
import qualified Database.LSMTree.Internal.RawPage as RawPage
import           Database.LSMTree.Internal.RunAcc (RunAcc, RunBloomFilterAlloc)
import qualified Database.LSMTree.Internal.RunAcc as RunAcc
import           Database.LSMTree.Internal.Serialise
import qualified System.FS.API as FS
import           System.FS.API (HasFS)
import qualified System.FS.BlockIO.API as FS
import           System.FS.BlockIO.API (HasBlockIO)

-- | The in-memory representation of an LSM run that is under construction.
-- (The \"M\" stands for mutable.) This is the output sink for two key
-- algorithms: 1. writing out the write buffer, and 2. incrementally merging
-- two or more runs.
--
-- It contains open file handles for all four files used in the disk
-- representation of a run. Each file handle is opened write-only and should be
-- written to using normal buffered I\/O.
--
-- __Not suitable for concurrent construction from multiple threads!__
--
data RunBuilder s fhandle = RunBuilder {
      -- | The file system paths for all the files used by the run.
      runBuilderFsPaths    :: !RunFsPaths

      -- | The run accumulator. This is the representation used for the
      -- morally pure subset of the run cnstruction functionality. In
      -- particular it contains the (mutable) index, bloom filter and buffered
      -- pending output for the key\/ops file.
    , runBuilderAcc        :: !(RunAcc s)

      -- | The byte offset within the blob file for the next blob to be written.
    , runBuilderBlobOffset :: !(MutVar s Word64)

      -- | The (write mode) file handles.
    , runBuilderHandles    :: {-# UNPACK #-} !(ForRunFiles (ChecksumHandle s fhandle))
    }

{-# SPECIALISE new ::
     HasFS IO h
  -> RunFsPaths
  -> NumEntries
  -> RunBloomFilterAlloc
  -> IO (RunBuilder RealWorld (FS.Handle h)) #-}
-- | Create an 'RunBuilder' to start building a run.
-- The result will have an initial reference count of 1.
--
-- NOTE: 'new' assumes that 'runDir' that the run is created in exists.
new ::
     (MonadST m, MonadSTM m)
  => HasFS m h
  -> RunFsPaths
  -> NumEntries  -- ^ an upper bound of the number of entries to be added
  -> RunBloomFilterAlloc
  -> m (RunBuilder (PrimState m) (FS.Handle h))
new fs runBuilderFsPaths numEntries alloc = do
    runBuilderAcc <- ST.stToIO $ RunAcc.new numEntries alloc
    runBuilderBlobOffset <- newMutVar 0

    runBuilderHandles <- traverse (makeHandle fs) (pathsForRunFiles runBuilderFsPaths)

    let builder = RunBuilder {..}
    writeIndexHeader fs builder
    return builder

{-# SPECIALISE addKeyOp ::
     HasFS IO h
  -> RunBuilder RealWorld (FS.Handle h)
  -> SerialisedKey
  -> Entry SerialisedValue (BlobRef IO (FS.Handle h))
  -> IO () #-}
-- | Add a key\/op pair.
--
-- In the 'InsertWithBlob' case, the 'BlobRef' identifies where the blob can be
-- found (which is either from a write buffer or another run). The blobs will
-- be copied from their existing blob file into the new run's blob file.
--
-- Use only for entries that are fully in-memory (other than any blob).
-- To handle larger-than-page values in a chunked style during run merging,
-- use 'addLargeSerialisedKeyOp'.
--
-- The k\/ops and the primary array of the index get written incrementally,
-- everything else only at the end when 'unsafeFinalise' is called.
--
addKeyOp ::
     (MonadST m, MonadSTM m, MonadThrow m)
  => HasFS m h
  -> RunBuilder (PrimState m) (FS.Handle h)
  -> SerialisedKey
  -> Entry SerialisedValue (BlobRef m (FS.Handle h))
  -> m ()
addKeyOp fs builder@RunBuilder{runBuilderAcc} key op = do
    --TODO: the fmap entry here reallocates even when there are no blobs.
    -- We need the Entry _ BlobSpan for RunAcc.add{Small,Large}KeyOp
    -- Perhaps pass the optional blob span separately from the Entry.
    op' <- traverse (copyBlob fs builder) op
    if RunAcc.entryWouldFitInPage key op'
      then do
        mpagemchunk <- ST.stToIO $ RunAcc.addSmallKeyOp runBuilderAcc key op'
        case mpagemchunk of
          Nothing -> return ()
          Just (page, mchunk) -> do
            writeRawPage fs builder page
            for_ mchunk $ writeIndexChunk fs builder

      else do
       (pages, overflowPages, chunks)
         <- ST.stToIO $ RunAcc.addLargeKeyOp runBuilderAcc key op'
       --TODO: consider optimisation: use writev to write all pages in one go
       for_ pages $ writeRawPage fs builder
       writeRawOverflowPages fs builder overflowPages
       for_ chunks $ writeIndexChunk fs builder

{-# SPECIALISE addLargeSerialisedKeyOp ::
     HasFS IO h
  -> RunBuilder RealWorld (FS.Handle h)
  -> SerialisedKey
  -> RawPage
  -> [RawOverflowPage]
  -> IO () #-}
-- | See 'RunAcc.addLargeSerialisedKeyOp' for details.
--
addLargeSerialisedKeyOp ::
     (MonadST m, MonadSTM m)
  => HasFS m h
  -> RunBuilder (PrimState m) (FS.Handle h)
  -> SerialisedKey
  -> RawPage
  -> [RawOverflowPage]
  -> m ()
addLargeSerialisedKeyOp fs builder@RunBuilder{runBuilderAcc} key page overflowPages = do
    (pages, overflowPages', chunks)
      <- ST.stToIO $
           RunAcc.addLargeSerialisedKeyOp runBuilderAcc key page overflowPages
    for_ pages $ writeRawPage fs builder
    writeRawOverflowPages fs builder overflowPages'
    for_ chunks $ writeIndexChunk fs builder

{-# SPECIALISE unsafeFinalise ::
     HasFS IO h
  -> HasBlockIO IO h
  -> Bool
  -> RunBuilder RealWorld (FS.Handle h)
  -> IO (RunFsPaths, Bloom SerialisedKey, IndexCompact, NumEntries) #-}
-- | Finish construction of the run.
-- Writes the filter and index to file and leaves all written files on disk.
--
-- __Do not use the 'RunBuilder' after calling this function!__
--
-- TODO: Ensure proper cleanup even in presence of exceptions.
unsafeFinalise ::
     (MonadST m, MonadSTM m, MonadThrow m)
  => HasFS m h
  -> HasBlockIO m h
  -> Bool -- ^ drop caches
  -> RunBuilder (PrimState m) (FS.Handle h)
  -> m (RunFsPaths, Bloom SerialisedKey, IndexCompact, NumEntries)
unsafeFinalise fs hbio dropCaches builder@RunBuilder {..} = do
    -- write final bits
    (mPage, mChunk, runFilter, runIndex, numEntries) <-
      ST.stToIO (RunAcc.unsafeFinalise runBuilderAcc)
    for_ mPage $ writeRawPage fs builder
    for_ mChunk $ writeIndexChunk fs builder
    writeIndexFinal fs builder numEntries runIndex
    writeFilter fs builder runFilter
    -- write checksums
    checksums <- toChecksumsFile <$> traverse readChecksum runBuilderHandles
    FS.withFile fs (runChecksumsPath runBuilderFsPaths) (FS.WriteMode FS.MustBeNew) $ \h -> do
      CRC.writeChecksumsFile' fs h checksums
      -- always drop the checksum file from the cache
      FS.hDropCacheAll hbio h
    -- always drop filter and index files from the cache
    dropCache hbio (forRunFilter runBuilderHandles)
    dropCache hbio (forRunIndex runBuilderHandles)
    -- drop the KOps and blobs files from the cache if asked for
    when dropCaches $ do
      dropCache hbio (forRunKOps runBuilderHandles)
      dropCache hbio (forRunBlob runBuilderHandles)
    mapM_ (closeHandle fs) runBuilderHandles
    return (runBuilderFsPaths, runFilter, runIndex, numEntries)

{-# SPECIALISE close :: HasFS IO h
   -> RunBuilder RealWorld (FS.Handle h)
   -> IO () #-}
-- | Close a run that is being constructed (has not been finalised yet),
-- removing all files associated with it from disk.
-- After calling this operation, the run must not be used anymore.
--
-- TODO: Ensure proper cleanup even in presence of exceptions.
close ::
     MonadSTM m
  => HasFS m h
  -> RunBuilder (PrimState m) (FS.Handle h)
  -> m ()
close fs RunBuilder {..} = do
    traverse_ (closeHandle fs) runBuilderHandles
    traverse_ (FS.removeFile fs) (pathsForRunFiles runBuilderFsPaths)

{-------------------------------------------------------------------------------
  Helpers
-------------------------------------------------------------------------------}

{-# SPECIALISE writeRawPage :: HasFS IO h
  -> RunBuilder RealWorld (FS.Handle h)
  -> RawPage
  -> IO () #-}
writeRawPage ::
     (MonadSTM m, PrimMonad m)
  => HasFS m h
  -> RunBuilder (PrimState m) (FS.Handle h)
  -> RawPage
  -> m ()
writeRawPage fs RunBuilder {..} =
    writeToHandle fs (forRunKOps runBuilderHandles)
  . BSL.fromStrict
  . RB.unsafePinnedToByteString -- 'RawPage' is guaranteed to be pinned
  . RawPage.rawPageRawBytes

{-# SPECIALISE writeRawOverflowPages :: HasFS IO h
  -> RunBuilder RealWorld (FS.Handle h)
  -> [RawOverflowPage]
  -> IO () #-}
writeRawOverflowPages ::
     (MonadSTM m, PrimMonad m)
  => HasFS m h
  -> RunBuilder (PrimState m) (FS.Handle h)
  -> [RawOverflowPage]
  -> m ()
writeRawOverflowPages fs RunBuilder {..} =
    writeToHandle fs (forRunKOps runBuilderHandles)
  . BSL.fromChunks
  . map (RawOverflowPage.rawOverflowPageToByteString)

{-# SPECIALISE writeBlob :: HasFS IO h
  -> RunBuilder RealWorld (FS.Handle h)
  -> SerialisedBlob
  -> IO BlobSpan #-}
writeBlob ::
     (MonadSTM m, PrimMonad m)
  => HasFS m h
  -> RunBuilder (PrimState m) (FS.Handle h)
  -> SerialisedBlob
  -> m BlobSpan
writeBlob fs RunBuilder{..} blob = do
    let size = sizeofBlob64 blob
    offset <- readMutVar runBuilderBlobOffset
    modifyMutVar' runBuilderBlobOffset (+size)
    let SerialisedBlob rb = blob
    let lbs = BSL.fromStrict $ RB.toByteString rb
    writeToHandle fs (forRunBlob runBuilderHandles) lbs
    return (BlobSpan offset (fromIntegral size))

{-# SPECIALISE copyBlob ::
     HasFS IO h
  -> RunBuilder RealWorld (FS.Handle h)
  -> BlobRef IO (FS.Handle h)
  -> IO BlobRef.BlobSpan #-}
copyBlob ::
     (MonadSTM m, MonadThrow m, PrimMonad m)
  => HasFS m h
  -> RunBuilder (PrimState m) (FS.Handle h)
  -> BlobRef m (FS.Handle h)
  -> m BlobRef.BlobSpan
copyBlob fs builder blobref = do
    blob <- BlobRef.readBlob fs blobref
    writeBlob fs builder blob

{-# SPECIALISE writeFilter ::
     HasFS IO h
  -> RunBuilder RealWorld (FS.Handle h)
  -> Bloom SerialisedKey
  -> IO () #-}
writeFilter :: (MonadSTM m, PrimMonad m) => HasFS m h -> RunBuilder (PrimState m) (FS.Handle h) -> Bloom SerialisedKey -> m ()
writeFilter fs RunBuilder {..} bf =
    writeToHandle fs (forRunFilter runBuilderHandles) (bloomFilterToLBS bf)

{-# SPECIALISE writeIndexHeader ::
     HasFS IO h
  -> RunBuilder RealWorld (FS.Handle h)
  -> IO () #-}
writeIndexHeader :: (MonadSTM m, PrimMonad m) => HasFS m h -> RunBuilder (PrimState m) (FS.Handle h) -> m ()
writeIndexHeader fs RunBuilder {..} =
    writeToHandle fs (forRunIndex runBuilderHandles) $
      Index.headerLBS

{-# SPECIALISE writeIndexChunk ::
     HasFS IO h
  -> RunBuilder RealWorld (FS.Handle h)
  -> Index.Chunk
  -> IO () #-}
writeIndexChunk :: (MonadSTM m, PrimMonad m) => HasFS m h -> RunBuilder (PrimState m) (FS.Handle h) -> Index.Chunk -> m ()
writeIndexChunk fs RunBuilder {..} chunk =
    writeToHandle fs (forRunIndex runBuilderHandles) $
      BSL.fromStrict $ Index.chunkToBS chunk

{-# SPECIALISE writeIndexFinal ::
     HasFS IO h
  -> RunBuilder RealWorld (FS.Handle h)
  -> NumEntries
  -> IndexCompact
  -> IO () #-}
writeIndexFinal ::
     (MonadSTM m, PrimMonad m)
  => HasFS m h
  -> RunBuilder (PrimState m) (FS.Handle h)
  -> NumEntries
  -> IndexCompact
  -> m ()
writeIndexFinal fs RunBuilder {..} numEntries index =
    writeToHandle fs (forRunIndex runBuilderHandles) $
      Index.finalLBS numEntries index

{-------------------------------------------------------------------------------
  ChecksumHandle
-------------------------------------------------------------------------------}

-- | Tracks the checksum of a (write mode) file handle.
data ChecksumHandle s fhandle = ChecksumHandle !fhandle !(MutVar s CRC32C)

{-# SPECIALISE makeHandle ::
     HasFS IO h
  -> FS.FsPath
  -> IO (ChecksumHandle RealWorld (FS.Handle h)) #-}
makeHandle ::
     (MonadSTM m, PrimMonad m)
  => HasFS m h
  -> FS.FsPath
  -> m (ChecksumHandle (PrimState m) (FS.Handle h))
makeHandle fs path =
    ChecksumHandle
      <$> FS.hOpen fs path (FS.WriteMode FS.MustBeNew)
      <*> newMutVar CRC.initialCRC32C

{-# SPECIALISE readChecksum ::
     ChecksumHandle RealWorld (FS.Handle h)
  -> IO CRC32C #-}
readChecksum ::
     PrimMonad m
  => ChecksumHandle (PrimState m) (FS.Handle h)
  -> m CRC32C
readChecksum (ChecksumHandle _h checksum) = readMutVar checksum

dropCache :: HasBlockIO m h -> ChecksumHandle (PrimState m) (FS.Handle h) -> m ()
dropCache hbio (ChecksumHandle h _) = FS.hDropCacheAll hbio h

closeHandle :: HasFS m h -> ChecksumHandle (PrimState m) (FS.Handle h) -> m ()
closeHandle fs (ChecksumHandle h _checksum) = FS.hClose fs h

{-# SPECIALISE writeToHandle ::
     HasFS IO h
  -> ChecksumHandle RealWorld (FS.Handle h)
  -> BSL.ByteString
  -> IO () #-}
writeToHandle ::
     (MonadSTM m, PrimMonad m)
  => HasFS m h
  -> ChecksumHandle (PrimState m) (FS.Handle h)
  -> BSL.ByteString
  -> m ()
writeToHandle fs (ChecksumHandle h checksum) lbs = do
    crc <- readMutVar checksum
    (_, crc') <- CRC.hPutAllChunksCRC32C fs h lbs crc
    writeMutVar checksum crc'
