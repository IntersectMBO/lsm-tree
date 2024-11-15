-- | A mutable run ('RunBuilder') that is under construction.
--
module Database.LSMTree.Internal.RunBuilder (
    RunBuilder (..)
  , new
  , addKeyOp
  , addLargeSerialisedKeyOp
  , unsafeFinalise
  , close
  ) where

import           Control.Monad (when)
import           Control.Monad.Class.MonadST (MonadST (..))
import qualified Control.Monad.Class.MonadST as ST
import           Control.Monad.Class.MonadSTM (MonadSTM (..))
import           Control.Monad.Class.MonadThrow (MonadThrow)
import           Control.Monad.Primitive
import           Data.BloomFilter (Bloom)
import           Data.Foldable (for_, traverse_)
import           Data.Primitive.PrimVar
import           Data.Word (Word64)
import           Database.LSMTree.Internal.BlobRef (RawBlobRef)
import           Database.LSMTree.Internal.ChecksumHandle
import qualified Database.LSMTree.Internal.CRC32C as CRC
import           Database.LSMTree.Internal.Entry
import           Database.LSMTree.Internal.IndexCompact (IndexCompact)
import           Database.LSMTree.Internal.Paths
import           Database.LSMTree.Internal.RawOverflowPage (RawOverflowPage)
import           Database.LSMTree.Internal.RawPage (RawPage)
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
data RunBuilder m h = RunBuilder {
      -- | The file system paths for all the files used by the run.
      runBuilderFsPaths    :: !RunFsPaths

      -- | The run accumulator. This is the representation used for the
      -- morally pure subset of the run cnstruction functionality. In
      -- particular it contains the (mutable) index, bloom filter and buffered
      -- pending output for the key\/ops file.
    , runBuilderAcc        :: !(RunAcc (PrimState m))

      -- | The byte offset within the blob file for the next blob to be written.
    , runBuilderBlobOffset :: !(PrimVar (PrimState m) Word64)

      -- | The (write mode) file handles.
    , runBuilderHandles    :: {-# UNPACK #-} !(ForRunFiles (ChecksumHandle (PrimState m) h))
    , runBuilderHasFS      :: !(HasFS m h)
    , runBuilderHasBlockIO :: !(HasBlockIO m h)
    }

{-# SPECIALISE new ::
     HasFS IO h
  -> HasBlockIO IO h
  -> RunFsPaths
  -> NumEntries
  -> RunBloomFilterAlloc
  -> IO (RunBuilder IO h) #-}
-- | Create an 'RunBuilder' to start building a run.
-- The result will have an initial reference count of 1.
--
-- NOTE: 'new' assumes that 'runDir' that the run is created in exists.
new ::
     (MonadST m, MonadSTM m)
  => HasFS m h
  -> HasBlockIO m h
  -> RunFsPaths
  -> NumEntries  -- ^ an upper bound of the number of entries to be added
  -> RunBloomFilterAlloc
  -> m (RunBuilder m h)
new hfs hbio runBuilderFsPaths numEntries alloc = do
    runBuilderAcc <- ST.stToIO $ RunAcc.new numEntries alloc
    runBuilderBlobOffset <- newPrimVar 0

    runBuilderHandles <- traverse (makeHandle hfs) (pathsForRunFiles runBuilderFsPaths)

    let builder = RunBuilder { runBuilderHasFS = hfs, runBuilderHasBlockIO = hbio, .. }
    writeIndexHeader hfs (forRunIndex runBuilderHandles)
    return builder

{-# SPECIALISE addKeyOp ::
     RunBuilder IO h
  -> SerialisedKey
  -> Entry SerialisedValue (RawBlobRef IO h)
  -> IO () #-}
-- | Add a key\/op pair.
--
-- In the 'InsertWithBlob' case, the 'RawBlobRef' identifies where the blob can be
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
  => RunBuilder m h
  -> SerialisedKey
  -> Entry SerialisedValue (RawBlobRef m h)
  -> m ()
addKeyOp RunBuilder{..} key op = do
    -- TODO: the fmap entry here reallocates even when there are no blobs.
    -- We need the Entry _ BlobSpan for RunAcc.add{Small,Large}KeyOp
    -- Perhaps pass the optional blob span separately from the Entry.
    op' <- traverse (copyBlob runBuilderHasFS runBuilderBlobOffset (forRunBlob runBuilderHandles)) op
    if RunAcc.entryWouldFitInPage key op'
      then do
        mpagemchunk <- ST.stToIO $ RunAcc.addSmallKeyOp runBuilderAcc key op'
        case mpagemchunk of
          Nothing -> return ()
          Just (page, mchunk) -> do
            writeRawPage runBuilderHasFS (forRunKOps runBuilderHandles) page
            for_ mchunk $ writeIndexChunk runBuilderHasFS (forRunIndex runBuilderHandles)

      else do
       (pages, overflowPages, chunks)
         <- ST.stToIO $ RunAcc.addLargeKeyOp runBuilderAcc key op'
       --TODO: consider optimisation: use writev to write all pages in one go
       for_ pages $ writeRawPage runBuilderHasFS (forRunKOps runBuilderHandles)
       writeRawOverflowPages runBuilderHasFS (forRunKOps runBuilderHandles) overflowPages
       for_ chunks $ writeIndexChunk runBuilderHasFS (forRunIndex runBuilderHandles)

{-# SPECIALISE addLargeSerialisedKeyOp ::
     RunBuilder IO h
  -> SerialisedKey
  -> RawPage
  -> [RawOverflowPage]
  -> IO () #-}
-- | See 'RunAcc.addLargeSerialisedKeyOp' for details.
--
addLargeSerialisedKeyOp ::
     (MonadST m, MonadSTM m)
  => RunBuilder m h
  -> SerialisedKey
  -> RawPage
  -> [RawOverflowPage]
  -> m ()
addLargeSerialisedKeyOp RunBuilder{..} key page overflowPages = do
    (pages, overflowPages', chunks)
      <- ST.stToIO $
           RunAcc.addLargeSerialisedKeyOp runBuilderAcc key page overflowPages
    for_ pages $ writeRawPage runBuilderHasFS (forRunKOps runBuilderHandles)
    writeRawOverflowPages runBuilderHasFS (forRunKOps runBuilderHandles) overflowPages'
    for_ chunks $ writeIndexChunk runBuilderHasFS (forRunIndex runBuilderHandles)

{-# SPECIALISE unsafeFinalise ::
     Bool
  -> RunBuilder IO h
  -> IO (HasFS IO h, HasBlockIO IO h, RunFsPaths, Bloom SerialisedKey, IndexCompact, NumEntries) #-}
-- | Finish construction of the run.
-- Writes the filter and index to file and leaves all written files on disk.
--
-- __Do not use the 'RunBuilder' after calling this function!__
--
-- TODO: Ensure proper cleanup even in presence of exceptions.
unsafeFinalise ::
     (MonadST m, MonadSTM m, MonadThrow m)
  => Bool -- ^ drop caches
  -> RunBuilder m h
  -> m (HasFS m h, HasBlockIO m h, RunFsPaths, Bloom SerialisedKey, IndexCompact, NumEntries)
unsafeFinalise dropCaches RunBuilder {..} = do
    -- write final bits
    (mPage, mChunk, runFilter, runIndex, numEntries) <-
      ST.stToIO (RunAcc.unsafeFinalise runBuilderAcc)
    for_ mPage $ writeRawPage runBuilderHasFS (forRunKOps runBuilderHandles)
    for_ mChunk $ writeIndexChunk runBuilderHasFS (forRunIndex runBuilderHandles)
    writeIndexFinal runBuilderHasFS (forRunIndex runBuilderHandles) numEntries runIndex
    writeFilter runBuilderHasFS (forRunFilter runBuilderHandles) runFilter
    -- write checksums
    checksums <- toChecksumsFile <$> traverse readChecksum runBuilderHandles
    FS.withFile runBuilderHasFS (runChecksumsPath runBuilderFsPaths) (FS.WriteMode FS.MustBeNew) $ \h -> do
      CRC.writeChecksumsFile' runBuilderHasFS h checksums
      -- always drop the checksum file from the cache
      FS.hDropCacheAll runBuilderHasBlockIO h
    -- always drop filter and index files from the cache
    dropCache runBuilderHasBlockIO (forRunFilterRaw runBuilderHandles)
    dropCache runBuilderHasBlockIO (forRunIndexRaw runBuilderHandles)
    -- drop the KOps and blobs files from the cache if asked for
    when dropCaches $ do
      dropCache runBuilderHasBlockIO (forRunKOpsRaw runBuilderHandles)
      dropCache runBuilderHasBlockIO (forRunBlobRaw runBuilderHandles)
    mapM_ (closeHandle runBuilderHasFS) runBuilderHandles
    return (runBuilderHasFS, runBuilderHasBlockIO, runBuilderFsPaths, runFilter, runIndex, numEntries)

{-# SPECIALISE close :: RunBuilder IO h -> IO () #-}
-- | Close a run that is being constructed (has not been finalised yet),
-- removing all files associated with it from disk.
-- After calling this operation, the run must not be used anymore.
--
-- TODO: Ensure proper cleanup even in presence of exceptions.
close :: MonadSTM m => RunBuilder m h -> m ()
close RunBuilder {..} = do
    traverse_ (closeHandle runBuilderHasFS) runBuilderHandles
    traverse_ (FS.removeFile runBuilderHasFS) (pathsForRunFiles runBuilderFsPaths)
