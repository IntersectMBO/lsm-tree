module Database.LSMTree.Internal.WriteBufferWriter
  (
    writeWriteBuffer
  ) where

import           Control.Exception (assert)
import           Control.Monad (void, when)
import           Control.Monad.Class.MonadST (MonadST (..))
import qualified Control.Monad.Class.MonadST as ST
import           Control.Monad.Class.MonadSTM (MonadSTM (..))
import           Control.Monad.Class.MonadThrow (MonadThrow)
import           Control.Monad.Primitive (PrimMonad (..))
import           Control.Monad.ST (ST)
import           Data.Foldable (for_)
import           Data.Maybe (maybeToList)
import           Data.Primitive.PrimVar (PrimVar, newPrimVar)
import           Data.Word (Word64)
import           Database.LSMTree.Internal.BlobFile (BlobSpan)
import           Database.LSMTree.Internal.BlobRef (RawBlobRef)
import           Database.LSMTree.Internal.ChecksumHandle (ChecksumHandle,
                     closeHandle, copyBlob, dropCache, makeHandle, readChecksum,
                     writeRawOverflowPages, writeRawPage)
import qualified Database.LSMTree.Internal.CRC32C as CRC
import           Database.LSMTree.Internal.Entry (Entry)
import           Database.LSMTree.Internal.PageAcc (PageAcc)
import qualified Database.LSMTree.Internal.PageAcc as PageAcc
import qualified Database.LSMTree.Internal.PageAcc1 as PageAcc
import           Database.LSMTree.Internal.Paths (ForWriteBufferFiles (..),
                     WriteBufferFsPaths, forWriteBufferBlobRaw,
                     forWriteBufferKOpsRaw, pathsForWriteBufferFiles,
                     toChecksumsFileForWriteBufferFiles,
                     writeBufferChecksumsPath)
import           Database.LSMTree.Internal.RawOverflowPage (RawOverflowPage)
import           Database.LSMTree.Internal.RawPage (RawPage)
import           Database.LSMTree.Internal.Serialise (SerialisedKey,
                     SerialisedValue)
import           Database.LSMTree.Internal.WriteBuffer (WriteBuffer)
import qualified Database.LSMTree.Internal.WriteBuffer as WB
import           Database.LSMTree.Internal.WriteBufferBlobs (WriteBufferBlobs)
import qualified Database.LSMTree.Internal.WriteBufferBlobs as WBB
import qualified System.FS.API as FS
import           System.FS.API (HasFS)
import qualified System.FS.BlockIO.API as FS
import           System.FS.BlockIO.API (HasBlockIO)


writeWriteBuffer ::
     (MonadSTM m, MonadST m, MonadThrow m)
  => HasFS m h
  -> HasBlockIO m h
  -> WriteBufferFsPaths
  -> WriteBuffer
  -> WriteBufferBlobs m h
  -> m ()
writeWriteBuffer hfs hbio fsPaths buffer blobs = do
  writer <- new hfs hbio fsPaths
  for_ (WB.toList buffer) $ \(key, op) ->
    -- TODO: The fmap entry here reallocates even when there are no blobs.
    addKeyOp writer key (WBB.mkRawBlobRef blobs <$> op)
  void $ unsafeFinalise True writer

-- data SerialisedWriteBuffer m h = SerialisedWriteBuffer
--     { -- | The file system paths for all the files used by the serialised write buffer.
--       serialisedWriteBufferFsPaths    :: !WriteBufferFsPaths
--       -- | The (read mode) file handles.
--     , serialisedWriteBufferHandles    :: !(ForWriteBufferFiles (FS.Handle h))
--     , serialisedWriteBufferHasFS      :: !(HasFS m h)
--     , serialisedWriteBufferHasBlockIO :: !(HasBlockIO m h)
--     }

data WriteBufferWriter m h = WriteBufferWriter
  { -- | The file system paths for all the files used by the serialised write buffer.
    writeBufferWriterFsPaths    :: !WriteBufferFsPaths,
    -- | The page accumulator.
    writeBufferWriterPageAcc    :: !(PageAcc (PrimState m)),
    -- | The byte offset within the blob file for the next blob to be written.
    writeBufferWriterBlobOffset :: !(PrimVar (PrimState m) Word64),
    -- | The (write mode) file handles.
    writeBufferWriterHandles    :: !(ForWriteBufferFiles (ChecksumHandle (PrimState m) h)),
    writeBufferWriterHasFS      :: !(HasFS m h),
    writeBufferWriterHasBlockIO :: !(HasBlockIO m h)
  }

{-# SPECIALISE new ::
     HasFS IO h
  -> HasBlockIO IO h
  -> WriteBufferFsPaths
  -> IO (WriteBufferWriter IO h) #-}
-- | Create a 'WriteBufferWriter' to start serialising a 'WriteBuffer'.
new ::
     (MonadST m, MonadSTM m)
  => HasFS m h
  -> HasBlockIO m h
  -> WriteBufferFsPaths
  -> m (WriteBufferWriter m h)
new hfs hbio fsPaths = do
  writeBufferWriterPageAcc <- ST.stToIO PageAcc.newPageAcc
  writeBufferWriterBlobOffset <- newPrimVar 0
  writeBufferWriterHandles <-
    traverse (makeHandle hfs) (pathsForWriteBufferFiles fsPaths)
  return WriteBufferWriter
    { writeBufferWriterFsPaths    = fsPaths,
      writeBufferWriterHasFS      = hfs,
      writeBufferWriterHasBlockIO = hbio,
      ..
    }

-- | See 'RunBuilder.unsafeFinalise'.
-- Finalise an incremental 'WriteBufferWriter'.
-- Do /not/ use a 'WriteBufferWriter' after finalising it.
unsafeFinalise ::
     (MonadST m, MonadSTM m, MonadThrow m)
  => Bool -- ^ drop caches
  -> WriteBufferWriter m h
  -> m (HasFS m h, HasBlockIO m h, WriteBufferFsPaths)
unsafeFinalise dropCaches WriteBufferWriter {..} = do
  -- write final bits
  mPage <- ST.stToIO $ flushPageIfNonEmpty writeBufferWriterPageAcc
  for_ mPage $ writeRawPage writeBufferWriterHasFS (forWriteBufferKOps writeBufferWriterHandles)
  checksums <- toChecksumsFileForWriteBufferFiles <$> traverse readChecksum writeBufferWriterHandles
  FS.withFile writeBufferWriterHasFS (writeBufferChecksumsPath writeBufferWriterFsPaths) (FS.WriteMode FS.MustBeNew) $ \h -> do
    CRC.writeChecksumsFile' writeBufferWriterHasFS h checksums
    FS.hDropCacheAll writeBufferWriterHasBlockIO h
  -- drop the KOps and blobs files from the cache if asked for
  when dropCaches $ do
    dropCache writeBufferWriterHasBlockIO (forWriteBufferKOpsRaw writeBufferWriterHandles)
    dropCache writeBufferWriterHasBlockIO (forWriteBufferBlobRaw writeBufferWriterHandles)
  for_ writeBufferWriterHandles $ closeHandle writeBufferWriterHasFS
  return (writeBufferWriterHasFS, writeBufferWriterHasBlockIO, writeBufferWriterFsPaths)


-- | See 'RunBuilder.addKeyOp'.
addKeyOp ::
     (MonadST m, MonadSTM m, MonadThrow m)
  => WriteBufferWriter m h
  -> SerialisedKey
  -> Entry SerialisedValue (RawBlobRef m h)
  -> m ()
addKeyOp WriteBufferWriter{..} key op = do
  op' <- traverse (copyBlob writeBufferWriterHasFS writeBufferWriterBlobOffset (forWriteBufferBlob writeBufferWriterHandles)) op
  if PageAcc.entryWouldFitInPage key op
    then do
      mPage <- ST.stToIO $ addSmallKeyOp writeBufferWriterPageAcc key op'
      for_ mPage $ writeRawPage writeBufferWriterHasFS (forWriteBufferKOps writeBufferWriterHandles)
    else do
      (pages, overflowPages) <- ST.stToIO $ addLargeKeyOp writeBufferWriterPageAcc key op'
      --TODO: consider optimisation: use writev to write all pages in one go (see RunBuilder)
      for_ pages $ writeRawPage writeBufferWriterHasFS (forWriteBufferKOps writeBufferWriterHandles)
      writeRawOverflowPages writeBufferWriterHasFS (forWriteBufferKOps writeBufferWriterHandles) overflowPages

-- addLargeSerialisedKeyOp ::
--      (MonadST m, MonadSTM m)
--   => WriteBufferWriter m h
--   -> RawPage
--   -> [RawOverflowPage]
--   -> m ()
-- addLargeSerialisedKeyOp WriteBufferWriter{..} page overflowPages =
--   assert (RawPage.rawPageNumKeys page == 1) $
--   assert (RawPage.rawPageHasBlobSpanAt page 0 == 0) $
--   assert (RawPage.rawPageOverflowPages page > 0) $
--   assert (RawPage.rawPageOverflowPages page == length overflowPages) $ do
--     !pages <- ST.stToIO $ selectPages <$> flushPageIfNonEmpty writeBufferWriterPageAcc <*> pure page
--     for_ pages $ writeRawPage writeBufferWriterHasFS (forWriteBufferKOps writeBufferWriterHandles)
--     writeRawOverflowPages writeBufferWriterHasFS (forWriteBufferKOps writeBufferWriterHandles) overflowPages

-- | See 'RunAcc.addSmallKeyOp'.
addSmallKeyOp ::
     PageAcc s
  -> SerialisedKey
  -> Entry SerialisedValue BlobSpan
  -> ST s (Maybe RawPage)
addSmallKeyOp pageAcc key op =
  assert (PageAcc.entryWouldFitInPage key op) $ do
    pageBoundaryNeeded <-
        -- Try adding the key/op to the page accumulator to see if it fits. If
        -- it does not fit, a page boundary is needed.
        not <$> PageAcc.pageAccAddElem pageAcc key op
    if pageBoundaryNeeded
      then do
        mPage <- flushPageIfNonEmpty pageAcc
        added <- PageAcc.pageAccAddElem pageAcc key op
        assert added $ pure mPage
      else do
        pure Nothing

-- | See 'RunAcc.addLargeKeyOp'.
addLargeKeyOp
  :: PageAcc s
  -> SerialisedKey
  -> Entry SerialisedValue BlobSpan -- ^ the full value, not just a prefix
  -> ST s ([RawPage], [RawOverflowPage])
addLargeKeyOp pageAcc key op =
  assert (not (PageAcc.entryWouldFitInPage key op)) $ do
    -- If the existing page accumulator is non-empty, we flush it, since the
    -- new large key/op will need more than one page to itself.
    mPagePre <- flushPageIfNonEmpty pageAcc
    -- Make the new page and overflow pages. Add the span of pages to the index.
    let (page, overflowPages) = PageAcc.singletonPage key op

    -- Combine the results with anything we flushed before
    let !pages = selectPages mPagePre page
    return (pages, overflowPages)

-- | Internal helper. See 'RunAcc.flushPageIfNonEmpty'.
flushPageIfNonEmpty :: PageAcc s -> ST s (Maybe RawPage)
flushPageIfNonEmpty pageAcc = do
    keysCount <- PageAcc.keysCountPageAcc pageAcc
    if keysCount > 0
      then do
        -- Serialise the page and reset the accumulator
        page <- PageAcc.serialisePageAcc pageAcc
        PageAcc.resetPageAcc pageAcc
        pure $ Just page
      else pure Nothing

-- | Internal helper. See 'RunAcc.selectPagesAndChunks'.
selectPages :: Maybe RawPage
                     -> RawPage
                     -> [RawPage]
selectPages mPagePre page =
  maybeToList mPagePre ++ [page]

-- -- | See 'RunBuilder.close'.
-- close :: MonadSTM m => WriteBufferWriter m h -> m ()
-- close WriteBufferWriter {..} = do
--     for_ writeBufferWriterHandles $ closeHandle writeBufferWriterHasFS
--     for_ (pathsForWriteBufferFiles writeBufferWriterFsPaths) $ FS.removeFile writeBufferWriterHasFS
