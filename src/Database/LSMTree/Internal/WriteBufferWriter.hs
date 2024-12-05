module Database.LSMTree.Internal.WriteBufferWriter
  ( writeWriteBuffer
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


{-# SPECIALISE
    writeWriteBuffer ::
         HasFS IO h
      -> HasBlockIO IO h
      -> WriteBufferFsPaths
      -> WriteBuffer
      -> WriteBufferBlobs IO h
      -> IO ()
    #-}
-- | Write a 'WriteBuffer' to disk.
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

-- | The in-memory representation of an LSM 'WriteBuffer' that is in the process of being serialised to disk.
data WriteBufferWriter m h = WriteBufferWriter
  { -- | The file system paths for all the files used by the serialised write buffer.
    writerFsPaths    :: !WriteBufferFsPaths,
    -- | The page accumulator.
    writerPageAcc    :: !(PageAcc (PrimState m)),
    -- | The byte offset within the blob file for the next blob to be written.
    writerBlobOffset :: !(PrimVar (PrimState m) Word64),
    -- | The (write mode) file handles.
    writerHandles    :: !(ForWriteBufferFiles (ChecksumHandle (PrimState m) h)),
    writerHasFS      :: !(HasFS m h),
    writerHasBlockIO :: !(HasBlockIO m h)
  }

{-# SPECIALISE
  new ::
      HasFS IO h
    -> HasBlockIO IO h
    -> WriteBufferFsPaths
    -> IO (WriteBufferWriter IO h)
  #-}
-- | Create a 'WriteBufferWriter' to start serialising a 'WriteBuffer'.
--
-- See 'Database.LSMTree.Internal.RunBuilder.new'.
--
-- NOTE: 'new' assumes that the directory passed via 'WriteBufferFsPaths' exists.
new ::
     (MonadST m, MonadSTM m)
  => HasFS m h
  -> HasBlockIO m h
  -> WriteBufferFsPaths
  -> m (WriteBufferWriter m h)
new hfs hbio fsPaths = do
  writerPageAcc <- ST.stToIO PageAcc.newPageAcc
  writerBlobOffset <- newPrimVar 0
  writerHandles <-
    traverse (makeHandle hfs) (pathsForWriteBufferFiles fsPaths)
  return WriteBufferWriter
    { writerFsPaths    = fsPaths,
      writerHasFS      = hfs,
      writerHasBlockIO = hbio,
      ..
    }

{-# SPECIALISE
    unsafeFinalise ::
        Bool
      -> WriteBufferWriter IO h
      -> IO (HasFS IO h, HasBlockIO IO h, WriteBufferFsPaths)
  #-}
-- | Finalise an incremental 'WriteBufferWriter'.
--
-- Do /not/ use a 'WriteBufferWriter' after finalising it.
--
-- See 'Database.LSMTree.Internal.RunBuilder.unsafeFinalise'.
--
-- TODO: Ensure proper cleanup even in presence of exceptions.
unsafeFinalise ::
     (MonadST m, MonadSTM m, MonadThrow m)
  => Bool -- ^ drop caches
  -> WriteBufferWriter m h
  -> m (HasFS m h, HasBlockIO m h, WriteBufferFsPaths)
unsafeFinalise dropCaches WriteBufferWriter {..} = do
  -- write final bits
  mPage <- ST.stToIO $ flushPageIfNonEmpty writerPageAcc
  for_ mPage $ writeRawPage writerHasFS (forWriteBufferKOps writerHandles)
  checksums <- toChecksumsFileForWriteBufferFiles <$> traverse readChecksum writerHandles
  FS.withFile writerHasFS (writeBufferChecksumsPath writerFsPaths) (FS.WriteMode FS.MustBeNew) $ \h -> do
    CRC.writeChecksumsFile' writerHasFS h checksums
    FS.hDropCacheAll writerHasBlockIO h
  -- drop the KOps and blobs files from the cache if asked for
  when dropCaches $ do
    dropCache writerHasBlockIO (forWriteBufferKOpsRaw writerHandles)
    dropCache writerHasBlockIO (forWriteBufferBlobRaw writerHandles)
  for_ writerHandles $ closeHandle writerHasFS
  return (writerHasFS, writerHasBlockIO, writerFsPaths)


{-# SPECIALIZE
    addKeyOp ::
        WriteBufferWriter IO h
      -> SerialisedKey
      -> Entry SerialisedValue (RawBlobRef IO h)
      -> IO ()
    #-}
-- | See 'Database.LSMTree.Internal.RunBuilder.addKeyOp'.
addKeyOp ::
     (MonadST m, MonadSTM m, MonadThrow m)
  => WriteBufferWriter m h
  -> SerialisedKey
  -> Entry SerialisedValue (RawBlobRef m h)
  -> m ()
addKeyOp WriteBufferWriter{..} key op = do
  -- TODO: consider optimisation described in 'Database.LSMTree.Internal.RunBuilder.addKeyOp'.
  op' <- traverse (copyBlob writerHasFS writerBlobOffset (forWriteBufferBlob writerHandles)) op
  if PageAcc.entryWouldFitInPage key op
    then do
      mPage <- ST.stToIO $ addSmallKeyOp writerPageAcc key op'
      for_ mPage $ writeRawPage writerHasFS (forWriteBufferKOps writerHandles)
    else do
      (pages, overflowPages) <- ST.stToIO $ addLargeKeyOp writerPageAcc key op'
      -- TODO: consider optimisation described in 'Database.LSMTree.Internal.RunBuilder.addKeyOp'.
      for_ pages $ writeRawPage writerHasFS (forWriteBufferKOps writerHandles)
      writeRawOverflowPages writerHasFS (forWriteBufferKOps writerHandles) overflowPages

-- | See 'Database.LSMTree.Internal.RunAcc.addSmallKeyOp'.
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
        -- We need a page boundary. If the current page is empty then we have
        -- a boundary already, otherwise we need to flush the current page.
        mPage <- flushPageIfNonEmpty pageAcc
        -- The current page is now empty, either because it was already empty
        -- or because we just flushed it. Adding the new key/op to an empty
        -- page must now succeed, because we know it fits in a page.
        added <- PageAcc.pageAccAddElem pageAcc key op
        assert added $ pure mPage
      else do
        pure Nothing

-- | See 'Database.LSMTree.Internal.RunAcc.addLargeKeyOp'.
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

-- | Internal helper. See 'Database.LSMTree.Internal.RunAcc.flushPageIfNonEmpty'.
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

-- | Internal helper. See 'Database.LSMTree.Internal.RunAcc.selectPagesAndChunks'.
selectPages :: Maybe RawPage
                     -> RawPage
                     -> [RawPage]
selectPages mPagePre page =
  maybeToList mPagePre ++ [page]
