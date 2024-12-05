-- | A write buffer that is being read incrementally.
--
module Database.LSMTree.Internal.WriteBufferReader (
    readWriteBuffer
  ) where

import           Control.Monad.Class.MonadST (MonadST (..))
import           Control.Monad.Class.MonadSTM (MonadSTM (..))
import           Control.Monad.Class.MonadThrow (MonadCatch (..), MonadMask,
                     MonadThrow (..))
import           Control.Monad.Primitive (PrimMonad (..))
import           Data.Primitive.MutVar (MutVar, newMutVar, readMutVar,
                     writeMutVar)
import           Data.Primitive.PrimVar
import qualified Data.Vector as V
import           Data.Word (Word16)
import           Database.LSMTree.Internal.BlobFile (BlobFile, openBlobFile)
import qualified Database.LSMTree.Internal.BlobFile as BlobFile
import           Database.LSMTree.Internal.BlobRef (RawBlobRef (..),
                     readRawBlobRef)
import qualified Database.LSMTree.Internal.Entry as E
import           Database.LSMTree.Internal.Lookup (ResolveSerialisedValue)
import           Database.LSMTree.Internal.MergeSchedule (addWriteBufferEntries)
import           Database.LSMTree.Internal.Paths
import           Database.LSMTree.Internal.RawPage
import           Database.LSMTree.Internal.RunReader (Entry (..), Result (..),
                     mkEntryOverflow, readDiskPage, readOverflowPages,
                     toFullEntry)
import           Database.LSMTree.Internal.Serialise (SerialisedValue)
import           Database.LSMTree.Internal.WriteBuffer (WriteBuffer)
import qualified Database.LSMTree.Internal.WriteBuffer as WB
import           Database.LSMTree.Internal.WriteBufferBlobs (WriteBufferBlobs)
import qualified Database.LSMTree.Internal.WriteBufferBlobs as WBB
import qualified System.FS.API as FS
import           System.FS.API (HasFS)
import qualified System.FS.BlockIO.API as FS
import           System.FS.BlockIO.API (HasBlockIO)

{-# SPECIALISE
    readWriteBuffer ::
         HasFS IO h
      -> HasBlockIO IO h
      -> ResolveSerialisedValue
      -> ForBlob FS.FsPath
      -> ForKOps FS.FsPath
      -> ForBlob FS.FsPath
      -> IO (WriteBuffer, WriteBufferBlobs IO h)
  #-}
readWriteBuffer ::
     (MonadMask m, MonadSTM m, MonadST m)
  => HasFS m h
  -> HasBlockIO m h
  -> ResolveSerialisedValue
  -> ForBlob FS.FsPath
  -> ForKOps FS.FsPath
  -> ForBlob FS.FsPath
  -> m (WriteBuffer, WriteBufferBlobs m h)
readWriteBuffer hfs hbio f wbbPath kOpsPath blobPath =
  bracket (new hfs hbio kOpsPath blobPath) close $ \reader -> do
    let readEntry =
          E.traverseBlobRef (readRawBlobRef hfs) . toFullEntry
    let readEntries = next reader >>= \case
          Empty -> pure []
          ReadEntry key entry ->
            readEntry entry >>= \entry' ->
              ((key,  entry') :) <$> readEntries
    es <- V.fromList <$> readEntries
    wbb <- WBB.new hfs (unForBlob wbbPath)
    -- TODO: We cannot derive the number of entries from the in-memory index.
    let maxn = E.NumEntries $ V.length es
    wb <- fst <$> addWriteBufferEntries hfs f wbb maxn WB.empty es
    pure (wb, wbb)

-- | Allows reading the k\/ops of a run incrementally, using its own read-only
-- file handle and in-memory cache of the current disk page.
--
-- New pages are loaded when trying to read their first entry.
data WriteBufferReader m h = WriteBufferReader {
      -- | The disk page currently being read. If it is 'Nothing', the reader
      -- is considered closed.
      readerCurrentPage    :: !(MutVar (PrimState m) (Maybe RawPage))
      -- | The index of the entry to be returned by the next call to 'next'.
    , readerCurrentEntryNo :: !(PrimVar (PrimState m) Word16)
    , readerKOpsHandle     :: !(FS.Handle h)
    , readerBlobFile       :: !(BlobFile m h)
    , readerHasFS          :: !(HasFS m h)
    , readerHasBlockIO     :: !(HasBlockIO m h)
    }

{-# SPECIALISE
    new ::
         HasFS IO h
      -> HasBlockIO IO h
      -> ForKOps FS.FsPath
      -> ForBlob FS.FsPath
      -> IO (WriteBufferReader IO h)
  #-}
-- | See 'Database.LSMTree.Internal.RunReader.new'.
new :: forall m h.
     (MonadCatch m, MonadSTM m, MonadST m)
  => HasFS m h
  -> HasBlockIO m h
  -> ForKOps FS.FsPath
  -> ForBlob FS.FsPath
  -> m (WriteBufferReader m h)
new readerHasFS readerHasBlockIO kOpsPath blobPath = do
  (readerKOpsHandle :: FS.Handle h) <- FS.hOpen readerHasFS (unForKOps kOpsPath) FS.ReadMode
  -- Double the file readahead window (only applies to this file descriptor)
  FS.hAdviseAll readerHasBlockIO readerKOpsHandle FS.AdviceSequential
  readerBlobFile <- openBlobFile readerHasFS (unForBlob blobPath) FS.ReadMode
  -- Load first page from disk, if it exists.
  readerCurrentEntryNo <- newPrimVar (0 :: Word16)
  firstPage <- readDiskPage readerHasFS readerKOpsHandle
  readerCurrentPage <- newMutVar firstPage
  pure $ WriteBufferReader{..}

{-# SPECIALISE
  next ::
       WriteBufferReader IO h
    -> IO (Result IO h)
  #-}
-- | See 'Database.LSMTree.Internal.RunReader.next'.
next :: forall m h.
     (MonadSTM m, MonadST m, MonadMask m)
  => WriteBufferReader m h
  -> m (Result m h)
next reader@WriteBufferReader {..} = do
    readMutVar readerCurrentPage >>= \case
      Nothing ->
        return Empty
      Just page -> do
        entryNo <- readPrimVar readerCurrentEntryNo
        go entryNo page
  where
    go :: Word16 -> RawPage -> m (Result m h)
    go !entryNo !page =
        -- take entry from current page (resolve blob if necessary)
        case rawPageIndex page entryNo of
          IndexNotPresent -> do
            -- if it is past the last one, load a new page from disk, try again
            newPage <- readDiskPage readerHasFS readerKOpsHandle
            stToIO $ writeMutVar readerCurrentPage newPage
            case newPage of
              Nothing -> do
                return Empty
              Just p -> do
                writePrimVar readerCurrentEntryNo 0
                go 0 p  -- try again on the new page
          IndexEntry key entry -> do
            modifyPrimVar readerCurrentEntryNo (+1)
            let entry' :: E.Entry SerialisedValue (RawBlobRef m h)
                entry' = fmap (RawBlobRef readerBlobFile) entry
            let rawEntry = Entry entry'
            return (ReadEntry key rawEntry)
          IndexEntryOverflow key entry lenSuffix -> do
            -- TODO: we know that we need the next page, could already load?
            modifyPrimVar readerCurrentEntryNo (+1)
            let entry' :: E.Entry SerialisedValue (RawBlobRef m h)
                entry' = fmap (RawBlobRef readerBlobFile) entry
            overflowPages <- readOverflowPages readerHasFS readerKOpsHandle lenSuffix
            let rawEntry = mkEntryOverflow entry' page lenSuffix overflowPages
            return (ReadEntry key rawEntry)

{-# SPECIALISE
  close ::
       WriteBufferReader IO h
    -> IO ()
  #-}
-- | See 'Database.LSMTree.Internal.RunReader.close'.
close ::
     (MonadMask m, PrimMonad m)
  => WriteBufferReader m h
  -> m ()
close WriteBufferReader{..} = do
    FS.hClose readerHasFS readerKOpsHandle
    BlobFile.removeReference readerBlobFile
