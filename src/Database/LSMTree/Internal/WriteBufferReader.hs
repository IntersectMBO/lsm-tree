-- | A write buffer that is being read incrementally.
--
module Database.LSMTree.Internal.WriteBufferReader (
    readWriteBuffer
  ) where

import           Control.Concurrent.Class.MonadMVar.Strict
import           Control.Monad.Class.MonadST (MonadST (..))
import           Control.Monad.Class.MonadSTM (MonadSTM (..))
import           Control.Monad.Class.MonadThrow (MonadMask, MonadThrow (..))
import           Control.Monad.Primitive (PrimMonad (..))
import           Control.RefCount (Ref, releaseRef)
import           Control.TempRegistry
import           Data.Primitive.MutVar (MutVar, newMutVar, readMutVar,
                     writeMutVar)
import           Data.Primitive.PrimVar
import           Data.Word (Word16)
import           Database.LSMTree.Internal.BlobRef (RawBlobRef (..))
import qualified Database.LSMTree.Internal.Entry as E
import           Database.LSMTree.Internal.Lookup (ResolveSerialisedValue)
import           Database.LSMTree.Internal.Paths
import qualified Database.LSMTree.Internal.Paths as Paths
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
         TempRegistry IO
      -> ResolveSerialisedValue
      -> HasFS IO h
      -> HasBlockIO IO h
      -> WriteBufferFsPaths
      -> IO (WriteBuffer, Ref (WriteBufferBlobs IO h))
  #-}
readWriteBuffer ::
     (MonadMVar m, MonadMask m, MonadSTM m, MonadST m)
  => TempRegistry m
  -> ResolveSerialisedValue
  -> HasFS m h
  -> HasBlockIO m h
  -> WriteBufferFsPaths
  -> m (WriteBuffer, Ref (WriteBufferBlobs m h))
readWriteBuffer reg resolve hfs hbio wbPaths =
  bracket (new reg hfs hbio wbPaths) close $ \reader@WriteBufferReader{..} ->
    (,) <$> readEntries reader <*> pure readerWriteBufferBlobs
  where
    readEntries reader = readEntriesAcc WB.empty
      where
        readEntriesAcc acc = next reader >>= \case
          Empty -> pure acc
          ReadEntry key entry -> readEntriesAcc $ WB.addEntry resolve key (rawBlobRefSpan <$> toFullEntry entry) acc

-- | Allows reading the k\/ops of a run incrementally, using its own read-only
-- file handle and in-memory cache of the current disk page.
--
-- New pages are loaded when trying to read their first entry.
data WriteBufferReader m h = WriteBufferReader {
      -- | The disk page currently being read. If it is 'Nothing', the reader
      -- is considered closed.
      readerCurrentPage      :: !(MutVar (PrimState m) (Maybe RawPage))
      -- | The index of the entry to be returned by the next call to 'next'.
    , readerCurrentEntryNo   :: !(PrimVar (PrimState m) Word16)
    , readerKOpsHandle       :: !(FS.Handle h)
    , readerWriteBufferBlobs :: !(Ref (WriteBufferBlobs m h))
    , readerHasFS            :: !(HasFS m h)
    , readerHasBlockIO       :: !(HasBlockIO m h)
    }

{-# SPECIALISE
    new ::
         TempRegistry IO
      -> HasFS IO h
      -> HasBlockIO IO h
      -> WriteBufferFsPaths
      -> IO (WriteBufferReader IO h)
  #-}
-- | See 'Database.LSMTree.Internal.RunReader.new'.
new :: forall m h.
     (MonadMVar m, MonadST m, MonadMask m)
  => TempRegistry m
  -> HasFS m h
  -> HasBlockIO m h
  -> WriteBufferFsPaths
  -> m (WriteBufferReader m h)
new reg readerHasFS readerHasBlockIO fsPaths = do
  (readerKOpsHandle :: FS.Handle h) <- FS.hOpen readerHasFS (Paths.writeBufferKOpsPath fsPaths) FS.ReadMode
  -- Double the file readahead window (only applies to this file descriptor)
  FS.hAdviseAll readerHasBlockIO readerKOpsHandle FS.AdviceSequential
  readerWriteBufferBlobs <- allocateTemp reg (WBB.open readerHasFS (Paths.writeBufferBlobPath fsPaths) FS.AllowExisting) releaseRef
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
next WriteBufferReader {..} = do
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
                entry' = fmap (WBB.mkRawBlobRef readerWriteBufferBlobs) entry
            let rawEntry = Entry entry'
            return (ReadEntry key rawEntry)
          IndexEntryOverflow key entry lenSuffix -> do
            -- TODO: we know that we need the next page, could already load?
            modifyPrimVar readerCurrentEntryNo (+1)
            let entry' :: E.Entry SerialisedValue (RawBlobRef m h)
                entry' = fmap (WBB.mkRawBlobRef readerWriteBufferBlobs) entry
            overflowPages <- readOverflowPages readerHasFS readerKOpsHandle lenSuffix
            let rawEntry = mkEntryOverflow entry' page lenSuffix overflowPages
            return (ReadEntry key rawEntry)

close :: WriteBufferReader m h -> m ()
close WriteBufferReader{..} = FS.hClose readerHasFS readerKOpsHandle
