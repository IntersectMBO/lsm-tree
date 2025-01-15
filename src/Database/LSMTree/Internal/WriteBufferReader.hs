-- | A write buffer that is being read incrementally.
--
module Database.LSMTree.Internal.WriteBufferReader (
    readWriteBuffer
  ) where

import           Control.Concurrent.Class.MonadMVar.Strict
import           Control.Monad.Class.MonadST (MonadST (..))
import           Control.Monad.Class.MonadSTM (MonadSTM (..))
import           Control.Monad.Class.MonadThrow (MonadMask, MonadThrow (..),
                     bracketOnError)
import           Control.Monad.Primitive (PrimMonad (..))
import           Control.RefCount (Ref, dupRef, releaseRef)
import           Data.Primitive.MutVar (MutVar, newMutVar, readMutVar,
                     writeMutVar)
import           Data.Primitive.PrimVar
import           Data.Word (Word16)
import           Database.LSMTree.Internal.BlobFile (BlobFile)
import           Database.LSMTree.Internal.BlobRef (RawBlobRef (..),
                     mkRawBlobRef)
import qualified Database.LSMTree.Internal.Entry as E
import           Database.LSMTree.Internal.Lookup (ResolveSerialisedValue)
import           Database.LSMTree.Internal.Paths
import           Database.LSMTree.Internal.RawPage
import           Database.LSMTree.Internal.RunReader (Entry (..), Result (..),
                     mkEntryOverflow, readDiskPage, readOverflowPages,
                     toFullEntry)
import           Database.LSMTree.Internal.Serialise (SerialisedValue)
import           Database.LSMTree.Internal.WriteBuffer (WriteBuffer)
import qualified Database.LSMTree.Internal.WriteBuffer as WB
import qualified System.FS.API as FS
import           System.FS.API (HasFS)
import qualified System.FS.BlockIO.API as FS
import           System.FS.BlockIO.API (HasBlockIO)

{-# SPECIALISE
  readWriteBuffer ::
       ResolveSerialisedValue
    -> HasFS IO h
    -> HasBlockIO IO h
    -> ForKOps FS.FsPath
    -> Ref (BlobFile IO h)
    -> IO WriteBuffer
  #-}
-- | Read a serialised `WriteBuffer` back into memory.
--
-- The argument blob file ('BlobFile') must be the file associated with the
-- argument key\/ops file ('ForKOps'). 'readWriteBuffer' does not check this.
readWriteBuffer ::
     (MonadMVar m, MonadMask m, MonadSTM m, MonadST m)
  => ResolveSerialisedValue
  -> HasFS m h
  -> HasBlockIO m h
  -> ForKOps FS.FsPath
  -> Ref (BlobFile m h)
  -> m WriteBuffer
readWriteBuffer resolve hfs hbio kOpsPath blobFile =
    bracket (new hfs hbio kOpsPath blobFile) close $ readEntries
  where
    readEntries reader = readEntriesAcc WB.empty
      where
        readEntriesAcc acc = next reader >>= \case
          Empty -> pure acc
          ReadEntry key entry -> readEntriesAcc $
            WB.addEntry resolve key (rawBlobRefSpan <$> toFullEntry entry) acc

-- | Allows reading the k\/ops of a serialised write buffer incrementally,
-- using its own read-only file handle and in-memory cache of the current disk page.
--
-- New pages are loaded when trying to read their first entry.
data WriteBufferReader m h = WriteBufferReader {
      -- | The disk page currently being read. If it is 'Nothing', the reader
      -- is considered closed.
      readerCurrentPage    :: !(MutVar (PrimState m) (Maybe RawPage))
      -- | The index of the entry to be returned by the next call to 'next'.
    , readerCurrentEntryNo :: !(PrimVar (PrimState m) Word16)
    , readerKOpsHandle     :: !(FS.Handle h)
    , readerBlobFile       :: !(Ref (BlobFile m h))
    , readerHasFS          :: !(HasFS m h)
    , readerHasBlockIO     :: !(HasBlockIO m h)
    }

{-# SPECIALISE
  new ::
       HasFS IO h
    -> HasBlockIO IO h
    -> ForKOps FS.FsPath
    -> Ref (BlobFile IO h)
    -> IO (WriteBufferReader IO h)
  #-}
-- | See 'Database.LSMTree.Internal.RunReader.new'.
--
-- REF: the resulting 'WriteBufferReader' must be closed once it is no longer
-- used.
--
-- ASYNC: this should be called with asynchronous exceptions masked because it
-- allocates/creates resources.
new :: forall m h.
     (MonadMVar m, MonadST m, MonadMask m)
  => HasFS m h
  -> HasBlockIO m h
  -> ForKOps FS.FsPath
  -> Ref (BlobFile m h)
  -> m (WriteBufferReader m h)
new readerHasFS readerHasBlockIO kOpsPath blobFile =
    bracketOnError openKOps (FS.hClose readerHasFS) $ \readerKOpsHandle -> do
      -- Double the file readahead window (only applies to this file descriptor)
      FS.hAdviseAll readerHasBlockIO readerKOpsHandle FS.AdviceSequential
      bracketOnError (dupRef blobFile) releaseRef $ \readerBlobFile -> do
        -- Load first page from disk, if it exists.
        readerCurrentEntryNo <- newPrimVar (0 :: Word16)
        firstPage <- readDiskPage readerHasFS readerKOpsHandle
        readerCurrentPage <- newMutVar firstPage
        pure $ WriteBufferReader{..}
  where
    openKOps = FS.hOpen readerHasFS (unForKOps kOpsPath) FS.ReadMode

{-# SPECIALISE
  next ::
       WriteBufferReader IO h
    -> IO (Result IO h)
  #-}
-- | See 'Database.LSMTree.Internal.RunReader.next'.
--
-- TODO: 'next' is currently only used in 'readWriteBuffer', where it is a safe
-- use of an unsafe function. If this function is ever exported and used
-- directly, the TODOs in the body of this function should be addressed first.
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
    -- TODO: if 'readerCurrentEntryNo' is incremented but an exception is thrown
    -- before the 'Result' is used by the caller of 'next', then we'll lose that
    -- 'Result'. The following call to 'next' will not return the 'Result' we
    -- missed.
    go :: Word16 -> RawPage -> m (Result m h)
    go !entryNo !page =
        -- take entry from current page (resolve blob if necessary)
        case rawPageIndex page entryNo of
          IndexNotPresent -> do
            -- if it is past the last one, load a new page from disk, try again
            newPage <- readDiskPage readerHasFS readerKOpsHandle
            -- TODO: if the next disk page is read but an (async) exception is
            -- thrown just before updating the MutVar below, then we lose the
            -- disk page because 'readDiskPage' has already updated its file
            -- pointer.
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
                entry' = fmap (mkRawBlobRef readerBlobFile) entry
            let rawEntry = Entry entry'
            return (ReadEntry key rawEntry)
          IndexEntryOverflow key entry lenSuffix -> do
            -- TODO: we know that we need the next page, could already load?
            modifyPrimVar readerCurrentEntryNo (+1)
            let entry' :: E.Entry SerialisedValue (RawBlobRef m h)
                entry' = fmap (mkRawBlobRef readerBlobFile) entry
            overflowPages <- readOverflowPages readerHasFS readerKOpsHandle lenSuffix
            let rawEntry = mkEntryOverflow entry' page lenSuffix overflowPages
            return (ReadEntry key rawEntry)

{-# SPECIALISE close :: WriteBufferReader IO h -> IO () #-}
-- | Close the 'WriteBufferReader'.
--
-- ASYNC: this should be called with asynchronous exceptions masked because it
-- releases/removes resources.
close ::
     (MonadMask m, PrimMonad m)
  => WriteBufferReader m h
  -> m ()
close WriteBufferReader{..} = do
  FS.hClose readerHasFS readerKOpsHandle
    `finally` releaseRef readerBlobFile
