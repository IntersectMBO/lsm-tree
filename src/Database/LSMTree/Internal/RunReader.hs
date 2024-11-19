-- | A run that is being read incrementally.
--
module Database.LSMTree.Internal.RunReader (
    RunReader (..)
  , OffsetKey (..)
  , new
  , next
  , close
  , Result (..)
  , Entry (..)
  , toFullEntry
  , appendOverflow
  ) where

import           Control.Exception (assert)
import           Control.Monad (guard, when)
import           Control.Monad.Class.MonadST (MonadST (..))
import           Control.Monad.Class.MonadSTM (MonadSTM (..))
import           Control.Monad.Class.MonadThrow (MonadCatch (..),
                     MonadMask (..), MonadThrow (..))
import           Control.Monad.Primitive (PrimMonad (..))
import           Control.RefCount
import           Data.Bifunctor (first)
import           Data.Maybe (isNothing)
import           Data.Primitive.ByteArray (newPinnedByteArray,
                     unsafeFreezeByteArray)
import           Data.Primitive.MutVar (MutVar, newMutVar, readMutVar,
                     writeMutVar)
import           Data.Primitive.PrimVar
import           Data.Word (Word16, Word32)
import           Database.LSMTree.Internal.BitMath (ceilDivPageSize,
                     mulPageSize, roundUpToPageSize)
import           Database.LSMTree.Internal.BlobFile as BlobFile
import           Database.LSMTree.Internal.BlobRef as BlobRef
import qualified Database.LSMTree.Internal.Entry as E
import qualified Database.LSMTree.Internal.Index as Index (search)
import           Database.LSMTree.Internal.Page (PageNo (..), PageSpan (..),
                     getNumPages, nextPageNo)
import           Database.LSMTree.Internal.Paths
import qualified Database.LSMTree.Internal.RawBytes as RB
import           Database.LSMTree.Internal.RawOverflowPage (RawOverflowPage,
                     pinnedByteArrayToOverflowPages, rawOverflowPageRawBytes)
import           Database.LSMTree.Internal.RawPage
import qualified Database.LSMTree.Internal.Run as Run
import           Database.LSMTree.Internal.Serialise
import qualified System.FS.API as FS
import           System.FS.API (HasFS)
import qualified System.FS.BlockIO.API as FS
import           System.FS.BlockIO.API (HasBlockIO)

-- | Allows reading the k\/ops of a run incrementally, using its own read-only
-- file handle and in-memory cache of the current disk page.
--
-- Creating a 'RunReader' does not increase the run's reference count, so make
-- sure the run remains open while using the reader.
--
-- New pages are loaded when trying to read their first entry.
--
-- TODO(optimise): Reuse page buffers using some kind of allocator. However,
-- deciding how long a page needs to stay around is not trivial.
data RunReader m h = RunReader {
      -- | The disk page currently being read. If it is 'Nothing', the reader
      -- is considered closed.
      readerCurrentPage    :: !(MutVar (PrimState m) (Maybe RawPage))
      -- | The index of the entry to be returned by the next call to 'next'.
    , readerCurrentEntryNo :: !(PrimVar (PrimState m) Word16)
      -- | Read mode file handle into the run's k\/ops file. We rely on it to
      -- track the position of the next disk page to read, instead of keeping
      -- a counter ourselves. Also, the run's handle is supposed to be opened
      -- with @O_DIRECT@, which is counterproductive here.
    , readerKOpsHandle     :: !(FS.Handle h)
      -- | The blob file from the run this reader is reading from.
    , readerBlobFile       :: !(BlobFile m h)
    , readerRunDataCaching :: !Run.RunDataCaching
    , readerHasFS          :: !(HasFS m h)
    , readerHasBlockIO     :: !(HasBlockIO m h)
    }

data OffsetKey = NoOffsetKey | OffsetKey !SerialisedKey

{-# SPECIALISE new ::
     OffsetKey
  -> Run.Run IO h
  -> IO (RunReader IO h) #-}
new :: forall m h.
     (MonadMask m, MonadSTM m, PrimMonad m)
  => OffsetKey
  -> Run.Run m h
  -> m  (RunReader m h)
new !offsetKey readerRun@(Run.Run {
                 runBlobFile,
                 runRunDataCaching = readerRunDataCaching,
                 runHasFS          = readerHasFS,
                 runHasBlockIO     = readerHasBlockIO,
                 runIndex          = index
               }) = do
    (readerKOpsHandle :: FS.Handle h) <-
      FS.hOpen readerHasFS (runKOpsPath (Run.runRunFsPaths readerRun)) FS.ReadMode >>= \h -> do
        fileSize <- FS.hGetSize readerHasFS h
        let fileSizeInPages = fileSize `div` toEnum pageSize
        let indexedPages = getNumPages $ Run.sizeInPages readerRun
        assert (indexedPages == fileSizeInPages) $ pure h
    -- Double the file readahead window (only applies to this file descriptor)
    FS.hAdviseAll readerHasBlockIO readerKOpsHandle FS.AdviceSequential

    (page, entryNo) <- seekFirstEntry readerKOpsHandle

    --TODO: instead use: readerBlobFile <- dupRef runBlobFile
    let readerBlobFile = runBlobFile
    addReference (blobFileRefCounter readerBlobFile)

    readerCurrentEntryNo <- newPrimVar entryNo
    readerCurrentPage <- newMutVar page
    let reader = RunReader {..}

    when (isNothing page) $
      close reader
    return reader
  where
    seekFirstEntry readerKOpsHandle =
        case offsetKey of
          NoOffsetKey -> do
            -- Load first page from disk, if it exists.
            firstPage <- readDiskPage readerHasFS readerKOpsHandle
            return (firstPage, 0)
          OffsetKey offset -> do
            -- Use the index to find the page number for the key (if it exists).
            let PageSpan pageNo pageEnd = Index.search offset index
            seekToDiskPage readerHasFS pageNo readerKOpsHandle
            readDiskPage readerHasFS readerKOpsHandle >>= \case
              Nothing ->
                return (Nothing, 0)
              Just foundPage -> do
                case rawPageFindKey foundPage offset of
                  Just n ->
                    -- Found an appropriate index within the index's page.
                    return (Just foundPage, n)

                  _ -> do
                    -- The index said that the key, if it were to exist, would
                    -- live on pageNo, but then rawPageFindKey tells us that in
                    -- fact there is no key greater than or equal to the given
                    -- offset on this page.
                    -- This tells us that the key does not exist, but that if it
                    -- were to exist, it would be between the last key in this
                    -- page and the first key in the next page.
                    -- Thus the reader should be initialised to return keys
                    -- starting from the next (non-overflow) page.
                    seekToDiskPage readerHasFS (nextPageNo pageEnd) readerKOpsHandle
                    nextPage <- readDiskPage readerHasFS readerKOpsHandle
                    return (nextPage, 0)

{-# SPECIALISE close ::
     RunReader IO h
  -> IO () #-}
-- | This function should be called when discarding a 'RunReader' before it
-- was done (i.e. returned 'Empty'). This avoids leaking file handles.
-- Once it has been called, do not use the reader any more!
close ::
     (MonadSTM m, MonadMask m, PrimMonad m)
  => RunReader m h
  -> m ()
close RunReader{..} = do
    when (readerRunDataCaching == Run.NoCacheRunData) $
      -- drop the file from the OS page cache
      FS.hDropCacheAll readerHasBlockIO readerKOpsHandle
    FS.hClose readerHasFS readerKOpsHandle
    BlobFile.removeReference readerBlobFile

-- | The 'SerialisedKey' and 'SerialisedValue' point into the in-memory disk
-- page. Keeping them alive will also prevent garbage collection of the 4k byte
-- array, so if they're long-lived, consider making a copy!
data Result m h
  = Empty
  | ReadEntry !SerialisedKey !(Entry m h)

data Entry m h =
    Entry
      !(E.Entry SerialisedValue (RawBlobRef m h))
  | -- | A large entry. The caller might be interested in various different
    -- (redundant) representation, so we return all of them.
    EntryOverflow
      -- | The value is just a prefix, with the remainder in the overflow pages.
      !(E.Entry SerialisedValue (RawBlobRef m h))
      -- | A page containing the single entry (or rather its prefix).
      !RawPage
      -- | Non-zero length of the overflow in bytes.
      !Word32
      -- | The overflow pages containing the suffix of the value (so at least
      -- the number of bytes specified above).
      --
      -- TODO(optimise): Sometimes, reading the overflow pages is not necessary.
      -- We could just return the page index and offer a separate function to do
      -- the disk I/O once needed.
      ![RawOverflowPage]

mkEntryOverflow ::
     E.Entry SerialisedValue (RawBlobRef m h)
  -> RawPage
  -> Word32
  -> [RawOverflowPage]
  -> Entry m h
mkEntryOverflow entryPrefix page len overflowPages =
    assert (len > 0) $
    assert (rawPageOverflowPages page == ceilDivPageSize (fromIntegral len)) $
    assert (rawPageOverflowPages page == length overflowPages) $
      EntryOverflow entryPrefix page len overflowPages

{-# INLINE toFullEntry #-}
toFullEntry :: Entry m h -> E.Entry SerialisedValue (RawBlobRef m h)
toFullEntry = \case
    Entry e ->
      e
    EntryOverflow prefix _ len overflowPages ->
      first (appendOverflow len overflowPages) prefix

{-# INLINE appendOverflow #-}
appendOverflow :: Word32 -> [RawOverflowPage] -> SerialisedValue -> SerialisedValue
appendOverflow len overflowPages (SerialisedValue prefix) =
    SerialisedValue $
      RB.take (RB.size prefix + fromIntegral len) $
        mconcat (prefix : map rawOverflowPageRawBytes overflowPages)

{-# SPECIALISE next ::
     RunReader IO h
  -> IO (Result IO h) #-}
-- | Stop using the 'RunReader' after getting 'Empty', because the 'Reader' is
-- automatically closed!
next :: forall m h.
     (MonadMask m, MonadSTM m, MonadST m)
  => RunReader m h
  -> m (Result m h)
next reader@RunReader {..} = do
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
                close reader
                return Empty
              Just p -> do
                writePrimVar readerCurrentEntryNo 0
                go 0 p  -- try again on the new page
          IndexEntry key entry -> do
            modifyPrimVar readerCurrentEntryNo (+1)
            let entry' = fmap (BlobRef.mkRawBlobRef readerBlobFile) entry
            let rawEntry = Entry entry'
            return (ReadEntry key rawEntry)
          IndexEntryOverflow key entry lenSuffix -> do
            -- TODO: we know that we need the next page, could already load?
            modifyPrimVar readerCurrentEntryNo (+1)
            let entry' :: E.Entry SerialisedValue (RawBlobRef m h)
                entry' = fmap (BlobRef.mkRawBlobRef readerBlobFile) entry
            overflowPages <- readOverflowPages readerHasFS readerKOpsHandle lenSuffix
            let rawEntry = mkEntryOverflow entry' page lenSuffix overflowPages
            return (ReadEntry key rawEntry)

{-------------------------------------------------------------------------------
  Utilities
-------------------------------------------------------------------------------}

seekToDiskPage :: HasFS m h -> PageNo -> FS.Handle h -> m ()
seekToDiskPage fs pageNo h = do
    FS.hSeek fs h FS.AbsoluteSeek (pageNoToByteOffset pageNo)
  where
    pageNoToByteOffset (PageNo n) =
        assert (n >= 0) $
          mulPageSize (fromIntegral n)

{-# SPECIALISE readDiskPage ::
     HasFS IO h
  -> FS.Handle h
  -> IO (Maybe RawPage) #-}
-- | Returns 'Nothing' on EOF.
readDiskPage ::
     (MonadCatch m, PrimMonad m)
  => HasFS m h
  -> FS.Handle h
  -> m (Maybe RawPage)
readDiskPage fs h = do
    mba <- newPinnedByteArray pageSize
    -- TODO: make sure no other exception type can be thrown
    handleJust (guard . FS.isFsErrorType FS.FsReachedEOF) (\_ -> pure Nothing) $ do
      bytesRead <- FS.hGetBufExactly fs h mba 0 (fromIntegral pageSize)
      assert (fromIntegral bytesRead == pageSize) $ pure ()
      ba <- unsafeFreezeByteArray mba
      let !rawPage = unsafeMakeRawPage ba 0
      return (Just rawPage)

pageSize :: Int
pageSize = 4096

{-# SPECIALISE readOverflowPages ::
     HasFS IO h
  -> FS.Handle h
  -> Word32
  -> IO [RawOverflowPage] #-}
-- | Throws exception on EOF. If a suffix was expected, the file should have it.
-- Reads full pages, despite the suffix only using part of the last page.
readOverflowPages ::
     (MonadSTM m, MonadThrow m, PrimMonad m)
   => HasFS m h
   -> FS.Handle h
   -> Word32
   -> m [RawOverflowPage]
readOverflowPages fs h len = do
    let lenPages = fromIntegral (roundUpToPageSize len)  -- always read whole pages
    mba <- newPinnedByteArray lenPages
    _ <- FS.hGetBufExactly fs h mba 0 (fromIntegral lenPages)
    ba <- unsafeFreezeByteArray mba
    -- should not copy since 'ba' is pinned and its length is a multiple of 4k.
    return $ pinnedByteArrayToOverflowPages 0 lenPages ba
