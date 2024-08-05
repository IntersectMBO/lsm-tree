-- | A run that is being read incrementally.
--
module Database.LSMTree.Internal.RunReader (
    RunReader (..)
  , new
  , next
  , close
  , Result (..)
  , Entry (..)
  , toFullEntry
  , appendOverflow
  ) where

import           Control.Exception (assert, handleJust)
import           Control.Monad (guard, when)
import           Control.Monad.Primitive (PrimMonad (..))
import           Data.Bifunctor (first)
import           Data.IORef
import           Data.Maybe (fromMaybe)
import           Data.Primitive.ByteArray (newPinnedByteArray,
                     unsafeFreezeByteArray)
import           Data.Primitive.PrimVar
import           Data.Word (Word16, Word32)
import           Database.LSMTree.Internal.BitMath (ceilDivPageSize,
                     mulPageSize, roundUpToPageSize)
import           Database.LSMTree.Internal.BlobRef (BlobRef (..))
import qualified Database.LSMTree.Internal.Entry as E
import qualified Database.LSMTree.Internal.IndexCompact as Index
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
data RunReader m fhandle = RunReader {
      readerCurrentPage    :: !(IORef RawPage)
      -- | The index of the entry to be returned by the next call to 'next'.
    , readerCurrentEntryNo :: !(PrimVar (PrimState m) Word16)
      -- | Read mode file handle into the run's k\/ops file. We rely on it to
      -- track the position of the next disk page to read, instead of keeping
      -- a counter ourselves. Also, the run's handle is supposed to be opened
      -- with @O_DIRECT@, which is counterproductive here.
    , readerKOpsHandle     :: !fhandle
      -- | The run this reader is reading from.
    , readerRun            :: !(Run.Run m fhandle)
    }

new ::
     HasFS IO h
  -> HasBlockIO IO h
  -> Maybe SerialisedKey  -- ^ offset
  -> Run.Run IO (FS.Handle h)
  -> IO (RunReader IO (FS.Handle h))
new fs hbio mOffset readerRun = do
    readerKOpsHandle <-
      FS.hOpen fs (runKOpsPath (Run.runRunFsPaths readerRun)) FS.ReadMode
    -- double the file readahead window (only applies to this file descriptor)
    FS.hAdviseAll hbio readerKOpsHandle FS.AdviceSequential

    (page, entryNo) <- case mOffset of
      Nothing -> do
        -- load first page from disk, if it exists.
        firstPage <- fromMaybe emptyRawPage <$> readDiskPage fs readerKOpsHandle
        return (firstPage, 0)

      Just offset -> do
        let pageNo = Index.pageSpanStart (Index.search offset index)
        -- load first page from disk, if it exists.
        firstPage <- fromMaybe emptyRawPage <$> readDiskPageAt fs pageNo readerKOpsHandle

        case rawPageFindKey firstPage offset of
          Just n ->
            return (firstPage, n)
          Nothing -> do
            -- actually, it's the next page
            nextPage <- fromMaybe emptyRawPage <$> readDiskPageAt fs pageNo readerKOpsHandle
            return (nextPage, 0)

    readerCurrentEntryNo <- newPrimVar entryNo
    readerCurrentPage <- newIORef page
    return RunReader {..}
  where
    index = Run.runIndex readerRun

-- | This function should be called when discarding a 'RunReader' before it
-- was done (i.e. returned 'Empty'). This avoids leaking file handles.
-- Once it has been called, do not use the reader any more!
close ::
     HasFS IO h
  -> HasBlockIO IO h
  -> RunReader m (FS.Handle h)
  -> IO ()
close fs hbio RunReader {..} = do
    when (Run.runRunDataCaching readerRun == Run.NoCacheRunData) $
      -- drop the file from the OS page cache
      FS.hDropCacheAll hbio readerKOpsHandle
    FS.hClose fs readerKOpsHandle

-- | The 'SerialisedKey' and 'SerialisedValue' point into the in-memory disk
-- page. Keeping them alive will also prevent garbage collection of the 4k byte
-- array, so if they're long-lived, consider making a copy!
data Result m fhandle
  = Empty
  | ReadEntry !SerialisedKey !(Entry m fhandle)

data Entry m fhandle =
    Entry
      !(E.Entry SerialisedValue (BlobRef m fhandle))
  | -- | A large entry. The caller might be interested in various different
    -- (redundant) representation, so we return all of them.
    EntryOverflow
      -- | The value is just a prefix, with the remainder in the overflow pages.
      !(E.Entry SerialisedValue (BlobRef m fhandle))
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
     E.Entry SerialisedValue (BlobRef m fhandle)
  -> RawPage
  -> Word32
  -> [RawOverflowPage]
  -> Entry m fhandle
mkEntryOverflow entryPrefix page len overflowPages =
    assert (len > 0) $
    assert (rawPageOverflowPages page == ceilDivPageSize (fromIntegral len)) $
    assert (rawPageOverflowPages page == length overflowPages) $
      EntryOverflow entryPrefix page len overflowPages

{-# INLINE toFullEntry #-}
toFullEntry :: Entry m fhandle -> E.Entry SerialisedValue (BlobRef m fhandle)
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

-- | Stop using the 'RunReader' after getting 'Empty', because the 'Reader' is
-- automatically closed!
next ::
     HasFS IO h
  -> HasBlockIO IO h
  -> RunReader IO (FS.Handle h)
  -> IO (Result IO (FS.Handle h))
next fs hbio reader@RunReader {..} = do
    entryNo <- readPrimVar readerCurrentEntryNo
    page <- readIORef readerCurrentPage
    go entryNo page
  where
    go !entryNo !page =
        -- take entry from current page (resolve blob if necessary)
        case rawPageIndex page entryNo of
          IndexNotPresent -> do
            -- if it is past the last one, load a new page from disk, try again
            readDiskPage fs readerKOpsHandle >>= \case
              Nothing -> do
                close fs hbio reader
                return Empty
              Just newPage -> do
                writeIORef readerCurrentPage newPage
                writePrimVar readerCurrentEntryNo 0
                go 0 newPage  -- try again on the new page
          IndexEntry key entry -> do
            modifyPrimVar readerCurrentEntryNo (+1)
            let entry' = fmap (Run.mkBlobRefForRun readerRun) entry
            let rawEntry = Entry entry'
            return (ReadEntry key rawEntry)
          IndexEntryOverflow key entry lenSuffix -> do
            -- TODO: we know that we need the next page, could already load?
            modifyPrimVar readerCurrentEntryNo (+1)
            let entry' = fmap (Run.mkBlobRefForRun readerRun) entry
            overflowPages <- readOverflowPages fs readerKOpsHandle lenSuffix
            let rawEntry = mkEntryOverflow entry' page lenSuffix overflowPages
            return (ReadEntry key rawEntry)

{-------------------------------------------------------------------------------
  Utilities
-------------------------------------------------------------------------------}

-- | Returns 'Nothing' on EOF.
readDiskPageAt :: HasFS IO h -> Index.PageNo -> FS.Handle h -> IO (Maybe RawPage)
readDiskPageAt fs pageNo h = do
    mba <- newPinnedByteArray pageSize
    -- TODO: make sure no other exception type can be thrown
    handleJust (guard . FS.isFsErrorType FS.FsReachedEOF) (\_ -> pure Nothing) $ do
      let byteOffset = pageNoToAbsOffset pageNo
      _ <- FS.hGetBufExactlyAt fs h mba 0 (fromIntegral pageSize) byteOffset
      ba <- unsafeFreezeByteArray mba
      let !rawPage = unsafeMakeRawPage ba 0
      return (Just rawPage)

pageNoToAbsOffset :: Index.PageNo -> FS.AbsOffset
pageNoToAbsOffset (Index.PageNo n) =
    assert (n >= 0) $
      FS.AbsOffset (mulPageSize (fromIntegral n))

-- | Returns 'Nothing' on EOF.
readDiskPage :: HasFS IO h -> FS.Handle h -> IO (Maybe RawPage)
readDiskPage fs h = do
    mba <- newPinnedByteArray pageSize
    -- TODO: make sure no other exception type can be thrown
    handleJust (guard . FS.isFsErrorType FS.FsReachedEOF) (\_ -> pure Nothing) $ do
      _ <- FS.hGetBufExactly fs h mba 0 (fromIntegral pageSize)
      ba <- unsafeFreezeByteArray mba
      let !rawPage = unsafeMakeRawPage ba 0
      return (Just rawPage)

pageSize :: Int
pageSize = 4096

-- | Throws exception on EOF. If a suffix was expected, the file should have it.
-- Reads full pages, despite the suffix only using part of the last page.
readOverflowPages :: HasFS IO h -> FS.Handle h -> Word32 -> IO [RawOverflowPage]
readOverflowPages fs h len = do
    let lenPages = fromIntegral (roundUpToPageSize len)  -- always read whole pages
    mba <- newPinnedByteArray lenPages
    _ <- FS.hGetBufExactly fs h mba 0 (fromIntegral lenPages)
    ba <- unsafeFreezeByteArray mba
    -- should not copy since 'ba' is pinned and its length is a multiple of 4k.
    return $ pinnedByteArrayToOverflowPages 0 lenPages ba
