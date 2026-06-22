{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE NoFieldSelectors      #-}

{-# OPTIONS_HADDOCK not-home #-}

module Database.LSMTree.Internal.RunReader.Keys (
    Keys (..)
  , unsafeNew
  , OffsetKey (..)
  , RunInfo (..)
  , close
  , next
  , Result (..)
  ) where

import           Control.Exception (assert)
import           Control.Monad (guard, when)
import           Control.Monad.Class.MonadST (MonadST (..))
import           Control.Monad.Class.MonadSTM (MonadSTM (..))
import           Control.Monad.Class.MonadThrow (MonadCatch (..),
                     MonadMask (..))
import           Control.Monad.Primitive (PrimMonad (..))
import           Data.Maybe (isNothing)
import           Data.Primitive.ByteArray (newPinnedByteArray,
                     unsafeFreezeByteArray)
import           Data.Primitive.MutVar (MutVar, newMutVar, readMutVar,
                     writeMutVar)
import           Data.Primitive.PrimVar
import           Data.Word (Word16, Word32)
import           Database.LSMTree.Internal.BitMath (mulPageSize,
                     roundUpToPageSize)
import           Database.LSMTree.Internal.Index (Index)
import qualified Database.LSMTree.Internal.Index as Index (search)
import           Database.LSMTree.Internal.Page (NumPages, PageNo (..),
                     PageSpan (..), getNumPages, nextPageNo)
import           Database.LSMTree.Internal.RawPage
import           Database.LSMTree.Internal.RunBuilder (RunDataCaching (..))
import           Database.LSMTree.Internal.Serialise
import qualified System.FS.API as FS
import           System.FS.API (FsPath, Handle, HasFS)
import qualified System.FS.BlockIO.API as FS
import           System.FS.BlockIO.API (HasBlockIO)

data Keys m h = Keys {
      currentPage    :: !(MutVar (PrimState m) (Maybe RawPage))
    , currentEntryNo :: !(PrimVar (PrimState m) Word16)
    , kOpsHandle     :: !(Handle h)
    , dataCaching    :: !RunDataCaching
    , hasFS          :: !(HasFS m h)
    , hasBlockIO     :: !(HasBlockIO m h)
    }

data OffsetKey = NoOffsetKey | OffsetKey !SerialisedKey
  deriving stock Show

data RunInfo m h = RunInfo {
    hasFS       :: HasFS m h
  , hasBlockIO  :: HasBlockIO m h
  , kOpsPath    :: FsPath
  , index       :: Index
  , dataCaching :: RunDataCaching
  , numPages    :: NumPages
  }

{-# SPECIALISE openKOpsFile ::
     RunInfo IO h
  -> IO (Handle h) #-}
openKOpsFile ::
     MonadCatch m
  => RunInfo m h
  -> m (Handle h)
openKOpsFile runInfo = bracketOnError acq rel $ \h -> do
    fileSize <- FS.hGetSize runInfo.hasFS h
    let fileSizeInPages = fileSize `div` toEnum pageSize
    let indexedPages = getNumPages runInfo.numPages
    assert (indexedPages == fileSizeInPages) $ pure ()
    pure h
  where
    acq = FS.hOpen runInfo.hasFS runInfo.kOpsPath FS.ReadMode
    rel = FS.hClose runInfo.hasFS

{-# SPECIALISE unsafeNew ::
     OffsetKey
  -> RunInfo IO h
  -> IO (Keys IO h) #-}
unsafeNew :: forall m h.
     (MonadMask m, MonadSTM m, PrimMonad m)
  => OffsetKey
  -> RunInfo m h
  -> m (Keys m h)
unsafeNew offsetKey runInfo = do
    bracketOnError (openKOpsFile runInfo) (FS.hClose runInfo.hasFS) $ \kOpsHandle -> do

      FS.hAdviseAll runInfo.hasBlockIO kOpsHandle FS.AdviceSequential

      (page, entryNo) <- seekFirstEntry kOpsHandle

      currentEntryNo <- newPrimVar entryNo
      currentPage <- newMutVar page
      let reader = Keys {
              currentPage = currentPage
            , currentEntryNo = currentEntryNo
            , kOpsHandle = kOpsHandle
            , dataCaching = runInfo.dataCaching
            , hasFS = runInfo.hasFS
            , hasBlockIO = runInfo.hasBlockIO
            }

      when (isNothing page) $
        close reader
      pure reader
  where
    seekFirstEntry readerKOpsHandle =
        case offsetKey of
          NoOffsetKey -> do
            -- Load first page from disk, if it exists.
            firstPage <- readDiskPage runInfo.hasFS readerKOpsHandle
            pure (firstPage, 0)
          OffsetKey offset -> do
            -- Use the index to find the page number for the key (if it exists).
            let PageSpan pageNo pageEnd = Index.search offset runInfo.index
            seekToDiskPage runInfo.hasFS pageNo readerKOpsHandle
            readDiskPage runInfo.hasFS readerKOpsHandle >>= \case
              Nothing ->
                pure (Nothing, 0)
              Just foundPage -> do
                case rawPageFindKey foundPage offset of
                  Just n ->
                    -- Found an appropriate index within the index's page.
                    pure (Just foundPage, n)

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
                    seekToDiskPage runInfo.hasFS (nextPageNo pageEnd) readerKOpsHandle
                    nextPage <- readDiskPage runInfo.hasFS readerKOpsHandle
                    pure (nextPage, 0)

{-# SPECIALISE close ::
     Keys IO h
  -> IO () #-}
close ::
     (MonadSTM m)
  => Keys m h
  -> m ()
close r = do
    when (r.dataCaching == NoCacheRunData) $
      FS.hDropCacheAll r.hasBlockIO r.kOpsHandle
    FS.hClose r.hasFS r.kOpsHandle

data Result m h
  = Empty
  | Key !SerialisedKey

{-# SPECIALISE next ::
     Keys IO h
  -> IO (Result IO h) #-}
-- | Stop using the 'RunReader' after getting 'Empty', because the 'Reader' is
-- automatically closed!
next :: forall m h.
     (MonadMask m, MonadSTM m, MonadST m)
  => Keys m h
  -> m (Result m h)
next reader = do
    readMutVar reader.currentPage >>= \case
      Nothing ->
        pure Empty
      Just page -> do
        entryNo <- readPrimVar reader.currentEntryNo
        go entryNo page
  where
    go :: Word16 -> RawPage -> m (Result m h)
    go !entryNo !page =
        -- take entry from current page (resolve blob if necessary)
        case rawPageIndex page entryNo of
          IndexNotPresent -> do
            -- if it is past the last one, load a new page from disk, try again
            newPage <- readDiskPage reader.hasFS reader.kOpsHandle
            stToIO $ writeMutVar reader.currentPage newPage
            case newPage of
              Nothing -> do
                close reader
                pure Empty
              Just p -> do
                writePrimVar reader.currentEntryNo 0
                go 0 p  -- try again on the new page
          IndexEntry key _entry -> do
            modifyPrimVar reader.currentEntryNo (+1)
            pure $ Key key
          IndexEntryOverflow key _entry lenSuffix -> do
            modifyPrimVar reader.currentEntryNo (+1)
            skipOverflowPages reader.hasFS reader.kOpsHandle lenSuffix
            pure $ Key key

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
    --
    -- TODO: if FS.FsReachEOF is thrown as an injected disk fault, then we
    -- incorrectly deduce that the file has no more contents. We should probably
    -- use an explicit file pointer instead in the style of 'FilePointer'.
    handleJust (guard . FS.isFsErrorType FS.FsReachedEOF) (\_ -> pure Nothing) $ do
      bytesRead <- FS.hGetBufExactly fs h mba 0 (fromIntegral pageSize)
      assert (fromIntegral bytesRead == pageSize) $ pure ()
      ba <- unsafeFreezeByteArray mba
      let !rawPage = unsafeMakeRawPage ba 0
      pure (Just rawPage)

pageSize :: Int
pageSize = 4096

skipOverflowPages ::
      HasFS m h
   -> FS.Handle h
   -> Word32
   -> m ()
skipOverflowPages fs h len = do
    let lenPages = fromIntegral (roundUpToPageSize len)  -- always read whole pages
    FS.hSeek fs h FS.RelativeSeek lenPages
