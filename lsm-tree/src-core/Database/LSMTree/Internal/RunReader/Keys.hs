{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE NoFieldSelectors      #-}

{-# OPTIONS_HADDOCK not-home #-}

module Database.LSMTree.Internal.RunReader.Keys (
    Keys (..)
  , unsafeNew
  , RunInfo (..)
  , close
  , next
  , Result (..)
  ) where

import           Control.Exception (assert)
import           Control.Monad (when)
import           Control.Monad.Class.MonadST (MonadST (..))
import           Control.Monad.Class.MonadSTM (MonadSTM (..))
import           Control.Monad.Class.MonadThrow (MonadCatch (..),
                     MonadMask (..))
import           Control.Monad.Primitive (PrimMonad (..))
import           Data.Maybe (isNothing)
import           Data.Primitive.MutVar (MutVar, newMutVar, readMutVar,
                     writeMutVar)
import           Data.Primitive.PrimVar (PrimVar, modifyPrimVar, newPrimVar,
                     readPrimVar, writePrimVar)
import           Data.Word (Word16)
import           Database.LSMTree.Internal.Page (NumPages, getNumPages,
                     pageSize, readDiskPage, skipPages)
import           Database.LSMTree.Internal.RawPage (RawPage,
                     RawPageIndex (IndexEntry, IndexEntryOverflow, IndexNotPresent),
                     rawPageIndex)
import           Database.LSMTree.Internal.RunBuilder (RunDataCaching (..))
import           Database.LSMTree.Internal.Serialise (SerialisedKey)
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

data RunInfo m h = RunInfo {
    hasFS       :: HasFS m h
  , hasBlockIO  :: HasBlockIO m h
  , kOpsPath    :: FsPath
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
     RunInfo IO h
  -> IO (Keys IO h) #-}
unsafeNew :: forall m h.
     (MonadMask m, MonadSTM m, PrimMonad m)
  => RunInfo m h
  -> m (Keys m h)
unsafeNew runInfo = do
    bracketOnError (openKOpsFile runInfo) (FS.hClose runInfo.hasFS) $ \kOpsHandle -> do

      FS.hAdviseAll runInfo.hasBlockIO kOpsHandle FS.AdviceSequential

      -- Load first page from disk, if it exists.
      page <- readDiskPage runInfo.hasFS kOpsHandle

      currentEntryNo <- newPrimVar 0
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
            skipPages reader.hasFS reader.kOpsHandle lenSuffix
            pure $ Key key
