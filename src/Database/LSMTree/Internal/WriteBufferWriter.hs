module Database.LSMTree.Internal.WriteBufferWriter
  (
    WriteBufferWriter (..),
    new,
    addKeyOp
  ) where

import           Control.Monad.Class.MonadST (MonadST (..))
import qualified Control.Monad.Class.MonadST as ST
import           Control.Monad.Class.MonadSTM (MonadSTM (..))
import           Control.Monad.Class.MonadThrow (MonadThrow)
import           Control.Monad.Primitive (PrimMonad (..))
import qualified Data.ByteString.Lazy as BSL
import           Data.Primitive.PrimVar (PrimVar, modifyPrimVar, newPrimVar,
                     readPrimVar)
import           Data.Traversable (for)
import           Data.Word (Word64)
import qualified Database.LSMTree.Internal.BlobFile as BlobFile
import           Database.LSMTree.Internal.BlobRef (RawBlobRef)
import qualified Database.LSMTree.Internal.BlobRef as BlobRef
import           Database.LSMTree.Internal.CRC32C (ChecksumHandle)
import qualified Database.LSMTree.Internal.CRC32C as CRC
import           Database.LSMTree.Internal.Entry (Entry)
import           Database.LSMTree.Internal.PageAcc (PageAcc)
import qualified Database.LSMTree.Internal.PageAcc as PageAcc
import           Database.LSMTree.Internal.Paths (ForWriteBufferFiles,
                     RunFsPaths, WriteBufferFsPaths, pathsForWriteBufferFiles)
import qualified Database.LSMTree.Internal.RawBytes as RB
import           Database.LSMTree.Internal.Serialise (SerialisedBlob (..),
                     SerialisedKey, SerialisedValue, sizeofBlob64)
import qualified System.FS.API as FS
import           System.FS.API (FsPath, HasFS)
import qualified System.FS.BlockIO.API as FS
import           System.FS.BlockIO.API (HasBlockIO)

data WriteBufferWriter m h = WriteBufferWriter
  { -- | The target file.
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
    traverse (CRC.makeHandle hfs) (pathsForWriteBufferFiles fsPaths)
  return WriteBufferWriter
    { writeBufferWriterFsPaths    = fsPaths,
      writeBufferWriterHasFS      = hfs,
      writeBufferWriterHasBlockIO = hbio,
      ..
    }

addKeyOp ::
     (MonadST m, MonadSTM m, MonadThrow m)
  => WriteBufferWriter m h
  -> SerialisedKey
  -> Entry SerialisedValue (RawBlobRef m h)
  -> m ()
addKeyOp WriteBufferWriter{..} key op = do
  op' <- for op $ \blobref -> do
      blob <- BlobRef.readRawBlobRef writeBufferWriterHasFS blobref
      let size = sizeofBlob64 blob
      offset <- readPrimVar writeBufferWriterBlobOffset
      modifyPrimVar writeBufferWriterBlobOffset (+size)
      let SerialisedBlob rb = blob
      let lbs = BSL.fromStrict $ RB.toByteString rb
      -- writeToHandle writeBufferWriterHasFS (forRunBlob runBuilderHandles) lbs
      _
  if PageAcc.entryWouldFitInPage key op
    then do
      _
    else do
      _
