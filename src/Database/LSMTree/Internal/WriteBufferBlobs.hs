{-# OPTIONS_GHC -Wno-partial-fields #-}
{-# OPTIONS_HADDOCK not-home #-}

-- | An on-disk store for blobs for the write buffer.
--
-- For table inserts with blobs, the blob get written out immediately to a
-- file, while the rest of the 'Entry' goes into the 'WriteBuffer'. The
-- 'WriteBufferBlobs' manages the storage of the blobs.
--
-- A single write buffer blob file can be shared between multiple tables. As a
-- consequence, the lifetime of the 'WriteBufferBlobs' must be managed using
-- 'new', and with the 'Ref' API: 'releaseRef' and 'dupRef'. When a table is
-- duplicated, the new table needs its own reference, so use 'dupRef' upon
-- duplication.
--
-- Blobs are copied from the write buffer blob file when the write buffer is
-- flushed to make a run. This is needed since the blob file is shared and so
-- not stable by the time one table wants to flush it.
--
-- Not all tables need a blob file so we defer opening the file until it
-- is needed.
--
module Database.LSMTree.Internal.WriteBufferBlobs (
    WriteBufferBlobs (..),
    new,
    open,
    addBlob,
    mkRawBlobRef,
    mkWeakBlobRef,
    -- * For tests
    FilePointer (..)
  ) where

import           Control.DeepSeq (NFData (..))
import           Control.Monad (void)
import           Control.Monad.Class.MonadThrow
import           Control.Monad.Primitive (PrimMonad, PrimState)
import           Control.RefCount
import           Data.Primitive.PrimVar as P
import           Data.Word (Word64)
import           Database.LSMTree.Internal.BlobFile
import qualified Database.LSMTree.Internal.BlobFile as BlobFile
import           Database.LSMTree.Internal.BlobRef (RawBlobRef (..),
                     WeakBlobRef (..))
import           Database.LSMTree.Internal.Serialise
import qualified System.FS.API as FS
import           System.FS.API (HasFS)

-- | A single 'WriteBufferBlobs' may be shared between multiple tables.
-- As a consequence of being shared, the management of the shared state has to
-- be quite careful.
--
-- In particular there is the blob file itself. We may have to write to this
-- blob file from multiple threads on behalf of independent tables.
-- The offset at which we write is thus shared mutable state. Our strategy for
-- the write offset is to explicitly track it (since we need to know the offset
-- to return correct 'BlobSpan's) and then to not use the file's own file
-- pointer. We do this by always writing at specific file offsets rather than
-- writing at the open file's normal file pointer. We use a 'PrimVar' with
-- atomic operations to manage the file offset.
--
-- A consequence of the blob file being shared between the write buffers of
-- many tables is that the blobs in the file will not all belong to one
-- table. The write buffer blob file is unsuitable to use as-is as the
-- blob file for a run when the write buffer is flushed. The run blob file must
-- be immutable and with a known CRC. Whereas because the write buffer blob
-- file is shared, it can still be appended to via inserts in one table
-- while another is trying to flush the write buffer. So there is no stable CRC
-- for the whole file (as required by the snapshot format). Further more we
-- cannot even incrementally calculate the blob file CRC without additional
-- expensive serialisation. To solve this we follow the design that the open
-- file handle for the blob file is only shared between multiple write buffers,
-- and is /not/ shared with the runs once flushed. This separates the lifetimes
-- of the files. Correspondingly, the reference counter is only for
-- tracking the lifetime of the read\/write mode file handle.
--
-- One concern with sharing blob files and the open blob file handle between
-- multiple write buffers is: can we guarantee that the blob file is eventually
-- closed?
--
-- A problematic example would be, starting from a root handle and then
-- repeatedly: duplicating; inserting (with blobs) into the duplicate; and then
-- closing it. This would use only a fixed number of tables at once, but
-- would keep inserting into the same the write buffer blob file. This could be
-- done indefinitely.
--
-- On the other hand, provided that there's a bound on the number of duplicates
-- that are created from any point, and each table is eventually closed,
-- then each write buffer blob file will eventually be closed.
--
-- The latter seems like the more realistic use case, and so the design here is
-- probably reasonable.
--
-- If not, an entirely different approach would be to manage blobs across all
-- runs (and the write buffer) differently: avoiding copying when blobs are
-- merged and using some kind of GC algorithm to recover space for blobs that
-- are not longer needed. There are LSM algorithms that do this for values
-- (i.e. copying keys only during merge and referring to values managed in a
-- separate disk heap), so the same could be applied to blobs.
--
data WriteBufferBlobs m h =
     WriteBufferBlobs {

       -- | The blob file
       --
       -- INVARIANT: the file may contain garbage bytes, but no blob reference
       -- ('RawBlobRef', 'WeakBlobRef', or 'StrongBlobRef) will reference these
       -- bytes.
      blobFile           :: !(Ref (BlobFile m h))

      -- | The manually tracked file pointer.
      --
      -- INVARIANT: the file pointer points to a file offset at or beyond the
      -- file size.
    , blobFilePointer    :: !(FilePointer m)

      -- The 'WriteBufferBlobs' is a shared reference-counted object type
    , writeBufRefCounter :: !(RefCounter m)
    }

instance NFData h => NFData (WriteBufferBlobs m h) where
  rnf (WriteBufferBlobs a b c) = rnf a `seq` rnf b `seq` rnf c

instance RefCounted m (WriteBufferBlobs m h) where
  getRefCounter = writeBufRefCounter

{-# SPECIALISE new :: HasFS IO h -> FS.FsPath -> IO (Ref (WriteBufferBlobs IO h)) #-}
-- | Create a new 'WriteBufferBlobs' with a new file.
--
-- REF: the resulting reference must be released once it is no longer used.
--
-- ASYNC: this should be called with asynchronous exceptions masked because it
-- allocates/creates resources.
new ::
     (PrimMonad m, MonadMask m)
  => HasFS m h
  -> FS.FsPath
  -> m (Ref (WriteBufferBlobs m h))
new fs blobFileName = open fs blobFileName FS.MustBeNew

{-# SPECIALISE open :: HasFS IO h -> FS.FsPath -> FS.AllowExisting -> IO (Ref (WriteBufferBlobs IO h)) #-}
-- | Open a `WriteBufferBlobs` file and sets the file pointer to the end of the file.
--
-- REF: the resulting reference must be released once it is no longer used.
--
-- ASYNC: this should be called with asynchronous exceptions masked because it
-- allocates/creates resources.
open ::
     (PrimMonad m, MonadMask m)
  => HasFS m h
  -> FS.FsPath
  -> FS.AllowExisting
  -> m (Ref (WriteBufferBlobs m h))
open fs blobFileName blobFileAllowExisting = do
    -- Must use read/write mode because we write blobs when adding, but
    -- we can also be asked to retrieve blobs at any time.
    bracketOnError
      (openBlobFile fs blobFileName (FS.ReadWriteMode blobFileAllowExisting))
      releaseRef
      (fromBlobFile fs)

{-# SPECIALISE fromBlobFile :: HasFS IO h -> Ref (BlobFile IO h) -> IO (Ref (WriteBufferBlobs IO h)) #-}
-- | Make a `WriteBufferBlobs` from a `BlobFile` and set the file pointer to the
-- end of the file.
--
-- REF: the resulting reference must be released once it is no longer used.
--
-- ASYNC: this should be called with asynchronous exceptions masked because it
-- allocates/creates resources.
fromBlobFile ::
     (PrimMonad m, MonadMask m)
  => HasFS m h
  -> Ref (BlobFile m h)
  -> m (Ref (WriteBufferBlobs m h))
fromBlobFile fs blobFile = do
    blobFilePointer <- newFilePointer
    -- Set the blob file pointer to the end of the file
    blobFileSize <- withRef blobFile $ FS.hGetSize fs . blobFileHandle
    void . updateFilePointer blobFilePointer . fromIntegral $ blobFileSize
    newRef (releaseRef blobFile) $ \writeBufRefCounter ->
      WriteBufferBlobs {
        blobFile,
        blobFilePointer,
        writeBufRefCounter
      }

{-# SPECIALISE addBlob :: HasFS IO h -> Ref (WriteBufferBlobs IO h) -> SerialisedBlob -> IO BlobSpan #-}
-- | Append a blob.
--
-- If no exception is returned, then the file pointer will be set to exactly the
-- file size.
--
-- If an exception is returned, the file pointer points to a file
-- offset at or beyond the file size. The bytes between the old and new offset
-- might be garbage or missing.
addBlob :: (PrimMonad m, MonadThrow m)
        => HasFS m h
        -> Ref (WriteBufferBlobs m h)
        -> SerialisedBlob
        -> m BlobSpan
addBlob fs (DeRef WriteBufferBlobs {blobFile, blobFilePointer}) blob = do
    let blobsize = sizeofBlob blob
    -- If an exception happens after updating the file pointer, then no write
    -- takes place. The next 'addBlob' will start writing at the new file
    -- offset, so there are going to be some uninitialised bytes in the file.
    bloboffset <- updateFilePointer blobFilePointer blobsize
    -- If an exception happens while writing the blob, the bytes in the file
    -- might be corrupted.
    BlobFile.writeBlob fs blobFile blob bloboffset
    pure BlobSpan {
      blobSpanOffset = bloboffset,
      blobSpanSize   = fromIntegral blobsize
    }

-- | Helper function to make a 'RawBlobRef' that points into a
-- 'WriteBufferBlobs'.
--
-- This function should only be used on the result of 'addBlob' on the same
-- 'WriteBufferBlobs'. For example:
--
-- @
--  'addBlob' hfs wbb blob >>= \\span -> pure ('mkRawBlobRef' wbb span)
-- @
mkRawBlobRef :: Ref (WriteBufferBlobs m h)
             -> BlobSpan
             -> RawBlobRef m h
mkRawBlobRef (DeRef WriteBufferBlobs {blobFile = DeRef blobfile}) blobspan =
    RawBlobRef {
      rawBlobRefFile = blobfile,
      rawBlobRefSpan = blobspan
    }

-- | Helper function to make a 'WeakBlobRef' that points into a
-- 'WriteBufferBlobs'.
--
-- This function should only be used on the result of 'addBlob' on the same
-- 'WriteBufferBlobs'. For example:
--
-- @
--  'addBlob' hfs wbb blob >>= \\span -> pure ('mkWeakBlobRef' wbb span)
-- @
mkWeakBlobRef :: Ref (WriteBufferBlobs m h)
              -> BlobSpan
              -> WeakBlobRef m h
mkWeakBlobRef (DeRef WriteBufferBlobs {blobFile}) blobspan =
    WeakBlobRef {
      weakBlobRefFile = mkWeakRef blobFile,
      weakBlobRefSpan = blobspan
    }


-- | A mutable file offset, suitable to share between threads.
--
-- This pointer is limited to 31-bit file offsets on 32-bit systems. This should
-- be a sufficiently large limit that we never reach it in practice.
newtype FilePointer m = FilePointer (PrimVar (PrimState m) Int)

instance NFData (FilePointer m) where
  rnf (FilePointer var) = var `seq` ()

{-# SPECIALISE newFilePointer :: IO (FilePointer IO) #-}
newFilePointer :: PrimMonad m => m (FilePointer m)
newFilePointer = FilePointer <$> P.newPrimVar 0

{-# SPECIALISE updateFilePointer :: FilePointer IO -> Int -> IO Word64 #-}
-- | Update the file offset by a given amount and return the new offset. This
-- is safe to use concurrently.
--
updateFilePointer :: PrimMonad m => FilePointer m -> Int -> m Word64
updateFilePointer (FilePointer var) n = fromIntegral <$> P.fetchAddInt var n
