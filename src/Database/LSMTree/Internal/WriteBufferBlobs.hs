{-# OPTIONS_GHC -Wno-partial-fields #-}
{- HLINT ignore "Use record patterns" -}

-- | An on-disk store for blobs for the write buffer.
--
-- For table inserts with blobs, the blob get written out immediately to a
-- file, while the rest of the 'Entry' goes into the 'WriteBuffer'. The
-- 'WriteBufferBlobs' manages the storage of the blobs.
--
-- A single write buffer blob file can be shared between multiple table handles.
-- As a consequence, the lifetime of the 'WriteBufferBlobs' must be managed
-- using 'new','addReference' and 'removeReference'. A fresh 'WriteBufferBlobs'
-- has a reference count of 1, and so must be matched by 'removeReference'.
-- When a table handle is duplicated, the new handle needs its own reference,
-- so use 'addReference' upon duplication.
--
-- Blobs are copied from the write buffer blob file when the write buffer is
-- flushed to make a run. This is needed since the blob file is shared and so
-- not stable by the time one table wants to flush it.
--
-- Not all table handles need a blob file so we defer opening the file until it
-- is needed.
--
module Database.LSMTree.Internal.WriteBufferBlobs (
    WriteBufferBlobs (..),
    new,
    addReference,
    removeReference,
    addBlob,
    readBlob,
    mkBlobRef,
    -- * For tests
    BlobFileState (..),
    FilePointer (..)
  ) where

import           Control.DeepSeq (NFData (..))
import           Control.Monad.Class.MonadThrow
import           Control.Monad.Primitive (PrimMonad, PrimState)
import qualified Control.RefCount as RC
import           Data.Primitive.ByteArray as P
import           Data.Primitive.MutVar as P
import           Data.Primitive.PrimVar as P
import qualified Data.Vector.Primitive as VP
import           Data.Word (Word64)
import           Database.LSMTree.Internal.BlobRef (BlobRef (..), BlobSpan (..))
import           Database.LSMTree.Internal.RawBytes as RB
import           Database.LSMTree.Internal.Serialise
import qualified System.FS.API as FS
import           System.FS.API (FsPath, HasFS)
import qualified System.Posix.Types as FS (ByteCount)

-- | A single 'WriteBufferBlobs' may be shared between multiple table handles.
-- As a consequence of being shared, the management of the shared state has to
-- be quite careful.
--
-- In particular there is the blob file itself. We may have to write to this
-- blob file from multiple threads on behalf of independent table handles.
-- The offset at which we write is thus shared mutable state. Our strategy for
-- the write offset is to explicitly track it (since we need to know the offset
-- to return correct 'BlobSpan's) and then to not use the file's own file
-- pointer. We do this by always writing at specific file offsets rather than
-- writing at the open file's normal file pointer. We use a 'PrimVar' with
-- atomic operations to manage the file offset.
--
-- A consequence of the blob file being shared between the write buffers of
-- many table handles is that the blobs in the file will not all belong to one
-- table handle. The write buffer blob file is unsuitable to use as-is as the
-- blob file for a run when the write buffer is flushed. The run blob file must
-- be immutable and with a known CRC. Whereas because the write buffer blob
-- file is shared, it can still be appended to via inserts in one table handle
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
-- closing it. This would use only a fixed number of table handles at once, but
-- would keep inserting into the same the write buffer blob file. This could be
-- done indefinitely.
--
-- On the other hand, provided that there's a bound on the number of duplicates
-- that are created from any point, and each table handle is eventually closed,
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
       blobFileState      :: !(MutVar (PrimState m) (BlobFileState m h))

       -- | The name is reserved in advance, even if it is not used.
     , blobFileName       :: !FsPath

       -- | The reference counter for the blob file.
     , blobFileRefCounter :: {-# UNPACK #-} !(RC.RefCounter m)
     }

-- | The blob file itself is opened on-demand, to avoid the cost in cases where
-- blobs are not needed.
data BlobFileState m h =

     NoBlobFileYet
   | OpenBlobFile {
       blobFileHandle  :: {-# UNPACK #-} !(FS.Handle h)

       -- | The manually tracked file pointer.
     , blobFilePointer :: !(FilePointer m)
     }

instance NFData (WriteBufferBlobs m h) where
  rnf (WriteBufferBlobs _ b c) = rnf b `seq` rnf c

new :: PrimMonad m
    => HasFS m h
    -> FS.FsPath
    -> m (WriteBufferBlobs m h)
new fs blobFileName = do
    blobFileState <- P.newMutVar NoBlobFileYet
    blobFileRefCounter <- RC.mkRefCounter1 (Just (finaliser fs blobFileState))
    return WriteBufferBlobs {
      blobFileState,
      blobFileName,
      blobFileRefCounter
    }

finaliser :: PrimMonad m
          => HasFS m h
          -> MutVar (PrimState m) (BlobFileState m h)
          -> m ()
finaliser fs bf = do
    bfs <- P.readMutVar bf
    case bfs of
      OpenBlobFile {blobFileHandle} -> FS.hClose fs blobFileHandle
      NoBlobFileYet {}              -> return ()

addReference :: PrimMonad m => WriteBufferBlobs m h -> m ()
addReference WriteBufferBlobs {blobFileRefCounter} =
    RC.addReference blobFileRefCounter

removeReference :: (PrimMonad m, MonadMask m) => WriteBufferBlobs m h -> m ()
removeReference WriteBufferBlobs {blobFileRefCounter} =
    RC.removeReference blobFileRefCounter

addBlob :: (PrimMonad m, MonadThrow m)
        => HasFS m h
        -> WriteBufferBlobs m h
        -> SerialisedBlob
        -> m BlobSpan
addBlob fs WriteBufferBlobs {blobFileName, blobFileState} blob = do
    bfs <- P.readMutVar blobFileState
    case bfs of
      OpenBlobFile {blobFileHandle, blobFilePointer} ->
        writeBlob blobFileHandle blobFilePointer

      NoBlobFileYet -> do
        -- Must use read/write mode because we write blobs when adding, but
        -- we can also be asked to retrieve blobs at any time.
        blobFileHandle <- FS.hOpen fs blobFileName (FS.ReadWriteMode FS.MustBeNew)
        blobFilePointer <- newFilePointer
        P.writeMutVar blobFileState $! OpenBlobFile {
          blobFileHandle,
          blobFilePointer
        }
        writeBlob blobFileHandle blobFilePointer

  where
    writeBlob h hoff = do
      let blobsize = sizeofBlob blob
      bloboffset <- updateFilePointer hoff blobsize
      writeBlobAtOffset fs h blob bloboffset
      pure BlobSpan {
        blobSpanOffset = bloboffset,
        blobSpanSize   = fromIntegral blobsize
      }

writeBlobAtOffset :: (PrimMonad m, MonadThrow m)
                  => HasFS m h
                  -> FS.Handle h
                  -> SerialisedBlob
                  -> Word64
                  -> m ()
writeBlobAtOffset fs h (SerialisedBlob' (VP.Vector boff blen ba)) off = do
    mba <- P.unsafeThawByteArray ba
    _   <- FS.hPutBufExactlyAt
             fs h mba
             (FS.BufferOffset boff)
             (fromIntegral blen :: FS.ByteCount)
             (FS.AbsOffset off)
    return ()

readBlob :: (PrimMonad m, MonadThrow m)
         => HasFS m h
         -> WriteBufferBlobs m h
         -> BlobSpan
         -> m SerialisedBlob
readBlob fs WriteBufferBlobs {blobFileState} BlobSpan {blobSpanOffset, blobSpanSize} = do
    bfs <- P.readMutVar blobFileState
    case bfs of
      OpenBlobFile {blobFileHandle} -> do
        let off = FS.AbsOffset blobSpanOffset
            len :: Int
            len = fromIntegral blobSpanSize
        mba <- P.newPinnedByteArray len
        _ <- FS.hGetBufExactlyAt fs blobFileHandle mba 0
                                 (fromIntegral len :: FS.ByteCount) off
        ba <- P.unsafeFreezeByteArray mba
        let !rb = RB.fromByteArray 0 len ba
        return (SerialisedBlob rb)

      -- We must not be asking to read blobs if no blobs have been written.
      -- That would be an internal error.
      NoBlobFileYet{} ->
        error "BlobFile.mkBlobRef: no such blob refs, no blob file open"

-- | Helper function to make a 'BlobRef' that points into a 'WriteBufferBlobs'.
-- TODO: this two stage function design isn't terribly nice, and allocates
-- more than necessary (a closure every call).
mkBlobRef :: PrimMonad m
          => WriteBufferBlobs m h
          -> m (BlobSpan -> BlobRef m (FS.Handle h))
mkBlobRef WriteBufferBlobs {blobFileState, blobFileRefCounter} = do
    bfs <- P.readMutVar blobFileState
    case bfs of
      --TODO: for assertion checking we could check the span exists within
      -- the file.
      OpenBlobFile {blobFileHandle} ->
        pure $ \blobRefSpan -> BlobRef {
          blobRefFile  = blobFileHandle,
          blobRefCount = blobFileRefCounter,
          blobRefSpan
        }
      -- We must not be handing out references to blobs if no blobs have been
      -- written. That would be an internal error.
      NoBlobFileYet{} ->
        pure $ \_blobRefSpan ->
          error "BlobFile.mkBlobRef: no such blob refs, no blob file open"


-- | A mutable file offset, suitable to share between threads.
newtype FilePointer m = FilePointer (PrimVar (PrimState m) Int)
--TODO: this would be better as Word64
-- otherwise this will not work on 32bit arches

newFilePointer :: PrimMonad m => m (FilePointer m)
newFilePointer = FilePointer <$> P.newPrimVar 0

-- | Update the file offset by a given amount and return the new offset. This
-- is safe to use concurrently.
--
updateFilePointer :: PrimMonad m => FilePointer m -> Int -> m Word64
updateFilePointer (FilePointer var) n = fromIntegral <$> P.fetchAddInt var n
