{-# LANGUAGE MagicHash #-}

module Database.LSMTree.Internal.ChecksumHandle
  (
    -- * Checksum handles
    -- $checksum-handles
    ChecksumHandle (..),
    makeHandle,
    readChecksum,
    dropCache,
    closeHandle,
    writeToHandle,
    -- * Specialised writers
    writeRawPage,
    writeRawOverflowPages,
    writeBlob,
    copyBlob,
    writeFilter,
    writeIndexHeader,
    writeIndexChunk,
    writeIndexFinal,
  ) where

import           Control.Monad.Class.MonadSTM (MonadSTM (..))
import           Control.Monad.Class.MonadThrow (MonadThrow)
import           Control.Monad.Primitive
import           Data.BloomFilter (Bloom)
import qualified Data.ByteString.Lazy as BSL
import           Data.Primitive.PrimVar
import           Data.Word (Word64)
import           Database.LSMTree.Internal.BlobRef (BlobSpan (..), RawBlobRef)
import qualified Database.LSMTree.Internal.BlobRef as BlobRef
import           Database.LSMTree.Internal.BloomFilter (bloomFilterToLBS)
import           Database.LSMTree.Internal.Chunk (Chunk)
import qualified Database.LSMTree.Internal.Chunk as Chunk (toByteString)
import           Database.LSMTree.Internal.CRC32C (CRC32C)
import qualified Database.LSMTree.Internal.CRC32C as CRC
import           Database.LSMTree.Internal.Entry
import           Database.LSMTree.Internal.Index (Index)
import qualified Database.LSMTree.Internal.Index as Index
import           Database.LSMTree.Internal.Paths (ForBlob (..), ForFilter (..),
                     ForIndex (..), ForKOps (..))
import qualified Database.LSMTree.Internal.RawBytes as RB
import           Database.LSMTree.Internal.RawOverflowPage (RawOverflowPage)
import qualified Database.LSMTree.Internal.RawOverflowPage as RawOverflowPage
import           Database.LSMTree.Internal.RawPage (RawPage)
import qualified Database.LSMTree.Internal.RawPage as RawPage
import           Database.LSMTree.Internal.Serialise
import           GHC.Exts (Proxy#)
import qualified System.FS.API as FS
import           System.FS.API
import qualified System.FS.BlockIO.API as FS
import           System.FS.BlockIO.API (HasBlockIO)

{-------------------------------------------------------------------------------
  ChecksumHandle
-------------------------------------------------------------------------------}

{- $checksum-handles
  A handle ('ChecksumHandle') that maintains a running CRC32 checksum.
-}

-- | Tracks the checksum of a (write mode) file handle.
data ChecksumHandle s h = ChecksumHandle !(FS.Handle h) !(PrimVar s CRC32C)

{-# SPECIALISE makeHandle ::
     HasFS IO h
  -> FS.FsPath
  -> IO (ChecksumHandle RealWorld h) #-}
makeHandle ::
     (MonadSTM m, PrimMonad m)
  => HasFS m h
  -> FS.FsPath
  -> m (ChecksumHandle (PrimState m) h)
makeHandle fs path =
    ChecksumHandle
      <$> FS.hOpen fs path (FS.WriteMode FS.MustBeNew)
      <*> newPrimVar CRC.initialCRC32C

{-# SPECIALISE readChecksum ::
     ChecksumHandle RealWorld h
  -> IO CRC32C #-}
readChecksum ::
     PrimMonad m
  => ChecksumHandle (PrimState m) h
  -> m CRC32C
readChecksum (ChecksumHandle _h checksum) = readPrimVar checksum

dropCache :: HasBlockIO m h -> ChecksumHandle (PrimState m) h -> m ()
dropCache hbio (ChecksumHandle h _) = FS.hDropCacheAll hbio h

closeHandle :: HasFS m h -> ChecksumHandle (PrimState m) h -> m ()
closeHandle fs (ChecksumHandle h _checksum) = FS.hClose fs h

{-# SPECIALISE writeToHandle ::
     HasFS IO h
  -> ChecksumHandle RealWorld h
  -> BSL.ByteString
  -> IO () #-}
writeToHandle ::
     (MonadSTM m, PrimMonad m)
  => HasFS m h
  -> ChecksumHandle (PrimState m) h
  -> BSL.ByteString
  -> m ()
writeToHandle fs (ChecksumHandle h checksum) lbs = do
    crc <- readPrimVar checksum
    (_, crc') <- CRC.hPutAllChunksCRC32C fs h lbs crc
    writePrimVar checksum crc'

{-------------------------------------------------------------------------------
  Specialised Writers for ChecksumHandle
-------------------------------------------------------------------------------}

{-# SPECIALISE writeRawPage ::
     HasFS IO h
  -> ForKOps (ChecksumHandle RealWorld h)
  -> RawPage
  -> IO () #-}
writeRawPage ::
     (MonadSTM m, PrimMonad m)
  => HasFS m h
  -> ForKOps (ChecksumHandle (PrimState m) h)
  -> RawPage
  -> m ()
writeRawPage hfs kOpsHandle =
    writeToHandle hfs (unForKOps kOpsHandle)
  . BSL.fromStrict
  . RB.unsafePinnedToByteString -- 'RawPage' is guaranteed to be pinned
  . RawPage.rawPageRawBytes

{-# SPECIALISE writeRawOverflowPages ::
     HasFS IO h
  -> ForKOps (ChecksumHandle RealWorld h)
  -> [RawOverflowPage]
  -> IO () #-}
writeRawOverflowPages ::
     (MonadSTM m, PrimMonad m)
  => HasFS m h
  -> ForKOps (ChecksumHandle (PrimState m) h)
  -> [RawOverflowPage]
  -> m ()
writeRawOverflowPages hfs kOpsHandle =
    writeToHandle hfs (unForKOps kOpsHandle)
  . BSL.fromChunks
  . map (RawOverflowPage.rawOverflowPageToByteString)

{-# SPECIALISE writeBlob ::
     HasFS IO h
  -> PrimVar RealWorld Word64
  -> ForBlob (ChecksumHandle RealWorld h)
  -> SerialisedBlob
  -> IO BlobSpan #-}
writeBlob ::
     (MonadSTM m, PrimMonad m)
  => HasFS m h
  -> PrimVar (PrimState m) Word64
  -> ForBlob (ChecksumHandle (PrimState m) h)
  -> SerialisedBlob
  -> m BlobSpan
writeBlob hfs blobOffset blobHandle blob = do
    -- NOTE: This is different from BlobFile.writeBlob. This is because BlobFile
    --  internalises a regular Handle, rather than a ChecksumHandle. These two
    --  functions cannot be easily unified, because BlobFile.writeBlob permits
    --  writing blobs to arbitrary positions in the blob file, whereas, by the
    --  very nature of CRC32 checksums, ChecksumHandle.writeBlob only supports
    --  sequential writes.
    let size = sizeofBlob64 blob
    offset <- readPrimVar blobOffset
    modifyPrimVar blobOffset (+size)
    let SerialisedBlob rb = blob
    let lbs = BSL.fromStrict $ RB.toByteString rb
    writeToHandle hfs (unForBlob blobHandle) lbs
    return (BlobSpan offset (fromIntegral size))

{-# SPECIALISE copyBlob ::
     HasFS IO h
  -> PrimVar RealWorld Word64
  -> ForBlob (ChecksumHandle RealWorld h)
  -> RawBlobRef IO h
  -> IO BlobSpan #-}
copyBlob ::
     (MonadSTM m, MonadThrow m, PrimMonad m)
  => HasFS m h
  -> PrimVar (PrimState m) Word64
  -> ForBlob (ChecksumHandle (PrimState m) h)
  -> RawBlobRef m h
  -> m BlobSpan
copyBlob hfs blobOffset blobHandle blobref = do
    blob <- BlobRef.readRawBlobRef hfs blobref
    writeBlob hfs blobOffset blobHandle blob

{-# SPECIALISE writeFilter ::
     HasFS IO h
  -> ForFilter (ChecksumHandle RealWorld h)
  -> Bloom SerialisedKey
  -> IO () #-}
writeFilter ::
     (MonadSTM m, PrimMonad m)
  => HasFS m h
  -> ForFilter (ChecksumHandle (PrimState m) h)
  -> Bloom SerialisedKey
  -> m ()
writeFilter hfs filterHandle bf =
    writeToHandle hfs (unForFilter filterHandle) (bloomFilterToLBS bf)

{-# SPECIALISE writeIndexHeader ::
     Index i
  => HasFS IO h
  -> ForIndex (ChecksumHandle RealWorld h)
  -> Proxy# i
  -> IO () #-}
writeIndexHeader ::
     (MonadSTM m, PrimMonad m, Index i)
  => HasFS m h
  -> ForIndex (ChecksumHandle (PrimState m) h)
  -> Proxy# i
  -> m ()
writeIndexHeader hfs indexHandle indexTypeProxy =
    writeToHandle hfs (unForIndex indexHandle) $
      Index.headerLBS indexTypeProxy

{-# SPECIALISE writeIndexChunk ::
     HasFS IO h
  -> ForIndex (ChecksumHandle RealWorld h)
  -> Chunk
  -> IO () #-}
writeIndexChunk ::
     (MonadSTM m, PrimMonad m)
  => HasFS m h
  -> ForIndex (ChecksumHandle (PrimState m) h)
  -> Chunk
  -> m ()
writeIndexChunk hfs indexHandle chunk =
    writeToHandle hfs (unForIndex indexHandle) $
      BSL.fromStrict $ Chunk.toByteString chunk

{-# SPECIALISE writeIndexFinal ::
     Index i
  => HasFS IO h
  -> ForIndex (ChecksumHandle RealWorld h)
  -> NumEntries
  -> i
  -> IO () #-}
writeIndexFinal ::
     (MonadSTM m, PrimMonad m, Index i)
  => HasFS m h
  -> ForIndex (ChecksumHandle (PrimState m) h)
  -> NumEntries
  -> i
  -> m ()
writeIndexFinal hfs indexHandle numEntries index =
    writeToHandle hfs (unForIndex indexHandle) $
      Index.finalLBS numEntries index
