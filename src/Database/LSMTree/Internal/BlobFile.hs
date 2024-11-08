module Database.LSMTree.Internal.BlobFile (
    BlobFile (..)
  , BlobSpan (..)
  , removeReference
  , RemoveFileOnClose (..)
  , openBlobFile
  , readBlob
  , writeBlob
  ) where

import           Control.DeepSeq (NFData (..))
import           Control.Monad (unless)
import           Control.Monad.Class.MonadThrow (MonadMask, MonadThrow)
import           Control.Monad.Primitive (PrimMonad)
import           Control.RefCount (RefCounter)
import qualified Control.RefCount as RC
import qualified Data.Primitive.ByteArray as P
import qualified Data.Vector.Primitive as VP
import           Data.Word (Word32, Word64)
import qualified Database.LSMTree.Internal.RawBytes as RB
import           Database.LSMTree.Internal.Serialise (SerialisedBlob (..))
import qualified System.FS.API as FS
import           System.FS.API (HasFS)
import qualified System.FS.BlockIO.API as FS

-- | A handle to a file containing blobs.
--
-- This is a reference counted object. Upon finalisation, the file is closed
-- and deleted.
--
data BlobFile m h = BlobFile {
       blobFileHandle     :: {-# UNPACK #-} !(FS.Handle h),
       blobFileRefCounter :: {-# UNPACK #-} !(RefCounter m)
     }
  deriving stock (Show)

instance NFData h => NFData (BlobFile m h) where
  rnf (BlobFile a b) = rnf a `seq` rnf b

-- | The location of a blob inside a blob file.
data BlobSpan = BlobSpan {
    blobSpanOffset :: {-# UNPACK #-} !Word64
  , blobSpanSize   :: {-# UNPACK #-} !Word32
  }
  deriving stock (Show, Eq)

instance NFData BlobSpan where
  rnf (BlobSpan a b) = rnf a `seq` rnf b

{-# INLINE removeReference #-}
removeReference ::
     (MonadMask m, PrimMonad m)
  => BlobFile m h
  -> m ()
removeReference BlobFile{blobFileRefCounter} =
    RC.removeReference blobFileRefCounter

-- | TODO: this hack can be removed once snapshots are done properly and so
-- runs can delete their files on close.
data RemoveFileOnClose = RemoveFileOnClose | DoNotRemoveFileOnClose
  deriving stock Eq

-- | Open the given file to make a 'BlobFile'. The finaliser will close and
-- delete the file.
--
-- TODO: Temporarily we have a 'RemoveFileOnClose' flag, which can be removed
-- once 'Run' no longer needs it, when snapshots are implemented.
--
{-# SPECIALISE openBlobFile :: HasFS IO h -> FS.FsPath -> FS.OpenMode -> RemoveFileOnClose -> IO (BlobFile IO h) #-}
openBlobFile ::
     PrimMonad m
  => HasFS m h
  -> FS.FsPath
  -> FS.OpenMode
  -> RemoveFileOnClose
  -> m (BlobFile m h)
openBlobFile fs path mode remove = do
    blobFileHandle <- FS.hOpen fs path mode
    let finaliser = do
          FS.hClose fs blobFileHandle
          unless (remove == DoNotRemoveFileOnClose) $
            FS.removeFile fs (FS.handlePath blobFileHandle)
    blobFileRefCounter <- RC.mkRefCounter1 (Just finaliser)
    return BlobFile {
      blobFileHandle,
      blobFileRefCounter
    }

{-# SPECIALISE readBlob :: HasFS IO h -> BlobFile IO h -> BlobSpan -> IO SerialisedBlob #-}
readBlob ::
     (MonadThrow m, PrimMonad m)
  => HasFS m h
  -> BlobFile m h
  -> BlobSpan
  -> m SerialisedBlob
readBlob fs BlobFile {blobFileHandle}
            BlobSpan {blobSpanOffset, blobSpanSize} = do
    let off = FS.AbsOffset blobSpanOffset
        len :: Int
        len = fromIntegral blobSpanSize
    mba <- P.newPinnedByteArray len
    _ <- FS.hGetBufExactlyAt fs blobFileHandle mba 0
                             (fromIntegral len :: FS.ByteCount) off
    ba <- P.unsafeFreezeByteArray mba
    let !rb = RB.fromByteArray 0 len ba
    return (SerialisedBlob rb)

{-# SPECIALISE writeBlob :: HasFS IO h -> BlobFile IO h -> SerialisedBlob -> Word64 -> IO () #-}
writeBlob ::
     (MonadThrow m, PrimMonad m)
  => HasFS m h
  -> BlobFile m h
  -> SerialisedBlob
  -> Word64
  -> m ()
writeBlob fs BlobFile {blobFileHandle}
          (SerialisedBlob' (VP.Vector boff blen ba)) off = do
    mba <- P.unsafeThawByteArray ba
    _   <- FS.hPutBufExactlyAt
             fs blobFileHandle mba
             (FS.BufferOffset boff)
             (fromIntegral blen :: FS.ByteCount)
             (FS.AbsOffset off)
    return ()