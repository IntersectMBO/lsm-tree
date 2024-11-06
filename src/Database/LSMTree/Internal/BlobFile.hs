{-# LANGUAGE DeriveAnyClass     #-}
{-# LANGUAGE DeriveGeneric      #-}
{-# LANGUAGE DerivingStrategies #-}
{- HLINT ignore "Use unless" -}

module Database.LSMTree.Internal.BlobFile (
    BlobFile (..)
  , BlobSpan (..)
  , removeReference
  , RemoveFileOnClose (..)
  , newBlobFile
  , readBlobFile
  ) where

import           Control.DeepSeq (NFData (..))
import           Control.Monad (unless)
import           Control.Monad.Class.MonadThrow (MonadMask, MonadThrow)
import           Control.Monad.Primitive (PrimMonad)
import           Control.RefCount (RefCounter)
import qualified Control.RefCount as RC
import qualified Data.Primitive.ByteArray as P (newPinnedByteArray,
                     unsafeFreezeByteArray)
import           Data.Word (Word32, Word64)
import qualified Database.LSMTree.Internal.RawBytes as RB
import           Database.LSMTree.Internal.Serialise (SerialisedBlob (..))
import qualified System.FS.API as FS
import           System.FS.API (HasFS)
import qualified System.FS.BlockIO.API as FS

-- | An open handle to a file containing blobs.
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

-- | Adopt an existing open file handle to make a 'BlobFile'. The file must at
-- least be open for reading (but may or may not be open for writing).
--
-- The finaliser will close and delete the file.
--
newBlobFile ::
     PrimMonad m
  => HasFS m h
  -> RemoveFileOnClose
  -> FS.Handle h
  -> m (BlobFile m h)
newBlobFile fs r blobFileHandle = do
    let finaliser = do
          FS.hClose fs blobFileHandle
          unless (r == DoNotRemoveFileOnClose) $
            FS.removeFile fs (FS.handlePath blobFileHandle)
    blobFileRefCounter <- RC.mkRefCounter1 (Just finaliser)
    return BlobFile {
      blobFileHandle,
      blobFileRefCounter
    }

{-# SPECIALISE readBlobFile :: HasFS IO h -> BlobFile IO h -> BlobSpan -> IO SerialisedBlob #-}
readBlobFile ::
     (MonadThrow m, PrimMonad m)
  => HasFS m h
  -> BlobFile m h
  -> BlobSpan
  -> m SerialisedBlob
readBlobFile fs BlobFile {blobFileHandle}
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
