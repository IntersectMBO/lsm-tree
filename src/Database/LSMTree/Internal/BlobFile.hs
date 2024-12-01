{-# LANGUAGE TypeFamilies #-}
module Database.LSMTree.Internal.BlobFile (
    BlobFile (..)
  , BlobSpan (..)
  , openBlobFile
  , readBlob
  , readBlobRaw
  , writeBlob
  ) where

import           Control.DeepSeq (NFData (..))
import           Control.Monad.Class.MonadThrow (MonadThrow)
import           Control.Monad.Primitive (PrimMonad)
import           Control.RefCount
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

instance RefCounted (BlobFile m h) where
    type FinaliserM (BlobFile m h) = m
    getRefCounter = blobFileRefCounter

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

-- | Open the given file to make a 'BlobFile'. The finaliser will close and
-- delete the file.
{-# SPECIALISE openBlobFile :: HasFS IO h -> FS.FsPath -> FS.OpenMode -> IO (Ref (BlobFile IO h)) #-}
openBlobFile ::
     PrimMonad m
  => HasFS m h
  -> FS.FsPath
  -> FS.OpenMode
  -> m (Ref (BlobFile m h))
openBlobFile fs path mode = do
    blobFileHandle <- FS.hOpen fs path mode
    let finaliser = do
          FS.hClose fs blobFileHandle
          FS.removeFile fs (FS.handlePath blobFileHandle)
    newRef finaliser $ \blobFileRefCounter ->
      BlobFile {
        blobFileHandle,
        blobFileRefCounter
      }

{-# INLINE readBlob #-}
readBlob ::
     (MonadThrow m, PrimMonad m)
  => HasFS m h
  -> Ref (BlobFile m h)
  -> BlobSpan
  -> m SerialisedBlob
readBlob fs (DeRef blobfile) blobspan = readBlobRaw fs blobfile blobspan

{-# SPECIALISE readBlobRaw :: HasFS IO h -> BlobFile IO h -> BlobSpan -> IO SerialisedBlob #-}
readBlobRaw ::
     (MonadThrow m, PrimMonad m)
  => HasFS m h
  -> BlobFile m h
  -> BlobSpan
  -> m SerialisedBlob
readBlobRaw fs BlobFile {blobFileHandle}
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

{-# SPECIALISE writeBlob :: HasFS IO h -> Ref (BlobFile IO h) -> SerialisedBlob -> Word64 -> IO () #-}
writeBlob ::
     (MonadThrow m, PrimMonad m)
  => HasFS m h
  -> Ref (BlobFile m h)
  -> SerialisedBlob
  -> Word64
  -> m ()
writeBlob fs (DeRef BlobFile {blobFileHandle})
          (SerialisedBlob' (VP.Vector boff blen ba)) off = do
    mba <- P.unsafeThawByteArray ba
    _   <- FS.hPutBufExactlyAt
             fs blobFileHandle mba
             (FS.BufferOffset boff)
             (fromIntegral blen :: FS.ByteCount)
             (FS.AbsOffset off)
    return ()
