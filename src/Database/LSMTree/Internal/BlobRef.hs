{-# LANGUAGE DeriveAnyClass     #-}
{-# LANGUAGE DeriveGeneric      #-}
{-# LANGUAGE DerivingStrategies #-}

module Database.LSMTree.Internal.BlobRef (
    BlobRef (..)
  , BlobSpan (..)
  , blobRefSpanSize
  , readBlob
  , readBlobIOOp
  ) where

import           Control.DeepSeq (NFData (..))
import           Control.RefCount (RefCounter)
import qualified Data.Primitive.ByteArray as P (MutableByteArray,
                     newPinnedByteArray, unsafeFreezeByteArray)
import           Data.Word (Word32, Word64)
import qualified Database.LSMTree.Internal.RawBytes as RB
import           Database.LSMTree.Internal.Serialise (SerialisedBlob (..))
import qualified System.FS.API as FS
import           System.FS.API (HasFS)
import qualified System.FS.BlockIO.API as FS

-- | A handle-like reference to an on-disk blob. The blob can be retrieved based
-- on the reference.
--
-- See 'Database.LSMTree.Common.BlobRef' for more info.
data BlobRef m h = BlobRef {
      blobRefFile  :: !h
    , blobRefCount :: {-# UNPACK #-} !(RefCounter m)
    , blobRefSpan  :: {-# UNPACK #-} !BlobSpan
    }
  deriving stock (Eq, Show)

instance NFData h => NFData (BlobRef m h) where
  rnf (BlobRef a b c) = rnf a `seq` rnf b `seq` rnf c

-- | Location of a blob inside a blob file.
data BlobSpan = BlobSpan {
    blobSpanOffset :: {-# UNPACK #-} !Word64
  , blobSpanSize   :: {-# UNPACK #-} !Word32
  }
  deriving stock (Show, Eq)

instance NFData BlobSpan where
  rnf (BlobSpan a b) = rnf a `seq` rnf b

blobRefSpanSize :: BlobRef m h -> Int
blobRefSpanSize = fromIntegral . blobSpanSize . blobRefSpan

-- | The 'BlobSpan' to read must come from this run!
readBlob :: HasFS IO h -> BlobRef m (FS.Handle h) -> IO SerialisedBlob
readBlob fs BlobRef {
              blobRefFile,
              blobRefSpan = BlobSpan {blobSpanOffset, blobSpanSize}
            } = do
    let off = FS.AbsOffset blobSpanOffset
        len :: Int
        len = fromIntegral blobSpanSize
    mba <- P.newPinnedByteArray len
    _ <- FS.hGetBufExactlyAt fs blobRefFile mba 0
                             (fromIntegral len :: FS.ByteCount) off
    ba <- P.unsafeFreezeByteArray mba
    let !rb = RB.fromByteArray 0 len ba
    return (SerialisedBlob rb)

readBlobIOOp :: P.MutableByteArray s -> Int
             -> BlobRef m (FS.Handle h)
             -> FS.IOOp s h
readBlobIOOp buf bufoff
             BlobRef {
               blobRefFile,
               blobRefSpan = BlobSpan {blobSpanOffset, blobSpanSize}
             } =
    FS.IOOpRead
      blobRefFile
      (fromIntegral blobSpanOffset :: FS.FileOffset)
      buf (FS.BufferOffset bufoff)
      (fromIntegral blobSpanSize :: FS.ByteCount)

