module Database.LSMTree.Internal.FS.PointedHandle (
    PointedHandle (..)
  , openReadMode
  , phClose
  , phGetSize
  , phSeekAbsolute
  , phSeekRelative
  , pageSize
  , seekToDiskPage
  , readDiskPage
  , readOverflowPages
  ) where

import           Control.Exception (assert)
import           Control.Monad
import           Control.Monad.Class.MonadSTM (MonadSTM (..))
import           Control.Monad.Class.MonadThrow (MonadCatch (..),
                     MonadThrow (..))
import           Control.Monad.Primitive (PrimMonad (..))
import           Data.Int
import           Data.Primitive.ByteArray
import           Data.Word
import           Database.LSMTree.Internal.BitMath (mulPageSize,
                     roundUpToPageSize)
import           Database.LSMTree.Internal.FS.FilePointer
import           Database.LSMTree.Internal.Page (PageNo (..))
import           Database.LSMTree.Internal.RawOverflowPage (RawOverflowPage,
                     pinnedByteArrayToOverflowPages)
import           Database.LSMTree.Internal.RawPage
import           System.FS.API
import           System.FS.BlockIO.API


data PointedHandle m h = PointedHandle {
      fileHandle  :: !(Handle h)
    , filePointer :: !(FilePointer m)
    , fileSize    :: !Word64
    }

openReadMode :: (MonadCatch m, PrimMonad m) => HasFS m h -> FsPath -> m (PointedHandle m h)
openReadMode hfs path =
    bracketOnError (hOpen hfs path ReadMode) (hClose hfs) $ \fileHandle -> do
      fileSize <- hGetSize hfs fileHandle
      filePointer <- newFilePointer
      pure PointedHandle{..}

phClose :: HasFS m h -> PointedHandle m h -> m ()
phClose hfs PointedHandle{..} = hClose hfs fileHandle

phGetSize :: Applicative m => PointedHandle m h -> m Word64
phGetSize PointedHandle{..} = pure fileSize

phSeekAbsolute :: PrimMonad m => PointedHandle m h -> Word64 -> m ()
phSeekAbsolute PointedHandle{..} off = setFilePointer filePointer off

phSeekRelative :: PrimMonad m => PointedHandle m h -> Int64 -> m Word64
phSeekRelative PointedHandle{..} x = updateFilePointer filePointer (fromIntegral x)

phGetBufExactly ::
     (MonadThrow m, PrimMonad m)
  => HasFS m h
  -> PointedHandle m h
  -> ByteCount
  -> m (Maybe ByteArray)
phGetBufExactly hfs PointedHandle{..} n = do
    currentOffset <- updateFilePointer filePointer 0
    if currentOffset + fromIntegral n <= fileSize then do
      mba <- newPinnedByteArray (fromIntegral n)
      _ <- hGetBufExactlyAt hfs fileHandle mba 0 n (AbsOffset currentOffset)
      void $ updateFilePointer filePointer (fromIntegral n)
      Just <$> unsafeFreezeByteArray mba
    else
      pure Nothing



{-# SPECIALISE seekToDiskPage ::
     PageNo
  -> PointedHandle IO h
  -> IO () #-}
seekToDiskPage :: PrimMonad m => PageNo -> PointedHandle m h -> m ()
seekToDiskPage pageNo h = phSeekAbsolute h (pageNoToByteOffset pageNo)
  where
    pageNoToByteOffset (PageNo n) =
        assert (n >= 0) $
          mulPageSize (fromIntegral n)

-- | Returns 'Nothing' on EOF.
readDiskPage ::
     (MonadCatch m, PrimMonad m)
  => HasFS m h
  -> PointedHandle m h
  -> m (Maybe RawPage)
readDiskPage hfs h = do
    maybeBytes <- phGetBufExactly hfs h pageSize
    let !rawPage = flip unsafeMakeRawPage 0 <$!> maybeBytes
    pure rawPage

-- TODO: put somewhere general
pageSize :: ByteCount
pageSize = 4096


{-# SPECIALISE readOverflowPages ::
     HasFS IO h
  -> PointedHandle IO h
  -> Word32
  -> IO [RawOverflowPage] #-}
-- | Throws exception on EOF. If a suffix was expected, the file should have it.
-- Reads full pages, despite the suffix only using part of the last page.
readOverflowPages ::
     (MonadSTM m, MonadThrow m, PrimMonad m)
   => HasFS m h
   -> PointedHandle m h
   -> Word32
   -> m [RawOverflowPage]
readOverflowPages hfs h len = do
    let lenPages = fromIntegral (roundUpToPageSize len)  -- always read whole pages
    maybeBytes <- phGetBufExactly hfs h (fromIntegral lenPages)
    case maybeBytes of
      Nothing -> error "readOverflowPages: unexpectedly, could not read overflow pages"
      Just ba ->
        -- should not copy since 'ba' is pinned and its length is a multiple of 4k.
        pure $ pinnedByteArrayToOverflowPages 0 lenPages ba
