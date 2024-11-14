module Database.LSMTree.Internal.ChecksumHandle
  (
    ChecksumHandle (..),
    makeHandle,
    readChecksum,
    dropCache,
    closeHandle,
    writeToHandle,
  ) where

import           Control.Monad.Class.MonadSTM (MonadSTM (..))
import           Control.Monad.Primitive
import qualified Data.ByteString.Lazy as BSL
import           Data.Primitive.PrimVar
import           Database.LSMTree.Internal.CRC32C (CRC32C)
import qualified Database.LSMTree.Internal.CRC32C as CRC
import qualified System.FS.API as FS
import           System.FS.API (HasFS)
import qualified System.FS.BlockIO.API as FS
import           System.FS.BlockIO.API (HasBlockIO)

{-------------------------------------------------------------------------------
  ChecksumHandle
-------------------------------------------------------------------------------}

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
