module Database.LSMTree.Internal.BloomFilter (
  BF.Bloom,
  BF.MBloom,
  bloomFilterToLBS,
  bloomFilterFromFile,
) where

import           Control.Monad (when, void)
import           Control.Monad.Class.MonadThrow
import           Control.Monad.Primitive (PrimMonad)
import qualified Data.ByteString.Builder.Extra as B
import qualified Data.ByteString.Lazy as LBS
import qualified Data.ByteString as BS
import qualified Data.Primitive.ByteArray as P
import           Data.Word (Word32, Word64, byteSwap32)
import           System.FS.API

import qualified Data.BloomFilter as BF
import           Database.LSMTree.Internal.ByteString (byteArrayToByteString)
import           Database.LSMTree.Internal.CRC32C (FileFormatError (..))

-- serialising
-----------------------------------------------------------

-- | By writing out the version in host endianness, we also indicate endianness.
-- During deserialisation, we would discover an endianness mismatch.
bloomFilterVersion :: Word32
bloomFilterVersion = 1

bloomFilterToLBS :: BF.Bloom a -> LBS.ByteString
bloomFilterToLBS bf =
    let (size, ba, off, len) = BF.serialise bf
     in header size <> byteArrayToLBS ba off len
  where
    header BF.BloomSize { sizeBits, sizeHashes } =
        -- creates a single 16 byte chunk
        B.toLazyByteStringWith (B.safeStrategy 16 B.smallChunkSize) mempty $
             B.word32Host bloomFilterVersion
          <> B.word32Host (fromIntegral sizeHashes)
          <> B.word64Host (fromIntegral sizeBits)

    byteArrayToLBS :: P.ByteArray -> Int -> Int -> LBS.ByteString
    byteArrayToLBS ba off len =
      LBS.fromStrict (byteArrayToByteString off len ba)

-- deserialising
-----------------------------------------------------------

{-# SPECIALISE bloomFilterFromFile ::
     HasFS IO h
  -> FsPath
  -> Handle h
  -> IO (BF.Bloom a) #-}
-- | Read a 'BF.Bloom' from a file.
--
bloomFilterFromFile ::
     (PrimMonad m, MonadCatch m)
  => HasFS m h
  -> FsPath    -- ^ File path just for error reporting
  -> Handle h  -- ^ The open file, in read mode
  -> m (BF.Bloom a)
bloomFilterFromFile hfs fp h = do
    header <- rethrowEOFError "Doesn't contain a header" $
              hGetByteArrayExactly hfs h 16

    let !version = P.indexByteArray header 0 :: Word32
        !nhashes = P.indexByteArray header 1 :: Word32
        !nbits   = P.indexByteArray header 1 :: Word64

    when (version /= bloomFilterVersion) $ throwFormatError $
      if byteSwap32 version == bloomFilterVersion
      then "Different byte order"
      else "Unsupported version"

    when (nbits <= 0) $ throwFormatError "Length is zero"

    -- limit to 2^48 bits
    when (nbits >= 0x1_0000_0000_0000) $ throwFormatError "Too large bloomfilter"
    --TODO: get max size from bloomfilter lib

    -- read the filter data from the file directly into the bloom filter
    bloom <-
      BF.deserialise
        BF.BloomSize {
          BF.sizeBits   = fromIntegral nbits,
          BF.sizeHashes = fromIntegral nhashes
        }
        (\buf off len ->
            rethrowEOFError "bloom filter file too short" $
              void $ hGetBufExactly hfs
                       h buf (BufferOffset off) (fromIntegral len))

    -- check we're now at EOF
    trailing <- hGetSome hfs h 1
    when (not (BS.null trailing)) $
      throwFormatError "Byte array is too large for components"
    return bloom
  where
    throwFormatError = throwIO . FileFormatError (mkFsErrorPath hfs fp)
    rethrowEOFError msg =
      handleJust
        (\e -> if isFsErrorType FsReachedEOF e then Just e else Nothing)
        (\e -> throwIO (FileFormatError (fsErrorPath e) msg))

hGetByteArrayExactly ::
     (PrimMonad m, MonadThrow m)
  => HasFS m h
  -> Handle h
  -> Int
  -> m P.ByteArray
hGetByteArrayExactly hfs h len = do
    buf <- P.newByteArray 16
    _   <- hGetBufExactly hfs h buf 0 (fromIntegral len)
    P.unsafeFreezeByteArray buf

