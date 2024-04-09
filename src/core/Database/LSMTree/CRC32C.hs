{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | Functionalty related to CRC-32C (Castagnoli) checksums:
--
-- * Support for calculating checksums while incrementally writing files.
-- * Support for verifying checksums of files.
-- * Support for a text file format listing file checksums.
--
module Database.LSMTree.CRC32C (

  CRC32C(..),

  -- * Pure incremental checksum calculation
  initialCRC32C,
  updateCRC32C,

  -- * I\/O with checksum calculation
  hGetSomeCRC32C,
  hGetExactlyCRC32C,
  hPutSomeCRC32C,
  hPutAllCRC32C,
  hPutAllChunksCRC32C,
  readFileCRC32C,

  -- * Checksum files
  -- $checksum-files
  ChecksumsFile,
  ChecksumsFileName(..),
  readChecksumsFile,
  writeChecksumsFile,
  ) where

import           Data.Digest.CRC32C as CRC

import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as BS
import qualified Data.ByteString.Char8 as BSC
import qualified Data.ByteString.Lazy as BSL
import           Data.Either (partitionEithers)
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map

import           Data.Bits
import           Data.Char (ord)
import           Data.Word

import           Control.Monad
import           Control.Monad.Class.MonadThrow

import           System.FS.API
import           System.FS.API.Lazy


newtype CRC32C = CRC32C Word32
  deriving (Eq, Ord, Show)


initialCRC32C :: CRC32C
initialCRC32C = CRC32C 0 -- same as crc32c BS.empty


updateCRC32C :: BS.ByteString -> CRC32C -> CRC32C
updateCRC32C bs (CRC32C crc) = CRC32C (CRC.crc32c_update crc bs)


hGetSomeCRC32C :: Monad m
               => HasFS m h
               -> Handle h
               -> Word64
               -> CRC32C -> m (BS.ByteString, CRC32C)
hGetSomeCRC32C fs h n crc = do
    bs <- hGetSome fs h n
    let !crc' = updateCRC32C bs crc
    return (bs, crc')


-- | This function ensures that exactly the requested number of bytes is read.
-- If the file is too short, an 'FsError' of type 'FsReachedEOF' is thrown.
--
-- It attempts to read everything into a single strict chunk, which should
-- almost always succeed. If it doesn't, multiple chunks are produced.
--
-- TODO: To reliably return a strict bytestring without additional copying,
-- @fs-api@ needs to support directly reading into a buffer, which is currently
-- work in progress: <https://github.com/input-output-hk/fs-sim/pull/46>
hGetExactlyCRC32C :: MonadThrow m
               => HasFS m h
               -> Handle h
               -> Word64
               -> CRC32C -> m (BSL.ByteString, CRC32C)
hGetExactlyCRC32C fs h n crc = do
    lbs <- hGetExactly fs h n
    let !crc' = BSL.foldlChunks (flip updateCRC32C) crc lbs
    return (lbs, crc')


hPutSomeCRC32C :: Monad m
               => HasFS m h
               -> Handle h
               -> BS.ByteString
               -> CRC32C -> m (Word64, CRC32C)
hPutSomeCRC32C fs h bs crc = do
    !n <- hPutSome fs h bs
    let !crc' = updateCRC32C (BS.take (fromIntegral n) bs) crc
    return (n, crc')


-- | This function makes sure that the whole 'BS.ByteString' is written.
hPutAllCRC32C :: forall m h
              .  Monad m
              => HasFS m h
              -> Handle h
              -> BS.ByteString
              -> CRC32C -> m (Word64, CRC32C)
hPutAllCRC32C fs h = go 0
  where
    go :: Word64 -> BS.ByteString -> CRC32C -> m (Word64, CRC32C)
    go !written !bs !crc = do
      (n, crc') <- hPutSomeCRC32C fs h bs crc
      let bs'      = BS.drop (fromIntegral n) bs
          written' = written + n
      if BS.null bs'
        then return (written', crc')
        else go written' bs' crc'

-- | This function makes sure that the whole /lazy/ 'BSL.ByteString' is written.
hPutAllChunksCRC32C :: forall m h
                    .  Monad m
                    => HasFS m h
                    -> Handle h
                    -> BSL.ByteString
                    -> CRC32C -> m (Word64, CRC32C)
hPutAllChunksCRC32C fs h = \lbs crc ->
    foldM (uncurry putChunk) (0, crc) (BSL.toChunks lbs)
  where
    putChunk :: Word64 -> CRC32C -> BS.ByteString -> m (Word64, CRC32C)
    putChunk !written !crc !bs = do
      (n, crc') <- hPutAllCRC32C fs h bs crc
      return (written + n, crc')

readFileCRC32C :: forall m h. MonadThrow m => HasFS m h -> FsPath -> m CRC32C
readFileCRC32C fs file =
    withFile fs file ReadMode (\h -> go h initialCRC32C)
  where
    go :: Handle h -> CRC32C -> m CRC32C
    go !h !crc = do
      bs <- hGetSome fs h 65504  -- 2^16 - 4 words overhead
      if BS.null bs
        then return crc
        else go h (updateCRC32C bs crc)


{- | $checksum-files
We use @.checksum@ files to help verify the integrity of on disk snapshots.
Each .checksum file lists the CRC-32C (Castagnoli) of other files. For further
details see @doc/format-directory.md@.

The file uses the BSD-style checksum format (e.g. as produced by tools like
@md5sum --tag@), with the algorithm name \"CRC32C\". This format is text,
one line per file, using hexedecimal for the 32bit output.

Checksum files are used for each LSM run, and for the snapshot metadata.

Typical examples are:

> CRC32C (keyops) = fd040004
> CRC32C (blobs) = 5a3b820c
> CRC32C (filter) = 6653e178
> CRC32C (index) = f4ec6724

Or

> CRC32C (snapshot) = 87972d7f
-}

type ChecksumsFile = Map ChecksumsFileName CRC32C

-- | File names must not include characters @'('@, @')'@ or @'\n'@.
--
newtype ChecksumsFileName = ChecksumsFileName BSC.ByteString
  deriving (Eq, Ord, Show)

data ChecksumsFileFormatError = ChecksumsFileFormatError FsPath BSC.ByteString
  deriving Show

instance Exception ChecksumsFileFormatError

readChecksumsFile :: MonadThrow m
                  => HasFS m h -> FsPath -> m ChecksumsFile
readChecksumsFile fs path = do
    str <- withFile fs path ReadMode (\h -> hGetAll fs h)
    case parseChecksumsFile (BSL.toStrict str) of
      Left  badline   -> throwIO (ChecksumsFileFormatError path badline)
      Right checksums -> return checksums

writeChecksumsFile :: MonadThrow m
                   => HasFS m h -> FsPath -> ChecksumsFile -> m ()
writeChecksumsFile fs path checksums =
    withFile fs path (WriteMode MustBeNew) $ \h -> do
      _ <- hPutAll fs h (formatChecksumsFile checksums)
      return ()

parseChecksumsFile :: BSC.ByteString -> Either BSC.ByteString ChecksumsFile
parseChecksumsFile content =
    case partitionEithers (parseLines content) of
      ([], entries)    -> Right $! Map.fromList entries
      ((badline:_), _) -> Left badline
  where
    parseLines = map (\l -> maybe (Left l) Right (parseChecksumFileLine l))
               . filter (not . BSC.null)
               . BSC.lines

parseChecksumFileLine :: BSC.ByteString -> Maybe (ChecksumsFileName, CRC32C)
parseChecksumFileLine str0 = do
    guard (BSC.take 8 str0 == "CRC32C (")
    let str1 = BSC.drop 8 str0
    let (name, str2) = BSC.break (==')') str1
    guard (BSC.take 4 str2 == ") = ")
    let str3 = BSC.drop 4 str2
    guard (BSC.length str3 == 8 && BSC.all isHexDigit str3)
    let !crc = fromIntegral (hexdigitsToInt str3)
    return (ChecksumsFileName name, CRC32C crc)

isHexDigit :: Char -> Bool
isHexDigit c = (c >= '0' && c <= '9')
            || (c >= 'a' && c <= 'f') --lower case only

-- Precondition: BSC.all isHexDigit
hexdigitsToInt :: BSC.ByteString -> Word
hexdigitsToInt =
    BSC.foldl' accumdigit 0
  where
    accumdigit :: Word -> Char -> Word
    accumdigit !a !c =
      (a `shiftL` 4) .|. hexdigitToWord c

-- Precondition: isHexDigit
hexdigitToWord :: Char -> Word
hexdigitToWord c
  | let !dec = fromIntegral (ord c - ord '0')
  , dec <= 9  = dec

  | let !hex = fromIntegral (ord c - ord 'a' + 10)
  , otherwise = hex

formatChecksumsFile :: ChecksumsFile -> BSL.ByteString
formatChecksumsFile checksums =
    BS.toLazyByteString $
      mconcat
        [    BS.byteString "CRC32C ("
          <> BS.byteString name
          <> BS.byteString ") = "
          <> BS.word32HexFixed crc
          <> BS.char8 '\n'
        | (ChecksumsFileName name, CRC32C crc) <- Map.toList checksums ]

