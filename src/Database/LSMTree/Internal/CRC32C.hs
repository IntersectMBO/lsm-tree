{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE MagicHash           #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# OPTIONS_HADDOCK not-home #-}

-- Needed by GHC <= 9.2 for newtype deriving Prim below
{-# LANGUAGE DataKinds           #-}
{-# LANGUAGE UnboxedTuples       #-}

-- | Functionalty related to CRC-32C (Castagnoli) checksums:
--
-- * Support for calculating checksums while incrementally writing files.
-- * Support for verifying checksums of files.
-- * Support for a text file format listing file checksums.
--
module Database.LSMTree.Internal.CRC32C (

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

  ChunkSize (..),
  defaultChunkSize,
  hGetExactlyCRC32C_SBS,
  hGetAllCRC32C',

  -- * Checksum files
  -- $checksum-files
  ChecksumsFile,
  ChecksumsFileName(..),
  getChecksum,
  readChecksumsFile,
  writeChecksumsFile,
  writeChecksumsFile',

  -- * Checksum checking
  checkCRC,
  expectChecksum,

  -- * File format errors
  FileFormat (..),
  FileCorruptedError (..),
  expectValidFile,
  ) where

import           Control.Monad
import           Control.Monad.Class.MonadThrow
import           Control.Monad.Primitive
import           Data.Bits
import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as BS
import qualified Data.ByteString.Char8 as BSC
import qualified Data.ByteString.Internal as BS.Internal
import qualified Data.ByteString.Lazy as BSL
import qualified Data.ByteString.Short as SBS
import           Data.Char (ord)
import           Data.Digest.CRC32C as CRC
import           Data.Either (partitionEithers)
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import           Data.Primitive
import           Data.Word
import           GHC.Exts
import qualified GHC.ForeignPtr as Foreign
import           System.FS.API
import           System.FS.API.Lazy
import           System.FS.BlockIO.API (Advice (..), ByteCount, HasBlockIO,
                     hAdviseAll, hDropCacheAll)

newtype CRC32C = CRC32C {unCRC32C :: Word32}
  deriving stock (Eq, Ord, Show)
  deriving newtype (Prim)


initialCRC32C :: CRC32C
initialCRC32C = CRC32C 0 -- same as crc32c BS.empty


updateCRC32C :: BS.ByteString -> CRC32C -> CRC32C
updateCRC32C bs (CRC32C crc) = CRC32C (CRC.crc32c_update crc bs)

{-# SPECIALISE hGetSomeCRC32C :: HasFS IO h -> Handle h -> Word64 -> CRC32C -> IO (BS.ByteString, CRC32C) #-}
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
{-# SPECIALISE hGetExactlyCRC32C :: HasFS IO h -> Handle h -> Word64 -> CRC32C -> IO (BSL.ByteString, CRC32C) #-}
hGetExactlyCRC32C :: MonadThrow m
               => HasFS m h
               -> Handle h
               -> Word64
               -> CRC32C -> m (BSL.ByteString, CRC32C)
hGetExactlyCRC32C fs h n crc = do
    lbs <- hGetExactly fs h n
    let !crc' = BSL.foldlChunks (flip updateCRC32C) crc lbs
    return (lbs, crc')


{-# SPECIALISE hPutSomeCRC32C :: HasFS IO h -> Handle h -> BS.ByteString -> CRC32C -> IO (Word64, CRC32C) #-}
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
{-# SPECIALISE hPutAllCRC32C :: HasFS IO h -> Handle h -> BS.ByteString -> CRC32C -> IO (Word64, CRC32C) #-}
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
{-# SPECIALISE hPutAllChunksCRC32C :: HasFS IO h -> Handle h -> BSL.ByteString -> CRC32C -> IO (Word64, CRC32C) #-}
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

{-# SPECIALISE readFileCRC32C :: HasFS IO h -> FsPath -> IO CRC32C #-}
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

newtype ChunkSize = ChunkSize ByteCount

defaultChunkSize :: ChunkSize
defaultChunkSize = ChunkSize 65504 -- 2^16 - 4 words overhead

{-# SPECIALISE hGetExactlyCRC32C_SBS :: HasFS IO h -> Handle h -> ByteCount -> CRC32C -> IO (SBS.ShortByteString, CRC32C) #-}
-- | Reads exactly as many bytes as requested, returning a 'ShortByteString' and
-- updating a given 'CRC32C' value.
--
-- If EOF is found before the requested number of bytes is read, an FsError
-- exception is thrown.
--
-- The returned 'ShortByteString' is backed by pinned memory.
hGetExactlyCRC32C_SBS ::
     forall m h. (MonadThrow m, PrimMonad m)
  => HasFS m h
  -> Handle h
  -> ByteCount -- ^ Number of bytes to read
  -> CRC32C
  -> m (SBS.ShortByteString, CRC32C)
hGetExactlyCRC32C_SBS hfs h !c !crc = do
    buf@(MutableByteArray !mba#) <- newPinnedByteArray (fromIntegral c)
    void $ hGetBufExactly hfs h buf 0 c
    (ByteArray !ba#) <- unsafeFreezeByteArray buf
    let fp = Foreign.ForeignPtr (byteArrayContents# ba#) (Foreign.PlainPtr (unsafeCoerce# mba#))
        !bs = BS.Internal.BS fp (fromIntegral c)
        !crc' = updateCRC32C bs crc
    pure (SBS.SBS ba#, crc')

{-# SPECIALISE hGetAllCRC32C' :: HasFS IO h -> Handle h -> ChunkSize -> CRC32C -> IO CRC32C #-}
-- | Reads all bytes, updating a given 'CRC32C' value without returning the
-- bytes.
hGetAllCRC32C' ::
     forall m h. PrimMonad m
  => HasFS m h
  -> Handle h
  -> ChunkSize -- ^ Chunk size, must be larger than 0
  -> CRC32C
  -> m CRC32C
hGetAllCRC32C' hfs h (ChunkSize !chunkSize) !crc0
  | chunkSize <= 0
  = error "hGetAllCRC32C': chunkSize must be >0"
  | otherwise
  = do
      buf@(MutableByteArray !mba#) <- newPinnedByteArray (fromIntegral chunkSize)
      (ByteArray !ba#) <- unsafeFreezeByteArray buf
      let fp = Foreign.ForeignPtr (byteArrayContents# ba#) (Foreign.PlainPtr (unsafeCoerce# mba#))
          !bs = BS.Internal.BS fp (fromIntegral chunkSize)
      go bs buf crc0
  where
    -- In particular, note that the "immutable" bs :: BS.ByteString aliases the
    -- mutable buf :: MutableByteArray. This is a bit hairy but we need to do
    -- something like this because the CRC code only works with ByteString.
    -- We thus have to be very careful about when bs is used.
    go :: BS.ByteString -> MutableByteArray (PrimState m) -> CRC32C -> m CRC32C
    go !bs buf !crc = do
      !n <- hGetBufSome hfs h buf 0 chunkSize
      if n == 0
        then return crc
        else do
          -- compute the update CRC value before reading the next bytes
          let !crc' = updateCRC32C (BS.take (fromIntegral n) bs) crc
          go bs buf crc'


{- $checksum-files
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
newtype ChecksumsFileName = ChecksumsFileName {unChecksumsFileName :: BSC.ByteString}
  deriving stock (Eq, Ord, Show)

{-# SPECIALISE
  getChecksum ::
     FsPath
  -> ChecksumsFile
  -> ChecksumsFileName
  -> IO CRC32C
  #-}
getChecksum ::
     MonadThrow m
  => FsPath
  -> ChecksumsFile
  -> ChecksumsFileName
  -> m CRC32C
getChecksum fsPath checksumsFile checksumsFileName =
  case Map.lookup checksumsFileName checksumsFile of
    Just checksum ->
      pure checksum
    Nothing ->
      throwIO . ErrFileFormatInvalid fsPath FormatChecksumsFile $
        "could not find checksum for " <> show (unChecksumsFileName checksumsFileName)

{-# SPECIALISE
  readChecksumsFile ::
       HasFS IO h
    -> FsPath
    -> IO ChecksumsFile
    #-}
readChecksumsFile ::
     (MonadThrow m)
  => HasFS m h
  -> FsPath
  -> m ChecksumsFile
readChecksumsFile fs path = do
    str <- withFile fs path ReadMode (\h -> hGetAll fs h)
    expectValidFile path FormatChecksumsFile (parseChecksumsFile (BSL.toStrict str))

{-# SPECIALISE writeChecksumsFile :: HasFS IO h -> FsPath -> ChecksumsFile -> IO () #-}
writeChecksumsFile :: MonadThrow m
                   => HasFS m h -> FsPath -> ChecksumsFile -> m ()
writeChecksumsFile fs path checksums =
    withFile fs path (WriteMode MustBeNew) $ \h -> do
      _ <- hPutAll fs h (formatChecksumsFile checksums)
      return ()

{-# SPECIALISE writeChecksumsFile' :: HasFS IO h -> Handle h -> ChecksumsFile -> IO () #-}
writeChecksumsFile' :: MonadThrow m
                    => HasFS m h -> Handle h -> ChecksumsFile -> m ()
writeChecksumsFile' fs h checksums = void $ hPutAll fs h (formatChecksumsFile checksums)

parseChecksumsFile :: BSC.ByteString -> Either String ChecksumsFile
parseChecksumsFile content =
    case partitionEithers (parseLines content) of
      ([], entries)    -> Right $! Map.fromList entries
      ((badline:_), _) -> Left $! "could not parse '" <> BSC.unpack badline <> "'"
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

{-------------------------------------------------------------------------------
  Checksum errors
-------------------------------------------------------------------------------}

-- | Check the CRC32C checksum for a file.
--
--   If the boolean argument is @True@, all file data for this path is evicted
--   from the page cache.
{-# SPECIALISE
  checkCRC ::
       HasFS IO h
    -> HasBlockIO IO h
    -> Bool
    -> CRC32C
    -> FsPath
    -> IO ()
  #-}
checkCRC ::
     forall m h.
     (MonadMask m, PrimMonad m)
  => HasFS m h
  -> HasBlockIO m h
  -> Bool
  -> CRC32C
  -> FsPath
  -> m ()
checkCRC fs hbio dropCache expected fp = withFile fs fp ReadMode $ \h -> do
  -- double the file readahead window (only applies to this file descriptor)
  hAdviseAll hbio h AdviceSequential
  !checksum <- hGetAllCRC32C' fs h defaultChunkSize initialCRC32C
  when dropCache $ hDropCacheAll hbio h
  expectChecksum fp expected checksum

{-# SPECIALISE
  expectChecksum ::
     FsPath
  -> CRC32C
  -> CRC32C
  -> IO ()
  #-}
expectChecksum ::
     MonadThrow m
  => FsPath
  -> CRC32C
  -> CRC32C
  -> m ()
expectChecksum fp expected checksum =
    when (expected /= checksum) $
      throwIO $ ErrFileChecksumMismatch fp (unCRC32C expected) (unCRC32C checksum)


{-------------------------------------------------------------------------------
  File Format Errors
-------------------------------------------------------------------------------}

data FileFormat
    = FormatChecksumsFile
    | FormatBloomFilterFile
    | FormatIndexFile
    | FormatWriteBufferFile
    | FormatSnapshotMetaData
    deriving stock (Show, Eq)

-- | The file is corrupted.
data FileCorruptedError
    = -- | The file fails to parse.
      ErrFileFormatInvalid
        -- | File.
        !FsPath
        -- | File format.
        !FileFormat
        -- | Error message.
        !String
    | -- | The file CRC32 checksum is invalid.
      ErrFileChecksumMismatch
        -- | File.
        !FsPath
        -- | Expected checksum.
        !Word32
        -- | Actual checksum.
        !Word32
    deriving stock (Show, Eq)
    deriving anyclass (Exception)

{-# SPECIALISE
  expectValidFile ::
      (MonadThrow m)
    => FsPath
    -> FileFormat
    -> Either String a
    -> m a
  #-}
expectValidFile ::
     (MonadThrow m)
  => FsPath
  -> FileFormat
  -> Either String a
  -> m a
expectValidFile _file _format (Right x) =
    pure x
expectValidFile file format (Left msg) =
    throwIO $ ErrFileFormatInvalid file format msg

