{-# LANGUAGE MagicHash       #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE UnboxedTuples   #-}
{-# OPTIONS_HADDOCK not-home #-}

module Database.LSMTree.Internal.BloomFilter (
  -- * Types
  Bloom.Bloom,
  Bloom.MBloom,

  -- * Bulk query
  bloomQueries,
  RunIxKeyIx(RunIxKeyIx),
  RunIx, KeyIx,

  -- * Serialisation
  bloomFilterVersion,
  bloomFilterToLBS,
  bloomFilterFromFile,
) where

import           Data.Bits
import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder.Extra as B
import qualified Data.ByteString.Lazy as LBS
import qualified Data.Primitive as P
import qualified Data.Vector as V
import qualified Data.Vector.Primitive as VP
import           Data.Word (Word32, Word64, byteSwap32)

import           Control.Exception (assert)
import           Control.Monad (void, when)
import           Control.Monad.Class.MonadThrow
import           Control.Monad.Primitive (PrimMonad)
import           Control.Monad.ST (ST, runST)
import           System.FS.API

import           Data.BloomFilter.Blocked (Bloom)
import qualified Data.BloomFilter.Blocked as Bloom
import           Database.LSMTree.Internal.ByteString (byteArrayToByteString)
import           Database.LSMTree.Internal.CRC32C (FileCorruptedError (..),
                     FileFormat (..))
import           Database.LSMTree.Internal.Serialise (SerialisedKey)
import qualified Database.LSMTree.Internal.Vector as P

import           Prelude hiding (filter)

-- Bulk query
-----------------------------------------------------------

type KeyIx = Int
type RunIx = Int

-- | A 'RunIxKeyIx' is a (compact) pair of a 'RunIx' and a 'KeyIx'.
--
-- We represent it as a 32bit word, using:
--
-- * 16 bits for the run\/filter index (MSB)
-- * 16 bits for the key index (LSB)
--
newtype RunIxKeyIx = MkRunIxKeyIx Word32
  deriving stock Eq
  deriving newtype P.Prim

pattern RunIxKeyIx :: RunIx -> KeyIx -> RunIxKeyIx
pattern RunIxKeyIx r k <- (unpackRunIxKeyIx -> (r, k))
  where
    RunIxKeyIx r k = packRunIxKeyIx r k
{-# INLINE RunIxKeyIx #-}
{-# COMPLETE RunIxKeyIx #-}

packRunIxKeyIx :: Int -> Int -> RunIxKeyIx
packRunIxKeyIx r k =
    assert (r >= 0 && r <= 0xffff
         && k >= 0 && k <= 0xffff) $
    MkRunIxKeyIx $
      (fromIntegral :: Word -> Word32) $
        (fromIntegral r `unsafeShiftL` 16)
     .|. fromIntegral k
{-# INLINE packRunIxKeyIx #-}

unpackRunIxKeyIx :: RunIxKeyIx -> (Int, Int)
unpackRunIxKeyIx (MkRunIxKeyIx c) =
    ( fromIntegral (c `unsafeShiftR` 16)
    , fromIntegral (c .&. 0xfff)
    )
{-# INLINE unpackRunIxKeyIx #-}

instance Show RunIxKeyIx where
  showsPrec _ (RunIxKeyIx r k) =
    showString "RunIxKeyIx " . showsPrec 11 r
              . showChar ' ' . showsPrec 11 k

type ResIx = Int -- Result index

-- | Perform a batch of bloom queries. The result is a tuple of indexes into the
-- vector of runs and vector of keys respectively. The order of keys and
-- runs\/filters in the input is maintained in the output. This implementation
-- produces results in key-major order.
--
-- The result vector can be of variable length. The initial estimate is 2x the
-- number of keys but this is grown if needed (using a doubling strategy).
--
bloomQueries ::
     V.Vector (Bloom SerialisedKey)
  -> V.Vector SerialisedKey
  -> VP.Vector RunIxKeyIx
bloomQueries !filters !keys | V.null filters || V.null keys = VP.empty
bloomQueries !filters !keys =
    runST $ do
      res  <- P.newPrimArray (ksN * 2)
      res' <- loop1 res 0 0
      parr <- P.unsafeFreezePrimArray res'
      pure $! P.primArrayToPrimVector parr
  where
    !rsN = V.length filters
    !ksN = V.length keys
    !keyhashes = P.generatePrimArray (V.length keys) $ \i ->
                   Bloom.hashes (V.unsafeIndex keys i)

    -- loop over all filters
    loop1 ::
         P.MutablePrimArray s RunIxKeyIx
      -> ResIx
      -> RunIx
      -> ST s (P.MutablePrimArray s RunIxKeyIx)
    loop1 !res !resix !rix | rix == rsN = P.resizeMutablePrimArray res resix
    loop1 !res !resix !rix = do
        loop2_prefetch 0
        (res', resix') <- loop2 res resix 0
        loop1 res' resix' (rix+1)
      where
        !filter = V.unsafeIndex filters rix

        -- loop over all keys
        loop2 ::
             P.MutablePrimArray s RunIxKeyIx
          -> ResIx
          -> KeyIx
          -> ST s (P.MutablePrimArray s RunIxKeyIx, ResIx)
        loop2 !res2 !resix2 !kix
          | kix == ksN = pure (res2, resix2)
          | let !keyhash = P.indexPrimArray keyhashes kix
          , Bloom.elemHashes filter keyhash = do
              P.writePrimArray res2 resix2 (RunIxKeyIx rix kix)
              ressz2 <- P.getSizeofMutablePrimArray res2
              res2'  <- if resix2+1 < ressz2
                         then return res2
                         else P.resizeMutablePrimArray res2 (ressz2 * 2)
              loop2 res2' (resix2+1) (kix+1)

          | otherwise =
              loop2 res2 resix2 (kix+1)

        loop2_prefetch :: KeyIx -> ST s ()
        loop2_prefetch !kix
          | kix == ksN = pure ()
          | otherwise  = do
              let !keyhash = P.indexPrimArray keyhashes kix
              Bloom.prefetchElem filter keyhash
              loop2_prefetch (kix+1)

-- serialising
-----------------------------------------------------------

-- | By writing out the version in host endianness, we also indicate endianness.
-- During deserialisation, we would discover an endianness mismatch.
--
-- We base our version number on the 'Bloom.formatVersion' from the @bloomfilter@
-- library, plus our own version here. This accounts both for changes in the
-- format code here, and changes in the library.
--
bloomFilterVersion :: Word32
bloomFilterVersion = 1 + fromIntegral Bloom.formatVersion

bloomFilterToLBS :: Bloom a -> LBS.ByteString
bloomFilterToLBS bf =
    let (size, ba, off, len) = Bloom.serialise bf
     in header size <> byteArrayToLBS ba off len
  where
    header Bloom.BloomSize { sizeBits, sizeHashes } =
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
  -> Handle h
  -> IO (Bloom a) #-}
-- | Read a 'Bloom' from a file.
--
bloomFilterFromFile ::
     (PrimMonad m, MonadCatch m)
  => HasFS m h
  -> Handle h  -- ^ The open file, in read mode
  -> m (Bloom a)
bloomFilterFromFile hfs h = do
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
      Bloom.deserialise
        Bloom.BloomSize {
          Bloom.sizeBits   = fromIntegral nbits,
          Bloom.sizeHashes = fromIntegral nhashes
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
    throwFormatError = throwIO
                     . ErrFileFormatInvalid
                         (mkFsErrorPath hfs (handlePath h))
                         FormatBloomFilterFile
    rethrowEOFError msg =
      handleJust
        (\e -> if isFsErrorType FsReachedEOF e then Just e else Nothing)
        (\e -> throwIO $
                 ErrFileFormatInvalid
                   (fsErrorPath e) FormatBloomFilterFile msg)

hGetByteArrayExactly ::
     (PrimMonad m, MonadThrow m)
  => HasFS m h
  -> Handle h
  -> Int
  -> m P.ByteArray
hGetByteArrayExactly hfs h len = do
    buf <- P.newByteArray len
    _   <- hGetBufExactly hfs h buf 0 (fromIntegral len)
    P.unsafeFreezeByteArray buf

