{-# LANGUAGE CPP             #-}
{-# LANGUAGE MagicHash       #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE UnboxedTuples   #-}
{-# OPTIONS_HADDOCK not-home #-}

module Database.LSMTree.Internal.BloomFilter (
  -- * Types
  Bloom.Bloom,
  Bloom.MBloom,
  Bloom.Salt,

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

#ifdef HAVE_STRICT_ARRAY
import qualified Database.LSMTree.Internal.StrictArray as P
#endif

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

{-# NOINLINE bloomQueries #-}
-- | Perform a batch of bloom queries. The result is a tuple of indexes into the
-- vector of runs and vector of keys respectively. The order of keys and
-- runs\/filters in the input is maintained in the output. This implementation
-- produces results in key-major order.
--
-- The result vector can be of variable length. The initial estimate is 2x the
-- number of keys but this is grown if needed (using a doubling strategy).
--
bloomQueries ::
     Bloom.Salt
  -> V.Vector (Bloom SerialisedKey)
  -> V.Vector SerialisedKey
  -> VP.Vector RunIxKeyIx
bloomQueries !_salt !filters !keys
  | V.null filters || V.null keys = VP.empty
bloomQueries !salt !filters !keys =
    runST (bloomQueries_loop1 filters' keyhashes)
  where
    filters'  = toFiltersArray filters
    keyhashes = P.generatePrimArray (V.length keys) $ \i ->
                  Bloom.hashesWithSalt salt (V.unsafeIndex keys i)

-- loop over all keys
bloomQueries_loop1 ::
     BloomFilters
  -> P.PrimArray (Bloom.Hashes SerialisedKey)
  -> ST s (VP.Vector RunIxKeyIx)
bloomQueries_loop1 !filters !keyhashes = do
    res <- P.newPrimArray (P.sizeofPrimArray keyhashes * 2)
    (res', resix') <- go res 0 0
    parr <- P.unsafeFreezePrimArray =<< P.resizeMutablePrimArray res' resix'
    pure $! P.primArrayToPrimVector parr
  where
    go !res !resix !kix
      | kix == P.sizeofPrimArray keyhashes = pure (res, resix)
      | otherwise = do
        let !keyhash = P.indexPrimArray keyhashes kix
        bloomQueries_loop2_prefetch filters keyhash 0
        (res', resix') <- bloomQueries_loop2 filters keyhash kix res resix 0
        go res' resix' (kix+1)

-- loop over all filters
bloomQueries_loop2 ::
     BloomFilters
  -> Bloom.Hashes SerialisedKey
  -> KeyIx
  -> P.MutablePrimArray s RunIxKeyIx
  -> ResIx
  -> RunIx
  -> ST s (P.MutablePrimArray s RunIxKeyIx, ResIx)
bloomQueries_loop2 !filters !keyhash !kix = go
  where
    go res2 resix2 rix
      | rix == lengthFiltersArray filters = pure (res2, resix2)
      | let !filter = indexFiltersArray filters rix
      , Bloom.elemHashes filter keyhash = do
          P.writePrimArray res2 resix2 (RunIxKeyIx rix kix)
          ressz2 <- P.getSizeofMutablePrimArray res2
          res2'  <- if resix2+1 < ressz2
                     then pure res2
                     else P.resizeMutablePrimArray res2 (ressz2 * 2)
          go res2' (resix2+1) (rix+1)

      | otherwise =
          go res2 resix2 (rix+1)

bloomQueries_loop2_prefetch ::
     BloomFilters
  -> Bloom.Hashes SerialisedKey
  -> RunIx
  -> ST s ()
bloomQueries_loop2_prefetch !filters !keyhash = go
  where
    go !rix
      | rix == lengthFiltersArray filters = pure ()
      | otherwise  = do
          let !filter = indexFiltersArray filters rix
          Bloom.prefetchElem filter keyhash
          go (rix+1)

type BloomFilters =
#ifdef HAVE_STRICT_ARRAY
  P.StrictArray (Bloom SerialisedKey)
#else
  V.Vector (Bloom SerialisedKey)
#endif

{-# INLINE toFiltersArray #-}
toFiltersArray :: V.Vector (Bloom SerialisedKey) -> BloomFilters

{-# INLINE indexFiltersArray #-}
indexFiltersArray :: BloomFilters -> Int -> Bloom SerialisedKey

{-# INLINE lengthFiltersArray #-}
lengthFiltersArray :: BloomFilters -> Int

#ifdef HAVE_STRICT_ARRAY
toFiltersArray     = P.vectorToStrictArray
indexFiltersArray  = P.indexStrictArray
lengthFiltersArray = P.sizeofStrictArray
#else
toFiltersArray     = id
indexFiltersArray  = V.unsafeIndex
lengthFiltersArray = V.length
#endif


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
    let (size, salt, ba, off, len) = Bloom.serialise bf
     in header size salt <> byteArrayToLBS ba off len
  where
    header Bloom.BloomSize { sizeBits, sizeHashes } salt =
        -- creates a single 24 byte chunk
        B.toLazyByteStringWith (B.safeStrategy 24 B.smallChunkSize) mempty $
             B.word32Host bloomFilterVersion
          <> B.word32Host (fromIntegral sizeHashes)
          <> B.word64Host (fromIntegral sizeBits)
          <> B.word64Host salt

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
              hGetByteArrayExactly hfs h 24

    let !version = P.indexByteArray header 0 :: Word32
        !nhashes = P.indexByteArray header 1 :: Word32
        !nbits   = P.indexByteArray header 1 :: Word64
        !salt    = P.indexByteArray header 2 :: Word64

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
        salt
        (\buf off len ->
            rethrowEOFError "bloom filter file too short" $
              void $ hGetBufExactly hfs
                       h buf (BufferOffset off) (fromIntegral len))

    -- check we're now at EOF
    trailing <- hGetSome hfs h 1
    when (not (BS.null trailing)) $
      throwFormatError "Byte array is too large for components"
    pure bloom
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

{-# SPECIALISE hGetByteArrayExactly ::
     HasFS IO h
  -> Handle h
  -> Int
  -> IO P.ByteArray #-}
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

