{-# LANGUAGE BangPatterns               #-}
{-# LANGUAGE CPP                        #-}
{-# LANGUAGE DerivingStrategies         #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE InstanceSigs               #-}
{-# LANGUAGE MagicHash                  #-}
{-# LANGUAGE TypeFamilies               #-}
{-# LANGUAGE UnboxedTuples              #-}
{- HLINT ignore "Redundant lambda" -}

#include <MachDeps.h>

-- See Note: [Export structure]
module Database.LSMTree.Internal.Serialise.RawBytes (
    -- * Raw bytes
    RawBytes (..)
  , mkRawBytes
    -- * Accessors
    -- ** Length information
  , sizeofRawBytes
    -- ** Extracting subvectors (slicing)
  , take
  , topBits16
  , sliceBits32
    -- * Construction
    -- ** Concatenation
  , (++)
  , concat
    -- * Conversions
    -- ** Lists
  , pack
  , unpack
    -- * @bytestring@ utils
  , fromByteString
  , unsafeFromByteString
  , toByteString
  , fromShortByteString
  , rawBytes
  ) where

import           Control.DeepSeq
import           Control.Exception (assert)
import           Data.Bits (Bits (shiftL, shiftR))
import           Data.BloomFilter.Hash (hashList32)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as BB
import qualified Data.ByteString.Builder.Internal as BB
import           Data.ByteString.Internal as BS.Internal
import           Data.ByteString.Short (ShortByteString (SBS))
import qualified Data.ByteString.Short as SBS
import qualified Data.ByteString.Short.Internal as SBS
import           Data.Primitive.ByteArray (ByteArray (..), compareByteArrays)
import qualified Data.Vector.Primitive as P
import           Database.LSMTree.Internal.Run.BloomFilter (Hashable (..))
import           Foreign.Ptr
import           GHC.Exts
import           GHC.ForeignPtr as GHC
import           GHC.Word
import           Prelude hiding (concat, take, (++))

{- Note: [Export structure]
   ~~~~~~~~~~~~~~~~~~~~~~~
   Since RawBytes are very similar to Primitive Vectors, the code is sectioned
   and structured much like the "Data.Vector.Primitive" module.
-}

{-------------------------------------------------------------------------------
  Raw bytes
-------------------------------------------------------------------------------}

-- | Raw bytes with no alignment constraint (i.e. byte aligned), and no
-- guarantee of pinned or unpinned memory (i.e. could be either).
newtype RawBytes = RawBytes (P.Vector Word8)
  deriving newtype (Show, NFData)

mkRawBytes :: Int -> Int -> ByteArray -> RawBytes
mkRawBytes off len ba = RawBytes (P.Vector off len ba)

instance Eq RawBytes where
  bs1 == bs2 = compareBytes bs1 bs2 == EQ

-- | Lexicographical 'Ord' instance.
instance Ord RawBytes where
  compare = compareBytes

-- | Based on @Ord 'ShortByteString'@.
compareBytes :: RawBytes -> RawBytes -> Ordering
compareBytes rb1@(RawBytes vec1) rb2@(RawBytes vec2) =
    let !len1 = sizeofRawBytes rb1
        !len2 = sizeofRawBytes rb2
        !len  = min len1 len2
     in case compareByteArrays ba1 off1 ba2 off2 len of
          EQ | len1 < len2 -> LT
             | len1 > len2 -> GT
          o  -> o
  where
    P.Vector off1 _size1 ba1 = vec1
    P.Vector off2 _size2 ba2 = vec2

instance Hashable RawBytes where
  hashIO32 :: RawBytes -> Word32 -> IO Word32
  hashIO32 = hashRawBytes

-- TODO: optimisation
hashRawBytes :: RawBytes -> Word32 -> IO Word32
hashRawBytes (RawBytes vec) = hashList32 (P.toList vec)

instance IsList RawBytes where
  type Item RawBytes = Word8

  fromList :: [Item RawBytes] -> RawBytes
  fromList = pack

  toList :: RawBytes -> [Item RawBytes]
  toList = unpack

{-------------------------------------------------------------------------------
  Accessors
-------------------------------------------------------------------------------}

sizeofRawBytes :: RawBytes -> Int
sizeofRawBytes = coerce P.length

take :: Int -> RawBytes -> RawBytes
take = coerce P.take

-- | @'topBits16' n rb@ slices the first @n@ bits from the /top/ of the raw
-- bytes @rb@. Returns the string of bits as a 'Word16'.
--
-- The /top/ corresponds to the most significant bit (big-endian).
--
-- PRECONDITION: @n >= 0 && n <= 16. We can slice out at most 16 bits,
-- all bits beyond that are truncated.
--
-- PRECONDITION: The byte-size of the raw bytes should be at least 2 bytes.
--
-- TODO: optimisation ideas: use unsafe shift/byteswap primops, look at GHC
-- core, find other opportunities for using primops.
--
topBits16 :: Int -> RawBytes -> Word16
topBits16 n rb@(RawBytes (P.Vector (I# off#) _size (ByteArray k#))) =
    assert (sizeofRawBytes rb >= 2) $ shiftR w16 (16 - n)
  where
    w16 = toWord16 (indexWord8ArrayAsWord16# k# off#)

toWord16 :: Word16# -> Word16
#if WORDS_BIGENDIAN
toWord16 = W16#
#else
toWord16 x# = byteSwap16 (W16# x#)
#endif

-- | @'sliceBits32' off rb@ slices from the raw bytes @rb@ a string of @32@
-- bits, starting at the @0@-based offset @off@. Returns the string of bits as a
-- 'Word32'.
--
-- Offsets are counted in bits from the /top/: offset @0@ corresponds to the
-- most significant bit (big-endian).
--
-- PRECONDITION: The raw bytes should be large enough that we can slice out 4
-- bytes after the bit-offset @off@, since we can only slice out bits that are
-- within the bounds of the byte array.
--
-- TODO: optimisation ideas: use unsafe shift/byteswap primops, look at GHC
-- core, find other opportunities for using primops.
--
sliceBits32 :: Int -> RawBytes -> Word32
sliceBits32 off@(I# off1#) rb@(RawBytes (P.Vector (I# off2#) _size (ByteArray ba#)))
    | 0# <- r#
    = assert (off + 32 <= 8 * sizeofRawBytes rb) $
      toWord32 (indexWord8ArrayAsWord32# ba# q#)
    | otherwise
    = assert (off + 32 <= 8 * sizeofRawBytes rb) $
        toWord32 (indexWord8ArrayAsWord32# ba# q#       ) `shiftL` r
      + w8w32#   (indexWord8Array#         ba# (q# +# 4#)) `shiftR` (8 - r)
  where
    !(# q0#, r# #) = quotRemInt# off1# 8#
    !q#            = q0# +# off2#
    r              = I# r#
    -- No need for byteswapping here
    w8w32# x#     = W32# (wordToWord32# (word8ToWord# x#))

toWord32 :: Word32# -> Word32
#if WORDS_BIGENDIAN
toWord32 = W32#
#else
toWord32 x# = byteSwap32 (W32# x#)
#endif

{-------------------------------------------------------------------------------
  Construction
-------------------------------------------------------------------------------}

infixr 5 ++

(++) :: RawBytes -> RawBytes -> RawBytes
(++) = coerce (P.++)

concat :: [RawBytes] -> RawBytes
concat = coerce P.concat

{-------------------------------------------------------------------------------
  Conversions
-------------------------------------------------------------------------------}

pack :: [Word8] -> RawBytes
pack = coerce P.fromList

unpack :: RawBytes -> [Word8]
unpack = coerce P.toList

{-------------------------------------------------------------------------------
  @bytestring@ utils
-------------------------------------------------------------------------------}

-- | \( O(n) \) conversion from a strict bytestring to raw bytes.
fromByteString :: BS.ByteString -> RawBytes
fromByteString = fromShortByteString . SBS.toShort

-- | \( O(1) \) conversion from a strict bytestring to raw bytes.
unsafeFromByteString :: BS.ByteString -> RawBytes
unsafeFromByteString (BS.Internal.BS (GHC.ForeignPtr _ contents) n) =
    case contents of
      -- Strict bytestrings are allocated using 'mallocPlainForeignPtrBytes', so
      -- we are expecting a 'PlainPtr' here.
      PlainPtr mba# -> case unsafeFreezeByteArray# mba# realWorld# of
                   (# _, ba# #) -> RawBytes (P.Vector 0 n (ByteArray ba#))
      _            -> error "unsafeFromByteString: expected plain pointer"

-- | \( O(n) \) conversion from raw bytes to a bytestring.
toByteString :: RawBytes -> BS.ByteString
toByteString = BS.pack . toList

-- | \( O(1) \) conversion from a short bytestring to raw bytes.
fromShortByteString :: ShortByteString -> RawBytes
fromShortByteString sbs@(SBS ba#) =
    RawBytes (P.Vector 0 (SBS.length sbs) (ByteArray ba#))

{-# INLINE rawBytes #-}
rawBytes :: RawBytes -> BB.Builder
rawBytes (RawBytes (P.Vector off size (ByteArray ba#))) =
    shortByteStringFromTo off (off + size) (SBS ba#)

-- | Copy of 'SBS.shortByteString', but with bounds (unchecked)
{-# INLINE shortByteStringFromTo #-}
shortByteStringFromTo :: Int -> Int -> ShortByteString -> BB.Builder
shortByteStringFromTo = \i j sbs -> BB.builder $ shortByteStringCopyStepFromTo i j sbs

-- | Copy of 'SBS.shortByteStringCopyStep' but with bounds (unchecked)
{-# INLINE shortByteStringCopyStepFromTo #-}
shortByteStringCopyStepFromTo ::
  Int -> Int -> ShortByteString -> BB.BuildStep a -> BB.BuildStep a
shortByteStringCopyStepFromTo !ip0 !ipe0 !sbs k =
    go ip0 ipe0
  where
    go !ip !ipe (BB.BufferRange op ope)
      | inpRemaining <= outRemaining = do
          SBS.copyToPtr sbs ip op inpRemaining
          let !br' = BB.BufferRange (op `plusPtr` inpRemaining) ope
          k br'
      | otherwise = do
          SBS.copyToPtr sbs ip op outRemaining
          let !ip' = ip + outRemaining
          return $ BB.bufferFull 1 ope (go ip' ipe)
      where
        outRemaining = ope `minusPtr` op
        inpRemaining = ipe - ip
