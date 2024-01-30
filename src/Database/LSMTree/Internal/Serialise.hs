{-# LANGUAGE BangPatterns               #-}
{-# LANGUAGE CPP                        #-}
{-# LANGUAGE DerivingStrategies         #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE InstanceSigs               #-}
{-# LANGUAGE MagicHash                  #-}
{-# LANGUAGE StandaloneKindSignatures   #-}
{-# LANGUAGE UnboxedTuples              #-}

#include <MachDeps.h>

-- | Temporary placeholders for serialisation.
module Database.LSMTree.Internal.Serialise (
    Serialise (..)
  , SerialisedKey (..)
  , topBits16
  , sliceBits32
  , sizeofKey
  , sizeofKey16
  , sizeofKey64
  , fromShortByteString
  ) where

import           Control.Exception (assert)
import           Data.Bits (Bits (shiftL, shiftR))
import           Data.BloomFilter.Hash (hashList32)
import           Data.ByteString.Short (ShortByteString (SBS))
import qualified Data.ByteString.Short as SBS
import           Data.Kind (Type)
import           Data.Primitive.ByteArray
import qualified Data.Vector.Primitive as P
import           Database.LSMTree.Internal.Run.BloomFilter (Hashable (..))
import           GHC.Exts
import           GHC.Word

-- | Serialisation into a 'SerialisedKey' (i.e., a 'ByteArray').
--
-- INVARIANT: Serialisation should preserve ordering, @x `compare` y ==
-- serialise x `compare` serialise y@. Serialised keys are lexicographically
-- ordered, in particular this means that values should be serialised into
-- big-endian formats.
class Serialise a where
  serialise :: a -> SerialisedKey

-- | Representation of a serialised key.
--
-- Serialisation should preserve equality and ordering. The 'Ord' instance for
-- 'SerialisedKey' uses lexicographical ordering.
type SerialisedKey :: Type
newtype SerialisedKey = SerialisedKey (P.Vector Word8)
  deriving newtype (Show)

instance Eq SerialisedKey where
  k1 == k2 = compareBytes k1 k2 == EQ

-- | Lexicographical 'Ord' instance.
instance Ord SerialisedKey where
  compare = compareBytes

-- | Based on @Ord 'ShortByteString'@.
compareBytes :: SerialisedKey -> SerialisedKey -> Ordering
compareBytes k1@(SerialisedKey vec1) k2@(SerialisedKey vec2) =
    let !len1 = sizeofKey k1
        !len2 = sizeofKey k2
        !len  = min len1 len2
     in case compareByteArrays ba1 off1 ba2 off2 len of
          EQ | len2 > len1 -> LT
             | len2 < len1 -> GT
          o  -> o
  where
    P.Vector off1 _size1 ba1 = vec1
    P.Vector off2 _size2 ba2 = vec2

instance Hashable SerialisedKey where
  hashIO32 :: SerialisedKey -> Word32 -> IO Word32
  hashIO32 = hashBytes

-- TODO: optimisation
hashBytes :: SerialisedKey -> Word32 -> IO Word32
hashBytes (SerialisedKey vec) = hashList32 (P.toList vec)

-- | @'topBits16' n k@ slices the first @n@ bits from the /top/ of the
-- serialised key @k@. Returns the string of bits as a 'Word16'.
--
-- The /top/ corresponds to the most significant bit (big-endian).
--
-- PRECONDITION: @n >= 0 && n <= 16. We can slice out at most 16 bits,
-- all bits beyond that are truncated.
--
-- PRECONDITION: The byte-size of the serialised key should be at least 2 bytes.
--
-- TODO: optimisation ideas: use unsafe shift/byteswap primops, look at GHC
-- core, find other opportunities for using primops.
--
topBits16 :: Int -> SerialisedKey -> Word16
topBits16 n k@(SerialisedKey (P.Vector (I# off#) _size (ByteArray k#))) =
    assert (sizeofKey k >= 2) $ shiftR w16 (16 - n)
  where
    w16 = toWord16 (indexWord8ArrayAsWord16# k# off#)

toWord16 :: Word16# -> Word16
#if WORDS_BIGENDIAN
toWord16 = W16#
#else
toWord16 x# = byteSwap16 (W16# x#)
#endif

-- | @'sliceBits32' off k@ slices from the serialised key @k@ a string of @32@
-- bits, starting at the @0@-based offset @off@. Returns the string of bits as a
-- 'Word32'.
--
-- Offsets are counted in bits from the /top/: offset @0@ corresponds to the
-- most significant bit (big-endian).
--
-- PRECONDITION: the byte-size @n@ of the serialised key should be at least 4
-- bytes.
--
-- PRECONDITION: The serialised key should be large enough that we can slice out
-- 4 bytes after of the bit-offset @off, since we can only slice out bits that
-- are within the bounds of the byte array.
--
-- TODO: optimisation ideas: use unsafe shift/byteswap primops, look at GHC
-- core, find other opportunities for using primops.
--
sliceBits32 :: Int -> SerialisedKey -> Word32
sliceBits32 off@(I# off1#) k@(SerialisedKey (P.Vector (I# off2#) _size (ByteArray k#)))
    | 0# <- r#
    = assert (off + 32 <= 8 * sizeofKey k) $
      toWord32 (indexWord8ArrayAsWord32# k# q#)
    | otherwise
    = assert (off + 32 <= 8 * sizeofKey k) $
        toWord32 (indexWord8ArrayAsWord32# k# q#       ) `shiftL` r
      + w8w32#   (indexWord8Array#         k# (q# +# 4#)) `shiftR` (8 - r)
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

-- | Size of key in number of bytes.
sizeofKey :: SerialisedKey -> Int
sizeofKey (SerialisedKey pvec) = P.length pvec

-- | Size of key in number of bytes.
sizeofKey16 :: SerialisedKey -> Word16
sizeofKey16 = fromIntegral . sizeofKey

-- | Size of key in number of bytes.
sizeofKey64 :: SerialisedKey -> Word64
sizeofKey64 = fromIntegral . sizeofKey

fromShortByteString :: ShortByteString -> SerialisedKey
fromShortByteString sbs@(SBS ba#) =
    SerialisedKey (P.Vector 0 (SBS.length sbs) (ByteArray ba#))
