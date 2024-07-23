{-# LANGUAGE CPP                #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE MagicHash          #-}
{-# LANGUAGE TypeFamilies       #-}
{-# LANGUAGE UnboxedTuples      #-}
{- HLINT ignore "Redundant lambda" -}

#include <MachDeps.h>

-- |
--
-- This module is intended to be imported qualified, to avoid name clashes with
-- "Prelude" functions:
--
-- @
--   import Database.LSMTree.Internal.RawBytes (RawBytes (..))
--   import qualified Database.LSMTree.Internal.RawBytes as RB
-- @
module Database.LSMTree.Internal.RawBytes (
    -- See Note: [Export structure]
    -- * Raw bytes
    RawBytes (..)
    -- * Accessors
    -- ** Length information
  , size
    -- ** Extracting subvectors (slicing)
  , take
  , drop
  , topBits16
  , sliceBits64
    -- * Construction
    -- | Use 'Semigroup' and 'Monoid' operations
    -- ** Restricting memory usage
  , copy
  , force
    -- * Conversions
  , fromVector
  , fromByteArray
    -- ** Lists
  , pack
  , unpack
    -- * @bytestring@ utils
  , fromByteString
  , unsafeFromByteString
  , toByteString
  , unsafePinnedToByteString
  , fromShortByteString
  , builder
  ) where

import           Control.DeepSeq (NFData)
import           Control.Exception (assert)
import           Data.Bits (Bits (shiftL, shiftR))
import           Data.BloomFilter.Hash (Hashable (..), hashByteArray)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as BB
import           Data.ByteString.Short (ShortByteString (SBS))
import qualified Data.ByteString.Short as SBS
import           Data.Primitive.ByteArray (ByteArray (..), compareByteArrays)
import qualified Data.Primitive.ByteArray as BA
import qualified Data.Vector.Primitive as VP
import           Database.LSMTree.Internal.ByteString (byteArrayToByteString,
                     shortByteStringFromTo, tryGetByteArray,
                     unsafePinnedByteArrayToByteString)
import           Database.LSMTree.Internal.Vector
import           Prelude hiding (drop, take)

import           GHC.Exts
import           GHC.Stack
import           GHC.Word

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
newtype RawBytes = RawBytes (VP.Vector Word8)
  deriving newtype (Show, NFData)

instance Eq RawBytes where
  bs1 == bs2 = compareBytes bs1 bs2 == EQ

-- | Lexicographical 'Ord' instance.
instance Ord RawBytes where
  compare = compareBytes

-- | Based on @Ord 'ShortByteString'@.
compareBytes :: RawBytes -> RawBytes -> Ordering
compareBytes rb1@(RawBytes vec1) rb2@(RawBytes vec2) =
    let !len1 = size rb1
        !len2 = size rb2
        !len  = min len1 len2
     in case compareByteArrays ba1 off1 ba2 off2 len of
          EQ | len1 < len2 -> LT
             | len1 > len2 -> GT
          o  -> o
  where
    VP.Vector off1 _size1 ba1 = vec1
    VP.Vector off2 _size2 ba2 = vec2

instance Hashable RawBytes where
  hashSalt64 :: Word64 -> RawBytes -> Word64
  hashSalt64 = hash

hash :: Word64 -> RawBytes -> Word64
hash salt (RawBytes (VP.Vector off len ba)) = hashByteArray ba off len salt

instance IsList RawBytes where
  type Item RawBytes = Word8

  fromList :: [Item RawBytes] -> RawBytes
  fromList = pack

  toList :: RawBytes -> [Item RawBytes]
  toList = unpack

-- | Mostly to make test cases shorter to write.
instance IsString RawBytes where
    fromString = pack . map (fromIntegral . fromEnum)

{-------------------------------------------------------------------------------
  Accessors
-------------------------------------------------------------------------------}

-- | \( O(1) \)
size :: RawBytes -> Int
size = coerce VP.length

-- | \( O(1) \)
take :: Int -> RawBytes -> RawBytes
take = coerce VP.take

-- | \( O(1) \)
drop :: Int -> RawBytes -> RawBytes
drop = coerce VP.drop

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
topBits16 n rb@(RawBytes (VP.Vector (I# off#) _size (ByteArray k#))) =
    assert (size rb >= 2) $ shiftR w16 (16 - n)
  where
    w16 = toWord16 (indexWord8ArrayAsWord16# k# off#)

toWord16 :: Word16# -> Word16
#if WORDS_BIGENDIAN
toWord16 = W16#
#else
toWord16 x# = byteSwap16 (W16# x#)
#endif

-- | @'sliceBits64' off rb@ slices from the raw bytes @rb@ a string of @64@
-- bits, starting at the @0@-based offset @off@. Returns the string of bits as a
-- 'Word64'.
--
-- Offsets are counted in bits from the /top/: offset @0@ corresponds to the
-- most significant bit (big-endian).
--
-- PRECONDITION: The raw bytes should be large enough that we can slice out 8
-- bytes after the bit-offset @off@, since we can only slice out bits that are
-- within the bounds of the byte array.
--
-- TODO: optimisation ideas: use unsafe shift/byteswap primops, look at GHC
-- core, find other opportunities for using primops.
--
sliceBits64 :: Int -> RawBytes -> Word64
sliceBits64 off@(I# off1#) rb@(RawBytes (VP.Vector (I# off2#) _size (ByteArray ba#)))
    | 0# <- r#
    = assert (off + 64 <= 8 * size rb) $
      toWord64 (indexWord8ArrayAsWord64# ba# q#)
    | otherwise
    = assert (off + 64 <= 8 * size rb) $
        toWord64 (indexWord8ArrayAsWord64# ba# q#        ) `shiftL` r
      + ww64#    (indexWord8Array#         ba# (q# +# 8#)) `shiftR` (8 - r)
  where
    !(# q0#, r# #) = quotRemInt# off1# 8#
    !q#            = q0# +# off2#
    r              = I# r#

#if (MIN_VERSION_GLASGOW_HASKELL(9, 4, 0, 0))

toWord64 :: Word64# -> Word64
#if WORDS_BIGENDIAN
toWord64 = W64#
#else
toWord64 x# = byteSwap64 (W64# x#)
#endif

-- No need for byteswapping here
ww64# :: Word8# -> Word64
ww64# x#     = W64# (wordToWord64# (word8ToWord# x#))

#else

toWord64 :: Word# -> Word64
#if WORDS_BIGENDIAN
toWord64 = W64#
#else
toWord64 x# = byteSwap64 (W64# x#)
#endif

-- No need for byteswapping here
ww64# :: Word8# -> Word64
ww64# x# = W64# (word8ToWord# x#)

#endif

{-------------------------------------------------------------------------------
  Construction
-------------------------------------------------------------------------------}

instance Semigroup RawBytes where
    (<>) = coerce (VP.++)

instance Monoid RawBytes where
    mempty = coerce VP.empty
    mconcat = coerce VP.concat

-- | O(n) Yield the argument, but force it not to retain any extra memory by
-- copying it.
--
-- Useful when dealing with slices. Also, see
-- "Database.LSMTree.Internal.Unsliced"
copy :: RawBytes -> RawBytes
copy (RawBytes pvec) = RawBytes (VP.force pvec)

-- | Force 'RawBytes' to not retain any extra memory. This may copy the contents.
force :: RawBytes -> ByteArray
force (RawBytes (VP.Vector off len ba))
    | off == 0
    , BA.sizeofByteArray ba == len
    = ba

    | otherwise
    = BA.runByteArray $ do
        mba <- BA.newByteArray len
        BA.copyByteArray mba 0 ba off len
        return mba

{-------------------------------------------------------------------------------
  Conversions
-------------------------------------------------------------------------------}

-- | \( O(1) \)
fromVector :: VP.Vector Word8 -> RawBytes
fromVector v = RawBytes v

-- | \( O(1) \)
fromByteArray :: Int -> Int -> ByteArray -> RawBytes
fromByteArray off len ba = RawBytes (mkPrimVector off len ba)

pack :: [Word8] -> RawBytes
pack = coerce VP.fromList

unpack :: RawBytes -> [Word8]
unpack = coerce VP.toList

{-------------------------------------------------------------------------------
  @bytestring@ utils
-------------------------------------------------------------------------------}

-- | \( O(n) \) conversion from a strict bytestring to raw bytes.
fromByteString :: BS.ByteString -> RawBytes
fromByteString = fromShortByteString . SBS.toShort

-- | \( O(1) \) conversion from a strict bytestring to raw bytes.
--
-- Strict bytestrings are allocated using 'mallocPlainForeignPtrBytes', so we
-- are expecting a 'PlainPtr' (or 'FinalPtr' with length 0).
-- For other variants, this function will fail.
unsafeFromByteString :: HasCallStack => BS.ByteString -> RawBytes
unsafeFromByteString bs =
    case tryGetByteArray bs of
      Right (ba, n) -> RawBytes (mkPrimVector 0 n ba)
      Left err      -> error $ "unsafeFromByteString: " <> err

-- | \( O(1) \) conversion from raw bytes to a bytestring if pinned,
-- \( O(n) \) if unpinned.
toByteString :: RawBytes -> BS.ByteString
toByteString (RawBytes (VP.Vector off len ba)) =
    byteArrayToByteString off len ba

-- | \( O(1) \) conversion from raw bytes to a bytestring.
-- Fails if the underlying byte array is not pinned.
unsafePinnedToByteString :: HasCallStack => RawBytes -> BS.ByteString
unsafePinnedToByteString (RawBytes (VP.Vector off len ba)) =
    unsafePinnedByteArrayToByteString off len ba

-- | \( O(1) \) conversion from a short bytestring to raw bytes.
fromShortByteString :: ShortByteString -> RawBytes
fromShortByteString sbs@(SBS ba#) =
    RawBytes (mkPrimVector 0 (SBS.length sbs) (ByteArray ba#))

{-# INLINE builder #-}
builder :: RawBytes -> BB.Builder
builder (RawBytes (VP.Vector off sz (ByteArray ba#))) =
    shortByteStringFromTo off (off + sz) (SBS ba#)
