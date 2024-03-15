{-# LANGUAGE NumericUnderscores #-}
module Database.LSMTree.Internal.BloomFilter (
  bloomFilterToBuilder,
  bloomFilterFromSBS,
) where

import           Control.Monad (when)
import qualified Data.BloomFilter as BF
import qualified Data.BloomFilter.BitVec64 as BV64
import qualified Data.BloomFilter.Internal as BF
import qualified Data.ByteString.Builder as B
import qualified Data.ByteString.Builder.Extra as B
import           Data.ByteString.Short (ShortByteString (SBS))
import qualified Data.Primitive as P
import           Data.Primitive.ByteArray (ByteArray (ByteArray))
import qualified Data.Vector.Primitive as PV
import           Data.Word (Word32, Word64, byteSwap32)
import           Database.LSMTree.Internal.BitMath
import           Database.LSMTree.Internal.ByteString (byteArrayFromTo)

-- serializing
-----------------------------------------------------------

-- | By writing out the version in host endianness, we also indicate endianness.
-- During deserialisation, we would discover an endianness mismatch.
bloomFilterVersion :: Word32
bloomFilterVersion = 1

bloomFilterToBuilder :: BF.Bloom a -> B.Builder
bloomFilterToBuilder bf =
    B.word32Host bloomFilterVersion <>
    B.word32Host (fromIntegral (BF.hashesN bf)) <>
    B.word64Host (BF.length bf) <>
    toBuilder' bf

toBuilder' :: BF.Bloom a -> B.Builder
toBuilder' (BF.Bloom _hfN _len (BV64.BV64 (PV.Vector off len v))) =
    byteArrayFromTo (mul8 off) (mul8 off + mul8 len) v

-- deserializing
-----------------------------------------------------------

-- | Read 'BF.Bloom' from a 'ShortByteString'.
--
-- In successful case the data portion of bloom filter is /not/ copied (the short bytestring has only 16 bytes of extra data in the header).
--
bloomFilterFromSBS :: ShortByteString -> Either String (BF.Bloom a)
bloomFilterFromSBS (SBS ba') = do
    when (P.sizeofByteArray ba < 16) $ Left "Doesn't contain a header"

    let ver = P.indexPrimArray word32pa 0
        hsn = P.indexPrimArray word32pa 1
        len = P.indexPrimArray word64pa 1 -- length in bits

    when (ver /= bloomFilterVersion) $ Left $
      if byteSwap32 ver == bloomFilterVersion
      then "Different byte order"
      else "Unsupported version"

    when (len <= 0) $ Left "Length is zero"

    -- limit to 2^48 bits
    when (len >= 0xffff_ffff_ffff) $ Left "Too large bloomfilter"

    -- we need to round the size of vector up
    let vec64 :: PV.Vector Word64
        vec64 = PV.Vector 2 (fromIntegral $ ceilDiv64 len) ba

    return (BF.Bloom (fromIntegral hsn) len (BV64.BV64 vec64))
  where
    ba :: ByteArray
    ba = ByteArray ba'

    word32pa :: P.PrimArray Word32
    word32pa = P.PrimArray ba'

    word64pa :: P.PrimArray Word64
    word64pa = P.PrimArray ba'
