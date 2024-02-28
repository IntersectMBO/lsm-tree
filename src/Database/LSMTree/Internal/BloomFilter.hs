{-# LANGUAGE BangPatterns #-}
module Database.LSMTree.Internal.BloomFilter (
  bloomFilterToBuilder,
  bloomFilterFromSBS,
) where

import           Control.Monad (when)
import qualified Data.BloomFilter as BF
import qualified Data.BloomFilter.BitVec64 as BV64
import qualified Data.ByteString.Builder as B
import           Data.ByteString.Short (ShortByteString (SBS))
import qualified Data.Primitive as P
import           Data.Primitive.ByteArray (ByteArray (ByteArray))
import qualified Data.Vector.Primitive as PV
import           Data.Word (Word32, Word64)
import           Database.LSMTree.Internal.BitMath
import           Database.LSMTree.Internal.ByteString (byteArrayFromTo)

-- serializing
-----------------------------------------------------------

bloomFilterVersion :: Word32
bloomFilterVersion = 0

bloomFilterEndianess :: Word32
bloomFilterEndianess = 0  -- little endian

bloomFilterToBuilder :: BF.Bloom a -> B.Builder
bloomFilterToBuilder bf =
    B.word32LE bloomFilterVersion <>
    B.word32LE (fromIntegral (BF.hashesN bf)) <>
    B.word32LE (fromIntegral (BF.length bf)) <>
    B.word32LE bloomFilterEndianess <>
    toBuilder' bf

toBuilder' :: BF.Bloom a -> B.Builder
toBuilder' (BF.B _hfN _len (BV64.BV64 (PV.Vector off len v))) = byteArrayFromTo (mul8 off) (mul8 off + mul8 len) v

-- deserializing
-----------------------------------------------------------

-- | Read 'BF.Bloom' from a 'ShortByteString'.
--
-- In successful case the data portion of bloom filter is /not/ copied (the short bytestring has only 16 bytes of extra data in the header).
--
bloomFilterFromSBS :: ShortByteString -> Either String (BF.Bloom a)
bloomFilterFromSBS (SBS ba') = do
    when (P.sizeofByteArray ba < 16) $ Left "doesn't contain a header"
    -- on big endian platforms we'd need to byte-swap
    let ver = P.indexPrimArray word32pa 0
        hsn = P.indexPrimArray word32pa 1
        len = P.indexPrimArray word32pa 2 -- length in bits
        end = P.indexPrimArray word32pa 3

    when (ver /= bloomFilterVersion) $ Left "Unsupported version"
    when (end /= bloomFilterEndianess) $ Left "Non-matching endianess"
    when (mod64 len /= 0) $ Left "Length is not multiple of 64"

    let vec64 :: PV.Vector Word64
        vec64 = PV.Vector 2 (fromIntegral $ div64 len) ba

    return (BF.B (fromIntegral hsn) (fromIntegral len) (BV64.BV64 vec64))
  where
    ba :: ByteArray
    ba = ByteArray ba'

    word32pa :: P.PrimArray Word32
    word32pa = P.PrimArray ba'
