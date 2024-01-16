{-# LANGUAGE RecordWildCards #-}
module Database.LSMTree.Internal.RawPage (
    RawPage,
    makeRawPage,
    rawPageNumKeys,
    rawPageNumBlobs,
    -- * Debug
    rawPageKeyOffsets,
    rawPageBlobRefs,
) where

import           Data.Bits (Bits, unsafeShiftR)
import           Data.Primitive.ByteArray (ByteArray (..), indexByteArray,
                     sizeofByteArray)
import           Data.Vector.Primitive (Vector (..))
import           Data.Word (Word16)

import qualified Database.LSMTree.Internal.BitVec as BV

-------------------------------------------------------------------------------
-- RawPage type
-------------------------------------------------------------------------------

data RawPage = RawPage
    !Int        -- ^ offset in Word16s.
    !ByteArray
  deriving (Show)

-- | This instance assumes pages are 4096 bytes in size
instance Eq RawPage where
    RawPage off1 ba1 == RawPage off2 ba2 = v1 == v2
      where
        v1, v2 :: Vector Word16
        v1 = Vector off1 (min 2048 (div2 (sizeofByteArray ba1))) ba1
        v2 = Vector off2 (min 2048 (div2 (sizeofByteArray ba2))) ba2

makeRawPage
    :: ByteArray  -- ^ bytearray
    -> Int        -- ^ offset in bytes, must be 8byte aligned.
    -> RawPage
makeRawPage ba off = RawPage (div2 off) ba

-------------------------------------------------------------------------------
-- Accessors
-------------------------------------------------------------------------------

-- Note: indexByteArray's offset is given in elements of type a rather than in bytes.

rawPageNumKeys :: RawPage -> Word16
rawPageNumKeys (RawPage off ba) = indexByteArray ba off

rawPageNumBlobs :: RawPage -> Word16
rawPageNumBlobs (RawPage off ba) = indexByteArray ba (off + 1)

type KeyOffset = Word16

rawPageKeyOffsets :: RawPage -> Vector KeyOffset
rawPageKeyOffsets page@(RawPage off ba) =
    Vector (off + fromIntegral (div2 dirOffset)) (fromIntegral dirNumKeys) ba
  where
    Dir {..} = rawPageDirectory page

rawPageBlobRefs :: RawPage -> BV.Vector BV.Bit
rawPageBlobRefs page@(RawPage off ba) =
    -- bitvec offset and length are in bits
    BV.BitVec (off * 16 + 64) (fromIntegral dirNumKeys) ba
  where
    Dir {..} = rawPageDirectory page

-------------------------------------------------------------------------------
-- Directory
-------------------------------------------------------------------------------

data Directory = Dir
    { dirNumKeys  :: !Word16
    , dirNumBlobs :: !Word16
    , dirOffset   :: !Word16
    }

rawPageDirectory :: RawPage -> Directory
rawPageDirectory (RawPage off ba) = Dir
    (indexByteArray ba off)
    (indexByteArray ba (off + 1))
    (indexByteArray ba (off + 2))

-------------------------------------------------------------------------------
-- Utils
-------------------------------------------------------------------------------

div2 :: Bits a => a -> a
div2 x = unsafeShiftR x 1
