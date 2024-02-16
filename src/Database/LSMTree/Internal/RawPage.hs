{-# LANGUAGE BangPatterns   #-}
{-# LANGUAGE NamedFieldPuns #-}
module Database.LSMTree.Internal.RawPage (
    RawPage,
    makeRawPage,
    rawPageNumKeys,
    rawPageNumBlobs,
    rawPageEntry,
    rawPageValue1Prefix,
    -- * Debug
    rawPageKeyOffsets,
    rawPageValueOffsets,
    rawPageValueOffsets1,
    rawPageHasBlobSpanAt,
    rawPageBlobSpanIndex,
    rawPageOpAt,
    rawPageKeys,
    rawPageValues,
) where

import           Control.DeepSeq (NFData (rnf))
import           Control.Exception (assert)
import           Data.Bits (Bits, complement, popCount, unsafeShiftL,
                     unsafeShiftR, (.&.))
import           Data.Primitive.ByteArray (ByteArray (..), indexByteArray,
                     sizeofByteArray)
import qualified Data.Vector as V
import qualified Data.Vector.Primitive as P
import           Data.Word (Word16, Word32, Word64, Word8)
import           Database.LSMTree.Internal.BlobRef (BlobSpan (..))
import           Database.LSMTree.Internal.Entry (Entry (..))
import           GHC.List (foldl')

-------------------------------------------------------------------------------
-- RawPage type
-------------------------------------------------------------------------------

data RawPage = RawPage
    !Int        -- ^ offset in Word16s.
    !ByteArray
  deriving (Show)

instance NFData RawPage where
  rnf (RawPage _ _) = ()

-- | This instance assumes pages are 4096 bytes in size
instance Eq RawPage where
    RawPage off1 ba1 == RawPage off2 ba2 = v1 == v2
      where
        v1, v2 :: P.Vector Word16
        v1 = P.Vector off1 (min 2048 (div2 (sizeofByteArray ba1))) ba1
        v2 = P.Vector off2 (min 2048 (div2 (sizeofByteArray ba2))) ba2

makeRawPage
    :: ByteArray  -- ^ bytearray
    -> Int        -- ^ offset in bytes, must be 8byte aligned.
    -> RawPage
makeRawPage ba off = RawPage (div2 off) ba

-------------------------------------------------------------------------------
-- Lookup function
-------------------------------------------------------------------------------

rawPageEntry
    :: RawPage
    -> P.Vector Word8 -- ^ key
    -> Maybe (Entry (P.Vector Word8) BlobSpan)
rawPageEntry !page !key = bisect 0 (fromIntegral dirNumKeys)
  where
    !dirNumKeys = rawPageNumKeys page

    -- when to switch to linear scan
    -- this a tuning knob
    -- can be set to zero.
    threshold = 3

    found :: Int -> Maybe (Entry (P.Vector Word8) BlobSpan)
    found !i = Just $! case rawPageOpAt page i of
        0 -> if rawPageHasBlobSpanAt page i == 0
             then Insert (rawPageValueAt page i)
             else InsertWithBlob (rawPageValueAt page i) (rawPageBlobSpanIndex page (rawPageCalculateBlobIndex page i))
        1 -> Mupdate (rawPageValueAt page i)
        _ -> Delete

    bisect :: Int -> Int -> Maybe (Entry (P.Vector Word8) BlobSpan)
    bisect !i !j
        | j - i < threshold = linear i j
        | otherwise = case compare key (rawPageKeyAt page k) of
            EQ -> found k
            GT -> bisect (k + 1) j
            LT -> bisect i k
      where
        k = i + div2 (j - i)

    linear :: Int -> Int -> Maybe (Entry (P.Vector Word8) BlobSpan)
    linear !i !j
        | i >= j                       = Nothing
        | key == rawPageKeyAt page i   = found i
        | otherwise                    = linear (i + 1) j

rawPageValue1Prefix :: RawPage -> Entry (P.Vector Word8, Word32) BlobSpan
rawPageValue1Prefix page = case rawPageOpAt page 0 of
  0 -> if rawPageHasBlobSpanAt page 0 == 0
       then Insert (rawPageSingleValue page)
       else InsertWithBlob (rawPageSingleValue page) (rawPageBlobSpanIndex page (rawPageCalculateBlobIndex page 0))
  1 -> Mupdate (rawPageSingleValue page)
  _ -> Delete

-------------------------------------------------------------------------------
-- Accessors
-------------------------------------------------------------------------------

-- Note: indexByteArray's offset is given in elements of type a rather than in bytes.

rawPageNumKeys :: RawPage -> Word16
rawPageNumKeys (RawPage off ba) = indexByteArray ba off

rawPageNumBlobs :: RawPage -> Word16
rawPageNumBlobs (RawPage off ba) = indexByteArray ba (off + 1)

rawPageKeysOffset :: RawPage -> Word16
rawPageKeysOffset (RawPage off ba) = indexByteArray ba (off + 2)

type KeyOffset = Word16
type ValueOffset = Word16

rawPageKeyOffsets :: RawPage -> P.Vector KeyOffset
rawPageKeyOffsets page@(RawPage off ba) =
    P.Vector (off + fromIntegral (div2 dirOffset))
             (fromIntegral dirNumKeys + 1) ba
  where
    !dirNumKeys = rawPageNumKeys page
    !dirOffset  = rawPageKeysOffset page

-- | for non-single key page case
rawPageValueOffsets :: RawPage -> P.Vector ValueOffset
rawPageValueOffsets page@(RawPage off ba) =
    assert (dirNumKeys /= 1) $
    P.Vector (off + fromIntegral (div2 dirOffset) + fromIntegral dirNumKeys)
             (fromIntegral dirNumKeys + 1) ba
  where
    !dirNumKeys = rawPageNumKeys page
    !dirOffset  = rawPageKeysOffset page

-- | single key page case
rawPageValueOffsets1 :: RawPage -> (Word16, Word32)
rawPageValueOffsets1 page@(RawPage off ba) =
    assert (rawPageNumKeys page == 1) $
    ( indexByteArray ba (off + fromIntegral (div2 dirOffset) + 1)
    , indexByteArray ba (div2 (off + fromIntegral (div2 dirOffset)) + 1)
    )
  where
    !dirOffset = rawPageKeysOffset page

rawPageHasBlobSpanAt :: RawPage -> Int -> Word64
rawPageHasBlobSpanAt _page@(RawPage off ba) i = do
    let j = unsafeShiftR i 6 -- `div` 64
    let k = i .&. 63         -- `mod` 64
    let word = indexByteArray ba (div4 off + 1 + j)
    unsafeShiftR word k .&. 1

rawPageOpAt :: RawPage -> Int -> Word64
rawPageOpAt page@(RawPage off ba) i = do
    let j = unsafeShiftR i 5 -- `div` 32
    let k = i .&. 31         -- `mod` 32
    let word = indexByteArray ba (div4 off + 1 + roundUpTo64 (fromIntegral dirNumKeys) + j)
    unsafeShiftR word (mul2 k) .&. 3
  where
    !dirNumKeys = rawPageNumKeys page

roundUpTo64 :: Int -> Int
roundUpTo64 i = unsafeShiftR (i + 63) 6

rawPageKeys :: RawPage -> V.Vector (P.Vector Word8)
rawPageKeys page@(RawPage off ba) = do
    let offs = rawPageKeyOffsets page
    V.fromList
        [ P.Vector (mul2 off + start) (end - start) ba
        | i <- [ 0 .. fromIntegral dirNumKeys -  1 ] :: [Int]
        , let start = fromIntegral (P.unsafeIndex offs i) :: Int
        , let end   = fromIntegral (P.unsafeIndex offs (i + 1)) :: Int
        ]
  where
    !dirNumKeys = rawPageNumKeys page

rawPageKeyAt :: RawPage -> Int -> P.Vector Word8
rawPageKeyAt page@(RawPage off ba) i = do
    P.Vector (mul2 off + start) (end - start) ba
  where
    offs  = rawPageKeyOffsets page
    start = fromIntegral (P.unsafeIndex offs i) :: Int
    end   = fromIntegral (P.unsafeIndex offs (i + 1)) :: Int

-- | Non-single page case
rawPageValues :: RawPage -> V.Vector (P.Vector Word8)
rawPageValues page@(RawPage off ba) = do
    let offs = rawPageValueOffsets page
    V.fromList
        [ P.Vector (mul2 off + start) (end - start) ba
        | i <- [ 0 .. fromIntegral dirNumKeys -  1 ] :: [Int]
        , let start = fromIntegral (P.unsafeIndex offs i) :: Int
        , let end   = fromIntegral (P.unsafeIndex offs (i + 1)) :: Int
        ]
  where
    !dirNumKeys = rawPageNumKeys page

rawPageValueAt :: RawPage -> Int -> P.Vector Word8
rawPageValueAt page@(RawPage off ba) i = do
    P.Vector (mul2 off + start) (end - start) ba
  where
    offs  = rawPageValueOffsets page
    start = fromIntegral (P.unsafeIndex offs i) :: Int
    end   = fromIntegral (P.unsafeIndex offs (i + 1)) :: Int

rawPageSingleValue :: RawPage -> (P.Vector Word8, Word32)
rawPageSingleValue page@(RawPage off ba) =
    if end > 4096
    then (P.Vector (mul2 off + fromIntegral start) (4096 - fromIntegral start) ba, end - 4096)
    else (P.Vector (mul2 off + fromIntegral start) (fromIntegral end - fromIntegral start) ba, 0)
  where
    (start, end) = rawPageValueOffsets1 page

-- we could create unboxed array.
{-# INLINE rawPageBlobSpanIndex #-}
rawPageBlobSpanIndex :: RawPage
    -> Int -- ^ blobspan index. Calculate with 'rawPageCalculateBlobIndex'
    -> BlobSpan
rawPageBlobSpanIndex page@(RawPage off ba) i = BlobSpan
    ( indexByteArray ba (off1 + i) )
    ( indexByteArray ba (off2 + i) )
  where
    !dirNumKeys  = rawPageNumKeys page
    !dirNumBlobs = rawPageNumBlobs page

    -- offset to start of blobspan arr
    off1 = div4 off + 1 + roundUpTo64 (fromIntegral dirNumKeys) + roundUpTo64 (fromIntegral (mul2 dirNumKeys))
    off2 = mul2 (off1 + fromIntegral dirNumBlobs)

rawPageCalculateBlobIndex
    :: RawPage
    -> Int  -- ^ key index
    -> Int  -- ^ blobspan index
rawPageCalculateBlobIndex (RawPage off ba) i = do
    let j = unsafeShiftR i 6 -- `div` 64
    let k = i .&. 63         -- `mod` 64
    -- generic sum isn't too great
    let s = foldl' (+) 0 [ popCount (indexByteArray ba (div4 off + 1 + jj) :: Word64) | jj <- [0 .. j-1 ] ]
    let word = indexByteArray ba (div4 off + 1 + j) :: Word64
    s + popCount (word .&. complement (unsafeShiftL 0xffffffffffffffff k))

-------------------------------------------------------------------------------
-- Utils
-------------------------------------------------------------------------------

div2 :: Bits a => a -> a
div2 x = unsafeShiftR x 1

mul2 :: Bits a => a -> a
mul2 x = unsafeShiftL x 1

div4 :: Bits a => a -> a
div4 x = unsafeShiftR x 2
