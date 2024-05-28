{-# LANGUAGE BangPatterns   #-}
{-# LANGUAGE DeriveFunctor  #-}
{-# LANGUAGE NamedFieldPuns #-}
module Database.LSMTree.Internal.RawOverflowPage (
    RawOverflowPage,
    makeRawOverflowPage,
    unsafeMakeRawOverflowPage,
    rawOverflowPageRawBytes,
    rawOverflowPageToByteString,
    rawBytesToOverflowPages,
    pinnedByteArrayToOverflowPages,
    unpinnedByteArrayToOverflowPages,
) where

import           Control.DeepSeq (NFData (rnf))
import           Control.Monad (when)
import           Data.ByteString (ByteString)
import           Data.Primitive.ByteArray (ByteArray (..), copyByteArray,
                     fillByteArray, isByteArrayPinned, newPinnedByteArray,
                     runByteArray)
import qualified Data.Vector.Primitive as PV
import           Database.LSMTree.Internal.Assertions
import           Database.LSMTree.Internal.BitMath (roundUpToPageSize)
import           Database.LSMTree.Internal.RawBytes (RawBytes (..))
import qualified Database.LSMTree.Internal.RawBytes as RB

-------------------------------------------------------------------------------
-- RawOverflowPage type
-------------------------------------------------------------------------------

-- | When a key\/op pair is too large to fit in a single disk page, the
-- representation is split into a normal page, and one or more overflow pages.
-- The normal 'RawPage' follows the run page format, and contains the key,
-- optional blob reference and a prefix of the value, while the overflow pages
-- contain the suffix of a large value that didn't fit in the normal page.
--
-- Each overflow page is the same size as normal pages (currently 4096 only).
--
data RawOverflowPage = RawOverflowPage
                         !Int        -- ^ offset in Word8s.
                         !ByteArray
  deriving (Show)

-- | This invariant is the same as for 'RawPage', but there is no alignment
-- constraint. This is for two reasons: 1. we don't need alignment, because
-- the page has no structure that we need to read with aligned memory
-- operations; 2. we don't want alignment because we want to convert the suffix
-- of serialised values (as 'RawBytes') into a 'RawOverflowPage' without
-- copying, and a suffix can start at an arbitrary offset.
--
invariant :: RawOverflowPage -> Bool
invariant (RawOverflowPage off ba) = and
    [ isValidSlice off 4096 ba    -- valid bytearray slice for length 4096
    , isByteArrayPinned ba        -- bytearray must be pinned (for I/O)
    ]

instance NFData RawOverflowPage where
  rnf (RawOverflowPage _ _) = ()

-- | This instance assumes pages are 4096 bytes in size
instance Eq RawOverflowPage where
    r1 == r2 = rawOverflowPageRawBytes r1 == rawOverflowPageRawBytes r2

rawOverflowPageRawBytes :: RawOverflowPage -> RawBytes
rawOverflowPageRawBytes (RawOverflowPage off ba) =
    RB.fromByteArray off 4096 ba

-- | \( O(1) \) since we can avoid copying the pinned byte array.
rawOverflowPageToByteString :: RawOverflowPage -> ByteString
rawOverflowPageToByteString =
    RB.unsafePinnedToByteString . rawOverflowPageRawBytes

-- | Create a 'RawOverflowPage'.
--
-- The length must be 4096 or less.
--
-- This function will copy data if the byte array is not pinned, or the length
-- is strictly less than 4096.
--
makeRawOverflowPage
    :: ByteArray  -- ^ bytearray
    -> Int        -- ^ offset in bytes into the bytearray
    -> Int        -- ^ length in bytes, must be @>= 0 && <= 4096@
    -> RawOverflowPage
makeRawOverflowPage ba off len
    | len == 4096
    , let page = RawOverflowPage off ba
    , invariant page
    = page

    | otherwise
    = makeRawOverflowPageCopy ba off len

makeRawOverflowPageCopy
    :: ByteArray  -- ^ bytearray
    -> Int        -- ^ offset in bytes into the bytearray
    -> Int        -- ^ length in bytes
    -> RawOverflowPage
makeRawOverflowPageCopy ba off len =
    assert (isValidSlice off len ba && len <= 4096) $
    (\page -> assert (invariant page) page) $
    RawOverflowPage 0 $ runByteArray $ do
      mba <- newPinnedByteArray 4096
      let suffixlen = min 4096 len -- would only do anything with assertions off
      copyByteArray mba 0 ba off suffixlen
      when (suffixlen < 4096) $ fillByteArray mba suffixlen (4096-suffixlen) 0
      return mba

-- | Create a 'RawOverflowPage' without copying. The byte array and offset must
-- satisfy the invariant for 'RawOverflowPage'.
--
unsafeMakeRawOverflowPage
    :: ByteArray  -- ^ bytearray, must be pinned and contain 4096 bytes (after offset)
    -> Int        -- ^ offset in bytes
    -> RawOverflowPage
unsafeMakeRawOverflowPage ba off =
    assert (invariant page) page
  where
    page = RawOverflowPage off ba

-- | Convert 'RawBytes' representing the \"overflow\" part of a value into one
-- or more 'RawOverflowPage's.
--
-- This will avoid copying where possible.
--
rawBytesToOverflowPages :: RawBytes -> [RawOverflowPage]
rawBytesToOverflowPages (RawBytes (PV.Vector off len ba))
  | isByteArrayPinned ba
  = pinnedByteArrayToOverflowPages off len ba
  | otherwise
  = unpinnedByteArrayToOverflowPages off len ba

pinnedByteArrayToOverflowPages :: Int -> Int -> ByteArray -> [RawOverflowPage]
pinnedByteArrayToOverflowPages !off !len !ba
      | len >= 4096
      , let !page = unsafeMakeRawOverflowPage ba off
      = page : pinnedByteArrayToOverflowPages (off+4096) (len-4096) ba

      | len <= 0
      = []

      | otherwise -- > 0 && < 4096
                  -- have to copy the partial last page
      , let !page = makeRawOverflowPageCopy ba off len
      = page : []

-- | Not pinned, in principle shouldn't happen much because if the value
-- is big enough to overflow then it's big enough to be pinned.
-- It is possible however if a page has a huge key and a small value.
--
-- Unfortunately, with GHC versions 9.6.x we also get this because the meaning
-- of pinned has changed. Sigh.
-- See <https://gitlab.haskell.org/ghc/ghc/-/issues/22255>
--
unpinnedByteArrayToOverflowPages :: Int -> Int -> ByteArray -> [RawOverflowPage]
unpinnedByteArrayToOverflowPages !off !len !ba =
    let !lenPages = roundUpToPageSize len
        ba'       = runByteArray $ do
                      mba <- newPinnedByteArray lenPages
                      copyByteArray mba 0 ba off len
                      fillByteArray mba len (lenPages-len) 0
                      return mba
        pages     = pinnedByteArrayToOverflowPages 0 lenPages ba'
        -- We've arranged to do the conversion without any extra copying,
        -- so assert that we got that right:
     in assert (all (isSliceNotCopy ba') pages)
        pages
  where
    isSliceNotCopy :: ByteArray -> RawOverflowPage -> Bool
    isSliceNotCopy ba1 (RawOverflowPage _ ba2) = sameByteArray ba1 ba2

