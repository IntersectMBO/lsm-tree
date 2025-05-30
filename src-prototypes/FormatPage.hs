{-# LANGUAGE ParallelListComp #-}

-- | This accompanies the format-page.md documentation as a sanity check
-- and a precise reference. It is intended to demonstrate that the page
-- format works. It is also used as a reference implementation for tests of
-- the real implementation.
--
-- Logically, a page is a sequence of key,operation pairs (with optional
-- blobrefs), sorted by key, and its serialised form fits within a disk page.
--
-- This reference implementation covers serialisation and deserialisation
-- (not lookups) which do not rely on (or enforce) the keys being sorted.
--
module FormatPage (
    -- * Page types
    Key (..),
    Operation (..),
    opHasBlobRef,
    Value (..),
    BlobRef (..),
    PageSerialised,
    PageIntermediate (..),
    PageSizesOffsets (..),

    -- * Page size
    PageSize (..),
    pageSizeEmpty,
    pageSizeAddElem,
    calcPageSize,

    -- * Encoding and decoding
    DiskPageSize(..),
    diskPageSizeBytes,
    encodePage,
    decodePage,
    serialisePage,
    deserialisePage,
    fromBitmap,
    toBitmap,

    -- * Overflow pages
    pageOverflowPrefixSuffixLen,
    pageDiskPages,
    pageSerialisedChunks,

    -- * Generators and shrinkers
    PageContentFits (..),
    genPageContentFits,
    PageContentMaybeOverfull (..),
    genPageContentMaybeOverfull,
    PageContentSingle (..),
    genPageContentSingle,
    genPageContentNearFull,
    genPageContentMedium,
    MinKeySize(..),
    noMinKeySize,
    maxKeySize,
    orderdKeyOps,
    shrinkKeyOps,
    shrinkOrderedKeyOps,
) where

import           Data.Bits
import           Data.Function (on)
import qualified Data.List as List
import           Data.Maybe (fromJust, fromMaybe)
import           Data.Word

import qualified Data.Binary.Get as Bin
import           Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as BB
import qualified Data.ByteString.Char8 as BSC
import qualified Data.ByteString.Lazy as BSL

import           Control.Exception (assert)
import           Control.Monad

import           Test.QuickCheck hiding ((.&.))

-------------------------------------------------------------------------------
-- Page content types
--

newtype Key   = Key   { unKey   :: ByteString } deriving stock (Eq, Ord, Show)
newtype Value = Value { unValue :: ByteString } deriving stock (Eq, Show)

data Operation = Insert  Value (Maybe BlobRef)
               | Mupsert Value
               | Delete
  deriving stock (Eq, Show)

data BlobRef = BlobRef Word64 Word32
  deriving stock (Eq, Show)

opHasBlobRef :: Operation -> Bool
opHasBlobRef (Insert _ (Just _blobref)) = True
opHasBlobRef _                          = False


-------------------------------------------------------------------------------
-- Disk page size
--

-- | A serialised page fits within chunks of memory of 4k, 8k, 16k, 32k or 64k.
--
data DiskPageSize = DiskPage4k  | DiskPage8k
                  | DiskPage16k | DiskPage32k
                  | DiskPage64k
  deriving stock (Eq, Show, Enum, Bounded)

diskPageSizeBytes :: DiskPageSize -> Int
diskPageSizeBytes DiskPage4k  = 2^(12::Int)
diskPageSizeBytes DiskPage8k  = 2^(13::Int)
diskPageSizeBytes DiskPage16k = 2^(14::Int)
diskPageSizeBytes DiskPage32k = 2^(15::Int)
diskPageSizeBytes DiskPage64k = 2^(16::Int)


-------------------------------------------------------------------------------
-- Calculating the page size (incrementally)
--

data PageSize = PageSize {
                  pageSizeElems :: !Int,
                  pageSizeBlobs :: !Int,
                  pageSizeBytes :: !Int,
                  pageSizeDisk  :: !DiskPageSize
                }
  deriving stock (Eq, Show)

pageSizeEmpty :: DiskPageSize -> PageSize
pageSizeEmpty = PageSize 0 0 10

pageSizeAddElem :: (Key, Operation) -> PageSize -> Maybe PageSize
pageSizeAddElem (Key key, op) (PageSize n b sz dpgsz)
  | sz' <= diskPageSizeBytes dpgsz || n' == 1
              = Just (PageSize n' b' sz' dpgsz)
  | otherwise = Nothing
  where
    n' = n+1
    b' | opHasBlobRef op = b+1
       | otherwise       = b
    sz' = sz
        + (if n `mod` 64 == 0 then 8 else 0)    -- blobrefs bitmap
        + (if n `mod` 32 == 0 then 8 else 0)    -- operations bitmap
        + (if opHasBlobRef op then 12 else 0)   -- blobref entry
        + 2                                     -- key offsets
        + (case n of { 0 -> 4; 1 -> 0; _ -> 2}) -- value offsets
        + BS.length key
        + (case op of
             Insert  (Value v) _ -> BS.length v
             Mupsert (Value v)   -> BS.length v
             Delete              -> 0)

calcPageSize :: DiskPageSize -> [(Key, Operation)] -> Maybe PageSize
calcPageSize dpgsz kops =
    go (pageSizeEmpty dpgsz) kops
  where
    go !pgsz [] = Just pgsz
    go !pgsz ((key, op):kops') =
      case pageSizeAddElem (key, op) pgsz of
        Nothing    -> Nothing
        Just pgsz' -> go pgsz' kops'


-------------------------------------------------------------------------------
-- Page encoding and serialisation types
--

-- | A serialised page consists of either a single disk page or several
-- disk pages. The latter is a primary page followed by one or more overflow
-- pages. Each disk page (single or multi) uses the same 'DiskPageSize', which
-- should be known from context (e.g. configuration).
--
type PageSerialised = ByteString

data PageIntermediate =
     PageIntermediate {
       pageNumKeys       :: !Word16,
       pageNumBlobs      :: !Word16,
       pageSizesOffsets  :: !PageSizesOffsets,
       pageBlobRefBitmap :: [Bool],
       pageOperations    :: [OperationEnum],
       pageBlobRefs      :: [BlobRef],
       pageKeyOffsets    :: [Word16],
       pageValueOffsets  :: Either [Word16] (Word16, Word32),
       pageKeys          :: !ByteString,
       pageValues        :: !ByteString,
       pagePadding       :: !ByteString, -- ^ Padding to the 'DiskPageSize'
       pageDiskPageSize  :: !DiskPageSize
     }
  deriving stock (Eq, Show)

data OperationEnum = OpInsert | OpMupsert | OpDelete
  deriving stock (Eq, Show)

data PageSizesOffsets =
     PageSizesOffsets {
       sizeDirectory     :: !Word16,
       sizeBlobRefBitmap :: !Word16,
       sizeOperations    :: !Word16,
       sizeBlobRefs      :: !Word16,
       sizeKeyOffsets    :: !Word16,
       sizeValueOffsets  :: !Word16,
       sizeKeys          :: !Word16,
       sizeValues        :: !Word32,

       offBlobRefBitmap  :: !Word16,
       offOperations     :: !Word16,
       offBlobRefs       :: !Word16,
       offKeyOffsets     :: !Word16,
       offValueOffsets   :: !Word16,
       offKeys           :: !Word16,
       offValues         :: !Word16,

       sizePageUsed      :: !Word32, -- ^ The size in bytes actually used
       sizePagePadding   :: !Word32, -- ^ The size in bytes of trailing padding
       sizePageDiskPage  :: !Word32  -- ^ The size in bytes rounded up to a
                                     -- multiple of the disk page size.
     }
  deriving stock (Eq, Show)


-------------------------------------------------------------------------------
-- Page encoding and serialisation
--

-- | Returns @Nothing@ if the size would be over-full for the given disk page
-- size.
--
calcPageSizeOffsets :: DiskPageSize  -- ^ underlying page size: 4k, 8k ... 64k
                    -> Int           -- ^ number of keys\/entries
                    -> Int           -- ^ number of blobs
                    -> Int           -- ^ total size of the keys
                    -> Int           -- ^ total size of the values
                    -> Maybe PageSizesOffsets
calcPageSizeOffsets dpgsz n b sizeKeys sizeValues
  | n < 0 || b < 0 || sizeKeys < 0 || sizeValues < 0
  = Nothing

  | n /= 1 --single entries can use multiple disk pages
  , sizePageUsed > diskPageSize
  = Nothing

  | otherwise
  = Just PageSizesOffsets {
      -- having checked for not over-full, we can now guarantee all
      -- these conversions into smaller types will not overflow:
      sizeDirectory     = fromIntegralChecked sizeDirectory,
      sizeBlobRefBitmap = fromIntegralChecked sizeBlobRefBitmap,
      sizeOperations    = fromIntegralChecked sizeOperations,
      sizeBlobRefs      = fromIntegralChecked sizeBlobRefs,
      sizeKeyOffsets    = fromIntegralChecked sizeKeyOffsets,
      sizeValueOffsets  = fromIntegralChecked sizeValueOffsets,
      sizeKeys          = fromIntegralChecked sizeKeys,
      sizeValues        = fromIntegralChecked sizeValues,

      offBlobRefBitmap  = fromIntegralChecked offBlobRefBitmap,
      offOperations     = fromIntegralChecked offOperations,
      offBlobRefs       = fromIntegralChecked offBlobRefs,
      offKeyOffsets     = fromIntegralChecked offKeyOffsets,
      offValueOffsets   = fromIntegralChecked offValueOffsets,
      offKeys           = fromIntegralChecked offKeys,
      offValues         = fromIntegralChecked offValues,

      sizePageUsed      = fromIntegralChecked sizePageUsed,
      sizePagePadding   = fromIntegralChecked sizePagePadding,
      sizePageDiskPage  = fromIntegralChecked sizePageDiskPage
    }
  where
    sizeDirectory, sizeBlobRefBitmap,
      sizeOperations, sizeBlobRefs,
      sizeKeyOffsets, sizeValueOffsets :: Int
    sizeDirectory     = 8
    sizeBlobRefBitmap = (n + 63) `shiftR` 3 .&. complement 0x7
    sizeOperations    = (2 * n + 63) `shiftR` 3 .&. complement 0x7
    sizeBlobRefs      = (4 + 8) * b
    sizeKeyOffsets    = 2 * n
    sizeValueOffsets  | n == 1    = 6
                      | otherwise = 2 * (n+1)

    offBlobRefBitmap, offOperations, offBlobRefs,
      offKeyOffsets, offValueOffsets,
      offKeys, offValues :: Int
    offBlobRefBitmap  =                    sizeDirectory
    offOperations     = offBlobRefBitmap + sizeBlobRefBitmap
    offBlobRefs       = offOperations    + sizeOperations
    offKeyOffsets     = offBlobRefs      + sizeBlobRefs
    offValueOffsets   = offKeyOffsets    + sizeKeyOffsets
    offKeys           = offValueOffsets  + sizeValueOffsets
    offValues         = offKeys          + sizeKeys

    sizePageUsed, sizePagePadding,
      sizePageDiskPage, diskPageSize :: Int
    sizePageUsed      = offValues        + sizeValues
    sizePagePadding   = case sizePageUsed `mod` diskPageSize of
                          0 -> 0
                          p -> diskPageSize - p
    sizePageDiskPage  = sizePageUsed + sizePagePadding
    diskPageSize      = diskPageSizeBytes dpgsz

encodePage :: DiskPageSize -> [(Key, Operation)] -> Maybe PageIntermediate
encodePage dpgsz kops = do
    let pageNumKeys       = length kops
        pageNumBlobs      = length (filter (opHasBlobRef . snd) kops)
        keys              = [ k | (k,_)  <- kops ]
        values            = [ v | (_,op)  <- kops
                            , let v = case op of
                                        Insert  v' _ -> v'
                                        Mupsert v'   -> v'
                                        Delete       -> Value (BS.empty)
                            ]

    pageSizesOffsets@PageSizesOffsets {
      offKeys, offValues, sizePagePadding
    } <- calcPageSizeOffsets
           dpgsz
           pageNumKeys pageNumBlobs
           (sum [ BS.length k | Key   k <- keys ])
           (sum [ BS.length v | Value v <- values ])

    let pageBlobRefBitmap = [ opHasBlobRef op | (_,op) <- kops ]
        pageOperations    = [ toOperationEnum op | (_,op) <- kops ]
        pageBlobRefs      = [ blobref | (_,Insert _ (Just blobref)) <- kops ]

        pageKeyOffsets    = init $ scanl (\o k -> o + keyLen16 k)
                                         offKeys keys
        pageValueOffsets  = case values of
                              [v] -> Right (offValues,
                                            fromIntegral offValues
                                            + valLen32 v)
                              _   -> Left  (scanl (\o v -> o + valLen16 v)
                                                  offValues values)
        pageKeys          = BS.concat [ k | Key   k <- keys ]
        pageValues        = BS.concat [ v | Value v <- values ]
        pagePadding       = BS.replicate (fromIntegral sizePagePadding) 0
        pageDiskPageSize  = dpgsz

    pure PageIntermediate {
      pageNumKeys  = fromIntegralChecked pageNumKeys,
      pageNumBlobs = fromIntegralChecked pageNumBlobs,
      ..
    }
  where
    toOperationEnum Insert{}  = OpInsert
    toOperationEnum Mupsert{} = OpMupsert
    toOperationEnum Delete{}  = OpDelete

    keyLen16 :: Key -> Word16
    valLen16 :: Value -> Word16
    valLen32 :: Value -> Word32
    keyLen16 (Key   k) = fromIntegral $ BS.length k
    valLen16 (Value v) = fromIntegral $ BS.length v
    valLen32 (Value v) = fromIntegral $ BS.length v

serialisePage :: PageIntermediate -> PageSerialised
serialisePage PageIntermediate{pageSizesOffsets = PageSizesOffsets{..}, ..} =
    BSL.toStrict . BB.toLazyByteString $

    -- the top level directory
    BB.word16LE pageNumKeys
 <> BB.word16LE pageNumBlobs
 <> BB.word16LE offKeyOffsets
 <> BB.word16LE 0 --spare
 <> mconcat [ BB.word64LE w | w <- toBitmap pageBlobRefBitmap ]
 <> mconcat [ BB.word64LE w | w <- toBitmap . concatMap opEnumToBits
                                            $ pageOperations ]
 <> mconcat [ BB.word64LE w64 | BlobRef w64 _w32 <- pageBlobRefs ]
 <> mconcat [ BB.word32LE w32 | BlobRef _w64 w32 <- pageBlobRefs ]
 <> mconcat [ BB.word16LE off | off <- pageKeyOffsets ]
 <> case pageValueOffsets of
      Left   offsets -> mconcat [ BB.word16LE off | off <- offsets ]
      Right (offset1, offset2) -> BB.word16LE offset1
                               <> BB.word32LE offset2
 <> BB.byteString pageKeys
 <> BB.byteString pageValues
 <> BB.byteString pagePadding
  where
    opEnumToBits OpInsert  = [False, False]
    opEnumToBits OpMupsert = [True,  False]
    opEnumToBits OpDelete  = [False, True]

deserialisePage :: DiskPageSize -> PageSerialised -> PageIntermediate
deserialisePage dpgsz p =
    flip Bin.runGet (BSL.fromStrict p) $ do
      pageNumKeys       <- fromIntegral <$> Bin.getWord16le
      pageNumBlobs      <- fromIntegral <$> Bin.getWord16le
      offsetKeyOffsets  <-                  Bin.getWord16le
      _                 <-                  Bin.getWord16le

      let sizeWord64BlobRefBitmap :: Int
          sizeWord64BlobRefBitmap = (pageNumKeys + 63) `shiftR` 6

          sizeWord64Operations :: Int
          sizeWord64Operations = (2 * pageNumKeys + 63) `shiftR` 6

      pageBlobRefBitmap <- take pageNumKeys . fromBitmap <$>
                             replicateM sizeWord64BlobRefBitmap Bin.getWord64le
      pageOperations    <- take pageNumKeys . opBitsToEnum . fromBitmap <$>
                             replicateM sizeWord64Operations Bin.getWord64le
      pageBlobRefsW64   <- replicateM pageNumBlobs Bin.getWord64le
      pageBlobRefsW32   <- replicateM pageNumBlobs Bin.getWord32le
      let pageBlobRefs   = zipWith BlobRef pageBlobRefsW64 pageBlobRefsW32

      pageKeyOffsets    <- replicateM pageNumKeys Bin.getWord16le
      pageValueOffsets  <-
        if pageNumKeys == 1
         then Right <$> ((,) <$> Bin.getWord16le
                             <*> Bin.getWord32le)
         else Left <$> replicateM (pageNumKeys + 1) Bin.getWord16le

      let sizeKeys :: Int
          sizeKeys
            | pageNumKeys > 0
            = fromIntegral (either head fst pageValueOffsets)
              - fromIntegral (head pageKeyOffsets)
            | otherwise       = 0

          sizeValues :: Int
          sizeValues
            | pageNumKeys > 0
            = either (fromIntegral . last) (fromIntegral . snd) pageValueOffsets
              - fromIntegral (either head fst pageValueOffsets)
            | otherwise = 0
      pageKeys   <- Bin.getByteString sizeKeys
      pageValues <- Bin.getByteString sizeValues
      pagePadding <- BSL.toStrict <$> Bin.getRemainingLazyByteString

      let pageSizesOffsets =
            fromMaybe (error "deserialisePage: disk page overflow") $
              calcPageSizeOffsets
                dpgsz
                pageNumKeys pageNumBlobs
                sizeKeys sizeValues

      assert (offsetKeyOffsets == offKeyOffsets pageSizesOffsets) $
        pure PageIntermediate {
          pageNumKeys      = fromIntegral pageNumKeys,
          pageNumBlobs     = fromIntegral pageNumBlobs,
          pageDiskPageSize = dpgsz,
          ..
        }
  where
    opBitsToEnum (False:False:bits) = OpInsert  : opBitsToEnum bits
    opBitsToEnum (True: False:bits) = OpMupsert : opBitsToEnum bits
    opBitsToEnum (False:True :bits) = OpDelete  : opBitsToEnum bits
    opBitsToEnum []                 = []
    opBitsToEnum _                  = error "opBitsToEnum"

decodePage :: PageIntermediate -> [(Key, Operation)]
decodePage PageIntermediate{pageSizesOffsets = PageSizesOffsets{..}, ..} =
    [ let op      = case opEnum of
                      OpInsert  -> Insert  (Value value) mblobref
                      OpMupsert -> Mupsert (Value value)
                      OpDelete  -> Delete
          mblobref | hasBlobref = Just (pageBlobRefs !! idxBlobref)
                   | otherwise  = Nothing
       in (Key key, op)
    | opEnum     <- pageOperations
    | hasBlobref <- pageBlobRefBitmap
    | idxBlobref <- scanl (\o b -> if b then o+1 else o) 0 pageBlobRefBitmap
    | keySpan    <- spans $ pageKeyOffsets
                         ++ either (take 1) ((:[]) . fst) pageValueOffsets
    , let key     = BS.take (fromIntegral $ snd keySpan - fst keySpan)
                  . BS.drop (fromIntegral $ fst keySpan - offKeys)
                  $ pageKeys
    | valSpan    <- spans $ case pageValueOffsets of
                              Left offs          -> map fromIntegral offs
                              Right (off1, off2) -> [ fromIntegral off1, off2 ]
    , let value   = BS.take (fromIntegral $ snd valSpan - fst valSpan)
                  . BS.drop (fromIntegral $ fst valSpan - fromIntegral offValues)
                  $ pageValues

    ]
  where
    spans xs = zip xs (drop 1 xs)

toBitmap :: [Bool] -> [Word64]
toBitmap =
    map toWord64 . group64
  where
    toWord64 :: [Bool] -> Word64
    toWord64 = List.foldl' (\w (n,b) -> if b then setBit w n else w) 0
             . zip [0 :: Int ..]
    group64  = List.unfoldr (\xs -> if null xs
                                 then Nothing
                                 else Just (splitAt 64 xs))

fromBitmap :: [Word64] -> [Bool]
fromBitmap =
    concatMap fromWord64
  where
    fromWord64 :: Word64 -> [Bool]
    fromWord64 w = [ testBit w i | i <- [0..63] ]

fromIntegralChecked :: (Integral a, Integral b) => a -> b
fromIntegralChecked x
  | let x' = fromIntegral x
  , fromIntegral x' == x
  = x'

  | otherwise
  = error "fromIntegralChecked: conversion failed"

-- | If a page uses overflow pages, return the:
--
-- 1. value prefix length (within the first page)
-- 2. value suffix length (within the overflow pages)
--
pageOverflowPrefixSuffixLen :: PageIntermediate -> Maybe (Int, Int)
pageOverflowPrefixSuffixLen p =
    case pageValueOffsets p of
      Right (offStart, offEnd)
        | let page1End = diskPageSizeBytes (pageDiskPageSize p)
        , fromIntegral offEnd > page1End
        , let prefixlen, suffixlen :: Int
              prefixlen = page1End - fromIntegral offStart
              suffixlen = fromIntegral offEnd - page1End
        -> Just (prefixlen, suffixlen)
      _ -> Nothing

-- | The total number of disk pages, including any overflow pages.
--
pageDiskPages :: PageIntermediate -> Int
pageDiskPages p =
    nbytes `div` diskPageSizeBytes (pageDiskPageSize p)
  where
    nbytes = fromIntegral (sizePageDiskPage (pageSizesOffsets p))

pageSerialisedChunks :: DiskPageSize -> PageSerialised -> [ByteString]
pageSerialisedChunks dpgsz =
    List.unfoldr (\p -> if BS.null p then Nothing
                                else Just (BS.splitAt dpgszBytes p))
  where
    dpgszBytes = diskPageSizeBytes dpgsz

-------------------------------------------------------------------------------
-- Test types and generators
--

data PageContentFits = PageContentFits DiskPageSize [(Key, Operation)]
  deriving stock Show

data PageContentMaybeOverfull = PageContentMaybeOverfull DiskPageSize
                                                         [(Key, Operation)]
  deriving stock Show

data PageContentSingle = PageContentSingle DiskPageSize Key Operation
  deriving stock Show

instance Arbitrary PageContentFits where
    arbitrary = do
      dpgsz <- arbitrary
      kops  <- genPageContentFits dpgsz noMinKeySize
      pure (PageContentFits dpgsz kops)

instance Arbitrary PageContentMaybeOverfull where
    arbitrary = do
      dpgsz <- arbitrary
      kops  <- genPageContentMaybeOverfull dpgsz noMinKeySize
      pure (PageContentMaybeOverfull dpgsz kops)

instance Arbitrary PageContentSingle where
    arbitrary = do
      dpgsz <- arbitrary
      (k,op) <- genPageContentSingle dpgsz noMinKeySize
      pure (PageContentSingle dpgsz k op)

-- | In some use cases it is necessary to generate 'Keys' that are at least of
-- some minimum length. Use 'noMinKeySize' if no such constraint is need.
newtype MinKeySize = MinKeySize Int
  deriving stock Show

-- | No minimum key size: @MinKeySize 0@.
noMinKeySize :: MinKeySize
noMinKeySize = MinKeySize 0

-- | Generate a test case consisting of a key\/operation sequence that is
-- guaranteed to fit into a given disk page size.
--
-- The distribution is designed to cover:
--
-- * small pages
-- * medium sized pages
-- * nearly full pages
-- * plus single key pages (possibly using one or more overflow pages)
-- * a corner case of a single large key\/operation pair followed by some small
--   key op pairs.
--
-- The keys are /not/ ordered: use 'orderdKeyOps' to sort and de-duplicate them
-- if that is needed (but note this will change the order of key sizes).
--
genPageContentFits :: DiskPageSize -> MinKeySize -> Gen [(Key, Operation)]
genPageContentFits dpgsz minkeysz =
    frequency
      [ (6, genPageContentMedium           dpgsz genkey genval)
      , (2, (:[]) <$> genPageContentSingle dpgsz minkeysz)
      , (1, genPageContentLargeSmallFits   dpgsz minkeysz)
      ]
  where
    genkey = genKeyMinSize dpgsz minkeysz
    genval = arbitrary

-- | Generate a test case consisting of a key\/operation sequence that is /not/
-- guaranteed to fit into a given disk page size.
--
-- These test cases are useful for checking the boundary conditions for what
-- can fit into a disk page. This covers a similar distribution to
-- 'genPageContentFits' but also includes about 20% of pages that are over full,
-- including the corner case of a large key ops pair followed by smaller key op
-- pairs (again possibly over full).
--
-- The keys are /not/ ordered: use 'orderdKeyOps' to sort and de-duplicate them
-- if that is needed.
--
genPageContentMaybeOverfull :: DiskPageSize
                            -> MinKeySize -> Gen [(Key, Operation)]
genPageContentMaybeOverfull dpgsz minkeysz =
    frequency
      [ (6, genPageContentMedium             dpgsz genkey genval)
      , (1, (:[]) <$> genPageContentSingle   dpgsz minkeysz)
      , (1, genPageContentOverfull           dpgsz genkey genval)
      , (1, genPageContentLargeSmallOverfull dpgsz minkeysz)
      ]
  where
    genkey = genKeyMinSize dpgsz minkeysz
    genval = arbitrary

-- | Generate a test case consisting of a single key\/operation pair.
--
genPageContentSingle :: DiskPageSize -> MinKeySize -> Gen (Key, Operation)
genPageContentSingle dpgsz minkeysz =
    oneof
      [ genPageContentSingleSmall     genkey genval
      , genPageContentSingleNearFull  dpgsz minkeysz
      , genPageContentSingleMultiPage dpgsz minkeysz
      ]
  where
    genkey = genKeyMinSize dpgsz minkeysz
    genval = arbitrary

-- | This generates a reasonable \"middle\" distribution of page sizes
-- (relative to the given disk page size). In particular it covers:
--
-- * small pages (~45% for 4k pages, ~15% for 64k pages)
-- * near-maximum pages (~20% for 4k pages, ~20% for 64k pages)
-- * some in between (~35% for 4k pages, ~60% for 64k pages)
--
-- The numbers above are when used with the normal 'arbitrary' 'Key' and
-- 'Value' generators. And with these generators, it tends to use lots of
-- small-to-medium size keys and values, rather than a few huge ones.
--
genPageContentMedium :: DiskPageSize
                     -> Gen Key
                     -> Gen Value
                     -> Gen [(Key, Operation)]
genPageContentMedium dpgsz genkey genval =
  takePageContentFits dpgsz <$>
    scale scaleForDiskPageSize
      (listOf (genPageContentSingleSmall genkey genval))
  where
    scaleForDiskPageSize :: Int -> Int
    scaleForDiskPageSize sz =
      ceiling $
        fromIntegral sz ** (1.0 + fromIntegral (fromEnum dpgsz) / 10 :: Float)

takePageContentFits :: DiskPageSize -> [(Key, Operation)] -> [(Key, Operation)]
takePageContentFits dpgsz = go (pageSizeEmpty dpgsz)
  where
    go _sz [] = []
    go sz (kop:kops)
      | Just sz' <- pageSizeAddElem kop sz = kop : go sz' kops
      | otherwise                          = []

-- | Generate only pages that are nearly full. This isn't the maximum possible
-- size, but where adding one more randomly-chosen key\/op pair would not fit
-- (but perhaps a smaller pair would still fit).
--
-- Consider if you really need this: the 'genPageContentMedium' also includes
-- these cases naturally as part of its distribution. On the other hand, this
-- can be good for generating benchmark data.
--
genPageContentNearFull :: DiskPageSize
                       -> Gen Key
                       -> Gen Value
                       -> Gen [(Key, Operation)]
genPageContentNearFull dpgsz genkey genval =
    --relies on first item being the one triggering over-full:
    drop 1 <$> genPageContentOverfull dpgsz genkey genval

-- | Generate pages that are just slightly over-full. This is where the last
-- key\/op pair takes it just over the disk page size (but this element is
-- first in the sequence).
--
genPageContentOverfull :: DiskPageSize
                       -> Gen Key
                       -> Gen Value
                       -> Gen [(Key, Operation)]
genPageContentOverfull dpgsz genkey genval =
    go [] (pageSizeEmpty dpgsz)
  where
    go :: [(Key, Operation)] -> PageSize -> Gen [(Key, Operation)]
    go kops sz = do
      kop <- genPageContentSingleSmall genkey genval
      case pageSizeAddElem kop sz of
         -- include as the /first/ element, the one that will make it overfull:
        Nothing  -> pure (kop:kops) -- not reversed!
        Just sz' -> go (kop:kops) sz'

genPageContentLargeSmallFits :: DiskPageSize
                             -> MinKeySize
                             -> Gen [(Key, Operation)]
genPageContentLargeSmallFits dpgsz minkeysz =
    takePageContentFits dpgsz <$>
      genPageContentLargeSmallOverfull dpgsz minkeysz

genPageContentLargeSmallOverfull :: DiskPageSize
                                 -> MinKeySize
                                 -> Gen [(Key, Operation)]
genPageContentLargeSmallOverfull dpgsz (MinKeySize minkeysz) =
    (\large small -> large : small)
      <$> genPageContentSingleOfSize genKeyValSizes
      <*> arbitrary
  where
    genKeyValSizes = do
      let size = maxKeySize dpgsz - 100
      split <- choose (minkeysz, size)
      pure (split, size - split)

genPageContentSingleOfSize :: Gen (Int, Int) -> Gen (Key, Operation)
genPageContentSingleOfSize genKeyValSizes = do
    (keySize, valSize) <- genKeyValSizes
    key <- Key   . BS.pack <$> vectorOf keySize arbitrary
    val <- Value . BS.pack <$> vectorOf valSize arbitrary
    op  <- oneof  -- no delete
             [ Insert val <$> arbitrary
             , pure (Mupsert val) ]
    pure (key, op)

genPageContentSingleSmall :: Gen Key -> Gen Value -> Gen (Key, Operation)
genPageContentSingleSmall genkey genval =
    (,) <$> genkey <*> genOperation genval

-- | Generate pages around the disk page size, above and below.
--
-- The key is always within the min key size given and max key size for the
-- page size.
genPageContentSingleNearFull :: DiskPageSize
                             -> MinKeySize
                             -> Gen (Key, Operation)
genPageContentSingleNearFull dpgsz (MinKeySize minkeysize) =
    genPageContentSingleOfSize genKeyValSizes
  where
    genKeyValSizes = do
      let maxkeysize = maxKeySize dpgsz
      size  <- choose (maxkeysize - 15, maxkeysize + 15)
      split <- choose (minkeysize, maxkeysize `min` size)
      pure (split, size - split)

genPageContentSingleMultiPage :: DiskPageSize
                              -> MinKeySize
                              -> Gen (Key, Operation)
genPageContentSingleMultiPage dpgsz (MinKeySize minkeysz) =
    genPageContentSingleOfSize genKeyValSizes
  where
    genKeyValSizes =
      (,) <$> choose (minkeysz, maxKeySize dpgsz)
          <*> choose (0, diskPageSizeBytes dpgsz * 3)

genKeyOfSize :: Gen Int -> Gen Key
genKeyOfSize genSize =
    genSize >>= \n -> Key . BS.pack <$!> vectorOf n arbitrary

genKeyMinSize :: DiskPageSize -> MinKeySize -> Gen Key
genKeyMinSize dpgsz (MinKeySize minsz) =
    genKeyOfSize
      (getSize >>= \sz -> chooseInt (minsz, sz `min` maxKeySize dpgsz))

instance Arbitrary Key where
  arbitrary =
    genKeyOfSize
      (getSize >>= \sz -> chooseInt (0, sz `min` maxKeySize DiskPage4k))

  shrink = shrinkMapBy Key unKey shrinkOpaqueByteString

genValueOfSize :: Gen Int -> Gen Value
genValueOfSize genSize =
    genSize >>= \n -> Value . BS.pack <$!> vectorOf n arbitrary

instance Arbitrary Value where
  arbitrary = genValueOfSize (getSize >>= \sz -> chooseInt (0, sz))

  shrink = shrinkMapBy Value unValue shrinkOpaqueByteString

genOperation :: Gen Value -> Gen Operation
genOperation genval =
    oneof
      [ Insert  <$> genval <*> arbitrary
      , Mupsert <$> genval
      , pure Delete
      ]

instance Arbitrary Operation where
  arbitrary = genOperation arbitrary

  shrink :: Operation -> [Operation]
  shrink Delete        = []
  shrink (Insert v mb) = Delete
                       : [ Insert  v' mb' | (v', mb') <- shrink (v, mb) ]
  shrink (Mupsert v)   = Insert v Nothing
                       : [ Mupsert v' | v' <- shrink v ]

instance Arbitrary BlobRef where
  arbitrary = BlobRef <$> arbitrary <*> arbitrary

  shrink (BlobRef 0 0) = []
  shrink (BlobRef _ _) = [BlobRef 0 0]

instance Arbitrary DiskPageSize where
  arbitrary = scale (`div` 5) $ growingElements [minBound..]
  shrink    = shrinkBoundedEnum

-- | Sort and de-duplicate a key\/operation sequence to ensure the sequence is
-- strictly ascending by key.
--
-- If you need this in a QC generator, you will need 'shrinkOrderedKeyOps' in
-- the corresponding shrinker.
--
orderdKeyOps :: [(Key, Operation)] -> [(Key, Operation)]
orderdKeyOps =
    List.nubBy ((==) `on` fst)
  . List.sortBy (compare `on` fst)

-- | Shrink a key\/operation sequence (without regard to key order).
shrinkKeyOps :: [(Key, Operation)] -> [[(Key, Operation)]]
shrinkKeyOps = shrink
  -- It turns out that the generic list shrink is actually good enough,
  -- but only because we've got carefully chosen shrinkers for Key and Value.
  -- Without those special shrinkers, this one would blow up.

-- | Shrink a key\/operation sequence, preserving key order.
shrinkOrderedKeyOps :: [(Key, Operation)] -> [[(Key, Operation)]]
shrinkOrderedKeyOps = map orderdKeyOps . shrink

-- | Shrink 'ByteString's that are used as opaque blobs, where their value
-- is generally not used, except for ordering. We minimise the number of
-- alternative shrinks, to help minimise the number of shrinks when lots
-- of such values are used, e.g. in key\/value containers.
--
-- This tries only three alternatives:
--
-- * take the first half
-- * take everything but the final byte
-- * replace the last (non-space) character by a space
--
-- > > shrinkOpaqueByteString "hello world!"
-- > ["hello ","hello world", "hello world "]
--
-- Using space as the replacement character makes the resulting strings
-- printable and shorter than lots of @\NUL\NUL\NUL@, which makes for test
-- failure cases that are easier to read.
--
shrinkOpaqueByteString :: ByteString -> [ByteString]
shrinkOpaqueByteString bs =
    [ BS.take (BS.length bs `div` 2) bs | BS.length bs > 2 ]
 ++ [ BS.init bs                        | BS.length bs > 0 ]
 ++ case BSC.spanEnd (==' ') bs of
      (prefix, spaces)
        | BS.null prefix -> []
        | otherwise      -> [ BS.init prefix <> BSC.cons ' ' spaces ]

-- | The maximum size of key that is guaranteed to always fit in an empty
-- 4k page. So this is a worst case maximum size: this size key will fit
-- irrespective of the corresponding operation, including the possibility
-- that the key\/op pair has a blob reference.
maxKeySize :: DiskPageSize -> Int
maxKeySize dpgsz = diskPageSizeBytes dpgsz - pageSizeOverhead

pageSizeOverhead :: Int
pageSizeOverhead =
    (pageSizeBytes . fromJust . calcPageSize DiskPage4k)
      [(Key BS.empty, Insert (Value BS.empty) (Just (BlobRef 0 0)))]
    -- the page size passed to calcPageSize here is irrelevant
