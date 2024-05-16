{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE InstanceSigs        #-}
{-# LANGUAGE NamedFieldPuns      #-}
{-# LANGUAGE ParallelListComp    #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | This accompanies the format-page.md documentation as a sanity check
-- and a precise reference. It is intended to demonstrate that the page
-- format works. It is also used as a reference implementation for tests of
-- the real implementation.
--
-- Logically, a page is a sequence of key,operation pairs (with optional
-- blobrefs), sorted by key, and its serialised form fits within a disk page.
--
-- The reference implementation does not rely on the keys being sorted.
--
module FormatPage (
    -- * Page types
    Key (..),
    Operation (..),
    Value (..),
    BlobRef (..),
    PageSerialised,
    PageIntermediate,

    -- * Page size
    PageSize (..),
    pageSizeEmpty,
    pageSizeAddElem,
    calcPageSize,

    -- * Encoding and decoding
    DiskPageSize(..),
    encodePage,
    decodePage,
    serialisePage,
    deserialisePage,

    -- * Tests and generators
    tests,
    -- ** Generators and shrinkers
    genPageContentFits,
    genPageContentMaybeOverfull,
    genPageContentSingle,
    genPageContentNearFull,
    genPageContentMedium,
    MinKeySize(..),
    noMinKeySize,
    orderdKeyOps,
    shrinkKeyOps,
    shrinkOrderedKeyOps,
) where

import           Data.Bits
import           Data.Function (on)
import           Data.List (foldl', nubBy, sortBy, unfoldr)
import           Data.Maybe (fromJust, fromMaybe)
import           Data.Word

import qualified Data.Binary.Get as Bin
import           Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as BB
import qualified Data.ByteString.Lazy as BSL

import           Control.Exception (assert)
import           Control.Monad

import           Test.QuickCheck hiding ((.&.))
import           Test.Tasty
import           Test.Tasty.QuickCheck (testProperty)


-------------------------------------------------------------------------------
-- Page content types
--

newtype Key   = Key   { unKey   :: ByteString } deriving (Eq, Ord, Show)
newtype Value = Value { unValue :: ByteString } deriving (Eq, Show)

data Operation = Insert  Value (Maybe BlobRef)
               | Mupsert Value
               | Delete
  deriving (Eq, Show)

data BlobRef = BlobRef Word64 Word32
  deriving (Eq, Show)

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
  deriving (Eq, Show, Enum, Bounded)

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
  deriving (Eq, Show)

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
  deriving (Eq, Show)

data OperationEnum = OpInsert | OpMupsert | OpDelete
  deriving (Eq, Show)

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
  deriving (Eq, Show)


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
        return PageIntermediate {
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
    toWord64 = foldl' (\w (n,b) -> if b then setBit w n else w) 0
             . zip [0 :: Int ..]
    group64  = unfoldr (\xs -> if null xs
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


-------------------------------------------------------------------------------
-- Test types and generators
--

data PageContentFits = PageContentFits DiskPageSize [(Key, Operation)]
  deriving Show

data PageContentMaybeOverfull = PageContentMaybeOverfull DiskPageSize
                                                         [(Key, Operation)]
  deriving Show

data PageContentSingle = PageContentSingle DiskPageSize Key Operation
  deriving Show

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
  deriving Show

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
        Nothing  -> return (kop:kops) -- not reversed!
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

genPageContentSingleNearFull :: DiskPageSize
                             -> MinKeySize
                             -> Gen (Key, Operation)
genPageContentSingleNearFull dpgsz (MinKeySize minkeysz) =
    genPageContentSingleOfSize genKeyValSizes
  where
    genKeyValSizes = do
      let maxsize = maxKeySize dpgsz
      size  <- choose (maxsize - 15, maxsize)
      split <- choose (minkeysz, size)
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

  shrink (Key k) = [ Key (BS.pack k') | k' <- shrink (BS.unpack k) ]

genValueOfSize :: Gen Int -> Gen Value
genValueOfSize genSize =
    genSize >>= \n -> Value . BS.pack <$!> vectorOf n arbitrary

instance Arbitrary Value where
  arbitrary = genValueOfSize (getSize >>= \sz -> chooseInt (0, sz))

  shrink (Value v) = [ Value (BS.pack v') | v' <- shrink (BS.unpack v) ]

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

  shrink (BlobRef w64 w32) =
      [ BlobRef w64' w32' | (w64', w32') <- shrink (w64, w32) ]

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
    nubBy ((==) `on` fst)
  . sortBy (compare `on` fst)

-- | Shrink a key\/operation sequence (without regard to key order).
shrinkKeyOps :: [(Key, Operation)] -> [[(Key, Operation)]]
shrinkKeyOps = shrink

-- | Shrink a key\/operation sequence, preserving key order.
shrinkOrderedKeyOps :: [(Key, Operation)] -> [[(Key, Operation)]]
shrinkOrderedKeyOps = map orderdKeyOps . shrink


-------------------------------------------------------------------------------
-- Tests
--

tests :: TestTree
tests = testGroup "FormatPage"
    [ testProperty "to/from bitmap" prop_toFromBitmap
    , testProperty "maxKeySize" prop_maxKeySize

    , let dpgsz = DiskPage4k in
      testGroup "size distribution"
      [ testProperty "genPageContentFits" $
        checkCoverage $
        coverTable "page size in bytes"
          [("0 <= n < 512",10)
          ,("3k < n <= 4k", 5)] $
        coverTable "page size in disk pages"
          [("1 page",  50)
          ,("2 pages",  0.5)
          ,("3+ pages", 0.5)] $
        forAll (genPageContentFits dpgsz noMinKeySize) $
          prop_size_distribution dpgsz (property False)

      , testProperty "genPageContentMaybeOverfull" $
        checkCoverage $
        forAll (genPageContentMaybeOverfull dpgsz noMinKeySize) $
          prop_size_distribution dpgsz
            (cover 10 True "over-full" (property True))

      , testProperty "genPageContentSingle" $
        checkCoverage $
        coverTable "page size in disk pages"
          [("1 page",  10)
          ,("2 pages",  2)
          ,("3+ pages", 2)] $
        forAll ((:[]) <$> genPageContentSingle dpgsz noMinKeySize) $
          prop_size_distribution dpgsz (property False)
      ]
    , testProperty "size 0" prop_size0
    , testProperty "size 1" prop_size1
    , testProperty "size 2" prop_size2
    , testProperty "size 3" prop_size3
    , testProperty "encode/decode" prop_encodeDecode
    , testProperty "serialise/deserialise" prop_serialiseDeserialise
    , testProperty "encode/serialise/deserialise/decode"
                   prop_encodeSerialiseDeserialiseDecode
    ]

prop_toFromBitmap :: [Bool] -> Bool
prop_toFromBitmap bits =
    bits == take (length bits) (roundTrip bits)
  where
    roundTrip = fromBitmap . toBitmap

prop_size_distribution :: DiskPageSize
                       -> Property -- ^ over-full sub-property
                       -> [(Key, Operation)]
                       -> Property
prop_size_distribution dpgsz propOverfull p =
  case calcPageSize dpgsz p of
    Nothing -> propOverfull
    Just PageSize{pageSizeElems, pageSizeBlobs, pageSizeBytes} ->
      tabulate "page size in elements"
        [ showNumElems pageSizeElems ] $
      tabulate "page number of blobs"
        [ showNumElems pageSizeBlobs ] $
      tabulate "page size in bytes"
        [ showPageSizeBytes pageSizeBytes ] $
      tabulate "page size in disk pages"
        [ showPageSizeDiskPages pageSizeBytes ] $
      tabulate "key size in bytes"
        [ showKeyValueSizeBytes (BS.length k) | (Key k, _) <- p ] $
      tabulate "value size in bytes"
        [ showKeyValueSizeBytes (BS.length v)
        | (_, op) <- p
        , Value v <- case op of
                       Insert  v _ -> [v]
                       Mupsert v   -> [v]
                       Delete      -> []
        ] $
      property $ (if pageSizeElems > 1
                     then pageSizeBytes <= dpgszBytes
                     else True)
              && (pageSizeElems == length p)
              && (pageSizeBlobs == length (filter (opHasBlobRef . snd) p))
  where
    dpgszBytes = diskPageSizeBytes dpgsz

    showNumElems :: Int -> String
    showNumElems n
      | n <= 1    = show n
      | n < 10    = "1 < n < 10"
      | otherwise = nearest 10 n

    showPageSizeBytes :: Int -> String
    showPageSizeBytes n
      | n > 4096  = nearest4k n
      | n > 1024  = nearest1k n
      | otherwise = nearest 512 n

    showPageSizeDiskPages :: Int -> String
    showPageSizeDiskPages n
      | npgs == 1 = "1 page"
      | npgs == 2 = "2 pages"
      | otherwise = "3+ pages"
      where
        npgs = (n + dpgszBytes - 1) `div` dpgszBytes

    showKeyValueSizeBytes :: Int -> String
    showKeyValueSizeBytes n
      | n < 20    = nearest 5 n
      | n < 100   = "20 <= n < 100"
      | n < 1024  = nearest 100 n
      | otherwise = nearest1k n

    nearest :: Int -> Int -> String
    nearest m n = show ((n `div` m) * m) ++ " <= n < "
               ++ show ((n `div` m) * m + m)

    nearest1k, nearest4k :: Int -> String
    nearest1k n = show ((n-1) `div` 1024) ++ "k < n <= "
               ++ show ((n-1) `div` 1024 + 1) ++ "k"
    nearest4k n = show (((n-1) `div` 4096) * 4) ++ "k < n <= "
               ++ show (((n-1) `div` 4096) * 4 + 4) ++ "k"

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

prop_maxKeySize :: Bool
prop_maxKeySize = maxKeySize DiskPage4k == 4052

-- | The 'calcPageSize' and 'calcPageSizeOffsets' (used by 'encodePage') had
-- better agree with each other!
--
-- The 'calcPageSize' uses the incremental 'PageSize' API, to work out the page
-- size, element by element, while 'calcPageSizeOffsets' is a bulk operation
-- used by 'encodePage'. It's critical that they agree on how many elements can
-- fit into a page.
--
prop_size0 :: PageContentMaybeOverfull -> Bool
prop_size0 (PageContentMaybeOverfull dpgsz p) =
    case (calcPageSize dpgsz p, encodePage dpgsz p) of
        (Nothing, Nothing) -> True
        (Nothing, Just{})  -> False -- they disagree!
        (Just{}, Nothing)  -> False -- they disagree!
        (Just PageSize{..},
         Just PageIntermediate{pageSizesOffsets = PageSizesOffsets{..}, ..}) ->
              pageSizeElems == fromIntegral pageNumKeys
           && pageSizeBlobs == fromIntegral pageNumBlobs
           && pageSizeBytes == fromIntegral sizePageUsed
           && pageSizeDisk  == pageDiskPageSize
           && pageSizeDisk  == dpgsz

prop_size1 :: PageContentFits -> Bool
prop_size1 (PageContentFits dpgsz p) =
    sizePageUsed > 0
 && sizePageUsed + sizePagePadding == sizePageDiskPage
 && if pageNumKeys p' == 1
      then fromIntegral sizePageDiskPage `mod` diskPageSizeBytes dpgsz == 0
      else fromIntegral sizePageDiskPage == diskPageSizeBytes dpgsz
  where
    Just p' = encodePage dpgsz p
    PageSizesOffsets{..} = pageSizesOffsets p'

prop_size2 :: PageContentFits -> Bool
prop_size2 (PageContentFits dpgsz p) =
    BS.length (serialisePage p')
 == fromIntegral (sizePageDiskPage (pageSizesOffsets p'))
  where
    Just p' = encodePage dpgsz p

prop_size3 :: PageContentFits -> Bool
prop_size3 (PageContentFits dpgsz p) =
  case (calcPageSize dpgsz p, encodePage dpgsz p) of
    (Just PageSize{pageSizeBytes}, Just p') ->
      pageSizeBytes == (fromIntegral . sizePageUsed . pageSizesOffsets) p'
    _ -> False

prop_encodeDecode :: PageContentFits -> Property
prop_encodeDecode (PageContentFits dpgsz p) =
    p === decodePage p'
  where
    Just p' = encodePage dpgsz p

prop_serialiseDeserialise :: PageContentFits -> Bool
prop_serialiseDeserialise (PageContentFits dpgsz p) =
    p' == roundTrip p'
  where
    Just p'   = encodePage dpgsz p
    roundTrip = deserialisePage dpgsz . serialisePage

prop_encodeSerialiseDeserialiseDecode :: PageContentFits -> Bool
prop_encodeSerialiseDeserialiseDecode (PageContentFits dpgsz p) =
    p == roundTrip p'
  where
    Just p'   = encodePage dpgsz p
    roundTrip = decodePage . deserialisePage dpgsz . serialisePage

