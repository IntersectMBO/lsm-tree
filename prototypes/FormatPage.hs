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
    Key (..), unKey,
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
    PageLogical (..),
    genFullPageLogical,
) where

import           Data.Bits
import           Data.Coerce (coerce)
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


-- | Logically, a page is a sequence of key,operation pairs (with optional
-- blobrefs), sorted by key.
newtype PageLogical = PageLogical [(Key, Operation)]
  deriving (Eq, Show)


-------------------------------------------------------------------------------
-- Page content types
--

newtype Key   = Key   ByteString deriving (Eq, Ord, Show)
newtype Value = Value ByteString deriving (Eq, Show)

unKey :: Key -> ByteString
unKey = coerce

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

calcPageSize :: DiskPageSize -> PageLogical -> Maybe PageSize
calcPageSize dpgsz (PageLogical kops) =
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

-- | Returns @Nothing@ if the size would overflow the given disk page size.
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

  | n /= 1 --single enties can use multiple disk pages
  , sizePageUsed > diskPageSize
  = Nothing

  | otherwise
  = Just PageSizesOffsets {
      -- having checkes for no overflow, we can now guarantee all
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

encodePage :: DiskPageSize -> PageLogical -> Maybe PageIntermediate
encodePage dpgsz (PageLogical kops) = do
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

decodePage :: PageIntermediate -> PageLogical
decodePage PageIntermediate{pageSizesOffsets = PageSizesOffsets{..}, ..} =
  PageLogical
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


instance Arbitrary Key where
  arbitrary = do
    sz <- getSize
    n  <- chooseInt (0, sz `min` maxKeySize)
    Key . BS.pack <$> vectorOf n arbitrary

  shrink (Key k) = [ Key (BS.pack k') | k' <- shrink (BS.unpack k) ]

instance Arbitrary Value where
  arbitrary = do
    sz <- getSize
    n  <- chooseInt (0, sz)
    Value . BS.pack <$> vectorOf n arbitrary

  shrink (Value v) = [ Value (BS.pack v') | v' <- shrink (BS.unpack v) ]

instance Arbitrary Operation where
  arbitrary = genOperation arbitrary

  shrink :: Operation -> [Operation]
  shrink Delete        = []
  shrink (Insert v mb) = Delete
                       : [ Insert  v' mb' | (v', mb') <- shrink (v, mb) ]
  shrink (Mupsert v)   = Insert v Nothing
                       : [ Mupsert v' | v' <- shrink v ]

genOperation :: Gen Value -> Gen Operation
genOperation gv = oneof
      [ Insert  <$> gv <*> arbitrary
      , Mupsert <$> gv
      , pure Delete
      ]

instance Arbitrary BlobRef where
  arbitrary = BlobRef <$> arbitrary <*> arbitrary

  shrink (BlobRef w64 w32) =
      [ BlobRef w64' w32' | (w64', w32') <- shrink (w64, w32) ]

instance Arbitrary PageLogical where
  arbitrary =
    frequency
      [ (1, (PageLogical . (:[])) <$>
               scale (\n' -> ceiling (fromIntegral n' ** 2.2 :: Float)) arbitrary)
      , (4, do n <- getSize
               scale (\n' -> ceiling (sqrt (fromIntegral n' :: Float))) $
                 go [] n (pageSizeEmpty DiskPage4k))
      ]
    where
      go es 0 _  = return (mkPageLogical es)
      go es i sz = do
        e <- arbitrary
        case pageSizeAddElem e sz of
          Nothing  -> return (mkPageLogical es)
          Just sz' -> go (e:es) (i-1) sz'

  shrink (PageLogical p) =
    [ mkPageLogical p' | p' <- shrink p ]

genFullPageLogical :: DiskPageSize -> Gen Key -> Gen Value -> Gen PageLogical
genFullPageLogical dpgsz gk gv =
    go [] (pageSizeEmpty dpgsz)
  where
    go :: [(Key, Operation)] -> PageSize -> Gen PageLogical
    go es sz = do
      e <- (,) <$> gk <*> genOperation gv
      case pageSizeAddElem e sz of
        Nothing  -> return (mkPageLogical es)
        Just sz' -> go (e:es) sz'

-- | Create 'PageLogical' enforcing the invariant.
mkPageLogical :: [(Key, Operation)] -> PageLogical
mkPageLogical =
    PageLogical
  . nubBy ((==) `on` fst)
  . sortBy (compare `on` fst)

instance Arbitrary DiskPageSize where
  arbitrary = growingElements [minBound..]
  shrink    = shrinkBoundedEnum


-------------------------------------------------------------------------------
-- Tests
--

tests :: TestTree
tests = testGroup "FormatPage"
    [ testProperty "invariant" prop_invariant
    , testProperty "shrink" prop_shrink_invariant
    , testProperty "to/from bitmap" prop_toFromBitmap
    , testProperty "size distribution" prop_size_distribution
    , testProperty "maxKeySize" prop_maxKeySize
    , testProperty "size 1" prop_size1
    , testProperty "size 2" prop_size2
    , testProperty "size 3" prop_size3
    , testProperty "encode/decode" prop_encodeDecode
    , testProperty "serialise/deserialise" prop_serialiseDeserialise
    , testProperty "encode/serialise/deserialise/decode"
                   prop_encodeSerialiseDeserialiseDecode
    ]

-- Generated keys are unique and in increasing order
prop_invariant :: PageLogical -> Property
prop_invariant page = case invariant page of
    Left ks  -> counterexample (show ks) $ property False
    Right () -> property True

prop_shrink_invariant :: PageLogical -> Property
prop_shrink_invariant page = case mapM_ invariant (shrink page) of
    Left ks  -> counterexample (show ks) $ property False
    Right () -> property True

invariant :: PageLogical -> Either (Key, Key) ()
invariant (PageLogical xs0) = go xs0
  where
    go :: [(Key, op)] -> Either (Key, Key) ()
    go []         = Right ()
    go ((k,_):xs) = go1 k xs

    go1 :: Key -> [(Key, op)] -> Either (Key, Key) ()
    go1 _  []            = Right ()
    go1 k1 ((k2,_):xs) =
        if k1 < k2
        then go1 k2 xs
        else Left (k1, k2)

prop_toFromBitmap :: [Bool] -> Bool
prop_toFromBitmap bits =
    bits == take (length bits) (roundTrip bits)
  where
    roundTrip = fromBitmap . toBitmap

prop_size_distribution :: DiskPageSize -> PageLogical -> Property
prop_size_distribution dpgsz p@(PageLogical es) =
  case calcPageSize dpgsz p of
    Nothing -> property False
    Just PageSize{pageSizeElems, pageSizeBlobs, pageSizeBytes} ->
      tabulate "page size in elements"
        [ showNumElems pageSizeElems ] $
      tabulate "page number of blobs"
        [ showNumElems pageSizeBlobs ] $
      tabulate "page size in bytes"
        [ showPageSizeBytes pageSizeBytes ] $
      tabulate "key size in bytes"
        [ showKeyValueSizeBytes (BS.length k) | (Key k, _) <- es ] $
      tabulate "value size in bytes"
        [ showKeyValueSizeBytes (BS.length v)
        | (_, op) <- es
        , Value v <- case op of
                       Insert  v _ -> [v]
                       Mupsert v   -> [v]
                       Delete      -> []
        ] $
      cover 0.5 (pageSizeBytes > 4096) "page over 4k" $

        property $ (if pageSizeElems > 1
                       then pageSizeBytes <= 4096
                       else True)
                && (pageSizeElems == length [ e | e <- es ])
                && (pageSizeBlobs == length [ b | (_,Insert _ (Just b)) <- es ])
  where
    showNumElems n
      | n == 0    = "0"
      | n == 1    = "1"
      | n < 10    = "1 < n < 10"
      | otherwise = nearest 10 n
    showPageSizeBytes n
      | n >= 4096  = show ((n `div` 4096) * 4) ++ "k <= n < "
                  ++ show (((n `div` 4096) + 1) * 4) ++ "k"
      | otherwise = nearest 512 n
    showKeyValueSizeBytes n
      | n < 16    = show n
      | n < 1024  = "16    < n < 1024"
      | otherwise = nearest 1024 n
    nearest m n = show ((n `div` m) * m)
     ++ " <= n < " ++ show ((n `div` m) * m + m)


-- | The maximum size of key that is guaranteed to always fit in an empty
-- 4k page. So this is a worst case maximum size: this size key will fit
-- irrespective of the corresponding operation, including the possibility
-- that the key\/op pair has a blob reference.
maxKeySize :: Int
maxKeySize = diskPageSizeBytes dpgsz - overhead  -- == 4052
  where
    overhead =
      (pageSizeBytes . fromJust . calcPageSize dpgsz . PageLogical)
        [(Key BS.empty, Insert (Value BS.empty) (Just (BlobRef 0 0)))]
    dpgsz = DiskPage4k

prop_maxKeySize :: Bool
prop_maxKeySize = maxKeySize == 4052

prop_size1 :: DiskPageSize -> PageLogical -> Bool
prop_size1 dpgsz p =
    sizePageUsed > 0
 && sizePageUsed + sizePagePadding == sizePageDiskPage
 && if pageNumKeys p' == 1
      then fromIntegral sizePageDiskPage `mod` diskPageSizeBytes dpgsz == 0
      else fromIntegral sizePageDiskPage == diskPageSizeBytes dpgsz
  where
    Just p' = encodePage dpgsz p
    PageSizesOffsets{..} = pageSizesOffsets p'

prop_size2 :: DiskPageSize -> PageLogical -> Bool
prop_size2 dpgsz p =
    BS.length (serialisePage p')
 == fromIntegral (sizePageDiskPage (pageSizesOffsets p'))
  where
    Just p' = encodePage dpgsz p

prop_size3 :: DiskPageSize -> PageLogical -> Bool
prop_size3 dpgsz p =
  case (calcPageSize dpgsz p, encodePage dpgsz p) of
    (Just PageSize{pageSizeBytes}, Just p') ->
      pageSizeBytes == (fromIntegral . sizePageUsed . pageSizesOffsets) p'
    _                  -> False

prop_encodeDecode :: DiskPageSize -> PageLogical -> Property
prop_encodeDecode dpgsz p =
    p === decodePage p'
  where
    Just p' = encodePage dpgsz p

prop_serialiseDeserialise :: DiskPageSize -> PageLogical -> Bool
prop_serialiseDeserialise dpgsz p =
    p' == roundTrip p'
  where
    Just p'   = encodePage dpgsz p
    roundTrip = deserialisePage dpgsz . serialisePage

prop_encodeSerialiseDeserialiseDecode :: DiskPageSize -> PageLogical -> Bool
prop_encodeSerialiseDeserialiseDecode dpgsz p =
    p == roundTrip p'
  where
    Just p'   = encodePage dpgsz p
    roundTrip = decodePage . deserialisePage dpgsz . serialisePage

