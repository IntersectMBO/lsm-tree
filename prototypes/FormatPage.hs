{-# LANGUAGE BangPatterns     #-}
{-# LANGUAGE NamedFieldPuns   #-}
{-# LANGUAGE ParallelListComp #-}
{-# LANGUAGE RecordWildCards  #-}

-- | This accompanies the format-page.md documentation as a sanity check
-- and a precise reference. It is intended to demonstrate that the page
-- format works.
--
module FormatPage (tests) where

import           Data.Bits
import           Data.Function (on)
import           Data.List (foldl', sortBy, unfoldr)
import           Data.Maybe (fromJust, isJust)
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
newtype PageLogical = PageLogical [(Key, Operation, Maybe BlobRef)]
  deriving (Eq, Show)

newtype Key   = Key   ByteString deriving (Eq, Ord, Show)
newtype Value = Value ByteString deriving (Eq, Show)

data Operation = Insert  Value
               | Mupsert Value
               | Delete
  deriving (Eq, Show)

data BlobRef = BlobRef Word64 Word32
  deriving (Eq, Show)

-- | A serialised page is 4k bytes (or 8,16,32 or 64k for big pages).
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
       pageValues        :: !ByteString
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

       sizePageTotal     :: !Word32
     }
  deriving (Eq, Show)

calcPageSizeOffsets :: Word16 -> Word16 -> Word16 -> Word32 -> PageSizesOffsets
calcPageSizeOffsets n b sizeKeys sizeValues =
    PageSizesOffsets {..}
  where
    sizeDirectory     = 8
    sizeBlobRefBitmap = (n + 63) `shiftR` 3 .&. complement 0x7
    sizeOperations    = (2 * n + 63) `shiftR` 3 .&. complement 0x7
    sizeBlobRefs      = (4 + 8) * b
    sizeKeyOffsets    = 2 * n
    sizeValueOffsets  | n == 1    = 6
                      | otherwise = 2 * (n+1)

    offBlobRefBitmap  =                    sizeDirectory
    offOperations     = offBlobRefBitmap + sizeBlobRefBitmap
    offBlobRefs       = offOperations    + sizeOperations
    offKeyOffsets     = offBlobRefs      + sizeBlobRefs
    offValueOffsets   = offKeyOffsets    + sizeKeyOffsets
    offKeys           = offValueOffsets  + sizeValueOffsets
    offValues         = offKeys          + sizeKeys
    sizePageTotal     = fromIntegral offValues + sizeValues

data PageSize = PageSize {
                  pageSizeElems :: !Int,
                  pageSizeBlobs :: !Int,
                  pageSizeBytes :: !Int
                }
  deriving (Eq, Show)

pageSizeEmpty :: PageSize
pageSizeEmpty = PageSize 0 0 10

pageSizeAddElem :: (Key, Operation, Maybe BlobRef)
                -> PageSize -> Maybe PageSize
pageSizeAddElem (Key key, op, mblobref) (PageSize n b sz)
  | sz' <= 4096 || n' == 1 = Just (PageSize n' b' sz')
  | otherwise              = Nothing
  where
    n' = n+1
    b' | isJust mblobref = b+1
       | otherwise       = b
    sz' = sz
        + (if n `mod` 64 == 0 then 8 else 0)    -- blobrefs bitmap
        + (if n `mod` 32 == 0 then 8 else 0)    -- operations bitmap
        + (if isJust mblobref then 12 else 0)   -- blobref entry
        + 2                                     -- key offsets
        + (case n of { 0 -> 4; 1 -> 0; _ -> 2}) -- value offsets
        + BS.length key
        + (case op of
             Insert  (Value v) -> BS.length v
             Mupsert (Value v) -> BS.length v
             Delete            -> 0)

calcPageSize :: PageLogical -> Maybe PageSize
calcPageSize (PageLogical kops) =
    go pageSizeEmpty kops
  where
    go !pgsz [] = Just pgsz
    go !pgsz ((key, op, mblobref):kops') =
      case pageSizeAddElem (key, op, mblobref) pgsz of
        Nothing    -> Nothing
        Just pgsz' -> go pgsz' kops'

encodePage :: PageLogical -> PageIntermediate
encodePage (PageLogical kops) =
    PageIntermediate {..}
  where
    pageNumKeys, pageNumBlobs :: Word16
    pageNumKeys       = fromIntegral (length kops)
    pageNumBlobs      = fromIntegral (length [ b | (_,_, Just b) <- kops ])

    pageSizesOffsets@PageSizesOffsets {offKeys, offValues}
                      = calcPageSizeOffsets
                          pageNumKeys pageNumBlobs
                          (fromIntegral (BS.length pageKeys))
                          (fromIntegral (BS.length pageValues))

    pageBlobRefBitmap = [ isJust mblobref | (_,_, mblobref) <- kops ]
    pageOperations    = [ toOperationEnum op | (_,op,_) <- kops ]
    pageBlobRefs      = [ blobref | (_,_, Just blobref) <- kops ]

    keys              = [ k | (k,_,_)  <- kops ]
    values            = [ v | (_,op,_)  <- kops
                        , let v = case op of
                                    Insert  v' -> v'
                                    Mupsert v' -> v'
                                    Delete     -> Value (BS.empty)
                        ]

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
  where
    opEnumToBits OpInsert  = [False, False]
    opEnumToBits OpMupsert = [True,  False]
    opEnumToBits OpDelete  = [False, True]

deserialisePage :: PageSerialised -> PageIntermediate
deserialisePage p =
    flip Bin.runGet (BSL.fromStrict p) $ do
      pageNumKeys       <- Bin.getWord16le
      pageNumBlobs      <- Bin.getWord16le
      offsetKeyOffsets  <- Bin.getWord16le
      _                 <- Bin.getWord16le

      let sizeWord64BlobRefBitmap :: Int
          sizeWord64BlobRefBitmap = (fromIntegral pageNumKeys + 63) `shiftR` 6

          sizeWord64Operations :: Int
          sizeWord64Operations = (fromIntegral (2 * pageNumKeys) + 63) `shiftR` 6

      pageBlobRefBitmap <- take (fromIntegral pageNumKeys) . fromBitmap <$>
                             replicateM sizeWord64BlobRefBitmap Bin.getWord64le
      pageOperations    <- take (fromIntegral pageNumKeys)
                         . opBitsToEnum . fromBitmap <$>
                             replicateM sizeWord64Operations Bin.getWord64le
      pageBlobRefsW64   <- replicateM (fromIntegral pageNumBlobs) Bin.getWord64le
      pageBlobRefsW32   <- replicateM (fromIntegral pageNumBlobs) Bin.getWord32le
      let pageBlobRefs   = zipWith BlobRef pageBlobRefsW64 pageBlobRefsW32

      pageKeyOffsets    <- replicateM (fromIntegral pageNumKeys) Bin.getWord16le
      pageValueOffsets  <-
        if pageNumKeys == 1
         then Right <$> ((,) <$> Bin.getWord16le
                             <*> Bin.getWord32le)
         else Left <$> replicateM (fromIntegral pageNumKeys + 1) Bin.getWord16le

      let sizeKeys :: Word16
          sizeKeys
            | pageNumKeys > 0
            = either head fst pageValueOffsets
              - head pageKeyOffsets
            | otherwise       = 0

          sizeValues :: Word32
          sizeValues
            | pageNumKeys > 0
            = either (fromIntegral . last) snd pageValueOffsets
              - fromIntegral (either head fst pageValueOffsets)
            | otherwise = 0
      pageKeys   <- Bin.getByteString (fromIntegral sizeKeys)
      pageValues <- Bin.getByteString (fromIntegral sizeValues)

      let pageSizesOffsets = calcPageSizeOffsets
                               pageNumKeys pageNumBlobs
                               sizeKeys sizeValues

      assert (offsetKeyOffsets == offKeyOffsets pageSizesOffsets) $
        return PageIntermediate{..}
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
                      OpInsert  -> Insert  (Value value)
                      OpMupsert -> Mupsert (Value value)
                      OpDelete  -> Delete
          mblobref | hasBlobref = Just (pageBlobRefs !! idxBlobref)
                   | otherwise  = Nothing
       in (Key key, op, mblobref)
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

tests :: TestTree
tests = testGroup "FormatPage" [
      testProperty "to/from bitmap" prop_toFromBitmap
    , testProperty "size distribution" prop_size_distribution
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

prop_encodeDecode :: PageLogical -> Bool
prop_encodeDecode p =
    p == roundTrip p
  where
    roundTrip = decodePage . encodePage

prop_size_distribution :: PageLogical -> Property
prop_size_distribution p@(PageLogical es) =
  case calcPageSize p of
    Nothing -> property False
    Just PageSize{pageSizeElems, pageSizeBlobs, pageSizeBytes} ->
      tabulate "page size in elements"
        [ showNumElems pageSizeElems ] $
      tabulate "page number of blobs"
        [ showNumElems pageSizeBlobs ] $
      tabulate "page size in bytes"
        [ showPageSizeBytes pageSizeBytes ] $
      tabulate "key size in bytes"
        [ showKeyValueSizeBytes (BS.length k) | (Key k, _, _) <- es ] $
      tabulate "value size in bytes"
        [ showKeyValueSizeBytes (BS.length v)
        | (_, op, _) <- es
        , Value v <- case op of
                       Insert  v -> [v]
                       Mupsert v -> [v]
                       Delete    -> []
        ] $
      cover 0.5 (pageSizeBytes > 4096) "page over 4k" $

        property $ (if pageSizeElems > 1
                       then pageSizeBytes <= 4096
                       else True)
                && (pageSizeElems == length [ e | e <- es ])
                && (pageSizeBlobs == length [ b | (_,_,Just b) <- es ])
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

prop_size1 :: PageLogical -> Bool
prop_size1 p =
    BS.length (serialisePage p')
 == fromIntegral (sizePageTotal (pageSizesOffsets p'))
  where
    p' = encodePage p

prop_size2 :: PageLogical -> Bool
prop_size2 p =
  case calcPageSize p of
    Nothing   -> True
    Just PageSize{pageSizeBytes} ->
      pageSizeBytes == (fromIntegral . sizePageTotal
                      . pageSizesOffsets . encodePage) p

prop_size3 :: PageLogical -> Bool
prop_size3 p =
  case calcPageSize p of
    Nothing                      -> True
    Just PageSize{pageSizeBytes} ->
      pageSizeBytes == BS.length (serialisePage (encodePage p))

prop_serialiseDeserialise :: PageLogical -> Bool
prop_serialiseDeserialise p =
    p' == roundTrip p'
  where
    p'        = encodePage p
    roundTrip = deserialisePage . serialisePage

prop_encodeSerialiseDeserialiseDecode :: PageLogical -> Bool
prop_encodeSerialiseDeserialiseDecode p =
    p == roundTrip p
  where
    roundTrip = decodePage . deserialisePage . serialisePage . encodePage

maxKeySize :: Int
maxKeySize = 4096 - overhead  -- 4052
  where
    overhead =
      (pageSizeBytes . fromJust . calcPageSize . PageLogical)
        [(Key BS.empty, Delete, Just (BlobRef 0 0))]

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
  arbitrary =
    oneof
      [ Insert  <$> arbitrary
      , Mupsert <$> arbitrary
      , pure Delete
      ]

  shrink Delete      = []
  shrink (Insert v)  = Delete   : [ Insert  v' | v' <- shrink v ]
  shrink (Mupsert v) = Insert v : [ Mupsert v' | v' <- shrink v ]

instance Arbitrary BlobRef where
  arbitrary = BlobRef <$> arbitrary <*> arbitrary

  shrink (BlobRef w64 w32) =
      [ BlobRef w64' w32' | (w64', w32') <- shrink (w64, w32) ]

instance Arbitrary PageLogical where
  arbitrary =
    frequency
      [ (1, (PageLogical . (:[])) <$>
               scale (\n' -> ceiling (fromIntegral n' ** 1.8 :: Float)) arbitrary)
      , (4, do n <- getSize
               scale (\n' -> ceiling (sqrt (fromIntegral n' :: Float))) $
                 go [] n pageSizeEmpty)
      ]
    where
      go es 0 _  = return (PageLogical (sortBy (compare `on` (\(k,_,_)->k)) es))
      go es i sz = do
        e <- arbitrary
        case pageSizeAddElem e sz of
          Nothing  -> return (PageLogical es)
          Just sz' -> go (e:es) (i-1) sz'

  shrink (PageLogical p) =
    [ PageLogical p' | p' <- shrink p ]

