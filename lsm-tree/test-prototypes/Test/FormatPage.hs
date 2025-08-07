{-# OPTIONS_GHC -Wno-incomplete-uni-patterns #-}

module Test.FormatPage (tests) where

import qualified Data.ByteString as BS

import           FormatPage

import           Test.QuickCheck hiding ((.&.))
import           Test.Tasty
import           Test.Tasty.QuickCheck (testProperty)

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
    , testProperty "overflow pages" prop_overflowPages
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

prop_overflowPages :: PageContentSingle -> Property
prop_overflowPages (PageContentSingle dpgsz k op) =
    label ("pages " ++ show (length ps)) $
      all ((== diskPageSizeBytes dpgsz) . BS.length) ps
 .&&. pageDiskPages p === length ps
 .&&. case pageOverflowPrefixSuffixLen p of
        Nothing -> length ps === 1
        Just (prefixlen, suffixlen) ->
              prefixlen + suffixlen === BS.length (unValue v)
         .&&.     Value (BS.drop (dpgszBytes - prefixlen) (head ps)
                      <> BS.take suffixlen (BS.concat (drop 1 ps)))
              === v
  where
    Just p = encodePage dpgsz [(k, op)]
    ps     = pageSerialisedChunks dpgsz (serialisePage p)
    v      = case op of
               Insert  v' _ -> v'
               Mupsert v'   -> v'
               Delete       -> error "unexpected"
    dpgszBytes = diskPageSizeBytes dpgsz

