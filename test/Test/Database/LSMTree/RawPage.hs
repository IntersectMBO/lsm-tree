{-# LANGUAGE BlockArguments    #-}
{-# LANGUAGE OverloadedStrings #-}
module Test.Database.LSMTree.RawPage (
    -- * Main test tree
    tests,
) where

import           Control.DeepSeq (deepseq)
import qualified Data.ByteString as BS
import           Data.Maybe (isJust)
import           Data.Primitive.ByteArray (byteArrayFromList)
import qualified Data.Vector as V
import qualified Data.Vector.Primitive as P
import           Data.Word (Word16, Word64)
import           GHC.Word (byteSwap16)
import           Test.QuickCheck.Instances ()
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.HUnit (testCase, (@=?))
import           Test.Tasty.QuickCheck
import           Test.Util.RawPage

import           Database.LSMTree.BlobRef (BlobSpan (..))
import qualified Database.LSMTree.Entry as Entry
import           Database.LSMTree.RawOverflowPage
import           Database.LSMTree.RawPage
import           Database.LSMTree.Serialise
import qualified Database.LSMTree.Serialise.RawBytes as RB
import           FormatPage (BlobRef (..), Key (..), Operation (..),
                     PageLogical (..), Value (..), unKey)

tests :: TestTree
tests = testGroup "Database.LSMTree.RawPage"
    [ testCase "empty" $ do
        -- directory:
        -- * 0 keys,
        -- * 0 blobs
        -- * key's offset offset at 8 bytes (as bitmaps are empty)
        -- * nil spare
        --
        -- and
        -- offset past last value.
        let bytes :: [Word16]
            bytes = [0, 0, 8, 0, 10]

        let page = makeRawPage (byteArrayFromList bytes) 0

        (page, []) @=? toRawPage (PageLogical [])
        rawPageNumKeys page @=? 0
        rawPageNumBlobs page @=? 0
        rawPageKeyOffsets page @=? P.fromList [10]
        rawPageValueOffsets page @=? P.fromList [10]

    , testCase "single-insert" $ do
        let bytes :: [Word16]
            bytes =
                [ 1, 0, 24, 0             -- directory
                , 0, 0, 0, 0, 0, 0, 0, 0  -- ...
                , 32                      -- key offsets
                , 34, 36                  -- value offsets
                , 0x00                    -- single key case (top bits of 32bit offset)
                , byteSwap16 0x4243       -- key
                , byteSwap16 0x8899       -- value
                ]

        let page = makeRawPage (byteArrayFromList bytes) 0

        assertEqualRawPages page $ fst (toRawPage (PageLogical [(Key "\x42\x43", Insert (Value "\x88\x99"), Nothing)]))
        rawPageNumKeys page @=? 1
        rawPageNumBlobs page @=? 0
        rawPageKeyOffsets page @=? P.fromList [32, 34]
        rawPageValueOffsets1 page @=? (34, 36)
        rawPageHasBlobSpanAt page 0 @=? 0
        rawPageOpAt page 0 @=? 0
        rawPageKeys page @=? V.singleton (SerialisedKey $ RB.pack [0x42, 0x43])

        rawPageLookup page (SerialisedKey $ RB.pack [0x42, 0x43]) @=? LookupEntry (Entry.Insert (SerialisedValue $ RB.pack [0x88, 0x99]))

    , testCase "single-insert-blobspan" $ do
        let bytes :: [Word16]
            bytes =
                [ 1, 1, 36, 0             -- directory
                , 1, 0, 0, 0              -- has blobspan
                , 0, 0, 0, 0              -- ops
                , 0xff, 0, 0, 0
                , 0xfe, 0
                , 44                      -- key offsets
                , 46, 48                  -- value offsets
                , 0x00                    -- single key case (top bits of 32bit offset)
                , byteSwap16 0x4243       -- key
                , byteSwap16 0x8899       -- value
                ]

        let page = makeRawPage (byteArrayFromList bytes) 0

        (page, []) @=? toRawPage (PageLogical [(Key "\x42\x43", Insert (Value "\x88\x99"), Just (BlobRef 0xff 0xfe))])
        rawPageNumKeys page @=? 1
        rawPageNumBlobs page @=? 1
        rawPageKeyOffsets page @=? P.fromList [44, 46]
        rawPageValueOffsets1 page @=? (46, 48)
        rawPageHasBlobSpanAt page 0 @=? 1
        rawPageBlobSpanIndex page 0 @=? BlobSpan 0xff 0xfe
        rawPageOpAt page 0 @=? 0
        rawPageKeys page @=? V.singleton (SerialisedKey $ RB.pack [0x42, 0x43])

        rawPageLookup page (SerialisedKey $ RB.pack [0x42, 0x43]) @=? LookupEntry (Entry.InsertWithBlob (SerialisedValue $ RB.pack [0x88, 0x99]) (BlobSpan 0xff 0xfe))

    , testCase "single-delete" $ do
        let bytes :: [Word16]
            bytes =
                [ 1, 0, 24, 0             -- directory
                , 0, 0, 0, 0, 2, 0, 0, 0  -- ...
                , 32                      -- key offsets
                , 34, 34                  -- value offsets
                , 0x00                    -- single key case (top bits of 32bit offset)
                , byteSwap16 0x4243       -- key
                ]

        let page = makeRawPage (byteArrayFromList bytes) 0

        (page, []) @=? toRawPage (PageLogical [(Key "\x42\x43", Delete, Nothing)])
        rawPageNumKeys page @=? 1
        rawPageNumBlobs page @=? 0
        rawPageKeyOffsets page @=? P.fromList [32, 34]
        rawPageValueOffsets1 page @=? (34, 34)
        rawPageHasBlobSpanAt page 0 @=? 0
        rawPageOpAt page 0 @=? 2
        rawPageKeys page @=? V.singleton (SerialisedKey $ RB.pack [0x42, 0x43])

        rawPageLookup page (SerialisedKey $ RB.pack [0x42, 0x43]) @=? LookupEntry Entry.Delete

    , testCase "double-mupsert" $ do
        let bytes :: [Word16]
            bytes =
                [ 2, 0, 24, 0             -- directory
                , 0, 0, 0, 0, 5, 0, 0, 0  -- ...
                , 34, 36                  -- key offsets
                , 38, 40, 42              -- value offsets
                , byteSwap16 0x4243       -- key 1
                , byteSwap16 0x5253       -- key 2
                , byteSwap16 0x4445       -- value 1
                , byteSwap16 0x5455       -- value 2
                ]

        let page = makeRawPage (byteArrayFromList bytes) 0

        (page, []) @=? toRawPage (PageLogical [(Key "\x42\x43", Mupsert (Value "\x44\x45"), Nothing), (Key "\x52\x53", Mupsert (Value "\x54\x55"), Nothing)])
        rawPageNumKeys page @=? 2
        rawPageNumBlobs page @=? 0
        rawPageKeyOffsets page @=? P.fromList [34, 36, 38]
        rawPageValueOffsets page @=? P.fromList [38, 40, 42]
        rawPageHasBlobSpanAt page 0 @=? 0
        rawPageHasBlobSpanAt page 1 @=? 0
        rawPageOpAt page 0 @=? 1
        rawPageOpAt page 1 @=? 1
        rawPageKeys page @=? V.fromList [SerialisedKey rb | rb <- [RB.pack [0x42, 0x43], RB.pack [0x52, 0x53]]]
        rawPageValues page @=? V.fromList [SerialisedValue rb | rb <- [RB.pack [0x44, 0x45], RB.pack [0x54, 0x55]]]

        rawPageLookup page (SerialisedKey $ RB.pack [0x52, 0x53]) @=? LookupEntry (Entry.Mupdate (SerialisedValue $ RB.pack [0x54,0x55]))
        rawPageLookup page (SerialisedKey $ RB.pack [0x99, 0x99]) @=? LookupEntryNotPresent

    , testProperty "toRawPage" prop_toRawPage
    , testProperty "keys" prop_keys
    , testProperty "values" prop_values
    , testProperty "hasblobspans" prop_hasblobspans
    , testProperty "blobspans" prop_blobspans
    , testProperty "ops" prop_ops
    , testProperty "entries" prop_entries_exists
    , testProperty "missing" prop_entries_all
    , testProperty "big-insert" prop_big_insert
    , testProperty "entry" prop_single_entry
    ]

prop_toRawPage :: PageLogical -> Property
prop_toRawPage p =
  let (_rawpage, overflowPages) = toRawPage p
   in tabulate "toRawPage number of overflowPages"
               [ show (length overflowPages) ] $
      property (deepseq overflowPages True)

prop_keys :: PageLogical -> Property
prop_keys p@(PageLogical xs) =
    rawPageKeys rawpage === V.fromList keys
  where
    rawpage = fst $ toRawPage p

    keys :: [SerialisedKey]
    keys = [ SerialisedKey $ RB.fromByteString bs | (Key bs, _, _) <- xs ]

prop_values :: PageLogical -> Property
prop_values p@(PageLogical xs) =
    length xs /= 1 ==> rawPageValues rawpage === V.fromList values
  where
    rawpage = fst $ toRawPage p

    values :: [SerialisedValue]
    values = [ SerialisedValue $ extractValue op | (_, op, _) <- xs ]

    extractValue (Insert (Value bs))  = RB.fromByteString bs
    extractValue (Mupsert (Value bs)) = RB.fromByteString bs
    extractValue Delete               = RB.fromByteString BS.empty

prop_blobspans :: PageLogical -> Property
prop_blobspans p@(PageLogical xs) =
    [rawPageBlobSpanIndex rawpage i | i <- [0 .. length blobSpans - 1] ] === blobSpans
  where
    rawpage = fst $ toRawPage p

    blobSpans :: [BlobSpan]
    blobSpans = [ BlobSpan x y | (_, _, Just (BlobRef x y)) <- xs ]

prop_hasblobspans :: PageLogical -> Property
prop_hasblobspans p@(PageLogical xs) =
    [ rawPageHasBlobSpanAt rawpage i /= 0 | i <- [0 .. length xs - 1] ] === blobSpans
  where
    rawpage = fst $ toRawPage p

    blobSpans :: [Bool]
    blobSpans = [ isJust mb | (_, _, mb) <- xs ]

prop_ops :: PageLogical -> Property
prop_ops p@(PageLogical xs) =
    [ rawPageOpAt rawpage i | i <- [0 .. length xs - 1] ] === ops
  where
    rawpage = fst $ toRawPage p

    ops :: [Word64]
    ops = [ fromOp o | (_, o, _) <- xs ]

    fromOp :: Operation -> Word64
    fromOp Insert {}  = 0
    fromOp Delete {}  = 2
    fromOp Mupsert {} = 1

prop_entries_exists :: PageLogical -> Property
prop_entries_exists (PageLogical xs) =
    length xs > 1 ==> forAll (elements xs) \(Key k, op, blobref) ->
    rawPageLookup rawpage (SerialisedKey $ RB.fromByteString k) === LookupEntry case op of
        Insert (Value v)       -> case blobref of
            Nothing            -> Entry.Insert (SerialisedValue $ RB.fromByteString v)
            Just (BlobRef x y) -> Entry.InsertWithBlob (SerialisedValue $ RB.fromByteString v) (BlobSpan x y)
        Mupsert (Value v)      -> Entry.Mupdate (SerialisedValue $ RB.fromByteString v)
        Delete                 -> Entry.Delete
  where
    rawpage = fst $ toRawPage (PageLogical xs)

prop_entries_all :: PageLogical -> BS.ByteString -> Property
prop_entries_all page@(PageLogical xs) bs =
    length xs /= 1 ==> rawPageLookup rawpage k === expected
  where
    k = SerialisedKey $ RB.fromByteString bs
    rawpage = fst $ toRawPage page

    lookup3 :: Eq a => a -> [(a,b,c)] -> Maybe (b, c)
    lookup3 _ []            = Nothing
    lookup3 a ((a',b,c):zs) = if a == a' then Just (b, c) else lookup3 a zs

    expected :: RawPageLookup (Entry.Entry SerialisedValue BlobSpan)
    expected = case lookup3 (Key bs) xs of
        Nothing                                     -> LookupEntryNotPresent
        Just (Insert (Value v), Nothing)            -> LookupEntry (Entry.Insert (SerialisedValue $ RB.fromByteString v))
        Just (Insert (Value v), Just (BlobRef x y)) -> LookupEntry (Entry.InsertWithBlob (SerialisedValue $ RB.fromByteString v) (BlobSpan x y))
        Just (Mupsert (Value v), _)                 -> LookupEntry (Entry.Mupdate (SerialisedValue $ RB.fromByteString v))
        Just (Delete, _)                            -> LookupEntry Entry.Delete

prop_big_insert :: Key -> Maybe BlobRef -> Property
prop_big_insert k mblobref =
    case rawPageLookup rawpage k' of
      LookupEntryOverflow entry suffixlen ->
        equivalentOpEntry (Insert v) mblobref
                          entry overflowPagesBS (fromIntegral suffixlen)

      other -> counterexample (show other) False
  where
    page = PageLogical [(k, Insert v, mblobref)]
    (rawpage, overflowPages) = toRawPage page
    overflowPagesBS = overflowPagesToByteString overflowPages
    k' = SerialisedKey $ RB.fromByteString (unKey k)

    -- original value
    v = Value (BS.replicate 5000 42)

prop_single_entry :: Key -> Operation -> Maybe BlobRef -> Property
prop_single_entry k op mblobref = label (show $ null overflowPages) $
    case rawPageLookup rawpage k' of
      LookupEntryNotPresent ->
        counterexample "LookupEntryNotPresent" False

      LookupEntry e ->
        equivalentOpEntry op mblobref
                          e overflowPagesBS 0

      LookupEntryOverflow e suffixlen ->
        equivalentOpEntry op mblobref
                          e overflowPagesBS (fromIntegral suffixlen)
  where
    page = PageLogical [(k, op, mblobref)]
    (rawpage, overflowPages) = toRawPage page
    overflowPagesBS = overflowPagesToByteString overflowPages
    k' = SerialisedKey $ RB.fromByteString (unKey k)

-- | Check that an 'Operation' and optional 'Blobref' (from the reference
-- implementation) is equivalent to (from the real implementation) the
-- combination of an 'Entry', containing the prefix of a value, plus a given
-- length suffix from the overflow pages.
equivalentOpEntry :: Operation -> Maybe BlobRef
                  -> Entry.Entry SerialisedValue BlobSpan
                  -> BS.ByteString
                  -> Int
                  -> Property
equivalentOpEntry (Insert v) Nothing
                  (Entry.Insert v') overflowPagesBS suffixlen =
    equivalentValue v v' overflowPagesBS suffixlen

equivalentOpEntry (Insert v) (Just blobref)
                  (Entry.InsertWithBlob v' blobspan) overflowPagesBS suffixlen =
    equivalentValue v v' overflowPagesBS suffixlen .&&.
    equivalentBlobRefSpan blobref blobspan

equivalentOpEntry (Mupsert v) _
                  (Entry.Mupdate v') overflowPagesBS suffixlen =
    equivalentValue v v' overflowPagesBS suffixlen

equivalentOpEntry Delete _ Entry.Delete _ 0 =
    property True

equivalentOpEntry op mblobref e _overflowPagesBS suffixlen =
    counterexample (show (op, mblobref) ++ " not equivalent to "
                                        ++ show (e, suffixlen))
                   False

-- | Check that a value is equality to the combination of a 'SerialisedValue'
-- prefix, plus a given length of suffix from the overflow pages.
--
equivalentValue :: Value -> SerialisedValue -> BS.ByteString -> Int -> Property
equivalentValue (Value v) (SerialisedValue v') overflowPagesBS suffixlen =
    v === RB.toByteString v' <> BS.take suffixlen overflowPagesBS

equivalentBlobRefSpan :: BlobRef -> BlobSpan -> Property
equivalentBlobRefSpan (BlobRef x y) (BlobSpan x' y') =
    (x, y) === (x', y')

overflowPagesToByteString :: [RawOverflowPage] -> BS.ByteString
overflowPagesToByteString =
    RB.toByteString
  . mconcat
  . map rawOverflowPageRawBytes
