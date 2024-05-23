{-# LANGUAGE BlockArguments    #-}
{-# LANGUAGE OverloadedStrings #-}
module Test.Database.LSMTree.Internal.RawPage (
    -- * Main test tree
    tests,
) where

import           Control.DeepSeq (deepseq)
import qualified Data.ByteString as BS
import           Data.Maybe (fromMaybe)
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

import           Database.LSMTree.Extras.ReferenceImpl
import           Database.LSMTree.Internal.BlobRef (BlobSpan (..))
import qualified Database.LSMTree.Internal.Entry as Entry
import qualified Database.LSMTree.Internal.RawBytes as RB
import           Database.LSMTree.Internal.RawPage
import           Database.LSMTree.Internal.Serialise
import           FormatPage (pageOverflowPrefixSuffixLen)

tests :: TestTree
tests = testGroup "Database.LSMTree.Internal.RawPage"
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

        (page, []) @=? toRawPage (PageContentFits [])
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

        assertEqualRawPages page $ fst (toRawPage (PageContentFits [(Key "\x42\x43", Insert (Value "\x88\x99") Nothing)]))
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

        (page, []) @=? toRawPage (PageContentFits [(Key "\x42\x43", Insert (Value "\x88\x99") (Just (BlobRef 0xff 0xfe)))])
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

        (page, []) @=? toRawPage (PageContentFits [(Key "\x42\x43", Delete)])
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

        (page, []) @=? toRawPage (PageContentFits [(Key "\x42\x43", Mupsert (Value "\x44\x45")), (Key "\x52\x53", Mupsert (Value "\x54\x55"))])
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
    , testProperty "rawPageOverflowPages" prop_rawPageOverflowPages
    , testProperty "from/to reference impl" prop_fromToReferenceImpl
    ]

prop_toRawPage :: PageContentFits -> Property
prop_toRawPage p =
  let (_rawpage, overflowPages) = toRawPage p
   in tabulate "toRawPage number of overflowPages"
               [ show (length overflowPages) ] $
      property (deepseq overflowPages True)

prop_keys :: PageContentFits -> Property
prop_keys (PageContentFits kops) =
        fromIntegral (rawPageNumKeys rawpage) === length kops
    .&&.
        rawPageKeys rawpage
    === V.fromList [ toSerialisedKey k | (k, _) <- kops ]
  where
    rawpage = fst $ toRawPage (PageContentFits kops)

prop_values :: PageContentFits -> Property
prop_values (PageContentFits kops) =
    length kops /= 1 ==>
        rawPageValues rawpage
    === V.fromList [ toSerialisedValue (extractValue op) | (_, op) <- kops ]
  where
    rawpage = fst $ toRawPage (PageContentFits kops)

    extractValue (Insert  v _) = v
    extractValue (Mupsert v)   = v
    extractValue Delete        = Value BS.empty

prop_blobspans :: PageContentFits -> Property
prop_blobspans (PageContentFits kops) =
        [ rawPageBlobSpanIndex rawpage i
        | i <- [0 .. fromIntegral (rawPageNumBlobs rawpage) - 1] ]
    === [ toBlobSpan b | (_, Insert _ (Just b)) <- kops ]
  where
    rawpage = fst $ toRawPage (PageContentFits kops)

prop_hasblobspans :: PageContentFits -> Property
prop_hasblobspans (PageContentFits kops) =
      [ rawPageHasBlobSpanAt rawpage i /= 0 | i <- [0 .. length kops - 1] ]
  === [ opHasBlobSpan op | (_, op) <- kops ]
  where
    rawpage = fst $ toRawPage (PageContentFits kops)

    opHasBlobSpan (Insert _ (Just _)) = True
    opHasBlobSpan _                   = False

prop_ops :: PageContentFits -> Property
prop_ops (PageContentFits kops) =
      [ rawPageOpAt rawpage i | i <- [0 .. length kops - 1] ]
  === [ fromOp op | (_, op) <- kops ]
  where
    rawpage = fst $ toRawPage (PageContentFits kops)

    fromOp :: Operation -> Word64
    fromOp Insert {}  = 0
    fromOp Delete {}  = 2
    fromOp Mupsert {} = 1

prop_rawPageOverflowPages :: PageContentFits -> Property
prop_rawPageOverflowPages (PageContentFits kops) =
    rawPageOverflowPages page === length overflowPages
  where
    (page, overflowPages) = toRawPage (PageContentFits kops)

prop_fromToReferenceImpl :: PageContentFits -> Property
prop_fromToReferenceImpl (PageContentFits kops) =
    -- serialise using the reference impl
    -- deserialise using the real impl and convert back to the reference types
        fromRawPage (toRawPage (PageContentFits kops))
    === PageContentFits kops

prop_entries_exists :: PageContentOrdered -> Property
prop_entries_exists (PageContentOrdered kops) =
    length kops > 1 ==>
    foldr1 (.&&.)
      [     rawPageLookup rawpage (toSerialisedKey k)
        === LookupEntry (toEntry op)
      | (k,op) <- kops ]
  where
    rawpage = fst $ toRawPage (PageContentFits kops)

prop_entries_all :: PageContentOrdered -> Key -> Property
prop_entries_all (PageContentOrdered kops) k =
    length kops /= 1 ==>
        rawPageLookup rawpage (toSerialisedKey k)
    === maybe LookupEntryNotPresent
              (LookupEntry . toEntry)
              (lookup k kops)
  where
    rawpage = fst $ toRawPage (PageContentFits kops)

prop_big_insert :: Key -> Maybe BlobRef -> Property
prop_big_insert k mblobref =
    rawPageLookup rawpage (toSerialisedKey k)
   ===
    let (prefixlen, suffixlen) =
           fromMaybe
            (error "expected overflow pages")
            (pageOverflowPrefixSuffixLen =<< encodePage DiskPage4k kops)
     in LookupEntryOverflow (toEntryPrefix op prefixlen)
                            (fromIntegral suffixlen)
  where
    v       = Value (BS.replicate 5000 42)
    op      = Insert v mblobref
    kops    = [(k, op)]
    rawpage = fst $ toRawPage (PageContentFits kops)

prop_single_entry :: PageContentSingle -> Property
prop_single_entry (PageContentSingle k op) =
    label ("pages " ++ show (length overflowPages)) $

    rawPageLookup rawpage (toSerialisedKey k)
   ===
    case pageOverflowPrefixSuffixLen =<< encodePage DiskPage4k [(k, op)] of
      Nothing ->
        LookupEntry (toEntry op)

      Just (prefixlen, suffixlen) ->
        LookupEntryOverflow (toEntryPrefix op prefixlen)
                            (fromIntegral suffixlen)
  where
    (rawpage, overflowPages) = toRawPage (PageContentFits [(k, op)])

