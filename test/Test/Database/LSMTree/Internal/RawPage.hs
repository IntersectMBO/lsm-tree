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
import qualified Data.Vector.Primitive as VP
import           Data.Word (Word16, Word64)
import           GHC.Word (byteSwap16)
import           Test.QuickCheck.Instances ()
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.HUnit (testCase, (@=?))
import           Test.Tasty.QuickCheck
import           Test.Util.RawPage

import qualified Database.LSMTree.Extras.ReferenceImpl as Ref
import           Database.LSMTree.Internal.BlobRef (BlobSpan (..))
import qualified Database.LSMTree.Internal.Entry as Entry
import           Database.LSMTree.Internal.RawPage
import           Database.LSMTree.Internal.Serialise

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

        (page, []) @=? Ref.toRawPage (Ref.PageContentFits [])
        rawPageNumKeys page @=? 0
        rawPageNumBlobs page @=? 0
        rawPageKeyOffsets page @=? VP.fromList [10]
        rawPageValueOffsets page @=? VP.fromList [10]

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

        assertEqualRawPages
          page
          ((fst . Ref.toRawPage . Ref.PageContentFits)
             [(Ref.Key "\x42\x43", Ref.Insert (Ref.Value "\x88\x99") Nothing)])
        rawPageNumKeys page @=? 1
        rawPageNumBlobs page @=? 0
        rawPageKeyOffsets page @=? VP.fromList [32, 34]
        rawPageValueOffsets1 page @=? (34, 36)
        rawPageHasBlobSpanAt page 0 @=? 0
        rawPageOpAt page 0 @=? 0
        rawPageKeys page @=? V.singleton (SerialisedKey "\x42\x43")

        rawPageLookup page (SerialisedKey "\x42\x43")
          @=? LookupEntry (Entry.Insert (SerialisedValue "\x88\x99"))

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

        (page, [])
          @=? (Ref.toRawPage . Ref.PageContentFits)
                [(Ref.Key "\x42\x43",
                  Ref.Insert (Ref.Value "\x88\x99")
                             (Just (Ref.BlobRef 0xff 0xfe)))]
        rawPageNumKeys page @=? 1
        rawPageNumBlobs page @=? 1
        rawPageKeyOffsets page @=? VP.fromList [44, 46]
        rawPageValueOffsets1 page @=? (46, 48)
        rawPageHasBlobSpanAt page 0 @=? 1
        rawPageBlobSpanIndex page 0 @=? BlobSpan 0xff 0xfe
        rawPageOpAt page 0 @=? 0
        rawPageKeys page @=? V.singleton (SerialisedKey "\x42\x43")

        rawPageLookup page (SerialisedKey "\x42\x43")
          @=? LookupEntry (Entry.InsertWithBlob (SerialisedValue "\x88\x99")
                                                (BlobSpan 0xff 0xfe))

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

        (page, [])
          @=? (Ref.toRawPage . Ref.PageContentFits)
                [(Ref.Key "\x42\x43", Ref.Delete)]
        rawPageNumKeys page @=? 1
        rawPageNumBlobs page @=? 0
        rawPageKeyOffsets page @=? VP.fromList [32, 34]
        rawPageValueOffsets1 page @=? (34, 34)
        rawPageHasBlobSpanAt page 0 @=? 0
        rawPageOpAt page 0 @=? 2
        rawPageKeys page @=? V.singleton (SerialisedKey "\x42\x43")

        rawPageLookup page (SerialisedKey "\x42\x43")
          @=? LookupEntry Entry.Delete

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

        (page, [])
          @=? (Ref.toRawPage . Ref.PageContentFits)
                [(Ref.Key "\x42\x43", Ref.Mupsert (Ref.Value "\x44\x45")),
                 (Ref.Key "\x52\x53", Ref.Mupsert (Ref.Value "\x54\x55"))]
        rawPageNumKeys page @=? 2
        rawPageNumBlobs page @=? 0
        rawPageKeyOffsets page @=? VP.fromList [34, 36, 38]
        rawPageValueOffsets page @=? VP.fromList [38, 40, 42]
        rawPageHasBlobSpanAt page 0 @=? 0
        rawPageHasBlobSpanAt page 1 @=? 0
        rawPageOpAt page 0 @=? 1
        rawPageOpAt page 1 @=? 1
        rawPageKeys page @=? V.fromList [SerialisedKey "\x42\x43",
                                         SerialisedKey "\x52\x53"]
        rawPageValues page @=? V.fromList [SerialisedValue "\x44\x45",
                                           SerialisedValue "\x54\x55"]

        rawPageLookup page (SerialisedKey "\x52\x53")
          @=? LookupEntry (Entry.Mupdate (SerialisedValue "\x54\x55"))
        rawPageLookup page (SerialisedKey "\x99\x99")
          @=? LookupEntryNotPresent

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

prop_toRawPage :: Ref.PageContentFits -> Property
prop_toRawPage p =
  let (_rawpage, overflowPages) = Ref.toRawPage p
   in tabulate "toRawPage number of overflowPages"
               [ show (length overflowPages) ] $
      property (deepseq overflowPages True)

prop_keys :: Ref.PageContentFits -> Property
prop_keys (Ref.PageContentFits kops) =
        fromIntegral (rawPageNumKeys rawpage) === length kops
    .&&.
        rawPageKeys rawpage
    === V.fromList [ Ref.toSerialisedKey k | (k, _) <- kops ]
  where
    rawpage = fst $ Ref.toRawPage (Ref.PageContentFits kops)

prop_values :: Ref.PageContentFits -> Property
prop_values (Ref.PageContentFits kops) =
    length kops /= 1 ==>
        rawPageValues rawpage
    === V.fromList [ Ref.toSerialisedValue (extractValue op) | (_, op) <- kops ]
  where
    rawpage = fst $ Ref.toRawPage (Ref.PageContentFits kops)

    extractValue (Ref.Insert  v _) = v
    extractValue (Ref.Mupsert v)   = v
    extractValue Ref.Delete        = Ref.Value BS.empty

prop_blobspans :: Ref.PageContentFits -> Property
prop_blobspans (Ref.PageContentFits kops) =
        [ rawPageBlobSpanIndex rawpage i
        | i <- [0 .. fromIntegral (rawPageNumBlobs rawpage) - 1] ]
    === [ Ref.toBlobSpan b | (_, Ref.Insert _ (Just b)) <- kops ]
  where
    rawpage = fst $ Ref.toRawPage (Ref.PageContentFits kops)

prop_hasblobspans :: Ref.PageContentFits -> Property
prop_hasblobspans (Ref.PageContentFits kops) =
      [ rawPageHasBlobSpanAt rawpage i /= 0 | i <- [0 .. length kops - 1] ]
  === [ opHasBlobSpan op | (_, op) <- kops ]
  where
    rawpage = fst $ Ref.toRawPage (Ref.PageContentFits kops)

    opHasBlobSpan (Ref.Insert _ (Just _)) = True
    opHasBlobSpan _                       = False

prop_ops :: Ref.PageContentFits -> Property
prop_ops (Ref.PageContentFits kops) =
      [ rawPageOpAt rawpage i | i <- [0 .. length kops - 1] ]
  === [ fromOp op | (_, op) <- kops ]
  where
    rawpage = fst $ Ref.toRawPage (Ref.PageContentFits kops)

    fromOp :: Ref.Operation -> Word64
    fromOp Ref.Insert {}  = 0
    fromOp Ref.Delete {}  = 2
    fromOp Ref.Mupsert {} = 1

prop_rawPageOverflowPages :: Ref.PageContentFits -> Property
prop_rawPageOverflowPages (Ref.PageContentFits kops) =
    rawPageOverflowPages page === length overflowPages
  where
    (page, overflowPages) = Ref.toRawPage (Ref.PageContentFits kops)

prop_fromToReferenceImpl :: Ref.PageContentFits -> Property
prop_fromToReferenceImpl (Ref.PageContentFits kops) =
    -- serialise using the reference impl
    -- deserialise using the real impl and convert back to the reference types
        Ref.fromRawPage (Ref.toRawPage (Ref.PageContentFits kops))
    === Ref.PageContentFits kops

prop_entries_exists :: Ref.PageContentOrdered -> Property
prop_entries_exists (Ref.PageContentOrdered kops) =
    length kops > 1 ==>
    foldr1 (.&&.)
      [     rawPageLookup rawpage (Ref.toSerialisedKey k)
        === LookupEntry (Ref.toEntry op)
      | (k,op) <- kops ]
  where
    rawpage = fst $ Ref.toRawPage (Ref.PageContentFits kops)

prop_entries_all :: Ref.PageContentOrdered -> Ref.Key -> Property
prop_entries_all (Ref.PageContentOrdered kops) k =
    length kops /= 1 ==>
        rawPageLookup rawpage (Ref.toSerialisedKey k)
    === maybe LookupEntryNotPresent
              (LookupEntry . Ref.toEntry)
              (lookup k kops)
  where
    rawpage = fst $ Ref.toRawPage (Ref.PageContentFits kops)

prop_big_insert :: Ref.Key -> Maybe Ref.BlobRef -> Property
prop_big_insert k mblobref =
    rawPageLookup rawpage (Ref.toSerialisedKey k)
   ===
    let (prefixlen, suffixlen) =
           fromMaybe
            (error "expected overflow pages")
            (Ref.pageOverflowPrefixSuffixLen
               =<< Ref.encodePage Ref.DiskPage4k kops)
     in LookupEntryOverflow (Ref.toEntryPrefix op prefixlen)
                            (fromIntegral suffixlen)
  where
    v       = Ref.Value (BS.replicate 5000 42)
    op      = Ref.Insert v mblobref
    kops    = [(k, op)]
    rawpage = fst $ Ref.toRawPage (Ref.PageContentFits kops)

prop_single_entry :: Ref.PageContentSingle -> Property
prop_single_entry (Ref.PageContentSingle k op) =
    label ("pages " ++ show (length overflowPages)) $

    rawPageLookup rawpage (Ref.toSerialisedKey k)
   ===
    case Ref.pageOverflowPrefixSuffixLen
           =<< Ref.encodePage Ref.DiskPage4k [(k, op)] of
      Nothing ->
        LookupEntry (Ref.toEntry op)

      Just (prefixlen, suffixlen) ->
        LookupEntryOverflow (Ref.toEntryPrefix op prefixlen)
                            (fromIntegral suffixlen)
  where
    (rawpage, overflowPages) = Ref.toRawPage (Ref.PageContentFits [(k, op)])

