module Database.LSMTree.Extras.ReferenceImpl (
    -- * Page types
    Key (..),
    Value (..),
    Operation (..),
    BlobRef (..),
    PageSerialised,
    PageIntermediate,

    -- * Page size
    PageSize (..),
    pageSizeEmpty,
    pageSizeAddElem,

    -- * Encoding and decoding
    DiskPageSize(..),
    encodePage,
    decodePage,
    serialisePage,
    deserialisePage,

    -- * Overflow pages
    pageOverflowPrefixSuffixLen,
    pageDiskPages,
    pageSerialisedChunks,

    -- * Conversions to real implementation types
    toRawPage,
    toRawPageMaybeOverfull,
    toEntry,
    toEntryPrefix,
    toSerialisedKey,
    toSerialisedValue,
    toBlobSpan,

    -- * Conversions from real implementation types
    fromRawPage,
    fromEntry,
    fromEntryPrefix,
    fromSerialisedKey,
    fromSerialisedValue,
    fromSerialisedValuePrefix,
    fromRawOverflowPages,
    fromBlobSpan,

    -- * Test case types and generators
    PageContentFits(..),
    pageContentFitsInvariant,
    PageContentOrdered(..),
    pageContentOrderedInvariant,
    PageContentMaybeOverfull(..),
    PageContentSingle(..),

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

import qualified Data.ByteString as BS
import qualified Data.ByteString.Short as SBS
import           Data.Maybe (fromMaybe, isJust)
import           Data.Primitive.ByteArray (ByteArray (..))

import           Database.LSMTree.Internal.BlobRef (BlobSpan (..))
import           Database.LSMTree.Internal.Entry (Entry)
import qualified Database.LSMTree.Internal.Entry as Entry
import qualified Database.LSMTree.Internal.RawBytes as RB
import           Database.LSMTree.Internal.RawOverflowPage
import           Database.LSMTree.Internal.RawPage
import           Database.LSMTree.Internal.Serialise
import           FormatPage

import           Test.QuickCheck

-- | A test case consisting of a key\/operation sequence that is guaranteed to
-- either fit into a single 4k disk page, or be a single entry that spans a
-- one primary 4k disk page and zero or more overflow disk pages.
--
-- The keys /are not/ ordered.
--
newtype PageContentFits = PageContentFits [(Key, Operation)]
  deriving stock (Eq, Show)

-- | A test case consisting of a key\/operation sequence that is guaranteed to
-- either fit into a single 4k disk page, or be a single entry that spans a
-- one primary 4k disk page and zero or more overflow disk pages.
--
-- The keys /are/ in strictly ascending order.
--
newtype PageContentOrdered = PageContentOrdered [(Key, Operation)]
  deriving stock (Eq, Show)

-- | A test case consisting of a key\/operation sequence that is /not/
-- guaranteed to fit into a a 4k disk page.
--
-- These test cases are useful for checking the boundary conditions for what
-- can fit into a disk page.
--
-- The keys /are not/ ordered.
--
newtype PageContentMaybeOverfull = PageContentMaybeOverfull [(Key, Operation)]
  deriving stock (Eq, Show)

-- | A tests case consisting of a single key\/operation pair.
--
data PageContentSingle = PageContentSingle Key Operation
  deriving stock (Eq, Show)

instance Arbitrary PageContentFits where
    arbitrary =
      PageContentFits <$>
        genPageContentFits DiskPage4k noMinKeySize

    shrink (PageContentFits kops) =
      map PageContentFits (shrinkKeyOps kops)

pageContentFitsInvariant :: PageContentFits -> Bool
pageContentFitsInvariant (PageContentFits kops) =
    isJust (calcPageSize DiskPage4k kops)

instance Arbitrary PageContentOrdered where
    arbitrary =
      PageContentOrdered . orderdKeyOps <$>
        genPageContentFits DiskPage4k noMinKeySize

    shrink (PageContentOrdered kops) =
      map PageContentOrdered (shrinkOrderedKeyOps kops)

-- | Strictly increasing, so no duplicates.
pageContentOrderedInvariant :: PageContentOrdered -> Bool
pageContentOrderedInvariant (PageContentOrdered kops) =
    pageContentFitsInvariant (PageContentFits kops)
 && let ks = map fst kops
     in and (zipWith (<) ks (drop 1 ks))

instance Arbitrary PageContentMaybeOverfull where
    arbitrary =
      PageContentMaybeOverfull <$>
        genPageContentMaybeOverfull DiskPage4k noMinKeySize

    shrink (PageContentMaybeOverfull kops) =
      map PageContentMaybeOverfull (shrinkKeyOps kops)

instance Arbitrary PageContentSingle where
    arbitrary =
      uncurry PageContentSingle <$>
        genPageContentSingle DiskPage4k noMinKeySize

    shrink (PageContentSingle k op) =
      map (uncurry PageContentSingle) (shrink (k, op))

-------------------------------------------------------------------------------
-- Conversions between reference and real implementation types
--

-- | Convert from 'PageContentFits' (from the reference implementation)
-- to 'RawPage' (from the real implementation).
--
toRawPage :: PageContentFits -> (RawPage, [RawOverflowPage])
toRawPage (PageContentFits kops) =
    fromMaybe overfull (toRawPageMaybeOverfull (PageContentMaybeOverfull kops))
  where
    overfull =
      error $ "toRawPage: encountered overfull page, but PageContentFits is "
           ++ "supposed to be guaranteed to fit, i.e. not to be overfull."

toRawPageMaybeOverfull :: PageContentMaybeOverfull
                       -> Maybe (RawPage, [RawOverflowPage])
toRawPageMaybeOverfull (PageContentMaybeOverfull kops) = do
    serialised <- serialisePage <$> encodePage DiskPage4k kops
    (page : overflowPages) <- pure (pageSerialisedChunks DiskPage4k serialised)
    pure (makeRawPageBS page, map makeRawOverflowPageBS overflowPages)

makeRawPageBS :: BS.ByteString -> RawPage
makeRawPageBS bs =
    case SBS.toShort bs of
      SBS.SBS ba -> makeRawPage (ByteArray ba) 0

makeRawOverflowPageBS :: BS.ByteString -> RawOverflowPage
makeRawOverflowPageBS bs =
    case SBS.toShort bs of
      SBS.SBS ba -> makeRawOverflowPage (ByteArray ba) 0 (BS.length bs)

fromRawPage :: (RawPage, [RawOverflowPage]) -> PageContentFits
fromRawPage (page, overflowPages)
  | rawPageNumKeys page == 1
  = PageContentFits . (:[]) $
      case rawPageIndex page 0 of
        IndexEntry k e ->
          (fromSerialisedKey k, fromEntry e)

        IndexEntryOverflow k e suffixlen ->
          ( fromSerialisedKey k
          , fromEntryPrefix e (fromIntegral suffixlen) overflowPages
          )

        IndexNotPresent -> error "fromRawPage: 'rawPageIndex page 0' fails"

  | otherwise
  = PageContentFits
      [ case rawPageIndex page (fromIntegral i) of
          IndexEntry k e -> (fromSerialisedKey k, fromEntry e)
          _              -> error "fromRawPage: 'rawPageIndex page i' fails"
      | i <- [0 .. fromIntegral (rawPageNumKeys page) - 1 :: Int] ]

toEntry :: Operation -> Entry SerialisedValue BlobSpan
toEntry op =
    case op of
      Insert v Nothing ->
        Entry.Insert (toSerialisedValue v)

      Insert v (Just b) ->
        Entry.InsertWithBlob (toSerialisedValue v) (toBlobSpan b)

      Mupsert v ->
        Entry.Mupdate (toSerialisedValue v)

      Delete ->
        Entry.Delete

toEntryPrefix :: Operation -> Int -> Entry SerialisedValue BlobSpan
toEntryPrefix op prefixlen =
    case op of
      Insert v Nothing ->
        Entry.Insert (toSerialisedValue (takeValue prefixlen v))

      Insert v (Just b) ->
        Entry.InsertWithBlob (toSerialisedValue (takeValue prefixlen v))
                             (toBlobSpan b)

      Mupsert v ->
        Entry.Mupdate (toSerialisedValue (takeValue prefixlen v))

      Delete ->
        Entry.Delete
  where
    takeValue n (Value v) = Value (BS.take n v)

fromEntry :: Entry SerialisedValue BlobSpan -> Operation
fromEntry e =
    case e of
      Entry.Insert v ->
        Insert (fromSerialisedValue v) Nothing

      Entry.InsertWithBlob v b ->
        Insert (fromSerialisedValue v) (Just (fromBlobSpan b))

      Entry.Mupdate v ->
        Mupsert (fromSerialisedValue v)

      Entry.Delete ->
        Delete

fromEntryPrefix :: Entry SerialisedValue BlobSpan
                -> Int
                -> [RawOverflowPage]
                -> Operation
fromEntryPrefix e suffix overflow =
    case e of
      Entry.Insert v ->
        Insert (fromSerialisedValuePrefix v suffix overflow) Nothing

      Entry.InsertWithBlob v b ->
        Insert (fromSerialisedValuePrefix v suffix overflow)
               (Just (fromBlobSpan b))

      Entry.Mupdate v ->
        Mupsert (fromSerialisedValuePrefix v suffix overflow)

      Entry.Delete ->
        Delete

toSerialisedKey :: Key -> SerialisedKey
toSerialisedKey (Key k) = SerialisedKey (RB.fromByteString k)

toSerialisedValue :: Value -> SerialisedValue
toSerialisedValue (Value v) = SerialisedValue (RB.fromByteString v)

fromSerialisedKey :: SerialisedKey -> Key
fromSerialisedKey (SerialisedKey k) = Key (RB.toByteString k)

fromSerialisedValue :: SerialisedValue -> Value
fromSerialisedValue (SerialisedValue v) = Value (RB.toByteString v)

fromSerialisedValuePrefix :: SerialisedValue -> Int -> [RawOverflowPage]
                          -> Value
fromSerialisedValuePrefix (SerialisedValue v) suffixlen overflowPages =
    Value $ RB.toByteString v
         <> BS.take suffixlen (fromRawOverflowPages overflowPages)

fromRawOverflowPages :: [RawOverflowPage] -> BS.ByteString
fromRawOverflowPages =
    RB.toByteString
  . mconcat
  . map rawOverflowPageRawBytes

toBlobSpan :: BlobRef -> BlobSpan
toBlobSpan  (BlobRef x y) = BlobSpan x y

fromBlobSpan :: BlobSpan -> BlobRef
fromBlobSpan  (BlobSpan x y) = BlobRef x y
