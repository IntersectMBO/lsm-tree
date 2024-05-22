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

import           Data.Maybe (isJust)
import           FormatPage
import           Test.QuickCheck

-- | A test case consisting of a key\/operation sequence that is guaranteed to
-- either fit into a single 4k disk page, or be a single entry that spans a
-- one primary 4k disk page and zero or more overflow disk pages.
--
-- The keys /are not/ ordered.
--
newtype PageContentFits = PageContentFits [(Key, Operation)]
  deriving (Eq, Show)

-- | A test case consisting of a key\/operation sequence that is guaranteed to
-- either fit into a single 4k disk page, or be a single entry that spans a
-- one primary 4k disk page and zero or more overflow disk pages.
--
-- The keys /are/ in strictly ascending order.
--
newtype PageContentOrdered = PageContentOrdered [(Key, Operation)]
  deriving (Eq, Show)

-- | A test case consisting of a key\/operation sequence that is /not/
-- guaranteed to fit into a a 4k disk page.
--
-- These test cases are useful for checking the boundary conditions for what
-- can fit into a disk page.
--
-- The keys /are not/ ordered.
--
newtype PageContentMaybeOverfull = PageContentMaybeOverfull [(Key, Operation)]
  deriving (Eq, Show)

-- | A tests case consisting of a single key\/operation pair.
--
data PageContentSingle = PageContentSingle Key Operation
  deriving (Eq, Show)

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

-- | Stricly increasing, so no duplicates.
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

