module Test.Util.KeyOpGenerators (
  PageContentFits(..),
  PageContentOrdered(..),
  PageContentMaybeOverfull(..),
  PageContentSingle(..),
) where

import           FormatPage
import           Test.QuickCheck

-- | A test case consisting of a key\/operation sequence that is guaranteed to
-- fit into a 4k disk page.
--
-- The keys /are not/ ordered.
--
newtype PageContentFits = PageContentFits [(Key, Operation)]
  deriving Show

-- | A test case consisting of a key\/operation sequence that is guaranteed to
-- fit into a 4k disk page.
--
-- The keys /are/ in strictly ascending order.
--
newtype PageContentOrdered = PageContentOrdered [(Key, Operation)]
  deriving Show

-- | A test case consisting of a key\/operation sequence that is /not/
-- guaranteed to fit into a a 4k disk page.
--
-- These test cases are useful for checking the boundary conditions for what
-- can fit into a disk page.
--
-- The keys /are not/ ordered.
--
newtype PageContentMaybeOverfull = PageContentMaybeOverfull [(Key, Operation)]
  deriving Show

-- | A tests case consisting of a single key\/operation pair.
--
data PageContentSingle = PageContentSingle Key Operation
  deriving Show

instance Arbitrary PageContentFits where
    arbitrary =
      PageContentFits <$>
        genPageContentFits DiskPage4k noMinKeySize

    shrink (PageContentFits kops) =
      map PageContentFits (shrinkKeyOps kops)

instance Arbitrary PageContentOrdered where
    arbitrary =
      PageContentOrdered . orderdKeyOps <$>
        genPageContentFits DiskPage4k noMinKeySize

    shrink (PageContentOrdered kops) =
      map PageContentOrdered (shrinkOrderedKeyOps kops)

instance Arbitrary PageContentMaybeOverfull where
    arbitrary =
      PageContentMaybeOverfull <$>
        genPageContentMaybeOverfull DiskPage4k noMinKeySize

    shrink (PageContentMaybeOverfull kops) =
      map PageContentMaybeOverfull (shrinkOrderedKeyOps kops)

instance Arbitrary PageContentSingle where
    arbitrary =
      uncurry PageContentSingle <$>
        genPageContentSingle DiskPage4k noMinKeySize

    shrink (PageContentSingle k op) =
      map (uncurry PageContentSingle) (shrink (k, op))

