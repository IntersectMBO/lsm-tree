module Test.Database.LSMTree.Internal.RawOverflowPage (
    -- * Main test tree
    tests,
) where

import qualified Data.Primitive.ByteArray as BA
import qualified Data.Vector.Primitive as PV

import           Test.QuickCheck
import           Test.QuickCheck.Instances ()
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.QuickCheck

import           Database.LSMTree.Extras.Generators (LargeRawBytes (..))
import           Database.LSMTree.Internal.BitMath
import qualified Database.LSMTree.Internal.RawBytes as RB
import           Database.LSMTree.Internal.RawOverflowPage

tests :: TestTree
tests =
  testGroup "Database.LSMTree.Internal.RawOverflowPage"
    [ testProperty "RawBytes prefix to RawOverflowPage"
                   prop_rawBytesToRawOverflowPage
    , testProperty "RawBytes to [RawOverflowPage]"
                   prop_rawBytesToRawOverflowPages
    ]

-- | Converting up to the first 4096 bytes of a 'RawBytes' to an
-- 'RawOverflowPage' and back gives us the original, padded with zeros to a
-- multiple of the page size.
prop_rawBytesToRawOverflowPage :: LargeRawBytes -> Property
prop_rawBytesToRawOverflowPage
  (LargeRawBytes bytes@(RB.RawBytes (PV.Vector off len ba))) =
    label (if RB.size bytes >= 4096 then "large" else "small") $
    label (if BA.isByteArrayPinned ba then "pinned" else "unpinned") $
    label (if off == 0 then "offset 0" else "offset non-0") $

        rawOverflowPageRawBytes (makeRawOverflowPage ba off (min len 4096))
    === RB.take 4096 bytes <> padding
  where
    padding    = RB.fromVector (PV.replicate paddinglen 0)
    paddinglen = 4096 - (min len 4096)


-- | Converting the bytes to @[RawOverflowPage]@ and back gives us the original
-- bytes, padded with zeros to a multiple of the page size.
--
prop_rawBytesToRawOverflowPages :: LargeRawBytes -> Property
prop_rawBytesToRawOverflowPages (LargeRawBytes bytes) =
        length pages === roundUpToPageSize (RB.size bytes) `div` 4096
   .&&. mconcat (map rawOverflowPageRawBytes pages) === bytes <> padding
  where
    pages      = rawBytesToOverflowPages bytes
    padding    = RB.fromVector (PV.replicate paddinglen 0)
    paddinglen = let trailing = RB.size bytes `mod` 4096 in
                 if trailing == 0 then 0 else 4096 - trailing

