module Test.Database.LSMTree.Generators (
    tests
  ) where

import           Data.Bifoldable (bifoldMap)
import           Data.Coerce (coerce)
import qualified Data.Map.Strict as Map
import qualified Data.Vector.Primitive as VP
import           Data.Word (Word64, Word8)

import           Database.LSMTree.Extras (showPowersOf)
import           Database.LSMTree.Extras.Generators
import           Database.LSMTree.Extras.MergingRunData
import           Database.LSMTree.Extras.MergingTreeData
import           Database.LSMTree.Extras.ReferenceImpl
import           Database.LSMTree.Extras.RunData
import           Database.LSMTree.Internal.BlobRef (BlobSpan)
import           Database.LSMTree.Internal.Entry
import qualified Database.LSMTree.Internal.MergingRun as MR
import           Database.LSMTree.Internal.PageAcc (entryWouldFitInPage,
                     sizeofEntry)
import           Database.LSMTree.Internal.RawBytes (RawBytes (..))
import qualified Database.LSMTree.Internal.RawBytes as RB
import           Database.LSMTree.Internal.Serialise


import qualified Test.QuickCheck as QC
import           Test.QuickCheck (Property)
import           Test.Tasty (TestTree, localOption, testGroup)
import           Test.Tasty.QuickCheck (QuickCheckMaxSize (..), testProperty,
                     (===))
import           Test.Util.Arbitrary

tests :: TestTree
tests = testGroup "Test.Database.LSMTree.Generators" [
      testGroup "PageContentFits" $
        prop_arbitraryAndShrinkPreserveInvariant noTags
          pageContentFitsInvariant
    , testGroup "PageContentOrdered" $
        prop_arbitraryAndShrinkPreserveInvariant noTags
          pageContentOrderedInvariant
    , localOption (QuickCheckMaxSize 20) $ -- takes too long!
      testGroup "LogicalPageSummaries" $
        prop_arbitraryAndShrinkPreserveInvariant noTags $
          pagesInvariant @Word64
    , testGroup "Chunk size" $
        prop_arbitraryAndShrinkPreserveInvariant noTags
          chunkSizeInvariant
    , testGroup "RawBytes" $
        [ testProperty "packRawBytesPinnedOrUnpinned"
            prop_packRawBytesPinnedOrUnpinned
        ]
     ++ prop_arbitraryAndShrinkPreserveInvariant labelRawBytes
          (deepseqInvariant @RawBytes)
    , testGroup "LargeRawBytes" $
        prop_arbitraryAndShrinkPreserveInvariant
          (\(LargeRawBytes rb) -> labelRawBytes rb)
          (deepseqInvariant @LargeRawBytes)
    , testGroup "KeyForIndexCompact" $
        prop_arbitraryAndShrinkPreserveInvariant noTags $
          isKeyForIndexCompact . getKeyForIndexCompact
    , testGroup "BiasedKeyForIndexCompact" $
        prop_arbitraryAndShrinkPreserveInvariant noTags $
          isKeyForIndexCompact . getBiasedKeyForIndexCompact
    , testGroup "lists of key/op pairs" $
        prop_arbitraryAndShrinkPreserveInvariant labelTestKOps $
          deepseqInvariant
    , testGroup "helpers"
        [ testProperty "prop_shrinkVec" $ \vec ->
            shrinkVec (QC.shrink @Int) vec === map VP.fromList (QC.shrink (VP.toList vec))
        ]
    , testGroup "RunData" $
        prop_arbitraryAndShrinkPreserveInvariant
          labelRunData
          noInvariant
    , testGroup "NonEmptyRunData" $
        prop_arbitraryAndShrinkPreserveInvariant
          labelNonEmptyRunData
          noInvariant
    , testGroup "MergingRunData" $
        prop_arbitraryAndShrinkPreserveInvariant
          @(SerialisedMergingRunData MR.LevelMergeType)
          labelMergingRunData
          ((=== Right ()) . mergingRunDataInvariant)
    , testGroup "MergingTreeData" $
        prop_arbitraryAndShrinkPreserveInvariant
          labelMergingTreeData
          ((=== Right ()) . mergingTreeDataInvariant)
    ]

prop_packRawBytesPinnedOrUnpinned :: Bool -> [Word8] -> Bool
prop_packRawBytesPinnedOrUnpinned pinned ws =
    packRawBytesPinnedOrUnpinned pinned ws == RawBytes (VP.fromList ws)

labelRawBytes :: RawBytes -> Property -> Property
labelRawBytes rb =
    QC.tabulate "size" [showPowersOf 2 (RB.size rb)]

type TestEntry = Entry SerialisedValue BlobSpan
type TestKOp = (BiasedKeyForIndexCompact, TestEntry)

labelTestKOps :: [TestKOp] -> Property -> Property
labelTestKOps kops' =
      QC.tabulate "key occurrences (>1 is collision)" (map (show . snd) (Map.assocs keyCounts))
    . QC.tabulate "key sizes" (map (showPowersOf 4 . sizeofKey) keys)
    . QC.tabulate "value sizes" (map (showPowersOf 4 . sizeofValue) values)
    . QC.tabulate "k/op sizes" (map (showPowersOf 4 . uncurry sizeofEntry) kops)
    . QC.tabulate "k/op is large" (map (show . isLarge) kops)
    . QC.checkCoverage
    . QC.cover 50 (any isLarge kops) "any k/op is large"
    . QC.cover  1 (ratioUniqueKeys < (0.9 :: Double)) ">10% of keys collide"
    . QC.cover  5 (any (> 2) keyCounts) "has key with >2 collisions"
  where
    kops = coerce kops' :: [(SerialisedKey, Entry SerialisedValue BlobSpan)]
    keys = map fst kops
    keyCounts = Map.fromListWith (+) [(k, (1 :: Int)) | k <- keys]
    uniqueKeys = Map.keys keyCounts
    ratioUniqueKeys = fromIntegral (length uniqueKeys) / fromIntegral (length keys)
    values = foldMap (bifoldMap pure mempty . snd) kops

    isLarge = not . uncurry entryWouldFitInPage
