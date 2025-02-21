{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}

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
import           Database.LSMTree.Extras.ReferenceImpl
import           Database.LSMTree.Internal.BlobRef (BlobSpan)
import           Database.LSMTree.Internal.Entry
import           Database.LSMTree.Internal.PageAcc (entryWouldFitInPage,
                     sizeofEntry)
import           Database.LSMTree.Internal.RawBytes (RawBytes (..))
import           Database.LSMTree.Internal.Serialise

import qualified Test.QuickCheck as QC
import           Test.QuickCheck (Property)
import           Test.Tasty (TestTree, localOption, testGroup)
import           Test.Tasty.QuickCheck (QuickCheckMaxSize (..), testProperty)
import           Test.Util.Arbitrary

tests :: TestTree
tests = testGroup "Test.Database.LSMTree.Generators" [
      testGroup "PageContentFits" $
        prop_arbitraryAndShrinkPreserveInvariant pageContentFitsInvariant
    , testGroup "PageContentOrdered" $
        prop_arbitraryAndShrinkPreserveInvariant pageContentOrderedInvariant
    , localOption (QuickCheckMaxSize 20) $ -- takes too long!
      testGroup "LogicalPageSummaries" $
        prop_arbitraryAndShrinkPreserveInvariant (pagesInvariant @Word64)
    , testGroup "Chunk size" $
        prop_arbitraryAndShrinkPreserveInvariant chunkSizeInvariant
    , testGroup "Raw bytes" $
        [testProperty "packRawBytesPinnedOrUnpinned"
                      prop_packRawBytesPinnedOrUnpinned
        ]
     ++ prop_arbitraryAndShrinkPreserveInvariant (deepseqInvariant @RawBytes)
    , testGroup "KeyForIndexCompact" $
        prop_arbitraryAndShrinkPreserveInvariant $
          isKeyForIndexCompact . getKeyForIndexCompact
    , testGroup "BiasedKeyForIndexCompact" $
        prop_arbitraryAndShrinkPreserveInvariant $
          isKeyForIndexCompact . getBiasedKeyForIndexCompact
    , testGroup "lists of key/op pairs" $
        [ testProperty "prop_distributionKOps" $
            prop_distributionKOps
        ]
    ]

prop_packRawBytesPinnedOrUnpinned :: Bool -> [Word8] -> Bool
prop_packRawBytesPinnedOrUnpinned pinned ws =
    packRawBytesPinnedOrUnpinned pinned ws == RawBytes (VP.fromList ws)

type TestEntry = Entry SerialisedValue BlobSpan
type TestKOp = (BiasedKeyForIndexCompact, TestEntry)

prop_distributionKOps :: [TestKOp] -> Property
prop_distributionKOps kops' =
    QC.tabulate "key occurrences (>1 is collision)" (map (show . snd) (Map.assocs keyCounts)) $
    QC.tabulate "key sizes" (map (showPowersOf 4 . sizeofKey) keys) $
    QC.tabulate "value sizes" (map (showPowersOf 4 . sizeofValue) values) $
    QC.tabulate "k/op sizes" (map (showPowersOf 4 . uncurry sizeofEntry) kops) $
    QC.tabulate "k/op is large" (map (show . isLarge) kops) $
    QC.checkCoverage $
    QC.cover 50 (any isLarge kops) "any k/op is large" $
    QC.cover  1 (ratioUniqueKeys < (0.9 :: Double)) ">10% of keys collide" $
    QC.cover  5 (any (> 2) keyCounts) "has key with >2 collisions" $
      True
  where
    kops = coerce kops' :: [(SerialisedKey, Entry SerialisedValue BlobSpan)]
    keys = map fst kops
    keyCounts = Map.fromListWith (+) [(k, (1 :: Int)) | k <- keys]
    uniqueKeys = Map.keys keyCounts
    ratioUniqueKeys = fromIntegral (length uniqueKeys) / fromIntegral (length keys)
    values = foldMap (bifoldMap pure mempty . snd) kops

    isLarge = not . uncurry entryWouldFitInPage
