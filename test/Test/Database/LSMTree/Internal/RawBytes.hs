{- HLINT ignore "Avoid restricted alias" -}
{- HLINT ignore "Use /=" -}

module Test.Database.LSMTree.Internal.RawBytes (tests) where

import           Database.LSMTree.Extras.Generators ()
import           Database.LSMTree.Internal.RawBytes (RawBytes)
import qualified Database.LSMTree.Internal.RawBytes as RawBytes (size)
import           Test.QuickCheck (Property, collect, mapSize, withDiscardRatio,
                     (.||.), (===), (==>))
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.QuickCheck (testProperty)

-- * Tests

tests :: TestTree
tests = testGroup "Test.Database.LSMTree.Internal.RawBytes" $
        [
            testGroup "Eq laws" $
            [
                testProperty "Reflexivity"  propEqReflexivity,
                testProperty "Symmetry"     propEqSymmetry,
                testProperty "Transitivity" propEqTransitivity,
                testProperty "Negation"     propEqNegation
            ],
            testGroup "Ord laws" $
            [
                testProperty "Comparability" propOrdComparability,
                testProperty "Transitivity"  propOrdTransitivity,
                testProperty "Reflexivity"   propOrdReflexivity,
                testProperty "Antisymmetry"  propOrdAntisymmetry
            ]
        ]

-- * Utilities

withFirstBlockSizeInfo :: RawBytes -> Property -> Property
withFirstBlockSizeInfo firstBlock
    = collect ("Size of first block is " ++ show (RawBytes.size firstBlock))

-- * Properties to test

-- ** 'Eq' laws

propEqReflexivity :: RawBytes -> Property
propEqReflexivity block = block === block

propEqSymmetry :: RawBytes -> RawBytes -> Property
propEqSymmetry block1 block2 = (block1 == block2) === (block2 == block1)

propEqTransitivity :: Property
propEqTransitivity = mapSize (const 3)     $
                     withDiscardRatio 1000 $
                     untunedProp
    where

    untunedProp :: RawBytes -> RawBytes -> RawBytes -> Property
    untunedProp block1 block2 block3
        = withFirstBlockSizeInfo block1 $
          block1 == block2 && block2 == block3 ==> block1 === block3

propEqNegation :: RawBytes -> RawBytes -> Property
propEqNegation block1 block2 = (block1 /= block2) === not (block1 == block2)

-- ** 'Ord' laws

propOrdComparability :: RawBytes -> RawBytes -> Property
propOrdComparability block1 block2 = block1 <= block2 .||. block2 <= block1

propOrdTransitivity :: RawBytes -> RawBytes -> RawBytes -> Property
propOrdTransitivity block1 block2 block3
    = block1 <= block2 && block2 <= block3 ==> block1 <= block3

propOrdReflexivity :: RawBytes -> Bool
propOrdReflexivity block = block <= block

propOrdAntisymmetry :: Property
propOrdAntisymmetry = mapSize (const 4)    $
                      withDiscardRatio 100 $
                      untunedProp
    where

    untunedProp :: RawBytes -> RawBytes -> Property
    untunedProp block1 block2
        = withFirstBlockSizeInfo block1 $
          block1 <= block2 && block2 <= block1 ==> block1 === block2
