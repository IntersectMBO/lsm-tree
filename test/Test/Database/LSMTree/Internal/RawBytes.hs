module Test.Database.LSMTree.Internal.RawBytes (tests) where

import           Database.LSMTree.Extras.Generators ()
import           Database.LSMTree.Internal.RawBytes (RawBytes)
import qualified Database.LSMTree.Internal.RawBytes as RB (size)
import           Test.QuickCheck (Property, classify, collect, mapSize,
                     withDiscardRatio, withMaxSuccess, (.||.), (===), (==>))
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.QuickCheck (testProperty)

-- * Tests

tests :: TestTree
tests = testGroup "Test.Database.LSMTree.Internal.RawBytes" $
        [
            testGroup "Eq laws" $
            [
                testProperty "Reflexivity"  prop_eqReflexivity,
                testProperty "Symmetry"     prop_eqSymmetry,
                testProperty "Transitivity" prop_eqTransitivity,
                testProperty "Negation"     prop_eqNegation
            ],
            testGroup "Ord laws" $
            [
                testProperty "Comparability" prop_ordComparability,
                testProperty "Transitivity"  prop_ordTransitivity,
                testProperty "Reflexivity"   prop_ordReflexivity,
                testProperty "Antisymmetry"  prop_ordAntisymmetry
            ]
        ]

-- * Utilities

twoBlocksProp :: String -> RawBytes -> RawBytes -> Property -> Property
twoBlocksProp msgAddition block1 block2
    = withMaxSuccess 10000 .
      classify (block1 == block2) ("equal blocks" ++ msgAddition)

withFirstBlockSizeInfo :: RawBytes -> Property -> Property
withFirstBlockSizeInfo firstBlock
    = collect ("Size of first block is " ++ show (RB.size firstBlock))

-- * Properties to test

-- ** 'Eq' laws

prop_eqReflexivity :: RawBytes -> Property
prop_eqReflexivity block = block === block

prop_eqSymmetry :: RawBytes -> RawBytes -> Property
prop_eqSymmetry block1 block2 = twoBlocksProp "" block1 block2 $
                                (block1 == block2) === (block2 == block1)

prop_eqTransitivity :: Property
prop_eqTransitivity = mapSize (const 3)     $
                      withDiscardRatio 1000 $
                      untunedProp
    where

    untunedProp :: RawBytes -> RawBytes -> RawBytes -> Property
    untunedProp block1 block2 block3
        = withFirstBlockSizeInfo block1 $
          block1 == block2 && block2 == block3 ==> block1 === block3

prop_eqNegation :: RawBytes -> RawBytes -> Property
prop_eqNegation block1 block2 = twoBlocksProp "" block1 block2 $
                                (block1 /= block2) === not (block1 == block2)

-- ** 'Ord' laws

prop_ordComparability :: RawBytes -> RawBytes -> Property
prop_ordComparability block1 block2 = twoBlocksProp "" block1 block2 $
                                      block1 <= block2 .||. block2 <= block1

prop_ordTransitivity :: RawBytes -> RawBytes -> RawBytes -> Property
prop_ordTransitivity block1 block2 block3
    = twoBlocksProp " front-side"   block1 block2 $
      twoBlocksProp " rear-side"    block2 block3 $
      twoBlocksProp " at the edges" block1 block3 $
      block1 <= block2 && block2 <= block3 ==> block1 <= block3

prop_ordReflexivity :: RawBytes -> Bool
prop_ordReflexivity block = block <= block

prop_ordAntisymmetry :: Property
prop_ordAntisymmetry = mapSize (const 4)    $
                       withDiscardRatio 100 $
                       untunedProp
    where

    untunedProp :: RawBytes -> RawBytes -> Property
    untunedProp block1 block2
        = withFirstBlockSizeInfo block1 $
          block1 <= block2 && block2 <= block1 ==> block1 === block2
