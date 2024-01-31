{-# LANGUAGE DerivingStrategies         #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# OPTIONS_GHC -Wno-orphans #-}
{- HLINT ignore "Use /=" -}

module Test.Database.LSMTree.Internal.Serialise (tests) where

import           Data.Bits
import qualified Data.Vector.Primitive as P
import           Data.Word
import           Database.LSMTree.Internal.Serialise
import           Database.LSMTree.Util (showPowersOf10)
import           Test.Tasty
import           Test.Tasty.HUnit
import           Test.Tasty.QuickCheck

tests :: TestTree
tests = testGroup "Test.Database.LSMTree.Internal.Serialise" [
      testGroup "Eq and Ord laws" [
          testProperty "Eq reflexivity" propEqReflexivity
        , testProperty "Eq symmetry" propEqSymmetry
        , localOption (QuickCheckMaxRatio 1000) $
          testProperty "Eq transitivity" propEqTransitivity
        , testProperty "Eq negation" propEqNegation
        , testProperty "Ord comparability" propOrdComparability
        , testProperty "Ord transitivity" propOrdTransitivity
        , testProperty "Ord reflexivity" propOrdReflexivity
        , localOption (QuickCheckMaxRatio 1000) $
          testProperty "Ord antisymmetry" propOrdAntiSymmetry
        ]
    , testGroup "Distributions" [
          testProperty "arbitrary SerialisedKey" distribution
        , testProperty "shrink serialisedKey" $ conjoin . fmap distribution . shrink
        ]
    , testCase "example topBits16" $ do
        let k = SerialisedKey (P.fromList [37, 42, 204, 130])
            expected :: Word16
            expected = 37 `shiftL` 8 + 42
        topBits16 16 k @=? expected
        topBits16 0  k @=? 0
        topBits16 9  k @=? expected `shiftR` (16 - 9)
    , testCase "example topBits16 on sliced byte array" $ do
        let pvec = P.fromList [0, 37, 42, 204, 130]
            k = SerialisedKey (P.slice 1 (P.length pvec - 1) pvec)
            expected :: Word16
            expected = 37 `shiftL` 8 + 42
        topBits16 16 k @=? expected
        topBits16 0  k @=? 0
        topBits16 9  k @=? expected `shiftR` (16 - 9)
    , testCase "example sliceBits32" $ do
        let k = SerialisedKey (P.fromList [0, 0, 255, 255, 255, 255, 0])
        0x0000FFFF @=? sliceBits32 0 k
        0xFFFFFFFF @=? sliceBits32 16 k
        0x7FFFFFFF @=? sliceBits32 15 k
        0xFFFFFFFE @=? sliceBits32 17 k
    , testCase "example sliceBits32 on sliced byte array" $ do
        let pvec = P.fromList [0, 0, 0, 255, 255, 255, 255, 0]
            k = SerialisedKey (P.slice 1 (P.length pvec - 1) pvec)
        0x0000FFFF @=? sliceBits32 0 k
        0xFFFFFFFF @=? sliceBits32 16 k
        0x7FFFFFFF @=? sliceBits32 15 k
        0xFFFFFFFE @=? sliceBits32 17 k
    ]

{-------------------------------------------------------------------------------
  Eq and Ord laws
-------------------------------------------------------------------------------}

propEqReflexivity :: SerialisedKey -> Property
propEqReflexivity k = k === k

propEqSymmetry :: SerialisedKey -> SerialisedKey -> Property
propEqSymmetry k1 k2 = (k1 == k2) === (k2 == k1)

propEqTransitivity :: SmallSerialisedKey -> SmallSerialisedKey -> SmallSerialisedKey -> Property
propEqTransitivity k1 k2 k3 = k1 == k2 && k2 == k3 ==> k1 === k3

propEqNegation :: SerialisedKey -> SerialisedKey -> Property
propEqNegation k1 k2 = (k1 /= k2) === not (k1 == k2)

propOrdComparability :: SerialisedKey -> SerialisedKey -> Property
propOrdComparability k1 k2 = k1 <= k2 .||. k2 <= k1

propOrdTransitivity :: SerialisedKey -> SerialisedKey -> SerialisedKey -> Property
propOrdTransitivity k1 k2 k3 = k1 <= k2 && k2 <= k3 ==> k1 <= k3

propOrdReflexivity :: SerialisedKey -> Property
propOrdReflexivity k = property $ k <= k

propOrdAntiSymmetry :: SmallSerialisedKey -> SmallSerialisedKey -> Property
propOrdAntiSymmetry k1 k2 = k1 <= k2 && k2 <= k1 ==> k1 === k2

{-------------------------------------------------------------------------------
  Arbitrary
-------------------------------------------------------------------------------}

distribution :: SerialisedKey -> Property
distribution k =
    tabulate "size of key in bytes" [showPowersOf10 $ sizeofKey k] $
    property True

instance Arbitrary SerialisedKey where
  arbitrary = do
    pvec <- P.fromList <$> arbitrary
    n <- chooseInt (0, P.length pvec)
    m <- chooseInt (0, P.length pvec - n)
    pure $ SerialisedKey (P.slice m n pvec)
  shrink (SerialisedKey pvec) =
         [ SerialisedKey (P.fromList ws) | ws <- shrink (P.toList pvec) ]
      ++ [ SerialisedKey (P.slice m n pvec)
         | n <- shrink (P.length pvec)
         , m <- shrink (P.length pvec - n)
         ]

newtype SmallSerialisedKey = SmallSerialisedKey SerialisedKey
  deriving newtype (Show, Eq, Ord)

instance Arbitrary SmallSerialisedKey where
  arbitrary = do
      n <- choose (0, 5)
      SerialisedKey pvec <- arbitrary
      pure $ SmallSerialisedKey (SerialisedKey (P.take n pvec))
  shrink (SmallSerialisedKey k) = SmallSerialisedKey <$> shrink k
