{-# OPTIONS_GHC -Wno-orphans #-}

module Test.Database.LSMTree.Internal.MergingRun (tests) where

import           Database.LSMTree.Internal.MergingRun
import           Test.QuickCheck
import           Test.Tasty
import           Test.Tasty.QuickCheck

tests :: TestTree
tests = testGroup "Test.Database.LSMTree.Internal.MergingRun"
    [ testProperty "prop_CreditsPair" prop_CreditsPair
    ]

-- | The representation of CreditsPair should round trip properly. This is
-- non-trivial because it uses a packed bitfield representation.
--
prop_CreditsPair :: SpentCredits -> UnspentCredits -> Property
prop_CreditsPair spentCredits unspentCredits =
    tabulate "bounds" [spentCreditsBound, unspentCreditsBound] $
    let cp :: Int
        !cp = CreditsPair spentCredits unspentCredits
     in case cp of
          CreditsPair spentCredits' unspentCredits' ->
            (spentCredits, unspentCredits) === (spentCredits', unspentCredits')
  where
    spentCreditsBound
      | spentCredits == minBound = "spentCredits == minBound"
      | spentCredits == maxBound = "spentCredits == maxBound"
      | otherwise                = "spentCredits == other"

    unspentCreditsBound
      | unspentCredits == minBound = "unspentCredits == minBound"
      | unspentCredits == maxBound = "unspentCredits == maxBound"
      | otherwise                  = "unspentCredits == other"

deriving newtype instance Enum SpentCredits
deriving newtype instance Enum UnspentCredits

deriving stock instance Show Credits
deriving stock instance Show SpentCredits
deriving stock instance Show UnspentCredits

instance Arbitrary SpentCredits where
  arbitrary =
    frequency [ (1, pure minBound)
              , (1, pure maxBound)
              , (10, arbitraryBoundedEnum)
              ]

instance Arbitrary UnspentCredits where
  arbitrary =
    frequency [ (1, pure minBound)
              , (1, pure maxBound)
              , (10, arbitraryBoundedEnum)
              ]

