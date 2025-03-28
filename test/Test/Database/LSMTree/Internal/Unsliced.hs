module Test.Database.LSMTree.Internal.Unsliced (tests) where

import           Database.LSMTree.Extras.Generators ()
import           Database.LSMTree.Internal.Serialise
import           Database.LSMTree.Internal.Unsliced
import           Test.Tasty
import           Test.Tasty.QuickCheck

tests :: TestTree
tests = testGroup "Test.Database.LSMTree.Internal.Unsliced" [
      testProperty "prop_makeUnslicedKeyPreservesEq" prop_makeUnslicedKeyPreservesEq
    , testProperty "prop_fromUnslicedKeyPreservesEq" prop_fromUnslicedKeyPreservesEq
    , testProperty "prop_makeUnslicedKeyPreservesOrd" prop_makeUnslicedKeyPreservesOrd
    , testProperty "prop_fromUnslicedKeyPreservesOrd" prop_fromUnslicedKeyPreservesOrd
    ]

-- 'Eq' on serialised keys is preserved when converting to /unsliced/ serialised
-- keys.
prop_makeUnslicedKeyPreservesEq :: SerialisedKey -> SerialisedKey -> Property
prop_makeUnslicedKeyPreservesEq k1 k2 = checkCoverage $
    cover 1 lhs "k1 == k2" $ lhs === rhs
  where
    lhs = k1 == k2
    rhs = makeUnslicedKey k1 == makeUnslicedKey k2

-- 'Eq' on /unsliced/ serialised keys is preserved when converting to serialised
-- keys.
prop_fromUnslicedKeyPreservesEq :: Unsliced SerialisedKey -> Unsliced SerialisedKey -> Property
prop_fromUnslicedKeyPreservesEq k1 k2 = checkCoverage $
    cover 1 lhs "k1 == k2" $ lhs === rhs
  where
    lhs = k1 == k2
    rhs = fromUnslicedKey k1 == fromUnslicedKey k2

-- 'Ord' on serialised keys is preserved when converting to /unsliced/
-- serialised keys.
prop_makeUnslicedKeyPreservesOrd :: SerialisedKey -> SerialisedKey -> Property
prop_makeUnslicedKeyPreservesOrd k1 k2 = checkCoverage $
     cover 50 lhs "k1 <= k2" $ lhs === rhs
  where
    lhs = k1 <= k2
    rhs = makeUnslicedKey k1 <= makeUnslicedKey k2

-- 'Ord' on /unsliced/ serialised keys is preserved when converting to serialised
-- keys.
prop_fromUnslicedKeyPreservesOrd :: Unsliced SerialisedKey -> Unsliced SerialisedKey -> Property
prop_fromUnslicedKeyPreservesOrd k1 k2 = checkCoverage $
    cover 50 lhs "k1 <= k2" $ lhs === rhs
  where
    lhs = k1 <= k2
    rhs = fromUnslicedKey k1 <= fromUnslicedKey k2
