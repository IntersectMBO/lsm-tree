module Test.Database.LSMTree.Internal.BloomFilter (tests) where

import qualified Data.Set as Set
import qualified Data.Vector as V
import qualified Data.Vector.Primitive as VP
import           Data.Word (Word64)

import           Test.QuickCheck.Gen (genDouble)
import           Test.QuickCheck.Instances ()
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.QuickCheck hiding ((.&.))

import qualified Data.BloomFilter.Blocked as Bloom
import           Database.LSMTree.Internal.BloomFilter
import           Database.LSMTree.Internal.Serialise (SerialisedKey,
                     serialiseKey)

--TODO: add a golden test for the BloomFilter format vs the 'formatVersion'
-- to ensure we don't change the format without conciously bumping the version.
tests :: TestTree
tests = testGroup "Database.LSMTree.Internal.BloomFilter"
    [ testProperty "bloomQueries (bulk)" $
        prop_bloomQueries
    , testProperty "prop_packUnpack_RunIxKeyIx" prop_packUnpack_RunIxKeyIx
    , testProperty "prop_packUnpack_RunIxKeyIx_limits" prop_packUnpack_RunIxKeyIx_limits
    ]

testSalt :: Bloom.Salt
testSalt = 4

newtype FPR = FPR Double deriving stock Show

instance Arbitrary FPR where
  arbitrary =
    FPR <$> frequency
      [ (1, pure 0.999)
      , (9, (fmap (/2) genDouble) `suchThat` \fpr -> fpr > 0) ]

prop_bloomQueries :: FPR
                  -> [[Small Word64]]
                  -> [Small Word64]
                  -> Property
prop_bloomQueries (FPR fpr) filters keys =
    let filters' :: [Bloom SerialisedKey]
        filters' = map (Bloom.fromList (Bloom.policyForFPR fpr) testSalt
                        . map (\(Small k) -> serialiseKey k))
                       filters

        keys' :: [SerialisedKey]
        keys' = map (\(Small k) -> serialiseKey k) keys

        referenceResults :: [(Int, Int)]
        referenceResults =
          [ (f_i, k_i)
          | (k, k_i) <- zip keys' [0..]
          , (f, f_i) <- zip filters' [0..]
          , Bloom.elem k f
          ]

        filterSets = map (Set.fromList . map (\(Small k) -> serialiseKey k)) filters
        referenceCmp =
          [ (Bloom.elem k f, k `Set.member` f')
          | (f, f') <- zip filters' filterSets
          , k       <- keys'
          ]
        truePositives  = [ "true positives"  | (True,  True)  <- referenceCmp ]
        falsePositives = [ "false positives" | (True,  False) <- referenceCmp ]
        trueNegatives  = [ "true negatives"  | (False, False) <- referenceCmp ]
        falseNegatives = [ "false negatives" | (False, True)  <- referenceCmp ]
        distribution   = truePositives ++ falsePositives
                      ++ trueNegatives ++ falseNegatives

    -- To get coverage of bloomQueries array resizing we want some
    -- cases with high FPRs.
     in tabulate "FPR" [show (round (fpr * 10) * 10 :: Int) ++ "%"] $
        coverTable "FPR" [("100%", 5)] $
        tabulate "distribution of true/false positives/negatives" distribution $
        referenceResults
       ===
        map (\(RunIxKeyIx rix kix) -> (rix, kix))
            (VP.toList (bloomQueries testSalt (V.fromList filters') (V.fromList keys')))

{-------------------------------------------------------------------------------
  RunIxKeyIx
-------------------------------------------------------------------------------}

-- | Test that 'RunIx' and 'KeyIx' roundtrip through the 'RunIxKeyIx' pattern
-- synonym
--
-- More specifically, if we apply a 'RunIxKeyIx' pattern synonym to a pair of
-- 'RunIx' and 'KeyIx', and then pattern match on it agains, then we would
-- expect to get the same 'RunIx' and 'KeyIx' out as the ones we put in.
--
-- There used to be a bug where this went wrong because of a typo in a bit-mask.
-- This property test should ensure that we catch such mistakes in the future.
-- See PR #841 for more information.
--
-- <PR https://github.com/IntersectMBO/lsm-tree/pull/841>
--
prop_packUnpack_RunIxKeyIx :: Int_0xffff -> Int_0xffff -> Property
prop_packUnpack_RunIxKeyIx r k =
    case RunIxKeyIx r.unwrap k.unwrap of
      RunIxKeyIx r' k' -> r.unwrap === r' .&&. k.unwrap === k'

-- | A variant of 'prop_packUnpack_RunIxKeyIx' applied to 'RunIx' and 'KeyIx'
-- that are close to their upper bounds.
prop_packUnpack_RunIxKeyIx_limits :: Property
prop_packUnpack_RunIxKeyIx_limits = conjoin [
      prop_packUnpack_RunIxKeyIx 0xffff       0xffff
    , prop_packUnpack_RunIxKeyIx (0xffff - 1) 0xffff
    , prop_packUnpack_RunIxKeyIx 0xffff       (0xffff - 1)
    , prop_packUnpack_RunIxKeyIx (0xffff - 1) (0xffff - 1)
    ]

-- | An Int in the inclusive range @(0, 0xffff)@
newtype Int_0xffff = Int_0xffff { unwrap :: Int }
  deriving stock (Show, Eq)
  deriving newtype Num

instance Arbitrary Int_0xffff where
  arbitrary = Int_0xffff <$> chooseInt (0, 0xffff)
  shrink x = [
        Int_0xffff y
      | y <- shrink x.unwrap
      , 0 <= y
      , y < 0xffff
      ]

