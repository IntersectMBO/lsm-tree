module Test.ScheduledMerges.RunSizes (tests) where

import           ScheduledMerges
import           Test.QuickCheck
import           Test.Tasty
import           Test.Tasty.QuickCheck

tests :: TestTree
tests = testGroup "Test.ScheduledMerges.RunSizes" [
      testProperty "prop_roundTrip_levelNumberMaxRunSize" $
        \mpl conf ln -> levelNumberInvariant mpl conf ln ==>
            prop_roundTrip_levelNumberMaxRunSize mpl conf ln
    , testProperty "prop_roundTrip_runSizeLevelNumber" prop_roundTrip_runSizeLevelNumber
    , testProperty "prop_maxWriteBufferSize" prop_maxWriteBufferSize
    ]

-- | Test 'levelNumberToMaxRunSize' roundtrips with 'runSizeToLevelNumber'.
prop_roundTrip_levelNumberMaxRunSize :: MergePolicyForLevel -> Config -> LevelNo -> Property
prop_roundTrip_levelNumberMaxRunSize (MergePolicyForLevel mpl) (Config conf) (LevelNo ln) =
    let n = levelNumberToMaxRunSize mpl conf ln
        ln' = runSizeToLevelNumber mpl conf n
    in  ln === ln'

-- | Test that 'runSizeToLevelNumber'roundtrips with 'levelNumberToMaxRunSize'.
prop_roundTrip_runSizeLevelNumber :: MergePolicyForLevel -> Config -> RunSize -> Property
prop_roundTrip_runSizeLevelNumber (MergePolicyForLevel mpl) (Config conf) (RunSize n) =
    let ln = runSizeToLevelNumber mpl conf n
    in  if ln == 0
        then 0 === n
        else let n1 = levelNumberToMaxRunSize mpl conf (ln - 1)
                 n2 = levelNumberToMaxRunSize mpl conf ln
             in  property (n1 < n && n <= n2)

-- | Test that 'maxWriteBufferSize' equals the configured 'configMaxWriteBufferSize'.
prop_maxWriteBufferSize :: Config -> Property
prop_maxWriteBufferSize (Config conf) =
    configMaxWriteBufferSize conf === maxWriteBufferSize conf

{-------------------------------------------------------------------------------
  Generators and shrinkers
-------------------------------------------------------------------------------}

newtype MergePolicyForLevel = MergePolicyForLevel MergePolicy
  deriving stock (Show, Eq)

instance Arbitrary MergePolicyForLevel where
  arbitrary = MergePolicyForLevel <$> elements [MergePolicyTiering, MergePolicyLevelling]
  shrink (MergePolicyForLevel x) = MergePolicyForLevel <$> case x of
      MergePolicyTiering   -> []
      MergePolicyLevelling -> [MergePolicyTiering]

newtype Config = Config LSMConfig
  deriving stock (Show, Eq)

instance Arbitrary Config where
  arbitrary = Config <$> do
      bufSize <- (getSmall <$> arbitrary) `suchThat` (>0)
      pure $ LSMConfig {
          configMaxWriteBufferSize = bufSize
        }
  shrink (Config LSMConfig{..}) =
      [ Config LSMConfig{configMaxWriteBufferSize = bufSize'}
      | bufSize' <- shrink configMaxWriteBufferSize
      , bufSize' > 0
      ]

newtype LevelNo = LevelNo Int
  deriving stock (Show, Eq, Ord)
  deriving Arbitrary via NonNegative Int

-- | The maximum size of a run on a level scales exponentially with the level
-- number, and linearly with the maximum write buffer size. There is a
-- possibility for 'Int' overflow primarily because of the exponential factor,
-- which would make the @ScheduledMerges@ prototype throw an error if using
-- @fromIntegerChecked@. The 'noOverflow' predicate makes sure that we do not
-- generate too large level numbers that could cause such overflows. But this
-- overflow condition also depends on the maximum write buffer size and merge
-- policy, so both are arguments to this function as well.
levelNumberInvariant :: MergePolicyForLevel -> Config -> LevelNo -> Bool
levelNumberInvariant
  (MergePolicyForLevel mpl)
  (Config LSMConfig{configMaxWriteBufferSize})
  (LevelNo ln)
  | ln < 0 = False
  | ln == 0 = True
  | otherwise = case mpl of
      MergePolicyTiering ->
        toInteger configMaxWriteBufferSize * (4 ^ toInteger (pred ln))
          <= toInteger (maxBound :: Int)
      MergePolicyLevelling ->
        toInteger configMaxWriteBufferSize * (4 ^ toInteger ln)
          <= toInteger (maxBound :: Int)

newtype RunSize = RunSize Int
  deriving stock (Show, Eq, Ord)
  deriving Arbitrary via NonNegative Int
