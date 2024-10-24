{-# OPTIONS_GHC -Wno-orphans #-}

module Test.Database.LSMTree.Internal.Snapshot (tests) where

import           Data.Aeson hiding (encode)
import           Data.Proxy
import           Database.LSMTree.Internal.Config
import           Database.LSMTree.Internal.RunNumber
import           Database.LSMTree.Internal.Snapshot
import           Test.Tasty
import           Test.Tasty.QuickCheck

tests :: TestTree
tests = testGroup "Test.Database.LSMTree.Internal.Snapshot" [
      testProperty "SnapshotVersion" $ roundtripJSON (Proxy @SnapshotVersion)
    , testProperty "SnapshotLabel" $ roundtripJSON (Proxy @SnapshotLabel)
      -- Table config
    , testProperty "MergePolicy" $ roundtripJSON (Proxy @MergePolicy)
    , testProperty "SizeRatio" $ roundtripJSON (Proxy @SizeRatio)
    , testProperty "MergePolicy" $ roundtripJSON (Proxy @MergePolicy)

    ]

roundtripJSON :: (ToEncoding a, FromJSON a, Eq a, Show a) => Proxy a -> a -> Property
roundtripJSON _ x = Just x === decode (encode x)

deriving newtype instance Arbitrary SnapshotVersion

deriving newtype instance Arbitrary SnapshotLabel

deriving newtype instance Arbitrary RunNumber

{-------------------------------------------------------------------------------
  Table config
-------------------------------------------------------------------------------}

instance Arbitrary TableConfig where
  arbitrary = undefined

instance Arbitrary MergePolicy where
  arbitrary = pure MergePolicyLazyLevelling
  shrink MergePolicyLazyLevelling = []

instance Arbitrary SizeRatio where
  arbitrary = pure Four
  shrink Four = []

-- instance Arbitrary WriteBufferAlloc where
--   arbitrary = AllocNumEntries <$> arbitrary
--   shrink (AllocNumEntries x) = AllocNumEntries <$> shrink x
