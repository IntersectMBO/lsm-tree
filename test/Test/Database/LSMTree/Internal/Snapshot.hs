{-# OPTIONS_GHC -Wno-orphans #-}

module Test.Database.LSMTree.Internal.Snapshot (tests) where

import           Codec.CBOR.Decoding
import           Codec.CBOR.Encoding
import           Codec.CBOR.FlatTerm
import           Codec.CBOR.Read
import           Codec.CBOR.Write
import qualified Data.ByteString.Lazy as BSL
import           Data.Proxy
import           Data.Text (Text)
import qualified Data.Text as Text
import qualified Data.Vector as V
import           Database.LSMTree.Internal.Config
import           Database.LSMTree.Internal.Entry
import qualified Database.LSMTree.Internal.Merge as Merge
import           Database.LSMTree.Internal.MergeSchedule
import           Database.LSMTree.Internal.RunNumber
import           Database.LSMTree.Internal.Snapshot
import           Test.Tasty
import           Test.Tasty.QuickCheck

-- TODO: we should add golden tests for the CBOR encoders. This should prevent
-- accidental breakage in the format.
tests :: TestTree
tests = testGroup "Test.Database.LSMTree.Internal.Snapshot" [
      testGroup "SnapshotVersion" [
          testProperty "roundtripCBOR" $ roundtripCBOR (Proxy @SnapshotVersion)
        , testProperty "roundtripFlatTerm" $ roundtripFlatTerm (Proxy @SnapshotVersion)
        ]
    , testGroup "Versioned SnapshotMetaData" [
          testProperty "roundtripCBOR" $ roundtripCBOR (Proxy @(Versioned SnapshotMetaData))
        , testProperty "roundtripFlatTerm" $ roundtripFlatTerm (Proxy @(Versioned SnapshotMetaData))
        ]
    , testGroup "roundtripCBOR'" (propAll roundtripCBOR')
    , testGroup "roundtripFlatTerm'" (propAll roundtripFlatTerm')
    ]

-- | Run a property on all types in the snapshot metadata hierarchy.
propAll ::
     (     forall a. (Encode a, DecodeVersioned a, Eq a, Show a)
        => Proxy a -> a -> Property
     )
  -> [TestTree]
propAll prop = [
      -- SnapshotMetaData
      testProperty "SnapshotMetaData" $ prop (Proxy @SnapshotMetaData)
    , testProperty "SnapshotLabel" $ prop (Proxy @SnapshotLabel)
    , testProperty "SnapshotTableType" $ prop (Proxy @SnapshotTableType)
      -- TableConfig
    , testProperty "TableConfig" $ prop (Proxy @TableConfig)
    , testProperty "MergePolicy" $ prop (Proxy @MergePolicy)
    , testProperty "SizeRatio" $ prop (Proxy @SizeRatio)
    , testProperty "WriteBufferAlloc" $ prop (Proxy @WriteBufferAlloc)
    , testProperty "NumEntries" $ prop (Proxy @NumEntries)
    , testProperty "BloomFilterAlloc" $ prop (Proxy @BloomFilterAlloc)
    , testProperty "FencePointerIndex" $ prop (Proxy @FencePointerIndex)
    , testProperty "DiskCachePolicy" $ prop (Proxy @DiskCachePolicy)
    , testProperty "MergeSchedule" $ prop (Proxy @MergeSchedule)
      -- SnapLevels
    , testProperty "SnapLevels" $ prop (Proxy @SnapLevels)
    , testProperty "SnapLevel" $ prop (Proxy @SnapLevel)
    , testProperty "Vector RunNumber" $ prop (Proxy @(V.Vector RunNumber))
    , testProperty "RunNumber" $ prop (Proxy @RunNumber)
    , testProperty "SnapMergingRun" $ prop (Proxy @SnapMergingRun)
    , testProperty "NumRuns" $ prop (Proxy @NumRuns)
    , testProperty "MergePolicyForLevel" $ prop (Proxy @MergePolicyForLevel)
    , testProperty "UnspentCredits" $ prop (Proxy @UnspentCredits)
    , testProperty "MergeKnownCompleted" $ prop (Proxy @MergeKnownCompleted)
    , testProperty "SnapMergingRunState" $ prop (Proxy @SnapMergingRunState)
    , testProperty "SpentCredits" $ prop (Proxy @SpentCredits)
    , testProperty "Merge.Level" $ prop (Proxy @Merge.Level)
    ]

-- | @decode . encode = id@
explicitRoundtripCBOR ::
     (Eq a, Show a)
  => (a -> Encoding)
  -> (forall s. Decoder s a)
  -> a
  -> Property
explicitRoundtripCBOR enc dec x = case back (there x) of
    Left e ->
           counterexample
             ("Decoding failed: " <> show e)
             (property False)
    Right (bs, y) ->
           counterexample
             ("Expected value and roundtripped value do not match")
             (x === y)
      .&&. counterexample
             ("Found trailing bytes")
             (property (BSL.null bs))
  where
    there = toLazyByteString . enc
    back = deserialiseFromBytes dec

-- | See 'explicitRoundtripCBOR'.
roundtripCBOR :: (Encode a, Decode a, Eq a, Show a) => Proxy a -> a -> Property
roundtripCBOR _ = explicitRoundtripCBOR encode decode

-- | See 'explicitRoundtripCBOR'.
roundtripCBOR' :: (Encode a, DecodeVersioned a, Eq a, Show a) => Proxy a -> a -> Property
roundtripCBOR' _ = explicitRoundtripCBOR encode (decodeVersioned currentSnapshotVersion)

-- | @fromFlatTerm . toFlatTerm = id@
--
-- This will also check @validFlatTerm@ on the result of @toFlatTerm@.
explicitRoundtripFlatTerm ::
     (Eq a, Show a)
  => (a -> Encoding)
  -> (forall s. Decoder s a)
  -> a
  -> Property
explicitRoundtripFlatTerm enc dec x = case back flatTerm of
    Left e ->
            counterexample
              ("Decoding failed: " <> show e)
              (property False)
    Right y ->
           counterexample
             ("Expected value and roundtripped value do not match")
             (x === y)
      .&&. counterexample
             ("Invalid flat term")
             (property (validFlatTerm flatTerm))
  where
    flatTerm = there x

    there = toFlatTerm . enc
    back = fromFlatTerm dec

-- | See 'explicitRoundtripFlatTerm'.
roundtripFlatTerm ::
     (Encode a, Decode a, Eq a, Show a)
  => Proxy a
  -> a
  -> Property
roundtripFlatTerm _ = explicitRoundtripFlatTerm encode decode

-- | See 'explicitRoundtripFlatTerm'.
roundtripFlatTerm' ::
     (Encode a, DecodeVersioned a, Eq a, Show a)
  => Proxy a
  -> a
  -> Property
roundtripFlatTerm' _ = explicitRoundtripFlatTerm encode (decodeVersioned currentSnapshotVersion)

{-------------------------------------------------------------------------------
  Arbitrary: versioning
-------------------------------------------------------------------------------}

instance Arbitrary SnapshotVersion where
  arbitrary = elements [V0]
  shrink V0 = []

deriving newtype instance Arbitrary a => Arbitrary (Versioned a)

{-------------------------------------------------------------------------------
  Arbitrary: SnapshotMetaData
-------------------------------------------------------------------------------}

instance Arbitrary SnapshotMetaData where
  arbitrary = SnapshotMetaData <$> arbitrary <*> arbitrary <*> arbitrary <*> arbitrary
  shrink (SnapshotMetaData a b c d) =
      [ SnapshotMetaData a' b' c' d'
      | (a', b', c', d') <- shrink (a, b, c, d)]

deriving newtype instance Arbitrary SnapshotLabel

instance Arbitrary Text where
  arbitrary = Text.pack <$> arbitrary
  shrink x = Text.pack <$> shrink (Text.unpack x)

instance Arbitrary SnapshotTableType where
  arbitrary = elements [SnapNormalTable, SnapMonoidalTable]
  shrink _ = []

{-------------------------------------------------------------------------------
  Arbitrary: TableConfig
-------------------------------------------------------------------------------}

instance Arbitrary TableConfig where
  arbitrary =
          TableConfig <$> arbitrary <*> arbitrary <*> arbitrary
      <*> arbitrary   <*> arbitrary <*> arbitrary <*> arbitrary
  shrink (TableConfig a b c d e f g) =
      [ TableConfig a' b' c' d' e' f' g'
      | (a', b', c', d', e', f', g') <- shrink (a, b, c, d, e, f, g) ]

instance Arbitrary MergePolicy where
  arbitrary = pure MergePolicyLazyLevelling
  shrink MergePolicyLazyLevelling = []

instance Arbitrary SizeRatio where
  arbitrary = pure Four
  shrink Four = []

instance Arbitrary WriteBufferAlloc where
  arbitrary = AllocNumEntries <$> arbitrary
  shrink (AllocNumEntries x) = AllocNumEntries <$> shrink x

deriving newtype instance Arbitrary NumEntries

instance Arbitrary BloomFilterAlloc where
  arbitrary = oneof [
        AllocFixed <$> arbitrary
      , AllocRequestFPR <$> arbitrary
      , AllocMonkey <$> arbitrary <*> arbitrary
      ]
  shrink (AllocFixed x)      = AllocFixed <$> shrink x
  shrink (AllocRequestFPR x) = AllocRequestFPR <$> shrink x
  shrink (AllocMonkey x y)   = [AllocMonkey x' y' | (x', y') <- shrink (x, y)]

instance Arbitrary FencePointerIndex where
  arbitrary = elements [CompactIndex, OrdinaryIndex]
  shrink _ = []

instance Arbitrary DiskCachePolicy where
  arbitrary = oneof [
        pure DiskCacheAll
      , DiskCacheLevelsAtOrBelow <$> arbitrary
      , pure DiskCacheNone
      ]
  shrink (DiskCacheLevelsAtOrBelow x) = DiskCacheLevelsAtOrBelow <$> shrink x
  shrink _                            = []

instance Arbitrary MergeSchedule where
  arbitrary = elements [OneShot, Incremental]
  shrink _ = []

{-------------------------------------------------------------------------------
  Arbitrary: SnapLevels
-------------------------------------------------------------------------------}

instance Arbitrary (V.Vector SnapLevel) where
  arbitrary = do
    n <- chooseInt (0, 10)
    (V.fromList <$> vector n)
  shrink x = V.fromList <$> shrink (V.toList x)

instance Arbitrary SnapLevel where
  arbitrary = SnapLevel <$> arbitrary <*> arbitrary
  shrink (SnapLevel a b) = [SnapLevel a' b' | (a', b') <- shrink (a, b)]

instance Arbitrary (V.Vector RunNumber) where
  arbitrary = do
    n <- chooseInt (0, 5)
    (V.fromList <$> vector n)
  shrink x = V.fromList <$> shrink (V.toList x)

deriving newtype instance Arbitrary RunNumber

instance Arbitrary SnapMergingRun where
  arbitrary = oneof [
        SnapMergingRun <$> arbitrary <*> arbitrary <*> arbitrary
                       <*> arbitrary <*> arbitrary <*> arbitrary
      , SnapSingleRun <$> arbitrary
      ]
  shrink (SnapMergingRun a b c d e f) =
      [ SnapMergingRun a' b' c' d' e' f'
      | (a', b', c', d', e', f') <- shrink (a, b, c, d, e, f) ]
  shrink (SnapSingleRun a)  = SnapSingleRun <$> shrink a

deriving newtype instance Arbitrary NumRuns

instance Arbitrary MergePolicyForLevel where
  arbitrary = elements [LevelTiering, LevelLevelling]
  shrink _ = []

deriving newtype instance Arbitrary UnspentCredits

instance Arbitrary MergeKnownCompleted where
  arbitrary = elements [MergeKnownCompleted, MergeMaybeCompleted]
  shrink _ = []

instance Arbitrary SnapMergingRunState where
  arbitrary = oneof [
        SnapCompletedMerge <$> arbitrary
      , SnapOngoingMerge <$> arbitrary <*> arbitrary <*> arbitrary
      ]
  shrink (SnapCompletedMerge x) = SnapCompletedMerge <$> shrink x
  shrink (SnapOngoingMerge x y z)   =
      [ SnapOngoingMerge x' y' z' | (x', y', z') <- shrink (x, y, z) ]

deriving newtype instance Arbitrary SpentCredits

instance Arbitrary Merge.Level where
  arbitrary = elements [Merge.MidLevel, Merge.LastLevel]
  shrink _ = []
