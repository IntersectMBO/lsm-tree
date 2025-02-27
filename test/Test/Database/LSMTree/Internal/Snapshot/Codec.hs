{-# OPTIONS_GHC -Wno-orphans #-}

module Test.Database.LSMTree.Internal.Snapshot.Codec (tests) where

import           Codec.CBOR.Decoding
import           Codec.CBOR.Encoding
import           Codec.CBOR.FlatTerm
import           Codec.CBOR.Read
import           Codec.CBOR.Write
import           Control.DeepSeq (NFData)
import qualified Data.ByteString.Lazy as BSL
import           Data.Proxy
import           Data.Typeable
import qualified Data.Vector as V
import           Database.LSMTree.Extras.Generators ()
import           Database.LSMTree.Internal.Config
import           Database.LSMTree.Internal.Entry
import           Database.LSMTree.Internal.MergeSchedule
import           Database.LSMTree.Internal.MergingRun
import           Database.LSMTree.Internal.RunBuilder (IndexType (..),
                     RunBloomFilterAlloc (..), RunDataCaching (..))
import           Database.LSMTree.Internal.RunNumber
import           Database.LSMTree.Internal.Snapshot
import           Database.LSMTree.Internal.Snapshot.Codec
import           Test.Tasty
import           Test.Tasty.QuickCheck
import           Test.Util.Arbitrary

-- TODO: we should add golden tests for the CBOR encoders. This should prevent
-- accidental breakage in the format.
tests :: TestTree
tests = testGroup "Test.Database.LSMTree.Internal.Snapshot.Codec" [
      testGroup "SnapshotVersion" [
          testProperty "roundtripCBOR" $ roundtripCBOR (Proxy @SnapshotVersion)
        , testProperty "roundtripFlatTerm" $ roundtripFlatTerm (Proxy @SnapshotVersion)
        ]
    , testGroup "Versioned SnapshotMetaData" [
          testProperty "roundtripCBOR" $ roundtripCBOR (Proxy @(Versioned SnapshotMetaData))
        , testProperty "roundtripFlatTerm" $ roundtripFlatTerm (Proxy @(Versioned SnapshotMetaData))
        ]
     , testGroup "roundtripCBOR'" $
        propAll roundtripCBOR'
    , testGroup "roundtripFlatTerm'" $
        propAll roundtripFlatTerm'
      -- Test generators and shrinkers
    , testGroup "Generators and shrinkers are finite" $
        testAll $ \(p :: Proxy a) ->
          testGroup (show $ typeRep p) $
            prop_arbitraryAndShrinkPreserveInvariant @a noTags deepseqInvariant
    ]

{-------------------------------------------------------------------------------
  Properties
-------------------------------------------------------------------------------}

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
  Test and property runners
-------------------------------------------------------------------------------}

type Constraints a = (
      Eq a, Show a, Typeable a, Arbitrary a
    , Encode a, DecodeVersioned a, NFData a
    )

-- | Run a property on all types in the snapshot metadata hierarchy.
propAll ::
     (forall a. Constraints a => Proxy a -> a -> Property)
  -> [TestTree]
propAll prop = testAll mkTest
  where
    mkTest :: forall a. Constraints a => Proxy a -> TestTree
    mkTest pa = testProperty (show $ typeRep pa) (prop pa)

-- | Run a test on all types in the snapshot metadata hierarchy.
testAll ::
     (forall a. Constraints a => Proxy a -> TestTree)
  -> [TestTree]
testAll test = [
      -- SnapshotMetaData
      test (Proxy @SnapshotMetaData)
    , test (Proxy @SnapshotLabel)
    , test (Proxy @SnapshotTableType)
    , test (Proxy @SnapshotRun)
      -- TableConfig
    , test (Proxy @TableConfig)
    , test (Proxy @MergePolicy)
    , test (Proxy @SizeRatio)
    , test (Proxy @WriteBufferAlloc)
    , test (Proxy @NumEntries)
    , test (Proxy @BloomFilterAlloc)
    , test (Proxy @FencePointerIndex)
    , test (Proxy @DiskCachePolicy)
    , test (Proxy @MergeSchedule)
      -- SnapLevels
    , test (Proxy @(SnapLevels SnapshotRun))
    , test (Proxy @(SnapLevel SnapshotRun))
    , test (Proxy @(V.Vector SnapshotRun))
    , test (Proxy @RunNumber)
    , test (Proxy @(SnapIncomingRun SnapshotRun))
    , test (Proxy @NumRuns)
    , test (Proxy @MergePolicyForLevel)
    , test (Proxy @RunDataCaching)
    , test (Proxy @RunBloomFilterAlloc)
    , test (Proxy @IndexType)
    , test (Proxy @RunParams)
    , test (Proxy @(SnapMergingRunState LevelMergeType SnapshotRun))
    , test (Proxy @MergeDebt)
    , test (Proxy @MergeCredits)
    , test (Proxy @NominalDebt)
    , test (Proxy @NominalCredits)
    , test (Proxy @LevelMergeType)
    , test (Proxy @TreeMergeType)
    , test (Proxy @(SnapMergingTree SnapshotRun))
    , test (Proxy @(SnapMergingTreeState SnapshotRun))
    , test (Proxy @(SnapPendingMerge SnapshotRun))
    , test (Proxy @(SnapPreExistingRun SnapshotRun))
    ]

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
  arbitrary = SnapshotMetaData <$>
      arbitrary <*> arbitrary <*> arbitrary <*>
      arbitrary <*> arbitrary <*> arbitrary
  shrink (SnapshotMetaData a b c d e f) =
      [ SnapshotMetaData a' b' c' d' e' f'
      | (a', b', c', d', e', f') <- shrink (a, b, c, d, e, f)]

deriving newtype instance Arbitrary SnapshotLabel

instance Arbitrary SnapshotTableType where
  arbitrary = elements [SnapNormalTable, SnapMonoidalTable]
  shrink _ = []

instance Arbitrary SnapshotRun where
  arbitrary = SnapshotRun <$> arbitrary <*> arbitrary <*> arbitrary
  shrink (SnapshotRun a b c) =
      [ SnapshotRun a' b' c'
      | (a', b', c') <- shrink (a, b, c)]

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

instance Arbitrary r => Arbitrary (SnapLevels r) where
  arbitrary = do
    n <- chooseInt (0, 10)
    SnapLevels . V.fromList <$> vector n
  shrink (SnapLevels x) = SnapLevels . V.fromList <$> shrink (V.toList x)

instance Arbitrary r => Arbitrary (SnapLevel r) where
  arbitrary = SnapLevel <$> arbitrary <*> arbitraryShortVector
  shrink (SnapLevel a b) = [SnapLevel a' b' | (a', b') <- shrink (a, b)]

arbitraryShortVector :: Arbitrary a => Gen (V.Vector a)
arbitraryShortVector = do
    n <- chooseInt (0, 5)
    V.fromList <$> vector n

deriving newtype instance Arbitrary RunNumber

instance Arbitrary r => Arbitrary (SnapIncomingRun r) where
  arbitrary = oneof [
        SnapMergingRun <$> arbitrary <*> arbitrary
                       <*> arbitrary <*> arbitrary
      , SnapSingleRun <$> arbitrary
      ]
  shrink (SnapMergingRun a b c d) =
      [ SnapMergingRun a' b' c' d'
      | (a', b', c', d') <- shrink (a, b, c, d) ]
  shrink (SnapSingleRun a)  = SnapSingleRun <$> shrink a

deriving newtype instance Arbitrary NumRuns

instance Arbitrary MergePolicyForLevel where
  arbitrary = elements [LevelTiering, LevelLevelling]
  shrink _ = []

instance (Arbitrary t, Arbitrary r) => Arbitrary (SnapMergingRunState t r) where
  arbitrary = oneof [
        SnapCompletedMerge <$> arbitrary <*> arbitrary <*> arbitrary
      , SnapOngoingMerge <$> arbitrary <*> arbitrary
                         <*> arbitrary <*> arbitrary
      ]
  shrink (SnapCompletedMerge a b c) =
      [ SnapCompletedMerge  a' b' c'
      | (a', b', c') <- shrink (a, b, c) ]
  shrink (SnapOngoingMerge a b c d) =
      [ SnapOngoingMerge  a' b' c' d'
      | (a', b', c', d') <- shrink (a, b, c, d) ]

deriving newtype instance Arbitrary MergeDebt
deriving newtype instance Arbitrary MergeCredits
deriving newtype instance Arbitrary NominalDebt
deriving newtype instance Arbitrary NominalCredits

{-------------------------------------------------------------------------------
  RunParams
-------------------------------------------------------------------------------}

instance Arbitrary RunParams where
  arbitrary = RunParams <$> arbitrary <*> arbitrary <*> arbitrary
  shrink (RunParams a b c) =
    [ RunParams  a' b' c'
    | (a', b', c') <- shrink (a, b, c) ]

instance Arbitrary RunDataCaching where
  arbitrary = elements [CacheRunData, NoCacheRunData]
  shrink NoCacheRunData = [CacheRunData]
  shrink _              = []

instance Arbitrary IndexType where
  arbitrary = elements [Ordinary, Compact]
  shrink Compact = [Ordinary]
  shrink _       = []

instance Arbitrary RunBloomFilterAlloc where
  arbitrary = oneof [
        RunAllocFixed      <$> arbitrary
      , RunAllocRequestFPR <$> arbitrary
      , RunAllocMonkey     <$> arbitrary
      ]
  shrink (RunAllocFixed x)      = RunAllocFixed <$> shrink x
  shrink (RunAllocRequestFPR x) = RunAllocRequestFPR <$> shrink x
  shrink (RunAllocMonkey x)     = RunAllocMonkey <$> shrink x

{-------------------------------------------------------------------------------
  Show
-------------------------------------------------------------------------------}

deriving stock instance Show SnapshotMetaData
deriving stock instance Show SnapshotRun
deriving stock instance Show r => Show (SnapLevels r)
deriving stock instance Show r => Show (SnapLevel r)
deriving stock instance Show r => Show (SnapIncomingRun r)
deriving stock instance (Show t, Show r) => Show (SnapMergingRunState t r)

deriving stock instance Show r => Show (SnapMergingTree r)
deriving stock instance Show r => Show (SnapMergingTreeState r)
deriving stock instance Show r => Show (SnapPendingMerge r)
deriving stock instance Show r => Show (SnapPreExistingRun r)

deriving stock instance Show MergeDebt
deriving stock instance Show MergeCredits
deriving stock instance Show NominalDebt
deriving stock instance Show NominalCredits

{-------------------------------------------------------------------------------
  Arbitrary: SnapshotMetaData
-------------------------------------------------------------------------------}

deriving newtype instance Arbitrary r => Arbitrary (SnapMergingTree r)

instance Arbitrary r => Arbitrary (SnapMergingTreeState r) where
  arbitrary = inductiveMergingTreeState inductiveLimit
  shrink (SnapCompletedTreeMerge a) = SnapCompletedTreeMerge <$> shrink a
  shrink (SnapPendingTreeMerge a)   = SnapPendingTreeMerge <$> shrink a
  shrink (SnapOngoingTreeMerge a)   = SnapOngoingTreeMerge <$> shrink a

instance Arbitrary r => Arbitrary (SnapPendingMerge r) where
  arbitrary = inductivePendingTreeMerge inductiveLimit
  shrink (SnapPendingUnionMerge a) = SnapPendingUnionMerge <$> shrinkList shrink a
  shrink (SnapPendingLevelMerge a b) =
      [ SnapPendingLevelMerge a' b' | a' <- shrinkList shrink a, b' <- shrink b ]


instance Arbitrary r => Arbitrary (SnapPreExistingRun r) where
  arbitrary = oneof [
        SnapPreExistingRun <$> arbitrary
      , SnapPreExistingMergingRun <$> arbitrary
      ]
  shrink (SnapPreExistingRun a)        = SnapPreExistingRun <$> shrink a
  shrink (SnapPreExistingMergingRun a) = SnapPreExistingMergingRun <$> shrink a

-- | The 'SnapMergingTree' is an inductive data-type and therefore we must limit
-- the recursive depth at which new 'Arbitrary' sub-trees are generated. Hence
-- the need for this limit. This limit is the "gas" for the inductive functions.
-- At reach recursive call, the "gas" value decremented until it reaches zero.
-- Each inductive function ensures it never create a forest of sub-trees greater
-- than the /monotonically decreasing/ gas parameter it received.
inductiveLimit :: Int
inductiveLimit = 4

-- |
-- Generate an 'Arbitrary', "gas-limited" 'SnapMergingTree'.
inductiveSized :: Arbitrary r => Int -> Gen (SnapMergingTree r)
inductiveSized = fmap SnapMergingTree . inductiveMergingTreeState

-- |
-- Generate an 'Arbitrary', "gas-limited" 'SnapMergingTreeState'.
inductiveMergingTreeState :: Arbitrary a => Int -> Gen (SnapMergingTreeState a)
inductiveMergingTreeState gas = oneof [
        SnapCompletedTreeMerge <$> arbitrary
      , SnapPendingTreeMerge <$> inductivePendingTreeMerge gas
      , SnapOngoingTreeMerge <$> arbitrary
      ]

-- |
-- Generate an 'Arbitrary', "gas-limited" 'SnapPendingMerge'.
inductivePendingTreeMerge :: Arbitrary a => Int -> Gen (SnapPendingMerge a)
inductivePendingTreeMerge gas = oneof [
        SnapPendingLevelMerge <$> genPreExistings <*> genMaybeSubTree
      , SnapPendingUnionMerge <$> genListSubtrees
      ]
    where
      subGen = inductiveSized . max 0 $ gas - 1
      -- Define custom generators to ensure that the sub-trees are less than
      -- or equal to the "gas" parameter.
      genPreExistings = genVectorsUpToBound gas arbitrary
      genListSubtrees = genVectorsUpToBound gas subGen
      genMaybeSubTree = oneof [ pure Nothing, Just <$> subGen ]
      genVectorsUpToBound x gen = oneof $ flip vectorOf gen <$> [ 0 .. x ]
