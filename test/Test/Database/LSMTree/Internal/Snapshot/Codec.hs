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
import qualified Data.Text as Text
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
    , test (Proxy @FencePointerIndexType)
    , test (Proxy @DiskCachePolicy)
    , test (Proxy @MergeSchedule)
      -- SnapLevels
    , test (Proxy @(SnapLevels SnapshotRun))
    , test (Proxy @(SnapLevel SnapshotRun))
    , test (Proxy @(V.Vector SnapshotRun))
    , test (Proxy @RunNumber)
    , test (Proxy @(SnapIncomingRun SnapshotRun))
    , test (Proxy @MergePolicyForLevel)
    , test (Proxy @RunDataCaching)
    , test (Proxy @RunBloomFilterAlloc)
    , test (Proxy @IndexType)
    , test (Proxy @RunParams)
    , test (Proxy @(SnapMergingRun LevelMergeType SnapshotRun))
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

instance Arbitrary SnapshotLabel where
  -- Ensure that the labeling string is not excessively long.
  -- If too long, negatively effects the number of shrinks required to reach the
  -- minimum example value.
  arbitrary = do
    prefix <- arbitraryPrintableChar
    suffix <- vectorOfUpTo 3 arbitraryPrintableChar
    pure . SnapshotLabel . Text.pack $ prefix : suffix
  shrink (SnapshotLabel txt) = SnapshotLabel <$> shrink txt

instance Arbitrary SnapshotTableType where
  arbitrary = elements [SnapSimpleTable, SnapFullTable]
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
      ]
  shrink (AllocFixed x)      = AllocFixed <$> shrink x
  shrink (AllocRequestFPR x) = AllocRequestFPR <$> shrink x

instance Arbitrary FencePointerIndexType where
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
  arbitrary = SnapLevels <$> arbitraryShortVector
  shrink (SnapLevels x) = SnapLevels . V.fromList <$> shrink (V.toList x)

instance Arbitrary r => Arbitrary (SnapLevel r) where
  arbitrary = SnapLevel <$> arbitrary <*> arbitraryShortVector
  shrink (SnapLevel a b) = [SnapLevel a' b' | (a', b') <- shrink (a, b)]

arbitraryShortVector :: Arbitrary a => Gen (V.Vector a)
arbitraryShortVector = V.fromList <$> vectorOfUpTo 5 arbitrary

vectorOfUpTo :: Int -> Gen a -> Gen [a]
vectorOfUpTo maxlen gen = do
      len <- chooseInt (0, maxlen)
      vectorOf len gen

instance Arbitrary RunNumber where
  arbitrary = RunNumber <$> arbitrarySizedNatural
  shrink (RunNumber n) =
       -- fewer shrinks
       [RunNumber 0 | n > 0]
    ++ [RunNumber (n `div` 2) | n >= 2]

instance Arbitrary r => Arbitrary (SnapIncomingRun r) where
  arbitrary = oneof [
        SnapIncomingMergingRun <$> arbitrary <*> arbitrary
                               <*> arbitrary <*> arbitrary
      , SnapIncomingSingleRun <$> arbitrary
      ]
  shrink (SnapIncomingMergingRun a b c d) =
      [ SnapIncomingMergingRun a' b' c' d'
      | (a', b', c', d') <- shrink (a, b, c, d) ]
  shrink (SnapIncomingSingleRun a)  = SnapIncomingSingleRun <$> shrink a

instance Arbitrary MergePolicyForLevel where
  arbitrary = elements [LevelTiering, LevelLevelling]
  shrink _ = []

instance (Arbitrary t, Arbitrary r) => Arbitrary (SnapMergingRun t r) where
  arbitrary = oneof [
        SnapCompletedMerge <$> arbitrary <*> arbitrary
      , SnapOngoingMerge <$> arbitrary <*> arbitrary
                         <*> arbitraryShortVector <*> arbitrary
      ]
  shrink (SnapCompletedMerge a b) =
      [ SnapCompletedMerge  a' b'
      | (a', b') <- shrink (a, b) ]
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
  shrink _ = []

instance Arbitrary IndexType where
  arbitrary = elements [Ordinary, Compact]
  shrink _ = []

instance Arbitrary RunBloomFilterAlloc where
  arbitrary = oneof [
        RunAllocFixed      <$> arbitrary
      , RunAllocRequestFPR <$> arbitrary
      ]
  shrink (RunAllocFixed x)      = RunAllocFixed <$> shrink x
  shrink (RunAllocRequestFPR x) = RunAllocRequestFPR <$> shrink x

{-------------------------------------------------------------------------------
  Show
-------------------------------------------------------------------------------}

deriving stock instance Show SnapshotMetaData
deriving stock instance Show SnapshotRun
deriving stock instance Show r => Show (SnapLevels r)
deriving stock instance Show r => Show (SnapLevel r)
deriving stock instance Show r => Show (SnapIncomingRun r)
deriving stock instance (Show t, Show r) => Show (SnapMergingRun t r)

deriving stock instance Show r => Show (SnapMergingTree r)
deriving stock instance Show r => Show (SnapMergingTreeState r)
deriving stock instance Show r => Show (SnapPendingMerge r)
deriving stock instance Show r => Show (SnapPreExistingRun r)

deriving stock instance Show MergeDebt
deriving stock instance Show NominalDebt
deriving stock instance Show NominalCredits

{-------------------------------------------------------------------------------
  Arbitrary: SnapshotMetaData
-------------------------------------------------------------------------------}

deriving newtype instance Arbitrary r => Arbitrary (SnapMergingTree r)

instance Arbitrary r => Arbitrary (SnapMergingTreeState r) where
  arbitrary = genMergingTreeState mergingTreeDepthLimit
  shrink (SnapCompletedTreeMerge a) = SnapCompletedTreeMerge <$> shrink a
  shrink (SnapPendingTreeMerge a)   = SnapPendingTreeMerge <$> shrink a
  shrink (SnapOngoingTreeMerge a)   = SnapOngoingTreeMerge <$> shrink a

instance Arbitrary r => Arbitrary (SnapPendingMerge r) where
  arbitrary = genPendingTreeMerge mergingTreeDepthLimit
  shrink (SnapPendingUnionMerge a) = SnapPendingUnionMerge <$> shrinkList shrink a
  shrink (SnapPendingLevelMerge a b) =
      [ SnapPendingLevelMerge a' b' | (a', b') <- shrink (a, b)]

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
mergingTreeDepthLimit :: Int
mergingTreeDepthLimit = 4

-- | Do not generate a number of direct child sub-trees greater than the this
-- branching limit. This simplifies the topology of trees generated
branchingLimit :: Int
branchingLimit = 3

-- | Generate an 'Arbitrary', "gas-limited" 'SnapMergingTreeState~'.
genMergingTreeState :: Arbitrary a => Int -> Gen (SnapMergingTreeState a)
genMergingTreeState gas =
   let perpetualCase = [
           SnapCompletedTreeMerge <$> arbitrary
         , SnapOngoingTreeMerge <$> arbitrary
         ]
       recursiveCase
         | gas == 0  = []
         | otherwise = [ SnapPendingTreeMerge <$> genPendingTreeMerge gas ]
   in  oneof $ perpetualCase <> recursiveCase

-- | Generate an 'Arbitrary', "gas-limited" 'SnapPendingMerge'.
genPendingTreeMerge :: Arbitrary a => Int -> Gen (SnapPendingMerge a)
genPendingTreeMerge gas =
    oneof [
      SnapPendingLevelMerge <$> genPreExistings <*> genMaybeSubTree
    , SnapPendingUnionMerge <$> genListSubtrees
    ]
  where
    -- Decrement the gas for the recursive calls
    nextGas = max 0 $ gas - 1
    subGen = SnapMergingTree <$> genMergingTreeState nextGas

    -- No recursive subtrees within here, so not constrained by gas.
    genPreExistings = vectorOfUpTo branchingLimit arbitrary

    -- Define custom generators to ensure that the sub-trees are less than
    -- or equal to the lesser of the "gas" parameter and the branching limit.
    genMaybeSubTree
      | gas == 0  = pure Nothing
      | otherwise = oneof [ pure Nothing, Just <$> subGen ]

    genListSubtrees = case gas of
      0 -> vectorOf 0 subGen
      _ ->
        -- This frequency distribution will uniformly at random select an
        -- n-ary tree topology with a specified branching factor.
        let recursiveOptions branching = \case
              0 -> 1
              depth ->
                let sub = recursiveOptions branching $ depth - 1
                in  sum $ (sub ^) <$> [ 0 .. branching ]
            probability e =
              let basis = recursiveOptions branchingLimit nextGas
              in  (basis ^ e, vectorOf e subGen)
        in  frequency $ probability <$> [ 0 .. branchingLimit ]
