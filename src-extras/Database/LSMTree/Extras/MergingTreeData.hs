-- | Utilities for generating 'MergingTree's. Tests and benchmarks should
-- preferably use these utilities instead of (re-)defining their own.
module Database.LSMTree.Extras.MergingTreeData (
    -- * Create merging trees
    withMergingTree
  , unsafeCreateMergingTree
    -- * MergingTreeData
  , MergingTreeData (..)
  , PreExistingRunData (..)
  , mergingTreeDataInvariant
  , mapMergingTreeData
  , SerialisedMergingTreeData
  , serialiseMergingTreeData
    -- * QuickCheck
  , labelMergingTreeData
  , genMergingTreeData
  , shrinkMergingTreeData
  ) where

import           Control.Exception (assert, bracket)
import           Control.RefCount
import           Data.Foldable (for_, toList)
import           Database.LSMTree.Extras (showPowersOf)
import           Database.LSMTree.Extras.Generators ()
import           Database.LSMTree.Extras.MergingRunData
import           Database.LSMTree.Extras.RunData
import qualified Database.LSMTree.Internal.BloomFilter as Bloom
import qualified Database.LSMTree.Internal.MergingRun as MR
import           Database.LSMTree.Internal.MergingTree (MergingTree)
import qualified Database.LSMTree.Internal.MergingTree as MT
import           Database.LSMTree.Internal.RunBuilder (RunParams)
import           Database.LSMTree.Internal.Serialise
import           Database.LSMTree.Internal.UniqCounter
import qualified System.FS.API as FS
import           System.FS.API (HasFS)
import           System.FS.BlockIO.API (HasBlockIO)
import           Test.QuickCheck as QC

{-------------------------------------------------------------------------------
  Create merging tree
-------------------------------------------------------------------------------}

-- | Create a temporary 'MergingTree' using 'unsafeCreateMergingTree'.
withMergingTree ::
     HasFS IO h
  -> HasBlockIO IO h
  -> RefCtx
  -> ResolveSerialisedValue
  -> Bloom.Salt
  -> RunParams
  -> FS.FsPath
  -> UniqCounter IO
  -> SerialisedMergingTreeData
  -> (Ref (MergingTree IO h) -> IO a)
  -> IO a
withMergingTree hfs hbio refCtx resolve salt runParams path counter mrd = do
    bracket
      (unsafeCreateMergingTree hfs hbio refCtx resolve salt runParams path counter mrd)
      releaseRef

-- | Flush serialised merging tree data to disk.
--
-- This might leak resources if not run with asynchronous exceptions masked.
-- Consider using 'withMergingTree' instead.
--
-- Use of this function should be paired with a 'releaseRef'.
unsafeCreateMergingTree ::
     HasFS IO h
  -> HasBlockIO IO h
  -> RefCtx
  -> ResolveSerialisedValue
  -> Bloom.Salt
  -> RunParams
  -> FS.FsPath
  -> UniqCounter IO
  -> SerialisedMergingTreeData
  -> IO (Ref (MergingTree IO h))
unsafeCreateMergingTree hfs hbio refCtx resolve salt runParams path counter = go
  where
    go = \case
      CompletedTreeMergeData rd ->
        withRun hfs hbio refCtx salt runParams path counter rd $ \run ->
          MT.newCompletedMerge refCtx run
      OngoingTreeMergeData mrd ->
        withMergingRun hfs hbio refCtx resolve salt runParams path counter mrd $ \mr ->
          MT.newOngoingMerge refCtx mr
      PendingLevelMergeData prds mtd ->
        withPreExistingRuns prds $ \prs ->
          withMaybeTree mtd $ \mt ->
            MT.newPendingLevelMerge refCtx prs mt
      PendingUnionMergeData mtds ->
        withTrees mtds $ \mts ->
          MT.newPendingUnionMerge refCtx mts

    withTrees []         act = act []
    withTrees (mtd:rest) act =
        bracket (go mtd) releaseRef $ \t ->
          withTrees rest $ \ts ->
            act (t:ts)

    withMaybeTree Nothing    act = act Nothing
    withMaybeTree (Just mtd) act =
        bracket (go mtd) releaseRef $ \t ->
          act (Just t)

    withPreExistingRuns [] act = act []
    withPreExistingRuns (PreExistingRunData rd : rest) act =
        withRun hfs hbio refCtx salt runParams path counter rd $ \r ->
          withPreExistingRuns rest $ \prs ->
            act (MT.PreExistingRun r : prs)
    withPreExistingRuns (PreExistingMergingRunData mrd : rest) act =
        withMergingRun hfs hbio refCtx resolve salt runParams path counter mrd $ \mr ->
          withPreExistingRuns rest $ \prs ->
            act (MT.PreExistingMergingRun mr : prs)

{-------------------------------------------------------------------------------
  MergingTreeData
-------------------------------------------------------------------------------}

-- TODO: This module has quite a lot duplication with the prototype's
-- ScheduledMergesTest module. Maybe we can share some code?

-- | A data structure suitable for creating arbitrary 'MergingTree's.
--
-- Note: 'b ~ Void' should rule out blobs.
data MergingTreeData k v b =
    CompletedTreeMergeData (RunData k v b)
  | OngoingTreeMergeData (MergingRunData MR.TreeMergeType k v b)
  | PendingLevelMergeData
      [PreExistingRunData k v b]
      (Maybe (MergingTreeData k v b))  -- ^ not both empty!
  | PendingUnionMergeData [MergingTreeData k v b]  -- ^ at least 2 children
  deriving stock (Show, Eq)

data PreExistingRunData k v b =
    PreExistingRunData (RunData k v b)
  | PreExistingMergingRunData (MergingRunData MR.LevelMergeType k v b)
  deriving stock (Show, Eq)

mergingTreeIsStructurallyEmpty :: MergingTreeData k v b -> Bool
mergingTreeIsStructurallyEmpty = \case
    CompletedTreeMergeData _   -> False  -- could be, but we match MT
    OngoingTreeMergeData _     -> False
    PendingLevelMergeData ps t -> null ps && null t
    PendingUnionMergeData ts   -> null ts

-- | See @treeInvariant@ in prototype.
mergingTreeDataInvariant :: MergingTreeData k v b -> Either String ()
mergingTreeDataInvariant mtd
  | mergingTreeIsStructurallyEmpty mtd = Right ()
  | otherwise = case mtd of
      CompletedTreeMergeData _rd ->
        Right ()
      OngoingTreeMergeData mr ->
        mergingRunDataInvariant mr
      PendingLevelMergeData prs t -> do
        assertI "pending level merges have at least one input" $
          length prs + length t > 0
        for_ prs $ \case
          PreExistingRunData        _r -> Right ()
          PreExistingMergingRunData mr -> mergingRunDataInvariant mr
        for_ (drop 1 (reverse prs)) $ \case
          PreExistingRunData        _r -> Right ()
          PreExistingMergingRunData mr ->
            assertI "only the last pre-existing run can be a last level merge" $
              mergingRunDataMergeType mr == MR.MergeMidLevel
        for_ t mergingTreeDataInvariant
      PendingUnionMergeData ts -> do
        assertI "pending union merges are non-trivial (at least two inputs)" $
          length ts >= 2
        for_ ts mergingTreeDataInvariant
  where
    assertI msg False = Left msg
    assertI _   True  = Right ()

mapMergingTreeData ::
     Ord k'
  => (k -> k') -> (v -> v') -> (b -> b')
  -> MergingTreeData k v b -> MergingTreeData k' v' b'
mapMergingTreeData f g h = \case
    CompletedTreeMergeData r ->
      CompletedTreeMergeData $ mapRunData f g h r
    OngoingTreeMergeData mr ->
      OngoingTreeMergeData $ mapMergingRunData f g h mr
    PendingLevelMergeData prs t ->
      PendingLevelMergeData
        (map (mapPreExistingRunData f g h) prs)
        (fmap (mapMergingTreeData f g h) t)
    PendingUnionMergeData ts ->
      PendingUnionMergeData $ fmap (mapMergingTreeData f g h) ts

mapPreExistingRunData ::
     Ord k'
  => (k -> k') -> (v -> v') -> (b -> b')
  -> PreExistingRunData k v b -> PreExistingRunData k' v' b'
mapPreExistingRunData f g h = \case
    PreExistingRunData r ->
      PreExistingRunData (mapRunData f g h r)
    PreExistingMergingRunData mr ->
      PreExistingMergingRunData (mapMergingRunData f g h mr)

type SerialisedMergingTreeData =
    MergingTreeData SerialisedKey SerialisedValue SerialisedBlob

type SerialisedPreExistingRunData =
    PreExistingRunData SerialisedKey SerialisedValue SerialisedBlob

serialiseMergingTreeData ::
     (SerialiseKey k, SerialiseValue v, SerialiseValue b)
  => MergingTreeData k v b -> SerialisedMergingTreeData
serialiseMergingTreeData =
    mapMergingTreeData serialiseKey serialiseValue serialiseBlob

{-------------------------------------------------------------------------------
  QuickCheck
-------------------------------------------------------------------------------}

labelMergingTreeData :: SerialisedMergingTreeData -> Property -> Property
labelMergingTreeData = \rd ->
    tabulate "tree depth" [showPowersOf 2 (depthTree rd)] . go rd
  where
    go (CompletedTreeMergeData rd) =
          tabulate "merging tree state" ["CompletedTreeMerge"]
        . labelRunData rd
    go (OngoingTreeMergeData mrd) =
          tabulate "merging tree state" ["OngoingTreeMerge"]
        . labelMergingRunData mrd
    go (PendingLevelMergeData prds mtd) =
          tabulate "merging tree state" ["PendingLevelMerge"]
        . foldr ((.) . labelPreExistingRunData) id prds
        . maybe id go mtd
    go (PendingUnionMergeData mtds) =
          tabulate "merging tree state" ["PendingUnionMerge"]
        . foldr ((.) . go) id mtds

    -- the longest path from the root to a run
    depthTree = (+1) . \case  -- maximum depth of children
        CompletedTreeMergeData _ -> 0
        OngoingTreeMergeData _   -> 0
        PendingLevelMergeData prds mtds ->
          maximum (0 : fmap (const 1) prds ++ map depthTree (toList mtds))
        PendingUnionMergeData mtds ->
          maximum (0 : map depthTree mtds)


labelPreExistingRunData :: SerialisedPreExistingRunData -> Property -> Property
labelPreExistingRunData (PreExistingRunData rd)         = labelRunData rd
labelPreExistingRunData (PreExistingMergingRunData mrd) = labelMergingRunData mrd

instance ( Ord k, Arbitrary k, Arbitrary v, Arbitrary b
         ) => Arbitrary (MergingTreeData k v b) where
  arbitrary = genMergingTreeData arbitrary arbitrary arbitrary
  shrink = shrinkMergingTreeData shrink shrink shrink

genMergingTreeData ::
     Ord k => Gen k -> Gen v -> Gen b -> Gen (MergingTreeData k v b)
genMergingTreeData genKey genVal genBlob =
    QC.frequency
      -- Only at the root, we can have pending merges with no children, see
      -- 'MR.newPendingLevelMerge' and 'MR.newPendingUnionMerge'.
      [ ( 1, pure $ PendingLevelMergeData [] Nothing)
      , ( 1, pure $ PendingUnionMergeData [])
      , (50, QC.sized $ \s -> do
          treeSize <- QC.chooseInt (1, 1 + (s `div` 4)) -- up to 26
          genMergingTreeDataOfSize genKey genVal genBlob treeSize)
      ]

-- | Minimal returned size will be 1. Doesn't generate structurally empty trees!
--
-- The size is measured by the number of MergingTreeData constructors.
genMergingTreeDataOfSize ::
     forall k v b. Ord k
  => Gen k -> Gen v -> Gen b -> Int -> Gen (MergingTreeData k v b)
genMergingTreeDataOfSize genKey genVal genBlob = \n0 -> do
    tree <- genMergingTree n0
    assert (mergingTreeDataSize tree == n0) $
      pure tree
  where
    genMergingTree n
      | n < 1
      = error ("arbitrary T: n == " <> show n)

      | n == 1
      = QC.oneof
          [ CompletedTreeMergeData <$> genRun
          , OngoingTreeMergeData <$> genMergingRun arbitrary
          , genPendingLevelMergeNoChild
          ]

      | n == 2
      = genPendingLevelMergeWithChild n

      | otherwise
      = QC.oneof [genPendingLevelMergeWithChild n, genPendingUnionMerge n]

    -- n == 1
    genPendingLevelMergeNoChild = do
        numPreExisting <- chooseIntSkewed (0, 6)
        initPreExisting <- QC.vectorOf numPreExisting $
          -- these can't be last level. we generate the last input below.
          genPreExistingRun (pure MR.MergeMidLevel)
        -- there must be at least one (last) input to the pending merge.
        lastPreExisting <- genPreExistingRun arbitrary
        let preExisting = initPreExisting ++ [lastPreExisting]
        pure (PendingLevelMergeData preExisting Nothing)

    -- n >= 2
    genPendingLevelMergeWithChild n = do
        numPreExisting <- chooseIntSkewed (0, 6)
        preExisting <- QC.vectorOf numPreExisting $
          -- there can't be a last level merge, child is last
          genPreExistingRun (pure MR.MergeMidLevel)
        tree <- genMergingTree (n - 1)
        pure (PendingLevelMergeData preExisting (Just tree))

    -- n >= 3, needs 1 constructor + 2 children
    genPendingUnionMerge n = do
        ns <- QC.shuffle =<< arbitraryPartition2 (n - 1)
        PendingUnionMergeData <$> traverse genMergingTree ns

    genRun                    = genScaled genRunData
    genMergingRun genType     = genScaled (genMergingRunData genType)
    genPreExistingRun genType = genScaled (genPreExistingRunData genType)

    -- To avoid generating too large test cases, we reduce the number of
    -- entries for each run. The size of the individual entries is unaffected.
    genScaled :: forall r. (Gen k -> Gen v -> Gen b -> Gen r) -> Gen r
    genScaled gen =
        QC.sized $ \s ->
          QC.scale (`div` 2) $
            gen (QC.resize s genKey) (QC.resize s genVal) (QC.resize s genBlob)

    -- skewed towards smaller values
    chooseIntSkewed (lb, ub) = do
        ub' <- QC.chooseInt (lb, ub)
        QC.chooseInt (lb, ub')

mergingTreeDataSize :: MergingTreeData k v b -> Int
mergingTreeDataSize = \case
    CompletedTreeMergeData _ -> 1
    OngoingTreeMergeData _ -> 1
    PendingLevelMergeData _ tree -> 1 + maybe 0 mergingTreeDataSize tree
    PendingUnionMergeData trees -> 1 + sum (map mergingTreeDataSize trees)

-- Split into at least two smaller positive numbers. The input needs to be
-- greater than or equal to 2.
arbitraryPartition2 :: Int -> QC.Gen [Int]
arbitraryPartition2 n = assert (n >= 2) $ do
    first <- QC.chooseInt (1, n-1)
    (first :) <$> arbitraryPartition (n - first)

-- Split into smaller positive numbers.
arbitraryPartition :: Int -> QC.Gen [Int]
arbitraryPartition n
      | n <  1 = pure []
      | n == 1 = pure [1]
      | otherwise = do
        first <- QC.chooseInt (1, n)
        (first :) <$> arbitraryPartition (n - first)

-- TODO: Would it be useful to shrink by merging subtrees into a single run?
-- This would simplify the tree while preserving many errors that depend on the
-- specific content of the tree. See prototype tests.
shrinkMergingTreeData ::
     Ord k
  => (k -> [k])
  -> (v -> [v])
  -> (b -> [b])
  -> MergingTreeData k v b
  -> [MergingTreeData k v b]
shrinkMergingTreeData shrinkKey shrinkVal shrinkBlob = \case
  CompletedTreeMergeData r ->
    [ CompletedTreeMergeData r'
    | r' <- shrinkRunData shrinkKey shrinkVal shrinkBlob r
    ]
  OngoingTreeMergeData mr ->
    [ OngoingTreeMergeData mr'
    | mr' <- shrinkMergingRunData shrinkKey shrinkVal shrinkBlob mr
    ]
  PendingLevelMergeData prs t ->
    -- just use the child tree, if present
    [ t' | Just t' <- [t] ]
    <>
    -- move completed child tree into regular levels
    [ PendingLevelMergeData (prs ++ [PreExistingRunData r]) Nothing
    | Just (CompletedTreeMergeData r) <- [t]
    ]
    <>
    [ PendingLevelMergeData prs' t'
    | (prs', t') <-
        liftShrink2
          (liftShrink (shrinkPreExistingRunData shrinkKey shrinkVal shrinkBlob))
          (liftShrink (shrinkMergingTreeData shrinkKey shrinkVal shrinkBlob))
          (prs, t)
    , length prs' + length t' > 0
    ]
  PendingUnionMergeData ts ->
    ts
    <>
    [ PendingUnionMergeData ts'
    | ts' <- liftShrink (shrinkMergingTreeData shrinkKey shrinkVal shrinkBlob) ts
    , length ts' >= 2
    ]

genPreExistingRunData ::
     Ord k
  => Gen MR.LevelMergeType
  -> Gen k
  -> Gen v
  -> Gen b
  -> Gen (PreExistingRunData k v b)
genPreExistingRunData genMergeType genKey genVal genBlob =
    QC.oneof
      [ PreExistingRunData <$> genRunData genKey genVal genBlob
      , PreExistingMergingRunData <$> genMergingRunData genMergeType genKey genVal genBlob
      ]

shrinkPreExistingRunData ::
     Ord k
  => (k -> [k])
  -> (v -> [v])
  -> (b -> [b])
  -> PreExistingRunData k v b
  -> [PreExistingRunData k v b]
shrinkPreExistingRunData shrinkKey shrinkVal shrinkBlob = \case
    PreExistingRunData r ->
      [ PreExistingRunData r'
      | r' <- shrinkRunData shrinkKey shrinkVal shrinkBlob r
      ]
    PreExistingMergingRunData mr ->
      [ PreExistingMergingRunData mr'
      | mr' <- shrinkMergingRunData shrinkKey shrinkVal shrinkBlob mr
      ]
