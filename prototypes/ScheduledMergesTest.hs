module ScheduledMergesTest (tests) where

import           Control.Exception
import           Control.Monad (replicateM_, when)
import           Control.Monad.ST
import           Control.Tracer (Tracer (Tracer))
import qualified Control.Tracer as Tracer
import           Data.Foldable (traverse_)
import qualified Data.Map as Map
import           Data.STRef

import           ScheduledMerges as LSM

import qualified Test.QuickCheck as QC
import           Test.QuickCheck (Arbitrary (arbitrary, shrink), Property)
import           Test.QuickCheck.Exception (isDiscard)
import           Test.Tasty
import           Test.Tasty.HUnit (HasCallStack, testCase)
import           Test.Tasty.QuickCheck (testProperty, (=/=), (===))

tests :: TestTree
tests = testGroup "Unit and property tests"
    [ testCase "test_regression_empty_run" test_regression_empty_run
    , testCase "test_merge_again_with_incoming" test_merge_again_with_incoming
    , testProperty "prop_union" prop_union
    , testProperty "prop_MergingTree" prop_MergingTree
    ]

-- | Results in an empty run on level 2.
test_regression_empty_run :: IO ()
test_regression_empty_run =
    runWithTracer $ \tracer -> do
      stToIO $ do
        lsm <- LSM.new
        let ins k = LSM.insert tracer lsm (K k) (V 0) Nothing
        let del k = LSM.delete tracer lsm (K k)
        -- run 1
        ins 0
        ins 1
        ins 2
        ins 3
        -- run 2
        ins 0
        ins 1
        ins 2
        ins 3
        -- run 3
        ins 0
        ins 1
        ins 2
        ins 3
        -- run 4, deletes all previous elements
        del 0
        del 1
        del 2
        del 3

        expectShape lsm
          0
          [ ([], [4,4,4,4])
          ]

        -- run 5, results in last level merge of run 1-4
        ins 0
        ins 1
        ins 2
        ins 3

        expectShape lsm
          0
          [ ([], [4])
          , ([4,4,4,4], [])
          ]

        -- finish merge
        LSM.supplyMergeCredits lsm 16

        expectShape lsm
          0
          [ ([], [4])
          , ([], [0])
          ]

        -- insert more data, so the empty run becomes input to a merge
        traverse_ ins [101..112]

        expectShape lsm
          0
          [ ([], [4,4,4,4])  -- about to trigger a new last level merge
          , ([], [0])
          ]

        traverse_ ins [113..116]

        expectShape lsm
          0
          [ ([], [4])
          , ([4,4,4,4], [])  -- merge started, empty run has been dropped
          ]

-- | Covers the case where a run ends up too small for a level, so it gets
-- merged again with the next incoming runs.
-- That 5-way merge gets completed by supplying credits That merge gets
-- completed by supplying credits and then becomes part of another merge.
test_merge_again_with_incoming :: IO ()
test_merge_again_with_incoming =
    runWithTracer $ \tracer -> do
      stToIO $ do
        lsm <- LSM.new
        let ins k = LSM.insert tracer lsm (K k) (V 0) Nothing
        -- get something to 3rd level (so 2nd level is not levelling)
        -- (needs 5 runs to go to level 2 so the resulting run becomes too big)
        traverse_ ins [101..100+(5*16)]

        -- also get a very small run (4 elements) to 2nd level by re-using keys
        replicateM_ 4 $
          traverse_ ins [201..200+4]

        expectShape lsm
          0
          [ ([], [4,4,4,4])     -- these runs share keys, will compact down to 4
          , ([4,4,4,4,64], [])  -- this run will end up in level 3
          ]

        -- get another run to 2nd level, which the small run can be merged with
        traverse_ ins [301..300+16]

        expectShape lsm
          0
          [ ([], [4,4,4,4])
          , ([4,4,4,4], [])
          , ([], [80])
          ]

        -- add just one more run so the 5-way merge on 2nd level gets created
        traverse_ ins [401..400+4]

        expectShape lsm
          0
          [ ([], [4])
          , ([4,4,4,4,4], [])
          , ([], [80])
          ]

        -- complete the merge (20 entries, but credits get scaled up by 1.25)
        LSM.supplyMergeCredits lsm 16

        expectShape lsm
          0
          [ ([], [4])
          , ([], [20])
          , ([], [80])
          ]

        -- get 3 more runs to 2nd level, so the 5-way merge completes
        -- and becomes part of a new merge.
        -- (actually 4, as runs only move once a fifth run arrives...)
        traverse_ ins [501..500+(4*16)]

        expectShape lsm
          0
          [ ([], [4])
          , ([4,4,4,4], [])
          , ([16,16,16,20,80], [])
          ]

-------------------------------------------------------------------------------
-- properties
--

-- | Supplying enough credits for the remaining debt completes the union merge.
prop_union :: [[(LSM.Key, LSM.Op)]] -> Property
prop_union kopss = length (filter (not . null) kopss) > 1 QC.==>
    QC.ioProperty $ runWithTracer $ \tr ->
      stToIO $ do
        ts <- traverse (mkTable tr) kopss
        t <- LSM.unions ts

        debt <- LSM.remainingUnionDebt t
        _ <- LSM.supplyUnionCredits t debt
        debt' <- LSM.remainingUnionDebt t

        rep <- dumpRepresentation t
        return $ QC.counterexample (show (debt, debt')) $ QC.conjoin
          [ debt =/= 0
          , debt' === 0
          , hasUnionWith isCompleted rep
          ]
  where
    isCompleted = \case
        MLeaf{} -> True
        MNode{} -> False

mkTable :: Tracer (ST s) Event -> [(LSM.Key, LSM.Op)] -> ST s (LSM s)
mkTable tr ks = do
    t <- LSM.new
    LSM.updates tr t ks
    return t

-------------------------------------------------------------------------------
-- tests for MergingTree
--

prop_MergingTree :: T -> QC.InfiniteList SmallCredit -> Property
prop_MergingTree TCompleted{} _ = QC.discard
prop_MergingTree (TOngoing MCompleted{}) _ = QC.discard
prop_MergingTree t credits =
    QC.ioProperty $ runWithTracer $ \_tr ->
      stToIO $ do
        tree <- fromT t
        go tree (QC.getInfiniteList credits)
        (d', _) <- LSM.remainingDebtMergingTree tree
        return $
          QC.classify (d' <= 0) "got completed" $
            True
  where
    go tree (SmallCredit c : cs) = do
        c' <- LSM.supplyCreditsMergingTree c tree
        treeInvariant tree
        if c' > 0 then return ()
                  else go tree cs
    go _ _ = error "infinite list is finite"

newtype SmallCredit = SmallCredit Credit
  deriving stock Show

instance Arbitrary SmallCredit where
  arbitrary = SmallCredit <$> QC.chooseInt (1, 10)
  shrink (SmallCredit c) = [SmallCredit c' | c' <- shrink c, c' > 0]

-- simplified non-ST version of MergingTree
data T = TCompleted Run
       | TOngoing (M TreeMergeType)
       | TPendingLevel [I] (Maybe T)  -- not both empty!
       | TPendingUnion [T]  -- at least 2 children
  deriving stock Show

-- simplified non-ST version of IncomingRun
data I = ISingle Run
       | IMerging (M LevelMergeType)
  deriving stock Show

-- simplified non-ST version of MergingRun
data M t = MCompleted t Run
         | MOngoing
             t
             MergeDebt  -- debt bounded by input sizes
             [NonEmptyRun]  -- at least 2 inputs
  deriving stock Show

newtype NonEmptyRun = NonEmptyRun { getNonEmptyRun :: Run }
  deriving stock Show

fromT :: T -> ST s (MergingTree s)
fromT t = do
    state <- case t of
      TCompleted r -> return (CompletedTreeMerge r)
      TOngoing mr  -> OngoingTreeMerge <$> fromM mr
      TPendingLevel is mt ->
        fmap PendingTreeMerge $
          PendingLevelMerge <$> traverse fromI is <*> traverse fromT mt
      TPendingUnion ts -> do
        fmap PendingTreeMerge $ PendingUnionMerge <$> traverse fromT ts
    MergingTree <$> newSTRef state

fromI :: I -> ST s (IncomingRun s)
fromI (ISingle r)  = return (Single r)
fromI (IMerging m) = Merging MergePolicyTiering <$> fromM m

fromM :: IsMergeType t => M t -> ST s (MergingRun t s)
fromM m = do
    let (mergeType, state) = case m of
          MCompleted mt r  -> (mt, CompletedMerge r)
          MOngoing mt d rs -> (mt, OngoingMerge d rs' (mergek mt rs'))
            where rs' = map getNonEmptyRun rs
    MergingRun mergeType <$> newSTRef state

completeT :: T -> Run
completeT (TCompleted r) = r
completeT (TOngoing m)   = completeM m
completeT (TPendingLevel is t) =
    mergek MergeLevel (map completeI is <> maybe [] (pure . completeT) t)
completeT (TPendingUnion ts) =
    mergek MergeUnion (map completeT ts)

completeI :: I -> Run
completeI (ISingle r)  = r
completeI (IMerging m) = completeM m

completeM :: IsMergeType t => M t -> Run
completeM (MCompleted _ r)   = r
completeM (MOngoing mt _ rs) = mergek mt (map getNonEmptyRun rs)

instance Arbitrary T where
  arbitrary = QC.frequency
      [ (1, TCompleted <$> arbitrary)
      , (1, TOngoing <$> arbitrary)
      , (1, do
          (incoming, tree) <- arbitrary
             `QC.suchThat` (\(i, t) -> length i + length t > 0)
          return (TPendingLevel incoming tree))
      , (1, do
          n <- QC.frequency
            [ (3, pure 2)
            , (1, QC.chooseInt (3, 8))
            ]
          TPendingUnion <$> QC.vectorOf n (QC.scale (`div` n) arbitrary))
      ]

  shrink (TCompleted r) =
      [ TCompleted r'
      | r' <- shrink r
      ]
  shrink tree@(TOngoing m) =
      [ TCompleted (completeT tree) ]
   <> [ TOngoing m'
      | m' <- shrink m
      ]
  shrink tree@(TPendingLevel is t) =
      [ TCompleted (completeT tree) ]
   <> [ t' | Just t' <- [t] ]
   <> [ TPendingLevel (is ++ [ISingle r]) Nothing  -- move into regular levels
      | Just (TCompleted r) <- [t]
      ]
   <> [ TPendingLevel is' t'
      | (is', t') <- shrink (is, t)
      , length is' + length t' > 0
      ]
  shrink tree@(TPendingUnion ts) =
      [ TCompleted (completeT tree) ]
   <> ts
   <> [ TPendingUnion ts'
      | ts' <- shrink ts
      , length ts' > 1
      ]

instance Arbitrary I where
  arbitrary = QC.oneof [ISingle <$> arbitrary, IMerging <$> arbitrary]
  shrink (ISingle r)  = [ISingle r' | r' <- shrink r]
  shrink (IMerging m) = [ISingle (completeM m)] <> [IMerging m' | m' <- shrink m]

instance (Arbitrary t, IsMergeType t) => Arbitrary (M t) where
  arbitrary = QC.frequency
      [ (1, MCompleted <$> arbitrary <*> arbitrary)
      , (1, do
          mt <- arbitrary
          n <- QC.chooseInt (2, 8)
          rs <- QC.vectorOf n (QC.scale (`div` n) arbitrary)
          let totalWork = sum (map (length . getNonEmptyRun) rs)
          workRemaining <- QC.chooseInt (1, totalWork)
          unspentCredits <- QC.chooseInt (0, min mergeBatchSize workRemaining - 1)
          let d = MergeDebt unspentCredits workRemaining
          return (MOngoing mt d rs))
      ]

  shrink (MCompleted mt r) =
      [ MCompleted mt r' | r' <- shrink r ]
  shrink m@(MOngoing mt (MergeDebt c d) rs) =
      [ MCompleted mt (completeM m) ]
   <> [ MOngoing mt (MergeDebt c' d') rs'
      | rs' <- shrink rs
      , length rs' > 1
      , let d' = min d (sum (map (length . getNonEmptyRun) rs))
      , let c' = min c (d' - 1)
      ]

instance Arbitrary NonEmptyRun where
  arbitrary = do
      s <- QC.getSize
      n <- QC.chooseInt (1, min s 40 + 1)
      NonEmptyRun . Map.fromList <$> QC.vector n
  shrink (NonEmptyRun r) = [NonEmptyRun r' | r' <- shrink r, not (null r')]

-------------------------------------------------------------------------------
-- tracing and expectations on LSM shape
--

-- | Provides a tracer and will add the log of traced events to the reported
-- failure.
runWithTracer :: (Tracer (ST RealWorld) Event -> IO a) -> IO a
runWithTracer action = do
    events <- stToIO $ newSTRef []
    let tracer = Tracer $ Tracer.emit $ \e -> modifySTRef events (e :)
    action tracer `catch` \e -> do
      if isDiscard e  -- don't intercept these
        then throwIO e
        else do
          ev <- reverse <$> stToIO (readSTRef events)
          throwIO (Traced e ev)

data TracedException = Traced SomeException [Event]
  deriving stock (Show)

instance Exception TracedException where
  displayException (Traced e ev) =
    displayException e <> "\ntrace:\n" <> unlines (map show ev)

expectShape :: HasCallStack => LSM s -> Int -> [([Int], [Int])] -> ST s ()
expectShape lsm expectedWb expectedLevels = do
    let expected = (expectedWb, expectedLevels, Nothing)
    shape <- representationShape <$> dumpRepresentation lsm
    when (shape /= expected) $
      error $ unlines
        [ "expected shape: " <> show expected
        , "actual shape:   " <> show shape
        ]

hasUnionWith :: (MTree Int -> Bool) -> Representation -> Property
hasUnionWith p rep = do
    let (_, _, shape) = representationShape rep
    QC.counterexample "expected suitable Union" $
      QC.counterexample (show shape) $
        case shape of
          Nothing -> False
          Just t  -> p t
