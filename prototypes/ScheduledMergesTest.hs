module ScheduledMergesTest (tests) where

import           Control.Exception
import           Control.Monad (replicateM_, when)
import           Control.Monad.ST
import           Control.Tracer (Tracer (Tracer))
import qualified Control.Tracer as Tracer
import           Data.Foldable (find, traverse_)
import           Data.Maybe (fromJust)
import           Data.STRef
import           Text.Printf (printf)

import           ScheduledMerges as LSM

import qualified Test.QuickCheck as QC
import           Test.QuickCheck (Arbitrary (arbitrary, shrink), Property)
import           Test.QuickCheck.Exception (isDiscard)
import           Test.Tasty
import           Test.Tasty.HUnit (HasCallStack, testCase)
import           Test.Tasty.QuickCheck (QuickCheckMaxSize (..),
                     QuickCheckTests (..), testProperty, (=/=), (===))

tests :: TestTree
tests = testGroup "Unit and property tests"
    [ testCase "test_regression_empty_run" test_regression_empty_run
    , testCase "test_merge_again_with_incoming" test_merge_again_with_incoming
    , testProperty "prop_union" prop_union
    , testGroup "T"
        [ localOption (QuickCheckTests 1000) $  -- super quick, run more
            testProperty "Arbitrary satisfies invariant" prop_arbitrarySatisfiesInvariant
        , localOption (QuickCheckMaxSize 60) $  -- many shrinks for huge trees
            testProperty "Shrinking satisfies invariant" prop_shrinkSatisfiesInvariant
        ]
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
        LSM.supplyMergeCredits lsm (NominalCredit 16)

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
        LSM.supplyMergeCredits lsm (NominalCredit 16)

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
        res <- go tree (QC.getInfiniteList credits)
        return $
          res === Right ()
  where
    -- keep supplying until there is an error or the tree merge is completed
    go :: MergingTree s -> [SmallCredit] -> ST s (Either String ())
    go tree (SmallCredit c : cs) = do
        c' <- LSM.supplyCreditsMergingTree c tree
        evalInvariant (treeInvariant tree) >>= \case
          Left e   -> return (Left e)
          Right () -> if c' > 0 then return (Right ())
                                else go tree cs
    go _ [] = error "infinite list is finite"

newtype SmallCredit = SmallCredit Credit
  deriving stock Show

instance Arbitrary SmallCredit where
  arbitrary = SmallCredit <$> QC.chooseInt (1, 10)
  shrink (SmallCredit c) = [SmallCredit c' | c' <- shrink c, c' > 0]

-- simplified non-ST version of MergingTree
data T = TCompleted Run
       | TOngoing (M TreeMergeType)
       | TPendingLevel [P] (Maybe T)  -- not both empty!
       | TPendingUnion [T]  -- at least 2 children
  deriving stock Show

-- simplified non-ST version of PreExistingRun
data P = PRun Run
       | PMergingRun (M LevelMergeType)
  deriving stock Show

-- simplified non-ST version of MergingRun
data M t = MCompleted t MergeDebt Run
         | MOngoing
             t
             MergeDebt  -- debt bounded by input sizes
             MergeCredit
             [NonEmptyRun]  -- at least 2 inputs
  deriving stock Show

newtype NonEmptyRun = NonEmptyRun { getNonEmptyRun :: Run }
  deriving stock Show

invariantT :: T -> Either String ()
invariantT t = runST $ do
    tree <- fromT t
    evalInvariant (treeInvariant tree)

-- | Size is the number of T and P constructors.
sizeT :: T -> Int
sizeT (TCompleted _)        = 1
sizeT (TOngoing _)          = 1
sizeT (TPendingLevel ps mt) = sum (fmap sizeP ps) + maybe 0 sizeT mt
sizeT (TPendingUnion ts)    = sum (fmap sizeT ts)

sizeP :: P -> Int
sizeP (PRun _)        = 1
sizeP (PMergingRun _) = 1

-- | Depth is the longest path through the tree from the root to a leaf using T
-- and P constructors.
depthT :: T -> Int
depthT (TCompleted _) = 0
depthT (TOngoing _) = 0
depthT (TPendingLevel ps mt) =
    let depthPs = case ps of
          [] -> 0
          _  -> maximum (fmap depthP ps)
        depthMt = maybe 0 depthT mt
    in 1 + max depthPs depthMt
depthT (TPendingUnion ts) = case ts of
    [] -> 0
    _  -> 1 + maximum (fmap depthT ts)

depthP :: P -> Int
depthP (PRun _)        = 0
depthP (PMergingRun _) = 0

fromT :: T -> ST s (MergingTree s)
fromT t = do
    state <- case t of
      TCompleted r -> return (CompletedTreeMerge r)
      TOngoing mr  -> OngoingTreeMerge <$> fromM mr
      TPendingLevel ps mt ->
        fmap PendingTreeMerge $
          PendingLevelMerge <$> traverse fromP ps <*> traverse fromT mt
      TPendingUnion ts -> do
        fmap PendingTreeMerge $ PendingUnionMerge <$> traverse fromT ts
    MergingTree <$> newSTRef state

fromP :: P -> ST s (PreExistingRun s)
fromP (PRun r)        = pure (PreExistingRun r)
fromP (PMergingRun m) = PreExistingMergingRun <$> fromM m

fromM :: IsMergeType t => M t -> ST s (MergingRun t s)
fromM m = do
    let (mergeType, mergeDebt, state) = case m of
          MCompleted  mt md r  -> (mt, md, CompletedMerge r)
          MOngoing mt md mc rs -> (mt, md, OngoingMerge mc rs' (mergek mt rs'))
            where rs' = map getNonEmptyRun rs
    MergingRun mergeType mergeDebt <$> newSTRef state

completeT :: T -> Run
completeT (TCompleted r) = r
completeT (TOngoing m)   = completeM m
completeT (TPendingLevel is t) =
    mergek MergeLevel (map completeP is <> maybe [] (pure . completeT) t)
completeT (TPendingUnion ts) =
    mergek MergeUnion (map completeT ts)

completeP :: P -> Run
completeP (PRun r)        = r
completeP (PMergingRun m) = completeM m

completeM :: IsMergeType t => M t -> Run
completeM (MCompleted _ _ r)   = r
completeM (MOngoing mt _ _ rs) = mergek mt (map getNonEmptyRun rs)

-------------------------------------------------------------------------------
-- Generators
--

instance Arbitrary T where
  arbitrary = QC.sized $ \s -> do
      n <- QC.chooseInt (1, max 1 s)
      go n
    where
      -- n is the number of constructors of T and P
      go n | n < 1 = error ("arbitrary T: n == " <> show n)
      go n | n == 1 =
          QC.frequency
            [ (1, TCompleted <$> arbitrary)
            , (1, TOngoing <$> arbitrary)
            ]
      go n =
          QC.frequency
            [ (1, do
                -- pending level merge without child
                preExisting <- QC.vector (n - 1)  -- 1 for constructor itself
                return (TPendingLevel preExisting Nothing))
            , (1, do
                -- pending level merge with child
                numPreExisting <- QC.chooseInt (0, min 20 (n - 2))
                preExisting <- QC.vector numPreExisting
                tree <- go (n - numPreExisting - 1)
                return (TPendingLevel preExisting (Just tree)))
            , (2, do
                -- pending union merge
                ns <- QC.shuffle =<< arbitraryPartition2 n
                TPendingUnion <$> traverse go ns)
            ]

      -- Split into at least two smaller positive numbers. The input needs to be
      -- greater than or equal to 2.
      arbitraryPartition2 :: Int -> QC.Gen [Int]
      arbitraryPartition2 n = assert (n >= 2) $ do
          first <- QC.chooseInt (1, n-1)
          (first :) <$> arbitraryPartition (n - first)

      -- Split into smaller positive numbers.
      arbitraryPartition :: Int -> QC.Gen [Int]
      arbitraryPartition n
            | n <  1 = return []
            | n == 1 = return [1]
            | otherwise = do
              first <- QC.chooseInt (1, n)
              (first :) <$> arbitraryPartition (n - first)

  shrink (TCompleted r) =
      [ TCompleted r'
      | r' <- shrink r
      ]
  shrink tree@(TOngoing m) =
      [ TCompleted (completeT tree) ]
   <> [ TOngoing m'
      | m' <- shrink m
      ]
  shrink tree@(TPendingLevel ps t) =
      [ TCompleted (completeT tree) ]
   <> [ t' | Just t' <- [t] ]
   <> [ TPendingLevel (ps ++ [PRun r]) Nothing  -- move into regular levels
      | Just (TCompleted r) <- [t]
      ]
   <> [ TPendingLevel ps' t'
      | (ps', t') <- shrink (ps, t)
      , length ps' + length t' > 0
      ]
  shrink tree@(TPendingUnion ts) =
      [ TCompleted (completeT tree) ]
   <> ts
   <> [ TPendingUnion ts'
      | ts' <- shrink ts
      , length ts' > 1
      ]

instance Arbitrary P where
  arbitrary = QC.oneof [PRun <$> arbitrary, PMergingRun <$> arbitrary]
  shrink (PRun r)        = [PRun r' | r' <- shrink r]
  shrink (PMergingRun m) = [PRun (completeM m)]
                        <> [PMergingRun m' | m' <- shrink m]

instance (Arbitrary t, IsMergeType t) => Arbitrary (M t) where
  arbitrary = QC.oneof
      [ do (mt, r) <- arbitrary
           let md = MergeDebt (runSize r)
           pure (MCompleted mt md r)
      , do mt <- arbitrary
           n  <- QC.chooseInt (2, 8)
           rs <- QC.vectorOf n (QC.scale (`div` n) arbitrary)
           (md, mc) <- genMergeCreditForRuns rs
           pure (MOngoing mt md mc rs)
      ]

  shrink (MCompleted mt md r) =
      [ MCompleted mt md r' | r' <- shrink r ]
  shrink m@(MOngoing mt md mc rs) =
      [ MCompleted mt md (completeM m) ]
   <> [ MOngoing mt md' mc' rs'
      | rs' <- shrink rs
      , length rs' > 1
      , (md', mc') <- shrinkMergeCreditForRuns rs' mc
      ]

-- | The 'MergeDebt' and 'MergeCredit' must maintain a couple invariants:
--
-- * the total debt must be the same as the sum of the input run sizes;
-- * the supplied credit is less than the total merge debt.
--
genMergeCreditForRuns :: [NonEmptyRun] -> QC.Gen (MergeDebt, MergeCredit)
genMergeCreditForRuns rs = do
      let totalDebt    = sum (map (length . getNonEmptyRun) rs)
      suppliedCredits <- QC.chooseInt (0, totalDebt-1)
      unspentCredits  <- QC.chooseInt (0, min (mergeBatchSize-1) suppliedCredits)
      let spentCredits = suppliedCredits - unspentCredits
          md           = MergeDebt {
                           totalDebt
                         }
          mc           = MergeCredit {
                            unspentCredits,
                            spentCredits
                         }
      assert (mergeDebtInvariant md mc) $
        pure (md, mc)

-- | Shrink the 'MergeDebt' and 'MergeCredit' given the old 'MergeCredit' and
-- the already-shrunk runs.
--
-- Thus must maintain invariants, see 'genMergeCreditForDebt'.
--
shrinkMergeCreditForRuns :: [NonEmptyRun]
                         -> MergeCredit -> [(MergeDebt, MergeCredit)]
shrinkMergeCreditForRuns rs' MergeCredit {spentCredits, unspentCredits} =
    [ assert (mergeDebtInvariant md' mc')
      (md', mc')
    | let totalDebt'    = sum (map (length . getNonEmptyRun) rs')
    , suppliedCredits' <- shrink (min (spentCredits+unspentCredits)
                                      (totalDebt'-1))
    , unspentCredits'  <- shrink (min unspentCredits suppliedCredits')
    , let spentCredits' = suppliedCredits' - unspentCredits'
          md'           = MergeDebt {
                            totalDebt      = totalDebt'
                          }
          mc'           = MergeCredit {
                            spentCredits   = spentCredits',
                            unspentCredits = unspentCredits'
                          }
    ]

instance Arbitrary NonEmptyRun where
  arbitrary = NonEmptyRun <$> (arbitrary `QC.suchThat` (not . null))
  shrink (NonEmptyRun r) = [NonEmptyRun r' | r' <- shrink r, not (null r')]

prop_arbitrarySatisfiesInvariant :: T -> Property
prop_arbitrarySatisfiesInvariant t =
    QC.tabulate "Tree size" [showPowersOf 2 $ sizeT t] $
    QC.tabulate "Tree depth" [showPowersOf 2 $ depthT t] $
      Right () === invariantT t

prop_shrinkSatisfiesInvariant :: T -> Property
prop_shrinkSatisfiesInvariant t =
    QC.forAll (genShrinkTrace 4 t) $ \trace ->
      QC.tabulate "Trace length" [showPowersOf 2 $ length trace] $
      QC.conjoin $ flip map trace $ \(numAlternatives, t') ->
        QC.tabulate "Shrink alternatives" [showPowersOf 2 numAlternatives] $
          Right () === invariantT t'

-- | Iterative shrinks, and how many alternatives were possible at each point.
genShrinkTrace :: Arbitrary a => Int -> a -> QC.Gen [(Int, a)]
genShrinkTrace !n x
  | n <= 0 = return []
  | otherwise =
    case shrink x of
      [] -> return []
      xs -> do
        -- like QC.elements, but we want access to the length
        let len = length xs
        x' <- (xs !!) <$> QC.chooseInt (0, len - 1)
        ((len, x') :) <$> genShrinkTrace (n - 1) x'

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

-------------------------------------------------------------------------------
-- Printing utils
--

-- | Copied from @lsm-tree:extras.Database.LSMTree.Extras@
showPowersOf :: Int -> Int -> String
showPowersOf factor n
  | factor <= 1 = error "showPowersOf: factor must be larger than 1"
  | n < 0       = "n < 0"
  | n == 0      = "n == 0"
  | otherwise   = printf "%d <= n < %d" lb ub
  where
    ub = fromJust (find (n <) (iterate (* factor) factor))
    lb = ub `div` factor
