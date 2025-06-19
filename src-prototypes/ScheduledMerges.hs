{-# LANGUAGE PatternSynonyms #-}

{-# OPTIONS_GHC -Wno-incomplete-uni-patterns #-}
{-# OPTIONS_GHC -Wno-partial-fields #-}

-- | A prototype of an LSM with explicitly scheduled incremental merges.
--
-- The scheduled incremental merges is about ensuring that the merging
-- work (CPU and I\/O) can be spread out over time evenly. This also means
-- the LSM update operations have worst case complexity rather than amortised
-- complexity, because they do a fixed amount of merging work each.
--
-- Another thing this prototype demonstrates is a design for duplicating tables
-- and sharing ongoing incremental merges.
--
-- Finally, it demonstrates a design for table unions, including a
-- representation for in-progress merging trees.
--
-- The merging policy that this prototype uses is \"lazy levelling\".
-- Each level is T times bigger than the previous level.
-- Lazy levelling means we use tiering for every level except the last level
-- which uses levelling. Though note that the first level always uses tiering,
-- even if the first level is also the last level. This is to simplify flushing
-- the write buffer: if we used levelling on the first level we would need a
-- code path for merging the write buffer into the first level.
--
module ScheduledMerges (
    -- * Main API
    LSM,
    LSMConfig (..),
    Key (K), Value (V), resolveValue, Blob (B),
    new,
    newWith,
    LookupResult (..),
    lookup, lookups,
    Op,
    Update (..),
    update, updates,
    insert, inserts,
    delete, deletes,
    mupsert, mupserts,
    supplyMergeCredits,
    duplicate,
    unions,
    Credit,
    Debt,
    remainingUnionDebt,
    supplyUnionCredits,

    -- * Test and trace
    MTree (..),
    logicalValue,
    Representation,
    dumpRepresentation,
    representationShape,
    Event,
    EventAt(..),
    EventDetail(..),
    MergingTree(..),
    MergingTreeState(..),
    PendingMerge(..),
    PreExistingRun(..),
    MergingRun(..),
    MergingRunState(..),
    MergePolicy(..),
    IsMergeType(..),
    TreeMergeType(..),
    LevelMergeType(..),
    MergeCredit(..),
    MergeDebt(..),
    NominalCredit(..),
    NominalDebt(..),
    Run,
    runSize,
    UnionCredits (..),
    supplyCreditsMergingTree,
    UnionDebt(..),
    remainingDebtMergingTree,
    mergek,
    mergeBatchSize,

    -- * Invariants
    Invariant,
    evalInvariant,
    treeInvariant,
    mergeDebtInvariant,

    -- * Run sizes
    levelNumberToMaxRunSize,
    runSizeToLevelNumber,
    maxWriteBufferSize,
    runSizeFitsInLevel,
    runSizeTooSmallForLevel,
    runSizeTooLargeForLevel,

    -- * Level capacity
    levelIsFull,
  ) where

import           Prelude hiding (lookup)

import           Data.Foldable (for_, toList, traverse_)
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import           Data.Maybe (catMaybes)
import           Data.STRef

import qualified Control.Exception as Exc (assert)
import           Control.Monad (foldM, forM, when)
import           Control.Monad.ST
import qualified Control.Monad.Trans.Except as E
import           Control.Tracer (Tracer, contramap, traceWith)
import           GHC.Stack (HasCallStack, callStack)

import           Text.Printf (printf)

import qualified Test.QuickCheck as QC

data LSM s  = LSMHandle !(STRef s Counter)
                        !LSMConfig
                        !(STRef s (LSMContent s))

-- | Configuration options for individual LSM tables.
data LSMConfig = LSMConfig {
      configMaxWriteBufferSize :: !Int
      -- | Also known as the parameter @T@
    , configSizeRatio          :: !Int
    }
  deriving stock (Show, Eq)

-- | A simple count of LSM operations to allow logging the operation
-- number in each event. This enables relating merge events to the
-- operation number (which is interesting for numerical representations
-- like this). We would not need this in the real implementation.
type Counter = Int

-- | The levels of the table, from most to least recently inserted.
data LSMContent s =
    LSMContent
      Buffer          -- ^ write buffer is level 0 of the table, in-memory
      (Levels s)      -- ^ \"regular\" levels 1+, on disk in real implementation
      (UnionLevel s)  -- ^ a potential last level

type Levels s = [Level s]

-- | The number of the level. The write buffer lives at level 0, and all other
-- levels are numbered starting from 1.
type LevelNo = Int

-- | A level is a sequence of resident runs at this level, prefixed by an
-- incoming run, which is usually multiple runs that are being merged. Once
-- completed, the resulting run will become a resident run at this level.
data Level s = Level !(IncomingRun s) ![Run]

-- | We represent single runs specially, rather than putting them in as a
-- 'CompletedMerge'. This is for two reasons: to see statically that it's a
-- single run without having to read the 'STRef', and secondly to make it easier
-- to avoid supplying merge credits. It's not essential, but simplifies things
-- somewhat.
data IncomingRun s = Merging !MergePolicy
                             !NominalDebt !(STRef s NominalCredit)
                             !(MergingRun LevelMergeType s)
                   | Single  !Run

-- | The merge policy for a LSM level can be either tiering or levelling.
-- In this design we use levelling for the last level, and tiering for
-- all other levels. The first level always uses tiering however, even if
-- it's also the last level. So 'MergePolicy' and 'LevelMergeType' are
-- orthogonal, all combinations are possible.
--
data MergePolicy = MergePolicyTiering | MergePolicyLevelling
  deriving stock (Eq, Show)

-- | A \"merging run\" is a mutable representation of an incremental merge.
-- It is also a unit of sharing between duplicated tables.
--
data MergingRun t s = MergingRun !t !MergeDebt
                                 !(STRef s MergingRunState)

data MergingRunState = CompletedMerge !Run
                     | OngoingMerge
                         !MergeCredit
                         ![Run]  -- ^ inputs of the merge
                         Run  -- ^ output of the merge (lazily evaluated)

-- | Merges can exist in different parts of the LSM, each with different options
-- for the exact merge operation performed.
class Show t => IsMergeType t where
  isLastLevel :: t -> Bool
  isUnion :: t -> Bool

-- | Different types of merges created as part of a regular (non-union) level.
--
-- A last level merge behaves differently from a mid-level merge: last level
-- merges can actually remove delete operations, whereas mid-level merges must
-- preserve them. This is orthogonal to the 'MergePolicy'.
data LevelMergeType = MergeMidLevel | MergeLastLevel
  deriving stock (Eq, Show)

instance IsMergeType LevelMergeType where
  isLastLevel = \case
      MergeMidLevel  -> False
      MergeLastLevel -> True
  isUnion = const False

-- | Different types of merges created as part of the merging tree.
--
-- Union merges follow the semantics of @Data.Map.unionWith (<>)@. Since
-- the input runs are semantically treated like @Data.Map@s, deletes are ignored
-- and inserts act like mupserts, so they need to be merged monoidally using
-- 'resolveValue'.
--
-- Trees can only exist on the union level, which is the last. Therefore, node
-- merges can always drop deletes.
data TreeMergeType = MergeLevel | MergeUnion
  deriving stock (Eq, Show)

instance IsMergeType TreeMergeType where
  isLastLevel = const True
  isUnion = \case
      MergeLevel -> False
      MergeUnion -> True

-- | An additional optional last level, created as a result of 'union'. It can
-- not only contain an ongoing merge of multiple runs, but a nested tree of
-- merges. See Note [Table Unions].
data UnionLevel s = NoUnion
                    -- | We track the debt to make sure it never increases.
                  | Union !(MergingTree s) !(STRef s Debt)

-- | A \"merging tree\" is a mutable representation of an incremental
-- tree-shaped nested merge. This allows to represent union merges of entire
-- tables, each of which itself first need to be merged to become a single run.
--
-- Trees have to support arbitrarily deep nesting, since each input to 'union'
-- might already contain an in-progress merging tree (which then becomes shared
-- between multiple tables).
--
-- See Note [Table Unions].
newtype MergingTree s = MergingTree (STRef s (MergingTreeState s))

data MergingTreeState s = CompletedTreeMerge !Run
                          -- | Reuses MergingRun (with its STRef) to allow
                          -- sharing existing merges.
                        | OngoingTreeMerge !(MergingRun TreeMergeType s)
                        | PendingTreeMerge !(PendingMerge s)

-- | A merge that is waiting for its inputs to complete.
--
-- The inputs can themselves be 'MergingTree's (with its STRef) to allow sharing
-- existing unions.
data PendingMerge s = -- | The inputs are entire content of a table, i.e. its
                      -- (merging) runs and finally a union merge (if that table
                      -- already contained a union).
                      PendingLevelMerge ![PreExistingRun s] !(Maybe (MergingTree s))
                      -- | Each input is a level merge of the entire content of
                      -- a table.
                    | PendingUnionMerge ![MergingTree s]

-- | This is much like an 'IncomingRun', and are created from them, but contain
-- only the essential information needed in a 'PendingLevelMerge'.
data PreExistingRun s = PreExistingRun  !Run
                      | PreExistingMergingRun !(MergingRun LevelMergeType s)

pendingContent :: PendingMerge s
               -> (TreeMergeType, [PreExistingRun s], [MergingTree s])
pendingContent = \case
    PendingLevelMerge prs t  -> (MergeLevel, prs, toList t)
    PendingUnionMerge     ts -> (MergeUnion, [],  ts)

{-# COMPLETE PendingMerge #-}
pattern PendingMerge :: TreeMergeType
                     -> [PreExistingRun s]
                     -> [MergingTree s]
                     -> PendingMerge s
pattern PendingMerge mt prs ts <- (pendingContent -> (mt, prs, ts))

type Run    = Map Key Op
type Buffer = Map Key Op

bufferToRun :: Buffer -> Run
bufferToRun = id

runSize :: Run -> Int
runSize = Map.size

bufferSize :: Buffer -> Int
bufferSize = Map.size

type Op = Update Value Blob

newtype Key = K Int
  deriving stock (Eq, Ord, Show)
  deriving newtype Enum

newtype Value  = V Int
  deriving stock (Eq, Show)

resolveValue :: Value -> Value -> Value
resolveValue (V x) (V y) = V (x + y)

newtype Blob = B Int
  deriving stock (Eq, Show)

-- | We use levelling on the last level, unless that is also the first level.
mergePolicyForLevel :: Int -> [Level s] -> UnionLevel s -> MergePolicy
mergePolicyForLevel 1 _  _       = MergePolicyTiering
mergePolicyForLevel _ [] NoUnion = MergePolicyLevelling
mergePolicyForLevel _ _  _       = MergePolicyTiering

-- | If there are no further levels provided, this level is the last one.
-- However, if a 'Union' is present, it acts as another (last) level.
mergeTypeForLevel :: [Level s] -> UnionLevel s -> LevelMergeType
mergeTypeForLevel [] NoUnion = MergeLastLevel
mergeTypeForLevel _  _       = MergeMidLevel

-- | Note that the invariants rely on the fact that levelling is only used on
-- the last level.
--
invariant :: forall s. LSMConfig -> LSMContent s -> ST s ()
invariant conf@LSMConfig{..} (LSMContent _ levels ul) = do
    levelsInvariant 1 levels
    case ul of
      NoUnion      -> pure ()
      Union tree _ -> expectInvariant (treeInvariant tree)
  where
    levelsInvariant :: Int -> Levels s -> ST s ()
    levelsInvariant !_ [] = pure ()

    levelsInvariant !ln (Level ir rs : ls) = do
      mrs <- case ir of
        Single r ->
          pure (CompletedMerge r)
        Merging mp _ _ (MergingRun mt _ ref) -> do
          assertST $ ln > 1  -- no merges on level 1
          assertST $ mp == mergePolicyForLevel ln ls ul
          assertST $ mt == mergeTypeForLevel ls ul
          readSTRef ref

      assertST $ length rs <= configSizeRatio - 1
      expectedRunLengths ln rs ls
      expectedMergingRunLengths ln ir mrs ls

      levelsInvariant (ln+1) ls

    -- All runs within a level "proper" (as opposed to the incoming runs
    -- being merged) should be of the correct size for the level.
    expectedRunLengths :: Int -> [Run] -> [Level s] -> ST s ()
    expectedRunLengths ln rs ls =
      case mergePolicyForLevel ln ls ul of
        -- Levels using levelling have only one (incoming) run, which almost
        -- always consists of an ongoing merge. The exception is when a
        -- levelling run becomes too large and is promoted, in that case
        -- initially there's no merge, but it is still represented as an
        -- 'IncomingRun', using 'Single'. Thus there are no other resident runs.
        MergePolicyLevelling -> assertST $ null rs && null ls
        -- Runs in tiering levels usually fit that size, but they can be one
        -- larger, if a run has been held back (creating a (T+1)-way merge).
        MergePolicyTiering   -> assertST $ all (\r -> runToLevelNumber MergePolicyTiering conf r `elem` [ln, ln+1]) rs
        -- (This is actually still not really true, but will hold in practice.
        -- In the pathological case, all runs passed to the next level can be
        -- factor ((T+1)/T) too large, and there the same holding back can lead to
        -- factor ((T+2)/T) etc., until at level 12 a run is two levels too large.

    -- Incoming runs being merged also need to be of the right size, but the
    -- conditions are more complicated.
    expectedMergingRunLengths :: Int -> IncomingRun s -> MergingRunState
                              -> [Level s] -> ST s ()
    expectedMergingRunLengths ln ir mrs ls =
      case mergePolicyForLevel ln ls ul of
        MergePolicyLevelling -> do
          case (ir, mrs) of
            -- A single incoming run (which thus didn't need merging) must be
            -- of the expected size range already
            (Single r, m) -> do
              assertST $ case m of CompletedMerge{} -> True
                                   OngoingMerge{}   -> False
              assertST $ runToLevelNumber MergePolicyLevelling conf r == ln

            -- A completed merge for levelling can be of almost any size at all!
            -- It can be smaller, due to deletions in the last level. But it
            -- can't be bigger than would fit into the next level.
            (_, CompletedMerge r) ->
              assertST $ runToLevelNumber MergePolicyLevelling conf r <= ln+1

            -- An ongoing merge for levelling should have T incoming runs of the
            -- right size for the level below (or slightly larger due to holding
            -- back underfull runs), and at most 1 run from this level. The run
            -- from this level can be of almost any size for the same reasons as
            -- above. Although if this is the first merge for a new level, it'll
            -- have only T runs.
            (_, OngoingMerge _ rs _) -> do
              assertST $ length rs `elem` [configSizeRatio, configSizeRatio + 1]
              assertST $ all (\r -> runSize r > 0) rs  -- don't merge empty runs
              let incoming = take configSizeRatio rs
              let resident = drop configSizeRatio rs
              assertST $ all (\r -> runToLevelNumber MergePolicyTiering conf r `elem` [ln-1, ln]) incoming
              assertST $ length resident `elem` [0, 1]
              assertST $ all (\r -> runToLevelNumber MergePolicyLevelling conf r <= ln+1) resident

        MergePolicyTiering ->
          case (ir, mrs, mergeTypeForLevel ls ul) of
            -- A single incoming run (which thus didn't need merging) must be
            -- of the expected size already
            (Single r, m, _) -> do
              assertST $ case m of CompletedMerge{} -> True
                                   OngoingMerge{}   -> False
              assertST $ runToLevelNumber MergePolicyTiering conf r == ln

            -- A completed last level run can be of almost any smaller size due
            -- to deletions, but it can't be bigger than the next level down.
            -- Note that tiering on the last level only occurs when there is
            -- a single level only.
            (_, CompletedMerge r, MergeLastLevel) -> do
              assertST $ ln == 1
              assertST $ runToLevelNumber MergePolicyTiering conf r <= ln+1

            -- A completed mid level run is usually of the size for the
            -- level it is entering, but can also be one smaller (in which case
            -- it'll be held back and merged again) or one larger (because it
            -- includes a run that has been held back before).
            (_, CompletedMerge r, MergeMidLevel) ->
              assertST $ runToLevelNumber MergePolicyTiering conf r `elem` [ln-1, ln, ln+1]

            -- An ongoing merge for tiering should have T incoming runs of the
            -- right size for the level below (or slightly larger due to holding
            -- back underfull runs), and at most 1 run held back due to being
            -- too small (which would thus also be of the size of the level
            -- below).
            (_, OngoingMerge _ rs _, _) -> do
              assertST $ length rs `elem` [configSizeRatio, configSizeRatio + 1]
              assertST $ all (\r -> runSize r > 0) rs  -- don't merge empty runs
              let incoming = take configSizeRatio rs
              let heldBack = drop configSizeRatio rs
              assertST $ all (\r -> runToLevelNumber MergePolicyTiering conf r `elem` [ln-1, ln]) incoming
              assertST $ length heldBack `elem` [0, 1]
              assertST $ all (\r -> runToLevelNumber MergePolicyTiering conf r == ln-1) heldBack

-- We don't make many assumptions apart from what the types already enforce.
-- In particular, there are no invariants on the progress of the merges,
-- since union merge credits are independent from the tables' regular level
-- merges.
treeInvariant :: MergingTree s -> Invariant s ()
treeInvariant tree@(MergingTree treeState) = do
    liftI (readSTRef treeState) >>= \case
      CompletedTreeMerge _ ->
        -- We don't require the completed merges to be non-empty, since even
        -- a (last-level) merge of non-empty runs can end up being empty.
        -- In the prototype it would be possible to ensure that empty runs are
        -- immediately trimmed from the tree, but this kind of normalisation
        -- is complicated with sharing. For example, merging runs and
        -- trees are shared, so if one of them completes as an empty run,
        -- all tables referencing it suddenly contain an empty run and would
        -- need to be updated immediately.
        pure ()

      OngoingTreeMerge mr ->
        mergeInvariant mr

      PendingTreeMerge (PendingLevelMerge prs t) -> do
        -- Non-empty, but can be just one input (see 'newPendingLevelMerge').
        -- Note that children of a pending merge can be empty runs, as noted
        -- above for 'CompletedTreeMerge'.
        assertI "pending level merges have at least one input" $
          length prs + length t > 0
        for_ prs $ \case
          PreExistingRun        _r -> pure ()
          PreExistingMergingRun mr -> mergeInvariant mr
        for_ t treeInvariant

      PendingTreeMerge (PendingUnionMerge ts) -> do
        assertI "pending union merges are non-trivial (at least two inputs)" $
          length ts > 1
        for_ ts treeInvariant

    (debt, _) <- liftI $ remainingDebtMergingTree tree
    when (debt <= 0) $ do
      _ <- isCompletedMergingTree tree
      pure ()

mergeInvariant :: MergingRun t s -> Invariant s ()
mergeInvariant (MergingRun _ mergeDebt ref) =
    liftI (readSTRef ref) >>= \case
      CompletedMerge _ -> pure ()
      OngoingMerge mergeCredit rs _ -> do
        assertI "merge debt & credit invariant" $
          mergeDebtInvariant mergeDebt mergeCredit
        assertI "inputs to ongoing merges aren't empty" $
          all (\r -> runSize r > 0) rs
        assertI "ongoing merges are non-trivial (at least two inputs)" $
          length rs > 1

isCompletedMergingRun :: MergingRun t s -> Invariant s Run
isCompletedMergingRun (MergingRun _ d ref) = do
    mrs <- liftI $ readSTRef ref
    case mrs of
      CompletedMerge r   -> pure r
      OngoingMerge c _ _ -> failI $ "not completed: OngoingMerge with"
                                 ++ " remaining debt "
                                 ++ show (mergeDebtLeft d c)

isCompletedMergingTree :: MergingTree s -> Invariant s Run
isCompletedMergingTree (MergingTree ref) = do
    mts <- liftI $ readSTRef ref
    case mts of
      CompletedTreeMerge r -> pure r
      OngoingTreeMerge mr  -> isCompletedMergingRun mr
      PendingTreeMerge _   -> failI $ "not completed: PendingTreeMerge"

type Invariant s = E.ExceptT String (ST s)

assertI :: String -> Bool -> Invariant s ()
assertI _ True  = pure ()
assertI e False = failI e

failI :: String -> Invariant s a
failI = E.throwE

liftI :: ST s a -> Invariant s a
liftI = E.ExceptT . fmap Right

expectInvariant :: HasCallStack => Invariant s a -> ST s a
expectInvariant act = E.runExceptT act >>= either error pure

evalInvariant :: Invariant s a -> ST s (Either String a)
evalInvariant = E.runExceptT

-- 'callStack' just ensures that the 'HasCallStack' constraint is not redundant
-- when compiling with debug assertions disabled.
assert :: HasCallStack => Bool -> a -> a
assert p x = Exc.assert p (const x callStack)

assertST :: HasCallStack => Bool -> ST s ()
assertST p = assert p $ pure ()

assertWithMsg :: HasCallStack => Maybe String -> a -> a
assertWithMsg = assert . p
  where
    p Nothing    = True
    p (Just msg) = error $ "Assertion failed: " <> msg

assertWithMsgM :: (HasCallStack, Monad m) => Maybe String -> m ()
assertWithMsgM mmsg = assertWithMsg mmsg $ pure ()

leq :: (Show a, Ord a) => a -> a -> Maybe String
leq x y = if x <= y then Nothing else Just $
    printf "Expected x <= y, but got %s > %s"
      (show x)
      (show y)

-------------------------------------------------------------------------------
-- Run sizes
--

-- | Compute the maximum size of a run for a given level.
--
-- The size of a tiering run at each level is allowed to be
-- @bufferSize*sizeRatio^(level-1) < size <= bufferSize*sizeRatio^level@.
--
-- >>> levelNumberToMaxRunSize MergePolicyTiering (LSMConfig 2 4) <$> [0, 1, 2, 3, 4]
-- [0,2,8,32,128]
--
-- The @size@ of a levelling run at each level is allowed to be
-- @bufferSize*sizeRatio^level < size <= bufferSize*sizeRatio^(level+1)@. A
-- levelling run can take take up a whole level, so the maximum size of a run is
-- @sizeRatio@ tmes larger than the maximum size of a tiering run on the same
-- level.
--
-- >>> levelNumberToMaxRunSize MergePolicyLevelling (LSMConfig 2 4) <$> [0, 1, 2, 3, 4]
-- [0,8,32,128,512]
levelNumberToMaxRunSize :: HasCallStack => MergePolicy -> LSMConfig -> LevelNo -> Int
levelNumberToMaxRunSize = \case
    MergePolicyTiering -> levelNumberToMaxRunSizeTiering
    MergePolicyLevelling -> levelNumberToMaxRunSizeLevelling

-- | See 'levelNumberToMaxRunSize'
levelNumberToMaxRunSizeTiering :: HasCallStack => LSMConfig -> LevelNo -> Int
levelNumberToMaxRunSizeTiering
  LSMConfig {configMaxWriteBufferSize = bufSize, configSizeRatio = sizeRatio}
  ln
  | ln < 0 = error "level number must be non-negative"
  | ln == 0 = 0
  | otherwise = fromIntegerChecked (toInteger bufSize * toInteger sizeRatio ^ pred (toInteger ln))
      -- Perform the computation with arbitrary precision using 'Integers', but
      -- throw an error if the result does not fit into an 'Int'.

-- | See 'levelNumberToMaxRunSize'
levelNumberToMaxRunSizeLevelling :: HasCallStack => LSMConfig -> LevelNo -> Int
levelNumberToMaxRunSizeLevelling conf ln
  | ln < 0 = error "level number must be non-negative"
  | ln == 0 = 0
  | otherwise = levelNumberToMaxRunSizeTiering conf (succ ln)

-- | See 'runSizeToLevelNumber'.
runToLevelNumber :: HasCallStack => MergePolicy -> LSMConfig -> Run -> LevelNo
runToLevelNumber mpl conf run = runSizeToLevelNumber mpl conf (runSize run)

-- | Compute the appropriate level for the size of the given run.
--
-- See 'levelNumberToMaxRunSize' for the bounds on (tiering or levelling) run
-- sizes at each level.
--
-- >>> runSizeToLevelNumber MergePolicyTiering (LSMConfig 2 4) <$> [0,2,8,32,128]
-- [0,1,2,3,4]
--
-- >>> runSizeToLevelNumber MergePolicyLevelling (LSMConfig 2 4) <$> [0,8,32,128,512]
-- [0,1,2,3,4]
runSizeToLevelNumber :: HasCallStack => MergePolicy -> LSMConfig -> Int -> LevelNo
runSizeToLevelNumber = \case
    MergePolicyTiering -> runSizeToLevelNumberTiering
    MergePolicyLevelling -> runSizeToLevelNumberLevelling

-- | See 'runSizeToLevelNumber'.
runSizeToLevelNumberTiering :: HasCallStack => LSMConfig -> Int -> LevelNo
runSizeToLevelNumberTiering conf n
  | n < 0 = error "run size must be positive"
  -- TODO: enumerating level numbers is potentially costly, but it does gives a
  -- precise answer, where we'd otherwise have to deal with Double rounding
  -- errors in computing @ln = logBase configSizeRatio (n / configMaxWriteBufferSize) + 1@
  | otherwise = head $ -- the list is guaranteed to be non-empty
      [ ln
      | ln <- [0..]
      , n <= levelNumberToMaxRunSizeTiering conf ln
      ]

-- | See 'runSizeToLevelNumber'.
runSizeToLevelNumberLevelling :: HasCallStack => LSMConfig -> Int -> LevelNo
runSizeToLevelNumberLevelling conf n
  | n < 0 = error "run size must be positive"
  -- TODO: enumerating level numbers is potentially costly, but it does gives a
  -- precise answer, where we'd otherwise have to deal with Double rounding
  -- errors in computing @ln = logBase configSizeRatio (n / configMaxWriteBufferSize)@
  | otherwise = head $ -- the list is guaranteed to be non-empty
      [ ln
      | ln <- [0..]
      , n <= levelNumberToMaxRunSizeLevelling conf ln
      ]

maxWriteBufferSize :: HasCallStack => LSMConfig -> Int
maxWriteBufferSize conf = levelNumberToMaxRunSizeTiering conf 1 -- equal to configMaxWriteBufferSize

{-# INLINABLE fromIntegerChecked #-}
-- | Like 'fromInteger', but throws an error when @(x :: Integer) /= toInteger
-- (fromInteger x :: b)@.
fromIntegerChecked :: (HasCallStack, Integral a) => Integer -> a
fromIntegerChecked x
  | x'' == x
  = x'
  | otherwise
  = error $ printf "fromIntegerChecked: conversion failed, %s /= %s" (show x) (show x'')
  where
    x' = fromInteger x
    x'' = toInteger x'

-- | See 'runSizeFitsInLevel'.
_runFitsInLevel :: HasCallStack => MergePolicy -> LSMConfig -> LevelNo -> Run -> Bool
_runFitsInLevel mpl conf ln r = runSizeFitsInLevel mpl conf ln (runSize r)

-- | Check wheter a run of the given size fits in the given level.
--
-- See 'levelNumberToMaxRunSize' for the bounds on (tiering or levelling) run
-- sizes at each level.
--
-- >>> runSizeFitsInLevel MergePolicyTiering (LSMConfig 2 4) 3 <$> [8,9,16,32,33]
-- [False,True,True,True,False]
--
-- >>> runSizeFitsInLevel MergePolicyLevelling (LSMConfig 2 4) 2 <$> [8,9,16,32,33]
-- [False,True,True,True,False]
runSizeFitsInLevel :: HasCallStack => MergePolicy -> LSMConfig -> LevelNo -> Int -> Bool
runSizeFitsInLevel mpl conf ln n
  | ln < 0 = error "level number must be non-negative"
  | ln == 0 = n == 0
  | otherwise =
         levelNumberToMaxRunSize mpl conf (pred ln) < n
      && n <= levelNumberToMaxRunSize mpl conf ln

-- | See 'runSizeTooSmallForLevel'.
runTooSmallForLevel :: HasCallStack => MergePolicy -> LSMConfig -> LevelNo -> Run -> Bool
runTooSmallForLevel mpl conf ln r = runSizeTooSmallForLevel mpl conf ln (runSize r)

-- | Check wheter a run of the given size is too small for the given level.
--
-- See 'levelNumberToMaxRunSize' for the bounds on (tiering or levelling) run
-- sizes at each level.
--
-- >>> runSizeTooSmallForLevel MergePolicyTiering (LSMConfig 2 4) 3 <$> [8,9]
-- [True,False]
--
-- >>> runSizeTooSmallForLevel MergePolicyLevelling (LSMConfig 2 4) 2 <$> [8,9]
-- [True,False]
runSizeTooSmallForLevel :: HasCallStack => MergePolicy -> LSMConfig -> LevelNo -> Int -> Bool
runSizeTooSmallForLevel mpl conf ln n
  | ln < 0 = error "level number must be non-negative"
  | ln == 0 = False
  | otherwise = case mpl of
      MergePolicyTiering ->
        n <= levelNumberToMaxRunSize MergePolicyTiering conf (pred ln)
      MergePolicyLevelling ->
        n <= levelNumberToMaxRunSize MergePolicyLevelling conf (pred ln)

-- | See 'runSizeTooLargeForLevel'.
runTooLargeForLevel :: HasCallStack =>MergePolicy -> LSMConfig -> LevelNo -> Run -> Bool
runTooLargeForLevel mpl conf ln r = runSizeTooLargeForLevel mpl conf ln (runSize r)

-- | Check wheter a run of the given size is too large for the given level.
--
-- See 'levelNumberToMaxRunSize' for the bounds on (tiering or levelling) run
-- sizes at each level.
--
-- >>> runSizeTooLargeForLevel MergePolicyTiering (LSMConfig 2 4) 2 <$> [8,9]
-- [False,True]
--
-- >>> runSizeTooLargeForLevel MergePolicyLevelling (LSMConfig 2 4) 1 <$> [8,9]
-- [False,True]
runSizeTooLargeForLevel :: HasCallStack => MergePolicy -> LSMConfig -> LevelNo -> Int -> Bool
runSizeTooLargeForLevel mpl conf ln n
  | ln < 0 = error "level number must be non-negative"
  | ln == 0 = not (n == 0)
  | otherwise = case mpl of
      MergePolicyTiering ->
        n > levelNumberToMaxRunSize MergePolicyTiering conf ln
      MergePolicyLevelling ->
        n > levelNumberToMaxRunSize MergePolicyLevelling conf ln

-------------------------------------------------------------------------------
-- Level capacity
--

levelIsFull :: MergePolicy -> LSMConfig -> LevelNo -> [Run] -> [Run] -> Bool
levelIsFull mpl conf ln incoming resident = case mpl of
    MergePolicyTiering -> levelIsFullTiering conf ln incoming resident
    MergePolicyLevelling ->
      assert (length resident == 1) $
      levelIsFullLevelling conf ln incoming (head resident)

-- | Only based on run count, not their sizes.
levelIsFullTiering :: LSMConfig -> LevelNo -> [Run] -> [Run] -> Bool
levelIsFullTiering LSMConfig{..} _ln _incoming resident =
    length resident >= configSizeRatio

-- | The level is only considered full once the resident run is /too large/
-- for the level.
levelIsFullLevelling :: LSMConfig -> LevelNo -> [Run] -> Run -> Bool
levelIsFullLevelling conf ln _incoming resident =
    runTooLargeForLevel MergePolicyLevelling conf ln resident

-------------------------------------------------------------------------------
-- Merging credits
--

-- | Credits for keeping track of merge progress. These credits correspond
-- directly to merge steps performed.
--
-- We also call these \"physical\" credits (since they correspond to steps
-- done), and as opposed to \"nominal\" credits in 'NominalCredit' and
-- 'NominalDebt'.
type Credit = Int

-- | Debt for keeping track of the total merge work to do.
type Debt = Int

data MergeCredit =
     MergeCredit {
       spentCredits   :: !Credit, -- accumulating
       unspentCredits :: !Credit  -- fluctuating
     }
  deriving stock Show

newtype MergeDebt =
        MergeDebt {
          totalDebt :: Debt  -- fixed
        }
  deriving stock Show

zeroMergeCredit :: MergeCredit
zeroMergeCredit =
    MergeCredit {
      spentCredits   = 0,
      unspentCredits = 0
    }

mergeDebtInvariant :: MergeDebt -> MergeCredit -> Bool
mergeDebtInvariant MergeDebt {totalDebt}
                   MergeCredit {spentCredits, unspentCredits} =
    let suppliedCredits = spentCredits + unspentCredits
     in spentCredits    >= 0
     -- unspentCredits could legitimately be negative, though that does not
     -- happen in this prototype
     && suppliedCredits >= 0
     && suppliedCredits <= totalDebt

mergeDebtLeft :: HasCallStack => MergeDebt -> MergeCredit -> Debt
mergeDebtLeft MergeDebt {totalDebt}
              MergeCredit {spentCredits, unspentCredits} =
    let suppliedCredits = spentCredits + unspentCredits
     in assert (suppliedCredits <= totalDebt)
               (totalDebt - suppliedCredits)

-- | As credits are paid, debt is reduced in batches when sufficient credits
-- have accumulated.
data MergeDebtPaydown =
    -- | This remaining merge debt is fully paid off, potentially with
    -- leftovers.
    MergeDebtDischarged !Debt !Credit

    -- | Credits were paid, but not enough for merge debt to be reduced by some
    -- batches of merging work.
  | MergeDebtPaydownCredited !MergeCredit

    -- | Enough credits were paid to reduce merge debt by performing some
    -- batches of merging work.
  | MergeDebtPaydownPerform !Debt !MergeCredit
  deriving stock Show

-- | Pay credits to merge debt, which might trigger performing some merge work
-- in batches. See 'MergeDebtPaydown'.
--
paydownMergeDebt :: MergeDebt -> MergeCredit -> Credit -> MergeDebtPaydown
paydownMergeDebt MergeDebt {totalDebt}
                 MergeCredit {spentCredits, unspentCredits}
                 c
  | suppliedCredits' >= totalDebt
  , let !leftover = suppliedCredits' - totalDebt
        !perform  = c - leftover
  = assert (dischargePostcondition perform leftover) $
    MergeDebtDischarged perform leftover

  | unspentCredits' >= mergeBatchSize
  , let (!b, !r)         = divMod unspentCredits' mergeBatchSize
        !perform         = b * mergeBatchSize
  = assert (performPostcondition perform r) $
    MergeDebtPaydownPerform
      perform
      MergeCredit {
        spentCredits   = spentCredits    + perform,
        unspentCredits = unspentCredits' - perform
      }

  | otherwise
  = assert creditedPostcondition $
    MergeDebtPaydownCredited
      MergeCredit {
        spentCredits,
        unspentCredits = unspentCredits'
      }
  where
    suppliedCredits' = spentCredits + unspentCredits + c
    unspentCredits'  =                unspentCredits + c

    dischargePostcondition perform leftover =
          (c >= 0)
       && (perform >= 0 && leftover >= 0)
       && (c == perform + leftover)
       && (spentCredits + unspentCredits + perform == totalDebt)

    performPostcondition perform r =
      let spentCredits'    = spentCredits    + perform
          unspentCredits'' = unspentCredits' - perform
       in (c >= 0)
       && (unspentCredits'' == r)
       && (suppliedCredits' == spentCredits' + unspentCredits'')
       && (suppliedCredits' < totalDebt)

    creditedPostcondition =
          (c >= 0)
       && (suppliedCredits' < totalDebt)

mergeBatchSize :: Int
mergeBatchSize = 32


-------------------------------------------------------------------------------
-- Merging run abstraction
--

newMergingRun :: IsMergeType t => t -> [Run] -> ST s (MergingRun t s)
newMergingRun mergeType runs = do
    assertST $ length runs > 1
    -- in some cases, no merging is required at all
    (debt, state) <- case filter (\r -> runSize r > 0) runs of
      []  -> let (r:_) = runs -- just reuse the empty input
              in pure (runSize r, CompletedMerge r)
      [r] -> pure (runSize r, CompletedMerge r)
      rs  -> do
        -- The (physical) debt is always exactly the cost (merge steps),
        -- which is the sum of run lengths in elements.
        let !debt  = sum (map runSize rs)
        let merged = mergek mergeType rs  -- deliberately lazy
        pure (debt, OngoingMerge zeroMergeCredit rs merged)
    MergingRun mergeType (MergeDebt debt) <$> newSTRef state

mergek :: IsMergeType t => t -> [Run] -> Run
mergek t =
      (if isLastLevel t then Map.filter (/= Delete) else id)
    . Map.unionsWith (if isUnion t then combineUnion else combine)

-- | Combines two entries that have been performed after another. Therefore, the
-- newer one overwrites the old one (or modifies it for 'Mupsert'). Only take a
-- blob from the left entry.
combine :: Op -> Op -> Op
combine new_ old = case new_ of
    Insert{}  -> new_
    Delete{}  -> new_
    Mupsert v -> case old of
      Insert v' _ -> Insert (resolveValue v v') Nothing
      Delete      -> Insert v Nothing
      Mupsert v'  -> Mupsert (resolveValue v v')

-- | Combines two entries of runs that have been 'union'ed together. If any one
-- has a value, the result should have a value (represented by 'Insert'). If
-- both have a value, these values get combined monoidally. Only take a blob
-- from the left entry.
--
-- See 'MergeUnion'.
combineUnion :: Op -> Op -> Op
combineUnion Delete         (Mupsert v)  = Insert v Nothing
combineUnion Delete         old          = old
combineUnion (Mupsert u)    Delete       = Insert u Nothing
combineUnion new_           Delete       = new_
combineUnion (Mupsert v')   (Mupsert v ) = Insert (resolveValue v' v) Nothing
combineUnion (Mupsert v')   (Insert v _) = Insert (resolveValue v' v) Nothing
combineUnion (Insert v' b') (Mupsert v)  = Insert (resolveValue v' v) b'
combineUnion (Insert v' b') (Insert v _) = Insert (resolveValue v' v) b'

expectCompletedMergingRun :: HasCallStack => MergingRun t s -> ST s Run
expectCompletedMergingRun = expectInvariant . isCompletedMergingRun

supplyCreditsMergingRun :: Credit -> MergingRun t s -> ST s Credit
supplyCreditsMergingRun =
    checked remainingDebtMergingRun $ \credits (MergingRun _ mergeDebt ref) -> do
    mrs <- readSTRef ref
    case mrs of
      CompletedMerge{} -> pure credits
      OngoingMerge mergeCredit rs r ->
        case paydownMergeDebt mergeDebt mergeCredit credits of
          MergeDebtDischarged _ leftover -> do
            writeSTRef ref (CompletedMerge r)
            pure leftover

          MergeDebtPaydownCredited mergeCredit' -> do
            writeSTRef ref (OngoingMerge mergeCredit' rs r)
            pure 0

          MergeDebtPaydownPerform _mergeSteps mergeCredit' -> do
            -- we're not doing any actual merging
            -- just tracking what we would do
            writeSTRef ref (OngoingMerge mergeCredit' rs r)
            pure 0

suppliedCreditMergingRun :: MergingRun t s -> ST s Credit
suppliedCreditMergingRun (MergingRun _ d ref) =
    readSTRef ref >>= \case
      CompletedMerge{} ->
        let MergeDebt { totalDebt } = d in
        pure totalDebt
      OngoingMerge MergeCredit {spentCredits, unspentCredits} _ _ ->
        pure (spentCredits + unspentCredits)

-------------------------------------------------------------------------------
-- LSM handle
--

new :: ST s (LSM s)
new = newWith conf
  where
    -- 4 was the default for both the max write buffer size and size ratio
    -- before they were made configurable
    conf = LSMConfig {
        configMaxWriteBufferSize = 4
      , configSizeRatio = 4
      }

newWith :: LSMConfig -> ST s (LSM s)
newWith conf
  | configMaxWriteBufferSize conf <= 0 =
      error "newWith: configMaxWriteBufferSize should be positive"
  | configSizeRatio conf <= 1 =
      error "newWith: configSizeRatio should be larger than 1"
  | otherwise = do
      c   <- newSTRef 0
      lsm <- newSTRef (LSMContent Map.empty [] NoUnion)
      pure (LSMHandle c conf lsm)

inserts :: Tracer (ST s) Event -> LSM s -> [(Key, Value, Maybe Blob)] -> ST s ()
inserts tr lsm kvbs = updates tr lsm [ (k, Insert v b) | (k, v, b) <- kvbs ]

insert :: Tracer (ST s) Event -> LSM s -> Key -> Value -> Maybe Blob -> ST s ()
insert tr lsm k v b = update tr lsm k (Insert v b)

deletes :: Tracer (ST s) Event -> LSM s -> [Key] ->  ST s ()
deletes tr lsm ks = updates tr lsm [ (k, Delete) | k <- ks ]

delete :: Tracer (ST s) Event -> LSM s -> Key ->  ST s ()
delete tr lsm k = update tr lsm k Delete

mupserts :: Tracer (ST s) Event -> LSM s -> [(Key, Value)] -> ST s ()
mupserts tr lsm kvbs = updates tr lsm [ (k, Mupsert v) | (k, v) <- kvbs ]

mupsert :: Tracer (ST s) Event -> LSM s -> Key -> Value -> ST s ()
mupsert tr lsm k v = update tr lsm k (Mupsert v)

data Update v b =
    Insert !v !(Maybe b)
  | Mupsert !v
  | Delete
  deriving stock (Eq, Show)

updates :: Tracer (ST s) Event -> LSM s -> [(Key, Op)] -> ST s ()
updates tr lsm = mapM_ (uncurry (update tr lsm))

update :: Tracer (ST s) Event -> LSM s -> Key -> Op -> ST s ()
update tr (LSMHandle scr conf lsmr) k op = do
    sc <- readSTRef scr
    content@(LSMContent wb ls unionLevel) <- readSTRef lsmr
    modifySTRef' scr (+1)
    supplyCreditsLevels (NominalCredit 1) ls
    invariant conf content
    let wb' = Map.insertWith combine k op wb
    if bufferSize wb' >= maxWriteBufferSize conf
      then do
        ls' <- increment tr sc conf (bufferToRun wb') ls unionLevel
        let content' = LSMContent Map.empty ls' unionLevel
        invariant conf content'
        writeSTRef lsmr content'
      else
        writeSTRef lsmr (LSMContent wb' ls unionLevel)

supplyMergeCredits :: LSM s -> NominalCredit -> ST s ()
supplyMergeCredits (LSMHandle scr conf lsmr) credits = do
    content@(LSMContent _ ls _) <- readSTRef lsmr
    modifySTRef' scr (+1)
    supplyCreditsLevels credits ls
    invariant conf content

data LookupResult v b =
    NotFound
  | Found !v !(Maybe b)
  deriving stock (Eq, Show)

lookups :: LSM s -> [Key] -> ST s [LookupResult Value Blob]
lookups (LSMHandle _ _conf lsmr) ks = do
    LSMContent wb ls ul <- readSTRef lsmr
    runs <- concat <$> flattenLevels ls
    traverse (doLookup wb runs ul) ks

lookup :: LSM s -> Key -> ST s (LookupResult Value Blob)
lookup (LSMHandle _ _conf lsmr) k = do
    LSMContent wb ls ul <- readSTRef lsmr
    runs <- concat <$> flattenLevels ls
    doLookup wb runs ul k

duplicate :: LSM s -> ST s (LSM s)
duplicate (LSMHandle _scr conf lsmr) = do
    scr'  <- newSTRef 0
    lsmr' <- newSTRef =<< readSTRef lsmr
    pure (LSMHandle scr' conf lsmr')
    -- it's that simple here, because we share all the pure value and all the
    -- STRefs and there's no ref counting to be done

-- | Similar to @Data.Map.unionWith@.
--
-- A call to 'union' itself is not expensive, as the input tables are not
-- immediately merged. Instead, it creates a representation of an in-progress
-- merge that can be performed incrementally (somewhat similar to a thunk).
--
-- The more merge work remains, the more expensive are lookups on the table.
unions :: [LSM s] -> ST s (LSM s)
unions lsms = do
    (confs, trees) <- fmap unzip $ forM lsms $ \(LSMHandle _ conf lsmr) ->
      (conf,) <$> (contentToMergingTree =<< readSTRef lsmr)
    -- Check that the configurations are equal
    conf <- case confs of
      []       -> error "unions: 0 tables"
      conf : _ -> assert (all (conf==) confs) $ pure conf
    -- TODO: if only one table is non-empty, we don't have to create a Union,
    -- we can just duplicate the table.
    unionLevel <- newPendingUnionMerge (catMaybes trees) >>= \case
      Nothing -> pure NoUnion
      Just tree -> do
        debt <- fst <$> remainingDebtMergingTree tree
        Union tree <$> newSTRef debt
    lsmr <- newSTRef (LSMContent Map.empty [] unionLevel)
    c    <- newSTRef 0
    pure (LSMHandle c conf lsmr)

-- | The /current/ upper bound on the number of 'UnionCredits' that have to be
-- supplied before a 'union' is completed.
--
-- The union debt is the number of merging steps that need to be performed /at
-- most/ until the delayed work of performing a 'union' is completed. This
-- includes the cost of completing merges that were part of the union's input
-- tables.
newtype UnionDebt = UnionDebt Debt
  deriving stock (Show, Eq, Ord)
  deriving newtype Num

-- | Return the current union debt. This debt can be reduced until it is paid
-- off using 'supplyUnionCredits'.
remainingUnionDebt :: LSM s -> ST s UnionDebt
remainingUnionDebt (LSMHandle _ _conf lsmr) = do
    LSMContent _ _ ul <- readSTRef lsmr
    UnionDebt <$> case ul of
      NoUnion      -> pure 0
      Union tree d -> checkedUnionDebt tree d

-- | Credits are used to pay off 'UnionDebt', completing a 'union' in the
-- process.
--
-- A union credit corresponds to a single merging step being performed.
newtype UnionCredits = UnionCredits Credit
  deriving stock (Show, Eq, Ord)
  deriving newtype Num

-- | Supply union credits to reduce union debt.
--
-- Supplying union credits leads to union merging work being performed in
-- batches. This reduces the union debt returned by 'remainingUnionDebt'. Union
-- debt will be reduced by /at least/ the number of supplied union credits. It
-- is therefore advisable to query 'remainingUnionDebt' every once in a while to
-- see what the current debt is.
--
-- This function returns any surplus of union credits as /leftover/ credits when
-- a union has finished. In particular, if the returned number of credits is
-- non-negative, then the union is finished.
supplyUnionCredits :: LSM s -> UnionCredits -> ST s UnionCredits
supplyUnionCredits (LSMHandle scr conf lsmr) (UnionCredits credits)
  | credits <= 0 = pure (UnionCredits 0)
  | otherwise = do
    content@(LSMContent _ _ ul) <- readSTRef lsmr
    UnionCredits <$> case ul of
      NoUnion ->
        pure credits
      Union tree debtRef -> do
        modifySTRef' scr (+1)
        _debt <- checkedUnionDebt tree debtRef  -- just to make sure it's checked
        c' <- supplyCreditsMergingTree credits tree
        debt' <- checkedUnionDebt tree debtRef
        when (debt' > 0) $
          assertST $ c' == 0  -- should have spent these credits
        invariant conf content
        pure c'

-- TODO: At some point the completed merging tree should to moved into the
-- regular levels, so it can be merged with other runs and last level merges can
-- happen again to drop deletes. Also, lookups then don't need to handle the
-- merging tree any more. There are two possible strategies:
--
-- 1. As soon as the merging tree completes, move the resulting run to the
--    regular levels. However, its size does generally not fit the last level,
--    which requires relaxing 'invariant' and adjusting 'increment'.
--
--    If the run is much larger than the resident and incoming runs of the last
--    level, it should also not be included into a merge yet, as that merge
--    would be expensive, but offer very little potential for compaction (the
--    run from the merging tree is already compacted after all). So it needs to
--    be bumped to the next level instead.
--
-- 2. Initially leave the completed run in the union level. Then every time a
--    new last level merge is created in 'increment', check if there is a
--    completed run in the union level with a size that fits the new merge. If
--    yes, move it over.

-- | Like 'remainingDebtMergingTree', but additionally asserts that the debt
-- never increases.
checkedUnionDebt :: MergingTree s -> STRef s Debt -> ST s Debt
checkedUnionDebt tree debtRef = do
    storedDebt <- readSTRef debtRef
    debt <- fst <$> remainingDebtMergingTree tree
    assertST $ debt <= storedDebt
    writeSTRef debtRef debt
    pure debt

-------------------------------------------------------------------------------
-- Lookups
--

type LookupAcc = Maybe Op

updateAcc :: (Op -> Op -> Op) -> LookupAcc -> Op -> LookupAcc
updateAcc _ Nothing     old = Just old
updateAcc f (Just new_) old = Just (f new_ old)  -- acc has more recent Op

mergeAcc :: TreeMergeType -> [LookupAcc] -> LookupAcc
mergeAcc mt = foldl (updateAcc com) Nothing . catMaybes
  where
    com = case mt of
      MergeLevel -> combine
      MergeUnion -> combineUnion

-- | We handle lookups by accumulating results by going through the runs from
-- most recent to least recent, starting with the write buffer.
--
-- In the real implementation, this is done not on an individual 'LookupAcc',
-- but one for each key, i.e. @Vector (Maybe Entry)@.
doLookup :: Buffer -> [Run] -> UnionLevel s -> Key -> ST s (LookupResult Value Blob)
doLookup wb runs ul k = do
    let acc0 = lookupBatch (Map.lookup k wb) k runs
    case ul of
      NoUnion ->
        pure (convertAcc acc0)
      Union tree _ -> do
        treeBatches <- buildLookupTree tree
        let treeResults = lookupBatch Nothing k <$> treeBatches
        pure $ convertAcc $ foldLookupTree $
          if null wb && null runs
          then treeResults
          else LookupNode MergeLevel [LookupBatch acc0, treeResults ]
  where
    convertAcc :: LookupAcc -> LookupResult Value Blob
    convertAcc = \case
        Nothing           -> NotFound
        Just (Insert v b) -> Found v b
        Just (Mupsert v)  -> Found v Nothing
        Just Delete       -> NotFound

-- | Perform a batch of lookups, accumulating the result onto an initial
-- 'LookupAcc'.
--
-- In a real implementation, this would take all keys at once and be in IO.
lookupBatch :: LookupAcc -> Key -> [Run] -> LookupAcc
lookupBatch acc k rs =
    let ops = [op | r <- rs, Just op <- [Map.lookup k r]]
    in foldl (updateAcc combine) acc ops

data LookupTree a = LookupBatch a
                  | LookupNode TreeMergeType [LookupTree a]
  deriving stock Functor

-- | Do lookups on runs at the leaves and recursively combine the resulting
-- 'LookupAcc's, either using 'mergeAcc' or 'unionAcc' depending on the merge
-- type.
--
-- Doing this naively would result in a call to 'lookupBatch' and creation of
-- a 'LookupAcc' for each run in the tree. However, when there are adjacent
-- 'Run's or 'MergingRuns' (with 'MergeLevel') as inputs to a level-merge, we
-- combine them into a single batch of runs.
--
-- For example, this means that if we union two tables (which themselves don't
-- have a union level) and then do lookups, two batches of lookups have to be
-- performed (plus a batch for the table's regular levels if it has been updated
-- after the union).
--
-- TODO: we can still improve the batching, for example combining the child of
-- PendingLevelMerge with the pre-existing runs when it is already completed.
buildLookupTree :: MergingTree s -> ST s (LookupTree [Run])
buildLookupTree = go
  where
    go :: MergingTree s -> ST s (LookupTree [Run])
    go (MergingTree treeState) = readSTRef treeState >>= \case
        CompletedTreeMerge r ->
          pure $ LookupBatch [r]
        OngoingTreeMerge (MergingRun mt _ mergeState) ->
          readSTRef mergeState >>= \case
            CompletedMerge r ->
              pure $ LookupBatch [r]
            OngoingMerge _ rs _ -> case mt of
              MergeLevel -> pure $ LookupBatch rs  -- combine into batch
              MergeUnion -> pure $ LookupNode MergeUnion $ map (\r -> LookupBatch [r]) rs
        PendingTreeMerge (PendingLevelMerge prs tree) -> do
          preExisting <- LookupBatch . concat <$>
            traverse flattenPreExistingRun prs -- combine into batch
          case tree of
            Nothing -> pure preExisting
            Just t  -> do
              lTree <- go t
              pure (LookupNode MergeLevel [preExisting, lTree])
        PendingTreeMerge (PendingUnionMerge trees) -> do
          LookupNode MergeUnion <$> traverse go trees

foldLookupTree :: LookupTree LookupAcc -> LookupAcc
foldLookupTree = \case
    LookupBatch acc        -> acc
    LookupNode mt children -> mergeAcc mt (map foldLookupTree children)

-------------------------------------------------------------------------------
-- Nominal credits
--

-- | Nominal credit is the credit supplied to each level as we insert update
-- operations, one credit per update operation inserted.
--
-- Nominal credit must be supplied up to the 'NominalDebt' to ensure the merge
-- is complete.
--
-- Nominal credits are a similar order of magnitude to physical credits (see
-- 'Credit') but not the same, and we have to scale linearly to convert between
-- them. Physical credits are the actual number of inputs to the merge, which
-- may be somewhat more or somewhat less than the number of update operations
-- we will insert before we need the merge to be complete.
--
newtype NominalCredit = NominalCredit Credit
  deriving stock Show

-- | The nominal debt for a merging run is the worst case (minimum) number of
-- update operations we expect to insert before we expect the merge to be
-- complete.
--
-- We require that an equal amount of nominal credit is supplied before we can
-- expect a merge to be complete.
--
-- We scale linearly to convert nominal credits to physical credits, such that
-- the nominal debt and physical debt are both considered \"100%\", and so that
-- both debts are paid off at exactly the same time.
--
newtype NominalDebt = NominalDebt Credit
  deriving stock Show

-- TODO: If there is a UnionLevel, there is no (more expensive) last level merge
-- in the regular levels, so a little less merging work is required than if
-- there was no UnionLevel. It might be a good idea to spend this "saved" work
-- on the UnionLevel instead. This makes future lookups cheaper and ensures that
-- we can get rid of the UnionLevel at some point, even if a user just keeps
-- inserting without calling 'supplyUnionCredits'.
supplyCreditsLevels :: NominalCredit -> Levels s -> ST s ()
supplyCreditsLevels nominalDeposit =
  traverse_ $ \(Level ir _rs) -> do
    case ir of
      Single{} -> pure ()
      Merging _mp nominalDebt nominalCreditVar
              mr@(MergingRun _  physicalDebt _) -> do

        nominalCredit       <- depositNominalCredit
                                 nominalDebt nominalCreditVar nominalDeposit
        physicalCredit      <- suppliedCreditMergingRun mr
        let !physicalCredit' = scaleNominalToPhysicalCredit
                                 nominalDebt physicalDebt nominalCredit
            -- Our target physicalCredit' could actually be less than the
            -- actual current physicalCredit if other tables were contributing
            -- credits to the shared merge.
            !physicalDeposit = physicalCredit' - physicalCredit

        -- So we may have a zero or negative deposit, which we ignore.
        when (physicalDeposit > 0) $ do
          leftoverCredits <- supplyCreditsMergingRun physicalDeposit mr
          -- For merges at ordinary levels (not unions) we expect to hit the
          -- debt limit exactly and not exceed it. However if we had a race
          -- on supplying credit then we could go over (which is not a problem).
          -- We can detect such races if the credit afterwards is not the amount
          -- that we credited. This is all just for sanity checking.
          physicalCredit'' <- suppliedCreditMergingRun mr
          assert (leftoverCredits == 0 || physicalCredit' /= physicalCredit'')
                 (pure ())

        -- There is a potential race here in between deciding how much physical
        -- credit to supply, and then supplying it. That's because we read the
        -- "current" (absolute) physical credits, decide how much extra
        -- (relative) credits to supply and then do the transaction to supply
        -- the extra (relative) credits. In between the reading and supplying
        -- the current (absolute) physical credits could have changed due to
        -- another thread doing a merge on a different table handle.
        --
        -- This race is relatively benign. When it happens, we will supply more
        -- credit to the merge than either thread intended, however, next time
        -- either thread comes round they'll find the merge has more physical
        -- credits and will thus supply less or none. The only minor problem is
        -- in asserting that we don't supply more physical credits than the
        -- debt limit.

        -- There is a trade-off, we could supply absolute physical credit to
        -- the merging run, and let it calculate the relative credit as part
        -- of the credit transaction. However, we would also need to support
        -- relative credit for the union merges, which do not have any notion
        -- of nominal credit and only work in terms of relative physical credit.
        -- So we can have a simple relative physical credit and rare benign
        -- races, or a more complex scheme for contributing physical credits
        -- either as absolute or relative values.

scaleNominalToPhysicalCredit ::
     NominalDebt
  -> MergeDebt
  -> NominalCredit
  -> Credit
scaleNominalToPhysicalCredit (NominalDebt nominalDebt)
                             MergeDebt { totalDebt = physicalDebt }
                             (NominalCredit nominalCredit) =
    floor $ toRational nominalCredit * toRational physicalDebt
                                     / toRational nominalDebt
    -- This specification using Rational as an intermediate representation can
    -- be implemented efficiently using only integer operations.

depositNominalCredit ::
     NominalDebt
  -> STRef s NominalCredit
  -> NominalCredit
  -> ST s NominalCredit
depositNominalCredit (NominalDebt nominalDebt)
                     nominalCreditVar
                     (NominalCredit deposit) = do
    NominalCredit before <- readSTRef nominalCreditVar
    -- Depositing _could_ leave the credit higher than the debt, because
    -- sometimes under-full runs mean we don't shuffle runs down the levels
    -- as quickly as the worst case. So here we do just drop excess nominal
    -- credits.
    let !after = NominalCredit (min (before + deposit) nominalDebt)
    writeSTRef nominalCreditVar after
    pure after

-------------------------------------------------------------------------------
-- Updates
--

increment :: forall s. Tracer (ST s) Event
          -> Counter
          -> LSMConfig
          -> Run -> Levels s -> UnionLevel s -> ST s (Levels s)
increment tr sc conf run0 ls0 ul = do
    go 1 [run0] ls0
  where
    mergeTypeFor :: Levels s -> LevelMergeType
    mergeTypeFor ls = mergeTypeForLevel ls ul

    go :: Int -> [Run] -> Levels s -> ST s (Levels s)
    go !ln incoming [] = do
        let mergePolicy = mergePolicyForLevel ln [] ul
        traceWith tr' AddLevelEvent
        ir <- newLevelMerge tr' conf ln mergePolicy (mergeTypeFor []) incoming
        pure (Level ir [] : [])
      where
        tr' = contramap (EventAt sc ln) tr

    go !ln incoming (Level ir rs : ls) = do
      r <- case ir of
        Single r -> pure r
        Merging mergePolicy _ _ mr -> do
          r <- expectCompletedMergingRun mr
          traceWith tr' MergeCompletedEvent {
              mergePolicy,
              mergeType = let MergingRun mt _ _ = mr in mt,
              mergeSize = runSize r
            }
          pure r

      let resident = r:rs
      case mergePolicyForLevel ln ls ul of

        -- If r is still too small for this level then keep it and merge again
        -- with the incoming runs.
        MergePolicyTiering | runTooSmallForLevel MergePolicyTiering conf ln r -> do
          ir' <- newLevelMerge tr' conf ln MergePolicyTiering (mergeTypeFor ls) (incoming ++ [r])
          pure (Level ir' rs : ls)

        -- This tiering level is now full. We take the completed merged run
        -- (the previous incoming runs), plus all the other runs on this level
        -- as a bundle and move them down to the level below. We start a merge
        -- for the new incoming runs. This level is otherwise empty.
        MergePolicyTiering | levelIsFullTiering conf ln incoming resident  -> do
          ir' <- newLevelMerge tr' conf ln MergePolicyTiering MergeMidLevel incoming
          ls' <- go (ln+1) resident ls
          pure (Level ir' [] : ls')

        -- This tiering level is not yet full. We move the completed merged run
        -- into the level proper, and start the new merge for the incoming runs.
        MergePolicyTiering -> do
          ir' <- newLevelMerge tr' conf ln MergePolicyTiering (mergeTypeFor ls) incoming
          traceWith tr' (AddRunEvent (length resident))
          pure (Level ir' resident : ls)

        -- The final level is using levelling. If the existing completed merge
        -- run is too large for this level, we promote the run to the next
        -- level and start merging the incoming runs into this (otherwise
        -- empty) level .
        MergePolicyLevelling | levelIsFullLevelling conf ln incoming r -> do
          assert (null rs && null ls) $ pure ()
          ir' <- newLevelMerge tr' conf ln MergePolicyTiering MergeMidLevel incoming
          ls' <- go (ln+1) [r] []
          pure (Level ir' [] : ls')

        -- Otherwise we start merging the incoming runs into the run.
        MergePolicyLevelling -> do
          assert (null rs && null ls) $ pure ()
          ir' <- newLevelMerge tr' conf ln MergePolicyLevelling (mergeTypeFor ls)
                          (incoming ++ [r])
          pure (Level ir' [] : [])

      where
        tr' = contramap (EventAt sc ln) tr

newLevelMerge :: Tracer (ST s) EventDetail
              -> LSMConfig
              -> Int -> MergePolicy -> LevelMergeType
              -> [Run] -> ST s (IncomingRun s)
newLevelMerge _ _ _ _ _ [r] = pure (Single r)
newLevelMerge tr conf@LSMConfig{..} level mergePolicy mergeType rs = do
    assertST (length rs `elem` [configSizeRatio, configSizeRatio + 1])
    mergingRun@(MergingRun _ physicalDebt _) <- newMergingRun mergeType rs
    assertWithMsgM $ leq (totalDebt physicalDebt) maxPhysicalDebt
    traceWith tr MergeStartedEvent {
                   mergePolicy,
                   mergeType,
                   mergeDebt     = totalDebt physicalDebt,
                   mergeRunsSize = map runSize rs
                 }
    nominalCreditVar <- newSTRef (NominalCredit 0)
    pure (Merging mergePolicy nominalDebt nominalCreditVar mergingRun)
  where
    -- The nominal debt equals the minimum of credits we will supply before we
    -- expect the merge to complete. This is the same as the number of updates
    -- in a run that gets moved to this level.
    nominalDebt = NominalDebt (levelNumberToMaxRunSize MergePolicyTiering conf level)

    -- The physical debt is the number of actual merge steps we will need to
    -- perform before the merge is complete. This is always the sum of the
    -- lengths of the input runs.
    --
    -- As we supply nominal credit, we scale them and supply physical credits,
    -- such that we pay off the physical and nominal debts at the same time.
    --
    -- We can bound the worst case physical debt: this is the maximum amount of
    -- steps a merge at this level could need. See the
    -- 'expectedMergingRunLengths' where-clause of the 'invariant' function for
    -- the full reasoning.
    maxPhysicalDebt =
      case mergePolicy of
        MergePolicyLevelling ->
          -- Incoming runs, which may be slightly overfull with respect to the
          -- previous level
          configSizeRatio * levelNumberToMaxRunSize MergePolicyTiering conf level
              -- The single run that was already on this level
            + levelNumberToMaxRunSize MergePolicyLevelling conf level
        MergePolicyTiering   ->
          -- Incoming runs, which may be slightly overfull with respect to the
          -- previous level
          configSizeRatio * levelNumberToMaxRunSize MergePolicyTiering conf level
              -- Held back run that is underfull with respect to the current
              -- level
            + levelNumberToMaxRunSize MergePolicyTiering conf (level - 1)

-------------------------------------------------------------------------------
-- MergingTree abstraction
--

-- Note [Table Unions]
-- ~~~~~~~~~~~~~~~~~~~
--
-- Semantically, tables are key-value stores like Haskell's @Map@. Table unions
-- then behave like @Map.unionWith (<>)@. If one of the input tables contains
-- a value at a particular key, the result will also contain it. If multiple
-- tables share that key, the values will be combined monoidally (using
-- 'resolveValue' in in this prototype).
--
-- Looking at the implementation, tables are not just key-value pairs, but
-- consist of runs. If each table was just a single run, unioning would involve
-- a run merge similar to the one used for compaction (when a level is full),
-- but with a different merge type 'MergeUnion' that differs semantically: Here,
-- runs don't represent updates (overwriting each other), but they each
-- represent the full state of a table. There is no distinction between no entry
-- and a 'Delete', between an 'Insert' and a 'Mupsert'.
--
-- To union two tables, we can therefore first merge down each table into a
-- single run (using regular level merges) and then union merge these.
--
-- However, we want to spread out the work required and perform these merges
-- incrementally. At first, we only create a new table that is empty except for
-- a data structure 'MergingTree', representing the merges that need to be done.
-- The usual operations can then be performed on the table while the merge is
-- in progress: Inserts go into the table as usual, not affecting its last level
-- ('UnionLevel'), lookups need to consider the tree (requiring some complexity
-- and runtime overhead), further unions incorporate the in-progress tree into
-- the resulting one, which also shares future merging work.
--
-- It seems necessary to represent the suspended merges using a tree. Other
-- approaches don't allow for full sharing of the incremental work (e.g. because
-- they effectively \"re-bracket\" nested unions). It also seems necessary to
-- first merge each input table into a single run, as there is no practical
-- distributive property between level and union merges.

-- | Ensures that the merge contains more than one input, avoiding creating a
-- pending merge where possible.
newPendingLevelMerge :: [IncomingRun s]
                     -> Maybe (MergingTree s)
                     -> ST s (Maybe (MergingTree s))
newPendingLevelMerge [] t = pure t
newPendingLevelMerge [Single r] Nothing =
    Just . MergingTree <$> newSTRef (CompletedTreeMerge r)
newPendingLevelMerge [Merging{}] Nothing =
    -- This case should never occur. If there is a single entry in the list,
    -- there can only be one level in the input table. At level 1 there are no
    -- merging runs, so it must be a PreExistingRun.
    error "newPendingLevelMerge: singleton Merging run"
newPendingLevelMerge irs tree = do
    let prs = map incomingToPreExistingRun irs
        st  = PendingTreeMerge (PendingLevelMerge prs tree)
    Just . MergingTree <$> newSTRef st
  where
    incomingToPreExistingRun (Single         r) = PreExistingRun r
    incomingToPreExistingRun (Merging _ _ _ mr) = PreExistingMergingRun mr

-- | Ensures that the merge contains more than one input.
newPendingUnionMerge :: [MergingTree s] -> ST s (Maybe (MergingTree s))
newPendingUnionMerge []  = pure Nothing
newPendingUnionMerge [t] = pure (Just t)
newPendingUnionMerge trees = do
    let st = PendingTreeMerge (PendingUnionMerge trees)
    Just . MergingTree <$> newSTRef st

contentToMergingTree :: LSMContent s -> ST s (Maybe (MergingTree s))
contentToMergingTree (LSMContent wb ls ul) =
    newPendingLevelMerge (buffers ++ levels) trees
  where
    -- flush the write buffer (but this should not modify the content)
    buffers
      | bufferSize wb == 0 = []
      | otherwise          = [Single (bufferToRun wb)]

    levels = flip concatMap ls $ \(Level ir rs) -> ir : map Single rs

    trees = case ul of
        NoUnion   -> Nothing
        Union t _ -> Just t

-- | When calculating (an upped bound of) the total debt of a recursive tree of
-- merges, we also need to return an upper bound on the size of the resulting
-- run. See 'remainingDebtPendingMerge'.
type Size = Int

remainingDebtMergingTree :: MergingTree s -> ST s (Debt, Size)
remainingDebtMergingTree (MergingTree ref) =
    readSTRef ref >>= \case
      CompletedTreeMerge r -> pure (0, runSize r)
      OngoingTreeMerge mr  -> addDebtOne <$> remainingDebtMergingRun mr
      PendingTreeMerge pm  -> addDebtOne <$> remainingDebtPendingMerge pm
  where
    -- An ongoing merge should never have 0 debt, even if the 'MergingRun' in it
    -- says it is completed. We still need to update it to 'CompletedTreeMerge'.
    -- Similarly, a pending merge needs some work to complete it, even if all
    -- its inputs are empty.
    --
    -- Note that we can't use @max 1@, as this would violate the property that
    -- supplying N credits reduces the remaining debt by at least N.
    addDebtOne (debt, size) = (debt + 1, size)

remainingDebtPendingMerge :: PendingMerge s -> ST s (Debt, Size)
remainingDebtPendingMerge (PendingMerge _ prs trees) = do
    (debts, sizes) <- unzip . concat <$> sequence
        [ traverse remainingDebtPreExistingRun prs
        , traverse remainingDebtMergingTree trees
        ]
    let totalSize = sum sizes
    let totalDebt = sum debts + totalSize
    pure (totalDebt, totalSize)
  where
    remainingDebtPreExistingRun = \case
        PreExistingRun         r -> pure (0, runSize r)
        PreExistingMergingRun mr -> remainingDebtMergingRun mr

remainingDebtMergingRun :: MergingRun t s -> ST s (Debt, Size)
remainingDebtMergingRun (MergingRun _ d ref) =
    readSTRef ref >>= \case
      CompletedMerge r ->
        pure (0, runSize r)
      OngoingMerge c inputRuns _ ->
        pure (mergeDebtLeft d c, sum (map runSize inputRuns))

-- | For each of the @supplyCredits@ type functions, we want to check some
-- common properties.
checked :: HasCallStack
        => (a -> ST s (Debt, Size))  -- ^ how to calculate the current debt
        -> (Credit -> a -> ST s Credit)  -- ^ how to supply the credits
        -> Credit -> a -> ST s Credit
checked query supply credits x = do
    assertST $ credits > 0   -- only call them when there are credits to spend
    debt <- fst <$> query x
    assertST $ debt >= 0     -- debt can't be negative
    c' <- supply credits x
    assertST $ c' <= credits -- can't have more leftovers than we started with
    assertST $ c' >= 0       -- leftovers can't be negative
    debt' <- fst <$> query x
    assertST $ debt' >= 0
    -- the debt was reduced sufficiently (amount of credits spent)
    assertST $ debt' <= debt - (credits - c')
    pure c'

supplyCreditsMergingTree :: Credit -> MergingTree s -> ST s Credit
supplyCreditsMergingTree = checked remainingDebtMergingTree $ \credits (MergingTree ref) -> do
    treeState <- readSTRef ref
    (!c', !treeState') <- supplyCreditsMergingTreeState credits treeState
    writeSTRef ref treeState'
    pure c'

supplyCreditsMergingTreeState :: Credit -> MergingTreeState s
                              -> ST s (Credit, MergingTreeState s)
supplyCreditsMergingTreeState credits !state = do
    assertST (credits >= 0)
    case state of
      CompletedTreeMerge{} ->
        pure (credits, state)
      OngoingTreeMerge mr -> do
        c' <- supplyCreditsMergingRun credits mr
        if c' <= 0
          then pure (0, state)
          else do
            r <- expectCompletedMergingRun mr
            -- all work is done, we can't spend any more credits
            pure (c', CompletedTreeMerge r)
      PendingTreeMerge pm -> do
        c' <- supplyCreditsPendingMerge credits pm
        if c' <= 0
          then
            -- still remaining work in children, we can't do more for now
            pure (c', state)
          else do
            -- all children must be done, create new merge!
            (mergeType, rs) <- expectCompletedChildren pm
            case rs of
              [r] -> pure (c', CompletedTreeMerge r)
              _   -> do
                state' <- OngoingTreeMerge <$> newMergingRun mergeType rs
                -- use any remaining credits to progress the new merge
                supplyCreditsMergingTreeState c' state'

supplyCreditsPendingMerge :: Credit -> PendingMerge s -> ST s Credit
supplyCreditsPendingMerge = checked remainingDebtPendingMerge $ \credits -> \case
    PendingLevelMerge prs tree ->
      leftToRight supplyPreExistingRun prs credits
        >>= leftToRight supplyCreditsMergingTree (toList tree)
    PendingUnionMerge trees ->
      splitEqually supplyCreditsMergingTree trees credits
  where
    supplyPreExistingRun c = \case
        PreExistingRun        _r -> pure c
        PreExistingMergingRun mr -> supplyCreditsMergingRun c mr

    -- supply credits left to right until they are used up
    leftToRight :: (Credit -> a -> ST s Credit) -> [a] -> Credit -> ST s Credit
    leftToRight _ _      0 = pure 0
    leftToRight _ []     c = pure c
    leftToRight f (x:xs) c = f c x >>= leftToRight f xs

    -- approximately equal, being more precise would require more iterations
    splitEqually :: (Credit -> a -> ST s Credit) -> [a] -> Credit -> ST s Credit
    splitEqually f xs credits =
        -- first give each tree k = ceil(1/n) credits (last ones might get less).
        -- it's important we fold here to collect leftovers.
        -- any remainders go left to right.
        foldM supply credits xs >>= leftToRight f xs
      where
        !n = length xs
        !k = (credits + (n - 1)) `div` n

        supply 0 _ = pure 0
        supply c t = do
            let creditsToSpend = min k c
            leftovers <- f creditsToSpend t
            pure (c - creditsToSpend + leftovers)

expectCompletedChildren :: HasCallStack
                        => PendingMerge s -> ST s (TreeMergeType, [Run])
expectCompletedChildren (PendingMerge mt prs trees) = do
    rs1 <- traverse expectCompletedPreExistingRun prs
    rs2 <- traverse expectCompletedMergingTree trees
    pure (mt, rs1 ++ rs2)
  where
    expectCompletedPreExistingRun = \case
        PreExistingRun         r -> pure r
        PreExistingMergingRun mr -> expectCompletedMergingRun mr

expectCompletedMergingTree :: HasCallStack => MergingTree s -> ST s Run
expectCompletedMergingTree = expectInvariant . isCompletedMergingTree

-------------------------------------------------------------------------------
-- Measurements
--

data MTree r = MLeaf r
             | MNode TreeMergeType [MTree r]
  deriving stock (Eq, Foldable, Functor, Show)

allLevels :: LSM s -> ST s (Buffer, [[Run]], Maybe (MTree Run))
allLevels (LSMHandle _ _conf lsmr) = do
    LSMContent wb ls ul <- readSTRef lsmr
    rs <- flattenLevels ls
    tree <- case ul of
      NoUnion   -> pure Nothing
      Union t _ -> Just <$> flattenTree t
    pure (wb, rs, tree)

flattenLevels :: Levels s -> ST s [[Run]]
flattenLevels = mapM flattenLevel

flattenLevel :: Level s -> ST s [Run]
flattenLevel (Level ir rs) = (++ rs) <$> flattenIncomingRun ir

flattenIncomingRun :: IncomingRun s -> ST s [Run]
flattenIncomingRun = \case
    Single r         -> pure [r]
    Merging _ _ _ mr -> flattenMergingRun mr

flattenMergingRun :: MergingRun t s -> ST s [Run]
flattenMergingRun (MergingRun _ _ ref) = do
    mrs <- readSTRef ref
    case mrs of
      CompletedMerge r    -> pure [r]
      OngoingMerge _ rs _ -> pure rs

flattenTree :: MergingTree s -> ST s (MTree Run)
flattenTree (MergingTree ref) = do
    mts <- readSTRef ref
    case mts of
      CompletedTreeMerge r ->
        pure (MLeaf r)
      OngoingTreeMerge (MergingRun mt _ mrs) ->
        readSTRef mrs >>= \case
          CompletedMerge r    -> pure (MLeaf r)
          OngoingMerge _ rs _ -> pure (MNode mt (MLeaf <$> rs))
      PendingTreeMerge (PendingMerge mt irs trees) -> do
        irs' <- map MLeaf . concat <$> traverse flattenPreExistingRun irs
        trees' <- traverse flattenTree trees
        pure (MNode mt (irs' ++ trees'))

flattenPreExistingRun :: PreExistingRun s -> ST s [Run]
flattenPreExistingRun = \case
    PreExistingRun         r -> pure [r]
    PreExistingMergingRun mr -> flattenMergingRun mr

logicalValue :: LSM s -> ST s (Map Key (Value, Maybe Blob))
logicalValue lsm = do
    (wb, levels, tree) <- allLevels lsm
    let r = mergek
              MergeLevel
              (wb : concat levels ++ toList (mergeTree <$> tree))
    pure (Map.mapMaybe justInsert r)
  where
    mergeTree :: MTree Run -> Run
    mergeTree (MLeaf r)     = r
    mergeTree (MNode mt ts) = mergek mt (map mergeTree ts)

    justInsert (Insert v b) = Just (v, b)
    justInsert  Delete      = Nothing
    justInsert (Mupsert v)  = Just (v, Nothing)

type Representation = (Run, [LevelRepresentation], Maybe (MTree Run))

type LevelRepresentation =
    (Maybe (MergePolicy, NominalDebt, NominalCredit,
            LevelMergeType, MergingRunState),
     [Run])

dumpRepresentation :: LSM s -> ST s Representation
dumpRepresentation (LSMHandle _ _conf lsmr) = do
    LSMContent wb ls ul <- readSTRef lsmr
    levels <- mapM dumpLevel ls
    tree <- case ul of
      NoUnion   -> pure Nothing
      Union t _ -> Just <$> flattenTree t
    pure (wb, levels, tree)

dumpLevel :: Level s -> ST s LevelRepresentation
dumpLevel (Level (Single r) rs) =
    pure (Nothing, (r:rs))
dumpLevel (Level (Merging mp nd ncv (MergingRun mt _ ref)) rs) = do
    mrs <- readSTRef ref
    nc  <- readSTRef ncv
    pure (Just (mp, nd, nc, mt, mrs), rs)

-- For each level:
-- 1. the runs involved in an ongoing merge
-- 2. the other runs (including completed merge)
representationShape :: Representation
                    -> (Int, [([Int], [Int])], Maybe (MTree Int))
representationShape (wb, levels, tree) =
    (summaryRun wb, map summaryLevel levels, fmap (fmap summaryRun) tree)
  where
    summaryLevel (mmr, rs) =
      let (ongoing, complete) = summaryMR mmr
      in (ongoing, complete <> map summaryRun rs)

    summaryRun = runSize

    summaryMR = \case
      Nothing                          -> ([], [])
      Just (_, _, _, _, CompletedMerge r)    -> ([], [summaryRun r])
      Just (_, _, _, _, OngoingMerge _ rs _) -> (map summaryRun rs, [])

-------------------------------------------------------------------------------
-- Tracing
--

-- TODO: these events are incomplete, in particular we should also trace what
-- happens in the union level.
type Event = EventAt EventDetail
data EventAt e = EventAt {
                   eventAtStep  :: Counter,
                   eventAtLevel :: Int,
                   eventDetail  :: e
                 }
  deriving stock Show

data EventDetail =
       AddLevelEvent
     | AddRunEvent {
         runsAtLevel   :: Int
       }
     | MergeStartedEvent {
         mergePolicy   :: MergePolicy,
         mergeType     :: LevelMergeType,
         mergeDebt     :: Debt,
         mergeRunsSize :: [Int]
       }
     | MergeCompletedEvent {
         mergePolicy :: MergePolicy,
         mergeType   :: LevelMergeType,
         mergeSize   :: Int
       }
  deriving stock Show

-------------------------------------------------------------------------------
-- Arbitrary
--

instance QC.Arbitrary Key where
  arbitrary = K <$> QC.arbitrarySizedNatural
  shrink (K v) = K <$> QC.shrink v

instance QC.Arbitrary Value where
  arbitrary = V <$> QC.arbitrarySizedNatural
  shrink (V v) = V <$> QC.shrink v

instance QC.Arbitrary Blob where
  arbitrary = B <$> QC.arbitrarySizedNatural
  shrink (B v) = B <$> QC.shrink v

instance (QC.Arbitrary v, QC.Arbitrary b) => QC.Arbitrary (Update v b) where
  arbitrary = QC.frequency
      [ (3, Insert <$> QC.arbitrary <*> QC.arbitrary)
      , (1, Mupsert <$> QC.arbitrary)
      , (1, pure Delete)
      ]

instance QC.Arbitrary LevelMergeType where
  arbitrary = QC.elements [MergeMidLevel, MergeLastLevel]

instance QC.Arbitrary TreeMergeType where
  arbitrary = QC.elements [MergeLevel, MergeUnion]
