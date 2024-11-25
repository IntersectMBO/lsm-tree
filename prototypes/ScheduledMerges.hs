{-# LANGUAGE PatternSynonyms #-}

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
-- The merging policy that this prototype uses is power 4 \"lazy levelling\".
-- Power 4 means each level is 4 times bigger than the previous level.
-- Lazy levelling means we use tiering for every level except the last level
-- which uses levelling. Though note that the first level always uses tiering,
-- even if the first level is also the last level. This is to simplify flushing
-- the write buffer: if we used levelling on the first level we would need a
-- code path for merging the write buffer into the first level.
--
module ScheduledMerges (
    -- * Main API
    LSM,
    Key (K), Value (V), resolveValue, Blob (B),
    new,
    LookupResult (..),
    lookup, lookups,
    Update (..),
    update, updates,
    insert, inserts,
    delete, deletes,
    mupsert, mupserts,
    supplyMergeCredits,
    duplicate,
    union,
    Credit,
    Debt,
    remainingUnionDebt,
    supplyUnionCredits,

    -- * Test and trace
    logicalValue,
    dumpRepresentation,
    representationShape,
    Event,
    EventAt(..),
    EventDetail(..)
  ) where

import           Prelude hiding (lookup)

import           Data.Bits
import           Data.Foldable (for_, toList, traverse_)
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import           Data.Maybe (catMaybes)
import           Data.STRef

import qualified Control.Exception as Exc (assert)
import           Control.Monad (when)
import           Control.Monad.ST
import           Control.Tracer (Tracer, contramap, traceWith)
import           GHC.Stack (HasCallStack, callStack)


data LSM s  = LSMHandle !(STRef s Counter)
                        !(STRef s (LSMContent s))

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

-- | A level is a sequence of resident runs at this level, prefixed by an
-- incoming run, which is usually multiple runs that are being merged. Once
-- completed, the resulting run will become a resident run at this level.
data Level s = Level !(IncomingRun s) ![Run]

-- | An additional optional last level, created as a result of 'union'. It can
-- not only contain an ongoing merge of multiple runs, but a nested tree of
-- merges. See Note [Table Unions].
data UnionLevel s = NoUnion
                    -- | We track the debt to make sure it never increases.
                  | Union !(MergingTree s) !(STRef s Debt)

-- | The merge policy for a LSM level can be either tiering or levelling.
-- In this design we use levelling for the last level, and tiering for
-- all other levels. The first level always uses tiering however, even if
-- it's also the last level. So 'MergePolicy' and 'MergeLastLevel' are
-- orthogonal, all combinations are possible.
--
data MergePolicy = MergePolicyTiering | MergePolicyLevelling
  deriving stock (Eq, Show)

-- | A last level merge behaves differently from a mid-level merge: last level
-- merges can actually remove delete operations, whereas mid-level merges must
-- preserve them. This is orthogonal to the 'MergePolicy'.
--
data MergeLastLevel = MergeMidLevel | MergeLastLevel
  deriving stock (Eq, Show)

-- | Merges can either exist on a level of the LSM, or be a union merge of two
-- tables. Union merges follow the semantics of @Data.Map.unionWith (<>)@. Since
-- the input runs are semantically treated like @Data.Map@s, deletes from one
-- input don't affect another (there is no distinction between a deleted value
-- and one that was never inserted) and inserts need to be merged monoidally
-- using 'resolveValue' (as if they were mupserts). See Note [Table Unions].
--
data MergeType = MergeLevel MergeLastLevel | MergeUnion
  deriving stock (Eq, Show)

-- | We represent single runs specially, rather than putting them in as a
-- 'CompletedMerge'. This is for two reasons: to see statically that it's a
-- single run without having to read the 'STRef', and secondly to make it easier
-- to avoid supplying merge credits. It's not essential, but simplifies things
-- somewhat.
data IncomingRun s = Merging !MergePolicy !(MergingRun s)
                   | Single  !Run

-- | A \"merging run\" is a mutable representation of an incremental merge.
-- It is also a unit of sharing between duplicated tables.
--
data MergingRun s = MergingRun !MergeType !(STRef s MergingRunState)

data MergingRunState = CompletedMerge !Run
                     | OngoingMerge
                         !MergeDebt
                         ![Run]  -- ^ inputs of the merge
                         Run  -- ^ output of the merge (lazily evaluated)

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
                        | OngoingTreeMerge !(MergingRun s)
                        | PendingTreeMerge !(PendingMerge s)

-- | A merge that is waiting for its inputs to complete.
--
-- The inputs can themselves be 'MergingTree's (with its STRef) to allow sharing
-- existing unions.
data PendingMerge s = -- | The inputs are entire content of a table, i.e. its
                      -- (merging) runs and finally a union merge (if that table
                      -- already contained a union).
                      PendingLevelMerge ![IncomingRun s] !(Maybe (MergingTree s))
                      -- | Each input is a level merge of the entire content of
                      -- a table.
                    | PendingUnionMerge ![MergingTree s]

pendingContent :: PendingMerge s -> (MergeType, [IncomingRun s], [MergingTree s])
pendingContent = \case
    PendingLevelMerge irs t  -> (MergeLevel MergeLastLevel, irs, toList t)
    PendingUnionMerge     ts -> (MergeUnion,                [],  ts)

{-# COMPLETE PendingMerge #-}
pattern PendingMerge :: MergeType -> [IncomingRun s] -> [MergingTree s] -> PendingMerge s
pattern PendingMerge mt irs ts <- (pendingContent -> (mt, irs, ts))

type Credit = Int
type Debt   = Int

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

resolveBlob :: Maybe Blob -> Maybe Blob -> Maybe Blob
resolveBlob (Just b) _ = Just b
resolveBlob Nothing mb = mb

newtype Blob = B Int
  deriving stock (Eq, Show)

-- | The size of the 4 tiering runs at each level are allowed to be:
-- @4^(level-1) < size <= 4^level@
--
tieringRunSize :: Int -> Int
tieringRunSize n = 4^n

-- | Levelling runs take up the whole level, so are 4x larger.
--
levellingRunSize :: Int -> Int
levellingRunSize n = 4^(n+1)

tieringRunSizeToLevel :: Run -> Int
tieringRunSizeToLevel r
  | s <= maxBufferSize = 1  -- level numbers start at 1
  | otherwise =
    1 + (finiteBitSize s - countLeadingZeros (s-1) - 1) `div` 2
  where
    s = runSize r

levellingRunSizeToLevel :: Run -> Int
levellingRunSizeToLevel r =
    max 1 (tieringRunSizeToLevel r - 1)  -- level numbers start at 1

maxBufferSize :: Int
maxBufferSize = tieringRunSize 1 -- 4

-- | We use levelling on the last level, unless that is also the first level.
mergePolicyForLevel :: Int -> [Level s] -> UnionLevel s -> MergePolicy
mergePolicyForLevel 1 _  _       = MergePolicyTiering
mergePolicyForLevel _ [] NoUnion = MergePolicyLevelling
mergePolicyForLevel _ _  _       = MergePolicyTiering

-- | If there are no further levels provided, this level is the last one.
-- However, if a 'Union' is present, it acts as another (last) level.
mergeLastForLevel :: [Level s] -> UnionLevel s -> MergeLastLevel
mergeLastForLevel [] NoUnion = MergeLastLevel
mergeLastForLevel _  _       = MergeMidLevel

-- | Note that the invariants rely on the fact that levelling is only used on
-- the last level.
--
invariant :: forall s. LSMContent s -> ST s ()
invariant (LSMContent _ levels ul) = do
    levelsInvariant 1 levels
    case ul of
      NoUnion      -> return ()
      Union tree _ -> treeInvariant tree
  where
    mergeLast :: Levels s -> MergeLastLevel
    mergeLast ls = mergeLastForLevel ls ul

    levelsInvariant :: Int -> Levels s -> ST s ()
    levelsInvariant !_ [] = return ()

    levelsInvariant !ln (Level ir rs : ls) = do
      mrs <- case ir of
        Single r ->
          return (CompletedMerge r)
        Merging mp (MergingRun ml ref) -> do
          assertST $ mp == mergePolicyForLevel ln ls ul
                  && ml == MergeLevel (mergeLast ls)
          readSTRef ref

      assertST $ length rs <= 3
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
        MergePolicyLevelling -> assertST $ null rs
        -- Runs in tiering levels usually fit that size, but they can be one
        -- larger, if a run has been held back (creating a 5-way merge).
        MergePolicyTiering   -> assertST $ all (\r -> tieringRunSizeToLevel r `elem` [ln, ln+1]) rs
        -- (This is actually still not really true, but will hold in practice.
        -- In the pathological case, all runs passed to the next level can be
        -- factor (5/4) too large, and there the same holding back can lead to
        -- factor (6/4) etc., until at level 12 a run is two levels too large.

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
              assertST $ levellingRunSizeToLevel r == ln

            -- A completed merge for levelling can be of almost any size at all!
            -- It can be smaller, due to deletions in the last level. But it
            -- can't be bigger than would fit into the next level.
            (_, CompletedMerge r) ->
              assertST $ levellingRunSizeToLevel r <= ln+1

            -- An ongoing merge for levelling should have 4 incoming runs of
            -- the right size for the level below (or slightly larger due to
            -- holding back underfull runs), and 1 run from this level,
            -- but the run from this level can be of almost any size for the
            -- same reasons as above. Although if this is the first merge for
            -- a new level, it'll have only 4 runs.
            (_, OngoingMerge _ rs _) -> do
              assertST $ length rs `elem` [4, 5]
              assertST $ all (\r -> runSize r > 0) rs  -- don't merge empty runs
              let incoming = take 4 rs
              let resident = drop 4 rs
              assertST $ all (\r -> tieringRunSizeToLevel r `elem` [ln-1, ln]) incoming
              assertST $ all (\r -> levellingRunSizeToLevel r <= ln+1) resident

        MergePolicyTiering ->
          case (ir, mrs, mergeLast ls) of
            -- A single incoming run (which thus didn't need merging) must be
            -- of the expected size already
            (Single r, m, _) -> do
              assertST $ case m of CompletedMerge{} -> True
                                   OngoingMerge{}   -> False
              assertST $ tieringRunSizeToLevel r == ln

            -- A completed last level run can be of almost any smaller size due
            -- to deletions, but it can't be bigger than the next level down.
            -- Note that tiering on the last level only occurs when there is
            -- a single level only.
            (_, CompletedMerge r, MergeLastLevel) -> do
              assertST $ ln == 1
              assertST $ tieringRunSizeToLevel r <= ln+1

            -- A completed mid level run is usually of the size for the
            -- level it is entering, but can also be one smaller (in which case
            -- it'll be held back and merged again) or one larger (because it
            -- includes a run that has been held back before).
            (_, CompletedMerge r, MergeMidLevel) ->
              assertST $ tieringRunSizeToLevel r `elem` [ln-1, ln, ln+1]

            -- An ongoing merge for tiering should have 4 incoming runs of
            -- the right size for the level below, and at most 1 run held back
            -- due to being too small (which would thus also be of the size of
            -- the level below).
            (_, OngoingMerge _ rs _, _) -> do
              assertST $ length rs == 4 || length rs == 5
              assertST $ all (\r -> tieringRunSizeToLevel r == ln-1) rs

    -- We don't make many assumptions apart from what the types already enforce.
    -- In particular, there are no invariants on the progress of the merges,
    -- since union merge credits are independent from the tables' regular level
    -- merges.
    treeInvariant :: MergingTree s -> ST s ()
    treeInvariant (MergingTree treeState) = readSTRef treeState >>= \case
        CompletedTreeMerge _ ->
          return ()

        OngoingTreeMerge (MergingRun mergeType mergeState) -> do
          readSTRef mergeState >>= \case
            CompletedMerge _ -> return ()
            OngoingMerge _ rs _ -> do
              -- Inputs to ongoing merges aren't empty (but can while pending!).
              assertST $ all (\r -> runSize r > 0) rs
              case mergeType of
                -- Union merges are always binary.
                MergeUnion   -> assertST $ length rs == 2
                -- Merges are non-trivial (at least two inputs).
                MergeLevel _ -> assertST $ length rs > 1

        PendingTreeMerge (PendingLevelMerge irs tree) -> do
          -- Merges are non-trivial (at least two inputs).
          assertST $ length irs + length tree > 1
          -- The IncomingRuns of a level merge come from the regular levels of a
          -- table, so any MergingRuns must be level merges.
          for_ irs $ \case
            Merging _ (MergingRun mt _) -> assertST $ mt /= MergeUnion
            Single _                    -> return ()
          for_ tree treeInvariant

        PendingTreeMerge (PendingUnionMerge trees) -> do
          -- Union merges are always binary.
          assertST $ length trees == 2
          for_ trees treeInvariant

-- 'callStack' just ensures that the 'HasCallStack' constraint is not redundant
-- when compiling with debug assertions disabled.
assert :: HasCallStack => Bool -> a -> a
assert p x = Exc.assert p (const x callStack)

assertST :: HasCallStack => Bool -> ST s ()
assertST p = assert p $ return ()

-------------------------------------------------------------------------------
-- Merging run abstraction
--

newMergingRun :: Maybe Debt -> MergeType -> [Run] -> ST s (MergingRun s)
newMergingRun mdebt mergeType runs = do
    assertST $ length runs > 1
    -- in some cases, no merging is required at all
    state <- case filter (\r -> runSize r > 0) runs of
      []  -> return $ CompletedMerge Map.empty
      [r] -> return $ CompletedMerge r
      rs  -> do
        let !cost = sum (map runSize rs)
        debt <- newMergeDebt <$> case mdebt of
          Nothing -> return cost
          Just d  -> assert (d >= cost) $ return d
        let merged = mergek mergeType rs  -- deliberately lazy
        return $ OngoingMerge debt rs merged
    MergingRun mergeType <$> newSTRef state

mergek :: MergeType -> [Run] -> Run
mergek = \case
    MergeLevel MergeMidLevel  -> Map.unionsWith combine
    MergeLevel MergeLastLevel -> dropDeletes . Map.unionsWith combine
    MergeUnion                -> dropDeletes . Map.unionsWith combineUnion
  where
    dropDeletes = Map.filter (/= Delete)

-- | Combines two entries that have been performed after another. Therefore, the
-- newer one overwrites the old one (or modifies it for 'Mupsert').
combine :: Op -> Op -> Op
combine new_ old = case new_ of
    Insert{}  -> new_
    Delete{}  -> new_
    Mupsert v -> case old of
      Insert v' b -> Insert (resolveValue v v') b
      Delete      -> Insert v Nothing
      Mupsert v'  -> Mupsert (resolveValue v v')

-- | Combines two entries of runs that have been 'union'ed together. If any one
-- has a value, the result should have a value (represented by 'Insert'). If
-- both have a value, these values get combined monoidally.
--
-- See 'MergeUnion'.
combineUnion :: Op -> Op -> Op
combineUnion Delete         old          = old
combineUnion new_           Delete       = new_
combineUnion (Mupsert v')   (Mupsert v ) = Insert (resolveValue v' v) Nothing
combineUnion (Mupsert v')   (Insert v b) = Insert (resolveValue v' v) b
combineUnion (Insert v' b') (Mupsert v)  = Insert (resolveValue v' v) b'
combineUnion (Insert v' b') (Insert v b) = Insert (resolveValue v' v)
                                                  (resolveBlob b' b)

expectCompletedMergingRun :: HasCallStack => MergingRun s -> ST s Run
expectCompletedMergingRun (MergingRun _ ref) = do
    mrs <- readSTRef ref
    case mrs of
      CompletedMerge r   -> return r
      OngoingMerge d _ _ -> error $ "expectCompletedMergingRun:"
                                 ++ " remaining debt of " ++ show d

supplyCreditsMergingRun :: Credit -> MergingRun s -> ST s Credit
supplyCreditsMergingRun = checked remainingDebtMergingRun $ \credits (MergingRun _ ref) -> do
    mrs <- readSTRef ref
    case mrs of
      CompletedMerge{} -> return credits
      OngoingMerge d rs r ->
        case paydownMergeDebt credits d of
          MergeDebtDischarged _ c' -> do
            writeSTRef ref (CompletedMerge r)
            return c'

          MergeDebtPaydownCredited  d' -> do
            writeSTRef ref (OngoingMerge d' rs r)
            return 0

          MergeDebtPaydownPerform _ d' -> do
            -- we're not doing any actual merging
            -- just tracking what we would do
            writeSTRef ref (OngoingMerge d' rs r)
            return 0

mergeBatchSize :: Int
mergeBatchSize = 32

data MergeDebt =
    MergeDebt
      Credit -- ^ Cumulative, not yet used credits.
      Debt -- ^ Leftover debt.
  deriving stock Show

newMergeDebt :: HasCallStack => Debt -> MergeDebt
newMergeDebt d = assert (d > 0) $ MergeDebt 0 d

mergeDebtLeft :: HasCallStack => MergeDebt -> Debt
mergeDebtLeft (MergeDebt c d) = assert (c < d) $ d - c

-- | As credits are paid, debt is reduced in batches when sufficient credits have accumulated.
data MergeDebtPaydown =
    -- | This remaining merge debt is fully paid off, potentially with leftovers.
    MergeDebtDischarged      !Debt !Credit
    -- | Credits were paid, but not enough for merge debt to be reduced by some batches of merging work.
  | MergeDebtPaydownCredited       !MergeDebt
    -- | Enough credits were paid to reduce merge debt by performing some batches of merging work.
  | MergeDebtPaydownPerform  !Debt !MergeDebt
  deriving stock Show

-- | Pay credits to merge debt, which might trigger performing some merge work in batches. See 'MergeDebtPaydown'.
--
paydownMergeDebt :: Credit -> MergeDebt -> MergeDebtPaydown
paydownMergeDebt c2 (MergeDebt c d)
  | d-c' <= 0
  = MergeDebtDischarged d (c'-d)

  | c' >= mergeBatchSize
  , let (!b, !r) = divMod c' mergeBatchSize
        !perform = b * mergeBatchSize
  = MergeDebtPaydownPerform perform (MergeDebt r (d-perform))

  | otherwise
  = MergeDebtPaydownCredited (MergeDebt c' d)
  where
    !c' = c+c2


-------------------------------------------------------------------------------
-- LSM handle
--

new :: ST s (LSM s)
new = do
  c   <- newSTRef 0
  lsm <- newSTRef (LSMContent Map.empty [] NoUnion)
  return (LSMHandle c lsm)

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
update tr (LSMHandle scr lsmr) k op = do
    sc <- readSTRef scr
    content@(LSMContent wb ls unionLevel) <- readSTRef lsmr
    modifySTRef' scr (+1)
    supplyCreditsLevels 1 ls
    invariant content
    let wb' = Map.insertWith combine k op wb
    if bufferSize wb' >= maxBufferSize
      then do
        ls' <- increment tr sc (bufferToRun wb') ls unionLevel
        let content' = LSMContent Map.empty ls' unionLevel
        invariant content'
        writeSTRef lsmr content'
      else
        writeSTRef lsmr (LSMContent wb' ls unionLevel)

supplyMergeCredits :: LSM s -> Credit -> ST s ()
supplyMergeCredits (LSMHandle scr lsmr) credits = do
    content@(LSMContent _ ls _) <- readSTRef lsmr
    modifySTRef' scr (+1)
    supplyCreditsLevels credits ls
    invariant content

data LookupResult v b =
    NotFound
  | Found !v !(Maybe b)
  deriving stock (Eq, Show)

lookups :: LSM s -> [Key] -> ST s [LookupResult Value Blob]
lookups (LSMHandle _ lsmr) ks = do
    LSMContent wb ls ul <- readSTRef lsmr
    runs <- concat <$> flattenLevels ls
    traverse (doLookup wb runs ul) ks

lookup :: LSM s -> Key -> ST s (LookupResult Value Blob)
lookup (LSMHandle _ lsmr) k = do
    LSMContent wb ls ul <- readSTRef lsmr
    runs <- concat <$> flattenLevels ls
    doLookup wb runs ul k

duplicate :: LSM s -> ST s (LSM s)
duplicate (LSMHandle _scr lsmr) = do
    scr'  <- newSTRef 0
    lsmr' <- newSTRef =<< readSTRef lsmr
    return (LSMHandle scr' lsmr')
    -- it's that simple here, because we share all the pure value and all the
    -- STRefs and there's no ref counting to be done

-- | Similar to @Data.Map.unionWith@.
--
-- A call to 'union' itself is not expensive, as the input tables are not
-- immediately merged. Instead, it creates a representation of an in-progress
-- merge that can be performed incrementally (somewhat similar to a thunk).
--
-- The more merge work remains, the more expensive are lookups on the table.
union :: LSM s -> LSM s -> ST s (LSM s)
union (LSMHandle _ lsmr1) (LSMHandle _ lsmr2) = do
    mt1 <- contentToMergingTree =<< readSTRef lsmr1
    mt2 <- contentToMergingTree =<< readSTRef lsmr2
    -- TODO: if only one table is non-empty, we don't have to create a Union,
    -- we can just duplicate the table.
    unionLevel <- newPendingUnionMerge (catMaybes [mt1, mt2]) >>= \case
      Nothing -> return NoUnion
      Just tree -> do
        debt <- fst <$> remainingDebtMergingTree tree
        Union tree <$> newSTRef debt
    lsmr <- newSTRef (LSMContent Map.empty [] unionLevel)
    c    <- newSTRef 0
    return (LSMHandle c lsmr)

-- | An upper bound on the number of merging steps that need to be performed
-- until the delayed work of performing a 'union' is completed. This debt can
-- be paid off using 'supplyUnionCredits'. This includes the cost for existing
-- merges that were part of the union's input tables.
remainingUnionDebt :: LSM s -> ST s Debt
remainingUnionDebt (LSMHandle _ lsmr) = do
    LSMContent _ _ ul <- readSTRef lsmr
    case ul of
      NoUnion      -> return 0
      Union tree d -> checkedUnionDebt tree d

-- | Supplying credits leads to union merging work being performed in batches.
-- This reduces the debt returned by 'remainingUnionDebt'.
supplyUnionCredits :: LSM s -> Credit -> ST s Credit
supplyUnionCredits (LSMHandle scr lsmr) credits
  | credits <= 0 = return 0
  | otherwise = do
    content@(LSMContent _ _ ul) <- readSTRef lsmr
    case ul of
      NoUnion ->
        return credits
      Union tree debtRef -> do
        modifySTRef' scr (+1)
        _debt <- checkedUnionDebt tree debtRef  -- just to make sure it's checked
        c' <- supplyCreditsMergingTree credits tree
        debt' <- checkedUnionDebt tree debtRef
        if (debt' > 0)
          then
            -- should have spent these credits
            assertST $ c' == 0
          else do
            -- check it really is done
            _ <- expectCompletedMergingTree tree
            return ()
        invariant content
        return c'

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
    return debt

-------------------------------------------------------------------------------
-- Lookups
--

type LookupAcc = Maybe Op

mergeAcc :: [LookupAcc] -> LookupAcc
mergeAcc = foldl (updateAcc combine) Nothing . catMaybes

unionAcc :: [LookupAcc] -> LookupAcc
unionAcc = foldl (updateAcc combineUnion) Nothing . catMaybes

updateAcc :: (Op -> Op -> Op) -> LookupAcc -> Op -> LookupAcc
updateAcc _ Nothing     old = Just old
updateAcc f (Just new_) old = Just (f new_ old)  -- acc has more recent Op

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
        return (convertAcc acc0)
      Union tree _ -> do
        accTree <- lookupsTree k tree
        return (convertAcc (mergeAcc [acc0, accTree]))
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
lookupsTree :: Key -> MergingTree s -> ST s LookupAcc
lookupsTree k = go
  where
    go :: MergingTree s -> ST s LookupAcc
    go (MergingTree treeState) = readSTRef treeState >>= \case
        CompletedTreeMerge r ->
          return $ lookupBatch' [r]
        OngoingTreeMerge (MergingRun mt mergeState) ->
          readSTRef mergeState >>= \case
            CompletedMerge r ->
              return $ lookupBatch' [r]
            OngoingMerge _ rs _ -> case mt of
              MergeLevel _ -> return $ lookupBatch' rs  -- combine into batch
              MergeUnion   -> return $ unionAcc (map (\r -> lookupBatch' [r]) rs)
        PendingTreeMerge (PendingUnionMerge trees) -> do
          unionAcc <$> traverse go trees
        PendingTreeMerge (PendingLevelMerge irs tree) -> do
          runs <- concat <$> traverse flattenIncomingRun irs -- combine into batch
          let acc0 = lookupBatch' runs
          case tree of
            Nothing -> return acc0  -- only runs and merging level runs, done
            Just t  -> do
              accTree <- go t
              return (mergeAcc [acc0, accTree])

    lookupBatch' = lookupBatch Nothing k

-------------------------------------------------------------------------------
-- Updates
--

-- TODO: If there is a UnionLevel, there is no (more expensive) last level merge
-- in the regular levels, so a little less merging work is required than if
-- there was no UnionLevel. It might be a good idea to spend this "saved" work
-- on the UnionLevel instead. This makes future lookups cheaper and ensures that
-- we can get rid of the UnionLevel at some point, even if a user just keeps
-- inserting without calling 'supplyUnionCredits'.
supplyCreditsLevels :: Credit -> Levels s -> ST s ()
supplyCreditsLevels unscaled =
  traverse_ $ \(Level ir _rs) -> do
    case ir of
      Single{} -> return ()
      Merging mp mr -> do
        factor <- creditsForMerge mp mr
        let credits = ceiling (fromIntegral unscaled * factor)
        when (credits > 0) $ do
          _ <- supplyCreditsMergingRun credits mr
          -- we don't mind leftover credits, each level completes independently
          return ()

-- | The general case (and thus worst case) of how many merge credits we need
-- for a level. This is based on the merging policy at the level.
--
creditsForMerge :: MergePolicy -> MergingRun s -> ST s Rational

-- A levelling merge has 1 input run and one resident run, which is (up to) 4x
-- bigger than the others.
-- It needs to be completed before another run comes in.
creditsForMerge MergePolicyLevelling _ =
    return $ (1 + 4) / 1

-- A tiering merge has 5 runs at most (once could be held back to merged again)
-- and must be completed before the level is full (once 4 more runs come in).
creditsForMerge MergePolicyTiering (MergingRun _ ref) = do
    readSTRef ref >>= \case
      CompletedMerge _ -> return 0
      OngoingMerge _ rs _ -> do
        let numRuns = length rs
        assertST $ numRuns `elem` [4, 5]
        return $ fromIntegral numRuns / 4

increment :: forall s. Tracer (ST s) Event
          -> Counter -> Run -> Levels s -> UnionLevel s -> ST s (Levels s)
increment tr sc run0 ls0 ul = do
    go 1 [run0] ls0
  where
    mergeLast :: Levels s -> MergeLastLevel
    mergeLast ls = mergeLastForLevel ls ul

    go :: Int -> [Run] -> Levels s -> ST s (Levels s)
    go !ln incoming [] = do
        let mergePolicy = mergePolicyForLevel ln [] ul
        traceWith tr' AddLevelEvent
        ir <- newLevelMerge tr' ln mergePolicy (mergeLast []) incoming
        return (Level ir [] : [])
      where
        tr' = contramap (EventAt sc ln) tr

    go !ln incoming (Level ir rs : ls) = do
      r <- case ir of
        Single r -> return r
        Merging mergePolicy mr -> do
          r <- expectCompletedMergingRun mr
          traceWith tr' MergeCompletedEvent {
              mergePolicy,
              mergeType = let MergingRun mt _ = mr in mt,
              mergeSize = runSize r
            }
          return r

      let resident = r:rs
      case mergePolicyForLevel ln ls ul of

        -- If r is still too small for this level then keep it and merge again
        -- with the incoming runs.
        MergePolicyTiering | tieringRunSizeToLevel r < ln -> do
          ir' <- newLevelMerge tr' ln MergePolicyTiering (mergeLast ls) (incoming ++ [r])
          return (Level ir' rs : ls)

        -- This tiering level is now full. We take the completed merged run
        -- (the previous incoming runs), plus all the other runs on this level
        -- as a bundle and move them down to the level below. We start a merge
        -- for the new incoming runs. This level is otherwise empty.
        MergePolicyTiering | tieringLevelIsFull ln incoming resident -> do
          ir' <- newLevelMerge tr' ln MergePolicyTiering MergeMidLevel incoming
          ls' <- go (ln+1) resident ls
          return (Level ir' [] : ls')

        -- This tiering level is not yet full. We move the completed merged run
        -- into the level proper, and start the new merge for the incoming runs.
        MergePolicyTiering -> do
          ir' <- newLevelMerge tr' ln MergePolicyTiering (mergeLast ls) incoming
          traceWith tr' (AddRunEvent (length resident))
          return (Level ir' resident : ls)

        -- The final level is using levelling. If the existing completed merge
        -- run is too large for this level, we promote the run to the next
        -- level and start merging the incoming runs into this (otherwise
        -- empty) level .
        MergePolicyLevelling | levellingLevelIsFull ln incoming r -> do
          assert (null rs && null ls) $ return ()
          ir' <- newLevelMerge tr' ln MergePolicyTiering MergeMidLevel incoming
          ls' <- go (ln+1) [r] []
          return (Level ir' [] : ls')

        -- Otherwise we start merging the incoming runs into the run.
        MergePolicyLevelling -> do
          assert (null rs && null ls) $ return ()
          ir' <- newLevelMerge tr' ln MergePolicyLevelling (mergeLast ls)
                          (incoming ++ [r])
          return (Level ir' [] : [])

      where
        tr' = contramap (EventAt sc ln) tr

newLevelMerge :: Tracer (ST s) EventDetail
              -> Int -> MergePolicy -> MergeLastLevel
              -> [Run] -> ST s (IncomingRun s)
newLevelMerge _ _ _ _ [r] = return (Single r)
newLevelMerge tr level mergePolicy mergeLast rs = do
    traceWith tr MergeStartedEvent {
                   mergePolicy,
                   mergeType,
                   mergeDebt     = debt,
                   mergeRunsSize = map runSize rs
                 }
    assertST (length rs `elem` [4, 5])
    Merging mergePolicy <$> newMergingRun (Just debt) mergeType rs
  where
    mergeType = MergeLevel mergeLast
    -- How much we need to discharge before the merge can be guaranteed
    -- complete. More precisely, this is the maximum amount a merge at this
    -- level could need. While the real @cost@ of a merge would lead to merges
    -- finishing early, the overestimation @debt@ means that in this prototype
    -- merges will only complete at the last possible moment.
    -- Note that for levelling this is includes the single run in the current
    -- level.
    debt = case mergePolicy of
             MergePolicyLevelling -> 4 * tieringRunSize (level-1)
                                       + levellingRunSize level
             MergePolicyTiering   -> length rs * tieringRunSize (level-1)

-- | Only based on run count, not their sizes.
tieringLevelIsFull :: Int -> [Run] -> [Run] -> Bool
tieringLevelIsFull _ln _incoming resident = length resident >= 4

-- | The level is only considered full once the resident run is /too large/ for
-- the level.
levellingLevelIsFull :: Int -> [Run] -> Run -> Bool
levellingLevelIsFull ln _incoming resident = levellingRunSizeToLevel resident > ln

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

-- | Ensures that the merge contains more than one input.
newPendingLevelMerge :: [IncomingRun s]
                     -> Maybe (MergingTree s)
                     -> ST s (Maybe (MergingTree s))
newPendingLevelMerge [] t = return t
newPendingLevelMerge [ir] Nothing = do
    let st = case ir of
          Single r     -> CompletedTreeMerge r
          Merging _ mr -> OngoingTreeMerge mr
    Just . MergingTree <$> newSTRef st
newPendingLevelMerge irs tree = do
    let st = PendingTreeMerge (PendingLevelMerge irs tree)
    Just . MergingTree <$> newSTRef st

-- | Ensures that the merge contains more than one input.
newPendingUnionMerge :: [MergingTree s] -> ST s (Maybe (MergingTree s))
newPendingUnionMerge []  = return Nothing
newPendingUnionMerge [t] = return (Just t)
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
      CompletedTreeMerge r -> return (0, runSize r)
      OngoingTreeMerge mr  -> remainingDebtMergingRun mr
      PendingTreeMerge pm  -> remainingDebtPendingMerge pm

remainingDebtPendingMerge :: PendingMerge s -> ST s (Debt, Size)
remainingDebtPendingMerge (PendingMerge _ irs trees) = do
    (debts, sizes) <- unzip . concat <$> sequence
        [ traverse remainingDebtIncomingRun irs
        , traverse remainingDebtMergingTree trees
        ]
    let totalSize = sum sizes
    let totalDebt = sum debts + totalSize
    return (totalDebt, totalSize)
  where
    remainingDebtIncomingRun = \case
        Single r     -> return (0, runSize r)
        Merging _ mr -> remainingDebtMergingRun mr

remainingDebtMergingRun :: MergingRun s -> ST s (Debt, Size)
remainingDebtMergingRun (MergingRun _ ref) =
    readSTRef ref >>= \case
      CompletedMerge r ->
        return (0, runSize r)
      OngoingMerge d inputRuns _ ->
        return (mergeDebtLeft d, sum (map runSize inputRuns))

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
    return c'

supplyCreditsMergingTree :: Credit -> MergingTree s -> ST s Credit
supplyCreditsMergingTree = checked remainingDebtMergingTree $ \credits (MergingTree ref) -> do
    treeState <- readSTRef ref
    (!c', !treeState') <- supplyCreditsMergingTreeState credits treeState
    writeSTRef ref treeState'
    return c'

supplyCreditsMergingTreeState :: Credit -> MergingTreeState s
                              -> ST s (Credit, MergingTreeState s)
supplyCreditsMergingTreeState credits !state = do
    assertST (credits >= 0)
    case state of
      CompletedTreeMerge{} ->
        return (credits, state)
      OngoingTreeMerge mr -> do
        c' <- supplyCreditsMergingRun credits mr
        if c' <= 0
          then return (0, state)
          else do
            r <- expectCompletedMergingRun mr
            -- all work is done, we can't spend any more credits
            return (c', CompletedTreeMerge r)
      PendingTreeMerge pm -> do
        c' <- supplyCreditsPendingMerge credits pm
        if c' <= 0
          then
            -- still remaining work in children, we can't do more for now
            return (c', state)
          else do
            -- all children must be done, create new merge!
            (mergeType, rs) <- expectCompletedChildren pm
            -- no reason to claim a larger debt than sum of run sizes
            let debt = Nothing
            state' <- OngoingTreeMerge <$> newMergingRun debt mergeType rs
            -- use any remaining credits to progress the new merge
            supplyCreditsMergingTreeState c' state'

supplyCreditsPendingMerge :: Credit -> PendingMerge s -> ST s Credit
supplyCreditsPendingMerge = checked remainingDebtPendingMerge $ \credits -> \case
    PendingLevelMerge irs tree ->
      leftToRight supplyIncoming irs credits
        >>= leftToRight supplyCreditsMergingTree (toList tree)
    PendingUnionMerge [tree1, tree2] ->
      splitEqually tree1 tree2 credits
    PendingUnionMerge trees ->
      error $ "supplyCreditsPendingMerge: "
           ++ "expected two union merge inputs, got " ++ show (length trees)
  where
    supplyIncoming c = \case
        Single _     -> return c
        Merging _ mr -> supplyCreditsMergingRun c mr

    -- supply credits left to right until they are used up
    leftToRight :: (Credit -> a -> ST s Credit) -> [a] -> Credit -> ST s Credit
    leftToRight _ _      0 = return 0
    leftToRight _ []     c = return c
    leftToRight f (x:xs) c = f c x >>= leftToRight f xs

    -- supply credit roughly evenly on both sides
    splitEqually :: MergingTree s -> MergingTree s -> Credit -> ST s Credit
    splitEqually mt1 mt2 c = do
        let (c1, c2) = (c `div` 2, c - c1)
        c1' <- if c1 > 0
          then supplyCreditsMergingTree c1 mt1
          else return 0
        if c1' > 0
          then
            -- left side done, use all remaining credits on the right side
            supplyCreditsMergingTree (c2 + c1') mt2
          else do
            -- left side still not done
            -- if the right side has leftovers, go back
            c2' <- supplyCreditsMergingTree c2 mt2
            if c2' > 0
              then supplyCreditsMergingTree c2' mt1
              else return 0

expectCompletedChildren :: HasCallStack => PendingMerge s -> ST s (MergeType, [Run])
expectCompletedChildren (PendingMerge mt irs trees) = do
    rs1 <- traverse expectCompletedIncomingRun irs
    rs2 <- traverse expectCompletedMergingTree trees
    return (mt, rs1 ++ rs2)
  where
    expectCompletedIncomingRun = \case
        Single     r -> return r
        Merging _ mr -> expectCompletedMergingRun mr

expectCompletedMergingTree :: HasCallStack => MergingTree s -> ST s Run
expectCompletedMergingTree (MergingTree ref) = do
    readSTRef ref >>= \case
      CompletedTreeMerge r -> return r
      OngoingTreeMerge mr  -> expectCompletedMergingRun mr
      PendingTreeMerge _   -> error $ "expectCompletedMergingTree: PendingTreeMerge"

-------------------------------------------------------------------------------
-- Measurements
--

data MTree = MLeaf Run
           | MNode MergeType [MTree]
  deriving stock Show

allLevels :: LSM s -> ST s (Buffer, [[Run]], Maybe MTree)
allLevels (LSMHandle _ lsmr) = do
    LSMContent wb ls ul <- readSTRef lsmr
    rs <- flattenLevels ls
    tree <- case ul of
      NoUnion   -> return Nothing
      Union t _ -> Just <$> flattenTree t
    return (wb, rs, tree)

flattenLevels :: Levels s -> ST s [[Run]]
flattenLevels = mapM flattenLevel

flattenLevel :: Level s -> ST s [Run]
flattenLevel (Level ir rs) = (++ rs) <$> flattenIncomingRun ir

flattenIncomingRun :: IncomingRun s -> ST s [Run]
flattenIncomingRun = \case
    Single r     -> return [r]
    Merging _ mr -> flattenMergingRun mr

flattenMergingRun :: MergingRun s -> ST s [Run]
flattenMergingRun (MergingRun _ ref) = do
    mrs <- readSTRef ref
    case mrs of
      CompletedMerge r    -> return [r]
      OngoingMerge _ rs _ -> return rs

flattenTree :: MergingTree s -> ST s MTree
flattenTree (MergingTree ref) = do
    mts <- readSTRef ref
    case mts of
      CompletedTreeMerge r ->
        return (MLeaf r)
      OngoingTreeMerge (MergingRun mt mrs) ->
        readSTRef mrs >>= \case
          CompletedMerge r    -> return (MLeaf r)
          OngoingMerge _ rs _ -> return (MNode mt (MLeaf <$> rs))
      PendingTreeMerge (PendingMerge mt irs trees) -> do
        irs' <- map MLeaf . concat <$> traverse flattenIncomingRun irs
        trees' <- traverse flattenTree trees
        return (MNode mt (irs' ++ trees'))

logicalValue :: LSM s -> ST s (Map Key (Value, Maybe Blob))
logicalValue lsm = do
    (wb, levels, tree) <- allLevels lsm
    let r = mergeRuns
              (MergeLevel MergeLastLevel)
              (wb : concat levels ++ toList (mergeTree <$> tree))
    return (Map.mapMaybe justInsert r)
  where
    mergeRuns :: MergeType -> [Run] -> Run
    mergeRuns = mergek

    mergeTree :: MTree -> Run
    mergeTree (MLeaf r)     = r
    mergeTree (MNode mt ts) = mergeRuns mt (map mergeTree ts)

    justInsert (Insert v b) = Just (v, b)
    justInsert  Delete      = Nothing
    justInsert (Mupsert v)  = Just (v, Nothing)

-- TODO: Consider MergingTree, or just remove this function? It's unused.
dumpRepresentation :: LSM s
                   -> ST s [(Maybe (MergePolicy, MergeType, MergingRunState), [Run])]
dumpRepresentation (LSMHandle _ lsmr) = do
    LSMContent wb ls _ <- readSTRef lsmr
    ((Nothing, [wb]) :) <$> mapM dumpLevel ls

dumpLevel :: Level s -> ST s (Maybe (MergePolicy, MergeType, MergingRunState), [Run])
dumpLevel (Level (Single r) rs) =
    return (Nothing, (r:rs))
dumpLevel (Level (Merging mp (MergingRun mt ref)) rs) = do
    mrs <- readSTRef ref
    return (Just (mp, mt, mrs), rs)

-- For each level:
-- 1. the runs involved in an ongoing merge
-- 2. the other runs (including completed merge)
representationShape :: [(Maybe (MergePolicy, MergeType, MergingRunState), [Run])]
                    -> [([Int], [Int])]
representationShape =
    map $ \(mmr, rs) ->
      let (ongoing, complete) = summaryMR mmr
      in (ongoing, complete <> map summaryRun rs)
  where
    summaryRun = runSize
    summaryMR = \case
      Nothing                          -> ([], [])
      Just (_, _, CompletedMerge r)    -> ([], [summaryRun r])
      Just (_, _, OngoingMerge _ rs _) -> (map summaryRun rs, [])

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
         mergeType     :: MergeType,
         mergeDebt     :: Debt,
         mergeRunsSize :: [Int]
       }
     | MergeCompletedEvent {
         mergePolicy :: MergePolicy,
         mergeType   :: MergeType,
         mergeSize   :: Int
       }
  deriving stock Show
