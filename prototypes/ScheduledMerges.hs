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
import           Data.Foldable (traverse_)
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import           Data.STRef

import           Control.Exception (assert)
import           Control.Monad.ST
import           Control.Tracer (Tracer, contramap, traceWith)
import           GHC.Stack (HasCallStack, callStack)


data LSM s  = LSMHandle !(STRef s Counter)
                        !(STRef s (LSMContent s))
data LSMContent s = LSMContent Buffer (Levels s)

-- | A simple count of LSM operations to allow logging the operation
-- number in each event. This enables relating merge events to the
-- operation number (which is interesting for numerical representations
-- like this). We would not need this in the real implementation.
type Counter = Int

type Levels s = [Level s]

data Level s =
       -- | A level is a sequence of resident runs at this level, prefixed by an
       -- incoming run, which is usually multiple runs that are being merged,
       -- with the result run to live at this level.
       Level !(IncomingRun s) ![Run]

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
-- using 'resolveValue' (as if they were mupserts).
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

                       -- let r = merge4 r1 r2 r3 r4
                       --  in OngoingMerge 0 [r1, r2, r3, r4] r
                     | OngoingMerge !MergeDebt ![Run] Run

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

mergePolicyForLevel :: Int -> [Level s] -> MergePolicy
mergePolicyForLevel 1 [] = MergePolicyTiering
mergePolicyForLevel _ [] = MergePolicyLevelling
mergePolicyForLevel _ _  = MergePolicyTiering

mergeLastForLevel :: [Level s] -> MergeLastLevel
mergeLastForLevel [] = MergeLastLevel
mergeLastForLevel _  = MergeMidLevel

-- | Note that the invariants rely on the fact that levelling is only used on
-- the last level.
--
invariant :: forall s. Levels s -> ST s ()
invariant = go 1
  where
    go :: Int -> [Level s] -> ST s ()
    go !_ [] = return ()

    go !ln (Level ir rs : ls) = do

      mrs <- case ir of
        Single r                     -> return (CompletedMerge r)
        Merging _ (MergingRun _ ref) -> readSTRef ref

      assertST $ case ir of
        Single{} -> True
        Merging mp (MergingRun ml _) ->
              mp == mergePolicyForLevel ln ls
           && ml == MergeLevel (mergeLastForLevel ls)
      assertST $ length rs <= 3
      expectedRunLengths ln rs ls
      expectedMergingRunLengths ln ir mrs ls

      go (ln+1) ls

    -- All runs within a level "proper" (as opposed to the incoming runs
    -- being merged) should be of the correct size for the level.
    expectedRunLengths :: Int -> [Run] -> [Level s] -> ST s ()
    expectedRunLengths ln rs ls =
      case mergePolicyForLevel ln ls of
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
      case mergePolicyForLevel ln ls of
        MergePolicyLevelling ->
          assert (mergeLastForLevel ls == MergeLastLevel) $
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
              let incoming = take 4 rs
              let resident = drop 4 rs
              assertST $ all (\r -> tieringRunSizeToLevel r `elem` [ln-1, ln]) incoming
              assertST $ all (\r -> levellingRunSizeToLevel r <= ln+1) resident

        MergePolicyTiering ->
          case (ir, mrs, mergeLastForLevel ls) of
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

-- 'callStack' just ensures that the 'HasCallStack' constraint is not redundant
-- when compiling with debug assertions disabled.
assertST :: HasCallStack => Bool -> ST s ()
assertST p = assert p $ return (const () callStack)


-------------------------------------------------------------------------------
-- Merging run abstraction
--

newLevelMerge :: Tracer (ST s) EventDetail
              -> Int -> MergePolicy -> MergeLastLevel
              -> [Run] -> ST s (IncomingRun s)
newLevelMerge _ _ _ _ [r] = return (Single r)
newLevelMerge tr level mergePolicy mergeLast rs = do
    traceWith tr MergeStartedEvent {
                   mergePolicy,
                   mergeType,
                   mergeDebt     = debt,
                   mergeCost     = cost,
                   mergeRunsSize = map runSize rs
                 }
    assert (length rs `elem` [4, 5]) $
      assert (mergeDebtLeft debt >= cost) $
        fmap (Merging mergePolicy . MergingRun mergeType) $
          newSTRef (OngoingMerge debt rs r)
  where
    mergeType = MergeLevel mergeLast
    cost = sum (map runSize rs)
    -- How much we need to discharge before the merge can be guaranteed
    -- complete. More precisely, this is the maximum amount a merge at this
    -- level could need. While the real @cost@ of a merge would lead to merges
    -- finishing early, the overestimation @debt@ means that in this prototype
    -- merges will only complete at the last possible moment.
    -- Note that for levelling this is includes the single run in the current
    -- level.
    debt = newMergeDebt $ case mergePolicy of
             MergePolicyLevelling -> 4 * tieringRunSize (level-1)
                                       + levellingRunSize level
             MergePolicyTiering   -> length rs * tieringRunSize (level-1)
    -- deliberately lazy:
    r    = mergek mergeType rs

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

expectCompletedMerge :: HasCallStack
                     => Tracer (ST s) EventDetail
                     -> MergePolicy -> MergingRun s -> ST s Run
expectCompletedMerge tr mergePolicy (MergingRun mergeType ref) = do
    mrs <- readSTRef ref
    case mrs of
      CompletedMerge r -> do
        traceWith tr MergeCompletedEvent {
            mergePolicy,
            mergeType,
            mergeSize = runSize r
          }
        return r
      OngoingMerge d _ _ ->
        error $ "expectCompletedMerge: false expectation, remaining debt of "
             ++ show d

supplyCreditsMergingRun :: Credit -> MergingRun s -> ST s Credit
supplyCreditsMergingRun c (MergingRun _ ref) = do
    mrs <- readSTRef ref
    case mrs of
      CompletedMerge{} -> return c
      OngoingMerge d rs r ->
        case paydownMergeDebt c d of
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

newMergeDebt :: Debt -> MergeDebt
newMergeDebt d = MergeDebt 0 d

mergeDebtLeft :: MergeDebt -> Int
mergeDebtLeft (MergeDebt c d) =
    assert (c < d) $ d - c

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
  lsm <- newSTRef (LSMContent Map.empty [])
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
    LSMContent wb ls <- readSTRef lsmr
    modifySTRef' scr (+1)
    supplyCreditsLevels 1 ls
    invariant ls
    let wb' = Map.insertWith combine k op wb
    if bufferSize wb' >= maxBufferSize
      then do
        ls' <- increment tr sc (bufferToRun wb') ls
        invariant ls'
        writeSTRef lsmr (LSMContent Map.empty ls')
      else
        writeSTRef lsmr (LSMContent wb' ls)

supplyMergeCredits :: LSM s -> Credit -> ST s ()
supplyMergeCredits (LSMHandle scr lsmr) credits = do
    LSMContent _ ls <- readSTRef lsmr
    modifySTRef' scr (+1)
    supplyCreditsLevels credits ls
    invariant ls

data LookupResult v b =
    NotFound
  | Found !v !(Maybe b)
  deriving stock (Eq, Show)

lookups :: LSM s -> [Key] -> ST s [LookupResult Value Blob]
lookups lsm ks = do
    (wb, runs) <- allLayers lsm
    return $ map (\k -> doLookup k wb (concat runs)) ks

lookup :: LSM s -> Key -> ST s (LookupResult Value Blob)
lookup lsm k = do
    (wb, runs) <- allLayers lsm
    return $ doLookup k wb (concat runs)

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
union = undefined

-- | An upper bound on the number of merging steps that need to be performed
-- until the delayed work of performing a 'union' is completed. This debt can
-- be paid off using 'supplyUnionCredits'.
remainingUnionDebt :: LSM s -> ST s Debt
remainingUnionDebt _ = return 0

-- | Supplying credits leads to union merging work being performed in batches.
-- This reduces the debt returned by 'remainingUnionDebt'.
supplyUnionCredits :: LSM s -> Credit -> ST s Credit
supplyUnionCredits _ credits = return credits

-------------------------------------------------------------------------------
-- Implementation
--

type LookupAcc = Maybe Op

-- | We handle lookups by accumulating results by going through the runs from
-- most recent to least recent, starting with the write buffer.
--
-- In the real implementation, this is done not on an individual 'LookupAcc',
-- but one for each key, i.e. @Vector (Maybe Entry)@.
doLookup :: Key -> Buffer -> [Run] -> LookupResult Value Blob
doLookup k wb =
    convertAcc . foldl updateAcc (Map.lookup k wb) . submitLookups k
  where
    updateAcc :: LookupAcc -> Op -> LookupAcc
    updateAcc Nothing     = Just
    updateAcc (Just new_) = Just . combine new_  -- acc has more recent Op

    convertAcc :: LookupAcc -> LookupResult Value Blob
    convertAcc = \case
        Nothing           -> NotFound
        Just (Insert v b) -> Found v b
        Just (Mupsert v)  -> Found v Nothing
        Just Delete       -> NotFound

-- | In a real implementation, this would use all keys at once and be in IO.
submitLookups :: Key -> [Run] -> [Op]
submitLookups k rs = [op | r <- rs, Just op <- [Map.lookup k r]]

supplyCreditsLevels :: Credit -> Levels s -> ST s ()
supplyCreditsLevels n =
  traverse_ $ \(Level ir _rs) -> do
    cr <- creditsForMerge ir
    case ir of
      Single{} ->
        return ()
      Merging _ mr -> do
        _ <- supplyCreditsMergingRun (ceiling (fromIntegral n * cr)) mr
        -- we don't mind leftover credits, each level completes independently
        return ()

-- | The general case (and thus worst case) of how many merge credits we need
-- for a level. This is based on the merging policy at the level.
--
creditsForMerge :: IncomingRun s -> ST s Rational
creditsForMerge Single{} =
    return 0

-- A levelling merge has 1 input run and one resident run, which is (up to) 4x
-- bigger than the others.
-- It needs to be completed before another run comes in.
creditsForMerge (Merging MergePolicyLevelling _) =
    return $ (1 + 4) / 1

-- A tiering merge has 5 runs at most (once could be held back to merged again)
-- and must be completed before the level is full (once 4 more runs come in).
creditsForMerge (Merging MergePolicyTiering (MergingRun _ ref)) = do
    readSTRef ref >>= \case
      CompletedMerge _ -> return 0
      OngoingMerge _ rs _ -> do
        let numRuns = length rs
        assertST $ numRuns `elem` [4, 5]
        return $ fromIntegral numRuns / 4

increment :: forall s. Tracer (ST s) Event
          -> Counter -> Run -> Levels s -> ST s (Levels s)
increment tr sc = \r ls -> do
    go 1 [r] ls
  where
    go :: Int -> [Run] -> Levels s -> ST s (Levels s)
    go !ln incoming [] = do
        let mergePolicy = mergePolicyForLevel ln []
        traceWith tr' AddLevelEvent
        ir <- newLevelMerge tr' ln mergePolicy MergeLastLevel incoming
        return (Level ir [] : [])
      where
        tr' = contramap (EventAt sc ln) tr

    go !ln incoming (Level ir rs : ls) = do
      r <- case ir of
        Single r      -> return r
        Merging mp mr -> expectCompletedMerge tr' mp mr
      let resident = r:rs
      case mergePolicyForLevel ln ls of

        -- If r is still too small for this level then keep it and merge again
        -- with the incoming runs.
        MergePolicyTiering | tieringRunSizeToLevel r < ln -> do
          let mergeLast = mergeLastForLevel ls
          ir' <- newLevelMerge tr' ln MergePolicyTiering mergeLast (incoming ++ [r])
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
          let mergeLast = mergeLastForLevel ls
          ir' <- newLevelMerge tr' ln MergePolicyTiering mergeLast incoming
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
          ir' <- newLevelMerge tr' ln MergePolicyLevelling MergeLastLevel
                          (incoming ++ [r])
          return (Level ir' [] : [])

      where
        tr' = contramap (EventAt sc ln) tr

-- | Only based on run count, not their sizes.
tieringLevelIsFull :: Int -> [Run] -> [Run] -> Bool
tieringLevelIsFull _ln _incoming resident = length resident >= 4

-- | The level is only considered full once the resident run is /too large/ for
-- the level.
levellingLevelIsFull :: Int -> [Run] -> Run -> Bool
levellingLevelIsFull ln _incoming resident = levellingRunSizeToLevel resident > ln

-------------------------------------------------------------------------------
-- Measurements
--

allLayers :: LSM s -> ST s (Buffer, [[Run]])
allLayers (LSMHandle _ lsmr) = do
    LSMContent wb ls <- readSTRef lsmr
    rs <- flattenLevels ls
    return (wb, rs)

flattenLevels :: Levels s -> ST s [[Run]]
flattenLevels = mapM flattenLevel

flattenLevel :: Level s -> ST s [Run]
flattenLevel (Level ir rs) = (++rs) <$> flattenIncomingRun ir

flattenIncomingRun :: IncomingRun s -> ST s [Run]
flattenIncomingRun (Single r) = return [r]
flattenIncomingRun (Merging _ (MergingRun _ ref)) = do
    mrs <- readSTRef ref
    case mrs of
      CompletedMerge r    -> return [r]
      OngoingMerge _ rs _ -> return rs

logicalValue :: LSM s -> ST s (Map Key (Value, Maybe Blob))
logicalValue lsm = do
    (wb, layers) <- allLayers lsm
    return $ Map.mapMaybe justInsert $ Map.unionsWith combine (bufferToRun wb : concat layers)
  where
    justInsert (Insert v b) = Just (v, b)
    justInsert  Delete      = Nothing
    justInsert (Mupsert v)  = Just (v, Nothing)

dumpRepresentation :: LSM s
                   -> ST s [(Maybe (MergePolicy, MergeType, MergingRunState), [Run])]
dumpRepresentation (LSMHandle _ lsmr) = do
    LSMContent wb ls <- readSTRef lsmr
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
         mergeDebt     :: MergeDebt,
         mergeCost     :: Int,
         mergeRunsSize :: [Int]
       }
     | MergeCompletedEvent {
         mergePolicy :: MergePolicy,
         mergeType   :: MergeType,
         mergeSize   :: Int
       }
  deriving stock Show
