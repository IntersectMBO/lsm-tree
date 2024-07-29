-- | A prototype of an LSM with explicitly scheduled incremental merges.
--
-- The scheduled incremental merges is about ensuring that the merging
-- work (CPU and I\/O) can be spread out over time evenly. This also means
-- the LSM update operations have worst case complexity rather than amortised
-- complexity, because they do a fixed amount of merging work each.
--
-- The other thing this prototype demonstrates is a design for duplicating
-- LSM handles and sharing ongoing incremental merges.
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
    Key, Value, Blob,
    new,
    LookupResult (..),
    lookup, lookups,
    Update (..),
    update, updates,
    insert, inserts,
    delete, deletes,
    supply,
    duplicate,

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
import           Data.Foldable (for_, toList)
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import           Data.STRef

import           Control.Exception (assert)
import           Control.Monad.ST
import           Control.Tracer (Tracer, contramap, traceWith)
import           GHC.Stack (HasCallStack, callStack)

import           Database.LSMTree.Normal (LookupResult (..), Update (..))


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
       -- | A level with a sequence of runs at this level, prefixed by
       -- a sequence of incoming run or runs that are being merged, with the
       -- result run to live at this level.
       Level !(MergingRun s) ![Run]

-- | The merge policy for a LSM level can be either tiering or levelling.
-- In this design we use levelling for the last level, and tiering for
-- all other levels. The first level always uses tiering however, even if
-- it's also the last level. So 'MergePolicy' and 'MergeLastLevel' are
-- orthogonal, all combinations are possible.
--
data MergePolicy = MergePolicyTiering | MergePolicyLevelling
  deriving stock (Eq, Show)

-- | A last level merge behaves differenrly from a mid-level merge: last level
-- merges can actually remove delete operations, whereas mid-level merges must
-- preserve them. This is orthogonal to the 'MergePolicy'.
--
data MergeLastLevel = MergeMidLevel | MergeLastLevel
  deriving stock (Eq, Show)

-- | A \"merging run\" is the representation of an ongoing incremental merge,
-- and in mutable. It is also a unit of sharing between duplicated LSM handles.
--
data MergingRun s = MergingRun !MergePolicy !MergeLastLevel
                               !(STRef s MergingRunState)
                  | SingleRun  !Run
                    -- ^ We represent single runs specially, rather than
                    -- putting them in as a 'CompletedMerge'. This is for two
                    -- reasons: to see statically that it's a single run
                    -- without having to read the 'STRef', and secondly
                    -- to make it easier to avoid supplying merge credits.
                    -- It's not essential, but simplifies things somewhat.

data MergingRunState = CompletedMerge !Run

                       -- let r = merge4 r1 r2 r3 r4
                       --  in OngoingMerge 0 [r1, r2, r3, r4] r
                     | OngoingMerge !MergeDebt ![Run] Run
  deriving stock Show

type Credit = Int
type Debt   = Int

type Run    = Map Key Op
type Buffer = Map Key Op

type Op     = Update Value Blob

type Key    = Int
type Value  = Int
type Blob   = Int


-- | The size of the 4 tiering runs at each level are allowed to be:
-- @4^(level-1) < size <= 4^level@
--
tieringRunSize :: Int -> Int
tieringRunSize n = 4^n

-- | Levelling runs take up the whole level, so are 4x larger.
--
levellingRunSize :: Int -> Int
levellingRunSize n = 4^(n+1)

tieringLevel :: Int -> Int
tieringLevel s
  | s <= bufferSize = 1  -- level numbers start at 1
  | otherwise =
    1 + (finiteBitSize s - countLeadingZeros (s-1) - 1) `div` 2

levellingLevel :: Int -> Int
levellingLevel s = max 1 (tieringLevel s - 1)  -- level numbers start at 1

tieringRunSizeToLevel :: Run -> Int
tieringRunSizeToLevel = tieringLevel . Map.size

levellingRunSizeToLevel :: Run -> Int
levellingRunSizeToLevel = levellingLevel . Map.size

bufferSize :: Int
bufferSize = tieringRunSize 1 -- 4

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

    go !ln (Level mr rs : ls) = do

      mrs <- case mr of
               SingleRun r        -> return (CompletedMerge r)
               MergingRun _ _ ref -> readSTRef ref

      assertST $ case mr of
        SingleRun{}        -> True
        MergingRun mp ml _ -> mergePolicyForLevel ln ls == mp
                           && mergeLastForLevel ls == ml
      assertST $ length rs <= 3
      expectedRunLengths ln rs ls
      expectedMergingRunLengths ln mr mrs ls

      go (ln+1) ls

    -- All runs within a level "proper" (as opposed to the incoming runs
    -- being merged) should be of the correct size for the level.
    expectedRunLengths :: Int -> [Run] -> [Level s] -> ST s ()
    expectedRunLengths ln rs ls =
      case mergePolicyForLevel ln ls of
        -- Levels using levelling have only one run, and that single run is
        -- (almost) always involved in an ongoing merge. Thus there are no
        -- other "normal" runs. The exception is when a levelling run becomes
        -- too large and is promoted, in that case initially there's no merge,
        -- but it is still represented as a 'MergingRun', using 'SingleRun'.
        MergePolicyLevelling -> assertST $ null rs
        -- Runs in tiering levels fit that size.
        MergePolicyTiering   -> assertST $ all (\r -> tieringRunSizeToLevel r == ln) rs

    -- Incoming runs being merged also need to be of the right size, but the
    -- conditions are more complicated.
    expectedMergingRunLengths :: Int -> MergingRun s -> MergingRunState
                              -> [Level s] -> ST s ()
    expectedMergingRunLengths ln mr mrs ls =
      case mergePolicyForLevel ln ls of
        MergePolicyLevelling ->
          assert (mergeLastForLevel ls == MergeLastLevel) $
          case (mr, mrs) of
            -- A single incoming run (which thus didn't need merging) must be
            -- of the expected size range already
            (SingleRun r, m) -> do
              assertST $ case m of CompletedMerge{} -> True
                                   OngoingMerge{}   -> False
              assertST $ levellingRunSizeToLevel r == ln

            -- A completed merge for levelling can be of almost any size at all!
            -- It can be smaller, due to deletions in the last level. But it
            -- can't be bigger than would fit into the next level.
            (_, CompletedMerge r) ->
              assertST $ levellingRunSizeToLevel r <= ln+1

            -- An ongoing merge for levelling should have 4 incoming runs of
            -- the right size for the level below (or slightly smaller),
            -- and 1 run from this level, but the run from this level can be of
            -- almost any size for the same reasons as above.
            -- Although if this is the first merge for a new level, it'll have
            -- only 4 runs.
            (_, OngoingMerge _ rs _) -> do
              let incoming = take 4 rs
              let resident = drop 4 rs
              assertST $ length incoming == 4
              assertST $ length resident <= 1
              assertST $ all (\r -> tieringRunSizeToLevel r == ln-1) incoming
              assertST $ all (\r -> levellingRunSizeToLevel r <= ln+1) resident

        MergePolicyTiering ->
          case (mr, mrs, mergeLastForLevel ls) of
            -- A single incoming run (which thus didn't need merging) must be
            -- of the expected size already
            (SingleRun r, m, _) -> do
              assertST $ case m of CompletedMerge{} -> True
                                   OngoingMerge{}   -> False
              assertST $ tieringRunSizeToLevel r == ln

            -- A completed last level run can be of almost any smaller size due
            -- to deletions, but it can't be bigger than the next level down.
            -- Note that tiering on the last level only occurs when there is
            -- a single level only.
            (_, CompletedMerge r, MergeLastLevel) -> do
              assertST $ ln == 1
              assertST $ tieringRunSizeToLevel r <= ln

            -- A completed mid level run is usually of the size for the
            -- level it is entering, but can also be one smaller (in which case
            -- it'll be held back and merged again).
            (_, CompletedMerge r, MergeMidLevel) ->
              assertST $ tieringRunSizeToLevel r `elem` [ln-1, ln]

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

newMerge :: Tracer (ST s) EventDetail
         -> Int -> MergePolicy -> MergeLastLevel
         -> [Run] -> ST s (MergingRun s)
newMerge _ _ _ _ [r] = return (SingleRun r)
newMerge tr level mergepolicy mergelast rs = do
    traceWith tr MergeStartedEvent {
                   mergePolicy   = mergepolicy,
                   mergeLast     = mergelast,
                   mergeDebt     = debt,
                   mergeCost     = cost,
                   mergeRunsSize = map Map.size rs
                 }
    assert (length rs >= 4) $
      assert (mergeDebtLeft debt >= cost) $
        MergingRun mergepolicy mergelast <$> newSTRef (OngoingMerge debt rs r)
  where
    cost = sum (map Map.size rs)
    -- How much we need to discharge before the merge can be guaranteed
    -- complete. More precisely, this is the maximum amount a merge at this
    -- level could need. This overestimation means that merges will only
    -- complete at the last possible moment.
    -- Note that for levelling this is includes the single run in the current
    -- level.
    debt = newMergeDebt $ case mergepolicy of
             MergePolicyLevelling -> 4 * tieringRunSize (level-1)
                                       + levellingRunSize level
             MergePolicyTiering   -> 4 * tieringRunSize (level-1)
    -- deliberately lazy:
    r    = case mergelast of
             MergeMidLevel  ->                (mergek rs)
             MergeLastLevel -> lastLevelMerge (mergek rs)

mergek :: [Run] -> Run
mergek = Map.unions

lastLevelMerge :: Run -> Run
lastLevelMerge = Map.filter isInsert
  where
    isInsert Insert{} = True
    isInsert Delete   = False

expectCompletedMerge :: HasCallStack
                     => Tracer (ST s) EventDetail
                     -> MergingRun s -> ST s Run
expectCompletedMerge _  (SingleRun r) = return r
expectCompletedMerge tr (MergingRun mergepolicy mergelast ref) = do
    mrs <- readSTRef ref
    case mrs of
      CompletedMerge r -> do
        traceWith tr MergeCompletedEvent {
            mergePolicy  = mergepolicy,
            mergeLast    = mergelast,
            mergeSize    = Map.size r
          }
        return r
      OngoingMerge d _ _ ->
        error $ "expectCompletedMerge: false expectation, remaining debt of "
             ++ show d

supplyMergeCredits :: Credit -> MergingRun s -> ST s ()
supplyMergeCredits _ SingleRun{} = return ()
supplyMergeCredits c (MergingRun _ _ ref) = do
    mrs <- readSTRef ref
    case mrs of
      CompletedMerge{} -> return ()
      OngoingMerge d rs r ->
        case paydownMergeDebt c d of
          MergeDebtDischarged _ ->
            writeSTRef ref (CompletedMerge r)

          MergeDebtPaydownCredited  d' ->
            writeSTRef ref (OngoingMerge d' rs r)

          MergeDebtPaydownPerform _ d' ->
            -- we're not doing any actual merging
            -- just tracking what we would do
            writeSTRef ref (OngoingMerge d' rs r)


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
    -- | This remaining merge debt is fully paid off with credits.
    MergeDebtDischarged      !Debt
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
  = MergeDebtDischarged d

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


inserts :: Tracer (ST s) Event -> LSM s -> [(Key, Value)] -> ST s ()
inserts tr lsm kvs = updates tr lsm [ (k, Insert v Nothing) | (k,v) <- kvs ]

insert :: Tracer (ST s) Event -> LSM s -> Key -> Value -> ST s ()
insert tr lsm k v = update tr lsm k (Insert v Nothing)

delete :: Tracer (ST s) Event -> LSM s -> Key ->  ST s ()
delete tr lsm k = update tr lsm k Delete

deletes :: Tracer (ST s) Event -> LSM s -> [Key] ->  ST s ()
deletes tr lsm ks = updates tr lsm [ (k, Delete) | k <- ks ]

updates :: Tracer (ST s) Event -> LSM s -> [(Key, Op)] -> ST s ()
updates tr lsm = mapM_ (uncurry (update tr lsm))

update :: Tracer (ST s) Event -> LSM s -> Key -> Op -> ST s ()
update tr (LSMHandle scr lsmr) k op = do
    sc <- readSTRef scr
    LSMContent wb ls <- readSTRef lsmr
    modifySTRef' scr (+1)
    supplyCredits 1 ls
    let wb' = Map.insert k op wb
    if Map.size wb' >= bufferSize
      then do
        ls' <- increment tr sc (bufferToRun wb') ls
        writeSTRef lsmr (LSMContent Map.empty ls')
      else
        writeSTRef lsmr (LSMContent wb' ls)

supply :: LSM s -> Credit -> ST s ()
supply (LSMHandle scr lsmr) credits = do
    LSMContent _ ls <- readSTRef lsmr
    modifySTRef' scr (+1)
    supplyCredits credits ls
    invariant ls

lookups :: LSM s -> [Key] -> ST s [(Key, LookupResult Value Blob)]
lookups lsm = mapM (\k -> (k,) <$> lookup lsm k)

lookup :: LSM s -> Key -> ST s (LookupResult Value Blob)
lookup lsm k = do
    rss <- allLayers lsm
    return $!
      foldr (\lookures continue ->
              case lookures of
                Nothing                  -> continue
                Just (Insert v Nothing)  -> Found v
                Just (Insert v (Just b)) -> FoundWithBlob v b
                Just  Delete             -> NotFound)
            NotFound
            [ Map.lookup k r | rs <- rss, r <- rs ]

bufferToRun :: Buffer -> Run
bufferToRun = id

supplyCredits :: Credit -> Levels s -> ST s ()
supplyCredits n ls =
  sequence_
    [ supplyMergeCredits (ceiling (fromIntegral n * creditsForMerge mr)) mr
    | Level mr _rs <- ls
    ]

-- | The general case (and thus worst case) of how many merge credits we need
-- for a level. This is based on the merging policy at the level.
--
creditsForMerge :: MergingRun s -> Float
creditsForMerge SingleRun{}                           = 0

-- A levelling merge has 1 input run and one resident run, which is (up to) 4x
-- bigger than the others.
-- It needs to be completed before another run comes in.
creditsForMerge (MergingRun MergePolicyLevelling _ _) = (1 + 4) / 1

-- A tiering merge has 4 runs at most (once could be held back to merged again)
-- and must be completed before the level is full (once 3 more runs come in,
-- as it could have started out with an additional refused run).
-- TODO: We could only increase the merging speed for the merges where this
-- applies, which should be rare.
creditsForMerge (MergingRun MergePolicyTiering   _ _) = 4 / 3

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
         mergeLast     :: MergeLastLevel,
         mergeDebt     :: MergeDebt,
         mergeCost     :: Int,
         mergeRunsSize :: [Int]
       }
     | MergeCompletedEvent {
         mergePolicy :: MergePolicy,
         mergeLast   :: MergeLastLevel,
         mergeSize   :: Int
       }
  deriving stock Show

increment :: forall s. Tracer (ST s) Event
          -> Counter -> Run -> Levels s -> ST s (Levels s)
increment tr sc = \r ls -> do
    (ls', refused) <- go 1 [r] ls
    assertST $ null refused
    invariant ls'
    return ls'
  where
    go, go' :: Int -> [Run] -> Levels s -> ST s (Levels s, Maybe Run)
    go !ln incoming ls = do
        case incoming of
          [r] -> do
            assertST $ tieringRunSizeToLevel r `elem` [ln, ln+1]  -- +1 from levelling
          _ -> do
            assertST $ length incoming == 4
            assertST $ all (\r -> tieringRunSizeToLevel r == ln-1) incoming
            assertST $ tieringLevel (sum (map Map.size incoming)) == ln
        (ls', refused) <- go' ln incoming ls
        for_ refused $ assertST . (== head incoming)
        return (ls', refused)

    go' !ln incoming [] = do
        let mergepolicy = mergePolicyForLevel ln []
        traceWith tr' AddLevelEvent
        mr <- newMerge tr' ln mergepolicy MergeLastLevel incoming
        return (Level mr [] : [], Nothing)
      where
        tr' = contramap (EventAt sc ln) tr

    go' !ln incoming (Level mr rs : ls) = do
      r <- expectCompletedMerge tr' mr
      let resident = r:rs
      case mergePolicyForLevel ln ls of

        -- If r is still too small for this level then keep it and merge again
        -- with the incoming runs, but only if the resulting run is guaranteed
        -- not to be too large for this level.
        -- If it might become too large, only create a 4-way merge and refuse
        -- the most recent of the incoming runs.
        MergePolicyTiering | tieringRunSizeToLevel r < ln ->
          if sum (map Map.size (r : incoming)) <= tieringRunSize ln
          then do
            let mergelast = mergeLastForLevel ls
            mr' <- newMerge tr' ln MergePolicyTiering mergelast (incoming ++ [r])
            return (Level mr' rs : ls, Nothing)
          else do
            -- TODO: comment
            let mergelast = mergeLastForLevel ls
            mr' <- newMerge tr' ln MergePolicyTiering mergelast (tail incoming ++ [r])
            return (Level mr' rs : ls, Just (head incoming))

        -- This tiering level is now full. We take the completed merged run
        -- (the previous incoming runs), plus all the other runs on this level
        -- as a bundle and move them down to the level below. We start a merge
        -- for the new incoming runs. This level is otherwise empty.
        MergePolicyTiering | tieringLevelIsFull ln incoming resident -> do
          mr' <- newMerge tr' ln MergePolicyTiering MergeMidLevel incoming
          (ls', refused) <- go (ln+1) resident ls
          return (Level mr' (toList refused) : ls', Nothing)

        -- This tiering level is not yet full. We move the completed merged run
        -- into the level proper, and start the new merge for the incoming runs.
        MergePolicyTiering -> do
          let mergelast = mergeLastForLevel ls
          mr' <- newMerge tr' ln MergePolicyTiering mergelast incoming
          traceWith tr' (AddRunEvent (length resident))
          return (Level mr' resident : ls, Nothing)

        -- The final level is using levelling. If the existing completed merge
        -- run is too large for this level, we promote the run to the next
        -- level and start merging the incoming runs into this (otherwise
        -- empty) level .
        MergePolicyLevelling | levellingLevelIsFull ln incoming r -> do
          assert (null rs && null ls) $ return ()
          mr' <- newMerge tr' ln MergePolicyTiering MergeMidLevel incoming
          (ls', refused) <- go (ln+1) [r] []
          return (Level mr' (toList refused) : ls', Nothing)

        -- Otherwise we start merging the incoming runs into the run.
        MergePolicyLevelling -> do
          assert (null rs && null ls) $ return ()
          mr' <- newMerge tr' ln MergePolicyLevelling MergeLastLevel
                          (incoming ++ [r])
          return (Level mr' [] : [], Nothing)

      where
        tr' = contramap (EventAt sc ln) tr

-- | The level is considered full if adding the incoming runs could result in a
-- merge that's too large for the next level.
-- For perfectly full runs, this is exactly when there are 4 resident runs.
tieringLevelIsFull :: Int -> [Run] -> [Run] -> Bool
tieringLevelIsFull ln incoming resident =
    -- (the same as @levellingRunSize ln@)
    sum (map Map.size (incoming ++ resident)) > tieringRunSize (ln+1)

-- | The level is only considered full once the resident run is /too large/ for
-- the level.
levellingLevelIsFull :: Int -> [Run] -> Run -> Bool
levellingLevelIsFull ln _incoming resident = levellingRunSizeToLevel resident > ln

duplicate :: LSM s -> ST s (LSM s)
duplicate (LSMHandle _scr lsmr) = do
    scr'  <- newSTRef 0
    lsmr' <- newSTRef =<< readSTRef lsmr
    return (LSMHandle scr' lsmr')
    -- it's that simple here, because we share all the pure value and all the
    -- STRefs and there's no ref counting to be done


-------------------------------------------------------------------------------
-- Measurements
--

allLayers :: LSM s -> ST s [[Run]]
allLayers (LSMHandle _ lsmr) = do
    LSMContent wb ls <- readSTRef lsmr
    rs <- flattenLevels ls
    return ([wb] : rs)

flattenLevels :: Levels s -> ST s [[Run]]
flattenLevels = mapM flattenLevel

flattenLevel :: Level s -> ST s [Run]
flattenLevel (Level mr rs) = (++rs) <$> flattenMergingRun mr

flattenMergingRun :: MergingRun s -> ST s [Run]
flattenMergingRun (SingleRun r) = return [r]
flattenMergingRun (MergingRun _ _ mr) = do
    mrs <- readSTRef mr
    case mrs of
      CompletedMerge r    -> return [r]
      OngoingMerge _ rs _ -> return rs

logicalValue :: LSM s -> ST s (Map Key Value)
logicalValue = fmap (Map.mapMaybe justInsert . Map.unions . concat)
             . allLayers
  where
    justInsert (Insert v _) = Just v
    justInsert  Delete      = Nothing

dumpRepresentation :: LSM s
                   -> ST s [(Maybe (MergePolicy, MergeLastLevel, MergingRunState), [Run])]
dumpRepresentation (LSMHandle _ lsmr) = do
    LSMContent wb ls <- readSTRef lsmr
    ((Nothing, [wb]) :) <$> mapM dumpLevel ls

dumpLevel :: Level s -> ST s (Maybe (MergePolicy, MergeLastLevel, MergingRunState), [Run])
dumpLevel (Level (SingleRun r) rs) =
    return (Nothing, (r:rs))
dumpLevel (Level (MergingRun mp ml mr) rs) = do
    mrs <- readSTRef mr
    return (Just (mp, ml, mrs), rs)

representationShape :: [(Maybe (MergePolicy, MergeLastLevel, MergingRunState), [Run])]
                    -> [(Maybe (MergePolicy, MergeLastLevel, Either Int [Int]), [Int])]
representationShape =
    map $ \(mmr, rs) ->
      ( fmap (\(mp, ml, mrs) -> (mp, ml, summaryMRS mrs)) mmr
      , map summaryRun rs)
  where
    summaryRun = Map.size
    summaryMRS (CompletedMerge r)    = Left (summaryRun r)
    summaryMRS (OngoingMerge _ rs _) = Right (map summaryRun rs)

