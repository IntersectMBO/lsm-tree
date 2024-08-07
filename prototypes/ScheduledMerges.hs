{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE EmptyCase           #-}
{-# LANGUAGE ScopedTypeVariables #-}

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
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import           Data.STRef

import           Control.Exception (assert)
import           Control.Monad.ST
import           Control.Tracer (Tracer, contramap, traceWith)
import           GHC.Stack (HasCallStack)

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

tieringRunSizeToLevel :: Run -> Int
tieringRunSizeToLevel r
  | s <= bufferSize = 1  -- level numbers start at 1
  | otherwise =
    1 + (finiteBitSize s - countLeadingZeros (s-1) - 1) `div` 2
  where
    s = Map.size r

levellingRunSizeToLevel :: Run -> Int
levellingRunSizeToLevel r =
    max 1 (tieringRunSizeToLevel r - 1)  -- level numbers start at 1

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
invariant :: forall s. Levels s -> ST s Bool
invariant = go 1
  where
    go :: Int -> [Level s] -> ST s Bool
    go !_ []     = return True

    go !ln (Level mr rs : ls) = do

      mrs <- case mr of
               SingleRun r        -> return (CompletedMerge r)
               MergingRun _ _ ref -> readSTRef ref

      assert (case mr of
                SingleRun{} -> True
                MergingRun mp ml _ -> mergePolicyForLevel ln ls == mp
                                   && mergeLastForLevel ls == ml)
        assert (length rs <= 3) $
        assert (expectedRunLengths ln rs ls) $
        assert (expectedMergingRunLengths ln mr mrs ls) $
        return ()

      go (ln+1) ls

    -- All runs within a level "proper" (as opposed to the incoming runs
    -- being merged) should be of the correct size for the level.
    expectedRunLengths :: Int -> [Run] -> [Level s] -> Bool
    expectedRunLengths ln rs ls =
      case mergePolicyForLevel ln ls of
        -- Levels using levelling have only one run, and that single run is
        -- (almost) always involved in an ongoing merge. Thus there are no
        -- other "normal" runs. The exception is when a levelling run becomes
        -- too large and is promoted, in that case initially there's no merge,
        -- but it is still represented as a 'MergingRun', using 'SingleRun'.
        MergePolicyLevelling -> null rs
        MergePolicyTiering   -> all (\r -> tieringRunSizeToLevel r == ln) rs

    -- Incoming runs being merged also need to be of the right size, but the
    -- conditions are more complicated.
    expectedMergingRunLengths :: Int -> MergingRun s -> MergingRunState
                              -> [Level s] -> Bool
    expectedMergingRunLengths ln mr mrs ls =
      case mergePolicyForLevel ln ls of
        MergePolicyLevelling ->
          case (mr, mrs) of
            -- A single incoming run (which thus didn't need merging) must be
            -- of the expected size range already
            (SingleRun r, CompletedMerge{}) ->
              assert (levellingRunSizeToLevel r == ln) True

            -- A completed merge for levelling can be of almost any size at all!
            -- It can be smaller, due to deletions in the last level. But it
            -- can't be bigger than would fit into the next level.
            (_, CompletedMerge r) ->
              assert (levellingRunSizeToLevel r <= ln+1) True

            -- An ongoing merge for levelling should have 4 incoming runs of
            -- the right size for the level below, and 1 run from this level,
            -- but the run from this level can be of almost any size for the
            -- same reasons as above. Although if this is the first merge for
            -- a new level, it'll have only 4 runs.
            (_, OngoingMerge _ rs _) ->
                assert (length rs == 4 || length rs == 5) True
             && assert (all (\r -> tieringRunSizeToLevel r == ln-1) (take 4 rs)) True
             && assert (all (\r -> levellingRunSizeToLevel r <= ln+1) (drop 4 rs)) True

        MergePolicyTiering ->
          case (mr, mrs, mergeLastForLevel ls) of
            -- A single incoming run (which thus didn't need merging) must be
            -- of the expected size already
            (SingleRun r, CompletedMerge{}, _) ->
              tieringRunSizeToLevel r == ln

            -- A completed last level run can be of almost any smaller size due
            -- to deletions, but it can't be bigger than the next level down.
            -- Note that tiering on the last level only occurs when there is
            -- a single level only.
            (_, CompletedMerge r, MergeLastLevel) ->
                ln == 1
             && tieringRunSizeToLevel r <= ln+1

            -- A completed mid level run is usually of the size for the
            -- level it is entering, but can also be one smaller (in which case
            -- it'll be held back and merged again).
            (_, CompletedMerge r, MergeMidLevel) ->
                rln == ln || rln == ln+1
              where
                rln = tieringRunSizeToLevel r

            -- An ongoing merge for tiering should have 4 incoming runs of
            -- the right size for the level below, and at most 1 run held back
            -- due to being too small (which would thus also be of the size of
            -- the level below).
            (_, OngoingMerge _ rs _, _) ->
                (length rs == 4 || length rs == 5)
             && all (\r -> tieringRunSizeToLevel r == ln-1) rs


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
    assert (let l = length rs in l >= 2 && l <= 5) $
      MergingRun mergepolicy mergelast <$> newSTRef (OngoingMerge debt rs r)
  where
    cost = sum (map Map.size rs)
    -- How much we need to discharge before the merge can be guaranteed
    -- complete.
    -- Note that for levelling this is includes the single run in the current
    -- level.
    debt = case mergepolicy of
             MergePolicyLevelling -> newMergeDebt (4 * tieringRunSize (level-1)
                                                 +     levellingRunSize level)
             MergePolicyTiering   -> newMergeDebt (4 * tieringRunSize (level-1))
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
    ok <- invariant ls
    assert ok $ return ()

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
    [ supplyMergeCredits (n * creditsForMerge mr) mr | Level mr _rs <- ls ]

-- | The general case (and thus worst case) of how many merge credits we need
-- for a level. This is based on the merging policy at the level.
--
creditsForMerge :: MergingRun s -> Credit
creditsForMerge SingleRun{}                           = 0

-- A levelling merge is 5x the cost of a tiering merge.
-- That's because for levelling one of the runs as an input to the merge
-- is the one levelling run which is (up to) 4x bigger than the others put
-- together, so it's 1 + 4.
creditsForMerge (MergingRun MergePolicyLevelling _ _) = 5
creditsForMerge (MergingRun MergePolicyTiering   _ _) = 1

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
    ls' <- go 1 [r] ls
    ok  <- invariant ls'
    assert ok (return ls')
  where
    go :: Int -> [Run] -> Levels s -> ST s (Levels s)
    go !ln rs [] = do
        let mergepolicy = mergePolicyForLevel ln []
        traceWith tr' AddLevelEvent
        mr <- newMerge tr' ln mergepolicy MergeLastLevel rs
        return (Level mr [] : [])
      where
        tr' = contramap (EventAt sc ln) tr

    go !ln rs' (Level mr rs : ls) = do
      r <- expectCompletedMerge tr' mr
      case mergePolicyForLevel ln ls of

        -- If r is still too small for this level then keep it and merge again
        -- with the incoming runs.
        MergePolicyTiering | tieringRunSizeToLevel r < ln -> do
          let mergelast = mergeLastForLevel ls
          mr' <- newMerge tr' ln MergePolicyTiering mergelast (rs' ++ [r])
          return (Level mr' rs : ls)

        -- This tiering level is now full. We take the completed merged run
        -- (the previous incoming runs), plus all the other runs on this level
        -- as a bundle and move them down to the level below. We start a merge
        -- for the new incoming runs. This level is otherwise empty.
        MergePolicyTiering | levelIsFull rs -> do
          mr' <- newMerge tr' ln MergePolicyTiering MergeMidLevel rs'
          ls' <- go (ln+1) (r:rs) ls
          return (Level mr' [] : ls')

        -- This tiering level is not yet full. We move the completed merged run
        -- into the level proper, and start the new merge for the incoming runs.
        MergePolicyTiering -> do
          let mergelast = mergeLastForLevel ls
          mr' <- newMerge tr' ln MergePolicyTiering mergelast rs'
          traceWith tr' (AddRunEvent (length (r:rs)))
          return (Level mr' (r:rs) : ls)

        -- The final level is using levelling. If the existing completed merge
        -- run is too large for this level, we promote the run to the next
        -- level and start merging the incoming runs into this (otherwise
        -- empty) level .
        MergePolicyLevelling | levellingRunSizeToLevel r > ln -> do
          assert (null rs && null ls) $ return ()
          mr' <- newMerge tr' ln MergePolicyTiering MergeMidLevel rs'
          ls' <- go (ln+1) [r] []
          return (Level mr' [] : ls')

        -- Otherwise we start merging the incoming runs into the run.
        MergePolicyLevelling -> do
          assert (null rs && null ls) $ return ()
          mr' <- newMerge tr' ln MergePolicyLevelling MergeLastLevel
                          (rs' ++ [r])
          return (Level mr' [] : [])

      where
        tr' = contramap (EventAt sc ln) tr

levelIsFull :: [Run] -> Bool
levelIsFull rs = length rs + 1 >= 4

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

