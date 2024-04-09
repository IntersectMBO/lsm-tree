{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE PatternSynonyms     #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Data.Map.Range (
    Bound (.., BoundExclusive, BoundInclusive)
  , Clusive (..)
  , rangeLookup
  ) where

import           Data.Map (Map)
import qualified Data.Map as Map
import           Data.Map.Internal (Map (..))

data Clusive = Exclusive | Inclusive deriving Show
data Bound k = NoBound | Bound !k !Clusive deriving Show

{-# COMPLETE BoundExclusive, BoundInclusive #-}

pattern BoundExclusive :: k -> Bound k
pattern BoundExclusive k = Bound k Exclusive

pattern BoundInclusive :: k -> Bound k
pattern BoundInclusive k = Bound k Inclusive

-- | Find all the keys in the given range and return the corresponding
-- (key, value) pairs (in ascending order).
--
rangeLookup
    :: forall k v. Ord k
    => Bound k    -- ^ lower bound
    -> Bound k    -- ^ upper bound
    -> Map k v
    -> [(k, v)]
rangeLookup NoBound       NoBound        m = Map.toList m
rangeLookup (Bound lb lc) NoBound        m = rangeLookupLo lb lc m []
rangeLookup NoBound       (Bound ub uc)  m = rangeLookupHi ub uc m []
rangeLookup (Bound lb lc) (Bound ub uc)  m = rangeLookupBoth lb lc ub uc m []

toDList :: Map k v -> [(k, v)] -> [(k, v)]
toDList Tip             = id
toDList (Bin _ k v l r) = toDList l . ((k,v):) . toDList r

rangeLookupLo :: Ord k => k -> Clusive -> Map k v -> [(k, v)] -> [(k, v)]
rangeLookupLo !_  !_  Tip = id
rangeLookupLo  lb  lc (Bin _ k v l r)
    -- ... | --- k -----
    | evalLowerBound lb lc k
    = rangeLookupLo lb lc l . ((k, v) :) . toDList r

    -- ... k ... |--------
    | otherwise
    = rangeLookupLo lb lc r

rangeLookupHi :: Ord k => k -> Clusive -> Map k v -> [(k, v)] -> [(k, v)]
rangeLookupHi !_  !_  Tip = id
rangeLookupHi  ub  uc (Bin _ k v l r)
    -- --- k --- | ...
    | evalUpperBound ub uc k
    = toDList l . ((k, v) :) . rangeLookupHi ub uc r

    -- --------- | ... k ...
    | otherwise
    = rangeLookupHi ub uc l

rangeLookupBoth :: Ord k => k -> Clusive -> k -> Clusive -> Map k v -> [(k, v)] -> [(k, v)]
rangeLookupBoth !_  !_  !_  !_  Tip = id
rangeLookupBoth  lb  lc  ub  uc (Bin _ k v l r)
    -- ... |--- k ---| ...
    | evalLowerBound lb lc k
    , evalUpperBound ub uc k
    = rangeLookupLo lb lc l . ((k,v):) . rangeLookupHi ub uc r

    -- ... |-------| ... k ...
    | evalLowerBound lb lc k
    = rangeLookupBoth lb lc ub uc l

    -- ... k ... |-------| ...
    | evalUpperBound ub uc k
    = rangeLookupBoth lb lc ub uc r

    | otherwise
    = id

evalLowerBound :: Ord k => k -> Clusive -> k -> Bool
evalLowerBound b Exclusive k = b < k
evalLowerBound b Inclusive k = b <= k

evalUpperBound :: Ord k => k -> Clusive -> k -> Bool
evalUpperBound b Exclusive k = k < b
evalUpperBound b Inclusive k = k <= b
