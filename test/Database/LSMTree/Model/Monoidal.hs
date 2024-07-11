{-# LANGUAGE RoleAnnotations          #-}
{-# LANGUAGE ScopedTypeVariables      #-}
{-# LANGUAGE StandaloneDeriving       #-}
{-# LANGUAGE StandaloneKindSignatures #-}
{-# LANGUAGE TupleSections            #-}
{-# LANGUAGE TypeApplications         #-}
{-# LANGUAGE TypeFamilies             #-}

-- lookup has redundant update constraint.
{-# OPTIONS_GHC -Wno-redundant-constraints #-}

-- |
--
-- This module is intended to be imported qualified.
--
-- > import qualified Database.LSMTree.Monoidal as LSMT
module Database.LSMTree.Model.Monoidal (
    -- * Serialisation
    SerialiseKey (..)
  , SerialiseValue (..)
    -- * Temporary placeholder types
  , SomeUpdateConstraint (..)
    -- * Tables
  , Table
  , empty
    -- * Table querying and updates
    -- ** Queries
  , Range
  , LookupResult (..)
  , lookups
  , RangeLookupResult (..)
  , rangeLookup
    -- ** Updates
  , Update (..)
  , updates
  , inserts
  , deletes
  , mupserts
    -- * Snapshots
  , snapshot
    -- * Multiple writable table handles
  , duplicate
    -- * Merging tables
  , merge
  ) where

import           Data.Bifunctor (second)
import qualified Data.ByteString as BS
import           Data.Foldable (foldl')
import           Data.Map (Map)
import qualified Data.Map.Range as Map.R
import qualified Data.Map.Strict as Map
import           Database.LSMTree.Common (Range (..), SerialiseKey (..),
                     SerialiseValue (..), SomeUpdateConstraint (..))
import           Database.LSMTree.Internal.RawBytes (RawBytes)
import           Database.LSMTree.Monoidal (LookupResult (..),
                     RangeLookupResult (..), Update (..))
import           GHC.Exts (IsList (..))

{-------------------------------------------------------------------------------
  Tables
-------------------------------------------------------------------------------}

data Table k v = Table
    { _values :: Map RawBytes RawBytes
    }

type role Table nominal nominal

-- | An empty table.
empty :: Table k v
empty = Table Map.empty

-- | This instance is for testing and debugging only.
instance (SerialiseKey k, SerialiseValue v) => IsList (Table k v) where
    type Item (Table k v) = (k, v)
    fromList xs = Table $ Map.fromList
        [ (serialiseKey k, serialiseValue v)
        | (k, v) <- xs
        ]

    toList (Table m) =
        [ (deserialiseKey k, deserialiseValue v)
        | (k, v) <- Map.toList m
        ]

-- | This instance is for testing and debugging only.
instance Show (Table k v) where
    showsPrec d (Table tbl) = showParen (d > 10)
        $ showString "fromList "
        . showsPrec 11 (toList (Table @BS.ByteString @BS.ByteString tbl))

-- | This instance is for testing and debugging only.
deriving stock instance Eq (Table k v)

{-------------------------------------------------------------------------------
  Table querying and updates
-------------------------------------------------------------------------------}

-- | Perform a batch of lookups.
--
-- Lookups can be performed concurrently from multiple Haskell threads.
lookups ::
     (SerialiseKey k, SerialiseValue v, SomeUpdateConstraint v)
  => [k]
  -> Table k v
  -> [LookupResult k v]
lookups ks tbl =
    [ case Map.lookup (serialiseKey k) (_values tbl) of
        Nothing -> NotFound k
        Just v  -> Found k (deserialiseValue v)
    | k <- ks
    ]

-- | Perform a range lookup.
--
-- Range lookups can be performed concurrently from multiple Haskell threads.
rangeLookup :: forall k v.
     (SerialiseKey k, SerialiseValue v, SomeUpdateConstraint v)
  => Range k
  -> Table k v
  -> [RangeLookupResult k v]
rangeLookup r tbl =
    [ FoundInRange (deserialiseKey k) (deserialiseValue v)
    | let (lb, ub) = convertRange r
    , (k, v) <- Map.R.rangeLookup lb ub (_values tbl)
    ]
  where
    convertRange :: Range k -> (Map.R.Bound RawBytes, Map.R.Bound RawBytes)
    convertRange (FromToExcluding lb ub) =
        ( Map.R.Bound (serialiseKey lb) Map.R.Inclusive
        , Map.R.Bound (serialiseKey ub) Map.R.Exclusive )
    convertRange (FromToIncluding lb ub) =
        ( Map.R.Bound (serialiseKey lb) Map.R.Inclusive
        , Map.R.Bound (serialiseKey ub) Map.R.Inclusive )

-- | Perform a mixed batch of inserts, deletes and monoidal upserts.
--
-- Updates can be performed concurrently from multiple Haskell threads.
updates :: forall k v.
     (SerialiseKey k, SerialiseValue v, SomeUpdateConstraint v)
  => [(k, Update v)]
  -> Table k v
  -> Table k v
updates ups tbl0 = foldl' update tbl0 ups where
    update :: Table k v -> (k, Update v) -> Table k v
    update tbl (k, Delete) = tbl
        { _values = Map.delete (serialiseKey k) (_values tbl) }
    update tbl (k, Insert v) = tbl
        { _values = Map.insert (serialiseKey k) (serialiseValue v) (_values tbl) }
    update tbl (k, Mupsert v) = tbl
        { _values = mapUpsert (serialiseKey k) (serialiseValue v) f (_values tbl) }
      where
        f old = serialiseValue (mergeU v (deserialiseValue old))

mapUpsert :: Ord k => k -> v -> (v -> v) -> Map k v -> Map k v
mapUpsert k v f = Map.alter (Just . g) k where
    g Nothing   = v
    g (Just v') = f v'

-- | Perform a batch of inserts.
--
-- Inserts can be performed concurrently from multiple Haskell threads.
inserts ::
     (SerialiseKey k, SerialiseValue v, SomeUpdateConstraint v)
  => [(k, v)]
  -> Table k v
  -> Table k v
inserts = updates . fmap (second Insert)

-- | Perform a batch of deletes.
--
-- Deletes can be performed concurrently from multiple Haskell threads.
deletes ::
     (SerialiseKey k, SerialiseValue v, SomeUpdateConstraint v)
  => [k]
  -> Table k v
  -> Table k v
deletes = updates . fmap (,Delete)

-- | Perform a batch of monoidal upserts.
--
-- Monoidal upserts can be performed concurrently from multiple Haskell threads.
mupserts ::
     (SerialiseKey k, SerialiseValue v, SomeUpdateConstraint v)
  => [(k, v)]
  -> Table k v
  -> Table k v
mupserts = updates . fmap (second Mupsert)

{-------------------------------------------------------------------------------
  Snapshots
-------------------------------------------------------------------------------}

snapshot ::
     Table k v
  -> Table k v
snapshot = id

{-------------------------------------------------------------------------------
  Mutiple writable table handles
-------------------------------------------------------------------------------}

duplicate ::
     Table k v
  -> Table k v
duplicate = id

{-------------------------------------------------------------------------------
  Merging tables
-------------------------------------------------------------------------------}

-- | Merge full tables, creating a new table handle.
--
-- NOTE: close table handles using 'close' as soon as they are
-- unused.
--
-- Multiple tables of the same type but with different configuration parameters
-- can live in the same session. However, some operations, like
merge :: forall k v.
     (SerialiseValue v, SomeUpdateConstraint v)
  => Table k v
  -> Table k v
  -> Table k v
merge (Table xs) (Table ys) =
    Table (Map.unionWith f xs ys)
  where
    f x y = serialiseValue (mergeU @v (deserialiseValue x) (deserialiseValue y))
