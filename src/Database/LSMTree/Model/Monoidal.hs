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
    -- * Temporary placeholder types
    SomeSerialisationConstraint (..)
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
  , mergeTables
  ) where

import           Data.Bifunctor (second)
import qualified Data.ByteString as BS
import           Data.Foldable (foldl')
import           Data.Map (Map)
import qualified Data.Map.Range as Map.R
import qualified Data.Map.Strict as Map
import           Database.LSMTree.Common (SomeSerialisationConstraint (..),
                     SomeUpdateConstraint (..))
import           Database.LSMTree.Normal (Range (..))
import           GHC.Exts (IsList (..))

{-------------------------------------------------------------------------------
  Tables
-------------------------------------------------------------------------------}

data Table k v = Table
    { _values :: Map BS.ByteString BS.ByteString
    }

type role Table nominal nominal

-- | An empty table.
empty :: Table k v
empty = Table Map.empty

-- | This instance is for testing and debugging only.
instance
    ( SomeSerialisationConstraint k
    , SomeSerialisationConstraint v
    ) => IsList (Table k v)
  where
    type Item (Table k v) = (k, v)
    fromList xs = Table $ Map.fromList
        [ (serialise k, serialise v)
        | (k, v) <- xs
        ]

    toList (Table m) =
        [ (deserialise k, deserialise v)
        | (k, v) <- Map.toList m
        ]

-- | This instance is for testing and debugging only.
instance Show (Table k v) where
    showsPrec d (Table tbl) = showParen (d > 10)
        $ showString "fromList "
        . showsPrec 11 (toList (Table @BS.ByteString @BS.ByteString tbl))

-- | This instance is for testing and debugging only.
deriving instance Eq (Table k v)

{-------------------------------------------------------------------------------
  Table querying and updates
-------------------------------------------------------------------------------}

-- | Result of a single point lookup.
data LookupResult k v =
    NotFound      !k
  | Found         !k !v
  deriving (Eq, Show)

-- | Perform a batch of lookups.
--
-- Lookups can be performed concurrently from multiple Haskell threads.
lookups ::
     ( SomeSerialisationConstraint k
     , SomeSerialisationConstraint v
     , SomeUpdateConstraint v
     )
  => [k]
  -> Table k v
  -> [LookupResult k v]
lookups ks tbl =
    [ case Map.lookup (serialise k) (_values tbl) of
        Nothing -> NotFound k
        Just v  -> Found k (deserialise v)
    | k <- ks
    ]

-- | A result for one point in a range lookup.
data RangeLookupResult k v =
    FoundInRange         !k !v
  deriving (Eq, Show)

-- | Perform a range lookup.
--
-- Range lookups can be performed concurrently from multiple Haskell threads.
rangeLookup :: forall k v.
     ( SomeSerialisationConstraint k
     , SomeSerialisationConstraint v
     , SomeUpdateConstraint v
     )
  => Range k
  -> Table k v
  -> [RangeLookupResult k v]
rangeLookup r tbl =
    [ FoundInRange (deserialise k) (deserialise v)
    | let (ub, lb) = convertRange r
    , (k, v) <- Map.R.rangeLookup lb ub (_values tbl)
    ]
  where
    convertRange :: Range k -> (Map.R.Bound BS.ByteString, Map.R.Bound BS.ByteString)
    convertRange (FromToExcluding lb ub) =
        ( Map.R.Bound (serialise lb) Map.R.Inclusive
        , Map.R.Bound (serialise ub) Map.R.Exclusive )
    convertRange (FromToIncluding lb ub) =
        ( Map.R.Bound (serialise lb) Map.R.Inclusive
        , Map.R.Bound (serialise ub) Map.R.Inclusive )

-- | Normal tables support insert, delete and monoidal upsert operations.
--
-- An __update__ is a term that groups all types of table-manipulating
-- operations, like inserts and deletes.
data Update v =
    Insert !v
  | Delete
    -- | TODO: should be given a more suitable name.
  | Mupsert !v
  deriving (Eq, Show)

-- | Perform a mixed batch of inserts, deletes and monoidal upserts.
--
-- Updates can be performed concurrently from multiple Haskell threads.
updates :: forall k v.
     ( SomeSerialisationConstraint k
     , SomeSerialisationConstraint v
     , SomeUpdateConstraint v
     )
  => [(k, Update v)]
  -> Table k v
  -> Table k v
updates ups tbl0 = foldl' update tbl0 ups where
    update :: Table k v -> (k, Update v) -> Table k v
    update tbl (k, Delete) = tbl
        { _values = Map.delete (serialise k) (_values tbl) }
    update tbl (k, Insert v) = tbl
        { _values = Map.insert (serialise k) (serialise v) (_values tbl) }
    update tbl (k, Mupsert v) = tbl
        { _values = mapUpsert (serialise k) (serialise v) f (_values tbl) }
      where
        f old = serialise (merge v (deserialise old))

mapUpsert :: Ord k => k -> v -> (v -> v) -> Map k v -> Map k v
mapUpsert k v f = Map.alter (Just . g) k where
    g Nothing   = v
    g (Just v') = f v'

-- | Perform a batch of inserts.
--
-- Inserts can be performed concurrently from multiple Haskell threads.
inserts ::
     ( SomeSerialisationConstraint k
     , SomeSerialisationConstraint v
     , SomeUpdateConstraint v
     )
  => [(k, v)]
  -> Table k v
  -> Table k v
inserts = updates . fmap (second Insert)

-- | Perform a batch of deletes.
--
-- Deletes can be performed concurrently from multiple Haskell threads.
deletes ::
     ( SomeSerialisationConstraint k
     , SomeSerialisationConstraint v
     , SomeUpdateConstraint v
     )
  => [k]
  -> Table k v
  -> Table k v
deletes = updates . fmap (,Delete)

-- | Perform a batch of monoidal upserts.
--
-- Monoidal upserts can be performed concurrently from multiple Haskell threads.
mupserts ::
     ( SomeSerialisationConstraint k
     , SomeSerialisationConstraint v
     , SomeUpdateConstraint v
     )
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
mergeTables :: forall k v.
     (SomeSerialisationConstraint v, SomeUpdateConstraint v)
  => Table k v
  -> Table k v
  -> Table k v
mergeTables (Table xs) (Table ys) =
    Table (Map.unionWith f xs ys)
  where
    f x y = serialise (merge @v (deserialise x) (deserialise y))
