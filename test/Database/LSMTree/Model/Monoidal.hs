{-# LANGUAGE TypeFamilies #-}

-- |
--
-- This module is intended to be imported qualified.
--
-- > import qualified Database.LSMTree.Monoidal as LSMT
module Database.LSMTree.Model.Monoidal (
    -- * Serialisation
    SerialiseKey (..)
  , SerialiseValue (..)
    -- * Monoidal value resolution
  , ResolveValue (..)
  , resolveDeserialised
    -- * Tables
  , Table
  , empty
    -- * Table querying and updates
    -- ** Queries
  , Range
  , LookupResult (..)
  , lookups
  , QueryResult (..)
  , rangeLookup
    -- ** Cursor
  , Cursor
  , newCursor
  , readCursor
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
import           Data.Kind (Type)
import           Data.Map (Map)
import qualified Data.Map.Range as Map.R
import qualified Data.Map.Strict as Map
import           Data.Proxy (Proxy (Proxy))
import qualified Data.Vector as V
import           Database.LSMTree.Common (Range (..), SerialiseKey (..),
                     SerialiseValue (..))
import           Database.LSMTree.Internal.RawBytes (RawBytes)
import           Database.LSMTree.Monoidal (LookupResult (..), QueryResult (..),
                     ResolveValue (..), Update (..), resolveDeserialised)
import           GHC.Exts (IsList (..))

{-------------------------------------------------------------------------------
  Tables
-------------------------------------------------------------------------------}

type Table :: Type -> Type -> Type
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
     (SerialiseKey k, SerialiseValue v)
  => V.Vector k
  -> Table k v
  -> V.Vector (LookupResult v)
lookups ks tbl = flip V.map ks $ \k ->
    case Map.lookup (serialiseKey k) (_values tbl) of
      Nothing -> NotFound
      Just v  -> Found (deserialiseValue v)

-- | Perform a range lookup.
--
-- Range lookups can be performed concurrently from multiple Haskell threads.
rangeLookup :: forall k v.
     (SerialiseKey k, SerialiseValue v)
  => Range k
  -> Table k v
  -> V.Vector (QueryResult k v)
rangeLookup r tbl = V.fromList
    [ FoundInQuery (deserialiseKey k) (deserialiseValue v)
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
     (SerialiseKey k, SerialiseValue v, ResolveValue v)
  => V.Vector (k, Update v)
  -> Table k v
  -> Table k v
updates ups tbl0 = V.foldl' update tbl0 ups where
    update :: Table k v -> (k, Update v) -> Table k v
    update tbl (k, Delete) = tbl
        { _values = Map.delete (serialiseKey k) (_values tbl) }
    update tbl (k, Insert v) = tbl
        { _values = Map.insert (serialiseKey k) (serialiseValue v) (_values tbl) }
    update tbl (k, Mupsert v) = tbl
        { _values = mapUpsert (serialiseKey k) (serialiseValue v) f (_values tbl) }
      where
        f = resolveValue (Proxy @v) (serialiseValue v)

mapUpsert :: Ord k => k -> v -> (v -> v) -> Map k v -> Map k v
mapUpsert k v f = Map.alter (Just . g) k where
    g Nothing   = v
    g (Just v') = f v'

-- | Perform a batch of inserts.
--
-- Inserts can be performed concurrently from multiple Haskell threads.
inserts ::
     (SerialiseKey k, SerialiseValue v, ResolveValue v)
  => V.Vector (k, v)
  -> Table k v
  -> Table k v
inserts = updates . fmap (second Insert)

-- | Perform a batch of deletes.
--
-- Deletes can be performed concurrently from multiple Haskell threads.
deletes ::
     (SerialiseKey k, SerialiseValue v, ResolveValue v)
  => V.Vector k
  -> Table k v
  -> Table k v
deletes = updates . fmap (,Delete)

-- | Perform a batch of monoidal upserts.
--
-- Monoidal upserts can be performed concurrently from multiple Haskell threads.
mupserts ::
     (SerialiseKey k, SerialiseValue v, ResolveValue v)
  => V.Vector (k, v)
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
  Cursor
-------------------------------------------------------------------------------}

type Cursor :: Type -> Type -> Type
data Cursor k v = Cursor
    { -- | these entries are already resolved, they do not contain duplicate keys.
      _cursorValues :: [(RawBytes, RawBytes)]
    }

type role Cursor nominal nominal

newCursor ::
     SerialiseKey k
  => Maybe k
  -> Table k v
  -> Cursor k v
newCursor offset tbl = Cursor (skip $ Map.toList $ _values tbl)
  where
    skip = case offset of
      Nothing -> id
      Just k  -> dropWhile ((< serialiseKey k) . fst)

readCursor ::
     (SerialiseKey k, SerialiseValue v)
  => Int
  -> Cursor k v
  -> (V.Vector (QueryResult k v), Cursor k v)
readCursor n c =
    ( V.fromList
        [ FoundInQuery (deserialiseKey k) (deserialiseValue v)
        | (k, v) <- take n (_cursorValues c)
        ]
    , Cursor $ drop n (_cursorValues c)
    )

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
     (ResolveValue v)
  => Table k v
  -> Table k v
  -> Table k v
merge (Table xs) (Table ys) =
    Table (Map.unionWith (resolveValue (Proxy @v)) xs ys)
