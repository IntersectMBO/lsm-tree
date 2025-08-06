{-# LANGUAGE TypeFamilies #-}

-- | A pure model of a single table, supporting both blobs and mupserts.
module Database.LSMTree.Model.Table (
    -- * Serialisation
    SerialiseKey (..)
  , SerialiseValue (..)
    -- * Tables
  , Table (..)
  , empty
    -- * Monoidal value resolution
  , ResolveSerialisedValue (..)
  , getResolve
  , noResolve
    -- * Table querying and updates
    -- ** Queries
  , Range (..)
  , LookupResult (..)
  , lookups
  , Entry (..)
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
    -- ** Blobs
  , BlobRef
  , retrieveBlobs
    -- * Snapshots
  , snapshot
    -- * Multiple writable tables
  , duplicate
    -- * Table union
  , union
  , unions
    -- * Testing
  , size
  ) where

import qualified Crypto.Hash.SHA256 as SHA256
import           Data.Bifunctor
import qualified Data.ByteString as BS
import           Data.Kind (Type)
import           Data.List.NonEmpty (NonEmpty (..))
import           Data.Map (Map)
import qualified Data.Map.Strict as Map
import           Data.Proxy (Proxy (Proxy))
import           Data.Semigroup (First (..))
import qualified Data.Vector as V
import           Database.LSMTree (Entry (..), LookupResult (..), Range (..),
                     ResolveValue (..), SerialiseKey (..), SerialiseValue (..),
                     Update (..))
import qualified Database.LSMTree.Internal.Map.Range as Map.R
import           Database.LSMTree.Internal.RawBytes (RawBytes)
import           GHC.Exts (IsList (..))

newtype ResolveSerialisedValue v =
    Resolve { resolveSerialisedValue :: RawBytes -> RawBytes -> RawBytes }

getResolve :: forall v. ResolveValue v => ResolveSerialisedValue v
getResolve = Resolve (resolveSerialised (Proxy @v))

noResolve :: ResolveSerialisedValue v
noResolve = Resolve const

resolveValueAndBlob ::
     ResolveSerialisedValue v
  -> (RawBytes, Maybe b)
  -> (RawBytes, Maybe b)
  -> (RawBytes, Maybe b)
resolveValueAndBlob r (v1, bMay1) (v2, bMay2) =
      (resolveSerialisedValue r v1 v2, getFirst (First bMay1 <> First bMay2))

{-------------------------------------------------------------------------------
  Tables
-------------------------------------------------------------------------------}

type Table :: Type -> Type -> Type -> Type
data Table k v b = Table
    { values :: Map RawBytes (RawBytes, Maybe (BlobRef b))
    }

type role Table nominal nominal nominal

-- | An empty table.
empty :: Table k v b
empty = Table Map.empty

size :: Table k v b -> Int
size (Table m) = Map.size m

-- | This instance is for testing and debugging only.
instance
    (SerialiseKey k, SerialiseValue v, SerialiseValue b)
      => IsList (Table k v b)
  where
    type Item (Table k v b) = (k, v, Maybe b)
    fromList xs = Table $ Map.fromList
        [ (serialiseKey k, (serialiseValue v, mkBlobRef <$> mblob))
        | (k, v, mblob) <- xs
        ]

    toList (Table m) =
        [ (deserialiseKey k, deserialiseValue v, getBlobFromRef <$> mbref)
        | (k, (v, mbref)) <- Map.toList m
        ]

-- | This instance is for testing and debugging only.
instance Show (Table k v b) where
    showsPrec d (Table tbl) = showParen (d > 10)
        $ showString "fromList "
        . showsPrec 11 (toList (Table @BS.ByteString @BS.ByteString @BS.ByteString tbl'))
      where
        tbl' :: Map RawBytes (RawBytes, Maybe (BlobRef BS.ByteString))
        tbl' = fmap (fmap (fmap coerceBlobRef)) tbl

-- | This instance is for testing and debugging only.
deriving stock instance Eq (Table k v b)

{-------------------------------------------------------------------------------
  Lookups
-------------------------------------------------------------------------------}

lookups ::
     (SerialiseKey k, SerialiseValue v)
  => V.Vector k
  -> Table k v b
  -> V.Vector (LookupResult v (BlobRef b))
lookups ks tbl = flip V.map ks $ \k ->
    case Map.lookup (serialiseKey k) (values tbl) of
      Nothing           -> NotFound
      Just (v, Nothing) -> Found (deserialiseValue v)
      Just (v, Just br) -> FoundWithBlob (deserialiseValue v) br

rangeLookup :: forall k v b.
     (SerialiseKey k, SerialiseValue v)
  => Range k
  -> Table k v b
  -> V.Vector (Entry k v (BlobRef b))
rangeLookup r tbl = V.fromList
    [ case v of
        (v', Nothing) -> Entry (deserialiseKey k) (deserialiseValue v')
        (v', Just br) -> EntryWithBlob (deserialiseKey k) (deserialiseValue v') br
    | let (lb, ub) = convertRange r
    , (k, v) <- Map.R.rangeLookup lb ub (values tbl)
    ]
  where
    convertRange :: Range k -> (Map.R.Bound RawBytes, Map.R.Bound RawBytes)
    convertRange (FromToExcluding lb ub) =
        ( Map.R.Bound (serialiseKey lb) Map.R.Inclusive
        , Map.R.Bound (serialiseKey ub) Map.R.Exclusive )
    convertRange (FromToIncluding lb ub) =
        ( Map.R.Bound (serialiseKey lb) Map.R.Inclusive
        , Map.R.Bound (serialiseKey ub) Map.R.Inclusive )

{-------------------------------------------------------------------------------
  Updates
-------------------------------------------------------------------------------}

updates :: forall k v b.
     (SerialiseKey k, SerialiseValue v, SerialiseValue b)
  => ResolveSerialisedValue v
  -> V.Vector (k, Update v b)
  -> Table k v b
  -> Table k v b
updates r ups tbl0 = V.foldl' update tbl0 ups where
    update :: Table k v b -> (k, Update v b) -> Table k v b
    update tbl (k, Delete) = tbl
        { values = Map.delete (serialiseKey k) (values tbl) }
    update tbl (k, Insert v Nothing) = tbl
        { values = Map.insert (serialiseKey k) (serialiseValue v, Nothing) (values tbl) }
    update tbl (k, Insert v (Just blob)) = tbl
        { values = Map.insert (serialiseKey k) (serialiseValue v, Just (mkBlobRef blob)) (values tbl)
        }
    update tbl (k, Upsert v) = tbl
        { values = mapUpsert (serialiseKey k) e f (values tbl) }
      where
        e = (serialiseValue v, Nothing)
        f = resolveValueAndBlob r e

mapUpsert :: Ord k => k -> v -> (v -> v) -> Map k v -> Map k v
mapUpsert k v f = Map.alter (Just . g) k where
    g Nothing   = v
    g (Just v') = f v'

inserts ::
     (SerialiseKey k, SerialiseValue v, SerialiseValue b)
  => ResolveSerialisedValue v
  -> V.Vector (k, v, Maybe b)
  -> Table k v b
  -> Table k v b
inserts r = updates r . fmap (\(k, v, blob) -> (k, Insert v blob))

deletes ::
     (SerialiseKey k, SerialiseValue v, SerialiseValue b)
  => ResolveSerialisedValue v
  -> V.Vector k
  -> Table k v b
  -> Table k v b
deletes r = updates r . fmap (,Delete)

mupserts ::
     (SerialiseKey k, SerialiseValue v, SerialiseValue b)
  => ResolveSerialisedValue v
  -> V.Vector (k, v)
  -> Table k v b
  -> Table k v b
mupserts r = updates r . fmap (second Upsert)

{-------------------------------------------------------------------------------
  Blobs
-------------------------------------------------------------------------------}

retrieveBlobs ::
     SerialiseValue b
  => V.Vector (BlobRef b)
  -> V.Vector b
retrieveBlobs refs = V.map getBlobFromRef refs

data BlobRef b = BlobRef
    !BS.ByteString  -- ^ digest
    !RawBytes       -- ^ actual contents
  deriving stock (Show)

type role BlobRef nominal

mkBlobRef :: SerialiseValue b => b -> BlobRef b
mkBlobRef blob =  BlobRef (SHA256.hash bs) rb
  where
    !rb = serialiseValue blob
    !bs = deserialiseValue rb :: BS.ByteString

coerceBlobRef :: BlobRef b -> BlobRef b'
coerceBlobRef (BlobRef d b) = BlobRef d b

getBlobFromRef :: SerialiseValue b => BlobRef b -> b
getBlobFromRef (BlobRef _ rb) = deserialiseValue rb

instance Eq (BlobRef b) where
    BlobRef x _ == BlobRef y _ = x == y

{-------------------------------------------------------------------------------
  Snapshots
-------------------------------------------------------------------------------}

snapshot ::
     Table k v b
  -> Table k v b
snapshot = id

{-------------------------------------------------------------------------------
  Multiple writable tables
-------------------------------------------------------------------------------}

duplicate ::
     Table k v b
  -> Table k v b
duplicate = id

{-------------------------------------------------------------------------------
  Cursors
-------------------------------------------------------------------------------}

type Cursor :: Type -> Type -> Type -> Type
data Cursor k v b = Cursor
    { -- | these entries are already resolved, they do not contain duplicate keys.
      _cursorValues :: [(RawBytes, (RawBytes, Maybe (BlobRef b)))]
    }

type role Cursor nominal nominal nominal

newCursor ::
     SerialiseKey k
  => Maybe k
  -> Table k v b
  -> Cursor k v b
newCursor offset tbl = Cursor (skip $ Map.toList $ values tbl)
  where
    skip = case offset of
      Nothing -> id
      Just k  -> dropWhile ((< serialiseKey k) . fst)

readCursor ::
     (SerialiseKey k, SerialiseValue v)
  => Int
  -> Cursor k v b
  -> (V.Vector (Entry k v (BlobRef b)), Cursor k v b)
readCursor n c =
    ( V.fromList
        [ case v of
            (v', Nothing) -> Entry (deserialiseKey k) (deserialiseValue v')
            (v', Just br) -> EntryWithBlob (deserialiseKey k) (deserialiseValue v') br
        | (k, v) <- take n (_cursorValues c)
        ]
    , Cursor $ drop n (_cursorValues c)
    )

{-------------------------------------------------------------------------------
  Table union
-------------------------------------------------------------------------------}

-- | Union two full tables, creating a new table.
union ::
     ResolveSerialisedValue v
  -> Table k v b
  -> Table k v b
  -> Table k v b
union r (Table xs) (Table ys) =
    Table (Map.unionWith (resolveValueAndBlob r) xs ys)

-- | Like 'union', but for @n@ tables.
unions ::
     ResolveSerialisedValue v
  -> NonEmpty (Table k v b)
  -> Table k v b
unions r tables =
    Table (Map.unionsWith (resolveValueAndBlob r) (fmap values tables))
