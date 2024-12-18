{-# LANGUAGE TypeFamilies #-}

-- | An abstraction of the normal LSM API, instantiated by both the real
-- implementation and a model (see "Database.LSMTree.Model.IO").
module Database.LSMTree.Class (
    IsTable (..)
  , withTableNew
  , withTableFromSnapshot
  , withTableDuplicate
  , withTableUnion
  , withTableUnions
  , withCursor
  , module Common
  , module Types
  ) where

import           Control.Monad.Class.MonadThrow (MonadThrow (..))
import           Data.Kind (Constraint, Type)
import           Data.List.NonEmpty (NonEmpty)
import           Data.Typeable (Proxy (..))
import qualified Data.Vector as V
import           Database.LSMTree as Types (LookupResult (..), QueryResult (..),
                     ResolveAsFirst (..), ResolveValue (..), Update (..),
                     resolveDeserialised)
import qualified Database.LSMTree as R
import           Database.LSMTree.Class.Common as Common

-- | Class abstracting over table operations.
--
type IsTable :: ((Type -> Type) -> Type -> Type -> Type -> Type) -> Constraint
class (IsSession (Session h)) => IsTable h where
    type Session h :: (Type -> Type) -> Type
    type TableConfig h :: Type
    type BlobRef h :: (Type -> Type) -> Type -> Type
    type Cursor h :: (Type -> Type) -> Type -> Type -> Type -> Type

    new ::
           ( IOLike m
           , C k v b
           )
        => Session h m
        -> TableConfig h
        -> m (h m k v b)

    close ::
           ( IOLike m
           , C k v b
           )
        => h m k v b
        -> m ()

    lookups ::
           ( IOLike m
           , C k v b
           )
        => h m k v b
        -> V.Vector k
        -> m (V.Vector (LookupResult v (BlobRef h m b)))

    rangeLookup ::
           ( IOLike m
           , C k v b
           )
        => h m k v b
        -> Range k
        -> m (V.Vector (QueryResult k v (BlobRef h m b)))

    newCursor ::
           ( IOLike m
           , C k v b
           )
        => Maybe k
        -> h m k v b
        -> m (Cursor h m k v b)

    closeCursor ::
           ( IOLike m
           , C k v b
           )
        => proxy h
        -> Cursor h m k v b
        -> m ()

    readCursor ::
           ( IOLike m
           , C k v b
           )
        => proxy h
        -> Int
        -> Cursor h m k v b
        -> m (V.Vector (QueryResult k v (BlobRef h m b)))

    retrieveBlobs ::
           ( IOLike m
           , CB b
           )
        => proxy h
        -> Session h m
        -> V.Vector (BlobRef h m b)
        -> m (V.Vector b)

    updates ::
           ( IOLike m
           , C k v b
           )
        => h m k v b
        -> V.Vector (k, Update v b)
        -> m ()

    inserts ::
           ( IOLike m
           , C k v b
           )
        => h m k v b
        -> V.Vector (k, v, Maybe b)
        -> m ()

    deletes ::
           ( IOLike m
           , C k v b
           )
        => h m k v b
        -> V.Vector k
        -> m ()

    mupserts ::
           ( IOLike m
           , C k v b
           )
        => h m k v b
        -> V.Vector (k, v)
        -> m ()

    createSnapshot ::
           ( IOLike m
           , C k v b
           )
        => SnapshotLabel
        -> SnapshotName
        -> h m k v b
        -> m ()

    openSnapshot ::
           ( IOLike m
           , C k v b
           )
        => Session h m
        -> SnapshotLabel
        -> SnapshotName
        -> m (h m k v b)

    duplicate ::
           ( IOLike m
           , C k v b
           )
        => h m k v b
        -> m (h m k v b)

    union ::
           ( IOLike m
           , C k v b
           )
        => h m k v b
        -> h m k v b
        -> m (h m k v b)

    unions ::
           ( IOLike m
           , C k v b
           )
        => NonEmpty (h m k v b)
        -> m (h m k v b)

withTableNew :: forall h m k v b a.
    (IOLike m, IsTable h, C k v b)
  => Session h m
  -> TableConfig h
  -> (h m k v b -> m a)
  -> m a
withTableNew sesh conf = bracket (new sesh conf) close

withTableFromSnapshot :: forall h m k v b a.
     (IOLike m, IsTable h, C k v b)
  => Session h m
  -> SnapshotLabel
  -> SnapshotName
  -> (h m k v b -> m a)
  -> m a
withTableFromSnapshot sesh label snap = bracket (openSnapshot sesh label snap) close

withTableDuplicate :: forall h m k v b a.
     (IOLike m, IsTable h, C k v b)
  => h m k v b
  -> (h m k v b -> m a)
  -> m a
withTableDuplicate table = bracket (duplicate table) close

withTableUnion :: forall h m k v b a.
     (IOLike m, IsTable h, C k v b)
  => h m k v b
  -> h m k v b
  -> (h m k v b -> m a)
  -> m a
withTableUnion table1 table2 = bracket (table1 `union` table2) close

withTableUnions :: forall h m k v b a.
     (IOLike m, IsTable h, C k v b)
  => NonEmpty (h m k v b)
  -> (h m k v b -> m a)
  -> m a
withTableUnions tables = bracket (unions tables) close

withCursor :: forall h m k v b a.
     (IOLike m, IsTable h, C k v b)
  => Maybe k
  -> h m k v b
  -> (Cursor h m k v b -> m a)
  -> m a
withCursor offset hdl = bracket (newCursor offset hdl) (closeCursor (Proxy @h))

{-------------------------------------------------------------------------------
  Real instance
-------------------------------------------------------------------------------}

instance IsTable R.Table where
    type Session R.Table = R.Session
    type TableConfig R.Table = R.TableConfig
    type BlobRef R.Table = R.BlobRef
    type Cursor R.Table = R.Cursor

    new = R.new
    close = R.close
    lookups = R.lookups
    updates = R.updates
    inserts = R.inserts
    deletes = R.deletes
    mupserts = R.mupserts

    rangeLookup = R.rangeLookup
    retrieveBlobs _ = R.retrieveBlobs

    newCursor = maybe R.newCursor R.newCursorAtOffset
    closeCursor _ = R.closeCursor
    readCursor _ = R.readCursor

    createSnapshot = R.createSnapshot
    openSnapshot sesh snap = R.openSnapshot sesh R.configNoOverride snap

    duplicate = R.duplicate
    union = R.union
    unions = R.unions
