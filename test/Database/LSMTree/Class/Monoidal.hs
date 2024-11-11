{-# LANGUAGE TypeFamilies #-}

-- | An abstraction of the monoidal LSM API, instantiated by both the real
-- implementation and a model (see "Database.LSMTree.Model.IO.Monoidal").
module Database.LSMTree.Class.Monoidal (
    C
  , C_
  , IsSession (..)
  , SessionArgs (..)
  , withSession
  , IsTable (..)
  , withTableNew
  , withTableFromSnapshot
  , withTableDuplicate
  , withTableUnion
  , withCursor
  , module Types
  ) where

import           Control.Monad.Class.MonadThrow (MonadThrow (..))
import           Data.Kind (Constraint, Type)
import           Data.Typeable (Proxy (Proxy), Typeable)
import qualified Data.Vector as V
import           Data.Void (Void)
import           Database.LSMTree.Class.Normal (IsSession (..),
                     SessionArgs (..), withSession)
import           Database.LSMTree.Common as Types (IOLike, Labellable (..),
                     Range (..), SerialiseKey, SerialiseValue, SnapshotName)
import           Database.LSMTree.Monoidal as Types (LookupResult (..),
                     QueryResult (..), ResolveValue, Update (..))
import qualified Database.LSMTree.Monoidal as R

-- | Model-specific constraints
type C k v blob = (C_ k, C_ v, C_ blob)
type C_ a = (Show a, Eq a, Typeable a)

-- | Class abstracting over table operations.
--
type IsTable :: ((Type -> Type) -> Type -> Type -> Type) -> Constraint
class (IsSession (Session h)) => IsTable h where
    type Session h :: (Type -> Type) -> Type
    type TableConfig h :: Type
    type Cursor h :: (Type -> Type) -> Type -> Type -> Type

    new ::
           ( IOLike m
           , C k v Void
           )
        => Session h m
        -> TableConfig h
        -> m (h m k v)

    close ::
           ( IOLike m
           , C k v Void
           )
        => h m k v
        -> m ()

    lookups ::
           ( IOLike m
           , ResolveValue v
           , SerialiseKey k
           , SerialiseValue v
           , C k v Void
           )
        => h m k v
        -> V.Vector k
        -> m (V.Vector (LookupResult v))

    rangeLookup ::
           ( IOLike m
           , ResolveValue v
           , SerialiseKey k
           , SerialiseValue v
           , C k v Void
           )
        => h m k v
        -> Range k
        -> m (V.Vector (QueryResult k v))

    newCursor ::
           ( IOLike m
           , SerialiseKey k
           , C k v Void
           )
        => Maybe k
        -> h m k v
        -> m (Cursor h m k v)

    closeCursor ::
           ( IOLike m
           , C k v Void
           )
        => proxy h
        -> Cursor h m k v
        -> m ()

    readCursor ::
           ( IOLike m
           , ResolveValue v
           , SerialiseKey k
           , SerialiseValue v
           , C k v Void
           )
        => proxy h
        -> Int
        -> Cursor h m k v
        -> m (V.Vector (QueryResult k v))

    updates ::
           ( IOLike m
           , SerialiseKey k
           , SerialiseValue v
           , ResolveValue v
           , C k v Void
           )
        => h m k v
        -> V.Vector (k, Update v)
        -> m ()

    inserts ::
           ( IOLike m
           , SerialiseKey k
           , SerialiseValue v
           , ResolveValue v
           , C k v Void
           )
        => h m k v
        -> V.Vector (k, v)
        -> m ()

    deletes ::
           ( IOLike m
           , SerialiseKey k
           , SerialiseValue v
           , ResolveValue v
           , C k v Void
           )
        => h m k v
        -> V.Vector k
        -> m ()

    mupserts ::
           ( IOLike m
           , SerialiseKey k
           , SerialiseValue v
           , ResolveValue v
           , C k v Void
           )
        => h m k v
        -> V.Vector (k, v)
        -> m ()

    createSnapshot ::
           ( IOLike m
           , Labellable (k, v)
           , ResolveValue v
           , SerialiseKey k
           , SerialiseValue v
           , C k v Void
           )
        => SnapshotName
        -> h m k v
        -> m ()

    openSnapshot ::
           ( IOLike m
           , Labellable (k, v)
           , ResolveValue v
           , SerialiseKey k
           , SerialiseValue v
           , C k v Void
           )
        => Session h m
        -> SnapshotName
        -> m (h m k v)

    duplicate ::
           ( IOLike m
           , C k v Void
           )
        => h m k v
        -> m (h m k v)

    union ::
           ( IOLike m
           , ResolveValue v
           , SerialiseValue v
           , C k v Void
           )
        => h m k v
        -> h m k v
        -> m (h m k v)

withTableNew :: forall h m k v a.
     ( IOLike m
     , IsTable h
     , C k v Void
     )
  => Session h m
  -> TableConfig h
  -> (h m k v -> m a)
  -> m a
withTableNew sesh conf = bracket (new sesh conf) close

withTableFromSnapshot :: forall h m k v a.
     ( IOLike m
     , IsTable h
     , ResolveValue v
     , SerialiseKey k
     , SerialiseValue v
     , Labellable (k, v)
     , C k v Void
     )
  => Session h m
  -> SnapshotName
  -> (h m k v -> m a)
  -> m a
withTableFromSnapshot sesh snap = bracket (openSnapshot sesh snap) close

withTableDuplicate :: forall h m k v a.
     ( IOLike m
     , IsTable h
     , C k v Void
     )
  => h m k v
  -> (h m k v -> m a)
  -> m a
withTableDuplicate table = bracket (duplicate table) close

withTableUnion :: forall h m k v a.
     ( IOLike m
     , IsTable h
     , SerialiseValue v
     , ResolveValue v
     , C k v Void
     )
  => h m k v
  -> h m k v
  -> (h m k v -> m a)
  -> m a
withTableUnion table1 table2 = bracket (table1 `union` table2) close

withCursor :: forall h m k v a.
     ( IOLike m
     , IsTable h
     , SerialiseKey k
     , C k v Void
     )
  => Maybe k
  -> h m k v
  -> (Cursor h m k v -> m a)
  -> m a
withCursor offset hdl = bracket (newCursor offset hdl) (closeCursor (Proxy @h))

{-------------------------------------------------------------------------------
  Real instance
-------------------------------------------------------------------------------}

instance IsTable R.Table where
    type Session R.Table = R.Session
    type TableConfig R.Table = R.TableConfig
    type Cursor R.Table = R.Cursor

    new = R.new
    close = R.close
    lookups = R.lookups
    updates = R.updates
    inserts = R.inserts
    deletes = R.deletes
    mupserts = R.mupserts

    rangeLookup = R.rangeLookup

    newCursor = maybe R.newCursor R.newCursorAtOffset
    closeCursor _ = R.closeCursor
    readCursor _ = R.readCursor

    createSnapshot = R.createSnapshot
    openSnapshot sesh snap = R.openSnapshot sesh R.configNoOverride snap

    duplicate = R.duplicate
    union = R.union
