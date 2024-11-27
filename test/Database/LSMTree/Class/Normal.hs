{-# LANGUAGE TypeFamilies #-}

-- | An abstraction of the normal LSM API, instantiated by both the real
-- implementation and a model (see "Database.LSMTree.Model.IO.Normal").
module Database.LSMTree.Class.Normal (
    IsTable (..)
  , withTableNew
  , withTableFromSnapshot
  , withTableDuplicate
  , withCursor
  , module Common
  , module Types
  ) where

import           Control.Monad.Class.MonadThrow (MonadThrow (..))
import           Data.Kind (Constraint, Type)
import           Data.Typeable (Proxy (..))
import qualified Data.Vector as V
import           Database.LSMTree.Class.Common as Common
import           Database.LSMTree.Normal as Types (LookupResult (..),
                     QueryResult (..), Update (..))
import qualified Database.LSMTree.Normal as R

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
           , C k v blob
           )
        => Session h m
        -> TableConfig h
        -> m (h m k v blob)

    close ::
           ( IOLike m
           , C k v blob
           )
        => h m k v blob
        -> m ()

    lookups ::
           ( IOLike m
           , SerialiseKey k
           , SerialiseValue v
           , C k v blob
           )
        => h m k v blob
        -> V.Vector k
        -> m (V.Vector (LookupResult v (BlobRef h m blob)))

    rangeLookup ::
           ( IOLike m
           , SerialiseKey k
           , SerialiseValue v
           , C k v blob
           )
        => h m k v blob
        -> Range k
        -> m (V.Vector (QueryResult k v (BlobRef h m blob)))

    newCursor ::
           ( IOLike m
           , SerialiseKey k
           , C k v blob
           )
        => Maybe k
        -> h m k v blob
        -> m (Cursor h m k v blob)

    closeCursor ::
           ( IOLike m
           , C k v blob
           )
        => proxy h
        -> Cursor h m k v blob
        -> m ()

    readCursor ::
           ( IOLike m
           , SerialiseKey k
           , SerialiseValue v
           , C k v blob
           )
        => proxy h
        -> Int
        -> Cursor h m k v blob
        -> m (V.Vector (QueryResult k v (BlobRef h m blob)))

    retrieveBlobs ::
           ( IOLike m
           , SerialiseValue blob
           , C_ blob
           )
        => proxy h
        -> Session h m
        -> V.Vector (BlobRef h m blob)
        -> m (V.Vector blob)

    updates ::
           ( IOLike m
           , SerialiseKey k
           , SerialiseValue v
           , SerialiseValue blob
           , C k v blob
           )
        => h m k v blob
        -> V.Vector (k, Update v blob)
        -> m ()

    inserts ::
           ( IOLike m
           , SerialiseKey k
           , SerialiseValue v
           , SerialiseValue blob
           , C k v blob
           )
        => h m k v blob
        -> V.Vector (k, v, Maybe blob)
        -> m ()

    deletes ::
           ( IOLike m
           , SerialiseKey k
           , SerialiseValue v
           , SerialiseValue blob
           , C k v blob
           )
        => h m k v blob
        -> V.Vector k
        -> m ()

    createSnapshot ::
           ( IOLike m
           , SerialiseKey k
           , SerialiseValue v
           , SerialiseValue blob
           , C k v blob
           )
        => SnapshotLabel
        -> SnapshotName
        -> h m k v blob
        -> m ()

    openSnapshot ::
           ( IOLike m
           , SerialiseKey k
           , SerialiseValue v
           , SerialiseValue blob
           , C k v blob
           )
        => Session h m
        -> SnapshotLabel
        -> SnapshotName
        -> m (h m k v blob)

    duplicate ::
           ( IOLike m
           , C k v blob
           )
        => h m k v blob
        -> m (h m k v blob)

    union ::
           ( IOLike m
           , SerialiseValue v
           , C k v blob
           )
        => h m k v blob
        -> h m k v blob
        -> m (h m k v blob)

withTableNew :: forall h m k v blob a.
    ( IOLike m
    , IsTable h
    , C k v blob
    )
  => Session h m
  -> TableConfig h
  -> (h m k v blob -> m a)
  -> m a
withTableNew sesh conf = bracket (new sesh conf) close

withTableFromSnapshot :: forall h m k v blob a.
     ( IOLike m, IsTable h
     , SerialiseKey k, SerialiseValue v, SerialiseValue blob
     , C k v blob
     )
  => Session h m
  -> SnapshotLabel
  -> SnapshotName
  -> (h m k v blob -> m a)
  -> m a
withTableFromSnapshot sesh label snap = bracket (openSnapshot sesh label snap) close

withTableDuplicate :: forall h m k v blob a.
     ( IOLike m
     , IsTable h
     , C k v blob
     )
  => h m k v blob
  -> (h m k v blob -> m a)
  -> m a
withTableDuplicate table = bracket (duplicate table) close

withCursor :: forall h m k v blob a.
     ( IOLike m
     , IsTable h
     , SerialiseKey k
     , C k v blob
     )
  => Maybe k
  -> h m k v blob
  -> (Cursor h m k v blob -> m a)
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

    rangeLookup = R.rangeLookup
    retrieveBlobs _ = R.retrieveBlobs

    newCursor = maybe R.newCursor R.newCursorAtOffset
    closeCursor _ = R.closeCursor
    readCursor _ = R.readCursor

    createSnapshot = R.createSnapshot
    openSnapshot sesh snap = R.openSnapshot sesh R.configNoOverride snap

    duplicate = R.duplicate
    union = R.union
