{-# LANGUAGE TypeFamilies #-}

module Database.LSMTree.Class.Monoidal (
    IsSession (..)
  , SessionArgs (..)
  , withSession
  , IsTableHandle (..)
  , withTableNew
  , withTableOpen
  , withTableDuplicate
  , withTableMerge
  , withCursor
  ) where

import           Control.Monad.Class.MonadThrow (MonadThrow (..))
import           Data.Kind (Constraint, Type)
import           Data.Typeable (Proxy (Proxy), Typeable)
import qualified Data.Vector as V
import           Database.LSMTree.Class.Normal (IsSession (..),
                     SessionArgs (..), withSession)
import           Database.LSMTree.Common (IOLike, Labellable (..), Range (..),
                     SerialiseKey, SerialiseValue, SnapshotName)
import qualified Database.LSMTree.ModelIO.Monoidal as M
import           Database.LSMTree.Monoidal (LookupResult (..), QueryResult (..),
                     ResolveValue, Update (..))
import qualified Database.LSMTree.Monoidal as R


-- | Class abstracting over table handle operations.
--
type IsTableHandle :: ((Type -> Type) -> Type -> Type -> Type) -> Constraint
class (IsSession (Session h)) => IsTableHandle h where
    type Session h :: (Type -> Type) -> Type
    type TableConfig h :: Type
    type Cursor h :: (Type -> Type) -> Type -> Type -> Type

    new ::
           IOLike m
        => Session h m
        -> TableConfig h
        -> m (h m k v)

    close ::
           IOLike m
        => h m k v
        -> m ()

    lookups ::
           ( IOLike m
           , ResolveValue v
           , SerialiseKey k
           , SerialiseValue v
           )
        => h m k v
        -> V.Vector k
        -> m (V.Vector (LookupResult v))

    rangeLookup ::
           ( IOLike m
           , ResolveValue v
           , SerialiseKey k
           , SerialiseValue v
           )
        => h m k v
        -> Range k
        -> m (V.Vector (QueryResult k v))

    newCursor ::
           ( IOLike m
           , SerialiseKey k
           )
        => Maybe k
        -> h m k v
        -> m (Cursor h m k v)

    closeCursor ::
           IOLike m
        => proxy h
        -> Cursor h m k v
        -> m ()

    readCursor ::
           ( IOLike m
           , ResolveValue v
           , SerialiseKey k
           , SerialiseValue v
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
           )
        => h m k v
        -> V.Vector (k, Update v)
        -> m ()

    inserts ::
           ( IOLike m
           , SerialiseKey k
           , SerialiseValue v
           , ResolveValue v
           )
        => h m k v
        -> V.Vector (k, v)
        -> m ()

    deletes ::
           ( IOLike m
           , SerialiseKey k
           , SerialiseValue v
           , ResolveValue v
           )
        => h m k v
        -> V.Vector k
        -> m ()

    mupserts ::
           ( IOLike m
           , SerialiseKey k
           , SerialiseValue v
           , ResolveValue v
           )
        => h m k v
        -> V.Vector (k, v)
        -> m ()

    snapshot ::
           ( IOLike m
           , Labellable (k, v)
           , ResolveValue v
           , SerialiseKey k
           , SerialiseValue v
             -- Model-specific constraints
           , Typeable k, Typeable v
           )
        => SnapshotName
        -> h m k v
        -> m ()

    open ::
           ( IOLike m
           , Labellable (k, v)
           , SerialiseKey k
           , SerialiseValue v
             -- Model-specific constraints
           , Typeable k, Typeable v
           )
        => Session h m
        -> SnapshotName
        -> m (h m k v)

    duplicate ::
           IOLike m
        => h m k v
        -> m (h m k v)

    merge ::
           ( IOLike m
           , ResolveValue v
           , SerialiseValue v
           )
        => h m k v
        -> h m k v
        -> m (h m k v)

withTableNew :: forall h m k v a.
     ( IOLike m
     , IsTableHandle h
     )
  => Session h m
  -> TableConfig h
  -> (h m k v -> m a)
  -> m a
withTableNew sesh conf = bracket (new sesh conf) close

withTableOpen :: forall h m k v a.
     ( IOLike m
     , IsTableHandle h
     , SerialiseKey k
     , SerialiseValue v
     , Labellable (k, v)
     , Typeable k, Typeable v
     )
  => Session h m
  -> SnapshotName
  -> (h m k v -> m a)
  -> m a
withTableOpen sesh snap = bracket (open sesh snap) close

withTableDuplicate :: forall h m k v a.
     ( IOLike m
     , IsTableHandle h
     )
  => h m k v
  -> (h m k v -> m a)
  -> m a
withTableDuplicate table = bracket (duplicate table) close

withTableMerge :: forall h m k v a.
     ( IOLike m
     , IsTableHandle h
     , SerialiseValue v
     , ResolveValue v
     )
  => h m k v
  -> h m k v
  -> (h m k v -> m a)
  -> m a
withTableMerge table1 table2 = bracket (merge table1 table2) close

withCursor :: forall h m k v a.
     ( IOLike m
     , IsTableHandle h
     , SerialiseKey k
     )
  => Maybe k
  -> h m k v
  -> (Cursor h m k v -> m a)
  -> m a
withCursor offset hdl = bracket (newCursor offset hdl) (closeCursor (Proxy @h))

{-------------------------------------------------------------------------------
  Model instance
-------------------------------------------------------------------------------}

instance IsTableHandle M.TableHandle where
    type Session M.TableHandle = M.Session
    type TableConfig M.TableHandle = M.TableConfig
    type Cursor M.TableHandle = M.Cursor

    new = M.new
    close = M.close
    lookups = flip M.lookups
    updates = flip M.updates
    inserts = flip M.inserts
    deletes = flip M.deletes
    mupserts = flip M.mupserts

    rangeLookup = flip M.rangeLookup

    newCursor = M.newCursor
    closeCursor _ = M.closeCursor
    readCursor _ = M.readCursor

    snapshot = M.snapshot
    open = M.open

    duplicate = M.duplicate
    merge = M.merge

{-------------------------------------------------------------------------------
  Real instance
-------------------------------------------------------------------------------}

instance IsTableHandle R.TableHandle where
    type Session R.TableHandle = R.Session
    type TableConfig R.TableHandle = R.TableConfig
    type Cursor R.TableHandle = R.Cursor

    new = R.new
    close = R.close
    lookups = flip R.lookups
    updates = flip R.updates
    inserts = flip R.inserts
    deletes = flip R.deletes
    mupserts = flip R.mupserts

    rangeLookup = flip R.rangeLookup

    newCursor = maybe R.newCursor R.newCursorAtOffset
    closeCursor _ = R.closeCursor
    readCursor _ = R.readCursor

    snapshot = R.snapshot
    open sesh snap = R.open sesh R.configNoOverride snap

    duplicate = R.duplicate
    merge = R.merge
