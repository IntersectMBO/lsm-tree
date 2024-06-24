{-# LANGUAGE FlexibleContexts         #-}
{-# LANGUAGE KindSignatures           #-}
{-# LANGUAGE LambdaCase               #-}
{-# LANGUAGE QuantifiedConstraints    #-}
{-# LANGUAGE StandaloneKindSignatures #-}
{-# LANGUAGE TypeFamilies             #-}

module Database.LSMTree.Class.Monoidal (
    IsSession (..)
  , IsTableHandle (..)
  ) where

import           Data.Kind (Constraint, Type)
import           Data.Proxy (Proxy)
import           Data.Typeable (Typeable)
import           Database.LSMTree.Class.Normal (IsSession (..))
import           Database.LSMTree.Common (IOLike, Range (..), SerialiseKey,
                     SerialiseValue, SnapshotName, SomeUpdateConstraint)
import qualified Database.LSMTree.ModelIO.Monoidal as M
import           Database.LSMTree.Monoidal (LookupResult (..),
                     RangeLookupResult (..), Update (..))
import qualified Database.LSMTree.Monoidal as R


-- | Class abstracting over table handle operations.
--
type IsTableHandle :: ((Type -> Type) -> Type -> Type -> Type) -> Constraint
class (IsSession (Session h)) => IsTableHandle h where
    type Session h :: (Type -> Type) -> Type
    type TableConfig h :: Type

    testTableConfig :: Proxy h -> TableConfig h

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
            (IOLike m, SerialiseKey k, SerialiseValue v, SomeUpdateConstraint v)
        => h m k v
        -> [k]
        -> m [LookupResult k v]

    rangeLookup ::
            (IOLike m, SerialiseKey k, SerialiseValue v, SomeUpdateConstraint v)
        => h m k v
        -> Range k
        -> m [RangeLookupResult k v]

    updates ::
        ( IOLike m
        , SerialiseKey k
        , SerialiseValue v
        , SomeUpdateConstraint v
        )
        => h m k v
        -> [(k, Update v)]
        -> m ()

    inserts ::
        ( IOLike m
        , SerialiseKey k
        , SerialiseValue v
        , SomeUpdateConstraint v
        )
        => h m k v
        -> [(k, v)]
        -> m ()

    deletes ::
        ( IOLike m
        , SerialiseKey k
        , SerialiseValue v
        , SomeUpdateConstraint v
        )
        => h m k v
        -> [k]
        -> m ()

    mupserts ::
        ( IOLike m
        , SerialiseKey k
        , SerialiseValue v
        , SomeUpdateConstraint v
        )
        => h m k v
        -> [(k, v)]
        -> m ()

    snapshot ::
        ( IOLike m
        , SerialiseKey k
        , SerialiseValue v
          -- Model-specific constraints
        , Typeable k
        , Typeable v
        )
        => SnapshotName
        -> h m k v
        -> m ()

    open ::
        ( IOLike m
        , SerialiseKey k
        , SerialiseValue v
          -- Model-specific constraints
        , Typeable k
        , Typeable v
        )
        => Session h m
        -> SnapshotName
        -> m (h m k v)

    duplicate ::
            IOLike m
        => h m k v
        -> m (h m k v)

    merge ::
        (IOLike m, SerialiseValue v, SomeUpdateConstraint v)
        => h m k v
        -> h m k v
        -> m (h m k v)

instance IsTableHandle M.TableHandle where
    type Session M.TableHandle = M.Session
    type TableConfig M.TableHandle = M.TableConfig

    testTableConfig _ = M.TableConfig

    new = M.new
    close = M.close
    lookups = flip M.lookups
    updates = flip M.updates
    inserts = flip M.inserts
    deletes = flip M.deletes
    mupserts = flip M.mupserts

    rangeLookup = flip M.rangeLookup

    snapshot = M.snapshot
    open = M.open

    duplicate = M.duplicate
    merge = M.merge

instance IsTableHandle R.TableHandle where
    type Session R.TableHandle = R.Session
    type TableConfig R.TableHandle = R.TableConfig

    testTableConfig _ = error "TODO: test TableConfig"

    new = R.new
    close = R.close
    lookups = flip R.lookups
    updates = flip R.updates
    inserts = flip R.inserts
    deletes = flip R.deletes
    mupserts = flip R.mupserts

    rangeLookup = flip R.rangeLookup

    snapshot = R.snapshot
    open = R.open

    duplicate = R.duplicate
    merge = R.merge
