{-# LANGUAGE FlexibleContexts         #-}
{-# LANGUAGE KindSignatures           #-}
{-# LANGUAGE LambdaCase               #-}
{-# LANGUAGE QuantifiedConstraints    #-}
{-# LANGUAGE StandaloneKindSignatures #-}
{-# LANGUAGE TypeFamilies             #-}
module Test.Database.LSMTree.ModelIO.Class (
    IsSession (..)
  , IsTableHandle (..)
  ) where

import           Control.Monad.Class.MonadThrow (MonadThrow (throwIO))
import           Data.Kind (Constraint, Type)
import           Data.Proxy (Proxy)
import           Database.LSMTree.Common (IOLike, SomeSerialisationConstraint)
import qualified Database.LSMTree.ModelIO.Normal as M
import           Database.LSMTree.Normal (LookupResult (..), Update (..))
import qualified Database.LSMTree.Normal as R

type IsSession :: ((Type -> Type) -> Type) -> Constraint
class IsSession s where
    newSession :: IOLike m => m (s m)

-- | Class abstracting over table handle operations.
--
type IsTableHandle :: ((Type -> Type) -> Type -> Type -> Type -> Type) -> Constraint
class (IsSession (Session h)) => IsTableHandle h where
    type Session h :: (Type -> Type) -> Type
    type TableConfig h :: Type
    type BlobRef h :: Type -> Type

    testTableConfig :: Proxy h -> TableConfig h

    new ::
           IOLike m
        => Session h m
        -> TableConfig h
        -> m (h m k v blob)

    lookups ::
            (IOLike m, SomeSerialisationConstraint k, SomeSerialisationConstraint v)
        => h m k v blob
        -> [k]
        -> m [LookupResult k v (BlobRef h blob)]

    updates ::
        ( IOLike m
        , SomeSerialisationConstraint k
        , SomeSerialisationConstraint v
        , SomeSerialisationConstraint blob
        )
        => h m k v blob
        -> [(k, Update v blob)]
        -> m ()

    inserts ::
        ( IOLike m
        , SomeSerialisationConstraint k
        , SomeSerialisationConstraint v
        , SomeSerialisationConstraint blob
        )
        => h m k v blob
        -> [(k, v, Maybe blob)]
        -> m ()

    deletes ::
        ( IOLike m
        , SomeSerialisationConstraint k
        , SomeSerialisationConstraint v
        , SomeSerialisationConstraint blob
        )
        => h m k v blob
        -> [k]
        -> m ()

instance IsSession M.Session where
    newSession = M.newSession

instance IsTableHandle M.TableHandle where
    type Session M.TableHandle = M.Session
    type TableConfig M.TableHandle = M.TableConfig
    type BlobRef M.TableHandle = M.BlobRef

    testTableConfig _ = M.TableConfig

    new = M.new
    lookups = flip M.lookups
    updates = flip M.updates
    inserts = flip M.inserts
    deletes = flip M.deletes

instance IsSession R.Session where
    newSession = throwIO (userError "newSession unimplemented")

instance IsTableHandle R.TableHandle where
    type Session R.TableHandle = R.Session
    type TableConfig R.TableHandle = R.TableConfig
    type BlobRef R.TableHandle = R.BlobRef

    testTableConfig _ = error "TODO: test TableConfig"

    new = R.new
    lookups = flip R.lookups
    updates = flip R.updates
    inserts = flip R.inserts
    deletes = flip R.deletes
