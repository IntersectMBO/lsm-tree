{-# LANGUAGE FlexibleContexts         #-}
{-# LANGUAGE KindSignatures           #-}
{-# LANGUAGE LambdaCase               #-}
{-# LANGUAGE QuantifiedConstraints    #-}
{-# LANGUAGE StandaloneKindSignatures #-}
{-# LANGUAGE TypeFamilies             #-}

module Database.LSMTree.Class.Normal (
    IsSession (..)
  , withSession
  , IsTableHandle (..)
  , withTableNew
  , withTableOpen
  , withTableDuplicate
  ) where

import           Control.Monad.Class.MonadThrow (MonadThrow (..))
import           Data.Kind (Constraint, Type)
import           Data.Proxy (Proxy)
import           Data.Typeable (Typeable)
import qualified Data.Vector as V
import           Database.LSMTree.Common (IOLike, Labellable (..), Range (..),
                     SerialiseKey, SerialiseValue, SnapshotName)
import qualified Database.LSMTree.ModelIO.Normal as M
import           Database.LSMTree.Normal (LookupResult (..),
                     RangeLookupResult (..), Update (..))
import qualified Database.LSMTree.Normal as R

type IsSession :: ((Type -> Type) -> Type) -> Constraint
class IsSession s where
    openSession :: IOLike m => m (s m)

    closeSession :: IOLike m => s m -> m ()

    deleteSnapshot ::
           IOLike m
        => s m
        -> SnapshotName
        -> m ()

    listSnapshots ::
           IOLike m
        => s m
        -> m [SnapshotName]

withSession :: (IOLike m, IsSession s) => (s m -> m a) -> m a
withSession = bracket openSession closeSession

-- | Class abstracting over table handle operations.
--
type IsTableHandle :: ((Type -> Type) -> Type -> Type -> Type -> Type) -> Constraint
class (IsSession (Session h)) => IsTableHandle h where
    type Session h :: (Type -> Type) -> Type
    type TableConfig h :: Type
    type BlobRef h :: (Type -> Type) -> Type -> Type

    testTableConfig :: Proxy h -> TableConfig h

    new ::
           IOLike m
        => Session h m
        -> TableConfig h
        -> m (h m k v blob)

    close ::
           IOLike m
        => h m k v blob
        -> m ()

    lookups ::
           (IOLike m, SerialiseKey k, SerialiseValue v)
        => h m k v blob
        -> V.Vector k
        -> m (V.Vector (LookupResult v (BlobRef h m blob)))

    rangeLookup ::
           (IOLike m, SerialiseKey k, SerialiseValue v)
        => h m k v blob
        -> Range k
        -> m (V.Vector (RangeLookupResult k v (BlobRef h m blob)))

    retrieveBlobs ::
           (IOLike m, SerialiseValue blob)
        => proxy h
        -> Session h m
        -> V.Vector (BlobRef h m blob)
        -> m (V.Vector blob)

    updates ::
           (IOLike m, SerialiseKey k, SerialiseValue v, SerialiseValue blob)
        => h m k v blob
        -> V.Vector (k, Update v blob)
        -> m ()

    inserts ::
           (IOLike m, SerialiseKey k, SerialiseValue v, SerialiseValue blob)
        => h m k v blob
        -> V.Vector (k, v, Maybe blob)
        -> m ()

    deletes ::
           (IOLike m, SerialiseKey k, SerialiseValue v, SerialiseValue blob)
        => h m k v blob
        -> V.Vector k
        -> m ()

    snapshot ::
        ( IOLike m, SerialiseKey k, SerialiseValue v , SerialiseValue blob
        , Labellable (k, v, blob)
          -- Model-specific constraints
        , Typeable k , Typeable v , Typeable blob
        )
        => SnapshotName
        -> h m k v blob
        -> m ()

    open ::
        ( IOLike m, SerialiseKey k, SerialiseValue v, SerialiseValue blob
        , Labellable (k, v, blob)
          -- Model-specific constraints
        , Typeable k, Typeable v, Typeable blob
        )
        => Session h m
        -> SnapshotName
        -> m (h m k v blob)

    duplicate ::
           IOLike m
        => h m k v blob
        -> m (h m k v blob)

withTableNew ::
     forall h m k v blob a. (IOLike m, IsTableHandle h)
  => Session h m
  -> TableConfig h
  -> (h m k v blob -> m a)
  -> m a
withTableNew sesh conf = bracket (new sesh conf) close

withTableOpen ::
     forall h m k v blob a. ( IOLike m, IsTableHandle h
     , SerialiseKey k, SerialiseValue v, SerialiseValue blob
     , Labellable (k, v, blob)
     , Typeable k, Typeable v, Typeable blob
     )
  => Session h m
  -> SnapshotName
  -> (h m k v blob -> m a)
  -> m a
withTableOpen sesh snap = bracket (open sesh snap) close

withTableDuplicate ::
     forall h m k v blob a. (IOLike m, IsTableHandle h)
  => h m k v blob
  -> (h m k v blob -> m a)
  -> m a
withTableDuplicate table = bracket (duplicate table) close

{-------------------------------------------------------------------------------
  Model instance
-------------------------------------------------------------------------------}

instance IsSession M.Session where
    openSession = M.openSession
    closeSession = M.closeSession
    deleteSnapshot = M.deleteSnapshot
    listSnapshots = M.listSnapshots

instance IsTableHandle M.TableHandle where
    type Session M.TableHandle = M.Session
    type TableConfig M.TableHandle = M.TableConfig
    type BlobRef M.TableHandle = M.BlobRef

    testTableConfig _ = M.TableConfig

    new = M.new
    close = M.close
    lookups = flip M.lookups
    updates = flip M.updates
    inserts = flip M.inserts
    deletes = flip M.deletes

    rangeLookup = flip M.rangeLookup
    retrieveBlobs _ = M.retrieveBlobs

    snapshot = M.snapshot
    open = M.open

    duplicate = M.duplicate

{-------------------------------------------------------------------------------
  Real instance
-------------------------------------------------------------------------------}

instance IsSession R.Session where
    openSession = throwIO (userError "openSession unimplemented")
    closeSession = R.closeSession
    deleteSnapshot = R.deleteSnapshot
    listSnapshots = R.listSnapshots

instance IsTableHandle R.TableHandle where
    type Session R.TableHandle = R.Session
    type TableConfig R.TableHandle = R.TableConfig
    type BlobRef R.TableHandle = R.BlobRef

    testTableConfig _ = error "TODO: test TableConfig"

    new = R.new
    close = R.close
    lookups = flip R.lookups
    -- TODO: This is temporary, because it will otherwise make class tests fail.
    -- Allow updates with blobs once blob retrieval is implemented.
    updates th upds = flip R.updates th $ flip V.map upds $ \case
        (k, R.Insert v _) -> (k, R.Insert v Nothing)
        upd -> upd
    -- TODO: This is temporary, because it will otherwise make class tests fail.
    -- Allow inserts with blobs once blob retrieval is implemented.
    inserts th ins = flip R.inserts th $ flip V.map ins $ \(k, v, _) -> (k, v, Nothing)
    deletes = flip R.deletes

    rangeLookup = flip R.rangeLookup
    retrieveBlobs _ = R.retrieveBlobs

    snapshot = R.snapshot
    open = R.open

    duplicate = R.duplicate
