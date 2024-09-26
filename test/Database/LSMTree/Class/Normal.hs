{-# LANGUAGE TypeFamilies #-}

module Database.LSMTree.Class.Normal (
    IsSession (..)
  , SessionArgs (..)
  , withSession
  , IsTableHandle (..)
  , withTableNew
  , withTableOpen
  , withTableDuplicate
  , withCursor
    -- * Model 2 instance
  , runInOpenSession
  , convLookupResult
  , convLookupResult'
  , convQueryResult
  , convQueryResult'
  , convUpdate
  , convUpdate'
  , MSession (..)
  , MTableHandle (..)
  , MCursor (..)
  , MErr (..)
  , MBlobRef (..)
  ) where

import           Control.Concurrent.Class.MonadSTM.Strict
import           Control.Exception (Exception)
import           Control.Monad.Class.MonadST (MonadST)
import           Control.Monad.Class.MonadThrow (MonadMask, MonadThrow (..))
import           Control.Monad.Fix (MonadFix)
import           Control.Tracer (nullTracer)
import           Data.Kind (Constraint, Type)
import           Data.Typeable (Proxy (Proxy), Typeable)
import qualified Data.Vector as V
import           Database.LSMTree.Common (IOLike, Labellable (..), Range (..),
                     SerialiseKey, SerialiseValue, SnapshotName)
import qualified Database.LSMTree.Monoidal as Monoidal
import           Database.LSMTree.Normal (LookupResult (..), QueryResult (..),
                     Update (..))
import qualified Database.LSMTree.Normal as R
import qualified Database.LSMTree.SessionModel as M2
import qualified Database.LSMTree.TableModel as M22
import           System.FS.API (FsPath, HasFS)
import           System.FS.BlockIO.API (HasBlockIO)

type IsSession :: ((Type -> Type) -> Type) -> Constraint
class IsSession s where
    data SessionArgs s :: (Type -> Type) -> Type

    openSession ::
           IOLike m
        => SessionArgs s m
        -> m (s m)

    closeSession ::
           IOLike m
        => s m
        -> m ()

    deleteSnapshot ::
           IOLike m
        => s m
        -> SnapshotName
        -> m ()

    listSnapshots ::
           IOLike m
        => s m
        -> m [SnapshotName]

withSession :: (IOLike m, IsSession s) => SessionArgs s m -> (s m -> m a) -> m a
withSession seshArgs = bracket (openSession seshArgs) closeSession

-- | Class abstracting over table handle operations.
--
type IsTableHandle :: ((Type -> Type) -> Type -> Type -> Type -> Type) -> Constraint
class (IsSession (Session h)) => IsTableHandle h where
    type Session h :: (Type -> Type) -> Type
    type TableConfig h :: Type
    type BlobRef h :: (Type -> Type) -> Type -> Type
    type Cursor h :: (Type -> Type) -> Type -> Type -> Type -> Type

    new ::
           ( IOLike m
           , M2.C k v blob
           )
        => Session h m
        -> TableConfig h
        -> m (h m k v blob)

    close ::
           ( IOLike m
           , M2.C k v blob
           )
        => h m k v blob
        -> m ()

    lookups ::
           ( IOLike m
           , SerialiseKey k
           , SerialiseValue v
           , M2.C k v blob
           )
        => h m k v blob
        -> V.Vector k
        -> m (V.Vector (LookupResult v (BlobRef h m blob)))

    rangeLookup ::
           ( IOLike m
           , SerialiseKey k
           , SerialiseValue v
           , M2.C k v blob
           )
        => h m k v blob
        -> Range k
        -> m (V.Vector (QueryResult k v (BlobRef h m blob)))

    newCursor ::
           ( IOLike m
           , SerialiseKey k
           , M2.C k v blob
           )
        => Maybe k
        -> h m k v blob
        -> m (Cursor h m k v blob)

    closeCursor ::
           ( IOLike m
           , M2.C k v blob
           )
        => proxy h
        -> Cursor h m k v blob
        -> m ()

    readCursor ::
           ( IOLike m
           , SerialiseKey k
           , SerialiseValue v
           , M2.C k v blob
           )
        => proxy h
        -> Int
        -> Cursor h m k v blob
        -> m (V.Vector (QueryResult k v (BlobRef h m blob)))

    retrieveBlobs ::
           ( IOLike m
           , SerialiseValue blob
           , M2.C_ blob
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
           , Monoidal.ResolveValue v
           , M2.C k v blob
           )
        => h m k v blob
        -> V.Vector (k, Update v blob)
        -> m ()

    inserts ::
           ( IOLike m
           , SerialiseKey k
           , SerialiseValue v
           , SerialiseValue blob
           , Monoidal.ResolveValue v
           , M2.C k v blob
           )
        => h m k v blob
        -> V.Vector (k, v, Maybe blob)
        -> m ()

    deletes ::
           ( IOLike m
           , SerialiseKey k
           , SerialiseValue v
           , SerialiseValue blob
           , Monoidal.ResolveValue v
           , M2.C k v blob
           )
        => h m k v blob
        -> V.Vector k
        -> m ()

    snapshot ::
           ( IOLike m
           , Labellable (k, v, blob)
           , SerialiseKey k
           , SerialiseValue v
           , SerialiseValue blob
           , M2.C k v blob
           )
        => SnapshotName
        -> h m k v blob
        -> m ()

    open ::
           ( IOLike m
           , Labellable (k, v, blob)
           , SerialiseKey k
           , SerialiseValue v
           , SerialiseValue blob
           , M2.C k v blob
           )
        => Session h m
        -> SnapshotName
        -> m (h m k v blob)

    duplicate ::
           ( IOLike m
           , M2.C k v blob
           )
        => h m k v blob
        -> m (h m k v blob)

withTableNew :: forall h m k v blob a.
    ( IOLike m
    , IsTableHandle h
    , M2.C k v blob
    )
  => Session h m
  -> TableConfig h
  -> (h m k v blob -> m a)
  -> m a
withTableNew sesh conf = bracket (new sesh conf) close

withTableOpen :: forall h m k v blob a.
     ( IOLike m, IsTableHandle h, Labellable (k, v, blob)
     , SerialiseKey k, SerialiseValue v, SerialiseValue blob
     , M2.C k v blob
     )
  => Session h m
  -> SnapshotName
  -> (h m k v blob -> m a)
  -> m a
withTableOpen sesh snap = bracket (open sesh snap) close

withTableDuplicate :: forall h m k v blob a.
     ( IOLike m
     , IsTableHandle h
     , M2.C k v blob
     )
  => h m k v blob
  -> (h m k v blob -> m a)
  -> m a
withTableDuplicate table = bracket (duplicate table) close

withCursor :: forall h m k v blob a.
     ( IOLike m
     , IsTableHandle h
     , SerialiseKey k
     , M2.C k v blob
     )
  => Maybe k
  -> h m k v blob
  -> (Cursor h m k v blob -> m a)
  -> m a
withCursor offset hdl = bracket (newCursor offset hdl) (closeCursor (Proxy @h))

{-------------------------------------------------------------------------------
  Model 2 instance
-------------------------------------------------------------------------------}

newtype MSession m = MSession (StrictTVar m (Maybe M2.Model))

data MTableHandle m k v blob = MTableHandle {
    _mthSession     :: !(MSession m)
  , _mthTableHandle :: !(M2.TableHandle k v blob)
  }

data MBlobRef m blob = MBlobRef {
    _mrbSession :: !(MSession m)
  , mrbBlobRef  :: !(M2.BlobRef blob)
  }

data MCursor m k v blob = MCursor {
    _mcSession :: !(MSession m)
  , _mcCursor  :: !(M2.Cursor k v blob)
  }

newtype MErr = MErr (M2.Err)
  deriving stock Show
  deriving anyclass Exception

runInOpenSession :: (MonadSTM m, MonadThrow (STM m)) => MSession m -> M2.ModelM a -> m a
runInOpenSession (MSession var) action = atomically $ do
    readTVar var >>= \case
      Nothing -> error "session closed"
      Just m  -> do
        let (r, m') = M2.runModelM action m
        case r of
          Left e  -> throwSTM (MErr e)
          Right x -> writeTVar var (Just m') >> pure x

instance IsSession MSession where
    data SessionArgs MSession m = NoMSessionArgs
    openSession NoMSessionArgs = MSession <$> newTVarIO (Just $! M2.initModel)
    closeSession (MSession var) = atomically $ writeTVar var Nothing
    deleteSnapshot s x = runInOpenSession s $ M2.deleteSnapshot x
    listSnapshots s = runInOpenSession s $ M2.listSnapshots

instance IsTableHandle MTableHandle where
    type Session MTableHandle = MSession
    type TableConfig MTableHandle = M2.TableConfig
    type BlobRef MTableHandle = MBlobRef
    type Cursor MTableHandle = MCursor

    new s x = MTableHandle s <$> runInOpenSession s (M2.new x)
    close (MTableHandle s t) = runInOpenSession s (M2.close t)
    lookups (MTableHandle s t) x1 = fmap convLookupResult . fmap (fmap (MBlobRef s)) <$>
      runInOpenSession s (M2.lookups x1 t)
    updates (MTableHandle s t) x1 = runInOpenSession s (M2.updates (fmap (fmap convUpdate) x1) t)
    inserts (MTableHandle s t) x1 = runInOpenSession s (M2.inserts x1 t)
    deletes (MTableHandle s t) x1 = runInOpenSession s (M2.deletes x1 t)

    rangeLookup (MTableHandle s t) x1 = fmap convQueryResult . fmap (fmap (MBlobRef s)) <$>
      runInOpenSession s (M2.rangeLookup x1 t)
    retrieveBlobs _ s x1 = runInOpenSession s (M2.retrieveBlobs (fmap mrbBlobRef x1))

    newCursor k (MTableHandle s t) = MCursor s <$> runInOpenSession s (M2.newCursor k t)
    closeCursor _ (MCursor s c) = runInOpenSession s (M2.closeCursor c)
    readCursor _ x1 (MCursor s c) = fmap convQueryResult . fmap (fmap (MBlobRef s)) <$>
      runInOpenSession s (M2.readCursor x1 c)

    snapshot x1 (MTableHandle s t) = runInOpenSession s (M2.snapshot x1 t)
    open s x1 = MTableHandle s <$> runInOpenSession s (M2.open x1)

    duplicate (MTableHandle s t) = MTableHandle s <$> runInOpenSession s (M2.duplicate t)

convLookupResult :: M22.LookupResult v b -> LookupResult v b
convLookupResult = \case
    M22.NotFound -> NotFound
    M22.Found v -> Found v
    M22.FoundWithBlob v b -> FoundWithBlob v b

convLookupResult' :: LookupResult v b ->  M22.LookupResult v b
convLookupResult' = \case
    NotFound ->  M22.NotFound
    Found v ->  M22.Found v
    FoundWithBlob v b ->  M22.FoundWithBlob v b

convQueryResult :: M22.QueryResult k v b -> QueryResult k v b
convQueryResult = \case
    M22.FoundInQuery k v -> FoundInQuery k v
    M22.FoundInQueryWithBlob k v b -> FoundInQueryWithBlob k v b

convQueryResult' :: QueryResult k v b -> M22.QueryResult k v b
convQueryResult' = \case
    FoundInQuery k v -> M22.FoundInQuery k v
    FoundInQueryWithBlob k v b -> M22.FoundInQueryWithBlob k v b

convUpdate :: Update v b -> M22.Update v b
convUpdate = \case
    Insert v b -> M22.Insert v b
    Delete     -> M22.Delete

convUpdate' :: M22.Update v b -> Update v b
convUpdate' = \case
    M22.Insert v b -> Insert v b
    M22.Delete     -> Delete
    M22.Mupsert _  -> error "convUpdate': did not expect a Mupsert"

{-------------------------------------------------------------------------------
  Real instance
-------------------------------------------------------------------------------}

instance IsSession R.Session where
    data SessionArgs R.Session m where
      SessionArgs ::
           forall m h. (MonadFix m, MonadMask m, MonadST m, Typeable h)
        => HasFS m h -> HasBlockIO m h -> FsPath
        -> SessionArgs R.Session m

    openSession (SessionArgs hfs hbio dir) = do
       R.openSession nullTracer hfs hbio dir
    closeSession = R.closeSession
    deleteSnapshot = R.deleteSnapshot
    listSnapshots = R.listSnapshots

instance IsTableHandle R.TableHandle where
    type Session R.TableHandle = R.Session
    type TableConfig R.TableHandle = R.TableConfig
    type BlobRef R.TableHandle = R.BlobRef
    type Cursor R.TableHandle = R.Cursor

    new = R.new
    close = R.close
    lookups = flip R.lookups
    updates = flip R.updates
    inserts = flip R.inserts
    deletes = flip R.deletes

    rangeLookup = flip R.rangeLookup
    retrieveBlobs _ = R.retrieveBlobs

    newCursor = maybe R.newCursor R.newCursorAtOffset
    closeCursor _ = R.closeCursor
    readCursor _ = R.readCursor

    snapshot = R.snapshot
    open sesh snap = R.open sesh R.configNoOverride snap

    duplicate = R.duplicate
