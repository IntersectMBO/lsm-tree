{-# LANGUAGE TypeFamilies #-}

{-# OPTIONS_GHC -Wno-orphans #-}

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

import           Control.Exception (Exception)
import           Control.Monad.Class.MonadThrow (MonadThrow (..))
import           Data.Kind (Constraint, Type)
import           Data.Typeable (Proxy (Proxy))
import qualified Data.Vector as V
import           Data.Void (Void)
import           Database.LSMTree.Class.Normal (IsSession (..), MSession (..),
                     SessionArgs (..), runInOpenSession, withSession)
import           Database.LSMTree.Common (IOLike, Labellable (..), Range (..),
                     SerialiseKey, SerialiseValue, SnapshotName)
import           Database.LSMTree.Monoidal (LookupResult (..), QueryResult (..),
                     ResolveValue, Update (..))
import qualified Database.LSMTree.Monoidal as R
import qualified Database.LSMTree.SessionModel as M2
import qualified Database.LSMTree.TableModel as M22


-- | Class abstracting over table handle operations.
--
type IsTableHandle :: ((Type -> Type) -> Type -> Type -> Type) -> Constraint
class (IsSession (Session h)) => IsTableHandle h where
    type Session h :: (Type -> Type) -> Type
    type TableConfig h :: Type
    type Cursor h :: (Type -> Type) -> Type -> Type -> Type

    new ::
           ( IOLike m
           , M2.C k v Void
           )
        => Session h m
        -> TableConfig h
        -> m (h m k v)

    close ::
           ( IOLike m
           , M2.C k v Void
           )
        => h m k v
        -> m ()

    lookups ::
           ( IOLike m
           , ResolveValue v
           , SerialiseKey k
           , SerialiseValue v
           ,  M2.C k v Void
           )
        => h m k v
        -> V.Vector k
        -> m (V.Vector (LookupResult v))

    rangeLookup ::
           ( IOLike m
           , ResolveValue v
           , SerialiseKey k
           , SerialiseValue v
           , M2.C k v Void
           )
        => h m k v
        -> Range k
        -> m (V.Vector (QueryResult k v))

    newCursor ::
           ( IOLike m
           , SerialiseKey k
           , M2.C k v Void
           )
        => Maybe k
        -> h m k v
        -> m (Cursor h m k v)

    closeCursor ::
           ( IOLike m
           , M2.C k v Void
           )
        => proxy h
        -> Cursor h m k v
        -> m ()

    readCursor ::
           ( IOLike m
           , ResolveValue v
           , SerialiseKey k
           , SerialiseValue v
           , M2.C k v Void
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
           , M2.C k v Void
           )
        => h m k v
        -> V.Vector (k, Update v)
        -> m ()

    inserts ::
           ( IOLike m
           , SerialiseKey k
           , SerialiseValue v
           , ResolveValue v
           , M2.C k v Void
           )
        => h m k v
        -> V.Vector (k, v)
        -> m ()

    deletes ::
           ( IOLike m
           , SerialiseKey k
           , SerialiseValue v
           , ResolveValue v
           , M2.C k v Void
           )
        => h m k v
        -> V.Vector k
        -> m ()

    mupserts ::
           ( IOLike m
           , SerialiseKey k
           , SerialiseValue v
           , ResolveValue v
           , M2.C k v Void
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
           , M2.C k v Void
           )
        => SnapshotName
        -> h m k v
        -> m ()

    open ::
           ( IOLike m
           , Labellable (k, v)
           , SerialiseKey k
           , SerialiseValue v
           , M2.C k v Void
           )
        => Session h m
        -> SnapshotName
        -> m (h m k v)

    duplicate ::
           ( IOLike m
           , M2.C k v Void
           )
        => h m k v
        -> m (h m k v)

    merge ::
           ( IOLike m
           , ResolveValue v
           , SerialiseValue v
           , M2.C k v Void
           )
        => h m k v
        -> h m k v
        -> m (h m k v)

withTableNew :: forall h m k v a.
     ( IOLike m
     , IsTableHandle h
     , M2.C k v Void
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
     , M2.C k v Void
     )
  => Session h m
  -> SnapshotName
  -> (h m k v -> m a)
  -> m a
withTableOpen sesh snap = bracket (open sesh snap) close

withTableDuplicate :: forall h m k v a.
     ( IOLike m
     , IsTableHandle h
     , M2.C k v Void
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
     , M2.C k v Void
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
     , M2.C k v Void
     )
  => Maybe k
  -> h m k v
  -> (Cursor h m k v -> m a)
  -> m a
withCursor offset hdl = bracket (newCursor offset hdl) (closeCursor (Proxy @h))

{-------------------------------------------------------------------------------
  Model 2 instance
-------------------------------------------------------------------------------}

data MTableHandle m k v = MTableHandle {
    _mthSession     :: !(MSession m)
  , _mthTableHandle :: !(M2.TableHandle k v Void)
  }

data MBlobRef m = MBlobRef {
    _mrbSession :: !(MSession m)
  , mrbBlobRef  :: !(M2.BlobRef Void)
  }

data MCursor m k v = MCursor {
    _mcSession :: !(MSession m)
  , _mcCursor  :: !(M2.Cursor k v Void)
  }

newtype MErr = MErr (M2.Err)
  deriving stock Show
  deriving anyclass Exception

-- runInOpenSession :: (MonadSTM m, MonadThrow (STM m)) => MSession m -> M2.ModelM a -> m a
-- runInOpenSession (MSession var) action = atomically $ do
--     readTVar var >>= \case
--       Nothing -> error "session closed"
--       Just m  -> do
--         let (r, m') = M2.runModelM action m
--         case r of
--           Left e  -> throwSTM (MErr e)
--           Right x -> writeTVar var (Just m') >> pure x

instance IsTableHandle MTableHandle where
    type Session MTableHandle = MSession
    type TableConfig MTableHandle = M2.TableConfig
    type Cursor MTableHandle = MCursor

    new s x = MTableHandle s <$> runInOpenSession s (M2.new x)
    close (MTableHandle s t) = runInOpenSession s (M2.close t)
    lookups (MTableHandle s t) x1 = fmap convLookupResult . fmap (fmap (MBlobRef s)) <$>
      runInOpenSession s (M2.lookups x1 t)
    updates (MTableHandle s t) x1 = runInOpenSession s (M2.updates (fmap (fmap convUpdate) x1) t)
    inserts (MTableHandle s t) x1 = runInOpenSession s (M2.inserts (fmap (\(k, v) -> (k, v, Nothing)) x1) t)
    deletes (MTableHandle s t) x1 = runInOpenSession s (M2.deletes x1 t)
    mupserts (MTableHandle s t) x1 = runInOpenSession s (M2.mupserts x1 t)

    rangeLookup (MTableHandle s t) x1 = fmap convQueryResult . fmap (fmap (MBlobRef s)) <$>
      runInOpenSession s (M2.rangeLookup x1 t)

    newCursor k (MTableHandle s t) = MCursor s <$> runInOpenSession s (M2.newCursor k t)
    closeCursor _ (MCursor s c) = runInOpenSession s (M2.closeCursor c)
    readCursor _ x1 (MCursor s c) = fmap convQueryResult . fmap (fmap (MBlobRef s)) <$>
      runInOpenSession s (M2.readCursor x1 c)

    snapshot x1 (MTableHandle s t) = runInOpenSession s (M2.snapshot x1 t)
    open s x1 = MTableHandle s <$> runInOpenSession s (M2.open x1)

    duplicate (MTableHandle s t) = MTableHandle s <$> runInOpenSession s (M2.duplicate t)

    merge (MTableHandle s1 t1) (MTableHandle _s2 t2) =
        MTableHandle s1 <$> runInOpenSession s1 (M2.merge t1 t2)

convLookupResult :: M22.LookupResult v b -> LookupResult v
convLookupResult = \case
    M22.NotFound -> NotFound
    M22.Found v -> Found v
    M22.FoundWithBlob{} -> error "convLookupResult: did not expect a blob"

convLookupResult' :: LookupResult v ->  M22.LookupResult v b
convLookupResult' = \case
    NotFound ->  M22.NotFound
    Found v ->  M22.Found v

convQueryResult :: M22.QueryResult k v b -> QueryResult k v
convQueryResult = \case
    M22.FoundInQuery k v -> FoundInQuery k v
    M22.FoundInQueryWithBlob{} -> error "convQueryResult: did not expect a blob"

convQueryResult' :: QueryResult k v -> M22.QueryResult k v b
convQueryResult' = \case
    FoundInQuery k v -> M22.FoundInQuery k v

convUpdate :: Update v -> M22.Update v b
convUpdate = \case
    Insert v -> M22.Insert v Nothing
    Delete   -> M22.Delete
    Mupsert v -> M22.Mupsert v

convUpdate' :: M22.Update v b -> Update v
convUpdate' = \case
    M22.Insert v Nothing -> Insert v
    M22.Insert _ (Just _) -> error "convUpdate': did not expect a blob"
    M22.Delete     -> Delete
    M22.Mupsert v -> Mupsert v

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
