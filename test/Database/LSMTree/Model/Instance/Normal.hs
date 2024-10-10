{-# LANGUAGE TypeFamilies #-}

module Database.LSMTree.Model.Instance.Normal (
    runInOpenSession
  , convLookupResult
  , convQueryResult
  , convUpdate
  , MSession (..)
  , MTableHandle (..)
  , MCursor (..)
  , MErr (..)
  , MBlobRef (..)
  , SessionArgs (NoMSessionArgs)
  ) where

import           Control.Concurrent.Class.MonadSTM.Strict
import           Control.Exception (Exception)
import           Control.Monad.Class.MonadThrow (MonadThrow (..))
import           Database.LSMTree.Class.Normal
import qualified Database.LSMTree.Model.Session as Model

newtype MSession m = MSession (StrictTVar m (Maybe Model.Model))

data MTableHandle m k v blob = MTableHandle {
    _mthSession     :: !(MSession m)
  , _mthTableHandle :: !(Model.TableHandle k v blob)
  }

data MBlobRef m blob = MBlobRef {
    _mrbSession :: !(MSession m)
  , mrbBlobRef  :: !(Model.BlobRef blob)
  }

data MCursor m k v blob = MCursor {
    _mcSession :: !(MSession m)
  , _mcCursor  :: !(Model.Cursor k v blob)
  }

newtype MErr = MErr (Model.Err)
  deriving stock Show
  deriving anyclass Exception

runInOpenSession :: (MonadSTM m, MonadThrow (STM m)) => MSession m -> Model.ModelM a -> m a
runInOpenSession (MSession var) action = atomically $ do
    readTVar var >>= \case
      Nothing -> error "session closed"
      Just m  -> do
        let (r, m') = Model.runModelM action m
        case r of
          Left e  -> throwSTM (MErr e)
          Right x -> writeTVar var (Just m') >> pure x

instance IsSession MSession where
    data SessionArgs MSession m = NoMSessionArgs
    openSession NoMSessionArgs = MSession <$> newTVarIO (Just $! Model.initModel)
    closeSession (MSession var) = atomically $ writeTVar var Nothing
    deleteSnapshot s x = runInOpenSession s $ Model.deleteSnapshot x
    listSnapshots s = runInOpenSession s $ Model.listSnapshots

instance IsTableHandle MTableHandle where
    type Session MTableHandle = MSession
    type TableConfig MTableHandle = Model.TableConfig
    type BlobRef MTableHandle = MBlobRef
    type Cursor MTableHandle = MCursor

    new s x = MTableHandle s <$> runInOpenSession s (Model.new x)
    close (MTableHandle s t) = runInOpenSession s (Model.close t)
    lookups (MTableHandle s t) x1 = fmap convLookupResult . fmap (fmap (MBlobRef s)) <$>
      runInOpenSession s (Model.lookups x1 t)
    updates (MTableHandle s t) x1 = runInOpenSession s (Model.updates Model.noResolve (fmap (fmap convUpdate) x1) t)
    inserts (MTableHandle s t) x1 = runInOpenSession s (Model.inserts Model.noResolve x1 t)
    deletes (MTableHandle s t) x1 = runInOpenSession s (Model.deletes Model.noResolve x1 t)

    rangeLookup (MTableHandle s t) x1 = fmap convQueryResult . fmap (fmap (MBlobRef s)) <$>
      runInOpenSession s (Model.rangeLookup x1 t)
    retrieveBlobs _ s x1 = runInOpenSession s (Model.retrieveBlobs (fmap mrbBlobRef x1))

    newCursor k (MTableHandle s t) = MCursor s <$> runInOpenSession s (Model.newCursor k t)
    closeCursor _ (MCursor s c) = runInOpenSession s (Model.closeCursor c)
    readCursor _ x1 (MCursor s c) = fmap convQueryResult . fmap (fmap (MBlobRef s)) <$>
      runInOpenSession s (Model.readCursor x1 c)

    snapshot x1 (MTableHandle s t) = runInOpenSession s (Model.snapshot x1 t)
    open s x1 = MTableHandle s <$> runInOpenSession s (Model.open x1)

    duplicate (MTableHandle s t) = MTableHandle s <$> runInOpenSession s (Model.duplicate t)

convLookupResult :: Model.LookupResult v b -> LookupResult v b
convLookupResult = \case
    Model.NotFound -> NotFound
    Model.Found v -> Found v
    Model.FoundWithBlob v b -> FoundWithBlob v b

convQueryResult :: Model.QueryResult k v b -> QueryResult k v b
convQueryResult = \case
    Model.FoundInQuery k v -> FoundInQuery k v
    Model.FoundInQueryWithBlob k v b -> FoundInQueryWithBlob k v b

convUpdate :: Update v b -> Model.Update v b
convUpdate = \case
    Insert v b -> Model.Insert v b
    Delete     -> Model.Delete
