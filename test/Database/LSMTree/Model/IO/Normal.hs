{-# LANGUAGE TypeFamilies #-}

-- | An instance of `Class.IsTable`, modelling normal (i.e. non-monoidal)
-- potentially closed sessions in @IO@ by lifting the pure session model from
-- "Database.LSMTree.Model.Session".
module Database.LSMTree.Model.IO.Normal (
    Err (..)
  , Session (..)
  , Class.SessionArgs (NoSessionArgs)
  , Table (..)
  , TableConfig (..)
  , BlobRef (..)
  , Cursor (..)
    -- * helpers
  , runInOpenSession
  , convLookupResult
  , convQueryResult
  , convUpdate
  ) where

import           Control.Concurrent.Class.MonadSTM.Strict
import           Control.Exception (Exception)
import           Control.Monad.Class.MonadThrow (MonadThrow (..))
import qualified Database.LSMTree.Class.Normal as Class
import           Database.LSMTree.Model.Session (TableConfig (..))
import qualified Database.LSMTree.Model.Session as Model

newtype Session m = Session (StrictTVar m (Maybe Model.Model))

data Table m k v blob = Table {
    _thSession :: !(Session m)
  , _thTable   :: !(Model.Table k v blob)
  }

data BlobRef m blob = BlobRef {
    _brSession :: !(Session m)
  , _brBlobRef :: !(Model.BlobRef blob)
  }

data Cursor m k v blob = Cursor {
    _cSession :: !(Session m)
  , _cCursor  :: !(Model.Cursor k v blob)
  }

newtype Err = Err (Model.Err)
  deriving stock Show
  deriving anyclass Exception

runInOpenSession :: (MonadSTM m, MonadThrow (STM m)) => Session m -> Model.ModelM a -> m a
runInOpenSession (Session var) action = atomically $ do
    readTVar var >>= \case
      Nothing -> error "session closed"
      Just m  -> do
        let (r, m') = Model.runModelM action m
        case r of
          Left e  -> throwSTM (Err e)
          Right x -> writeTVar var (Just m') >> pure x

instance Class.IsSession Session where
    data SessionArgs Session m = NoSessionArgs
    openSession NoSessionArgs = Session <$> newTVarIO (Just $! Model.initModel)
    closeSession (Session var) = atomically $ writeTVar var Nothing
    deleteSnapshot s x = runInOpenSession s $ Model.deleteSnapshot x
    listSnapshots s = runInOpenSession s $ Model.listSnapshots

instance Class.IsTable Table where
    type Session Table = Session
    type TableConfig Table = Model.TableConfig
    type BlobRef Table = BlobRef
    type Cursor Table = Cursor

    new s x = Table s <$> runInOpenSession s (Model.new x)
    close (Table s t) = runInOpenSession s (Model.close t)
    lookups (Table s t) x1 = fmap convLookupResult . fmap (fmap (BlobRef s)) <$>
      runInOpenSession s (Model.lookups x1 t)
    updates (Table s t) x1 = runInOpenSession s (Model.updates Model.noResolve (fmap (fmap convUpdate) x1) t)
    inserts (Table s t) x1 = runInOpenSession s (Model.inserts Model.noResolve x1 t)
    deletes (Table s t) x1 = runInOpenSession s (Model.deletes Model.noResolve x1 t)

    rangeLookup (Table s t) x1 = fmap convQueryResult . fmap (fmap (BlobRef s)) <$>
      runInOpenSession s (Model.rangeLookup x1 t)
    retrieveBlobs _ s x1 = runInOpenSession s (Model.retrieveBlobs (fmap _brBlobRef x1))

    newCursor k (Table s t) = Cursor s <$> runInOpenSession s (Model.newCursor k t)
    closeCursor _ (Cursor s c) = runInOpenSession s (Model.closeCursor c)
    readCursor _ x1 (Cursor s c) = fmap convQueryResult . fmap (fmap (BlobRef s)) <$>
      runInOpenSession s (Model.readCursor x1 c)

    snapshot x1 (Table s t) = runInOpenSession s (Model.snapshot x1 t)
    open s x1 = Table s <$> runInOpenSession s (Model.open x1)

    duplicate (Table s t) = Table s <$> runInOpenSession s (Model.duplicate t)

convLookupResult :: Model.LookupResult v b -> Class.LookupResult v b
convLookupResult = \case
    Model.NotFound -> Class.NotFound
    Model.Found v -> Class.Found v
    Model.FoundWithBlob v b -> Class.FoundWithBlob v b

convQueryResult :: Model.QueryResult k v b -> Class.QueryResult k v b
convQueryResult = \case
    Model.FoundInQuery k v -> Class.FoundInQuery k v
    Model.FoundInQueryWithBlob k v b -> Class.FoundInQueryWithBlob k v b

convUpdate :: Class.Update v b -> Model.Update v b
convUpdate = \case
    Class.Insert v b -> Model.Insert v b
    Class.Delete     -> Model.Delete
