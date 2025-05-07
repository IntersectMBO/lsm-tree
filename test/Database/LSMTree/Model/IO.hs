{-# LANGUAGE TypeFamilies #-}

-- | An instance of `Class.IsTable`, modelling potentially closed sessions in
-- @IO@ by lifting the pure session model from "Database.LSMTree.Model.Session".
module Database.LSMTree.Model.IO (
    Err (..)
  , Session (..)
  , Class.SessionArgs (NoSessionArgs)
  , Table (..)
  , TableConfig (..)
  , BlobRef (..)
  , Cursor (..)
    -- * helpers
  , runInOpenSession
  ) where

import           Control.Concurrent.Class.MonadSTM.Strict
import           Control.Exception (Exception)
import           Control.Monad.Class.MonadThrow (MonadThrow (..))
import qualified Data.List.NonEmpty as NE
import qualified Database.LSMTree.Class as Class
import           Database.LSMTree.Model.Session (TableConfig (..))
import qualified Database.LSMTree.Model.Session as Model

newtype Session m = Session (StrictTVar m (Maybe Model.Model))

data Table m k v b = Table {
    thSession :: !(Session m)
  , thTable   :: !(Model.Table k v b)
  }

data BlobRef m b = BlobRef {
    brSession :: !(Session m)
  , brBlobRef :: !(Model.BlobRef b)
  }

data Cursor m k v b = Cursor {
    cSession :: !(Session m)
  , cCursor  :: !(Model.Cursor k v b)
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

    newTableWith x s = Table s <$> runInOpenSession s (Model.new x)
    close (Table s t) = runInOpenSession s (Model.close t)
    lookups (Table s t) x1 = fmap (fmap (BlobRef s)) <$>
      runInOpenSession s (Model.lookups x1 t)
    updates (Table s t) x1 = runInOpenSession s (Model.updates Model.getResolve x1 t)
    inserts (Table s t) x1 = runInOpenSession s (Model.inserts Model.getResolve x1 t)
    deletes (Table s t) x1 = runInOpenSession s (Model.deletes Model.getResolve x1 t)
    mupserts (Table s t) x1 = runInOpenSession s (Model.mupserts Model.getResolve x1 t)

    rangeLookup (Table s t) x1 = fmap (fmap (BlobRef s)) <$>
      runInOpenSession s (Model.rangeLookup x1 t)
    retrieveBlobs _ s x1 = runInOpenSession s (Model.retrieveBlobs (fmap brBlobRef x1))

    newCursor k (Table s t) = Cursor s <$> runInOpenSession s (Model.newCursor k t)
    closeCursor _ (Cursor s c) = runInOpenSession s (Model.closeCursor c)
    readCursor _ x1 (Cursor s c) = fmap (fmap (BlobRef s)) <$>
      runInOpenSession s (Model.readCursor x1 c)

    saveSnapshot x1 x2 (Table s t) = runInOpenSession s (Model.saveSnapshot x1 x2 t)
    corruptSnapshot _ x (Table s _t) = runInOpenSession s (Model.corruptSnapshot x)
    openTableFromSnapshot s x1 x2 = Table s <$> runInOpenSession s (Model.openTableFromSnapshot x1 x2)

    duplicate (Table s t) = Table s <$> runInOpenSession s (Model.duplicate t)

    union (Table s1 t1) (Table _s2 t2) =
      Table s1 <$> runInOpenSession s1 (Model.union Model.getResolve t1 t2)
    unions ts =
        Table s <$> runInOpenSession s (Model.unions Model.getResolve (fmap thTable ts))
      where
        Table s _ = NE.head ts
    remainingUnionDebt (Table s t) = runInOpenSession s (Model.remainingUnionDebt t)
    supplyUnionCredits (Table s t) credits = runInOpenSession s (Model.supplyUnionCredits t credits)
