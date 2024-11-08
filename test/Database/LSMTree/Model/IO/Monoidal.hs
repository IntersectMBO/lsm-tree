{-# LANGUAGE TypeFamilies #-}

-- | An instance of `Class.IsTable`, modelling monoidal,
-- potentially closed sessions in @IO@ by lifting the pure session model from
-- "Database.LSMTree.Model.Session".
module Database.LSMTree.Model.IO.Monoidal (
    Err (..)
  , Session (..)
  , SessionArgs (NoSessionArgs)
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

import           Control.Exception (Exception)
import           Data.Void (Void)
import qualified Database.LSMTree.Class.Monoidal as Class
import           Database.LSMTree.Model.IO.Normal (Session (..),
                     SessionArgs (NoSessionArgs), runInOpenSession)
import           Database.LSMTree.Model.Session (TableConfig (..))
import qualified Database.LSMTree.Model.Session as Model

data Table m k v = Table {
    _thSession :: !(Session m)
  , _thTable   :: !(Model.Table k v Void)
  }

data BlobRef m = BlobRef {
    _brSession :: !(Session m)
  , _brBlobRef :: !(Model.BlobRef Void)
  }

data Cursor m k v = Cursor {
    _cSession :: !(Session m)
  , _cCursor  :: !(Model.Cursor k v Void)
  }

newtype Err = Err (Model.Err)
  deriving stock Show
  deriving anyclass Exception

instance Class.IsTable Table where
    type Session Table = Session
    type TableConfig Table = Model.TableConfig
    type Cursor Table = Cursor

    new s x = Table s <$> runInOpenSession s (Model.new x)
    close (Table s t) = runInOpenSession s (Model.close t)
    lookups (Table s t) x1 = fmap convLookupResult . fmap (fmap (BlobRef s)) <$>
      runInOpenSession s (Model.lookups x1 t)
    updates (Table s t) x1 = runInOpenSession s (Model.updates Model.getResolve (fmap (fmap convUpdate) x1) t)
    inserts (Table s t) x1 = runInOpenSession s (Model.inserts Model.getResolve (fmap (\(k, v) -> (k, v, Nothing)) x1) t)
    deletes (Table s t) x1 = runInOpenSession s (Model.deletes Model.getResolve x1 t)
    mupserts (Table s t) x1 = runInOpenSession s (Model.mupserts Model.getResolve x1 t)

    rangeLookup (Table s t) x1 = fmap convQueryResult . fmap (fmap (BlobRef s)) <$>
      runInOpenSession s (Model.rangeLookup x1 t)

    newCursor k (Table s t) = Cursor s <$> runInOpenSession s (Model.newCursor k t)
    closeCursor _ (Cursor s c) = runInOpenSession s (Model.closeCursor c)
    readCursor _ x1 (Cursor s c) = fmap convQueryResult . fmap (fmap (BlobRef s)) <$>
      runInOpenSession s (Model.readCursor x1 c)

    createSnapshot x1 (Table s t) = runInOpenSession s (Model.createSnapshot x1 t)
    openSnapshot s x1 = Table s <$> runInOpenSession s (Model.openSnapshot x1)

    duplicate (Table s t) = Table s <$> runInOpenSession s (Model.duplicate t)

    union (Table s1 t1) (Table _s2 t2) =
        Table s1 <$> runInOpenSession s1 (Model.union Model.getResolve t1 t2)

convLookupResult :: Model.LookupResult v b -> Class.LookupResult v
convLookupResult = \case
    Model.NotFound -> Class.NotFound
    Model.Found v -> Class.Found v
    Model.FoundWithBlob{} -> error "convLookupResult: did not expect a blob"

convQueryResult :: Model.QueryResult k v b -> Class.QueryResult k v
convQueryResult = \case
    Model.FoundInQuery k v -> Class.FoundInQuery k v
    Model.FoundInQueryWithBlob{} -> error "convQueryResult: did not expect a blob"

convUpdate :: Class.Update v -> Model.Update v b
convUpdate = \case
    Class.Insert v -> Model.Insert v Nothing
    Class.Delete   -> Model.Delete
    Class.Mupsert v -> Model.Mupsert v
