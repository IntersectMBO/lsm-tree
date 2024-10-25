{-# LANGUAGE TypeFamilies #-}

-- | An instance of `Class.IsTableHandle`, modelling monoidal,
-- potentially closed sessions in @IO@ by lifting the pure session model from
-- "Database.LSMTree.Model.Session".
module Database.LSMTree.Model.IO.Monoidal (
    Err (..)
  , Session (..)
  , SessionArgs (NoSessionArgs)
  , TableHandle (..)
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

data TableHandle m k v = TableHandle {
    _thSession     :: !(Session m)
  , _thTableHandle :: !(Model.TableHandle k v Void)
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

instance Class.IsTableHandle TableHandle where
    type Session TableHandle = Session
    type TableConfig TableHandle = Model.TableConfig
    type Cursor TableHandle = Cursor

    new s x = TableHandle s <$> runInOpenSession s (Model.new x)
    close (TableHandle s t) = runInOpenSession s (Model.close t)
    lookups (TableHandle s t) x1 = fmap convLookupResult . fmap (fmap (BlobRef s)) <$>
      runInOpenSession s (Model.lookups x1 t)
    updates (TableHandle s t) x1 = runInOpenSession s (Model.updates Model.getResolve (fmap (fmap convUpdate) x1) t)
    inserts (TableHandle s t) x1 = runInOpenSession s (Model.inserts Model.getResolve (fmap (\(k, v) -> (k, v, Nothing)) x1) t)
    deletes (TableHandle s t) x1 = runInOpenSession s (Model.deletes Model.getResolve x1 t)
    mupserts (TableHandle s t) x1 = runInOpenSession s (Model.mupserts Model.getResolve x1 t)

    rangeLookup (TableHandle s t) x1 = fmap convQueryResult . fmap (fmap (BlobRef s)) <$>
      runInOpenSession s (Model.rangeLookup x1 t)

    newCursor k (TableHandle s t) = Cursor s <$> runInOpenSession s (Model.newCursor k t)
    closeCursor _ (Cursor s c) = runInOpenSession s (Model.closeCursor c)
    readCursor _ x1 (Cursor s c) = fmap convQueryResult . fmap (fmap (BlobRef s)) <$>
      runInOpenSession s (Model.readCursor x1 c)

    snapshot x1 (TableHandle s t) = runInOpenSession s (Model.snapshot x1 t)
    open s x1 = TableHandle s <$> runInOpenSession s (Model.open x1)

    duplicate (TableHandle s t) = TableHandle s <$> runInOpenSession s (Model.duplicate t)

    merge (TableHandle s1 t1) (TableHandle _s2 t2) =
        TableHandle s1 <$> runInOpenSession s1 (Model.merge Model.getResolve t1 t2)

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
