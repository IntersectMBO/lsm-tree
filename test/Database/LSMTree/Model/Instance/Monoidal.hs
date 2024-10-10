{-# LANGUAGE TypeFamilies #-}

module Database.LSMTree.Model.Instance.Monoidal (
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

import           Control.Exception (Exception)
import           Data.Void (Void)
import           Database.LSMTree.Class.Monoidal
import           Database.LSMTree.Model.Instance.Normal (MSession (..),
                     SessionArgs (NoMSessionArgs), runInOpenSession)
import qualified Database.LSMTree.Model.Session as Model

data MTableHandle m k v = MTableHandle {
    _mthSession     :: !(MSession m)
  , _mthTableHandle :: !(Model.TableHandle k v Void)
  }

data MBlobRef m = MBlobRef {
    _mrbSession :: !(MSession m)
  , mrbBlobRef  :: !(Model.BlobRef Void)
  }

data MCursor m k v = MCursor {
    _mcSession :: !(MSession m)
  , _mcCursor  :: !(Model.Cursor k v Void)
  }

newtype MErr = MErr (Model.Err)
  deriving stock Show
  deriving anyclass Exception

instance IsTableHandle MTableHandle where
    type Session MTableHandle = MSession
    type TableConfig MTableHandle = Model.TableConfig
    type Cursor MTableHandle = MCursor

    new s x = MTableHandle s <$> runInOpenSession s (Model.new x)
    close (MTableHandle s t) = runInOpenSession s (Model.close t)
    lookups (MTableHandle s t) x1 = fmap convLookupResult . fmap (fmap (MBlobRef s)) <$>
      runInOpenSession s (Model.lookups x1 t)
    updates (MTableHandle s t) x1 = runInOpenSession s (Model.updates Model.getResolve (fmap (fmap convUpdate) x1) t)
    inserts (MTableHandle s t) x1 = runInOpenSession s (Model.inserts Model.getResolve (fmap (\(k, v) -> (k, v, Nothing)) x1) t)
    deletes (MTableHandle s t) x1 = runInOpenSession s (Model.deletes Model.getResolve x1 t)
    mupserts (MTableHandle s t) x1 = runInOpenSession s (Model.mupserts Model.getResolve x1 t)

    rangeLookup (MTableHandle s t) x1 = fmap convQueryResult . fmap (fmap (MBlobRef s)) <$>
      runInOpenSession s (Model.rangeLookup x1 t)

    newCursor k (MTableHandle s t) = MCursor s <$> runInOpenSession s (Model.newCursor k t)
    closeCursor _ (MCursor s c) = runInOpenSession s (Model.closeCursor c)
    readCursor _ x1 (MCursor s c) = fmap convQueryResult . fmap (fmap (MBlobRef s)) <$>
      runInOpenSession s (Model.readCursor x1 c)

    snapshot x1 (MTableHandle s t) = runInOpenSession s (Model.snapshot x1 t)
    open s x1 = MTableHandle s <$> runInOpenSession s (Model.open x1)

    duplicate (MTableHandle s t) = MTableHandle s <$> runInOpenSession s (Model.duplicate t)

    merge (MTableHandle s1 t1) (MTableHandle _s2 t2) =
        MTableHandle s1 <$> runInOpenSession s1 (Model.merge Model.getResolve t1 t2)

convLookupResult :: Model.LookupResult v b -> LookupResult v
convLookupResult = \case
    Model.NotFound -> NotFound
    Model.Found v -> Found v
    Model.FoundWithBlob{} -> error "convLookupResult: did not expect a blob"

convQueryResult :: Model.QueryResult k v b -> QueryResult k v
convQueryResult = \case
    Model.FoundInQuery k v -> FoundInQuery k v
    Model.FoundInQueryWithBlob{} -> error "convQueryResult: did not expect a blob"

convUpdate :: Update v -> Model.Update v b
convUpdate = \case
    Insert v -> Model.Insert v Nothing
    Delete   -> Model.Delete
    Mupsert v -> Model.Mupsert v
