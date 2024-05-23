{-# LANGUAGE ConstraintKinds            #-}
{-# LANGUAGE DeriveFunctor              #-}
{-# LANGUAGE DerivingStrategies         #-}
{-# LANGUAGE EmptyDataDeriving          #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE GADTs                      #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE NamedFieldPuns             #-}
{-# LANGUAGE RankNTypes                 #-}
{-# LANGUAGE RecordWildCards            #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE StandaloneDeriving         #-}
{-# LANGUAGE TupleSections              #-}
{-# LANGUAGE TypeApplications           #-}

-- | Model a single session, allowing multiple tables.
--
-- Builds on top of "Database.LSMTree.Model.Normal", adding table handle and
-- snapshot administration.
module Database.LSMTree.Model.Normal.Session (
    -- * Model
    Model (..)
  , initModel
  , UpdateCounter (..)
    -- ** Constraints
  , C
  , C_
  , Model.SerialiseKey (..)
  , Model.SerialiseValue (..)
    -- ** ModelT and ModelM
  , ModelT (..)
  , runModelT
  , ModelM
  , runModelM
    -- ** Errors
  , Err (..)
    -- * Tables
  , TableHandle
  , TableConfig (..)
  , new
  , close
    -- * Table querying and updates
    -- ** Queries
  , Model.Range (..)
  , Model.LookupResult (..)
  , lookups
  , Model.RangeLookupResult (..)
  , rangeLookup
    -- ** Updates
  , Model.Update (..)
  , updates
  , inserts
  , deletes
    -- ** Blobs
  , BlobRef
  , retrieveBlobs
    -- * Snapshots
  , SUT.SnapshotName
  , snapshot
  , open
  , deleteSnapshot
  , listSnapshots
    -- * Multiple writable table handles
  , duplicate
  ) where

import           Control.Monad (when)
import           Control.Monad.Except (ExceptT (..), MonadError (..),
                     runExceptT)
import           Control.Monad.Identity (Identity (runIdentity))
import           Control.Monad.State.Strict (MonadState (..), StateT (..), gets,
                     modify)
import           Data.Data
import           Data.Dynamic
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import           Data.Maybe (fromJust)
import qualified Data.Vector as V
import           Data.Word
import qualified Database.LSMTree.Model.Normal as Model
import qualified Database.LSMTree.Normal as SUT

{-------------------------------------------------------------------------------
  Model
-------------------------------------------------------------------------------}

data Model = Model {
    tableHandles      :: Map TableHandleID (UpdateCounter, SomeTable)
  , nextTableHandleID :: TableHandleID
  , snapshots         :: Map SUT.SnapshotName Snapshot
  }
  deriving Show

initModel :: Model
initModel = Model {
      tableHandles = Map.empty
    , nextTableHandleID = 0
    , snapshots = Map.empty
    }

-- | We conservatively model blob reference invalidation: each update after
-- acquiring a blob reference will invalidate it. We use 'UpdateCounter' to
-- track updates.
newtype UpdateCounter = UpdateCounter Word64
  deriving (Show, Eq, Ord, Num)

newtype SomeTable = SomeTable Dynamic

instance Show SomeTable where
  show (SomeTable table) = show table

toSomeTable ::
     (Typeable k, Typeable v, Typeable blob)
  => Model.Table k v blob
  -> SomeTable
toSomeTable = SomeTable . toDyn

fromSomeTable ::
     (Typeable k, Typeable v, Typeable blob)
  => SomeTable
  -> Maybe (Model.Table k v blob)
fromSomeTable (SomeTable tbl) = fromDynamic tbl

--
-- Constraints
--

type C_ a = (Show a, Eq a, Typeable a)

-- | Common constraints for keys, values and blobs
type C k v blob = (C_ k, C_ v, C_ blob)

--
-- ModelT and ModelM
--

newtype ModelT m a = ModelT { _runModelT :: ExceptT Err (StateT Model m) a }
  deriving stock Functor
  deriving newtype ( Applicative
                   , Monad
                   , MonadState Model
                   , MonadError Err
                   )

runModelT :: ModelT m a -> Model -> m (Either Err a, Model)
runModelT = runStateT . runExceptT . _runModelT

type ModelM = ModelT Identity

runModelM :: ModelM a -> Model -> (Either Err a, Model)
runModelM m = runIdentity . runModelT m

--
-- Errors
--

data Err =
    ErrTableHandleClosed
  | ErrSnapshotExists
  | ErrSnapshotDoesNotExist
  | ErrSnapshotWrongType
  | ErrBlobRefInvalidated
  deriving (Show, Eq)

{-------------------------------------------------------------------------------
  Tables
-------------------------------------------------------------------------------}

type TableHandleID = Int

--
-- API
--

data TableHandle k v blob = TableHandle {
    tableHandleID :: TableHandleID
  , config        :: TableConfig
  }
  deriving Show

data TableConfig = TableConfig
  deriving Show

new ::
     forall k v blob m. (MonadState Model m, C k v blob)
  => TableConfig
  -> m (TableHandle k v blob)
new config = state $ \Model{..} ->
    let tableHandle = TableHandle {
            tableHandleID = nextTableHandleID
          , ..
          }
        someTable = toSomeTable $ Model.empty @k @v @blob
        tableHandles' = Map.insert nextTableHandleID (0, someTable) tableHandles
        nextTableHandleID' = nextTableHandleID + 1
        model' = Model {
            tableHandles = tableHandles'
          , nextTableHandleID = nextTableHandleID'
          , ..
          }
    in  (tableHandle, model')

-- |
--
-- This is idempotent.
close :: MonadState Model m => TableHandle k v blob -> m ()
close TableHandle{..} = state $ \Model{..} ->
    let tableHandles' = Map.delete tableHandleID tableHandles
        model' = Model {
            tableHandles = tableHandles'
          , nextTableHandleID
          , ..
          }
    in ((), model')

--
-- Utility
--

guardTableHandleIsOpen ::
     forall k v blob m. (
       MonadState Model m, MonadError Err m
     , Typeable k, Typeable v, Typeable blob
     )
  => TableHandle k v blob
  -> m (UpdateCounter, Model.Table k v blob)
guardTableHandleIsOpen TableHandle{..} =
    gets (Map.lookup tableHandleID . tableHandles) >>= \case
      Nothing ->
        throwError ErrTableHandleClosed
      Just (updc, tbl) ->
        pure (updc, fromJust $ fromSomeTable tbl)

guardTableHandleIsOpen' ::
     forall m. (MonadState Model m, MonadError Err m)
  => TableHandleID
  -> m UpdateCounter
guardTableHandleIsOpen' thid =
    gets (Map.lookup thid . tableHandles) >>= \case
      Nothing   -> throwError ErrTableHandleClosed
      Just (updc, _) -> pure updc

newTableWith ::
     (MonadState Model m, C k v blob)
  => TableConfig
  -> Model.Table k v blob
  -> m (TableHandle k v blob)
newTableWith config tbl = state $ \Model{..} ->
  let tableHandle = TableHandle {
          tableHandleID = nextTableHandleID
        , config
        }
      someTable = toSomeTable tbl
      tableHandles' = Map.insert nextTableHandleID (0, someTable) tableHandles
      nextTableHandleID' = nextTableHandleID + 1
      model' = Model {
          tableHandles = tableHandles'
        , nextTableHandleID = nextTableHandleID'
        , ..
        }
  in  (tableHandle, model')

{-------------------------------------------------------------------------------
  Table querying and updates
-------------------------------------------------------------------------------}

--
-- API
--

lookups ::
     ( MonadState Model m
     , MonadError Err m
     , Model.SerialiseKey k
     , Model.SerialiseValue v
     , C k v blob
     )
  => V.Vector k
  -> TableHandle k v blob
  -> m (V.Vector (Model.LookupResult v (BlobRef blob)))
lookups ks th = do
    (updc, table) <- guardTableHandleIsOpen th
    pure $ liftBlobRefs th updc $ Model.lookups ks table

type RangeLookupResult k v blobref = Model.RangeLookupResult k v blobref

rangeLookup ::
     ( MonadState Model m
     , MonadError Err m
     , Model.SerialiseKey k
     , Model.SerialiseValue v
     , C k v blob
     )
  => Model.Range k
  -> TableHandle k v blob
  -> m (V.Vector (RangeLookupResult k v (BlobRef blob)))
rangeLookup r th = do
    (updc, table) <- guardTableHandleIsOpen th
    pure $ liftBlobRefs th updc $ Model.rangeLookup r table

updates ::
     ( MonadState Model m
     , MonadError Err m
     , Model.SerialiseKey k
     , Model.SerialiseValue v
     , Model.SerialiseValue blob
     , C k v blob
     )
  => V.Vector (k, Model.Update v blob)
  -> TableHandle k v blob
  -> m ()
updates ups th@TableHandle{..} = do
  (updc, table) <- guardTableHandleIsOpen th
  let table' = Model.updates ups table
  modify (\m -> m {
      tableHandles = Map.insert tableHandleID (updc + 1, toSomeTable table') (tableHandles m)
    })

inserts ::
     ( MonadState Model m
     , MonadError Err m
     , Model.SerialiseKey k
     , Model.SerialiseValue v
     , Model.SerialiseValue blob
     , C k v blob
     )
  => V.Vector (k, v, Maybe blob)
  -> TableHandle k v blob
  -> m ()
inserts = updates . fmap (\(k, v, blob) -> (k, Model.Insert v blob))

deletes ::
     ( MonadState Model m
     , MonadError Err m
     , Model.SerialiseKey k
     , Model.SerialiseValue v
     , Model.SerialiseValue blob
     , C k v blob
     )
  => V.Vector k
  -> TableHandle k v blob
  -> m ()
deletes = updates . fmap (,Model.Delete)

-- | For more details: 'Database.LSMTree.Internal.BlobRef' describes the
-- intended semantics of blob references.
data BlobRef blob = BlobRef {
    parentTableID :: TableHandleID
  , createdAt     :: UpdateCounter
  , innerBlob     :: Model.BlobRef blob
  }

deriving instance Show blob => Show (BlobRef blob)

retrieveBlobs ::
     ( MonadState Model m
     , MonadError Err m
     , Model.SerialiseValue blob
     )
  => V.Vector (BlobRef blob)
  -> m (V.Vector blob)
retrieveBlobs refs = Model.retrieveBlobs <$> V.mapM guard refs
  where
    -- guard that the table is still open, and the table wasn't updated
    guard BlobRef{..} = do
        updc <- guardTableHandleIsOpen' parentTableID
        when (updc /= createdAt) $ throwError ErrBlobRefInvalidated
        pure innerBlob

--
-- Utility
--

liftBlobRefs ::
     (Functor f, Functor g)
  => TableHandle k v blob
  -> UpdateCounter
  -> g (f (Model.BlobRef blob))
  -> g (f (BlobRef blob))
liftBlobRefs th c = fmap (fmap (BlobRef (tableHandleID th) c))

{-------------------------------------------------------------------------------
  Snapshots
-------------------------------------------------------------------------------}

data Snapshot = Snapshot TableConfig SomeTable
  deriving Show

--
-- API
--

snapshot ::
     ( MonadState Model m
     , MonadError Err m
     , C k v blob
     )
  => SUT.SnapshotName
  -> TableHandle k v blob
  -> m ()
snapshot name th@TableHandle{..} = do
    table <- snd <$> guardTableHandleIsOpen th
    snaps <- gets snapshots
    when (Map.member name snaps) $
      throwError ErrSnapshotExists
    modify (\m -> m {
        snapshots = Map.insert name (Snapshot config $ toSomeTable $ Model.snapshot table) (snapshots m)
      })

open ::
     forall k v blob m.(
       MonadState Model m
     , MonadError Err m
     , C k v blob
     )
  => SUT.SnapshotName
  -> m (TableHandle k v blob)
open name = do
    snaps <- gets snapshots
    case Map.lookup name snaps of
      Nothing ->
        throwError ErrSnapshotDoesNotExist
      Just (Snapshot conf tbl) ->
        case fromSomeTable tbl of
          Nothing ->
            throwError ErrSnapshotWrongType
          Just table' ->
            newTableWith conf table'

deleteSnapshot ::
     (MonadState Model m, MonadError Err m)
  => SUT.SnapshotName
  -> m ()
deleteSnapshot name = do
    snaps <- gets snapshots
    case Map.lookup name snaps of
      Nothing ->
        throwError ErrSnapshotDoesNotExist
      Just _ ->
        modify (\m -> m {
            snapshots = Map.delete name snaps
          })

listSnapshots ::
     MonadState Model m
  => m [SUT.SnapshotName]
listSnapshots = gets (Map.keys . snapshots)

{-------------------------------------------------------------------------------
  Mutiple writable table handles
-------------------------------------------------------------------------------}

--
-- API
--

duplicate ::
     ( MonadState Model m
     , MonadError Err m
     , C k v blob
     )
  => TableHandle k v blob
  -> m (TableHandle k v blob)
duplicate th@TableHandle{..} = do
    table <- snd <$> guardTableHandleIsOpen th
    newTableWith config $ Model.duplicate table
