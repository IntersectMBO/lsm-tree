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
  , Model.QueryResult (..)
  , rangeLookup
    -- ** Cursor
  , Cursor
  , newCursor
  , closeCursor
  , readCursor
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
    tableHandles :: Map TableHandleID (UpdateCounter, SomeTable)
  , cursors      :: Map CursorID SomeCursor
  , nextID       :: Int
  , snapshots    :: Map SUT.SnapshotName Snapshot
  }
  deriving stock Show

initModel :: Model
initModel = Model {
      tableHandles = Map.empty
    , cursors = Map.empty
    , nextID = 0
    , snapshots = Map.empty
    }

-- | We conservatively model blob reference invalidation: each update after
-- acquiring a blob reference will invalidate it. We use 'UpdateCounter' to
-- track updates.
newtype UpdateCounter = UpdateCounter Word64
  deriving stock (Show, Eq, Ord)
  deriving newtype (Num)

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

newtype SomeCursor = SomeCursor Dynamic

instance Show SomeCursor where
  show (SomeCursor c) = show c

toSomeCursor ::
     (Typeable k, Typeable v, Typeable blob)
  => Model.Cursor k v blob
  -> SomeCursor
toSomeCursor = SomeCursor . toDyn

fromSomeCursor ::
     (Typeable k, Typeable v, Typeable blob)
  => SomeCursor
  -> Maybe (Model.Cursor k v blob)
fromSomeCursor (SomeCursor c) = fromDynamic c


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
  | ErrCursorClosed
  deriving stock (Show, Eq)

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
  deriving stock Show

data TableConfig = TableConfig
  deriving stock Show

new ::
     forall k v blob m. (MonadState Model m, C k v blob)
  => TableConfig
  -> m (TableHandle k v blob)
new config = state $ \Model{..} ->
    let tableHandle = TableHandle {
            tableHandleID = nextID
          , ..
          }
        someTable = toSomeTable $ Model.empty @k @v @blob
        tableHandles' = Map.insert nextID (0, someTable) tableHandles
        nextID' = nextID + 1
        model' = Model {
            tableHandles = tableHandles'
          , nextID = nextID'
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

newTableWith ::
     (MonadState Model m, C k v blob)
  => TableConfig
  -> Model.Table k v blob
  -> m (TableHandle k v blob)
newTableWith config tbl = state $ \Model{..} ->
  let tableHandle = TableHandle {
          tableHandleID = nextID
        , config
        }
      someTable = toSomeTable tbl
      tableHandles' = Map.insert nextID (0, someTable) tableHandles
      nextID' = nextID + 1
      model' = Model {
          tableHandles = tableHandles'
        , nextID = nextID'
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
    pure $ liftBlobRefs (SomeTableID updc (tableHandleID th)) $ Model.lookups ks table

type QueryResult k v blobref = Model.QueryResult k v blobref

rangeLookup ::
     ( MonadState Model m
     , MonadError Err m
     , Model.SerialiseKey k
     , Model.SerialiseValue v
     , C k v blob
     )
  => Model.Range k
  -> TableHandle k v blob
  -> m (V.Vector (QueryResult k v (BlobRef blob)))
rangeLookup r th = do
    (updc, table) <- guardTableHandleIsOpen th
    pure $ liftBlobRefs (SomeTableID updc (tableHandleID th)) $ Model.rangeLookup r table

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
    handleRef :: !(SomeHandleID blob)
  , innerBlob :: !(Model.BlobRef blob)
  }

deriving stock instance Show blob => Show (BlobRef blob)

retrieveBlobs ::
     forall m blob. ( MonadState Model m
     , MonadError Err m
     , Model.SerialiseValue blob
     )
  => V.Vector (BlobRef blob)
  -> m (V.Vector blob)
retrieveBlobs refs = Model.retrieveBlobs <$> V.mapM guard refs
  where
    guard BlobRef{..} = do
        m <- get
        -- In the real implementation, a blob reference /could/ be invalidated
        -- every time you modify a table or cursor. This model takes the most
        -- conservative approach: a blob reference is immediately invalidated
        -- every time a table/cursor is modified.
        case handleRef of
          SomeTableID createdAt tableID ->
            case Map.lookup tableID (tableHandles m) of
              -- If the table is now closed, it means the table has been modified.
              Nothing -> errInvalid
              -- If the table is open, we check timestamps (i.e., UpdateCounter)
              -- to see if any modifications were made.
              Just (updc, _) -> do
                when (updc /= createdAt) $ errInvalid
                pure innerBlob
          SomeCursorID cursorID ->
            -- The only modification to a cursor is that it can be closed.
            case Map.lookup cursorID (cursors m) of
              Nothing -> errInvalid
              Just _  -> pure innerBlob

    errInvalid :: m a
    errInvalid = throwError ErrBlobRefInvalidated

--
-- Utility
--

data SomeHandleID blob where
  SomeTableID  :: !UpdateCounter -> !TableHandleID -> SomeHandleID blob
  SomeCursorID :: !CursorID -> SomeHandleID blob
  deriving stock Show

liftBlobRefs ::
     (Functor f, Functor g)
  => SomeHandleID blob
  -> g (f (Model.BlobRef blob))
  -> g (f (BlobRef blob))
liftBlobRefs hid = fmap (fmap (BlobRef hid))

{-------------------------------------------------------------------------------
  Snapshots
-------------------------------------------------------------------------------}

data Snapshot = Snapshot TableConfig SomeTable
  deriving stock Show

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

{-------------------------------------------------------------------------------
  Cursor
-------------------------------------------------------------------------------}

type CursorID = Int

data Cursor k v blob = Cursor {
    cursorID :: !CursorID
  }
  deriving stock Show

newCursor ::
     forall k v blob m. (
       MonadState Model m, MonadError Err m
     , Model.SerialiseKey k
     , C k v blob
     )
  => Maybe k
  -> TableHandle k v blob
  -> m (Cursor k v blob)
newCursor offset th = do
  table <- snd <$> guardTableHandleIsOpen th
  state $ \Model{..} ->
    let cursor = Cursor { cursorID = nextID }
        someCursor = toSomeCursor $ Model.newCursor offset table
        cursors' = Map.insert nextID someCursor cursors
        nextID' = nextID + 1
        model' = Model {
            cursors = cursors'
          , nextID = nextID'
          , ..
          }
    in  (cursor, model')

closeCursor :: MonadState Model m => Cursor k v blob -> m ()
closeCursor Cursor {..} = state $ \Model{..} ->
    let cursors' = Map.delete cursorID cursors
        model' = Model {
            cursors = cursors'
          , ..
          }
    in ((), model')

readCursor ::
     ( MonadState Model m
     , MonadError Err m
     , Model.SerialiseKey k
     , Model.SerialiseValue v
     , C k v blob
     )
  => Int
  -> Cursor k v blob
  -> m (V.Vector (QueryResult k v (BlobRef blob)))
readCursor n c = do
    cursor <- guardCursorIsOpen c
    let (qrs, cursor') = Model.readCursor n cursor
    modify (\m -> m {
        cursors = Map.insert (cursorID c) (toSomeCursor cursor') (cursors m)
      })
    pure $ liftBlobRefs (SomeCursorID (cursorID c)) $ qrs

guardCursorIsOpen ::
     forall k v blob m. (
       MonadState Model m, MonadError Err m
     , Typeable k, Typeable v, Typeable blob
     )
  => Cursor k v blob
  -> m (Model.Cursor k v blob)
guardCursorIsOpen Cursor{..} =
    gets (Map.lookup cursorID . cursors) >>= \case
      Nothing ->
        throwError ErrCursorClosed
      Just c ->
        pure (fromJust $ fromSomeCursor c)
