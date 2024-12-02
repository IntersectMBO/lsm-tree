-- | A pure model of a single session containing multiple tables.
--
-- This model supports all features for /both/ normal and monoidal tables,
-- in particular both blobs and mupserts. The former is typically only provided
-- by the normal API, and the latter only by the monoidal API.
--
-- The session model builds on top of "Database.LSMTree.Model.Table", adding
-- table and snapshot administration.
module Database.LSMTree.Model.Session (
  -- * Model
    Model (..)
  , initModel
  , UpdateCounter (..)
    -- ** SomeTable, for testing
  , SomeTable (..)
  , toSomeTable
  , fromSomeTable
  , withSomeTable
  , TableID
  , tableID
  , Model.size
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
  , Table
  , TableConfig (..)
  , new
  , close
    -- * Monoidal value resolution
  , ResolveSerialisedValue (..)
  , getResolve
  , noResolve
    -- * Table querying and updates
    -- ** Queries
  , Range (..)
  , LookupResult (..)
  , lookups
  , QueryResult (..)
  , rangeLookup
    -- ** Cursor
  , Cursor
  , CursorID
  , cursorID
  , newCursor
  , closeCursor
  , readCursor
    -- ** Updates
  , Update (..)
  , updates
  , inserts
  , deletes
  , mupserts
    -- ** Blobs
  , BlobRef
  , retrieveBlobs
    -- * Snapshots
  , SnapshotName
  , createSnapshot
  , openSnapshot
  , deleteSnapshot
  , listSnapshots
    -- * Multiple writable tables
  , duplicate
    -- * Table union
  , union
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
import           Database.LSMTree.Common (SerialiseKey (..),
                     SerialiseValue (..), SnapshotLabel, SnapshotName)
import           Database.LSMTree.Model.Table (LookupResult (..),
                     QueryResult (..), Range (..), ResolveSerialisedValue (..),
                     Update (..), getResolve, noResolve)
import qualified Database.LSMTree.Model.Table as Model

{-------------------------------------------------------------------------------
  Model
-------------------------------------------------------------------------------}

data Model = Model {
    tables    :: Map TableID (UpdateCounter, SomeTable)
  , cursors   :: Map CursorID SomeCursor
  , nextID    :: Int
  , snapshots :: Map SnapshotName Snapshot
  }
  deriving stock Show

initModel :: Model
initModel = Model {
      tables = Map.empty
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

data SomeTable where
     SomeTable :: (Typeable k, Typeable v, Typeable blob)
               => Model.Table k v blob -> SomeTable

instance Show SomeTable where
  show (SomeTable table) = show table

toSomeTable ::
     (Typeable k, Typeable v, Typeable blob)
  => Model.Table k v blob
  -> SomeTable
toSomeTable = SomeTable

fromSomeTable ::
     (Typeable k, Typeable v, Typeable blob)
  => SomeTable
  -> Maybe (Model.Table k v blob)
fromSomeTable (SomeTable tbl) = cast tbl

withSomeTable ::
     (forall k v blob. (Typeable k, Typeable v, Typeable blob)
                    => Model.Table k v blob -> a)
  -> SomeTable
  -> a
withSomeTable f (SomeTable tbl) = f tbl

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

-- | Common constraints for keys, values and blobs
type C_ a = (Show a, Eq a, Typeable a)
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
    ErrTableClosed
  | ErrSnapshotExists
  | ErrSnapshotDoesNotExist
  | ErrSnapshotWrongType
  | ErrBlobRefInvalidated
  | ErrCursorClosed
  deriving stock (Show, Eq)

{-------------------------------------------------------------------------------
  Tables
-------------------------------------------------------------------------------}

type TableID = Int

--
-- API
--

data Table k v blob = Table {
    tableID :: TableID
  , config  :: TableConfig
  }
  deriving stock Show

data TableConfig = TableConfig
  deriving stock (Show, Eq)

new ::
     forall k v blob m. (MonadState Model m, C k v blob)
  => TableConfig
  -> m (Table k v blob)
new config = newTableWith config Model.empty

-- |
--
-- This is idempotent.
close :: MonadState Model m => Table k v blob -> m ()
close Table{..} = state $ \Model{..} ->
    let tables' = Map.delete tableID tables
        model' = Model {
            tables = tables'
          , ..
          }
    in ((), model')

--
-- Utility
--

guardTableIsOpen ::
     forall k v blob m. (
       MonadState Model m, MonadError Err m
     , Typeable k, Typeable v, Typeable blob
     )
  => Table k v blob
  -> m (UpdateCounter, Model.Table k v blob)
guardTableIsOpen Table{..} =
    gets (Map.lookup tableID . tables) >>= \case
      Nothing ->
        throwError ErrTableClosed
      Just (updc, tbl) ->
        pure (updc, fromJust $ fromSomeTable tbl)

newTableWith ::
     (MonadState Model m, C k v blob)
  => TableConfig
  -> Model.Table k v blob
  -> m (Table k v blob)
newTableWith config tbl = state $ \Model{..} ->
  let table = Table {
          tableID = nextID
        , config
        }
      someTable = toSomeTable tbl
      tables' = Map.insert nextID (0, someTable) tables
      nextID' = nextID + 1
      model' = Model {
          tables = tables'
        , nextID = nextID'
        , ..
        }
  in  (table, model')

{-------------------------------------------------------------------------------
  Lookups
-------------------------------------------------------------------------------}

lookups ::
     ( MonadState Model m
     , MonadError Err m
     , SerialiseKey k
     , SerialiseValue v
     , C k v blob
     )
  => V.Vector k
  -> Table k v blob
  -> m (V.Vector (Model.LookupResult v (BlobRef blob)))
lookups ks t = do
    (updc, table) <- guardTableIsOpen t
    pure $ liftBlobRefs (SomeTableID updc (tableID t)) $ Model.lookups ks table

rangeLookup ::
     ( MonadState Model m
     , MonadError Err m
     , SerialiseKey k
     , SerialiseValue v
     , C k v blob
     )
  => Range k
  -> Table k v blob
  -> m (V.Vector (Model.QueryResult k v (BlobRef blob)))
rangeLookup r t = do
    (updc, table) <- guardTableIsOpen t
    pure $ liftBlobRefs (SomeTableID updc (tableID t)) $ Model.rangeLookup r table

{-------------------------------------------------------------------------------
  Updates
-------------------------------------------------------------------------------}

updates ::
     ( MonadState Model m
     , MonadError Err m
     , SerialiseKey k
     , SerialiseValue v
     , SerialiseValue blob
     , C k v blob
     )
  => ResolveSerialisedValue v
  -> V.Vector (k, Model.Update v blob)
  -> Table k v blob
  -> m ()
updates r ups t@Table{..} = do
  (updc, table) <- guardTableIsOpen t
  let table' = Model.updates r ups table
  modify (\m -> m {
      tables = Map.insert tableID (updc + 1, toSomeTable table') (tables m)
    })

inserts ::
     ( MonadState Model m
     , MonadError Err m
     , SerialiseKey k
     , SerialiseValue v
     , SerialiseValue blob
     , C k v blob
     )
  => ResolveSerialisedValue v
  -> V.Vector (k, v, Maybe blob)
  -> Table k v blob
  -> m ()
inserts r = updates r . fmap (\(k, v, blob) -> (k, Model.Insert v blob))

deletes ::
     ( MonadState Model m
     , MonadError Err m
     , SerialiseKey k
     , SerialiseValue v
     , SerialiseValue blob
     , C k v blob
     )
  => ResolveSerialisedValue v
  -> V.Vector k
  -> Table k v blob
  -> m ()
deletes r = updates r . fmap (,Model.Delete)

mupserts ::
     ( MonadState Model m
     , MonadError Err m
     , SerialiseKey k
     , SerialiseValue v
     , SerialiseValue blob
     , C k v blob
     )
  => ResolveSerialisedValue v
  -> V.Vector (k, v)
  -> Table k v blob
  -> m ()
mupserts r = updates r . fmap (fmap Model.Mupsert)

{-------------------------------------------------------------------------------
  Blobs
-------------------------------------------------------------------------------}

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
     , SerialiseValue blob
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
            case Map.lookup tableID (tables m) of
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

data SomeHandleID blob where
  SomeTableID  :: !UpdateCounter -> !TableID -> SomeHandleID blob
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

data Snapshot = Snapshot TableConfig SnapshotLabel SomeTable
  deriving stock Show

createSnapshot ::
     ( MonadState Model m
     , MonadError Err m
     , C k v blob
     )
  => SnapshotLabel
  -> SnapshotName
  -> Table k v blob
  -> m ()
createSnapshot label name t@Table{..} = do
    (updc, table) <- guardTableIsOpen t
    snaps <- gets snapshots
    -- TODO: For the moment we allow snapshot to invalidate blob refs.
    -- Ideally we should change the implementation to not invalidate on
    -- snapshot, and then we can remove the artificial invalidation from
    -- the model (i.e. delete the lines below that increments updc).
    -- Furthermore, we invalidate them _before_ checking if there is a
    -- duplicate snapshot. This is a bit barmy, but it matches the
    -- implementation. The implementation should be fixed.
    -- TODO: See https://github.com/IntersectMBO/lsm-tree/issues/392
    modify (\m -> m {
        tables = Map.insert tableID (updc + 1, toSomeTable table) (tables m)
      })
    when (Map.member name snaps) $
      throwError ErrSnapshotExists
    modify (\m -> m {
        snapshots = Map.insert name (Snapshot config label $ toSomeTable $ Model.snapshot table) (snapshots m)
      })

openSnapshot ::
     forall k v blob m.(
       MonadState Model m
     , MonadError Err m
     , C k v blob
     )
  => SnapshotLabel
  -> SnapshotName
  -> m (Table k v blob)
openSnapshot label name = do
    snaps <- gets snapshots
    case Map.lookup name snaps of
      Nothing ->
        throwError ErrSnapshotDoesNotExist
      Just (Snapshot conf label' tbl) -> do
        when (label /= label') $
          throwError ErrSnapshotWrongType
        case fromSomeTable tbl of
          Nothing ->
            -- The label should contain enough information to type snapshots,
            -- but it's up to the user to pick labels. If the labels are picked
            -- badly, then the SUT would fail with any number of and type of
            -- errors. The model simply does not allow this to occur: if we fail
            -- to cast modelled tables, then we consider it to be a bug in the
            -- test setup, and so we use @error@ instead of @throwError@.
            error "openSnapshot: snapshot opened at wrong type"
          Just table' ->
            newTableWith conf table'

deleteSnapshot ::
     (MonadState Model m, MonadError Err m)
  => SnapshotName
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
  => m [SnapshotName]
listSnapshots = gets (Map.keys . snapshots)

{-------------------------------------------------------------------------------
  Mutiple writable tables
-------------------------------------------------------------------------------}

duplicate ::
     ( MonadState Model m
     , MonadError Err m
     , C k v blob
     )
  => Table k v blob
  -> m (Table k v blob)
duplicate t@Table{..} = do
    table <- snd <$> guardTableIsOpen t
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
     , SerialiseKey k
     , C k v blob
     )
  => Maybe k
  -> Table k v blob
  -> m (Cursor k v blob)
newCursor offset t = do
  table <- snd <$> guardTableIsOpen t
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
     , SerialiseKey k
     , SerialiseValue v
     , C k v blob
     )
  => Int
  -> Cursor k v blob
  -> m (V.Vector (Model.QueryResult k v (BlobRef blob)))
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

{-------------------------------------------------------------------------------
  Table union
-------------------------------------------------------------------------------}

union ::
     ( MonadState Model m
     , MonadError Err m
     , C k v b
     )
  => ResolveSerialisedValue v
  -> Table k v b
  -> Table k v b
  -> m (Table k v b)
union r th1 th2 = do
  (_, t1) <- guardTableIsOpen th1
  (_, t2) <- guardTableIsOpen th2
  newTableWith TableConfig $ Model.union r t1 t2
