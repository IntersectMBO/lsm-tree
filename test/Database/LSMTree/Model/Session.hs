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
  , runModelMWithInjectedErrors
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
  , invalidateBlobRefs
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
  , unions
  ) where

import           Control.Monad (forM, when)
import           Control.Monad.Except (ExceptT (..), MonadError (..),
                     runExceptT)
import           Control.Monad.Identity (Identity (runIdentity))
import           Control.Monad.State.Strict (MonadState (..), StateT (..), gets,
                     modify)
import           Data.Data
import           Data.Dynamic
import           Data.List.NonEmpty (NonEmpty (..))
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
import           GHC.Show (appPrec)

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
     SomeTable :: (Typeable k, Typeable v, Typeable b)
               => Model.Table k v b -> SomeTable

instance Show SomeTable where
  show (SomeTable table) = show table

toSomeTable ::
     (Typeable k, Typeable v, Typeable b)
  => Model.Table k v b
  -> SomeTable
toSomeTable = SomeTable

fromSomeTable ::
     (Typeable k, Typeable v, Typeable b)
  => SomeTable
  -> Maybe (Model.Table k v b)
fromSomeTable (SomeTable tbl) = cast tbl

withSomeTable ::
     (forall k v b. (Typeable k, Typeable v, Typeable b)
        => Model.Table k v b -> a)
  -> SomeTable
  -> a
withSomeTable f (SomeTable tbl) = f tbl

newtype SomeCursor = SomeCursor Dynamic

instance Show SomeCursor where
  show (SomeCursor c) = show c

toSomeCursor ::
     (Typeable k, Typeable v, Typeable b)
  => Model.Cursor k v b
  -> SomeCursor
toSomeCursor = SomeCursor . toDyn

fromSomeCursor ::
     (Typeable k, Typeable v, Typeable b)
  => SomeCursor
  -> Maybe (Model.Cursor k v b)
fromSomeCursor (SomeCursor c) = fromDynamic c

--
-- Constraints
--

-- | Common constraints for keys, values and blobs
type C_ a = (Show a, Eq a, Typeable a)
type C k v b = (C_ k, C_ v, C_ b)

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

-- | @'runModelMWithInjectedErrors' merrs onNoErrors onErrors@ runs the model
-- with injected disk faults @merrs@.
--
-- The model may run different actions based on whether there are injections or
-- not. 'onNoErrors' runs in case @merrs == Nothing@, and 'onErrors@ runs in
-- case @merrs == Just _@.
--
-- Typically, @onNoErrors@ will be a model function like 'lookups', and
-- @onErrors@ should be the identity state transition. This models the approach
-- of the @lsm-tree@ library: the *logical* state of the database should remain
-- unchanged if there were any disk faults.
--
-- The model functions in the remainder of the module only describe the happy
-- path with respect to disk faults: they assume there aren't any such faults.
-- The intent of 'runModelMWithInjectedErrors' is to augment the model with
-- responses to disk fault injection.
--
-- The real system's is not guaranteed to fail with an error if there are
-- injected disk faults. However, the approach taken in
-- 'runModelMWithInjectedErrors' is to *always* throw a modelled disk error in
-- case there are injections, *even if* the real system happened to not fail
-- completely. This makes 'runModelMWithInjectedErrors' an approximation of the
-- real system's behaviour in case of injections. For the model to more
-- accurately model the system's behaviour, the model would have to know for
-- each API function precisely which errors are injected in which order, and
-- whether they are handled internally by the library or not. This is simply
-- infeasible: the model would become very complex.
runModelMWithInjectedErrors ::
     Maybe e -- ^ The errors that are injected
  -> ModelM a -- ^ Action to run on 'Nothing'
  -> ModelM () -- ^ Action to run on 'Just'
  -> Model -> (Either Err a, Model)
runModelMWithInjectedErrors Nothing onNoErrors _ st =
    runModelM onNoErrors st
runModelMWithInjectedErrors (Just _) _ onErrors st =
    runModelM (onErrors >> throwError (ErrFsError "modelled FsError")) st

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
    -- | Some file system error occurred
  | ErrFsError String

instance Show Err where
  showsPrec d = \case
      ErrTableClosed ->
        showString "ErrTableClosed"
      ErrSnapshotExists ->
        showString "ErrSnapshotExists"
      ErrSnapshotDoesNotExist ->
        showString "ErrSnapshotDoesNotExist"
      ErrSnapshotWrongType ->
        showString "ErrSnapshotWrongType"
      ErrBlobRefInvalidated ->
        showString "ErrBlobRefInvalidated"
      ErrCursorClosed ->
        showString "ErrCursorCosed"
      ErrFsError s ->
        showParen (d > appPrec) $
        showString "ErrFsError " .
        showParen True (showString s)

instance Eq Err where
  (==) ErrTableClosed ErrTableClosed = True
  (==) ErrSnapshotExists ErrSnapshotExists = True
  (==) ErrSnapshotDoesNotExist ErrSnapshotDoesNotExist = True
  (==) ErrSnapshotWrongType ErrSnapshotWrongType = True
  (==) ErrBlobRefInvalidated ErrBlobRefInvalidated = True
  (==) ErrCursorClosed ErrCursorClosed = True
  (==) (ErrFsError _) (ErrFsError _) = True
  (==) _ _ = False
    where
      _coveredAllCases x = case x of
          ErrTableClosed{}          -> ()
          ErrSnapshotExists{}       -> ()
          ErrSnapshotDoesNotExist{} -> ()
          ErrSnapshotWrongType{}    -> ()
          ErrBlobRefInvalidated{}   -> ()
          ErrCursorClosed{}         -> ()
          ErrFsError{}              -> ()


{-------------------------------------------------------------------------------
  Tables
-------------------------------------------------------------------------------}

type TableID = Int

--
-- API
--

data Table k v b = Table {
    tableID :: TableID
  , config  :: TableConfig
  }
  deriving stock Show

data TableConfig = TableConfig
  deriving stock (Show, Eq)

new ::
     forall k v b m. (MonadState Model m, C k v b)
  => TableConfig
  -> m (Table k v b)
new config = newTableWith config Model.empty

-- |
--
-- This is idempotent.
close :: MonadState Model m => Table k v b -> m ()
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
     forall k v b m. (
       MonadState Model m, MonadError Err m
     , Typeable k, Typeable v, Typeable b
     )
  => Table k v b
  -> m (UpdateCounter, Model.Table k v b)
guardTableIsOpen Table{..} =
    gets (Map.lookup tableID . tables) >>= \case
      Nothing ->
        throwError ErrTableClosed
      Just (updc, tbl) ->
        pure (updc, fromJust $ fromSomeTable tbl)

newTableWith ::
     (MonadState Model m, C k v b)
  => TableConfig
  -> Model.Table k v b
  -> m (Table k v b)
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
     , C k v b
     )
  => V.Vector k
  -> Table k v b
  -> m (V.Vector (Model.LookupResult v (BlobRef b)))
lookups ks t = do
    (updc, table) <- guardTableIsOpen t
    pure $ liftBlobRefs (SomeTableID updc (tableID t)) $ Model.lookups ks table

rangeLookup ::
     ( MonadState Model m
     , MonadError Err m
     , SerialiseKey k
     , SerialiseValue v
     , C k v b
     )
  => Range k
  -> Table k v b
  -> m (V.Vector (Model.QueryResult k v (BlobRef b)))
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
     , SerialiseValue b
     , C k v b
     )
  => ResolveSerialisedValue v
  -> V.Vector (k, Model.Update v b)
  -> Table k v b
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
     , SerialiseValue b
     , C k v b
     )
  => ResolveSerialisedValue v
  -> V.Vector (k, v, Maybe b)
  -> Table k v b
  -> m ()
inserts r = updates r . fmap (\(k, v, blob) -> (k, Model.Insert v blob))

deletes ::
     ( MonadState Model m
     , MonadError Err m
     , SerialiseKey k
     , SerialiseValue v
     , SerialiseValue b
     , C k v b
     )
  => ResolveSerialisedValue v
  -> V.Vector k
  -> Table k v b
  -> m ()
deletes r = updates r . fmap (,Model.Delete)

mupserts ::
     ( MonadState Model m
     , MonadError Err m
     , SerialiseKey k
     , SerialiseValue v
     , SerialiseValue b
     , C k v b
     )
  => ResolveSerialisedValue v
  -> V.Vector (k, v)
  -> Table k v b
  -> m ()
mupserts r = updates r . fmap (fmap Model.Mupsert)

{-------------------------------------------------------------------------------
  Blobs
-------------------------------------------------------------------------------}

-- | For more details: 'Database.LSMTree.Internal.BlobRef' describes the
-- intended semantics of blob references.
data BlobRef b = BlobRef {
    handleRef :: !(SomeHandleID b)
  , innerBlob :: !(Model.BlobRef b)
  }

deriving stock instance Show b => Show (BlobRef b)

retrieveBlobs ::
     forall m b. ( MonadState Model m
     , MonadError Err m
     , SerialiseValue b
     )
  => V.Vector (BlobRef b)
  -> m (V.Vector b)
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

data SomeHandleID b where
  SomeTableID  :: !UpdateCounter -> !TableID -> SomeHandleID b
  SomeCursorID :: !CursorID -> SomeHandleID b
  deriving stock Show

liftBlobRefs ::
     (Functor f, Functor g)
  => SomeHandleID b
  -> g (f (Model.BlobRef b))
  -> g (f (BlobRef b))
liftBlobRefs hid = fmap (fmap (BlobRef hid))

-- | Invalidate blob references that were created from the given table. This
-- function assumes that it is called on an open table.
--
-- This is useful in tests where blob references should be invalidated for other
-- reasons than normal operation of the model.
invalidateBlobRefs ::
     MonadState Model m
  => Table k v b
  -> m ()
invalidateBlobRefs Table{..} = do
    gets (Map.lookup tableID . tables) >>= \case
      Nothing -> error "invalidateBlobRefs: table is closed!"
      Just (updc, tbl) -> do
        modify (\m -> m {
            tables = Map.insert tableID (updc + 1, tbl) (tables m)
          })

{-------------------------------------------------------------------------------
  Snapshots
-------------------------------------------------------------------------------}

data Snapshot = Snapshot TableConfig SnapshotLabel SomeTable
  deriving stock Show

createSnapshot ::
     ( MonadState Model m
     , MonadError Err m
     , C k v b
     )
  => SnapshotLabel
  -> SnapshotName
  -> Table k v b
  -> m ()
createSnapshot label name t@Table{..} = do
    (_updc, table) <- guardTableIsOpen t
    snaps <- gets snapshots
    when (Map.member name snaps) $
      throwError ErrSnapshotExists
    modify (\m -> m {
        snapshots = Map.insert name (Snapshot config label $ toSomeTable $ Model.snapshot table) (snapshots m)
      })

openSnapshot ::
     forall k v b m.(
       MonadState Model m
     , MonadError Err m
     , C k v b
     )
  => SnapshotLabel
  -> SnapshotName
  -> m (Table k v b)
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
     , C k v b
     )
  => Table k v b
  -> m (Table k v b)
duplicate t@Table{..} = do
    table <- snd <$> guardTableIsOpen t
    newTableWith config $ Model.duplicate table

{-------------------------------------------------------------------------------
  Cursor
-------------------------------------------------------------------------------}

type CursorID = Int

data Cursor k v b = Cursor {
    cursorID :: !CursorID
  }
  deriving stock Show

newCursor ::
     forall k v b m. (
       MonadState Model m, MonadError Err m
     , SerialiseKey k
     , C k v b
     )
  => Maybe k
  -> Table k v b
  -> m (Cursor k v b)
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

closeCursor :: MonadState Model m => Cursor k v b -> m ()
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
     , C k v b
     )
  => Int
  -> Cursor k v b
  -> m (V.Vector (Model.QueryResult k v (BlobRef b)))
readCursor n c = do
    cursor <- guardCursorIsOpen c
    let (qrs, cursor') = Model.readCursor n cursor
    modify (\m -> m {
        cursors = Map.insert (cursorID c) (toSomeCursor cursor') (cursors m)
      })
    pure $ liftBlobRefs (SomeCursorID (cursorID c)) $ qrs

guardCursorIsOpen ::
     forall k v b m. (
       MonadState Model m, MonadError Err m
     , Typeable k, Typeable v, Typeable b
     )
  => Cursor k v b
  -> m (Model.Cursor k v b)
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

unions ::
     ( MonadState Model m
     , MonadError Err m
     , C k v b
     )
  => ResolveSerialisedValue v
  -> NonEmpty (Table k v b)
  -> m (Table k v b)
unions r tables = do
    tables' <- forM tables $ \table -> do
      (_, table') <- guardTableIsOpen table
      pure table'
    newTableWith TableConfig $ Model.unions r tables'
