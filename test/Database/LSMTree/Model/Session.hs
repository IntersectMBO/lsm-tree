{-# LANGUAGE PatternSynonyms #-}
-- | A pure model of a single session containing multiple tables.
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
  , isUnionDescendant
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
  , isDiskFault
  , isSnapshotCorrupted
  , isOther
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
  , corruptSnapshot
  , deleteSnapshot
  , listSnapshots
    -- * Multiple writable tables
  , duplicate
    -- * Table union
  , IsUnionDescendant (..)
  , union
  , unions
  , UnionDebt (..)
  , remainingUnionDebt
  , UnionCredits (..)
  , supplyUnionCredits
  , supplyPortionOfDebt
  ) where

import           Control.Monad (forM, when)
import           Control.Monad.Except (ExceptT (..), MonadError (..),
                     runExceptT)
import           Control.Monad.Identity (Identity (runIdentity))
import           Control.Monad.State.Strict (MonadState (..), StateT (..), gets,
                     modify)
import           Data.Data
import           Data.Dynamic
import qualified Data.List as L
import           Data.List.NonEmpty (NonEmpty (..))
import qualified Data.List.NonEmpty as NE
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import           Data.Maybe (fromJust)
import qualified Data.Vector as V
import           Data.Word
import           Database.LSMTree.Common (SerialiseKey (..),
                     SerialiseValue (..), SnapshotLabel (..), SnapshotName,
                     UnionCredits (..), UnionDebt (..))
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
--
-- Supplying union credits is also considered an update, though this can only
-- invalidate a blob reference that is associated with a (descendant of a) union
-- table.
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
    runModelM (onErrors >> throwError ModelErrDiskFault) st

-- | The default 'ErrDiskFault' that model operations will throw.
pattern ModelErrDiskFault :: Err
pattern ModelErrDiskFault = ErrDiskFault "model does not produce an error message"

-----
-- Errors
--

data Err
  = ErrSessionDirDoesNotExist
  | ErrSessionDirLocked
  | ErrSessionDirCorrupted
  | ErrSessionClosed
  | ErrTableClosed
  | ErrTableCorrupted
  | ErrTableUnionHandleTypeMismatch
  | ErrTableUnionSessionMismatch
  | ErrSnapshotExists !SnapshotName
  | ErrSnapshotDoesNotExist !SnapshotName
  | ErrSnapshotCorrupted !SnapshotName
  | ErrSnapshotWrongTableType !SnapshotName
  | ErrSnapshotWrongLabel !SnapshotName
  | ErrBlobRefInvalid
  | ErrCursorClosed
  | ErrDiskFault !String
  | ErrFsError !String
  | ErrCommitActionRegistry !(NonEmpty Err)
  | ErrAbortActionRegistry !(Maybe Err) !(NonEmpty Err)
  | ErrOther !String
  deriving stock (Eq, Show)

isSnapshotCorrupted :: Err -> Bool
isSnapshotCorrupted (ErrSnapshotCorrupted _)      = True
isSnapshotCorrupted (ErrFsError _)                = True
isSnapshotCorrupted (ErrCommitActionRegistry es)  = firstNotOtherSatisfies isSnapshotCorrupted es
isSnapshotCorrupted (ErrAbortActionRegistry e es) = firstNotOtherSatisfies isSnapshotCorrupted (maybe id NE.cons e es)
isSnapshotCorrupted _                             = False

isDiskFault :: Err -> Bool
isDiskFault (ErrDiskFault _)              = True
isDiskFault (ErrFsError _)                = True
isDiskFault (ErrCommitActionRegistry es)  = firstNotOtherSatisfies isDiskFault es
isDiskFault (ErrAbortActionRegistry e es) = firstNotOtherSatisfies isDiskFault (maybe id NE.cons e es)
isDiskFault _                             = False

isOther :: Err -> Bool
isOther (ErrOther _)                  = True
isOther (ErrCommitActionRegistry es)  = all isOther es
isOther (ErrAbortActionRegistry e es) = all isOther (maybe id NE.cons e es)
isOther _                             = False

firstNotOtherSatisfies :: (Err -> Bool) -> NonEmpty Err -> Bool
firstNotOtherSatisfies p =
  maybe False (p . fst) . L.uncons . NE.dropWhile isOther

{-------------------------------------------------------------------------------
  Tables
-------------------------------------------------------------------------------}

type TableID = Int

--
-- API
--

data Table k v b = Table {
    tableID           :: TableID
  , config            :: TableConfig
  , isUnionDescendant :: IsUnionDescendant
  }
  deriving stock Show

-- | The model does not distinguish multiple values of the config type. This is
-- the right thing because the table config is never observed directly, only
-- via the influence it might have on other operation results. And we /do not
-- want/ table configs to have observable value-level effects. The table config
-- should only have performance effects. Thus having only one config value is
-- the right abstraction for the model.
--
data TableConfig = TableConfig
  deriving stock (Show, Eq)

new ::
     forall k v b m. (MonadState Model m, C k v b)
  => TableConfig
  -> m (Table k v b)
new config = newTableWith config IsNotUnionDescendant Model.empty

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
  -> IsUnionDescendant
  -> Model.Table k v b
  -> m (Table k v b)
newTableWith config isUnionDescendant tbl = state $ \Model{..} ->
  let table = Table {
          tableID = nextID
        , config
        , isUnionDescendant
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
    errInvalid = throwError ErrBlobRefInvalid

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

data Snapshot = Snapshot
  { snapshotConfig            :: TableConfig
  , snapshotLabel             :: SnapshotLabel
  , snapshotTable             :: SomeTable
  , snapshotIsUnionDescendant :: IsUnionDescendant
  , snapshotCorrupted         :: Bool
  }
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
      throwError $ ErrSnapshotExists name
    let snap =
          Snapshot
            config label
            (toSomeTable $ Model.snapshot table)
            isUnionDescendant False
    modify (\m -> m {
        snapshots = Map.insert name snap (snapshots m)
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
        throwError $ ErrSnapshotDoesNotExist name
      Just (Snapshot conf label' tbl snapshotIsUnion corrupted) -> do
        when corrupted $
          throwError $ ErrSnapshotCorrupted name
        when (label /= label') $
          throwError $ ErrSnapshotWrongLabel name
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
            newTableWith conf snapshotIsUnion table'

-- To match the implementation of the real table, this should not corrupt the
-- snapshot if there are _no non-empty files_; however, since there are no such
-- snapshots, this is probably fine.
corruptSnapshot ::
     (MonadState Model m, MonadError Err m)
  => SnapshotName
  -> m ()
corruptSnapshot name = do
  snapshots <- gets snapshots
  if Map.notMember name snapshots
    then throwError $ ErrSnapshotDoesNotExist name
    else modify $ \m -> m {snapshots = Map.adjust corruptSnapshotEntry name snapshots}
  where
    corruptSnapshotEntry (Snapshot c l t u _) = Snapshot c l t u True

deleteSnapshot ::
     (MonadState Model m, MonadError Err m)
  => SnapshotName
  -> m ()
deleteSnapshot name = do
    snaps <- gets snapshots
    case Map.lookup name snaps of
      Nothing ->
        throwError $ ErrSnapshotDoesNotExist name
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
    newTableWith config isUnionDescendant $ Model.duplicate table

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

-- Is this a (descendant of a) union table?
--
-- This is important for invalidating blob references: if a table is a
-- (descendant of a) union table, then 'supplyUnionCredits' can invalidate blob
-- references.
data IsUnionDescendant = IsUnionDescendant | IsNotUnionDescendant
  deriving stock (Show, Eq)

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
  newTableWith TableConfig IsUnionDescendant $ Model.union r t1 t2

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
    newTableWith TableConfig IsUnionDescendant $ Model.unions r tables'

-- | The model can not accurately predict union debt without considerable
-- knowledge about the implementation of /real/ tables. Therefore the model
-- considers unions to be finished right away, and the resulting debt will
-- always be 0.
remainingUnionDebt ::
     ( MonadState Model m
     , MonadError Err m
     , C k v b
     )
  => Table k v b
  -> m UnionDebt
remainingUnionDebt t = do
    (_updc, _table) <- guardTableIsOpen t
    pure (UnionDebt 0)

-- | The union debt is always 0, so supplying union credits has no effect on the
-- tables, except for invalidating its blob references in some cases.
--
-- In the /real/ implementation, blob references can be associated with a run in
-- a regular level, or in a union level. In the former case, only updates can
-- invalidate the blob reference. In the latter case, only supplying union
-- credits can invalidate the blob reference.
--
-- Without considerable knowledge about the /real/ implementation, the model can
-- not /always/ accurately predict which of two cases a blob reference belongs
-- to. However, there is one case where a table is guaranteed not to contain a
-- union level: if the table is /not/ a (descendant of a) union table.
--
-- There is another caveat: without considerable knowledge about the real
-- implementation, the model can not accurately predict after how many supplied
-- union credits /real/ blob references are invalidated. Therefore, we model
-- invalidation conservatively in a similar way to 'updates': any supply of
-- @>=1@ union credits is enough to invalidate union blob references.
--
-- To summarise, 'supplyUnionCredits' will invalidate blob references associated
-- with the input table if:
--
-- * The table is a (descendant of a) union table
--
-- * The number of supplied union credits is at least 1.
supplyUnionCredits ::
     ( MonadState Model m
     , MonadError Err m
     , C k v b
     )
  => Table k v b
  -> UnionCredits
  -> m UnionCredits
supplyUnionCredits t@Table{..} c@(UnionCredits credits)
  | credits <= 0 = do
      _ <- guardTableIsOpen t
      pure (UnionCredits 0) -- always 0, not negative
  | otherwise = do
      (updc, table) <- guardTableIsOpen t
      when (isUnionDescendant == IsUnionDescendant) $
        modify (\m -> m {
            tables = Map.insert tableID (updc + 1, toSomeTable table) (tables m)
          })
      pure c

-- | A version of 'supplyUnionCredits' that supplies a portion of the current debt.
--
-- The debt in the model is always 0, so any portion of that debt is also 0. The
-- real system might have non-zero debt, but the model does not know how much,
-- so it has to assume that *any* portion (assuming it's a positive portion)
-- leads to invalidated blob references.
supplyPortionOfDebt ::
     ( MonadState Model m
     , MonadError Err m
     , C k v b
     )
  => Table k v b
  -> portion
  -> m UnionCredits
supplyPortionOfDebt t@Table{..} _ = do
    (updc, table) <- guardTableIsOpen t
    when (isUnionDescendant == IsUnionDescendant) $
      modify (\m -> m {
          tables = Map.insert tableID (updc + 1, toSomeTable table) (tables m)
        })
    pure (UnionCredits 0)
