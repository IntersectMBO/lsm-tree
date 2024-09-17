-- | IO-based model implementation.
--
-- Differences from the (current) real API:
--
-- * `openSession` doesn't take file-system arguments.
--
-- * `snapshot` and `open` require `Typeable` constraints
--
module Database.LSMTree.ModelIO.Normal (
    -- * Serialisation
    SerialiseKey
  , SerialiseValue
    -- * Utility types
  , IOLike
    -- * Sessions
  , Session
  , openSession
  , closeSession
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
  , SnapshotName
  , snapshot
  , open
  , deleteSnapshot
  , listSnapshots
    -- * Multiple writable table handles
  , duplicate
  ) where

import           Control.Concurrent.Class.MonadSTM
import           Control.Monad (void, when)
import           Data.Dynamic (fromDynamic, toDyn)
import           Data.Kind (Type)
import qualified Data.Map.Strict as Map
import           Data.Typeable (Typeable)
import qualified Data.Vector as V
import           Database.LSMTree.Common (IOLike, SerialiseKey, SerialiseValue,
                     SnapshotName)
import qualified Database.LSMTree.Model.Normal as Model
import           Database.LSMTree.Model.Normal.Session (UpdateCounter)
import           Database.LSMTree.ModelIO.Session
import           Database.LSMTree.Normal (LookupResult (..), QueryResult (..),
                     Update (..))
import           GHC.IO.Exception (IOErrorType (..), IOException (..))
import           System.IO.Error (alreadyExistsErrorType)

{-------------------------------------------------------------------------------
  Tables
-------------------------------------------------------------------------------}

-- | A handle to a table.
type TableHandle :: (Type -> Type) -> Type -> Type -> Type -> Type
data TableHandle m k v blob = TableHandle {
    thSession :: !(Session m)
  , thId      :: !Int
  , thRef     :: !(TMVar m (UpdateCounter, Model.Table k v blob))
  }

data TableConfig = TableConfig
  deriving stock Show

-- | Configs should be comparable, because only tables with the same config
-- options are __compatible__.
deriving stock instance Eq TableConfig

-- | Create a new table referenced by a table handle.
new ::
     IOLike m
  => Session m
  -> TableConfig
  -> m (TableHandle m k v blob)
new session _config = atomically $ do
    ref <- newTMVar (0, Model.empty)
    i <- new_handle session ref
    return TableHandle {thSession = session, thId = i, thRef = ref }

-- | Close a table handle.
close ::
     IOLike m
  => TableHandle m k v blob
  -> m ()
close TableHandle {..} = atomically $ do
    close_handle thSession thId
    void $ tryTakeTMVar thRef

{-------------------------------------------------------------------------------
  Table querying and updates
-------------------------------------------------------------------------------}

-- | Perform a batch of lookups.
lookups ::
     (IOLike m, SerialiseKey k, SerialiseValue v)
  => V.Vector k
  -> TableHandle m k v blob
  -> m (V.Vector (LookupResult v (BlobRef m blob)))
lookups ks TableHandle {..} = atomically $
    withModel "lookups" "table handle" thSession thRef $ \(updc, tbl) ->
      return $ liftBlobRefs thSession (SomeTableRef updc thRef) $ Model.lookups ks tbl

-- | Perform a range lookup.
rangeLookup ::
     (IOLike m, SerialiseKey k, SerialiseValue v)
  => Model.Range k
  -> TableHandle m k v blob
  -> m (V.Vector (QueryResult k v (BlobRef m blob)))
rangeLookup r TableHandle {..} = atomically $
    withModel "rangeLookup" "table handle" thSession thRef $ \(updc, tbl) ->
      return $ liftBlobRefs thSession (SomeTableRef updc thRef) $ Model.rangeLookup r tbl

-- | Perform a mixed batch of inserts and deletes.
updates ::
     (IOLike m, SerialiseKey k, SerialiseValue v, SerialiseValue blob)
  => V.Vector (k, Update v blob)
  -> TableHandle m k v blob
  -> m ()
updates ups TableHandle {..} = atomically $
    withModel "updates" "table handle" thSession thRef $ \(updc, tbl) ->
        writeTMVar thRef (updc + 1, Model.updates ups tbl)

-- | Perform a batch of inserts.
inserts ::
     (IOLike m, SerialiseKey k, SerialiseValue v, SerialiseValue blob)
  => V.Vector (k, v, Maybe blob)
  -> TableHandle m k v blob
  -> m ()
inserts = updates . fmap (\(k, v, blob) -> (k, Model.Insert v blob))

-- | Perform a batch of deletes.
deletes ::
     (IOLike m, SerialiseKey k, SerialiseValue v, SerialiseValue blob)
  => V.Vector k
  -> TableHandle m k v blob
  -> m ()
deletes = updates . fmap (,Model.Delete)

data BlobRef m blob = BlobRef {
    brSession   :: !(Session m)
  , brHandleRef :: !(SomeHandleRef m blob)
  , brBlob      :: !(Model.BlobRef blob)
  }

data SomeHandleRef m blob where
  SomeTableRef  :: !UpdateCounter -> !(TMVar m (UpdateCounter, Model.Table k v blob)) -> SomeHandleRef m blob
  SomeCursorRef :: !(TMVar m (Model.Cursor k v blob)) -> SomeHandleRef m blob

liftBlobRefs ::
     forall m f g blob. (Functor f, Functor g)
  => Session m
  -> SomeHandleRef m blob
  -> g (f (Model.BlobRef blob))
  -> g (f (BlobRef m blob))
liftBlobRefs s href = fmap (fmap liftBlobRef)
  where liftBlobRef = BlobRef s href

-- | Perform a batch of blob retrievals.
retrieveBlobs ::
     forall m blob. (IOLike m, SerialiseValue blob)
  => Session m
  -> V.Vector (BlobRef m blob)
  -> m (V.Vector blob)
retrieveBlobs _ brefs = atomically $ Model.retrieveBlobs <$> mapM guard brefs
  where
    guard :: BlobRef m blob -> STM m (Model.BlobRef blob)
    guard BlobRef{..} = do
      check_session_open "retrieveBlobs" brSession
      -- In the real implementation, a blob reference /could/ be invalidated
      -- every time you modify a table or cursor. This model takes the most
      -- conservative approach: a blob reference is immediately invalidated
      -- every time a table/cursor is modified.
      case brHandleRef of
        SomeTableRef createdAt tableRef ->
          tryReadTMVar tableRef >>= \case
            -- If the table is now closed, it means the table has been modified.
            Nothing -> errInvalid
            -- If the table is open, we check timestamps (i.e., UpdateCounter)
            -- to see if any modifications were made.
            Just (updc, _) -> do
              when (updc /= createdAt) $ errInvalid
              pure brBlob
        SomeCursorRef cursorRef ->
          -- The only modification to a cursor is that it can be closed.
          tryReadTMVar cursorRef >>= \case
            Nothing -> errInvalid
            Just _  -> pure brBlob

    errInvalid :: STM m a
    errInvalid = throwSTM IOError
        { ioe_handle      = Nothing
        , ioe_type        = IllegalOperation
        , ioe_location    = "retrieveBlobs"
        , ioe_description = "blob reference invalidated"
        , ioe_errno       = Nothing
        , ioe_filename    = Nothing
        }

{-------------------------------------------------------------------------------
  Snapshots
-------------------------------------------------------------------------------}

-- | Take a snapshot.
snapshot ::
     ( IOLike m
     , Typeable k
     , Typeable v
     , Typeable blob
     )
  => SnapshotName
  -> TableHandle m k v blob
  -> m ()
snapshot n TableHandle {..} = atomically $
    withModel "snapshot" "table handle" thSession thRef $ \tbl -> do
        snaps <- readTVar $ snapshots thSession
        if Map.member n snaps then
          throwSTM IOError
            { ioe_handle      = Nothing
            , ioe_type        = alreadyExistsErrorType
            , ioe_location    = "snapshot"
            , ioe_description = "snapshot already exists"
            , ioe_errno       = Nothing
            , ioe_filename    = Nothing
            }
        else
          modifyTVar' (snapshots thSession) (Map.insert n (toDyn tbl))

-- | Open a table through a snapshot, returning a new table handle.
open ::
     ( IOLike m
     , Typeable k
     , Typeable v
     , Typeable blob
     )
  => Session m
  -> SnapshotName
  -> m (TableHandle m k v blob)
open s n = atomically $ do
    ss <- readTVar (snapshots s)
    case Map.lookup n ss of
        Nothing -> throwSTM IOError
            { ioe_handle      = Nothing
            , ioe_type        = NoSuchThing
            , ioe_location    = "open"
            , ioe_description = "no such snapshot"
            , ioe_errno       = Nothing
            , ioe_filename    = Nothing
            }

        Just dyn -> case fromDynamic dyn of
            Nothing -> throwSTM IOError
                { ioe_handle      = Nothing
                , ioe_type        = InappropriateType
                , ioe_location    = "open"
                , ioe_description = "table type mismatch"
                , ioe_errno       = Nothing
                , ioe_filename    = Nothing
                }

            Just tbl' -> do
                ref <- newTMVar tbl'
                i <- new_handle s ref
                return TableHandle { thRef = ref, thId = i, thSession = s }

{-------------------------------------------------------------------------------
  Mutiple writable table handles
-------------------------------------------------------------------------------}

-- | Create a cheap, independent duplicate of a table handle. This returns a new
-- table handle.
duplicate ::
     IOLike m
  => TableHandle m k v blob
  -> m (TableHandle m k v blob)
duplicate TableHandle {..} = atomically $
    withModel "duplicate" "table handle" thSession thRef $ \tbl -> do
        thRef' <- newTMVar tbl
        i <- new_handle thSession thRef'
        return TableHandle { thRef = thRef', thId = i, thSession = thSession }

{-------------------------------------------------------------------------------
  Cursor
-------------------------------------------------------------------------------}

type Cursor :: (Type -> Type) -> Type -> Type -> Type -> Type
data Cursor m k v blob = Cursor {
      cSession :: !(Session m)
    , cId      :: !Int
    , cRef     :: !(TMVar m (Model.Cursor k v blob))
    }

newCursor ::
     (IOLike m, SerialiseKey k)
  => Maybe k
  -> TableHandle m k v blob
  -> m (Cursor m k v blob)
newCursor offset TableHandle{..} = atomically $
    withModel "newCursor" "table handle" thSession thRef $ \(_updc, tbl) -> do
      cRef <- newTMVar (Model.newCursor offset tbl)
      i <- new_handle thSession cRef
      pure Cursor {
          cSession = thSession
        , cId = i
        , cRef
        }

closeCursor ::
     IOLike m
  => Cursor m k v blob
  -> m ()
closeCursor Cursor {..} = atomically $ do
    close_handle cSession cId
    void $ tryTakeTMVar cRef

readCursor ::
     (IOLike m, SerialiseKey k, SerialiseValue v)
  => Int
  -> Cursor m k v blob
  -> m (V.Vector (QueryResult k v (BlobRef m blob)))
readCursor n Cursor{..} = atomically $
    withModel "readCursor" "cursor" cSession cRef $ \c -> do
      let (qss, c') = Model.readCursor n c
      writeTMVar cRef c'
      return $ liftBlobRefs cSession (SomeCursorRef cRef) qss

{-------------------------------------------------------------------------------
  Internal helpers
-------------------------------------------------------------------------------}

withModel :: IOLike m => String -> String -> Session m -> TMVar m a -> (a -> STM m r) -> STM m r
withModel fun hdl s ref kont = do
    m <- tryReadTMVar ref
    case m of
        Nothing -> throwSTM IOError
            { ioe_handle      = Nothing
            , ioe_type        = IllegalOperation
            , ioe_location    = fun
            , ioe_description = hdl <> " closed"
            , ioe_errno       = Nothing
            , ioe_filename    = Nothing
            }
        Just m' -> do
            check_session_open fun s
            kont m'
