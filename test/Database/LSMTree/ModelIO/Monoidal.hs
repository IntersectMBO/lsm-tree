-- | IO-based monoidal table model implementation.
--
module Database.LSMTree.ModelIO.Monoidal (
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
  , Range (..)
  , LookupResult (..)
  , lookups
  , RangeLookupResult (..)
  , rangeLookup
    -- ** Updates
  , Update (..)
  , updates
  , inserts
  , deletes
  , mupserts
    -- * Snapshots
  , SnapshotName
  , snapshot
  , open
  , deleteSnapshot
  , listSnapshots
    -- * Multiple writable table handles
  , duplicate
    -- * Merging tables
  , merge
  ) where

import           Control.Concurrent.Class.MonadSTM
import           Control.Monad (void)
import           Data.Bifunctor (Bifunctor (second))
import           Data.Dynamic (fromDynamic, toDyn)
import           Data.Kind (Type)
import qualified Data.Map.Strict as Map
import           Data.Typeable (Typeable)
import qualified Data.Vector as V
import           Database.LSMTree.Common (IOLike, Range (..), SerialiseKey,
                     SerialiseValue, SnapshotName)
import           Database.LSMTree.Model.Monoidal (ResolveValue)
import qualified Database.LSMTree.Model.Monoidal as Model
import           Database.LSMTree.ModelIO.Session
import           Database.LSMTree.Monoidal (LookupResult (..),
                     RangeLookupResult (..), Update (..))
import           GHC.IO.Exception (IOErrorType (..), IOException (..))

{-------------------------------------------------------------------------------
  Tables
-------------------------------------------------------------------------------}

-- | A handle to a table.
type TableHandle :: (Type -> Type) -> Type -> Type -> Type
data TableHandle m k v = TableHandle {
    thSession :: !(Session m)
  , thId      :: !Int
  , thRef     :: !(TMVar m (Model.Table k v))
  }

-- | Table configuration parameters, like tuning parameters.
data TableConfig = TableConfig

deriving stock instance Eq TableConfig
deriving stock instance Show TableConfig

-- | Create a new table referenced by a table handle.
new ::
     IOLike m
  => Session m
  -> TableConfig
  -> m (TableHandle m k v)
new session _config = atomically $ do
    ref <- newTMVar Model.empty
    i <- new_handle session ref
    return TableHandle {thSession = session, thId = i, thRef = ref }

-- | Close a table handle.
close ::
     IOLike m
  => TableHandle m k v
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
  -> TableHandle m k v
  -> m (V.Vector (LookupResult v))
lookups ks TableHandle {..} = atomically $
    withModel "lookups" thSession thRef $ \tbl ->
        return $ Model.lookups ks tbl

-- | Perform a range lookup.
rangeLookup ::
     (IOLike m, SerialiseKey k, SerialiseValue v)
  => Range k
  -> TableHandle m k v
  -> m (V.Vector (RangeLookupResult k v))
rangeLookup r TableHandle {..} = atomically $
    withModel "rangeLookup" thSession thRef $ \tbl ->
        return $ Model.rangeLookup r tbl

-- | Perform a mixed batch of inserts, deletes and monoidal upserts.
updates ::
     (IOLike m, SerialiseKey k, SerialiseValue v, ResolveValue v)
  => V.Vector (k, Update v)
  -> TableHandle m k v
  -> m ()
updates ups TableHandle {..} = atomically $
    withModel "updates" thSession thRef $ \tbl ->
        writeTMVar thRef $ Model.updates ups tbl

-- | Perform a batch of inserts.
inserts ::
     (IOLike m, SerialiseKey k, SerialiseValue v, ResolveValue v)
  => V.Vector (k, v)
  -> TableHandle m k v
  -> m ()
inserts = updates . fmap (second Insert)

-- | Perform a batch of deletes.
deletes ::
     (IOLike m, SerialiseKey k, SerialiseValue v, ResolveValue v)
  => V.Vector k
  -> TableHandle m k v
  -> m ()
deletes = updates . fmap (,Delete)

-- | Perform a batch of monoidal upserts.
mupserts ::
     (IOLike m, SerialiseKey k, SerialiseValue v, ResolveValue v)
  => V.Vector (k, v)
  -> TableHandle m k v
  -> m ()
mupserts = updates . fmap (second Mupsert)

{-------------------------------------------------------------------------------
  Snapshots
-------------------------------------------------------------------------------}

-- | Take a snapshot.
snapshot ::
     ( IOLike m
     , Typeable k
     , Typeable v
     )
  => SnapshotName
  -> TableHandle m k v
  -> m ()
snapshot n TableHandle {..} = atomically $
    withModel "snapshot" thSession thRef $ \tbl ->
        modifyTVar' (snapshots thSession) (Map.insert n (toDyn tbl))

-- | Open a table through a snapshot, returning a new table handle.
open ::
     ( IOLike m
     , Typeable k
     , Typeable v
     )
  => Session m
  -> SnapshotName
  -> m (TableHandle m k v)
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
  => TableHandle m k v
  -> m (TableHandle m k v)
duplicate TableHandle {..} = atomically $
    withModel "duplicate" thSession thRef $ \tbl -> do
        thRef' <- newTMVar tbl
        i <- new_handle thSession thRef'
        return TableHandle { thRef = thRef', thId = i, thSession = thSession }

{-------------------------------------------------------------------------------
  Merging tables
-------------------------------------------------------------------------------}

-- | Merge full tables, creating a new table handle.
merge ::
     (IOLike m, ResolveValue v)
  => TableHandle m k v
  -> TableHandle m k v
  -> m (TableHandle m k v)
merge hdl1 hdl2
{-
    -- cannot == io-sim TVars.
    | thSession hdl1 /= thSession hdl2
    = throwSTM IOError
        { ioe_handle      = Nothing
        , ioe_type        = InappropriateType
        , ioe_location    = "merge"
        , ioe_description = "different sessions"
        , ioe_errno       = Nothing
        , ioe_filename    = Nothing
        }
-}

    | otherwise = atomically $
    withModel "merge" (thSession hdl1) (thRef hdl1) $ \tbl1 ->
    withModel "merge" (thSession hdl2) (thRef hdl2) $ \tbl2 -> do
        let tbl = Model.merge tbl1 tbl2
        thRef' <- newTMVar tbl
        i <- new_handle (thSession hdl1) thRef'
        return TableHandle { thRef = thRef', thId = i, thSession = thSession hdl1 }

{-------------------------------------------------------------------------------
  Internal helpers
-------------------------------------------------------------------------------}

withModel :: IOLike m => String -> Session m -> TMVar m a -> (a -> STM m r) -> STM m r
withModel fun s ref kont = do
    m <- tryReadTMVar ref
    case m of
        Nothing -> throwSTM IOError
            { ioe_handle      = Nothing
            , ioe_type        = IllegalOperation
            , ioe_location    = fun
            , ioe_description = "table handle closed"
            , ioe_errno       = Nothing
            , ioe_filename    = Nothing
            }
        Just m' -> do
            check_session_open fun s
            kont m'

writeTMVar :: MonadSTM m => TMVar m a -> a -> STM m ()
writeTMVar t n = tryTakeTMVar t >> putTMVar t n
