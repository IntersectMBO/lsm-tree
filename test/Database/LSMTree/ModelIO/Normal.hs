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
import           Database.LSMTree.Normal (LookupResult (..),
                     RangeLookupResult (..), Update (..))
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
lookups ks th@TableHandle {..} = atomically $
    withModel "lookups" thSession thRef $ \(updc, tbl) ->
        return $ liftBlobRefs th updc $ Model.lookups ks tbl

-- | Perform a range lookup.
rangeLookup ::
     (IOLike m, SerialiseKey k, SerialiseValue v)
  => Model.Range k
  -> TableHandle m k v blob
  -> m (V.Vector (RangeLookupResult k v (BlobRef m blob)))
rangeLookup r th@TableHandle {..} = atomically $
    withModel "rangeLookup" thSession thRef $ \(updc, tbl) ->
        return $ liftBlobRefs th updc $ Model.rangeLookup r tbl

-- | Perform a mixed batch of inserts and deletes.
updates ::
     (IOLike m, SerialiseKey k, SerialiseValue v, SerialiseValue blob)
  => V.Vector (k, Update v blob)
  -> TableHandle m k v blob
  -> m ()
updates ups TableHandle {..} = atomically $
    withModel "updates" thSession thRef $ \(updc, tbl) ->
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
  , brRef       :: !(SomeTableRef m blob)
  , brCreatedAt :: !UpdateCounter
  , brBlob      :: !(Model.BlobRef blob)
  }

data SomeTableRef m blob where
  SomeTableRef :: !(TMVar m (UpdateCounter, Model.Table k v blob)) -> SomeTableRef m blob

liftBlobRefs ::
     forall m f g k v blob. (Functor f, Functor g)
  => TableHandle m k v blob
  -> UpdateCounter
  -> g (f (Model.BlobRef blob))
  -> g (f (BlobRef m blob))
liftBlobRefs TableHandle{..} updc = fmap (fmap liftBlobRef)
  where liftBlobRef = BlobRef thSession (SomeTableRef thRef) updc

-- | Perform a batch of blob retrievals.
retrieveBlobs ::
     forall m blob. (IOLike m, SerialiseValue blob)
  => Session m
  -> V.Vector (BlobRef m blob)
  -> m (V.Vector blob)
retrieveBlobs _ brefs = atomically $ Model.retrieveBlobs <$> mapM guard brefs
  where
    -- Ensure that the session and table handle for each of the blob refs are
    -- still open, and that the table wasn't updated.
    guard :: BlobRef m blob -> STM m (Model.BlobRef blob)
    guard BlobRef{brRef = SomeTableRef ref, ..} =
      withModel "retrieveBlobs" brSession ref $ \(updc, _tbl) -> do
        when (updc /= brCreatedAt) $ throwSTM IOError
            { ioe_handle      = Nothing
            , ioe_type        = IllegalOperation
            , ioe_location    = "retrieveBlobs"
            , ioe_description = "blob reference invalidated"
            , ioe_errno       = Nothing
            , ioe_filename    = Nothing
            }
        pure brBlob

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
    withModel "snapshot" thSession thRef $ \tbl -> do
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
    withModel "duplicate" thSession thRef $ \tbl -> do
        thRef' <- newTMVar tbl
        i <- new_handle thSession thRef'
        return TableHandle { thRef = thRef', thId = i, thSession = thSession }

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
