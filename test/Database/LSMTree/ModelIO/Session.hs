{-# LANGUAGE ConstraintKinds          #-}
{-# LANGUAGE FlexibleContexts         #-}
{-# LANGUAGE GADTs                    #-}
{-# LANGUAGE RecordWildCards          #-}
{-# LANGUAGE ScopedTypeVariables      #-}
{-# LANGUAGE StandaloneDeriving       #-}
{-# LANGUAGE StandaloneKindSignatures #-}
{-# LANGUAGE TupleSections            #-}
{-# LANGUAGE TypeApplications         #-}

module Database.LSMTree.ModelIO.Session (
    -- * Sessions
     Session (..)
  , newSession
  , closeSession
  , listSnapshots
  , deleteSnapshot
    -- * Internals
  , new_handle
  , close_handle
  , check_session_open
) where

import           Control.Concurrent.Class.MonadSTM
import           Control.Monad (unless, void)
import           Control.Monad.Class.MonadThrow (MonadThrow)
import           Data.Dynamic (Dynamic)
import           Data.Kind (Type)
import           Data.Map (Map)
import qualified Data.Map.Strict as Map
import           Database.LSMTree.Common (IOLike, SnapshotName)
import           GHC.IO.Exception (IOErrorType (..), IOException (..))

type Session :: (Type -> Type) -> Type
data Session m = Session
  { session_open :: !(TVar m Bool)
    -- ^ technically we don't need to track whether session is open.
    -- When session is closed, all the table handles are closed as well,
    -- So there shouldn't be situation where we have an open table handle
    -- pointing to closed session.
    --
    -- In a model we track this anyway, to catch bugs.

  , snapshots    :: !(TVar m (Map SnapshotName Dynamic))
    -- ^ model session keeps snapshots in memory.

  , counter      :: !(TVar m Int)
    -- ^ counter for new handles.
  , openHandles  :: !(TVar m (Map Int (STM m ())))
    -- ^ actions to close each open handle
  }

new_handle :: MonadSTM m => Session m -> TMVar m b -> STM m Int
new_handle Session {..} ref = do
    i <- readTVar counter
    writeTVar counter (i + 1)
    modifyTVar' openHandles $ Map.insert i $ void $ tryTakeTMVar ref
    return i

close_handle :: MonadSTM m => Session m -> Int -> STM m ()
close_handle Session {..} i = do
    modifyTVar' openHandles $ Map.delete i

check_session_open :: (MonadSTM m, MonadThrow (STM m)) => String -> Session m -> STM m ()
check_session_open fun s = do
    sok <- readTVar (session_open s)
    unless sok $ throwSTM IOError
        { ioe_handle      = Nothing
        , ioe_type        = IllegalOperation
        , ioe_location    = fun
        , ioe_description = "session closed"
        , ioe_errno       = Nothing
        , ioe_filename    = Nothing
        }

-- | Create a new empty table session.
newSession :: IOLike m => m (Session m)
newSession = atomically $ do
    session_open <- newTVar True
    snapshots <- newTVar Map.empty
    counter <- newTVar 0
    openHandles <- newTVar Map.empty
    return Session {..}

-- | Close the table session.
--
-- This also closes any open table handles in the session.
--
closeSession :: IOLike m => Session m -> m ()
closeSession Session {..} = atomically $ do
    writeTVar session_open False
    hdls <- readTVar openHandles
    sequence_ hdls

-- | List snapshots.
listSnapshots :: IOLike m => Session m -> m [SnapshotName]
listSnapshots s@Session {..} = atomically $ do
    check_session_open "listSnapshots" s
    Map.keys <$> readTVar snapshots

-- | Delete a named snapshot.
--
-- Exceptions:
--
-- * Deleting a snapshot that doesn't exist is an error.
deleteSnapshot :: IOLike m => Session m -> SnapshotName -> m ()
deleteSnapshot s@Session {..} name = atomically $ do
    check_session_open "deleteSnapshot" s

    sss <- readTVar snapshots
    if Map.member name sss
    then writeTVar snapshots (Map.delete name sss)
    else throwSTM IOError
        { ioe_handle      = Nothing
        , ioe_type        = NoSuchThing
        , ioe_location    = "deleteSnapshot"
        , ioe_description = "no such snapshot"
        , ioe_errno       = Nothing
        , ioe_filename    = Nothing
        }
