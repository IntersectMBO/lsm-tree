{-# LANGUAGE ConstraintKinds          #-}
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
    -- * Internals
  , new_handle
  , close_handle
) where

import           Control.Concurrent.Class.MonadSTM
import           Control.Monad (void)
import           Data.Dynamic (Dynamic)
import           Data.Kind (Type)
import           Data.Map (Map)
import qualified Data.Map.Strict as Map
import           Database.LSMTree.Common (IOLike, SnapshotName)

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

-- | Create either a new empty table session.
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
