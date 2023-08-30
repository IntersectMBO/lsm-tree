{-# LANGUAGE StandaloneKindSignatures #-}

-- TODO: remove once the API is implemented.
{-# OPTIONS_GHC -Wno-redundant-constraints #-}
{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Database.LSMTree.Common (
    -- * IOLike
    IOLike
    -- * Sessions
  , Session
  , newSession
  , closeSession
  ) where

import           Control.Concurrent.Class.MonadMVar (MonadMVar)
import           Control.Monad.Class.MonadThrow (MonadCatch, MonadThrow)
import           Data.Kind (Type)
import           System.FS.API (FsPath, HasFS, SomeHasFS)

{-------------------------------------------------------------------------------
  IOLike
-------------------------------------------------------------------------------}

-- | Utility class for grouping @io-classes@ constraints.
class (MonadMVar m, MonadThrow m, MonadCatch m) => IOLike m where
instance IOLike IO

{-------------------------------------------------------------------------------
  Sessions
-------------------------------------------------------------------------------}

-- | Context shared across multiple table handles, like counters and filesystem
-- information.
--
-- For one, this is necessary if we want to be able to manipulate and query
-- table handles, duplicate table handles, and load snapshots, all at the same
-- time in the same directory.
--
-- Different types of tables can be live in the same session, but operations
-- like 'merge' only work for __compatible__ tables: tables that belong to the
-- same session, store the same key and value types, and have the same
-- configuration parameters.
type Session :: (Type -> Type) -> Type
data Session m = Session {
    sessionRoot  :: !FsPath
  , sessionHasFS :: !(SomeHasFS m)
  }

newSession ::
     IOLike m
  => HasFS m h
  -> FsPath -- ^ Path to an empty directory.
  -> m (Session m)
newSession = undefined

closeSession :: IOLike m => Session m -> m ()
closeSession = undefined
