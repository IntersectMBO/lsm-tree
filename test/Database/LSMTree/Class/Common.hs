{-# LANGUAGE TypeFamilies #-}

module Database.LSMTree.Class.Common (
    C
  , C_
  , IsSession (..)
  , SessionArgs (..)
  , withSession
  , module Types
  ) where

import           Control.Monad.Class.MonadThrow (MonadThrow (..))
import           Control.Tracer (nullTracer)
import           Data.Kind (Constraint, Type)
import           Data.Typeable (Typeable)
import           Database.LSMTree.Common as Types (IOLike, Range (..),
                     SerialiseKey, SerialiseValue, SnapshotLabel (..),
                     SnapshotName)
import qualified Database.LSMTree.Common as R
import           System.FS.API (FsPath, HasFS)
import           System.FS.BlockIO.API (HasBlockIO)

-- | Model-specific constraints
type C k v blob = (C_ k, C_ v, C_ blob)
type C_ a = (Show a, Eq a, Typeable a)

-- | Class abstracting over session operations.
--
type IsSession :: ((Type -> Type) -> Type) -> Constraint
class IsSession s where
    data SessionArgs s :: (Type -> Type) -> Type

    openSession ::
           IOLike m
        => SessionArgs s m
        -> m (s m)

    closeSession ::
           IOLike m
        => s m
        -> m ()

    deleteSnapshot ::
           IOLike m
        => s m
        -> SnapshotName
        -> m ()

    listSnapshots ::
           IOLike m
        => s m
        -> m [SnapshotName]

withSession :: (IOLike m, IsSession s) => SessionArgs s m -> (s m -> m a) -> m a
withSession seshArgs = bracket (openSession seshArgs) closeSession

{-------------------------------------------------------------------------------
  Real instance
-------------------------------------------------------------------------------}

instance IsSession R.Session where
    data SessionArgs R.Session m where
      SessionArgs ::
           forall m h. Typeable h
        => HasFS m h -> HasBlockIO m h -> FsPath
        -> SessionArgs R.Session m

    openSession (SessionArgs hfs hbio dir) = do
       R.openSession nullTracer hfs hbio dir
    closeSession = R.closeSession
    deleteSnapshot = R.deleteSnapshot
    listSnapshots = R.listSnapshots
