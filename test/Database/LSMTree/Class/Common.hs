{-# LANGUAGE TypeFamilies #-}

module Database.LSMTree.Class.Common (
    C, CK, CV, CB, C_
  , IsSession (..)
  , SessionArgs (..)
  , withSession
  , module Types
  ) where

import           Control.Monad.Class.MonadThrow (MonadThrow (..))
import           Control.Tracer (nullTracer)
import           Data.Kind (Constraint, Type)
import           Data.Typeable (Typeable)
import           Database.LSMTree (ResolveValue)
import           Database.LSMTree as Types (IOLike, Range (..), SerialiseKey,
                     SerialiseValue, SnapshotLabel (..), SnapshotName,
                     UnionCredits (..), UnionDebt (..))
import qualified Database.LSMTree as R
import           System.FS.API (FsPath, HasFS)
import           System.FS.BlockIO.API (HasBlockIO)

{-------------------------------------------------------------------------------
  Constraints
-------------------------------------------------------------------------------}

-- | Constraints for keys, values, and blobs
type C k v b = (CK k, CV v, CB b)

-- | Constraints for keys
type CK k = (C_ k, SerialiseKey k)

-- | Constraints for values
type CV v = (C_ v, SerialiseValue v, ResolveValue v)

-- | Constraints for blobs
type CB b = (C_ b, SerialiseValue b)

-- | Model-specific constraints for keys, values, and blobs
type C_ a = (Show a, Eq a, Typeable a)

{-------------------------------------------------------------------------------
  Session
-------------------------------------------------------------------------------}

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

testSalt :: R.Salt
testSalt = 4

instance IsSession R.Session where
    data SessionArgs R.Session m where
      SessionArgs ::
           forall m h. Typeable h
        => HasFS m h -> HasBlockIO m h -> FsPath
        -> SessionArgs R.Session m

    openSession (SessionArgs hfs hbio dir) = do
       R.openSession nullTracer hfs hbio testSalt dir
    closeSession = R.closeSession
    deleteSnapshot = R.deleteSnapshot
    listSnapshots = R.listSnapshots
