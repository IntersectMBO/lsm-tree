{-# LANGUAGE FlexibleContexts         #-}
{-# LANGUAGE StandaloneDeriving       #-}
{-# LANGUAGE StandaloneKindSignatures #-}
{-# LANGUAGE UndecidableInstances     #-}

-- | Type family wrappers are useful for a variety of reasons:
--
-- * Type families can not be partially applied, but type wrappers can.
--
-- * Type family synonyms can not appear in a class head, but type wrappers can.
--
-- * Wrappers can be used to direct type family reduction. For an example, see
--   the uses of 'WrapTableHandle' and co in the definition of 'RealizeIOSim',
--   which can be found in "Test.Util.Orphans".
module Test.Util.TypeFamilyWrappers (
    WrapSession (..)
  , WrapTableHandle (..)
  , WrapCursor (..)
  , WrapBlobRef (..)
  , WrapBlob (..)
  ) where

import           Data.Kind (Type)
import qualified Database.LSMTree.Class.Normal as SUT.Class

type WrapSession ::
     ((Type -> Type) -> Type -> Type -> Type -> Type)
  -> (Type -> Type) -> Type
newtype WrapSession h m = WrapSession {
    unwrapSession :: SUT.Class.Session h m
  }

type WrapTableHandle ::
     ((Type -> Type) -> Type -> Type -> Type -> Type)
  -> (Type -> Type) -> Type -> Type -> Type -> Type
newtype WrapTableHandle h m k v blob = WrapTableHandle {
    unwrapTableHandle :: h m k v blob
  }
  deriving stock (Show, Eq)

type WrapCursor ::
     ((Type -> Type) -> Type -> Type -> Type -> Type)
  -> (Type -> Type) -> Type -> Type -> Type -> Type
newtype WrapCursor h m k v blob = WrapCursor {
    unwrapCursor :: SUT.Class.Cursor h m k v blob
  }

type WrapBlobRef ::
     ((Type -> Type) -> Type -> Type -> Type -> Type)
  -> (Type -> Type) -> Type -> Type
newtype WrapBlobRef h m blob = WrapBlobRef {
    unwrapBlobRef :: SUT.Class.BlobRef h m blob
  }

deriving stock instance Show (SUT.Class.BlobRef h m blob) => Show (WrapBlobRef h m blob)
deriving stock instance Eq (SUT.Class.BlobRef h m blob) => Eq (WrapBlobRef h m blob)

type WrapBlob :: Type -> Type
newtype WrapBlob blob = WrapBlob {
    unwrapBlob :: blob
  }
  deriving stock (Show, Eq)
