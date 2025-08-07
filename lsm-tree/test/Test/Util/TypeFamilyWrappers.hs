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
--   the uses of 'WrapTable' and co in the definition of 'RealizeIOSim',
--   which can be found in "Test.Util.Orphans".
module Test.Util.TypeFamilyWrappers (
    WrapSession (..)
  , WrapTable (..)
  , WrapCursor (..)
  , WrapBlobRef (..)
  , WrapBlob (..)
  ) where

import           Data.Kind (Type)
import qualified Database.LSMTree.Class as SUT.Class

type WrapSession ::
     ((Type -> Type) -> Type -> Type -> Type -> Type)
  -> (Type -> Type) -> Type
newtype WrapSession h m = WrapSession {
    unwrapSession :: SUT.Class.Session h m
  }

type WrapTable ::
     ((Type -> Type) -> Type -> Type -> Type -> Type)
  -> (Type -> Type) -> Type -> Type -> Type -> Type
newtype WrapTable h m k v b = WrapTable {
    unwrapTable :: h m k v b
  }
  deriving stock (Show, Eq)

type WrapCursor ::
     ((Type -> Type) -> Type -> Type -> Type -> Type)
  -> (Type -> Type) -> Type -> Type -> Type -> Type
newtype WrapCursor h m k v b = WrapCursor {
    unwrapCursor :: SUT.Class.Cursor h m k v b
  }

type WrapBlobRef ::
     ((Type -> Type) -> Type -> Type -> Type -> Type)
  -> (Type -> Type) -> Type -> Type
newtype WrapBlobRef h m b = WrapBlobRef {
    unwrapBlobRef :: SUT.Class.BlobRef h m b
  }

deriving stock instance Show (SUT.Class.BlobRef h m b) => Show (WrapBlobRef h m b)
deriving stock instance Eq (SUT.Class.BlobRef h m b) => Eq (WrapBlobRef h m b)

type WrapBlob :: Type -> Type
newtype WrapBlob b = WrapBlob {
    unwrapBlob :: b
  }
  deriving stock (Show, Eq)
