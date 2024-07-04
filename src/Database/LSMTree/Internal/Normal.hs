module Database.LSMTree.Internal.Normal (
    LookupResult (..),
    RangeLookupResult (..),
    Update (..),
) where

import           Control.DeepSeq (NFData (..))
import           Data.Bifunctor (Bifunctor (..))

-- | Result of a single point lookup.
data LookupResult v blobref =
    NotFound
  | Found         !v
  | FoundWithBlob !v !blobref
  deriving (Eq, Show, Functor, Foldable, Traversable)

instance Bifunctor LookupResult where
  first f = \case
      NotFound          -> NotFound
      Found v           -> Found (f v)
      FoundWithBlob v b -> FoundWithBlob (f v) b

  second g = \case
      NotFound          -> NotFound
      Found v           -> Found v
      FoundWithBlob v b -> FoundWithBlob v (g b)

-- | A result for one point in a range lookup.
data RangeLookupResult k v blobref =
    FoundInRange         !k !v
  | FoundInRangeWithBlob !k !v !blobref
  deriving (Eq, Show, Functor, Foldable, Traversable)

-- | Normal tables support insert and delete operations.
--
-- An __update__ is a term that groups all types of table-manipulating
-- operations, like inserts and deletes.
data Update v blob =
    Insert !v !(Maybe blob)
  | Delete
  deriving (Show, Eq)

instance (NFData v, NFData blob) => NFData (Update v blob) where
  rnf Delete       = ()
  rnf (Insert v b) = rnf v `seq` rnf b
