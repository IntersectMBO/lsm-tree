module Database.LSMTree.Internal.Normal (
    LookupResult (..),
    QueryResult (..),
    Update (..),
) where

import           Control.DeepSeq (NFData (..))
import           Data.Bifunctor (Bifunctor (..))

-- | Result of a single point lookup.
data LookupResult v blobref =
    NotFound
  | Found         !v
  | FoundWithBlob !v !blobref
  deriving stock (Eq, Show, Functor, Foldable, Traversable)

instance Bifunctor LookupResult where
  first f = \case
      NotFound          -> NotFound
      Found v           -> Found (f v)
      FoundWithBlob v b -> FoundWithBlob (f v) b

  second g = \case
      NotFound          -> NotFound
      Found v           -> Found v
      FoundWithBlob v b -> FoundWithBlob v (g b)

-- | A result for one point in a cursor read or range lookup.
data QueryResult k v blobref =
    FoundInQuery         !k !v
  | FoundInQueryWithBlob !k !v !blobref
  deriving stock (Eq, Show, Functor, Foldable, Traversable)

instance Bifunctor (QueryResult k) where
  bimap f g = \case
      FoundInQuery k v           -> FoundInQuery k (f v)
      FoundInQueryWithBlob k v b -> FoundInQueryWithBlob k (f v) (g b)

-- | Normal tables support insert and delete operations.
--
-- An __update__ is a term that groups all types of table-manipulating
-- operations, like inserts and deletes.
data Update v blob =
    Insert !v !(Maybe blob)
  | Delete
  deriving stock (Show, Eq)

instance (NFData v, NFData blob) => NFData (Update v blob) where
  rnf Delete       = ()
  rnf (Insert v b) = rnf v `seq` rnf b
