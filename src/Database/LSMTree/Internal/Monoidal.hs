module Database.LSMTree.Internal.Monoidal (
    LookupResult (..),
    QueryResult (..),
    Update (..),
) where

import           Control.DeepSeq (NFData (..))

-- | Result of a single point lookup.
data LookupResult v =
    NotFound
  | Found !v
  deriving stock (Eq, Show, Functor, Foldable, Traversable)

-- | A result for one point in a cursor read or range query.
data QueryResult k v =
    FoundInQuery !k !v
  deriving stock (Eq, Show, Functor, Foldable, Traversable)

-- | Monoidal tables support insert, delete and monoidal upsert operations.
--
-- An __update__ is a term that groups all types of table-manipulating
-- operations, like inserts, deletes and mupserts.
data Update v =
    Insert !v
  | Delete
    -- | TODO: should be given a more suitable name.
  | Mupsert !v
  deriving stock (Show, Eq)

instance NFData v => NFData (Update v) where
  rnf (Insert v)  = rnf v
  rnf Delete      = ()
  rnf (Mupsert v) = rnf v
