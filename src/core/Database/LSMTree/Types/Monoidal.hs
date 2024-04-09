module Database.LSMTree.Types.Monoidal (
    LookupResult (..),
    RangeLookupResult (..),
    Update (..),
) where

-- | Result of a single point lookup.
data LookupResult k v =
    NotFound      !k
  | Found         !k !v
  deriving (Eq, Show)


-- | A result for one point in a range lookup.
data RangeLookupResult k v =
    FoundInRange         !k !v
  deriving (Eq, Show)

-- | Monoidal tables support insert, delete and monoidal upsert operations.
--
-- An __update__ is a term that groups all types of table-manipulating
-- operations, like inserts, deletes and mupserts.
data Update v =
    Insert !v
  | Delete
    -- | TODO: should be given a more suitable name.
  | Mupsert !v
  deriving (Show, Eq)
