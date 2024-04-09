{-# LANGUAGE DeriveTraversable #-}
module Database.LSMTree.Types.Normal (
    LookupResult (..),
    RangeLookupResult (..),
    Update (..),
) where

-- | Result of a single point lookup.
data LookupResult k v blobref =
    NotFound      !k
  | Found         !k !v
  | FoundWithBlob !k !v !blobref
  deriving (Eq, Show, Functor, Foldable, Traversable)

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
