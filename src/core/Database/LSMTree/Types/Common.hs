{-# LANGUAGE DeriveFunctor #-}

module Database.LSMTree.Types.Common (Range (..)) where

-- | A range of keys.
--
-- TODO: consider adding key prefixes to the range type.
data Range k =
    -- | Inclusive lower bound, exclusive upper bound
    FromToExcluding k k
    -- | Inclusive lower bound, inclusive upper bound
  | FromToIncluding k k
  deriving (Show, Eq, Functor)
