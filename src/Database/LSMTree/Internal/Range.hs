{-# LANGUAGE DeriveFunctor #-}

module Database.LSMTree.Internal.Range (
    Range (..)
  ) where

import           Control.DeepSeq (NFData (..))

{-------------------------------------------------------------------------------
  Small auxiliary types
-------------------------------------------------------------------------------}

-- | A range of keys.
data Range k =
    -- | Inclusive lower bound, exclusive upper bound
    FromToExcluding k k
    -- | Inclusive lower bound, inclusive upper bound
  | FromToIncluding k k
  deriving stock (Show, Eq, Functor)

instance NFData k => NFData (Range k) where
  rnf (FromToExcluding k1 k2) = rnf k1 `seq` rnf k2
  rnf (FromToIncluding k1 k2) = rnf k1 `seq` rnf k2
