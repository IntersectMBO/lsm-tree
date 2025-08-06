{-# LANGUAGE DeriveFunctor #-}
{-# OPTIONS_HADDOCK not-home #-}

module Database.LSMTree.Internal.Range (
    Range (..)
  ) where

import           Control.DeepSeq (NFData (..))

{-------------------------------------------------------------------------------
  Small auxiliary types
-------------------------------------------------------------------------------}

-- | A range of keys.
data Range k =
    {- |
    @'FromToExcluding' i j@ is the range from @i@ (inclusive) to @j@ (exclusive).
    -}
    FromToExcluding k k
    {- |
    @'FromToIncluding' i j@ is the range from @i@ (inclusive) to @j@ (inclusive).
    -}
  | FromToIncluding k k
  deriving stock (Show, Eq, Functor)

instance NFData k => NFData (Range k) where
  rnf (FromToExcluding k1 k2) = rnf k1 `seq` rnf k2
  rnf (FromToIncluding k1 k2) = rnf k1 `seq` rnf k2
