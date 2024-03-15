{-# LANGUAGE RoleAnnotations          #-}
{-# LANGUAGE StandaloneKindSignatures #-}
{-# OPTIONS_HADDOCK not-home #-}
-- | This module exports 'MBloom'' internals.
module Data.BloomFilter.Mutable.Internal (
    MBloom'(..),
) where

import qualified Data.BloomFilter.BitVec64 as V
import           Data.Kind (Type)
import           Data.Word (Word64)

import           Prelude hiding (div, divMod, elem, length, mod, notElem, rem,
                     (*), (/))

-- | A mutable Bloom filter, for use within the 'ST' monad.
type MBloom' :: Type -> (Type -> Type) -> Type -> Type
data MBloom' s h a = MBloom {
      hashesN  :: {-# UNPACK #-} !Int
    , size     :: {-# UNPACK #-} !Word64  -- ^ size is non-zero
    , bitArray :: {-# UNPACK #-} !(V.MBitVec64 s)
    }
type role MBloom' nominal nominal nominal

instance Show (MBloom' s h a) where
    show mb = "MBloom { " ++ show (size mb) ++ " bits } "
