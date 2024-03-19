{-# LANGUAGE BangPatterns             #-}
{-# LANGUAGE RoleAnnotations          #-}
{-# LANGUAGE StandaloneKindSignatures #-}
{-# OPTIONS_HADDOCK not-home #-}
-- | This module exports 'Bloom'' definition.
module Data.BloomFilter.Internal (
    Bloom'(..),
) where

import           Control.DeepSeq (NFData (..))
import           Data.Bits
import qualified Data.BloomFilter.BitVec64 as V
import           Data.Kind (Type)
import qualified Data.Vector.Primitive as P
import           Data.Word (Word64)

type Bloom' :: (Type -> Type) -> Type -> Type
data Bloom' h a = Bloom {
      hashesN  :: {-# UNPACK #-} !Int
    , size     :: {-# UNPACK #-} !Word64 -- ^ size is non-zero
    , bitArray :: {-# UNPACK #-} !V.BitVec64
    }
type role Bloom' nominal nominal

instance Eq (Bloom' h a) where
    -- We support arbitrary sized bitvectors,
    -- therefore an equality is a bit involved:
    -- we need to be careful when comparing the last bits of bitArray.
    Bloom k n (V.BV64 v) == Bloom k' n' (V.BV64 v') =
        k == k' &&
        n == n' &&
        P.take w v == P.take w v' && -- compare full words
        if l == 0 then True else unsafeShiftL x s == unsafeShiftL x' s -- compare last words
      where
        !w = fromIntegral (unsafeShiftR n 6) :: Int  -- n `div` 64
        !l = fromIntegral (n .&. 63) :: Int          -- n `mod` 64
        !s = 64 - l

        -- last words
        x = P.unsafeIndex v w
        x' = P.unsafeIndex v' w

instance Show (Bloom' h a) where
    show mb = "Bloom { " ++ show (size mb) ++ " bits } "

instance NFData (Bloom' h a) where
    rnf !_ = ()
