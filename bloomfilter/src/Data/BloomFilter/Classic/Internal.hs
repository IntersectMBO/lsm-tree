{-# OPTIONS_HADDOCK not-home #-}
-- | This module exports 'Bloom'' definition.
module Data.BloomFilter.Classic.Internal (
    Bloom(..),
    bloomInvariant,
) where

import           Control.DeepSeq (NFData (..))
import           Data.Bits
import qualified Data.BloomFilter.Classic.BitVec64 as V
import           Data.Kind (Type)
import           Data.Primitive.ByteArray (sizeofByteArray)
import qualified Data.Vector.Primitive as VP

type Bloom :: Type -> Type
data Bloom a = Bloom {
      numBits   :: {-# UNPACK #-} !Int  -- ^ non-zero
    , numHashes :: {-# UNPACK #-} !Int
    , bitArray  :: {-# UNPACK #-} !V.BitVec64
    }
type role Bloom nominal

bloomInvariant :: Bloom a -> Bool
bloomInvariant Bloom { numBits = s, bitArray = V.BV64 (VP.Vector off len ba) } =
       s > 0
    && s <= 2^(48 :: Int)
    && off >= 0
    && ceilDiv64 s == fromIntegral len
    && (off + len) * 8 <= sizeofByteArray ba
  where
    ceilDiv64 x = unsafeShiftR (x + 63) 6

instance Eq (Bloom a) where
    -- We support arbitrary sized bitvectors,
    -- therefore an equality is a bit involved:
    -- we need to be careful when comparing the last bits of bitArray.
    (==) Bloom { numBits = n,  numHashes = k,  bitArray = V.BV64 v  }
         Bloom { numBits = n', numHashes = k', bitArray = V.BV64 v' } =
        k == k' &&
        n == n' &&
        VP.take w v == VP.take w v' && -- compare full words
        if l == 0 then True else unsafeShiftL x s == unsafeShiftL x' s -- compare last words
      where
        !w = fromIntegral (unsafeShiftR n 6) :: Int  -- n `div` 64
        !l = fromIntegral (n .&. 63) :: Int          -- n `mod` 64
        !s = 64 - l

        -- last words
        x = VP.unsafeIndex v w
        x' = VP.unsafeIndex v' w

instance Show (Bloom a) where
    show mb = "Bloom { " ++ show (numBits mb) ++ " bits } "

instance NFData (Bloom a) where
    rnf !_ = ()
