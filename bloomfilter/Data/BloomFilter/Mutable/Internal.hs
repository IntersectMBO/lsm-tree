-- |
-- Module: Data.BloomFilter.Mutable.Internal
-- Copyright: Bryan O'Sullivan
-- License: BSD3
--
-- Maintainer: Bryan O'Sullivan <bos@serpentine.com>
-- Stability: unstable
-- Portability: portable
{-# LANGUAGE RoleAnnotations #-}
module Data.BloomFilter.Mutable.Internal
    (
    -- * Types
      Hash.Hash
    , MBloom(..)
    , hashes
    ) where

import Data.Bits (shiftL)

import Data.Bit (Bit)
import qualified Data.Vector.Unboxed as UV

import qualified Data.BloomFilter.Hash as Hash

import Prelude hiding (elem, length, notElem,
                       (/), (*), div, divMod, mod, rem)

-- | A mutable Bloom filter, for use within the 'ST' monad.
data MBloom s a = MB {
      hashesN :: {-# UNPACK #-} !Int
    , shift :: {-# UNPACK #-} !Int
    , mask :: {-# UNPACK #-} !Int
    , bitArray :: {-# UNPACK #-} !(UV.MVector s Bit)
    }
type role MBloom nominal nominal

hashes :: Hash.Hashable a => MBloom s a -> a -> [Hash.Hash]
hashes mb = Hash.cheapHashes (hashesN mb)
{-# INLINE hashes #-}

instance Show (MBloom s a) where
    show mb = "MBloom { " ++ show ((1::Int) `shiftL` shift mb) ++ " bits } "
