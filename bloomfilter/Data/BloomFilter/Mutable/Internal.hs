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

import qualified Data.BloomFilter.BitVec64 as V

import qualified Data.BloomFilter.Hash as Hash

import Prelude hiding (elem, length, notElem,
                       (/), (*), div, divMod, mod, rem)

-- | A mutable Bloom filter, for use within the 'ST' monad.
data MBloom s a = MB {
      hashesN  :: {-# UNPACK #-} !Int
    , size     :: {-# UNPACK #-} !Int  -- ^ size is multiple of 64
    , bitArray :: {-# UNPACK #-} !(V.MBitVec64 s)
    }
type role MBloom nominal nominal

hashes :: Hash.Hashable a => MBloom s a -> a -> [Hash.Hash]
hashes mb = Hash.cheapHashes (hashesN mb)
{-# INLINE hashes #-}

instance Show (MBloom s a) where
    show mb = "MBloom { " ++ show (size mb) ++ " bits } "
