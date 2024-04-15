{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}

-- | Public API for serialisation of keys, blobs and values
module Database.LSMTree.Internal.Serialise.Class (
    SerialiseKey (..)
  , serialiseKeyIdentity
  , serialiseKeyPreservesOrdering
  , serialiseKeyMinimalSize
  , SerialiseValue (..)
  , serialiseValueIdentity
  , serialiseValueConcatDistributes
  , RawBytes (..)
  ) where

import           Data.Proxy (Proxy)
import           Database.LSMTree.Internal.RawBytes (RawBytes (..))
import qualified Database.LSMTree.Internal.RawBytes as RB

-- | Serialisation of keys.
--
-- Instances should satisfy the following:
--
-- [Identity] @'deserialiseKey' ('serialiseKey' x) == x@
-- [Ordering-preserving] @x \`'compare'\` y == 'serialiseKey' x \`'compare'\` 'serialiseKey' y@
--
-- Raw bytes are lexicographically ordered, so in particular this means that
-- values should be serialised into big-endian formats.
--
-- === IndexCompact constraints
--
-- When using the 'IndexCompact', additional constraints apply to the
-- serialisation function, so in that case instances should also satisfy the
-- following:
--
-- [Minimal size] @'sizeofRawBytes' >= 6@
class SerialiseKey k where
  serialiseKey :: k -> RawBytes
  deserialiseKey :: RawBytes -> k

-- | Test the __Identity__ law for the 'SerialiseKey' class
serialiseKeyIdentity :: (Eq k, SerialiseKey k) => k -> Bool
serialiseKeyIdentity x = deserialiseKey (serialiseKey x) == x

-- | Test the __Ordering-preserving__ law for the 'SerialiseKey' class
serialiseKeyPreservesOrdering :: (Ord k, SerialiseKey k) => k -> k -> Bool
serialiseKeyPreservesOrdering x y = x `compare` y == serialiseKey x `compare` serialiseKey y

-- | Test the __Minimal size__ law for the 'SerialiseKey' class.
serialiseKeyMinimalSize :: SerialiseKey k => k -> Bool
serialiseKeyMinimalSize x = RB.size (serialiseKey x) >= 6

-- | Serialisation of values and blobs.
--
-- Instances should satisfy the following:
--
-- [Identity] @'deserialiseValue' ('serialiseValue' x) == x@
-- [Concat distributes] @'deserialiseValueN' xs == 'deserialiseValue' ('mconcat' xs)@
class SerialiseValue v where
  serialiseValue :: v -> RawBytes
  deserialiseValue :: RawBytes -> v
  -- | Deserialisation when bytes are split into multiple chunks.
  deserialiseValueN :: [RawBytes] -> v

-- | Test the __Identity__ law for the 'SerialiseValue' class
serialiseValueIdentity :: (Eq v, SerialiseValue v) => v -> Bool
serialiseValueIdentity x = deserialiseValue (serialiseValue x) == x

-- | Test the __Concat distributes__ law for the 'SerialiseValue' class
serialiseValueConcatDistributes :: forall v. (Eq v, SerialiseValue v) => Proxy v -> [RawBytes] -> Bool
serialiseValueConcatDistributes _ xs = deserialiseValueN @v xs == deserialiseValue (mconcat xs)
