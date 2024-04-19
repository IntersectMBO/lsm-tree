{-# LANGUAGE MagicHash           #-}
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

import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as B
import qualified Data.ByteString.Lazy as LBS
import qualified Data.ByteString.Short.Internal as SBS
import qualified Data.Primitive as P
import           Data.Proxy (Proxy)
import qualified Data.Vector.Primitive as PV
import           Data.Word
import           Database.LSMTree.Internal.ByteString (byteArrayToSBS)
import           Database.LSMTree.Internal.RawBytes (RawBytes (..))
import qualified Database.LSMTree.Internal.RawBytes as RB
import           Database.LSMTree.Internal.Vector
import           GHC.Exts (Int (..), indexWord8ArrayAsWord64#)
import           GHC.Word (Word64 (..))

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

{-------------------------------------------------------------------------------
  Word64
-------------------------------------------------------------------------------}

instance SerialiseKey Word64 where
  serialiseKey x =
    RB.RawBytes $ mkPrimVector 0 8 $ P.runByteArray $ do
      ba <- P.newByteArray 8
      P.writeByteArray ba 0 $ byteSwap64 x
      return ba

  deserialiseKey (RawBytes (PV.Vector (I# off#) len (P.ByteArray ba#)))
    | len >= 8  = byteSwap64 (W64# (indexWord8ArrayAsWord64# ba# off# ))
    | otherwise = error "deserialiseKey: not enough bytes for Word64"

{-------------------------------------------------------------------------------
  ByteString
-------------------------------------------------------------------------------}

-- | Placeholder instance, not optimised
instance SerialiseKey LBS.ByteString where
  serialiseKey = serialiseKey . LBS.toStrict
  deserialiseKey = B.toLazyByteString . RB.builder

-- | Placeholder instance, not optimised
instance SerialiseKey BS.ByteString where
  serialiseKey = RB.fromShortByteString . SBS.toShort
  deserialiseKey = LBS.toStrict . deserialiseKey

-- | Placeholder instance, not optimised
instance SerialiseValue LBS.ByteString where
  serialiseValue = serialiseValue . LBS.toStrict
  deserialiseValue = deserialiseValueN . pure
  deserialiseValueN = B.toLazyByteString . foldMap RB.builder

-- | Placeholder instance, not optimised
instance SerialiseValue BS.ByteString where
  serialiseValue = RB.fromShortByteString . SBS.toShort
  deserialiseValue = deserialiseValueN . pure
  deserialiseValueN = LBS.toStrict . deserialiseValueN

{-------------------------------------------------------------------------------
 ShortByteString
-------------------------------------------------------------------------------}

instance SerialiseKey SBS.ShortByteString where
  serialiseKey = RB.fromShortByteString
  deserialiseKey = byteArrayToSBS . RB.force

instance SerialiseValue SBS.ShortByteString where
  serialiseValue = RB.fromShortByteString
  deserialiseValue = byteArrayToSBS . RB.force
  deserialiseValueN = byteArrayToSBS . foldMap RB.force

{-------------------------------------------------------------------------------
 ByteArray
-------------------------------------------------------------------------------}

instance SerialiseKey P.ByteArray where
  serialiseKey ba = RB.fromByteArray 0 (P.sizeofByteArray ba) ba
  deserialiseKey = RB.force

instance SerialiseValue P.ByteArray where
  serialiseValue ba = RB.fromByteArray 0 (P.sizeofByteArray ba) ba
  deserialiseValue = RB.force
  deserialiseValueN = foldMap RB.force
