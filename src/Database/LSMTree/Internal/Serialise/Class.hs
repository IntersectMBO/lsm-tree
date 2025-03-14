-- | Public API for serialisation of keys, blobs and values
module Database.LSMTree.Internal.Serialise.Class (
    SerialiseKey (..)
  , serialiseKeyIdentity
  , serialiseKeyIdentityUpToSlicing
  , serialiseKeyPreservesOrdering
  , serialiseKeyMinimalSize
  , SerialiseValue (..)
  , serialiseValueIdentity
  , serialiseValueIdentityUpToSlicing
  , RawBytes (..)
  , packSlice
    -- * Errors
  , requireBytesExactly
  ) where

import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as B
import qualified Data.ByteString.Lazy as LBS
import qualified Data.ByteString.Short.Internal as SBS
import           Data.Monoid (Sum (..))
import qualified Data.Primitive as P
import qualified Data.Vector.Primitive as VP
import           Data.Void (Void, absurd)
import           Data.Word
import           Database.LSMTree.Internal.ByteString (byteArrayToSBS)
import           Database.LSMTree.Internal.Primitive (indexWord8ArrayAsWord64)
import           Database.LSMTree.Internal.RawBytes (RawBytes (..))
import qualified Database.LSMTree.Internal.RawBytes as RB
import           Database.LSMTree.Internal.Vector
import           Numeric (showInt)

-- | Serialisation of keys.
--
-- Instances should satisfy the following:
--
-- [Identity] @'deserialiseKey' ('serialiseKey' x) == x@
-- [Identity up to slicing] @'deserialiseKey' ('packSlice' prefix ('serialiseKey' x) suffix) == x@
--
-- Instances /may/ satisfy the following:
--
-- [Ordering-preserving] @x \`'compare'\` y == 'serialiseKey' x \`'compare'\` 'serialiseKey' y@
--
-- Raw bytes are lexicographically ordered, so in particular this means that
-- values should be serialised into big-endian formats. This constraint mainly
-- exists for range queries, where the range is specified in terms of
-- unserialised values, but the internal implementation works on the serialised
-- representation.
class SerialiseKey k where
  serialiseKey :: k -> RawBytes
  -- TODO: 'deserialiseKey' is only strictly necessary for range queries.
  -- It might make sense to move it to a separate class, which could also
  -- require total deserialisation (potentially using 'Either').
  deserialiseKey :: RawBytes -> k

-- | Test the __Identity__ law for the 'SerialiseKey' class
serialiseKeyIdentity :: (Eq k, SerialiseKey k) => k -> Bool
serialiseKeyIdentity x = deserialiseKey (serialiseKey x) == x

-- | Test the __Identity up to slicing__ law for the 'SerialiseKey' class
serialiseKeyIdentityUpToSlicing ::
     (Eq k, SerialiseKey k)
  => RawBytes -> k -> RawBytes -> Bool
serialiseKeyIdentityUpToSlicing prefix x suffix =
    deserialiseKey (packSlice prefix (serialiseKey x) suffix) == x

-- | Test the __Ordering-preserving__ law for the 'SerialiseKey' class
serialiseKeyPreservesOrdering :: (Ord k, SerialiseKey k) => k -> k -> Bool
serialiseKeyPreservesOrdering x y = x `compare` y == serialiseKey x `compare` serialiseKey y

-- | Test the __Minimal size__ law for the 'SerialiseKey' class.
serialiseKeyMinimalSize :: SerialiseKey k => k -> Bool
serialiseKeyMinimalSize x = RB.size (serialiseKey x) >= 8

-- | Serialisation of values and blobs.
--
-- Instances should satisfy the following:
--
-- [Identity] @'deserialiseValue' ('serialiseValue' x) == x@
-- [Identity up to slicing] @'deserialiseValue' ('packSlice' prefix ('serialiseValue' x) suffix) == x@
class SerialiseValue v where
  serialiseValue :: v -> RawBytes
  deserialiseValue :: RawBytes -> v


-- | An instance for 'Sum' which is transparent to the serialisation of @a@.
--
-- Note: If you want to serialize @Sum a@ differently than @a@, then you should
-- create another @newtype@ over 'Sum' and define your alternative serialization.
instance SerialiseValue a => SerialiseValue (Sum a) where
  serialiseValue (Sum v) = serialiseValue v

  deserialiseValue = Sum . deserialiseValue

-- | Test the __Identity__ law for the 'SerialiseValue' class
serialiseValueIdentity :: (Eq v, SerialiseValue v) => v -> Bool
serialiseValueIdentity x = deserialiseValue (serialiseValue x) == x

-- | Test the __Identity up to slicing__ law for the 'SerialiseValue' class
serialiseValueIdentityUpToSlicing ::
     (Eq v, SerialiseValue v)
  => RawBytes -> v -> RawBytes -> Bool
serialiseValueIdentityUpToSlicing prefix x suffix =
    deserialiseValue (packSlice prefix (serialiseValue x) suffix) == x

{-------------------------------------------------------------------------------
  RawBytes
-------------------------------------------------------------------------------}

-- | @'packSlice' prefix x suffix@ makes @x@ into a slice with @prefix@ bytes on
-- the left and @suffix@ bytes on the right.
packSlice :: RawBytes -> RawBytes -> RawBytes -> RawBytes
packSlice prefix x suffix =
    RB.take (RB.size x) (RB.drop (RB.size prefix) (prefix <> x <> suffix))

{-------------------------------------------------------------------------------
  Errors
-------------------------------------------------------------------------------}

-- | @'requireBytesExactly' tyName expected actual x@
requireBytesExactly :: String -> Int -> Int -> a -> a
requireBytesExactly tyName expected actual x
  | expected == actual = x
  | otherwise          =
        error
      $ showString "deserialise "
      . showString tyName
      . showString ": expected "
      . showInt expected
      . showString " bytes, but got "
      . showInt actual
      $ ""

{-------------------------------------------------------------------------------
  Word64
-------------------------------------------------------------------------------}

instance SerialiseKey Word64 where
  serialiseKey x = RB.RawBytes $ byteVectorFromPrim $ byteSwap64 x

  deserialiseKey (RawBytes (VP.Vector off len ba)) =
    requireBytesExactly "Word64" 8 len $ byteSwap64 (indexWord8ArrayAsWord64 ba off)

instance SerialiseValue Word64 where
  serialiseValue x = RB.RawBytes $ byteVectorFromPrim $ x

  deserialiseValue (RawBytes (VP.Vector off len ba)) =
    requireBytesExactly "Word64" 8 len $ indexWord8ArrayAsWord64 ba off

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
  deserialiseValue = B.toLazyByteString . RB.builder

-- | Placeholder instance, not optimised
instance SerialiseValue BS.ByteString where
  serialiseValue = RB.fromShortByteString . SBS.toShort
  deserialiseValue = LBS.toStrict . deserialiseValue

{-------------------------------------------------------------------------------
 ShortByteString
-------------------------------------------------------------------------------}

instance SerialiseKey SBS.ShortByteString where
  serialiseKey = RB.fromShortByteString
  deserialiseKey = byteArrayToSBS . RB.force

instance SerialiseValue SBS.ShortByteString where
  serialiseValue = RB.fromShortByteString
  deserialiseValue = byteArrayToSBS . RB.force

{-------------------------------------------------------------------------------
 ByteArray
-------------------------------------------------------------------------------}

-- | The 'Ord' instance of 'ByteArray' is not lexicographic, so there cannot be
-- an order-preserving instance of 'SerialiseKey'.
-- Use 'ShortByteString' instead.
instance SerialiseValue P.ByteArray where
  serialiseValue ba = RB.fromByteArray 0 (P.sizeofByteArray ba) ba
  deserialiseValue = RB.force

{-------------------------------------------------------------------------------
Void
-------------------------------------------------------------------------------}

-- | The 'deserialiseValue' of this instance throws. (as does e.g. 'Word64' instance on invalid input.)
--
-- This instance is useful for tables without blobs.
instance SerialiseValue Void where
  serialiseValue = absurd
  deserialiseValue = error "panic"
