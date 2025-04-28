{-# OPTIONS_HADDOCK not-home #-}

-- | Public API for serialisation of keys, blobs and values
--
module Database.LSMTree.Internal.Serialise.Class (
    -- * SerialiseKey
    SerialiseKey (..)
  , serialiseKeyIdentity
  , serialiseKeyIdentityUpToSlicing
  , SerialiseKeyOrderPreserving
  , serialiseKeyPreservesOrdering
    -- * SerialiseValue
  , SerialiseValue (..)
  , serialiseValueIdentity
  , serialiseValueIdentityUpToSlicing
    -- * RawBytes
  , RawBytes (..)
  , packSlice
    -- * Errors
  , requireBytesExactly
  ) where

import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as B
import qualified Data.ByteString.Lazy as LBS
import qualified Data.ByteString.Short.Internal as SBS
import qualified Data.ByteString.UTF8 as UTF8
import           Data.Int (Int16, Int32, Int64, Int8)
import           Data.Monoid (Sum (..))
import qualified Data.Primitive as P
import qualified Data.Vector.Primitive as VP
import           Data.Void (Void, absurd)
import           Data.Word (Word16, Word32, Word64, Word8)
import           Database.LSMTree.Internal.ByteString (byteArrayToSBS)
import           Database.LSMTree.Internal.Primitive
import           Database.LSMTree.Internal.RawBytes (RawBytes (..))
import qualified Database.LSMTree.Internal.RawBytes as RB
import           Database.LSMTree.Internal.Vector
import           Numeric (showInt)

{-------------------------------------------------------------------------------
  SerialiseKey
-------------------------------------------------------------------------------}

-- | Serialisation of keys.
--
-- Instances should satisfy the following laws:
--
-- [Identity] @'deserialiseKey' ('serialiseKey' x) == x@
-- [Identity up to slicing] @'deserialiseKey' ('packSlice' prefix ('serialiseKey' x) suffix) == x@
--
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

-- | Order-preserving serialisation of keys
--
-- Internally, the library sorts key\/value pairs using the ordering of
-- /serialised/ keys. Range lookups and cursor reads return key\/value according
-- to this ordering. As such, if serialisation does not preserve the ordering of
-- /unserialised/ keys, then range lookups and cursor reads will return
-- /unserialised/ keys out of order.
--
-- Instances that prevent keys from being returned out of order should satisfy
-- the following law:
--
-- [Ordering-preserving] @x \`'compare'\` y == 'serialiseKey' x \`'compare'\` 'serialiseKey' y@
--
-- Serialised keys (raw bytes) are lexicographically ordered, which means that
-- keys should be serialised into big-endian formats to satisfy the
-- __Ordering-preserving__ law,
--
class SerialiseKey k => SerialiseKeyOrderPreserving k where

-- | Test the __Ordering-preserving__ law for the 'SerialiseKeyOrderPreserving' class
serialiseKeyPreservesOrdering :: (Ord k, SerialiseKey k) => k -> k -> Bool
serialiseKeyPreservesOrdering x y = x `compare` y == serialiseKey x `compare` serialiseKey y

{-------------------------------------------------------------------------------
  SerialiseValue
-------------------------------------------------------------------------------}

-- | Serialisation of values and blobs.
--
-- Instances should satisfy the following laws:
--
-- [Identity] @'deserialiseValue' ('serialiseValue' x) == x@
-- [Identity up to slicing] @'deserialiseValue' ('packSlice' prefix ('serialiseValue' x) suffix) == x@
class SerialiseValue v where
  serialiseValue :: v -> RawBytes
  deserialiseValue :: RawBytes -> v

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
  Int
-------------------------------------------------------------------------------}

instance SerialiseKey Int8 where
  serialiseKey x = RB.RawBytes $ byteVectorFromPrim x

  deserialiseKey (RawBytes (VP.Vector off len ba)) =
    requireBytesExactly "Int8" 1 len $ indexInt8Array ba off

instance SerialiseValue Int8 where
  serialiseValue x = RB.RawBytes $ byteVectorFromPrim $ x

  deserialiseValue (RawBytes (VP.Vector off len ba)) =
    requireBytesExactly "Int8" 1 len $ indexInt8Array ba off

instance SerialiseKey Int16 where
  serialiseKey x = RB.RawBytes $ byteVectorFromPrim $ byteSwapInt16 x

  deserialiseKey (RawBytes (VP.Vector off len ba)) =
    requireBytesExactly "Int16" 2 len $ byteSwapInt16 (indexWord8ArrayAsInt16 ba off)

instance SerialiseValue Int16 where
  serialiseValue x = RB.RawBytes $ byteVectorFromPrim $ x

  deserialiseValue (RawBytes (VP.Vector off len ba)) =
    requireBytesExactly "Int16" 2 len $ indexWord8ArrayAsInt16 ba off

instance SerialiseKey Int32 where
  serialiseKey x = RB.RawBytes $ byteVectorFromPrim $ byteSwapInt32 x

  deserialiseKey (RawBytes (VP.Vector off len ba)) =
    requireBytesExactly "Int32" 4 len $ byteSwapInt32 (indexWord8ArrayAsInt32 ba off)

instance SerialiseValue Int32 where
  serialiseValue x = RB.RawBytes $ byteVectorFromPrim $ x

  deserialiseValue (RawBytes (VP.Vector off len ba)) =
    requireBytesExactly "Int32" 4 len $ indexWord8ArrayAsInt32 ba off

instance SerialiseKey Int64 where
  serialiseKey x = RB.RawBytes $ byteVectorFromPrim $ byteSwapInt64 x

  deserialiseKey (RawBytes (VP.Vector off len ba)) =
    requireBytesExactly "Int64" 8 len $ byteSwapInt64 (indexWord8ArrayAsInt64 ba off)

instance SerialiseValue Int64 where
  serialiseValue x = RB.RawBytes $ byteVectorFromPrim $ x

  deserialiseValue (RawBytes (VP.Vector off len ba)) =
    requireBytesExactly "Int64" 8 len $ indexWord8ArrayAsInt64 ba off

instance SerialiseKey Int where
  serialiseKey x = RB.RawBytes $ byteVectorFromPrim $ byteSwapInt x

  deserialiseKey (RawBytes (VP.Vector off len ba)) =
    requireBytesExactly "Int" 8 len $ byteSwapInt (indexWord8ArrayAsInt ba off)

instance SerialiseValue Int where
  serialiseValue x = RB.RawBytes $ byteVectorFromPrim $ x

  deserialiseValue (RawBytes (VP.Vector off len ba)) =
    requireBytesExactly "Int" 8 len $ indexWord8ArrayAsInt ba off

{-------------------------------------------------------------------------------
  Word
-------------------------------------------------------------------------------}

instance SerialiseKey Word8 where
  serialiseKey x = RB.RawBytes $ byteVectorFromPrim  x

  deserialiseKey (RawBytes (VP.Vector off len ba)) =
    requireBytesExactly "Word8" 1 len  (indexWord8Array ba off)

instance SerialiseKeyOrderPreserving Word8

instance SerialiseValue Word8 where
  serialiseValue x = RB.RawBytes $ byteVectorFromPrim $ x

  deserialiseValue (RawBytes (VP.Vector off len ba)) =
    requireBytesExactly "Word8" 1 len $ indexWord8Array ba off


instance SerialiseKey Word16 where
  serialiseKey x = RB.RawBytes $ byteVectorFromPrim $ byteSwapWord16 x

  deserialiseKey (RawBytes (VP.Vector off len ba)) =
    requireBytesExactly "Word16" 2 len $ byteSwapWord16 (indexWord8ArrayAsWord16 ba off)

instance SerialiseKeyOrderPreserving Word16

instance SerialiseValue Word16 where
  serialiseValue x = RB.RawBytes $ byteVectorFromPrim $ x

  deserialiseValue (RawBytes (VP.Vector off len ba)) =
    requireBytesExactly "Word16" 2 len $ indexWord8ArrayAsWord16 ba off

instance SerialiseKey Word32 where
  serialiseKey x = RB.RawBytes $ byteVectorFromPrim $ byteSwapWord32 x

  deserialiseKey (RawBytes (VP.Vector off len ba)) =
    requireBytesExactly "Word32" 4 len $ byteSwapWord32 (indexWord8ArrayAsWord32 ba off)

instance SerialiseKeyOrderPreserving Word32

instance SerialiseValue Word32 where
  serialiseValue x = RB.RawBytes $ byteVectorFromPrim $ x

  deserialiseValue (RawBytes (VP.Vector off len ba)) =
    requireBytesExactly "Word32" 4 len $ indexWord8ArrayAsWord32 ba off

instance SerialiseKey Word64 where
  serialiseKey x = RB.RawBytes $ byteVectorFromPrim $ byteSwapWord64 x

  deserialiseKey (RawBytes (VP.Vector off len ba)) =
    requireBytesExactly "Word64" 8 len $ byteSwapWord64 (indexWord8ArrayAsWord64 ba off)

instance SerialiseKeyOrderPreserving Word64

instance SerialiseValue Word64 where
  serialiseValue x = RB.RawBytes $ byteVectorFromPrim $ x

  deserialiseValue (RawBytes (VP.Vector off len ba)) =
    requireBytesExactly "Word64" 8 len $ indexWord8ArrayAsWord64 ba off

instance SerialiseKey Word where
  serialiseKey x = RB.RawBytes $ byteVectorFromPrim $ byteSwapWord x

  deserialiseKey (RawBytes (VP.Vector off len ba)) =
    requireBytesExactly "Word" 8 len $ byteSwapWord (indexWord8ArrayAsWord ba off)

instance SerialiseKeyOrderPreserving Word

instance SerialiseValue Word where
  serialiseValue x = RB.RawBytes $ byteVectorFromPrim $ x

  deserialiseValue (RawBytes (VP.Vector off len ba)) =
    requireBytesExactly "Word" 8 len $ indexWord8ArrayAsWord ba off

{-------------------------------------------------------------------------------
  String
-------------------------------------------------------------------------------}

-- | \( O(n) \) (de-)serialisation, where \(n\) is the number of characters in
-- the string. The string is encoded using UTF8.
--
-- TODO: optimise, it's \( O(n) + O(n) \) where it could be \( O(n) \).
instance SerialiseKey String where
  serialiseKey = serialiseKey . UTF8.fromString
  deserialiseKey = UTF8.toString . deserialiseKey

instance SerialiseKeyOrderPreserving String

-- | \( O(n) \) (de-)serialisation, where \(n\) is the number of characters in
-- the string.
--
-- TODO: optimise, it's \( O(n) + O(n) \) where it could be \( O(n) \).
instance SerialiseValue String where
  serialiseValue = serialiseValue . UTF8.fromString
  deserialiseValue = UTF8.toString . deserialiseValue

{-------------------------------------------------------------------------------
  ByteString
-------------------------------------------------------------------------------}

-- | \( O(n) \) (de-)serialisation, where \(n\) is the number of bytes
--
-- TODO: optimise, it's \( O(n) + O(n) \) where it could be \( O(n) \).
instance SerialiseKey LBS.ByteString where
  serialiseKey = serialiseKey . LBS.toStrict
  deserialiseKey = B.toLazyByteString . RB.builder

instance SerialiseKeyOrderPreserving LBS.ByteString

-- | \( O(n) \) (de-)serialisation, where \(n\) is the number of bytes
--
-- TODO: optimise, it's \( O(n) + O(n) \) where it could be \( O(n) \).
instance SerialiseValue LBS.ByteString where
  serialiseValue = serialiseValue . LBS.toStrict
  deserialiseValue = B.toLazyByteString . RB.builder

-- | \( O(n) \) (de-)serialisation, where \(n\) is the number of bytes
instance SerialiseKey BS.ByteString where
  serialiseKey = serialiseKey . SBS.toShort
  deserialiseKey = SBS.fromShort . deserialiseKey

instance SerialiseKeyOrderPreserving BS.ByteString

-- | \( O(n) \) (de-)serialisation, where \(n\) is the number of bytes
instance SerialiseValue BS.ByteString where
  serialiseValue = serialiseValue . SBS.toShort
  deserialiseValue = SBS.fromShort . deserialiseValue

-- | \( O(1) \) serialisation, \( O(n) \) deserialisation
instance SerialiseKey SBS.ShortByteString where
  serialiseKey = RB.fromShortByteString
  deserialiseKey = byteArrayToSBS . RB.force

instance SerialiseKeyOrderPreserving SBS.ShortByteString

-- | \( O(1) \) serialisation, \( O(n) \) deserialisation
instance SerialiseValue SBS.ShortByteString where
  serialiseValue = RB.fromShortByteString
  deserialiseValue = byteArrayToSBS . RB.force

{-------------------------------------------------------------------------------
  ByteArray
-------------------------------------------------------------------------------}

-- | \( O(1) \) serialisation, \( O(n) \) deserialisation
instance SerialiseKey P.ByteArray where
  serialiseKey ba = RB.fromByteArray 0 (P.sizeofByteArray ba) ba
  deserialiseKey = RB.force

-- | \( O(1) \) serialisation, \( O(n) \) deserialisation
instance SerialiseValue P.ByteArray where
  serialiseValue ba = RB.fromByteArray 0 (P.sizeofByteArray ba) ba
  deserialiseValue = RB.force

{-------------------------------------------------------------------------------
  Void
-------------------------------------------------------------------------------}

-- | The 'deserialiseValue' of this instance throws. (as does e.g. 'Word64'
-- instance on invalid input.)
--
-- This instance is useful for tables without blobs.
instance SerialiseValue Void where
  serialiseValue = absurd
  deserialiseValue = error "deserialiseValue: Void can not be deserialised"

{-------------------------------------------------------------------------------
  Sum
-------------------------------------------------------------------------------}

-- | An instance for 'Sum' which is transparent to the serialisation of @a@.
--
-- Note: If you want to serialize @Sum a@ differently than @a@, then you should
-- create another @newtype@ over 'Sum' and define your alternative serialization.
instance SerialiseValue a => SerialiseValue (Sum a) where
  serialiseValue (Sum v) = serialiseValue v

  deserialiseValue = Sum . deserialiseValue
