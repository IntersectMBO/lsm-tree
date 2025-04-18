{-# LANGUAGE DerivingStrategies         #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE PatternSynonyms            #-}
{-# OPTIONS_HADDOCK not-home #-}

-- | Newtype wrappers and utilities for serialised keys, values and blobs.
--
module Database.LSMTree.Internal.Serialise (
    -- * Re-exports
    SerialiseKey
  , SerialiseValue
    -- * Keys
  , SerialisedKey (SerialisedKey, SerialisedKey')
  , serialiseKey
  , deserialiseKey
  , sizeofKey
  , sizeofKey16
  , sizeofKey32
  , sizeofKey64
  , serialisedKey
  , keyTopBits64
    -- * Values
  , SerialisedValue (SerialisedValue, SerialisedValue')
  , serialiseValue
  , deserialiseValue
  , sizeofValue
  , sizeofValue16
  , sizeofValue32
  , sizeofValue64
  , serialisedValue
  , ResolveSerialisedValue
    -- * Blobs
  , SerialisedBlob (SerialisedBlob, SerialisedBlob')
  , serialiseBlob
  , deserialiseBlob
  , sizeofBlob
  , sizeofBlob64
  , serialisedBlob
  ) where

import           Control.DeepSeq (NFData)
import           Data.BloomFilter.Hash (Hashable (..))
import qualified Data.ByteString.Builder as BB
import qualified Data.Vector.Primitive as VP
import           Data.Word
import           Database.LSMTree.Internal.RawBytes (RawBytes (..))
import qualified Database.LSMTree.Internal.RawBytes as RB
import           Database.LSMTree.Internal.Serialise.Class (SerialiseKey,
                     SerialiseValue)
import qualified Database.LSMTree.Internal.Serialise.Class as Class

{-------------------------------------------------------------------------------
  Keys
-------------------------------------------------------------------------------}

-- | Representation of a serialised key.
--
-- Serialisation should preserve equality and ordering. The 'Ord' instance for
-- 'SerialisedKey' uses lexicographical ordering.
newtype SerialisedKey = SerialisedKey RawBytes
  deriving stock Show
  deriving newtype (Eq, Ord, Hashable, NFData)

{-# COMPLETE SerialisedKey' #-}
pattern SerialisedKey' :: VP.Vector Word8 -> SerialisedKey
pattern SerialisedKey' pvec = SerialisedKey (RawBytes pvec)

{-# INLINE serialiseKey #-}
serialiseKey :: SerialiseKey k => k -> SerialisedKey
serialiseKey k = SerialisedKey (Class.serialiseKey k)

{-# INLINE deserialiseKey #-}
deserialiseKey :: SerialiseKey k => SerialisedKey -> k
deserialiseKey (SerialisedKey bytes) = Class.deserialiseKey bytes

{-# INLINE sizeofKey #-}
-- | Size of key in number of bytes.
sizeofKey :: SerialisedKey -> Int
sizeofKey (SerialisedKey rb) = RB.size rb

{-# INLINE sizeofKey16 #-}
-- | Size of key in number of bytes.
sizeofKey16 :: SerialisedKey -> Word16
sizeofKey16 = fromIntegral . sizeofKey

{-# INLINE sizeofKey32 #-}
-- | Size of key in number of bytes.
sizeofKey32 :: SerialisedKey -> Word32
sizeofKey32 = fromIntegral . sizeofKey

{-# INLINE sizeofKey64 #-}
-- | Size of key in number of bytes.
sizeofKey64 :: SerialisedKey -> Word64
sizeofKey64 = fromIntegral . sizeofKey

{-# INLINE serialisedKey #-}
serialisedKey :: SerialisedKey -> BB.Builder
serialisedKey (SerialisedKey rb) = RB.builder rb

{-# INLINE keyTopBits64 #-}
-- | See 'RB.topBits64'
keyTopBits64 :: SerialisedKey -> Word64
keyTopBits64 (SerialisedKey rb) = RB.topBits64 rb

{-------------------------------------------------------------------------------
  Values
-------------------------------------------------------------------------------}

-- | Representation of a serialised value.
newtype SerialisedValue = SerialisedValue RawBytes
  deriving stock Show
  deriving newtype (Eq, Ord, NFData)

{-# COMPLETE SerialisedValue' #-}
pattern SerialisedValue' :: VP.Vector Word8 -> SerialisedValue
pattern SerialisedValue' pvec = (SerialisedValue (RawBytes pvec))

{-# INLINE serialiseValue #-}
serialiseValue :: SerialiseValue v => v -> SerialisedValue
serialiseValue v = SerialisedValue (Class.serialiseValue v)

{-# INLINE deserialiseValue #-}
deserialiseValue :: SerialiseValue v => SerialisedValue -> v
deserialiseValue (SerialisedValue bytes) = Class.deserialiseValue bytes

{-# INLINE sizeofValue #-}
sizeofValue :: SerialisedValue -> Int
sizeofValue (SerialisedValue rb) = RB.size rb

{-# INLINE sizeofValue16 #-}
-- | Size of value in number of bytes.
sizeofValue16 :: SerialisedValue -> Word16
sizeofValue16 = fromIntegral . sizeofValue

{-# INLINE sizeofValue32 #-}
-- | Size of value in number of bytes.
sizeofValue32 :: SerialisedValue -> Word32
sizeofValue32 = fromIntegral . sizeofValue

{-# INLINE sizeofValue64 #-}
-- | Size of value in number of bytes.
sizeofValue64 :: SerialisedValue -> Word64
sizeofValue64 = fromIntegral . sizeofValue

{-# LANGUAGE serialisedValue #-}
serialisedValue :: SerialisedValue -> BB.Builder
serialisedValue (SerialisedValue rb) = RB.builder rb

type ResolveSerialisedValue =
  SerialisedValue -> SerialisedValue -> SerialisedValue

{-------------------------------------------------------------------------------
  Blobs
-------------------------------------------------------------------------------}

-- | Representation of a serialised blob.
newtype SerialisedBlob = SerialisedBlob RawBytes
  deriving stock Show
  deriving newtype (Eq, Ord, NFData)

{-# COMPLETE SerialisedBlob' #-}
pattern SerialisedBlob' :: VP.Vector Word8 -> SerialisedBlob
pattern SerialisedBlob' pvec = (SerialisedBlob (RawBytes pvec))

{-# INLINE serialiseBlob #-}
serialiseBlob :: SerialiseValue v => v -> SerialisedBlob
serialiseBlob v = SerialisedBlob (Class.serialiseValue v)

{-# INLINE deserialiseBlob #-}
deserialiseBlob :: SerialiseValue v => SerialisedBlob -> v
deserialiseBlob (SerialisedBlob bytes) = Class.deserialiseValue bytes

{-# INLINE sizeofBlob #-}
-- | Size of blob in number of bytes.
sizeofBlob :: SerialisedBlob -> Int
sizeofBlob (SerialisedBlob rb) = RB.size rb

{-# INLINE sizeofBlob64 #-}
sizeofBlob64 :: SerialisedBlob -> Word64
sizeofBlob64 = fromIntegral . sizeofBlob

{-# INLINE serialisedBlob #-}
serialisedBlob :: SerialisedBlob -> BB.Builder
serialisedBlob (SerialisedBlob rb) = RB.builder rb
