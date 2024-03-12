{-# LANGUAGE DeriveAnyClass             #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE DerivingStrategies         #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE NamedFieldPuns             #-}
{-# LANGUAGE StandaloneDeriving         #-}

{-# OPTIONS_GHC -Wno-orphans #-}

module Database.LSMTree.Util.Orphans () where

import           Control.DeepSeq (NFData (..))
import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as B
import qualified Data.ByteString.Lazy as LBS
import qualified Data.ByteString.Short.Internal as SBS
import qualified Data.Primitive as P
import qualified Data.Vector.Primitive as PV
import           Data.WideWord.Word256 (Word256 (..))
import           Data.Word (Word64, byteSwap64)
import           Database.LSMTree.Internal.Run.BloomFilter (Hashable (..))
import           Database.LSMTree.Internal.Run.Index.Compact (Append (..),
                     CompactIndex (..), PageNo (..), PageSpan (..),
                     SearchResult (..))
import           Database.LSMTree.Internal.Serialise (SerialisedBlob (..),
                     SerialisedKey (..), SerialisedValue (..))
import           Database.LSMTree.Internal.Serialise.Class
import qualified Database.LSMTree.Internal.Serialise.RawBytes as RB
import           GHC.Generics (Generic)
import           System.Random (Uniform)

deriving newtype instance NFData SerialisedKey
deriving newtype instance NFData SerialisedValue
deriving newtype instance NFData SerialisedBlob

deriving stock instance Generic SearchResult
deriving anyclass instance NFData SearchResult

deriving newtype instance NFData PageNo

deriving stock instance Generic PageSpan
deriving anyclass instance NFData PageSpan

deriving stock instance Generic Append
deriving anyclass instance NFData Append

deriving stock instance Generic CompactIndex
deriving anyclass instance NFData CompactIndex

{-------------------------------------------------------------------------------
  Word256
-------------------------------------------------------------------------------}

deriving anyclass instance Uniform Word256

instance Hashable Word256 where
  hashIO32 (Word256 a b c d) = hashIO32 (a, b, c, d)

instance SerialiseKey Word256 where
  serialiseKey (Word256{word256hi, word256m1, word256m0, word256lo}) =
    RB.RawBytes $ PV.Vector 0 32 $ P.runByteArray $ do
      ba <- P.newByteArray 32
      P.writeByteArray ba 0 $ byteSwap64 word256hi
      P.writeByteArray ba 1 $ byteSwap64 word256m1
      P.writeByteArray ba 2 $ byteSwap64 word256m0
      P.writeByteArray ba 3 $ byteSwap64 word256lo
      return ba

  deserialiseKey = error "deserialiseKey: Word256" -- TODO

{-------------------------------------------------------------------------------
  Word64
-------------------------------------------------------------------------------}

instance SerialiseKey Word64 where
  serialiseKey x =
    RB.RawBytes $ PV.Vector 0 8 $ P.runByteArray $ do
      ba <- P.newByteArray 8
      P.writeByteArray ba 0 $ byteSwap64 x
      return ba

  deserialiseKey = error "deserialiseKey: Word64" -- TODO

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
