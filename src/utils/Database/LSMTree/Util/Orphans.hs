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
import           Data.WideWord.Word256 (Word256 (..))
import           Data.Word (Word64)
import           Database.LSMTree.Internal.Run.BloomFilter (Hashable (..))
import           Database.LSMTree.Internal.Run.Index.Compact (Append (..),
                     CompactIndex (..), PageNr (..), PageSpan (..),
                     SearchResult (..))
import           Database.LSMTree.Internal.Serialise (SerialisedBlob (..),
                     SerialisedKey (..), SerialisedValue (..))
import           Database.LSMTree.Internal.Serialise.Class
import           Database.LSMTree.Internal.Serialise.RawBytes
import           GHC.Generics (Generic)
import           System.Random (Uniform)

deriving newtype instance NFData SerialisedKey
deriving newtype instance NFData SerialisedValue
deriving newtype instance NFData SerialisedBlob

deriving stock instance Generic SearchResult
deriving anyclass instance NFData SearchResult

deriving newtype instance NFData PageNr

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

-- | Placeholder instance, not optimised
instance SerialiseKey Word256 where
  serialiseKey (Word256{word256hi, word256m1, word256m0, word256lo}) =
      serialiseKey $ B.toLazyByteString $ mconcat [
          B.word64BE word256hi
        , B.word64BE word256m1
        , B.word64BE word256m0
        , B.word64BE word256lo
        ]
  deserialiseKey = error "deserialiseKey: Word256" -- TODO

{-------------------------------------------------------------------------------
  Word64
-------------------------------------------------------------------------------}

-- | Placeholder instance, not optimised
instance SerialiseKey Word64 where
  serialiseKey x = serialiseKey $ B.toLazyByteString $ B.word64BE x
  deserialiseKey = error "deserialiseKey: Word64" -- TODO

{-------------------------------------------------------------------------------
  ByteString
-------------------------------------------------------------------------------}

-- | Placeholder instance, not optimised
instance SerialiseKey LBS.ByteString where
  serialiseKey = serialiseKey . LBS.toStrict
  deserialiseKey = B.toLazyByteString . rawBytes

-- | Placeholder instance, not optimised
instance SerialiseKey BS.ByteString where
  serialiseKey = fromShortByteString . SBS.toShort
  deserialiseKey = LBS.toStrict . deserialiseKey

-- | Placeholder instance, not optimised
instance SerialiseValue LBS.ByteString where
  serialiseValue = serialiseValue . LBS.toStrict
  deserialiseValue = deserialiseValueN . pure
  deserialiseValueN = B.toLazyByteString . foldMap rawBytes

-- | Placeholder instance, not optimised
instance SerialiseValue BS.ByteString where
  serialiseValue = fromShortByteString . SBS.toShort
  deserialiseValue = deserialiseValueN . pure
  deserialiseValueN = LBS.toStrict . deserialiseValueN
