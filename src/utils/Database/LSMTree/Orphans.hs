{-# LANGUAGE DeriveAnyClass             #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE DerivingStrategies         #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE NamedFieldPuns             #-}
{-# LANGUAGE StandaloneDeriving         #-}

{-# OPTIONS_GHC -Wno-orphans #-}

module Database.LSMTree.Orphans () where

import           Control.DeepSeq (NFData (..))
import qualified Data.Primitive as P
import           Data.WideWord.Word256 (Word256 (..))
import           Data.Word (byteSwap64)
import           Database.LSMTree.Internal.Entry (NumEntries (..))
import           Database.LSMTree.Internal.IndexCompact (IndexCompact (..),
                     PageNo (..), PageSpan (..))
import           Database.LSMTree.Internal.IndexCompactAcc (Append (..))
import qualified Database.LSMTree.Internal.RawBytes as RB
import           Database.LSMTree.Internal.Serialise (SerialisedBlob (..),
                     SerialisedKey (..), SerialisedValue (..))
import           Database.LSMTree.Internal.Serialise.Class
import           Database.LSMTree.Internal.Unsliced (Unsliced)
import           Database.LSMTree.Internal.Vector
import           GHC.Generics (Generic)
import           System.Random (Uniform)

deriving newtype instance NFData SerialisedKey
deriving newtype instance NFData SerialisedValue
deriving newtype instance NFData SerialisedBlob
deriving newtype instance NFData NumEntries

deriving newtype instance NFData PageNo

deriving stock instance Generic PageSpan
deriving anyclass instance NFData PageSpan

deriving stock instance Generic Append
deriving anyclass instance NFData Append

deriving stock instance Generic IndexCompact
deriving anyclass instance NFData IndexCompact

instance NFData (Unsliced a) where
  rnf x = x `seq` ()

{-------------------------------------------------------------------------------
  Word256
-------------------------------------------------------------------------------}

deriving anyclass instance Uniform Word256

instance SerialiseKey Word256 where
  serialiseKey (Word256{word256hi, word256m1, word256m0, word256lo}) =
    RB.RawBytes $ mkPrimVector 0 32 $ P.runByteArray $ do
      ba <- P.newByteArray 32
      P.writeByteArray ba 0 $ byteSwap64 word256hi
      P.writeByteArray ba 1 $ byteSwap64 word256m1
      P.writeByteArray ba 2 $ byteSwap64 word256m0
      P.writeByteArray ba 3 $ byteSwap64 word256lo
      return ba

  deserialiseKey = error "deserialiseKey: Word256" -- TODO
