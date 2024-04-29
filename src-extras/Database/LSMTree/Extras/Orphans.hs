{-# LANGUAGE DeriveAnyClass     #-}
{-# LANGUAGE DerivingStrategies #-}

{-# OPTIONS_GHC -Wno-orphans #-}

module Database.LSMTree.Extras.Orphans () where

import           Control.DeepSeq
import qualified Data.Primitive as P
import           Data.WideWord.Word256 (Word256 (..))
import           Data.Word (byteSwap64)
import qualified Database.LSMTree.Internal.RawBytes as RB
import           Database.LSMTree.Internal.Serialise.Class
import           Database.LSMTree.Internal.Vector
import           GHC.Generics (Generic)
import qualified System.FS.API as FS
import qualified System.FS.IO.Internal.Handle as FS
import           System.Posix.Types (COff (..))
import           System.Random (Uniform)

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

{-------------------------------------------------------------------------------
  NFData
-------------------------------------------------------------------------------}

deriving stock instance Generic (FS.HandleOS h)
deriving anyclass instance NFData (FS.HandleOS h)
deriving newtype instance NFData FS.BufferOffset
deriving newtype instance NFData COff
deriving anyclass instance NFData FS.FsPath
deriving instance NFData h => NFData (FS.Handle h)
instance NFData (FS.HasFS m h) where
  rnf x = x `seq` ()
