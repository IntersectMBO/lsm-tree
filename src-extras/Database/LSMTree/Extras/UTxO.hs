{-# LANGUAGE DeriveAnyClass             #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE DerivingStrategies         #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE NamedFieldPuns             #-}
{- HLINT ignore "Redundant <$>" -}

module Database.LSMTree.Extras.UTxO (
    UTxOKey (..)
  , UTxOValue (..)
  , UTxOBlob (..)
  ) where

import           Control.DeepSeq
import qualified Data.ByteString as BS
import qualified Data.Primitive as P
import qualified Data.Vector.Primitive as PV
import           Data.WideWord.Word128
import           Data.WideWord.Word256
import           Data.Word
import           Database.LSMTree.Extras.Orphans
import           Database.LSMTree.Internal.Primitive
import qualified Database.LSMTree.Internal.RawBytes as RB
import           Database.LSMTree.Internal.Serialise ()
import           Database.LSMTree.Internal.Serialise.Class as Class
import           Database.LSMTree.Internal.Vector
import           GHC.Generics
import           System.Random
import           Test.QuickCheck

{-------------------------------------------------------------------------------
  UTxO keys
-------------------------------------------------------------------------------}

-- | A model of a UTxO key (34 bytes) after @TxIn@: a 256-bit hash, 16-bit identifier
data UTxOKey = UTxOKey {
    txId :: !Word256 -- no unpack, since the @TxId@ field doesn't have it
  , txIx :: {-# UNPACK #-} !Word16
  }
  deriving stock (Show, Eq, Ord, Generic)
  deriving anyclass (Uniform, NFData)

instance SerialiseKey UTxOKey where
  serialiseKey UTxOKey{txId, txIx} =
    RB.RawBytes $ mkPrimVector 0 34 $ P.runByteArray $ do
      ba <- P.newByteArray 34
      P.writeByteArray ba 0 $ byteSwapWord256 txId
      P.writeByteArray ba 16 $ byteSwap16 txIx
      return ba
  deserialiseKey (RawBytes (PV.Vector off len ba)) =
    requireBytesExactly "UTxOKey" 34 len $
      UTxOKey (byteSwapWord256 $ indexWord8ArrayAsWord256 ba off)
              (byteSwap16      $ indexWord8ArrayAsWord16  ba (off + 32))

instance Arbitrary UTxOKey where
  arbitrary = UTxOKey <$> arbitrary <*> arbitrary
  shrink (UTxOKey a b) = [ UTxOKey a' b' | (a', b') <- shrink (a, b) ]

{-------------------------------------------------------------------------------
  UTxO values
-------------------------------------------------------------------------------}

-- | A model of a UTxO value (60 bytes)
data UTxOValue = UTxOValue {
    utxoValue256 :: !Word256
  , utxoValue128 :: !Word128
  , utxoValue64  :: !Word64
  , utxoValue32  :: !Word32
  }
  deriving stock (Show, Eq, Ord, Generic)
  deriving anyclass (Uniform, NFData)

instance SerialiseValue UTxOValue where
  serialiseValue (UTxOValue {utxoValue256, utxoValue128, utxoValue64, utxoValue32}) =
    RB.RawBytes $ mkPrimVector 0 60 $ P.runByteArray $ do
      ba <- P.newByteArray 60
      P.writeByteArray ba 0 utxoValue256
      P.writeByteArray ba 2 utxoValue128
      P.writeByteArray ba 6 utxoValue64
      P.writeByteArray ba 14 utxoValue32
      return ba
  deserialiseValue (RawBytes (PV.Vector off len ba)) =
    requireBytesExactly "UTxOValue" 60 len $
      UTxOValue (indexWord8ArrayAsWord256 ba off)
                (indexWord8ArrayAsWord128 ba (off + 32))
                (indexWord8ArrayAsWord64  ba (off + 48))
                (indexWord8ArrayAsWord32  ba (off + 56))
  deserialiseValueN = deserialiseValue . mconcat -- TODO: optimise

instance Arbitrary UTxOValue where
  arbitrary = UTxOValue <$> arbitrary <*> arbitrary <*> arbitrary <*> arbitrary
  shrink (UTxOValue a b c d) = [ UTxOValue a' b' c' d'
                               | (a', b', c', d') <- shrink (a, b, c, d) ]

{-------------------------------------------------------------------------------
  UTxO blobs
-------------------------------------------------------------------------------}

-- | A blob of arbitrary size
newtype UTxOBlob = UTxOBlob BS.ByteString
  deriving stock (Show, Eq, Ord, Generic)
  deriving anyclass NFData

instance SerialiseValue UTxOBlob where
  serialiseValue (UTxOBlob bs) = Class.serialiseValue bs
  deserialiseValue = error "deserialiseValue: UTxOBlob"
  deserialiseValueN = error "deserialiseValueN: UTxOBlob"
