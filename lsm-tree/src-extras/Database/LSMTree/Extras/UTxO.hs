{-# LANGUAGE DeriveAnyClass             #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE DerivingStrategies         #-}
{-# LANGUAGE DerivingVia                #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE NamedFieldPuns             #-}
{-# LANGUAGE TypeFamilies               #-}

module Database.LSMTree.Extras.UTxO (
    UTxOKey (..)
  , UTxOValue (..)
  , zeroUTxOValue
  , UTxOBlob (..)
  ) where

import           Control.DeepSeq
import           Data.Bits
import qualified Data.ByteString as BS
import qualified Data.Primitive as P
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Generic.Mutable as VGM
import qualified Data.Vector.Primitive as VP
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
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
import           Test.QuickCheck hiding ((.&.))

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

-- TODO: reduce number of allocations, optimise (by using unsafe functions)
instance SerialiseKey UTxOKey where
  serialiseKey UTxOKey{txId, txIx} =
    RB.RawBytes $ mkPrimVector 0 34 $ P.runByteArray $ do
      ba <- P.newByteArray 34
      let !cut = fromIntegral ((txId .&. (fromIntegral (0xffff :: Word16) `unsafeShiftL` 192)) `unsafeShiftR` 192)
      P.writeByteArray ba 0  $ byteSwapWord256 txId
      P.writeByteArray ba 3  $ byteSwap16 txIx
      P.writeByteArray ba 16 $ byteSwap16 cut
      pure ba
  deserialiseKey (RawBytes (VP.Vector off len ba)) =
    requireBytesExactly "UTxOKey" 34 len $
      let !cut   = byteSwap16      $ indexWord8ArrayAsWord16  ba (off + 32)
          !txIx  = byteSwap16      $ indexWord8ArrayAsWord16  ba (off + 6)
          !txId_ = byteSwapWord256 $ indexWord8ArrayAsWord256 ba off
          !txId  = (txId_ .&. (complement (fromIntegral (0xffff :: Word16) `unsafeShiftL` 192)))
                      .|. (fromIntegral cut `unsafeShiftL` 192)
      in UTxOKey{txId, txIx}

instance Arbitrary UTxOKey where
  arbitrary = UTxOKey <$> arbitrary <*> arbitrary
  shrink (UTxOKey a b) = [ UTxOKey a' b' | (a', b') <- shrink (a, b) ]

newtype instance VUM.MVector s UTxOKey = MV_UTxOKey (VU.MVector s (Word256, Word16))
newtype instance VU.Vector     UTxOKey = V_UTxOKey  (VU.Vector    (Word256, Word16))

instance VU.IsoUnbox UTxOKey (Word256, Word16) where
  toURepr (UTxOKey a b) = (a, b)
  fromURepr (a, b) = UTxOKey a b
  {-# INLINE toURepr #-}
  {-# INLINE fromURepr #-}

deriving via VU.As UTxOKey (Word256, Word16) instance VGM.MVector VU.MVector UTxOKey
deriving via VU.As UTxOKey (Word256, Word16) instance VG.Vector   VU.Vector  UTxOKey

instance VUM.Unbox UTxOKey

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
      pure ba
  deserialiseValue (RawBytes (VP.Vector off len ba)) =
    requireBytesExactly "UTxOValue" 60 len $
      UTxOValue (indexWord8ArrayAsWord256 ba off)
                (indexWord8ArrayAsWord128 ba (off + 32))
                (indexWord8ArrayAsWord64  ba (off + 48))
                (indexWord8ArrayAsWord32  ba (off + 56))

instance Arbitrary UTxOValue where
  arbitrary = UTxOValue <$> arbitrary <*> arbitrary <*> arbitrary <*> arbitrary
  shrink (UTxOValue a b c d) = [ UTxOValue a' b' c' d'
                               | (a', b', c', d') <- shrink (a, b, c, d) ]

newtype instance VUM.MVector s UTxOValue = MV_UTxOValue (VU.MVector s (Word256, Word128, Word64, Word32))
newtype instance VU.Vector     UTxOValue = V_UTxOValue  (VU.Vector    (Word256, Word128, Word64, Word32))

instance VU.IsoUnbox UTxOValue (Word256, Word128, Word64, Word32) where
  toURepr (UTxOValue a b c d) = (a, b, c, d)
  fromURepr (a, b, c, d) = UTxOValue a b c d
  {-# INLINE toURepr #-}
  {-# INLINE fromURepr #-}

deriving via VU.As UTxOValue (Word256, Word128, Word64, Word32) instance VGM.MVector VU.MVector UTxOValue
deriving via VU.As UTxOValue (Word256, Word128, Word64, Word32) instance VG.Vector   VU.Vector  UTxOValue

instance VUM.Unbox UTxOValue

zeroUTxOValue :: UTxOValue
zeroUTxOValue = UTxOValue 0 0 0 0

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
