{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE MagicHash    #-}
{-# OPTIONS_HADDOCK not-home #-}

module Database.LSMTree.Internal.Primitive (
    byteSwapInt
  , indexWord8ArrayAsInt
  , indexWord8ArrayAsWord16
  , indexWord8ArrayAsWord32
  , indexWord8ArrayAsWord64
  ) where

import           Data.Primitive.ByteArray (ByteArray (..))
import           GHC.Exts
import           GHC.Word

{-# INLINE byteSwapInt #-}
byteSwapInt :: Int -> Int
byteSwapInt (I# i#) = I# (word2Int# (byteSwap# (int2Word# i#)))

{-# INLINE indexWord8ArrayAsInt #-}
indexWord8ArrayAsInt :: ByteArray -> Int -> Int
indexWord8ArrayAsInt (ByteArray !ba#) (I# !off#) =
  I# (indexWord8ArrayAsInt# ba# off#)

{-# INLINE indexWord8ArrayAsWord16 #-}
indexWord8ArrayAsWord16 :: ByteArray -> Int -> Word16
indexWord8ArrayAsWord16 (ByteArray !ba#) (I# !off#) =
  W16# (indexWord8ArrayAsWord16# ba# off#)

{-# INLINE indexWord8ArrayAsWord32 #-}
indexWord8ArrayAsWord32 :: ByteArray -> Int -> Word32
indexWord8ArrayAsWord32 (ByteArray !ba#) (I# !off#) =
  W32# (indexWord8ArrayAsWord32# ba# off#)

{-# INLINE indexWord8ArrayAsWord64 #-}
indexWord8ArrayAsWord64 :: ByteArray -> Int -> Word64
indexWord8ArrayAsWord64 (ByteArray !ba#) (I# !off#) =
  W64# (indexWord8ArrayAsWord64# ba# off#)
