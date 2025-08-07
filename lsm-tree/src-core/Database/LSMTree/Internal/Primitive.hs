{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE CPP          #-}
{-# LANGUAGE MagicHash    #-}
{-# OPTIONS_HADDOCK not-home #-}

-- | Primitive operations lifted into boxed types
module Database.LSMTree.Internal.Primitive (
    -- * Byte swaps
    byteSwapInt16
  , byteSwapInt32
  , byteSwapInt64
  , byteSwapInt
  , byteSwapWord16
  , byteSwapWord32
  , byteSwapWord64
  , byteSwapWord
    -- * Indexing byte arrays
  , indexInt8Array
  , indexWord8ArrayAsInt16
  , indexWord8ArrayAsInt32
  , indexWord8ArrayAsInt64
  , indexWord8ArrayAsInt
  , indexWord8Array
  , indexWord8ArrayAsWord16
  , indexWord8ArrayAsWord32
  , indexWord8ArrayAsWord64
  , indexWord8ArrayAsWord
  ) where

import           Data.Primitive.ByteArray (ByteArray (..))
import           GHC.Exts
import           GHC.Int
import           GHC.Word

{-------------------------------------------------------------------------------
  Conversions
-------------------------------------------------------------------------------}

{-# INLINE wordToInt16# #-}
wordToInt16# :: Word# -> Int16#
wordToInt16# w# = intToInt16# (word2Int# w#)

{-# INLINE int16ToWord# #-}
int16ToWord# :: Int16# -> Word#
int16ToWord# i# = int2Word# (int16ToInt# i#)

{-# INLINE wordToInt32# #-}
wordToInt32# :: Word# -> Int32#
wordToInt32# w# = intToInt32# (word2Int# w#)

{-# INLINE int32ToWord# #-}
int32ToWord# :: Int32# -> Word#
int32ToWord# i# = int2Word# (int32ToInt# i#)

#if MIN_VERSION_base(4,17,0)

{-# INLINE wordToInt64# #-}
wordToInt64# :: Word# -> Int64#
wordToInt64# w# = intToInt64# (word2Int# w#)

{-# INLINE int64ToWord# #-}
int64ToWord# :: Int64# -> Word#
int64ToWord# i# = int2Word# (int64ToInt# i#)

#endif

{-------------------------------------------------------------------------------
  Conversions: shims
-------------------------------------------------------------------------------}

{-# INLINE wordToInt64Shim# #-}
{-# INLINE int64ToWordShim# #-}

#if MIN_VERSION_base(4,17,0)

wordToInt64Shim# :: Word# -> Int64#
wordToInt64Shim# = wordToInt64#

int64ToWordShim# :: Int64# -> Word#
int64ToWordShim# = int64ToWord#

#else

wordToInt64Shim# :: Word# -> Int#
wordToInt64Shim# = word2Int#

int64ToWordShim# :: Int# -> Word#
int64ToWordShim# = int2Word#

#endif

{-------------------------------------------------------------------------------
  Byte swaps
-------------------------------------------------------------------------------}

{-# INLINE byteSwapInt16 #-}
byteSwapInt16 :: Int16 -> Int16
byteSwapInt16 (I16# i#) = I16# (wordToInt16# (byteSwap16# (int16ToWord# i#)))

{-# INLINE byteSwapInt32 #-}
byteSwapInt32 :: Int32 -> Int32
byteSwapInt32 (I32# i#) = I32# (wordToInt32# (byteSwap32# (int32ToWord# i#)))

{-# INLINE byteSwapInt64 #-}
byteSwapInt64 :: Int64 -> Int64
byteSwapInt64 (I64# i#) = I64# (wordToInt64Shim# (byteSwap# (int64ToWordShim# i#)))

{-# INLINE byteSwapInt #-}
byteSwapInt :: Int -> Int
byteSwapInt (I# i#) = I# (word2Int# (byteSwap# (int2Word# i#)))

{-# INLINE byteSwapWord16 #-}
byteSwapWord16 :: Word16 -> Word16
byteSwapWord16 = byteSwap16

{-# INLINE byteSwapWord32 #-}
byteSwapWord32 :: Word32 -> Word32
byteSwapWord32 = byteSwap32

{-# INLINE byteSwapWord64 #-}
byteSwapWord64 :: Word64 -> Word64
byteSwapWord64 = byteSwap64

{-# INLINE byteSwapWord #-}
byteSwapWord :: Word -> Word
byteSwapWord (W# w#) = W# (byteSwap# w#)

{-------------------------------------------------------------------------------
  Indexing byte arrays
-------------------------------------------------------------------------------}

{-# INLINE indexInt8Array #-}
indexInt8Array :: ByteArray -> Int -> Int8
indexInt8Array (ByteArray !ba#) (I# !off#) =
  I8# (indexInt8Array# ba# off#)

{-# INLINE indexWord8ArrayAsInt16 #-}
indexWord8ArrayAsInt16 :: ByteArray -> Int -> Int16
indexWord8ArrayAsInt16 (ByteArray !ba#) (I# !off#) =
  I16# (indexWord8ArrayAsInt16# ba# off#)

{-# INLINE indexWord8ArrayAsInt32 #-}
indexWord8ArrayAsInt32 :: ByteArray -> Int -> Int32
indexWord8ArrayAsInt32 (ByteArray !ba#) (I# !off#) =
  I32# (indexWord8ArrayAsInt32# ba# off#)

{-# INLINE indexWord8ArrayAsInt64 #-}
indexWord8ArrayAsInt64 :: ByteArray -> Int -> Int64
indexWord8ArrayAsInt64 (ByteArray !ba#) (I# !off#) =
  I64# (indexWord8ArrayAsInt64# ba# off#)

{-# INLINE indexWord8ArrayAsInt #-}
indexWord8ArrayAsInt :: ByteArray -> Int -> Int
indexWord8ArrayAsInt (ByteArray !ba#) (I# !off#) =
  I# (indexWord8ArrayAsInt# ba# off#)

{-# INLINE indexWord8Array #-}
indexWord8Array :: ByteArray -> Int -> Word8
indexWord8Array (ByteArray !ba#) (I# !off#) =
  W8# (indexWord8Array# ba# off#)

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

{-# INLINE indexWord8ArrayAsWord #-}
indexWord8ArrayAsWord :: ByteArray -> Int -> Word
indexWord8ArrayAsWord (ByteArray !ba#) (I# !off#) =
  W# (indexWord8ArrayAsWord# ba# off#)
