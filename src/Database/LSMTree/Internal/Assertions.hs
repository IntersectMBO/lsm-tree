{-# LANGUAGE CPP       #-}
{-# LANGUAGE MagicHash #-}
{-# OPTIONS_HADDOCK not-home #-}

module Database.LSMTree.Internal.Assertions (
    assert,
    isValidSlice,
    sameByteArray,
    fromIntegralChecked,
) where

#if MIN_VERSION_base(4,17,0)
import           GHC.Exts (isTrue#, sameByteArray#)
#else
import           GHC.Exts (ByteArray#, MutableByteArray#, isTrue#,
                     sameMutableByteArray#, unsafeCoerce#)
#endif

import           Control.Exception (assert)
import           Data.Primitive.ByteArray (ByteArray (..), sizeofByteArray)
import           GHC.Stack (HasCallStack)
import           Text.Printf

isValidSlice :: Int -> Int -> ByteArray -> Bool
isValidSlice off len ba =
    off >= 0 &&
    len >= 0 &&
    (off + len) >= 0 && -- sum doesn't overflow
    (off + len) <= sizeofByteArray ba

sameByteArray :: ByteArray -> ByteArray -> Bool
sameByteArray (ByteArray ba1#) (ByteArray ba2#) =
#if MIN_VERSION_base(4,17,0)
    isTrue# (sameByteArray# ba1# ba2#)
#else
    isTrue# (sameMutableByteArray# (unsafeCoerceByteArray# ba1#)
                                   (unsafeCoerceByteArray# ba2#))
  where
    unsafeCoerceByteArray# :: ByteArray# -> MutableByteArray# s
    unsafeCoerceByteArray# = unsafeCoerce#
#endif

{-# INLINABLE fromIntegralChecked #-}
-- | Like 'fromIntegral', but throws an error when @(x :: a) /= fromIntegral
-- (fromIntegral x :: b)@.
fromIntegralChecked :: (HasCallStack, Integral a, Integral b, Show a) => a -> b
fromIntegralChecked x
  | x'' == x
  = x'
  | otherwise
  = error $ printf "fromIntegralChecked: conversion failed, %s /= %s" (show x) (show x'')
  where
    x' = fromIntegral x
    x'' = fromIntegral x'
