{-# LANGUAGE CPP       #-}
{-# LANGUAGE MagicHash #-}
module Database.LSMTree.Internal.Assertions (
    assert,
    assertWith,
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

import           Control.Exception
import           Data.Primitive.ByteArray (ByteArray (..), sizeofByteArray)
import           GHC.Stack (HasCallStack)
import           Text.Printf
import           System.IO.Unsafe (unsafePerformIO) -- for 'assertWith'

-- | Like 'assert' but appends a customized message to the assertion exception's
-- location  information. Intended to be used to add salient contextual information
-- regarding the assertion failure.
assertWith :: String -> Bool -> a -> a
assertWith msg cond v = unsafePerformIO . catch (evaluate (assert cond v)) $
    \x -> throwIO (addMessage x)
  where
    addMessage (AssertionFailed locInfo) = AssertionFailed $ locInfo <> "\nAttached Message:\n" <> msg

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
