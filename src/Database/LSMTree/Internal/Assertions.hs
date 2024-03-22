{-# LANGUAGE MagicHash #-}
module Database.LSMTree.Internal.Assertions (
    assert,
    isValidSlice,
    sameByteArray,
) where

import           Control.Exception (assert)
import           Data.Primitive.ByteArray (ByteArray (..), sizeofByteArray)
import           GHC.Exts (isTrue#, sameByteArray#)

isValidSlice :: Int -> Int -> ByteArray -> Bool
isValidSlice off len ba =
    off >= 0 &&
    len >= 0 &&
    (off + len) >= 0 && -- sum doesn't overflow
    (off + len) <= sizeofByteArray ba

sameByteArray :: ByteArray -> ByteArray -> Bool
sameByteArray (ByteArray ba1) (ByteArray ba2) =
    isTrue# (sameByteArray# ba1 ba2)
