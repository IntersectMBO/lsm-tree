module Database.LSMTree.Internal.Assertions (
    assert,
    isValidSlice,
) where

import           Control.Exception (assert)
import           Data.Primitive.ByteArray (ByteArray, sizeofByteArray)

isValidSlice :: Int -> Int -> ByteArray -> Bool
isValidSlice off len ba =
    off >= 0 &&
    len >= 0 &&
    (off + len) >= 0 && -- sum doesn't overflow
    (off + len) <= sizeofByteArray ba
