{-# LANGUAGE CPP       #-}
{-# LANGUAGE MagicHash #-}
module Database.LSMTree.Assertions (
    assert,
    isValidSlice,
    sameByteArray,
) where

import           Control.Exception (assert)
import           Data.Primitive.ByteArray (ByteArray (..), sizeofByteArray)
#if MIN_VERSION_base(4,17,0)
import           GHC.Exts (isTrue#, sameByteArray#)
#else
import           GHC.Exts (ByteArray#, MutableByteArray#, isTrue#,
                     sameMutableByteArray#, unsafeCoerce#)
#endif

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
