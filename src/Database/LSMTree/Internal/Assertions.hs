{-# LANGUAGE CPP       #-}
{-# LANGUAGE MagicHash #-}
module Database.LSMTree.Internal.Assertions (
    assert,
    isValidSlice,
    sameByteArray,
    assertNoThunks,
) where

import           Control.Exception (assert)
import           Data.Primitive.ByteArray (ByteArray (..), sizeofByteArray)
#if MIN_VERSION_base(4,17,0)
import           GHC.Exts (isTrue#, sameByteArray#)
#else
import           GHC.Exts (ByteArray#, MutableByteArray#, isTrue#,
                     sameMutableByteArray#, unsafeCoerce#)
#endif
import           NoThunks.Class (NoThunks, unsafeNoThunks)

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

assertNoThunks :: NoThunks a => a -> b -> b
assertNoThunks x = assert p
  where p = case unsafeNoThunks x of
              Nothing -> True
              Just thunkInfo -> error $ "Assertion failed: found thunk" <> show thunkInfo
