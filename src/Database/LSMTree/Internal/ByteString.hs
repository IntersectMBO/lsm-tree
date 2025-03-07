{-# LANGUAGE CPP           #-}
{-# LANGUAGE MagicHash     #-}
{-# LANGUAGE UnboxedTuples #-}
{-# OPTIONS_HADDOCK not-home #-}

-- | @bytestring@ extras
--
module Database.LSMTree.Internal.ByteString (
    tryCheapToShort,
    tryGetByteArray,
    shortByteStringFromTo,
    byteArrayFromTo,
    byteArrayToByteString,
    unsafePinnedByteArrayToByteString,
    byteArrayToSBS,
) where

import           Control.Exception (assert)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as BB
import qualified Data.ByteString.Builder.Internal as BB
import qualified Data.ByteString.Internal as BS.Internal
import           Data.ByteString.Short (ShortByteString (SBS))
import qualified Data.ByteString.Short.Internal as SBS
import           Data.Primitive.ByteArray
import           Database.LSMTree.Internal.Assertions (isValidSlice)
import           Foreign.Ptr (minusPtr, plusPtr)
import           GHC.Exts
import qualified GHC.ForeignPtr as Foreign
import           GHC.Stack (HasCallStack)

-- | \( O(1) \) conversion, if possible.
--
-- In addition to the conditions explained for 'tryGetByteArray', the
-- bytestring must use the full length of the underlying byte array.
tryCheapToShort :: BS.ByteString -> Either String ShortByteString
tryCheapToShort bs =
    tryGetByteArray bs >>= \(ba , n) ->
      if n /= sizeofByteArray ba then
        Left "ByteString does not use full ByteArray"
      else
        let !(ByteArray ba#) = ba in Right (SBS ba#)


-- | \( O(1) \) conversion from a strict 'BS.ByteString' to its underlying
-- pinned 'ByteArray', if possible. Also returns the length (in bytes) of the
-- byte array prefix that was used by the bytestring.
--
-- Strict bytestrings are allocated using 'mallocPlainForeignPtrBytes', so we
-- are expecting a 'PlainPtr' (or 'FinalPtr' when the length is 0).
-- We also require that bytestrings referencing a byte array point point at the
-- beginning, without any offset.
tryGetByteArray :: BS.ByteString -> Either String (ByteArray, Int)
tryGetByteArray (BS.Internal.BS (Foreign.ForeignPtr addr# contents) n) =
    case contents of
      Foreign.PlainPtr mba# ->
        case mutableByteArrayContentsShim# mba# `eqAddr#` addr# of
          0# -> Left "non-zero offset into ByteArray"
          _  -> -- safe, ByteString's content is considered immutable
                Right $ case unsafeFreezeByteArray# mba# realWorld# of
                  (# _, ba# #) -> (ByteArray ba#, n)
      Foreign.MallocPtr {} ->
        Left ("unsupported MallocPtr (length " <> show n <> ")")
      Foreign.PlainForeignPtr {} ->
        Left ("unsupported PlainForeignPtr (length " <> show n <> ")")
#if __GLASGOW_HASKELL__ >= 902
      Foreign.FinalPtr | n == 0 ->
        -- We can also handle empty bytestrings ('BS.empty' uses 'FinalPtr').
        Right (emptyByteArray, 0)
      Foreign.FinalPtr ->
        Left ("unsupported FinalPtr (length "  <> show n <> ")")
#endif

-- | Copied from the @primitive@ package
mutableByteArrayContentsShim# :: MutableByteArray# s -> Addr#
{-# INLINE mutableByteArrayContentsShim# #-}
mutableByteArrayContentsShim# x =
#if __GLASGOW_HASKELL__ >= 902
  mutableByteArrayContents# x
#else
  byteArrayContents# (unsafeCoerce# x)
#endif

-- | Copy of 'SBS.shortByteString', but with bounds (unchecked).
--
-- https://github.com/haskell/bytestring/issues/664
{-# INLINE shortByteStringFromTo #-}
shortByteStringFromTo :: Int -> Int -> ShortByteString -> BB.Builder
shortByteStringFromTo = \i j sbs -> BB.builder $ shortByteStringCopyStepFromTo i j sbs

-- | Like 'shortByteStringFromTo' but for 'ByteArray'
--
-- https://github.com/haskell/bytestring/issues/664
byteArrayFromTo :: Int -> Int -> ByteArray -> BB.Builder
byteArrayFromTo = \i j (ByteArray ba) -> BB.builder $ shortByteStringCopyStepFromTo i j (SBS ba)

-- | Copy of 'SBS.shortByteStringCopyStep' but with bounds (unchecked)
{-# INLINE shortByteStringCopyStepFromTo #-}
shortByteStringCopyStepFromTo ::
  Int -> Int -> ShortByteString -> BB.BuildStep a -> BB.BuildStep a
shortByteStringCopyStepFromTo !ip0 !ipe0 !sbs k =
    go ip0 ipe0
  where
    go !ip !ipe (BB.BufferRange op ope)
      | inpRemaining <= outRemaining = do
          SBS.copyToPtr sbs ip op inpRemaining
          let !br' = BB.BufferRange (op `plusPtr` inpRemaining) ope
          k br'
      | otherwise = do
          SBS.copyToPtr sbs ip op outRemaining
          let !ip' = ip + outRemaining
          return $ BB.bufferFull 1 ope (go ip' ipe)
      where
        outRemaining = ope `minusPtr` op
        inpRemaining = ipe - ip

-- | \( O(1) \) conversion if the byte array is pinned, \( O(n) \) otherwise.
-- Takes offset and length of the slice to be used.
byteArrayToByteString :: Int -> Int -> ByteArray -> BS.ByteString
byteArrayToByteString off len ba =
    assert (isValidSlice off len ba) $
      if isByteArrayPinned ba
      then unsafePinnedByteArrayToByteString off len ba
      else unsafePinnedByteArrayToByteString 0 len $ runByteArray $ do
        mba <- newPinnedByteArray len
        copyByteArray mba 0 ba off len
        return mba

-- | \( O(1) \) conversion. Takes offset and length of the slice to be used.
-- Fails if the byte array is not pinned.
--
-- Based on 'SBS.fromShort'.
unsafePinnedByteArrayToByteString :: HasCallStack => Int -> Int -> ByteArray -> BS.ByteString
unsafePinnedByteArrayToByteString off@(I# off#) len ba@(ByteArray ba#) =
    assert (isValidSlice off len ba) $
      if isByteArrayPinned ba
      then BS.Internal.BS fp len
      else error $ "unsafePinnedByteArrayToByteString: not pinned, length "
                <> show (sizeofByteArray ba)
  where
    addr# = plusAddr# (byteArrayContents# ba#) off#
    fp = Foreign.ForeignPtr addr# (Foreign.PlainPtr (unsafeCoerce# ba#))

byteArrayToSBS :: ByteArray -> ShortByteString
#if MIN_VERSION_bytestring(0,12,0)
byteArrayToSBS ba             = SBS.ShortByteString ba
#else
byteArrayToSBS (ByteArray ba) = SBS.SBS ba
#endif
