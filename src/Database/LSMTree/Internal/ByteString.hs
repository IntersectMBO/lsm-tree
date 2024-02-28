{-# LANGUAGE BangPatterns  #-}
{-# LANGUAGE CPP           #-}
{-# LANGUAGE MagicHash     #-}
{-# LANGUAGE UnboxedTuples #-}
-- | @bytestring@ extras
module Database.LSMTree.Internal.ByteString (
    tryCheapToShort,
    tryGetByteArray,
    shortByteStringFromTo,
    byteArrayFromTo,
) where

import qualified Data.ByteString as BS
import qualified Data.ByteString.Builder as BB
import qualified Data.ByteString.Builder.Internal as BB
import qualified Data.ByteString.Internal as BS.Internal
import           Data.ByteString.Short (ShortByteString (SBS))
import qualified Data.ByteString.Short.Internal as SBS
import           Data.Primitive.ByteArray (ByteArray (..), emptyByteArray,
                     sizeofByteArray)
import           Foreign.Ptr (minusPtr, plusPtr)
import           GHC.Exts (eqAddr#, mutableByteArrayContents#, realWorld#,
                     unsafeFreezeByteArray#)
import qualified GHC.ForeignPtr as Foreign

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
-- pinned 'ByteArray', if possible.
--
-- Strict bytestrings are allocated using 'mallocPlainForeignPtrBytes', so we
-- are expecting a 'PlainPtr' (or 'FinalPtr' when the length is 0).
-- We also require that bytestrings referencing a byte array point point at the
-- beginning, without any offset.
tryGetByteArray :: BS.ByteString -> Either String (ByteArray, Int)
tryGetByteArray (BS.Internal.BS (Foreign.ForeignPtr addr# contents) n) =
    case contents of
      Foreign.PlainPtr mba# ->
        case mutableByteArrayContents# mba# `eqAddr#` addr# of
          0# -> Left "non-zero offset into ByteArray"
          _  -> -- safe, ByteString's content is considered immutable
                Right $ case unsafeFreezeByteArray# mba# realWorld# of
                  (# _, ba# #) -> (ByteArray ba#, n)
      Foreign.FinalPtr | n == 0 ->
        -- We can also handle empty bytestrings ('BS.empty' uses 'FinalPtr').
        Right (emptyByteArray, 0)
      Foreign.FinalPtr ->
        Left ("unsupported FinalPtr (length "  <> show n <> ")")
      Foreign.MallocPtr {} ->
        Left ("unsupported MallocPtr (length " <> show n <> ")")
      Foreign.PlainForeignPtr {} ->
        Left ("unsupported PlainForeignPtr (length " <> show n <> ")")

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
