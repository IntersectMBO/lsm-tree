{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE CPP          #-}
-- | @bytestring@ extras
module Database.LSMTree.Internal.ByteString (
    shortByteStringFromTo,
    byteArrayFromTo,
) where

import qualified Data.ByteString.Builder as BB
import qualified Data.ByteString.Builder.Internal as BB
import           Data.ByteString.Short (ShortByteString (SBS))
import qualified Data.ByteString.Short.Internal as SBS
import           Data.Primitive.ByteArray (ByteArray (..))
import           Foreign.Ptr (minusPtr, plusPtr)

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
