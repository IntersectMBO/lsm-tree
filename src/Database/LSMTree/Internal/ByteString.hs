{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE CPP                 #-}
{-# LANGUAGE MagicHash           #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}
-- | @bytestring@ extras
module Database.LSMTree.Internal.ByteString (
    shortByteStringFromTo,
    byteArrayFromTo,
    unsafePinnedPrimVectorToByteString,
) where

import           Data.ByteString.Internal (ByteString (..))
import qualified Data.ByteString.Builder as BB
import qualified Data.ByteString.Builder.Internal as BB
import           Data.ByteString.Short (ShortByteString (SBS))
import qualified Data.ByteString.Short.Internal as SBS
import           Data.Primitive.ByteArray (ByteArray (..), ByteArray#,
                    MutableByteArray#, isByteArrayPinned, sizeofByteArray)
import qualified Data.Vector.Primitive as VP
import           Foreign.Ptr (minusPtr, plusPtr)
import           GHC.Exts (Int (I#), RealWorld, byteArrayContents#, plusAddr#)
import           GHC.ForeignPtr (ForeignPtr (..), ForeignPtrContents (..))
import           Unsafe.Coerce (unsafeCoerce#)
import           Data.Primitive.Types (sizeOf)

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

-- | Assumes vector uses the full underlying 'ByteArray', which must be pinned!
unsafePinnedPrimVectorToByteString :: forall a. VP.Prim a => VP.Vector a -> ByteString
unsafePinnedPrimVectorToByteString (VP.Vector offset len ba) =
  -- | offset /= 0 =
  --     error "unsafePinnedPrimVectorToByteString: offset"
  -- | elemSize * len /= sizeofByteArray ba =
  --     error $ "unsafePinnedPrimVectorToByteString: expected length "
  --          <> show elemSize <> " * " <> show len <> " = " <> show (elemSize * len)
  --          <> ", got " <> show (sizeofByteArray ba)
  -- | otherwise =
      unsafePinnedByteArrayToByteString (offset * elemSize) (len * elemSize) ba
  where
    elemSize = sizeOf (undefined :: a)

-- | Assumes the 'ByteArray' is pinned.
unsafePinnedByteArrayToByteString :: Int -> Int -> ByteArray -> ByteString
unsafePinnedByteArrayToByteString _ _ ba
  | not (isByteArrayPinned ba) = error "unsafePinnedByteArrayToByteString: not pinned"
unsafePinnedByteArrayToByteString (I# offset#) len (ByteArray ba#) =
    BS (ForeignPtr
          (plusAddr# (byteArrayContents# ba#) offset#)
          (PlainPtr (unsafeThaw ba#)))
       len
  where
    unsafeThaw :: ByteArray# -> MutableByteArray# RealWorld
    unsafeThaw = unsafeCoerce#
