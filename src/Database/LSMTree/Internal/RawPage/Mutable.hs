{-# LANGUAGE ScopedTypeVariables #-}

-- TODO: remove once implemented
{-# OPTIONS_GHC -Wno-unused-top-binds #-}
{-# OPTIONS_GHC -Wno-redundant-constraints #-}

module Database.LSMTree.Internal.RawPage.Mutable (
    MRawPage
  , makeMRawPage
  , putEntries
  ) where

import           Control.DeepSeq (NFData (..))
import           Data.Bits
import           Data.Primitive.ByteArray
import           Data.Vector.Primitive.Mutable as PM
import           Data.Word
import           Database.LSMTree.Internal.Entry
import           Database.LSMTree.Internal.Serialise (SerialisedKey)

-------------------------------------------------------------------------------
-- MRawPage type
-------------------------------------------------------------------------------

data MRawPage s = MRawPage
    !Int        -- ^ offset in Word16s.
    !(MutableByteArray s)

instance NFData (MRawPage s) where
  rnf (MRawPage _ _) = ()

makeMRawPage ::
     MutableByteArray s -- ^ bytearray
  -> Int                -- ^ offset in bytes, must be 8byte aligned.
  -> MRawPage s
makeMRawPage mba off = MRawPage (div2 off) mba

-------------------------------------------------------------------------------
-- Modifiers
-------------------------------------------------------------------------------

-- (1) directory
writeNumKeys :: PrimMonad m => MRawPage (PrimState m) -> Word16 -> m ()
writeNumKeys (MRawPage off mba) = writeByteArray mba off

-- (1) directory
writeNumBlobs :: PrimMonad m => MRawPage (PrimState m) -> Word16 -> m ()
writeNumBlobs (MRawPage off mba) = writeByteArray mba (off + 1)

-- (1) directory
writePageKeysOffset :: PrimMonad m => MRawPage (PrimState m) -> Word16 -> m ()
writePageKeysOffset (MRawPage off mba) = writeByteArray mba (off + 2)

-- (2) blob-reference indicators
writeBlobRefIndicators :: PrimMonad m => MRawPage (PrimState m) ->  m ()
writeBlobRefIndicators = undefined

-- (3) operation types
writeOpTypes :: PrimMonad m => MRawPage (PrimState m) -> m ()
writeOpTypes = undefined

-- (4) blob references
writeBlobRefs :: PrimMonad m => MRawPage (PrimState m) -> m ()
writeBlobRefs = undefined

-- (5) key offsets
writeKeyOffsets :: PrimMonad m => MRawPage (PrimState m) -> m ()
writeKeyOffsets = undefined

-- (6) value offsets
writeValueOffsets :: PrimMonad m => MRawPage (PrimState m) -> m ()
writeValueOffsets = undefined

-- (7) keys
writeKeys :: PrimMonad m => MRawPage (PrimState m) -> m ()
writeKeys = undefined

-- (8) values
writeValues :: PrimMonad m => MRawPage (PrimState m) -> m ()
writeValues = undefined

putEntries :: PrimMonad m => MRawPage (PrimState m) -> [(SerialisedKey, RawEntry)] -> m ()
putEntries = undefined

-------------------------------------------------------------------------------
-- Utils
-------------------------------------------------------------------------------

div2 :: Bits a => a -> a
div2 x = unsafeShiftR x 1
