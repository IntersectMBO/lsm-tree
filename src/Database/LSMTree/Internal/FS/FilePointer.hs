module Database.LSMTree.Internal.FS.FilePointer (
    FilePointer (..)
  , newFilePointer
  , updateFilePointer
  , setFilePointer
  ) where

import           Control.DeepSeq (NFData (..))
import           Control.Monad.Primitive (PrimMonad, PrimState)
import           Data.Primitive.PrimVar as P
import           Data.Word (Word64)

-- | A mutable file offset, suitable to share between threads.
--
-- This pointer is limited to 31-bit file offsets on 32-bit systems. This should
-- be a sufficiently large limit that we never reach it in practice.
newtype FilePointer m = FilePointer (PrimVar (PrimState m) Int)

instance NFData (FilePointer m) where
  rnf (FilePointer var) = var `seq` ()

{-# SPECIALISE newFilePointer :: IO (FilePointer IO) #-}
newFilePointer :: PrimMonad m => m (FilePointer m)
newFilePointer = FilePointer <$> P.newPrimVar 0


{-# SPECIALISE updateFilePointer :: FilePointer IO -> Int -> IO Word64 #-}
-- | Update the file offset by a given amount and return the new offset. This
-- is safe to use concurrently.
--
updateFilePointer :: PrimMonad m => FilePointer m -> Int -> m Word64
updateFilePointer (FilePointer var) n = fromIntegral <$> P.fetchAddInt var n

-- TODO: assertion
{-# SPECIALISE setFilePointer :: FilePointer IO -> Word64 -> IO () #-}
setFilePointer :: PrimMonad m => FilePointer m -> Word64 -> m ()
setFilePointer (FilePointer var) off = P.atomicWriteInt var (fromIntegral off)
