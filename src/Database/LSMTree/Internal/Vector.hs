{-# LANGUAGE MagicHash           #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}
module Database.LSMTree.Internal.Vector (
    mkPrimVector,
) where

import           Data.Primitive.ByteArray (ByteArray)
import           Data.Primitive.Types (Prim (sizeOfType#))
import           Data.Proxy (Proxy (..))
import qualified Data.Vector.Primitive as PV
import           Database.LSMTree.Internal.Assertions
import           GHC.Exts (Int (..))

mkPrimVector :: forall a. Prim a => Int -> Int -> ByteArray -> PV.Vector a
mkPrimVector off len ba =
    assert (isValidSlice (off * sizeof) (len * sizeof) ba) $
    PV.Vector off len ba
  where
    sizeof = I# (sizeOfType# (Proxy @a))
{-# INLINE mkPrimVector #-}
