{-# LANGUAGE MagicHash           #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}
module Database.LSMTree.Internal.Vector (
    mkPrimVector,
    noRetainedExtraMemory,
    mapStrict,
    mapMStrict,
    zipWithStrict,
) where

import           Control.Monad
import           Data.Primitive.ByteArray (ByteArray, sizeofByteArray)
import           Data.Primitive.Types (Prim (sizeOfType#))
import           Data.Proxy (Proxy (..))
import qualified Data.Vector as V
import qualified Data.Vector.Primitive as PV
import           Database.LSMTree.Internal.Assertions
import           GHC.Exts (Int (..))
import           GHC.ST (runST)

mkPrimVector :: forall a. Prim a => Int -> Int -> ByteArray -> PV.Vector a
mkPrimVector off len ba =
    assert (isValidSlice (off * sizeof) (len * sizeof) ba) $
    PV.Vector off len ba
  where
    sizeof = I# (sizeOfType# (Proxy @a))
{-# INLINE mkPrimVector #-}

noRetainedExtraMemory :: forall a. Prim a => PV.Vector a -> Bool
noRetainedExtraMemory (PV.Vector off len ba) =
    off == 0 && len * sizeof == sizeofByteArray ba
   where
    sizeof = I# (sizeOfType# (Proxy @a))

{-# INLINE mapStrict #-}
-- | /( O(n) /) Like 'V.map', but strict in the produced elements of type @b@.
mapStrict :: forall a b. (a -> b) -> V.Vector a -> V.Vector b
mapStrict f v = runST (V.mapM (\x -> pure $! f x) v)

{-# INLINE mapMStrict #-}
-- | /( O(n) /) Like 'V.mapM', but strict in the produced elements of type @b@.
mapMStrict :: Monad m => (a -> m b) -> V.Vector a -> m (V.Vector b)
mapMStrict f v = V.mapM (f >=> (pure $!)) v

{-# INLINE zipWithStrict #-}
-- | /( O(min(m,n)) /) Like 'V.zipWithM', but strict in the produced elements of
-- type @c@.
zipWithStrict :: forall a b c. (a -> b -> c) -> V.Vector a -> V.Vector b -> V.Vector c
zipWithStrict f xs ys = runST (V.zipWithM (\x y -> pure $! f x y) xs ys)
