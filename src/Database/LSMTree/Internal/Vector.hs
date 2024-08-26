{-# LANGUAGE MagicHash           #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}
module Database.LSMTree.Internal.Vector (
    mkPrimVector,
    byteVectorFromPrim,
    noRetainedExtraMemory,
    mapStrict,
    mapMStrict,
    imapMStrict,
    zipWithStrict,
    binarySearchL,
    unsafeInsertWithMStrict,
) where

import           Control.Monad
import           Control.Monad.Primitive (PrimMonad, PrimState)
import           Data.Primitive.ByteArray (ByteArray, newByteArray,
                     runByteArray, sizeofByteArray, writeByteArray)
import           Data.Primitive.Types (Prim (sizeOfType#), sizeOfType)
import           Data.Proxy (Proxy (..))
import qualified Data.Vector as V
import qualified Data.Vector.Algorithms.Search as VA
import qualified Data.Vector.Mutable as VM
import qualified Data.Vector.Primitive as VP
import           Data.Word (Word8)
import           Database.LSMTree.Internal.Assertions
import           GHC.Exts (Int (..))
import           GHC.ST (runST)

mkPrimVector :: forall a. Prim a => Int -> Int -> ByteArray -> VP.Vector a
mkPrimVector off len ba =
    assert (isValidSlice (off * sizeof) (len * sizeof) ba) $
    VP.Vector off len ba
  where
    sizeof = I# (sizeOfType# (Proxy @a))
{-# INLINE mkPrimVector #-}

byteVectorFromPrim :: forall a. Prim a => a -> VP.Vector Word8
byteVectorFromPrim prim = mkPrimVector 0 (sizeOfType @a) $
                          runByteArray $ do
                              rep <- newByteArray (sizeOfType @a)
                              writeByteArray rep 0 prim
                              return rep
{-# INLINE byteVectorFromPrim #-}

noRetainedExtraMemory :: forall a. Prim a => VP.Vector a -> Bool
noRetainedExtraMemory (VP.Vector off len ba) =
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

{-# INLINE imapMStrict #-}
-- | /( O(n) /) Like 'V.imapM', but strict in the produced elements of type @b@.
imapMStrict :: Monad m => (Int -> a -> m b) -> V.Vector a -> m (V.Vector b)
imapMStrict f v = V.imapM (\i -> f i >=> (pure $!)) v

{-# INLINE zipWithStrict #-}
-- | /( O(min(m,n)) /) Like 'V.zipWithM', but strict in the produced elements of
-- type @c@.
zipWithStrict :: forall a b c. (a -> b -> c) -> V.Vector a -> V.Vector b -> V.Vector c
zipWithStrict f xs ys = runST (V.zipWithM (\x y -> pure $! f x y) xs ys)

{-|
    Finds the lowest index in a given sorted vector at which the given element
    could be inserted while maintaining the sortedness.

    This is a variant of 'Data.Vector.Algorithms.Search.binarySearchL' for
    immutable vectors.
-}
binarySearchL :: Ord a => V.Vector a -> a -> Int
binarySearchL vec val = runST $ V.unsafeThaw vec >>= flip VA.binarySearchL val

{-# INLINE unsafeInsertWithMStrict #-}
-- | Insert (in a broad sense) an entry in a mutable vector at a given index,
-- but if a @Just@ entry already exists at that index, combine the two entries
-- using @f@.
unsafeInsertWithMStrict ::
     PrimMonad m
  => VM.MVector (PrimState m) (Maybe a)
  -> (a -> a -> a)  -- ^ function @f@, called as @f new old@
  -> Int
  -> a
  -> m ()
unsafeInsertWithMStrict mvec f i y = VM.unsafeModifyM mvec g i
  where
    g x = pure $! Just $! maybe y (`f` y) x
