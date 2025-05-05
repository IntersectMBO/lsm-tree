{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE CPP                 #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# OPTIONS_GHC -fexpose-all-unfoldings #-}

-- | Mutable heap for k-merge algorithm.
--
-- This data-structure represents a min-heap with the root node *removed*.
-- (internally the filling of root value and sifting down is delayed).
--
-- Also there isn't *insert* operation, i.e. the heap can only shrink.
-- Other heap usual heap operations are *create-heap*, *extract-min* and *replace*.
-- However, as the 'MutableHeap' always represents a heap with its root (minimum value)
-- extracted, *extract-min* is "fused" to other operations.
module KMerge.Heap (
    MutableHeap (..),
    newMutableHeap,
    replaceRoot,
    extract,
) where

import           Control.Monad (when)
import           Control.Monad.Primitive (PrimMonad (PrimState), RealWorld)
import qualified Control.Monad.ST as Lazy
import qualified Control.Monad.ST as Strict
import           Data.Bits (unsafeShiftL, unsafeShiftR)
import           Data.Foldable.WithIndex (ifor_)
import           Data.List.NonEmpty (NonEmpty (..))
import           Data.Primitive (SmallMutableArray, newSmallArray,
                     readSmallArray, writeSmallArray)
import           Data.Primitive.PrimVar (PrimVar, newPrimVar, readPrimVar,
                     writePrimVar)
import           Unsafe.Coerce (unsafeCoerce)

-- | Mutable heap for k-merge algorithm.
data MutableHeap s a = MH
    !(PrimVar s Int) -- ^ element count, size
    !(SmallMutableArray s a)

-- | Placeholder value used to fill the internal array.
placeholder :: a
placeholder = unsafeCoerce ()

-- | Create new heap, and immediately extract its minimum value.
newMutableHeap :: forall a m. (PrimMonad m, Ord a) => NonEmpty a -> m (MutableHeap (PrimState m) a, a)
newMutableHeap xs = do
    let !size = length xs

    arr <- newSmallArray size placeholder
    ifor_ xs $ \idx x -> do
        writeSmallArray arr idx x
        siftUp arr x idx

    sizeRef <- newPrimVar size

    -- This indexing is safe!
    -- Due to the required NonEmpty input type, there must be at least one element to read.
    x <- readSmallArray arr 0
    writeSmallArray arr 0 placeholder
    return $! (MH sizeRef arr, x)

-- | Replace the minimum-value, and immediately extract the new minimum value.
replaceRoot :: forall a m. (PrimMonad m, Ord a) => MutableHeap (PrimState m) a -> a -> m a
replaceRoot (MH sizeRef arr) val = do
    size <- readPrimVar sizeRef
    if size <= 1
    then return val
    else do
        writeSmallArray arr 0 val
        siftDown arr size val 0
        x <- readSmallArray arr 0
        return x

{-# SPECIALISE replaceRoot :: forall a.   Ord a => MutableHeap RealWorld a -> a -> IO          a #-}
{-# SPECIALISE replaceRoot :: forall a s. Ord a => MutableHeap s         a -> a -> Strict.ST s a #-}
{-# SPECIALISE replaceRoot :: forall a s. Ord a => MutableHeap s         a -> a -> Lazy.ST   s a #-}

-- | Extract the next minimum value.
extract :: forall a m. (PrimMonad m, Ord a) => MutableHeap (PrimState m) a -> m (Maybe a)
extract (MH sizeRef arr) = do
    size <- readPrimVar sizeRef
    if size <= 1
    then return Nothing
    else do
        writePrimVar sizeRef $! size - 1
        val <- readSmallArray arr (size - 1)
        writeSmallArray arr 0 val
        siftDown arr size val 0
        x <- readSmallArray arr 0
        writeSmallArray arr (size - 1) placeholder
        return $! Just x

{-# SPECIALISE extract :: forall a.   Ord a => MutableHeap RealWorld a -> IO          (Maybe a) #-}
{-# SPECIALISE extract :: forall a s. Ord a => MutableHeap s         a -> Strict.ST s (Maybe a) #-}
{-# SPECIALISE extract :: forall a s. Ord a => MutableHeap s         a -> Lazy.ST   s (Maybe a) #-}

{-------------------------------------------------------------------------------
  Internal operations
-------------------------------------------------------------------------------}

siftUp :: forall a m. (PrimMonad m, Ord a) => SmallMutableArray (PrimState m) a -> a -> Int -> m ()
siftUp !arr !x = loop where
    loop !idx
        | idx <= 0
        = return ()

        | otherwise
        = do
            let !parent = halfOf (idx - 1)
            p <- readSmallArray arr parent
            when (x < p) $ do
              writeSmallArray arr parent x
              writeSmallArray arr idx    p
              loop parent

{-# SPECIALISE siftUp :: forall a.   Ord a => SmallMutableArray RealWorld a -> a -> Int -> IO          () #-}
{-# SPECIALISE siftUp :: forall a s. Ord a => SmallMutableArray s         a -> a -> Int -> Strict.ST s () #-}
{-# SPECIALISE siftUp :: forall a s. Ord a => SmallMutableArray s         a -> a -> Int -> Lazy.ST   s () #-}

siftDown :: forall a m. (PrimMonad m, Ord a) => SmallMutableArray (PrimState m) a -> Int -> a -> Int -> m ()
siftDown !arr !size !x = loop where
    loop !idx
        | rgt < size
        = do
            l <- readSmallArray arr lft
            r <- readSmallArray arr rgt

            if x <= l
            then do
                if x <= r
                then return ()
                else do
                    -- r < x <= l; swap x and r
                    writeSmallArray arr rgt x
                    writeSmallArray arr idx r
                    loop rgt
            else do
                if l <= r
                then do
                    -- l < x, l <= r; swap x and l
                    writeSmallArray arr idx l
                    writeSmallArray arr lft x
                    loop lft
                else do
                    -- r < l <= x; swap x and r
                    writeSmallArray arr rgt x
                    writeSmallArray arr idx r
                    loop rgt

        -- there's only left value
        | lft < size
        = do
            l <- readSmallArray arr lft
            if x <= l
            then return ()
            else do
                writeSmallArray arr idx l
                writeSmallArray arr lft x
                -- there is no need to loop further, lft was the last value.

        | otherwise
        = return ()
      where
        !lft = doubleOf idx + 1
        !rgt = doubleOf idx + 2

{-# SPECIALISE siftDown :: forall a.   Ord a => SmallMutableArray RealWorld a -> Int -> a -> Int -> IO          () #-}
{-# SPECIALISE siftDown :: forall a s. Ord a => SmallMutableArray s         a -> Int -> a -> Int -> Strict.ST s () #-}
{-# SPECIALISE siftDown :: forall a s. Ord a => SmallMutableArray s         a -> Int -> a -> Int -> Lazy.ST   s () #-}

{-------------------------------------------------------------------------------
  Helpers
-------------------------------------------------------------------------------}

halfOf :: Int -> Int
halfOf i = unsafeShiftR i 1
{-# INLINE halfOf #-}

doubleOf :: Int -> Int
doubleOf i = unsafeShiftL i 1
{-# INLINE doubleOf #-}
