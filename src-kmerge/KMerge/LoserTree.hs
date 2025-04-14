{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE CPP                 #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# OPTIONS_GHC -fexpose-all-unfoldings #-}

module KMerge.LoserTree (
    MutableLoserTree,
    newLoserTree,
    replace,
    remove,
) where

import           Control.Monad.Primitive (PrimMonad (PrimState), RealWorld)
import qualified Control.Monad.ST as Lazy
import qualified Control.Monad.ST as Strict
import           Data.Bits (unsafeShiftR)
import           Data.List.NonEmpty (NonEmpty (..))
import           Data.Primitive (MutablePrimArray, SmallMutableArray,
                     newPrimArray, newSmallArray, readPrimArray, readSmallArray,
                     setPrimArray, writePrimArray, writeSmallArray)
import           Data.Primitive.PrimVar (PrimVar, newPrimVar, readPrimVar,
                     writePrimVar)
import           Unsafe.Coerce (unsafeCoerce)

-- | Mutable Loser Tree.
data MutableLoserTree s a = MLT
    !(PrimVar s Int)                 -- ^ element count, i.e. size.
    !(PrimVar s Int)                 -- ^ index of the hole (i.e. winner's initial index)
    !(MutablePrimArray s Int)        -- ^ indices, we store the index of first match. -1 if there is no match.
    !(SmallMutableArray s a)         -- ^ values

placeholder :: a
placeholder = unsafeCoerce ()

-- | Create new 'MutableLoserTree'.
--
-- The second half of a pair is the winner value (only losers are stored in the tree).
--
newLoserTree :: forall a m. (PrimMonad m, Ord a) => NonEmpty a -> m (MutableLoserTree (PrimState m) a, a)
newLoserTree (x0 :| xs0) = do
    -- allocate array, we need one less than there are elements.
    -- one of the elements will be the winner.
    ids <- newPrimArray  len
    arr <- newSmallArray len placeholder
    case xs0 of
      [] -> do
        sizeRef <- newPrimVar 0
        holeRef <- newPrimVar 0
        return $! (MLT sizeRef holeRef ids arr, x0)
      _ -> do
        setPrimArray ids 0 len (-1)
        loop ids arr len $ x0 :| xs0
  where
    !len = length xs0

    loop :: MutablePrimArray (PrimState m) Int -> SmallMutableArray (PrimState m) a -> Int -> NonEmpty a -> m (MutableLoserTree (PrimState m) a, a)
    loop  ids  arr  idx (x :| xs) = do
        sift ids arr (parentOf idx) (parentOf idx) x idx xs

    sift :: MutablePrimArray (PrimState m) Int -> SmallMutableArray (PrimState m) a -> Int -> Int -> a -> Int -> [a] -> m (MutableLoserTree (PrimState m) a, a)
    sift !ids !arr !idxX !j !x !idx0 xs = do
        !idxY <- readPrimArray ids j
        y     <- readSmallArray arr j
        -- NOTE: The length of xs is equal to number of uninitialised entries
        -- from this we can deduce that an entry at j is uninitialised implies
        -- that xs cannot be empty.
        -- We check this invariant here and throw an exception
        -- with a descriptive error message if it is violated.
        if idxY < 0
        then case xs of
          [] -> error $ unlines
            [ "Error in KMerge.LoserTree.newLoserTree"
            , unwords [ "Invariant violated at entry # j =", show j, "with xs = [] and idxY =", show idxY ]
            ]
          e:es -> do
            writePrimArray  ids j idxX
            writeSmallArray arr j x
            loop ids arr (idx0 + 1) $ e :| es
        else
            if j <= 0
            then do
                    if x <= y
                    then do
                        sizeRef <- newPrimVar len
                        holeRef <- newPrimVar idxX
                        return (MLT sizeRef holeRef ids arr, x)
                    else do
                        writePrimArray  ids j idxX
                        writeSmallArray arr j x
                        sizeRef <- newPrimVar len
                        holeRef <- newPrimVar idxY
                        return (MLT sizeRef holeRef ids arr, y)
            else do
                    if x < y
                    then do
                        sift ids arr idxX (parentOf j) x idx0 xs
                    else do
                        writePrimArray  ids j idxX
                        writeSmallArray arr j x
                        sift ids arr idxY (parentOf j) y idx0 xs

{-# SPECIALISE newLoserTree :: forall a.   Ord a => NonEmpty a -> IO          (MutableLoserTree RealWorld a, a) #-}
{-# SPECIALISE newLoserTree :: forall a s. Ord a => NonEmpty a -> Strict.ST s (MutableLoserTree s         a, a) #-}
{-# SPECIALISE newLoserTree :: forall a s. Ord a => NonEmpty a -> Lazy.ST   s (MutableLoserTree s         a, a) #-}

{-------------------------------------------------------------------------------
  Updates
-------------------------------------------------------------------------------}

{-# SPECIALISE replace :: forall a.   Ord a => MutableLoserTree RealWorld a -> a -> IO          a #-}
{-# SPECIALISE replace :: forall a s. Ord a => MutableLoserTree s         a -> a -> Strict.ST s a #-}
{-# SPECIALISE replace :: forall a s. Ord a => MutableLoserTree s         a -> a -> Lazy.ST s   a #-}

{-# SPECIALISE remove :: forall a.   Ord a => MutableLoserTree RealWorld a -> IO          (Maybe a) #-}
{-# SPECIALISE remove :: forall a s. Ord a => MutableLoserTree s         a -> Strict.ST s (Maybe a) #-}
{-# SPECIALISE remove :: forall a s. Ord a => MutableLoserTree s         a -> Lazy.ST s   (Maybe a) #-}

-- | Don't fill the winner "hole". Return a next winner of (smaller) tournament.
remove :: forall a m. (PrimMonad m, Ord a) => MutableLoserTree (PrimState m) a -> m (Maybe a)
remove (MLT sizeRef holeRef ids arr) = do
    size <- readPrimVar sizeRef
    if size <= 0
    then return Nothing
    else do
        writePrimVar sizeRef (size - 1)
        hole <- readPrimVar holeRef
        siftEmpty hole
  where
    siftEmpty :: Int -> m (Maybe a)
    siftEmpty !j = do
        !idxY <- readPrimArray ids j
        y     <- readSmallArray arr j
        if j <= 0
        then if idxY < 0
            then return Nothing
            else do
                writePrimArray  ids j (-1)
                writeSmallArray arr j placeholder
                writePrimVar holeRef idxY
                return (Just y)
        else if idxY < 0
            then
                siftEmpty (parentOf j)
            else do
                writePrimArray  ids j (-1)
                writeSmallArray arr j placeholder
                Just <$> siftUp ids arr holeRef (parentOf j) idxY y

-- | Fill the winner "hole" with a new element. Return a new tournament winner.
replace :: forall a m. (PrimMonad m, Ord a) => MutableLoserTree (PrimState m) a -> a -> m a
replace (MLT sizeRef holeRef ids arr) val = do
    size <- readPrimVar sizeRef
    if size <= 0
    then return val
    else do
        hole <- readPrimVar holeRef
        siftUp ids arr holeRef hole hole val

{-# SPECIALISE siftUp :: forall a.   Ord a => MutablePrimArray RealWorld Int -> SmallMutableArray RealWorld a -> PrimVar RealWorld Int -> Int -> Int -> a -> IO          a #-}
{-# SPECIALISE siftUp :: forall a s. Ord a => MutablePrimArray s Int         -> SmallMutableArray s         a -> PrimVar s         Int -> Int -> Int -> a -> Strict.ST s a #-}
{-# SPECIALISE siftUp :: forall a s. Ord a => MutablePrimArray s Int         -> SmallMutableArray s         a -> PrimVar s         Int -> Int -> Int -> a -> Lazy.ST s   a #-}

siftUp :: forall a m. (PrimMonad m, Ord a) => MutablePrimArray (PrimState m) Int -> SmallMutableArray (PrimState m) a -> PrimVar (PrimState m) Int -> Int -> Int -> a -> m a
siftUp ids arr holeRef = sift
  where
    sift :: Int -> Int -> a -> m a
    sift !j !idxX !x = do
        !idxY <- readPrimArray ids j
        y     <- readSmallArray arr j
        if j <= 0
        then if idxY < 0
            then do
                writePrimVar holeRef idxX
                return x
            else do
                if x <= y
                then do
                    writePrimVar holeRef idxX
                    return x
                else do
                    writePrimArray  ids j idxX
                    writeSmallArray arr j x
                    writePrimVar holeRef idxY
                    return y
        else if idxY < 0
            then sift (parentOf j) idxX x
            else do
                if x <= y
                then do
                    sift (parentOf j) idxX x
                else do
                    writePrimArray  ids j idxX
                    writeSmallArray arr j x
                    sift (parentOf j) idxY y

{-------------------------------------------------------------------------------
  Helpers
-------------------------------------------------------------------------------}

halfOf :: Int -> Int
halfOf i = unsafeShiftR i 1
{-# INLINE halfOf #-}

parentOf :: Int -> Int
parentOf i = halfOf (i - 1)
{-# INLINE parentOf #-}
