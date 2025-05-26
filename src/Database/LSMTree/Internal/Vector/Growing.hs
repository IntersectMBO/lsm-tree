{-# OPTIONS_GHC -Wno-incomplete-uni-patterns #-}
{-# OPTIONS_HADDOCK not-home #-}
{- HLINT ignore "Avoid restricted alias" -}

-- | Vectors with support for appending elements.
module Database.LSMTree.Internal.Vector.Growing
(
    GrowingVector (GrowingVector),
    new,
    append,
    freeze,
    readMaybeLast
)
where

import           Prelude hiding (init, last, length, read)

import           Control.Monad (when)
import           Control.Monad.ST.Strict (ST)
import           Data.Primitive.PrimVar (PrimVar, newPrimVar, readPrimVar,
                     writePrimVar)
import           Data.STRef.Strict (STRef, newSTRef, readSTRef, writeSTRef)
import           Data.Vector (Vector)
import qualified Data.Vector as Mutable (freeze)
import           Data.Vector.Mutable (MVector)
import qualified Data.Vector.Mutable as Mutable (grow, length, new, read, set,
                     slice, take)

{-|
    A vector with support for appending elements.

    Internally, the elements of a growing vector are stored in a buffer. This
    buffer is automatically enlarged whenever this is needed for storing
    additional elements. On each such enlargement, the size of the buffer is
    multiplied by a power of 2, whose exponent is chosen just big enough to make
    the final buffer size at least as high as the new vector length.

    Note that, while buffer sizes and vector lengths are represented as 'Int'
    values internally, the above-described buffer enlargement scheme has the
    consequence that the largest possible buffer size and thus the largest
    possible vector length may not be the maximum 'Int' value. That said, they
    are always greater than half the maximum 'Int' value, which should be enough
    for all practical purposes.
-}
data GrowingVector s a = GrowingVector
                             !(STRef s (MVector s a)) -- Reference to the buffer
                             !(PrimVar s Int)         -- Reference to the length

-- | Creates a new, initially empty, vector.
new :: Int                      -- ^ Initial buffer size
    -> ST s (GrowingVector s a) -- ^ Construction of the vector
new illegalInitialBufferSize | illegalInitialBufferSize <= 0
    = error "Initial buffer size not positive"
new initialBufferSize
    = do
        buffer <- Mutable.new initialBufferSize
        bufferRef <- newSTRef $! buffer
        lengthRef <- newPrimVar 0
        pure (GrowingVector bufferRef lengthRef)

{-|
    Appends a value a certain number of times to a vector. If a negative number
    is provided as the count, the vector is not changed.
-}
append :: forall s a . GrowingVector s a -> Int -> a -> ST s ()
append _ pseudoCount _ | pseudoCount <= 0
    = pure ()
append (GrowingVector bufferRef lengthRef) count val
    = do
          length <- readPrimVar lengthRef
          makeRoom
          buffer' <- readSTRef bufferRef
          Mutable.set (Mutable.slice length count buffer') $! val
    where

    makeRoom :: ST s ()
    makeRoom = do
        length <- readPrimVar lengthRef
        when (count > maxBound - length) (error "New length too large")
        buffer <- readSTRef bufferRef
        let

            bufferSize :: Int
            !bufferSize = Mutable.length buffer

            length' :: Int
            !length' = length + count

        when (bufferSize < length') $ do
            let

                higherBufferSizes :: [Int]
                higherBufferSizes = tail (init ++ [last]) where

                    init :: [Int]
                    last :: Int
                    (init, last : _) = span (<= maxBound `div` 2) $
                                       iterate (* 2) bufferSize
                    {-NOTE:
                        In order to prevent overflow, we have to start with the
                        current buffer size here, although we know that it is
                        not sufficient.
                    -}

                sufficientBufferSizes :: [Int]
                sufficientBufferSizes = dropWhile (< length') higherBufferSizes

            case sufficientBufferSizes of
                []
                    -> error "No sufficient buffer size available"
                bufferSize' : _
                    -> Mutable.grow buffer (bufferSize' - bufferSize) >>=
                       (writeSTRef bufferRef $!)
        writePrimVar lengthRef length'

-- | Turns a growing vector into an ordinary vector.
freeze :: GrowingVector s a -> ST s (Vector a)
freeze (GrowingVector bufferRef lengthRef) = do
    buffer <- readSTRef bufferRef
    length <- readPrimVar lengthRef
    Mutable.freeze (Mutable.take length buffer)

-- | Reads the last element of a growing vector if it exists.
readMaybeLast :: GrowingVector s a -> ST s (Maybe a)
readMaybeLast (GrowingVector bufferRef lengthRef) = do
    length <- readPrimVar lengthRef
    if length == 0
        then pure Nothing
        else do
                 buffer <- readSTRef bufferRef
                 Just <$> Mutable.read buffer (pred length)
