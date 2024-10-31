{- HLINT ignore "Avoid restricted alias" -}

-- | Chunks of bytes, typically output during incremental index serialisation.
module Database.LSMTree.Internal.Chunk
(
    -- * Chunks
    Chunk (Chunk),
    fromChunk,

    -- * Balers
    Baler,
    createBaler,
    feedBaler,
    unsafeEndBaler
)
where

import           Prelude hiding (length)

import           Control.Exception (assert)
import           Control.Monad.ST.Strict (ST)
import           Data.List (scanl')
import           Data.Primitive.PrimVar (PrimVar, newPrimVar, readPrimVar,
                     writePrimVar)
import           Data.Vector.Primitive (Vector, length, unsafeCopy,
                     unsafeFreeze)
import           Data.Vector.Primitive.Mutable (MVector)
import qualified Data.Vector.Primitive.Mutable as Mutable (drop, length, slice,
                     take, unsafeCopy, unsafeNew)
import           Data.Word (Word8)

-- * Chunks

-- | A chunk of bytes, typically output during incremental index serialisation.
newtype Chunk = Chunk (Vector Word8)

fromChunk :: Chunk -> Vector Word8
fromChunk (Chunk content) = content

-- * Balers

{-|
    An object that receives blocks of bytes and repackages them into chunks such
    that all chunks except for a possible remnant chunk at the end are of at
    least a given minimum size.
-}
data Baler s = Baler
                   !(MVector s Word8) -- Buffer storing queued bytes
                   !(PrimVar s Int)   -- Reference to the number of queued bytes

-- | Creates a new baler.
createBaler :: Int            -- ^ Minimum chunk size in bytes
            -> ST s (Baler s) -- ^ Creation of the baler
createBaler minChunkSize = assert (minChunkSize > 0)              $
                           Baler                                 <$>
                           Mutable.unsafeNew (pred minChunkSize) <*>
                           newPrimVar 0

{-|
    Feeds a baler blocks of bytes.

    Bytes received by a baler are generally queued for later output, but if
    feeding new bytes makes the accumulated content exceed the minimum chunk
    size then a chunk containing all the accumulated content is output.
-}
feedBaler :: forall s . [Vector Word8] -> Baler s -> ST s (Maybe Chunk)
feedBaler blocks (Baler buffer remnantSizeRef) = do
    remnantSize <- readPrimVar remnantSizeRef
    let

        inputSize :: Int
        !inputSize = sum (map length blocks)

        totalSize :: Int
        !totalSize = remnantSize + inputSize

    if totalSize <= Mutable.length buffer
        then do
                 unsafeCopyBlocks (Mutable.drop remnantSize buffer)
                 writePrimVar remnantSizeRef totalSize
                 return Nothing
        else do
                 protoChunk <- Mutable.unsafeNew totalSize
                 Mutable.unsafeCopy (Mutable.take remnantSize protoChunk)
                                    (Mutable.take remnantSize buffer)
                 unsafeCopyBlocks (Mutable.drop remnantSize protoChunk)
                 writePrimVar remnantSizeRef 0
                 chunk <- Chunk <$> unsafeFreeze protoChunk
                 return (Just chunk)
    where

    unsafeCopyBlocks :: MVector s Word8 -> ST s ()
    unsafeCopyBlocks vec
        = sequence_ $
          zipWith3 (\ start size block -> unsafeCopy
                                              (Mutable.slice start size vec)
                                              block)
                   (scanl' (+) 0 blockSizes)
                   blockSizes
                   blocks
        where

        blockSizes :: [Int]
        blockSizes = map length blocks

{-|
    Returns the bytes still queued in a baler, if any, thereby invalidating the
    baler. Executing @unsafeEndBaler baler@ is only safe when @baler@ is not
    used afterwards.
-}
unsafeEndBaler :: forall s . Baler s -> ST s (Maybe Chunk)
unsafeEndBaler (Baler buffer remnantSizeRef) = do
    remnantSize <- readPrimVar remnantSizeRef
    if remnantSize == 0
        then return Nothing
        else do
                 let

                     protoChunk :: MVector s Word8
                     !protoChunk = Mutable.take remnantSize buffer

                 chunk <- Chunk <$> unsafeFreeze protoChunk
                 return (Just chunk)
