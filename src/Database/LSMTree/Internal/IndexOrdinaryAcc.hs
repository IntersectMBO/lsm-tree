{- HLINT ignore "Avoid restricted alias" -}

{-|
    Incremental construction functionality for the general-purpose fence pointer
    index.
-}
module Database.LSMTree.Internal.IndexOrdinaryAcc
(
    IndexOrdinaryAcc,
    new,
    appendSingle,
    appendMulti,
    unsafeEnd
)
where

import           Prelude hiding (take)

import           Control.Exception (assert)
import           Control.Monad.ST.Strict (ST)
import           Data.Maybe (maybeToList)
import qualified Data.Vector.Primitive as Primitive (Vector, length)
import           Data.Word (Word16, Word32, Word8)
import           Database.LSMTree.Internal.Chunk (Baler, Chunk, createBaler,
                     feedBaler, unsafeEndBaler)
import           Database.LSMTree.Internal.IndexOrdinary
                     (IndexOrdinary (IndexOrdinary))
import           Database.LSMTree.Internal.Serialise
                     (SerialisedKey (SerialisedKey'))
import           Database.LSMTree.Internal.Vector (byteVectorFromPrim)
import           Database.LSMTree.Internal.Vector.Growing (GrowingVector)
import qualified Database.LSMTree.Internal.Vector.Growing as Growing (append,
                     freeze, new)

{-|
    A general-purpose fence pointer index under incremental construction.

    A value @IndexOrdinaryAcc lastKeys baler@ denotes a partially constructed
    index that assigns keys to pages according to @lastKeys@ and uses @baler@
    for incremental output of the serialised key list.
-}
data IndexOrdinaryAcc s = IndexOrdinaryAcc
                              !(GrowingVector s SerialisedKey)
                              !(Baler s)

-- | Creates a new, initially empty, index.
new :: Int                       -- ^ Initial size of the key buffer
    -> Int                       -- ^ Minimum chunk size in bytes
    -> ST s (IndexOrdinaryAcc s) -- ^ Construction of the index
new initialKeyBufferSize minChunkSize = IndexOrdinaryAcc                 <$>
                                        Growing.new initialKeyBufferSize <*>
                                        createBaler minChunkSize

-- Yields the serialisation of an element of a key list.
keyListElem :: SerialisedKey -> [Primitive.Vector Word8]
keyListElem (SerialisedKey' keyBytes) = [keySizeBytes, keyBytes] where

    keySize :: Int
    !keySize = Primitive.length keyBytes

    keySizeAsWord16 :: Word16
    !keySizeAsWord16 = assert (keySize <= fromIntegral (maxBound :: Word16)) $
                       fromIntegral keySize

    keySizeBytes :: Primitive.Vector Word8
    !keySizeBytes = byteVectorFromPrim keySizeAsWord16

{-|
    Adds information about a single page that fully comprises one or more
    key–value pairs to an index and outputs newly available chunks of the
    serialised key list.

    __Warning:__ Using keys whose length cannot be represented by a 16-bit word
    may result in a corrupted serialised key list.
-}
appendSingle :: (SerialisedKey, SerialisedKey)
             -> IndexOrdinaryAcc s
             -> ST s (Maybe Chunk)
appendSingle (_, key) (IndexOrdinaryAcc lastKeys baler)
    = do
          Growing.append lastKeys 1 key
          feedBaler (keyListElem key) baler

{-|
    Adds information about multiple pages that together comprise a single
    key–value pair to an index and outputs newly available chunks of the
    serialised key list.

    __Warning:__ Using keys whose length cannot be represented by a 16-bit word
    may result in a corrupted serialised key list.
-}
appendMulti :: (SerialisedKey, Word32)
            -> IndexOrdinaryAcc s
            -> ST s [Chunk]
appendMulti (key, overflowPageCount) (IndexOrdinaryAcc lastKeys baler)
    = do
          Growing.append lastKeys pageCount key
          maybeToList <$> feedBaler keyListElems baler
    where

    pageCount :: Int
    !pageCount = succ (fromIntegral overflowPageCount)

    keyListElems :: [Primitive.Vector Word8]
    keyListElems = concat (replicate pageCount (keyListElem key))

{-|
    Returns the constructed index, along with a final chunk in case the
    serialised key list has not been fully output yet, thereby invalidating the
    index under construction. Executing @unsafeEnd index@ is only safe when
    @index@ is not used afterwards.
-}
unsafeEnd :: IndexOrdinaryAcc s -> ST s (Maybe Chunk, IndexOrdinary)
unsafeEnd (IndexOrdinaryAcc lastKeys baler) = do
    keys <- Growing.freeze lastKeys
    remnant <- unsafeEndBaler baler
    return (remnant, IndexOrdinary keys)
