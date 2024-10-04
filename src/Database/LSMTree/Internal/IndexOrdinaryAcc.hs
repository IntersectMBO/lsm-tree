{- HLINT ignore "Avoid restricted alias" -}

{-|
    Incremental construction functionality for the general-purpose fence pointer
    index.
-}
module Database.LSMTree.Internal.IndexOrdinaryAcc
(
    IndexOrdinaryAcc,
    new,
    append,
    unsafeEnd
)
where

import           Prelude hiding (take)

import           Control.Exception (assert)
import           Control.Monad.ST.Strict (ST)
import qualified Data.Vector.Primitive as Primitive (Vector, length)
import           Data.Word (Word16, Word8)
import           Database.LSMTree.Internal.Chunk (Baler, Chunk, createBaler,
                     feedBaler, unsafeEndBaler)
import           Database.LSMTree.Internal.IndexCompactAcc
                     (Append (AppendMultiPage, AppendSinglePage))
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

{-|
    Appends keys to the key list of an index and outputs newly available chunks
    of the serialised key list.

    __Warning:__ Appending keys whose length cannot be represented by a 16-bit
    word may result in a corrupted serialised key list.
-}
append :: Append -> IndexOrdinaryAcc s -> ST s (Maybe Chunk)
append instruction (IndexOrdinaryAcc lastKeys baler)
    = case instruction of
          AppendSinglePage _ key -> do
              Growing.append lastKeys 1 key
              feedBaler (keyListElem key) baler
          AppendMultiPage key overflowPageCount -> do
              let

                  pageCount :: Int
                  !pageCount = succ (fromIntegral overflowPageCount)

              Growing.append lastKeys pageCount key
              feedBaler (concat (replicate pageCount (keyListElem key))) baler
    where

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
