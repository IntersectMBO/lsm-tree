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

import           Control.Exception (assert)
import           Control.Monad.ST.Strict (ST)
import           Data.List (genericReplicate)
import           Data.Maybe (catMaybes)
import           Data.Primitive.ByteArray (byteArrayFromListN)
import           Data.STRef.Strict (STRef, modifySTRef', newSTRef, readSTRef)
import           Data.Vector (fromList)
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
import           Database.LSMTree.Internal.Vector (mkPrimVector)

{-|
    A general-purpose fence pointer index under incremental construction.

    A value @IndexOrdinaryAcc lastKeysRevRef baler@ denotes a partially
    constructed index with the following properties:

      * The keys pointed to by @lastKeysRevRef@ are the keys that the index
        assigns to pages, but in reverse order.
      * The @baler@ object is used by the index for incremental output of the
        serialised key list.
-}
data IndexOrdinaryAcc s = IndexOrdinaryAcc !(STRef s [SerialisedKey]) !(Baler s)

-- | Creates a new, initially empty, index.
new :: Int                       -- ^ Maximum chunk size in bytes
    -> ST s (IndexOrdinaryAcc s) -- ^ Construction of the index
new maxChunkSize = IndexOrdinaryAcc <$> newSTRef [] <*> createBaler maxChunkSize

{-
    Appends a single key to the key list of an index and outputs newly available
    chunks of the serialised key list.
-}
appendKey :: SerialisedKey -> IndexOrdinaryAcc s -> ST s [Chunk]
appendKey lastKey@(SerialisedKey' lastKeyBytes)
          (IndexOrdinaryAcc lastKeysRevRef baler)
    = do
          modifySTRef' lastKeysRevRef (lastKey :)
          maybeChunkIntoLastKeySize <- feedBaler lastKeySizeBytes baler
          maybeChunkIntoLastKey <- feedBaler lastKeyBytes baler
          return $ catMaybes [maybeChunkIntoLastKeySize, maybeChunkIntoLastKey]
    where

    lastKeySize :: Int
    !lastKeySize = Primitive.length lastKeyBytes

    lastKeySizeAsWord16 :: Word16
    !lastKeySizeAsWord16
        = assert (lastKeySize <= fromIntegral (maxBound :: Word16)) $
          fromIntegral lastKeySize

    lastKeySizeBytes :: Primitive.Vector Word8
    !lastKeySizeBytes = mkPrimVector 0 2 $
                        byteArrayFromListN 1 [lastKeySizeAsWord16]

{-|
    Appends keys to the key list of an index and outputs newly available chunks
    of the serialised key list.

    __Warning:__ Appending keys whose length cannot be represented by a 16-bit
    word may result in a corrupted serialised key list.
-}
append :: Append -> IndexOrdinaryAcc s -> ST s [Chunk]
append (AppendSinglePage _ lastKey)    index = appendKey lastKey index
append (AppendMultiPage key pageCount) index = fmap concat                $
                                               sequence                   $
                                               genericReplicate pageCount $
                                               appendKey key index

{-|
    Returns the constructed index, along with a final chunk in case the
    serialised key list has not been fully output yet, thereby invalidating the
    index under construction. Executing @unsafeEnd index@ is only safe when
    @index@ is not used afterwards.
-}
unsafeEnd :: IndexOrdinaryAcc s -> ST s (Maybe Chunk, IndexOrdinary)
unsafeEnd (IndexOrdinaryAcc lastKeysRevRef baler) = do
    lastKeysRev <- readSTRef lastKeysRevRef
    remnant <- unsafeEndBaler baler
    return (remnant, IndexOrdinary (fromList (reverse lastKeysRev)))
