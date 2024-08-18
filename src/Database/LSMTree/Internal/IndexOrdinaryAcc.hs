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
import           Control.Monad.ST.Strict (ST, runST)
import           Data.Primitive.ByteArray (newByteArray, unsafeFreezeByteArray,
                     writeByteArray)
import           Data.Primitive.PrimVar (PrimVar, newPrimVar, readPrimVar,
                     writePrimVar)
import           Data.Vector (force, take, unsafeFreeze)
import           Data.Vector.Mutable (MVector)
import qualified Data.Vector.Mutable as Mutable (unsafeNew, write)
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

    A value @IndexOrdinaryAcc buffer keyCountRef baler@ denotes a partially
    constructed index with the following properties:

      * The keys that the index assigns to pages are stored as a prefix of the
        mutable vector @buffer@.
      * The reference @keyCountRef@ points to the number of those keys.
      * The @baler@ object is used by the index for incremental output of the
        serialised key list.
-}
data IndexOrdinaryAcc s = IndexOrdinaryAcc
                              !(MVector s SerialisedKey)
                              !(PrimVar s Int)
                              !(Baler s)

-- | Creates a new, initially empty, index.
new :: Int                       -- ^ Maximum number of keys
    -> Int                       -- ^ Minimum chunk size in bytes
    -> ST s (IndexOrdinaryAcc s) -- ^ Construction of the index
new maxKeyCount minChunkSize = assert (maxKeyCount >= 0)      $
                               IndexOrdinaryAcc              <$>
                               Mutable.unsafeNew maxKeyCount <*>
                               newPrimVar 0                  <*>
                               createBaler minChunkSize

{-|
    Appends keys to the key list of an index and outputs newly available chunks
    of the serialised key list.

    __Warning:__ Appending keys whose length cannot be represented by a 16-bit
    word may result in a corrupted serialised key list.
-}
append :: Append -> IndexOrdinaryAcc s -> ST s (Maybe Chunk)
append instruction (IndexOrdinaryAcc buffer keyCountRef baler)
    = case instruction of
          AppendSinglePage _ key -> do
              keyCount <- readPrimVar keyCountRef
              Mutable.write buffer keyCount key
              writePrimVar keyCountRef (succ keyCount)
              feedBaler (keyListElem key) baler
          AppendMultiPage key overflowPageCount -> do
              keyCount <- readPrimVar keyCountRef
              let

                  pageCount :: Int
                  !pageCount = succ (fromIntegral overflowPageCount)

                  keyCount' :: Int
                  !keyCount' = keyCount + pageCount

              mapM_ (flip (Mutable.write buffer) key)
                    [keyCount .. pred keyCount']
              writePrimVar keyCountRef keyCount'
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
        !keySizeBytes = mkPrimVector 0 2 $
                        runST $ do
                            rep <- newByteArray 2
                            writeByteArray rep 0 keySizeAsWord16
                            unsafeFreezeByteArray rep

{-|
    Returns the constructed index, along with a final chunk in case the
    serialised key list has not been fully output yet, thereby invalidating the
    index under construction. Executing @unsafeEnd index@ is only safe when
    @index@ is not used afterwards.
-}
unsafeEnd :: IndexOrdinaryAcc s -> ST s (Maybe Chunk, IndexOrdinary)
unsafeEnd (IndexOrdinaryAcc buffer keyCountRef baler) = do
    keyCount <- readPrimVar keyCountRef
    keys <- force <$> take keyCount <$> unsafeFreeze buffer
    remnant <- unsafeEndBaler baler
    return (remnant, IndexOrdinary keys)
