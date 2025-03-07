{-# LANGUAGE CPP #-}
{-# OPTIONS_HADDOCK not-home #-}
{- HLINT ignore "Avoid restricted alias" -}

{-|
    Incremental construction functionality for the general-purpose fence pointer
    index.
-}
module Database.LSMTree.Internal.Index.OrdinaryAcc
(
    IndexOrdinaryAcc (IndexOrdinaryAcc),
    new,
    newWithDefaults,
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
import           Database.LSMTree.Internal.Index.Ordinary
                     (IndexOrdinary (IndexOrdinary))
import           Database.LSMTree.Internal.Serialise
                     (SerialisedKey (SerialisedKey'))
import           Database.LSMTree.Internal.Vector (byteVectorFromPrim)
import           Database.LSMTree.Internal.Vector.Growing (GrowingVector)
import qualified Database.LSMTree.Internal.Vector.Growing as Growing (append,
                     freeze, new)
#ifdef NO_IGNORE_ASSERTS
import qualified Database.LSMTree.Internal.Vector.Growing as Growing
                     (readMaybeLast)
#endif

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
    For a specification of this operation, see the documentation of [its
    type-agnostic version]('Database.LSMTree.Internal.Index.newWithDefaults').
-}
newWithDefaults :: ST s (IndexOrdinaryAcc s)
newWithDefaults = new 1024 4096

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
    For a specification of this operation, see the documentation of [its
    type-agnostic version]('Database.LSMTree.Internal.Index.appendSingle').
-}
appendSingle :: (SerialisedKey, SerialisedKey)
             -> IndexOrdinaryAcc s
             -> ST s (Maybe Chunk)
appendSingle (firstKey, lastKey) (IndexOrdinaryAcc lastKeys baler)
    = assert (firstKey <= lastKey) $
      do
#ifdef NO_IGNORE_ASSERTS
          maybeLastLastKey <- Growing.readMaybeLast lastKeys
          assert (all (< firstKey) maybeLastLastKey) $ return ()
#endif
          Growing.append lastKeys 1 lastKey
          feedBaler (keyListElem lastKey) baler

{-|
    For a specification of this operation, see the documentation of [its
    type-agnostic version]('Database.LSMTree.Internal.Index.appendMulti').
-}
appendMulti :: (SerialisedKey, Word32)
            -> IndexOrdinaryAcc s
            -> ST s [Chunk]
appendMulti (key, overflowPageCount) (IndexOrdinaryAcc lastKeys baler)
    = do
#ifdef NO_IGNORE_ASSERTS
          maybeLastLastKey <- Growing.readMaybeLast lastKeys
          assert (all (< key) maybeLastLastKey) $ return ()
#endif
          Growing.append lastKeys pageCount key
          maybeToList <$> feedBaler keyListElems baler
    where

    pageCount :: Int
    !pageCount = succ (fromIntegral overflowPageCount)

    keyListElems :: [Primitive.Vector Word8]
    keyListElems = concat (replicate pageCount (keyListElem key))

{-|
    For a specification of this operation, see the documentation of [its
    type-agnostic version]('Database.LSMTree.Internal.Index.unsafeEnd').
-}
unsafeEnd :: IndexOrdinaryAcc s -> ST s (Maybe Chunk, IndexOrdinary)
unsafeEnd (IndexOrdinaryAcc lastKeys baler) = do
    keys <- Growing.freeze lastKeys
    remnant <- unsafeEndBaler baler
    return (remnant, IndexOrdinary keys)
