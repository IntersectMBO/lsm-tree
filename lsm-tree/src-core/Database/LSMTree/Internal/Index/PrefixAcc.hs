{-# OPTIONS_HADDOCK not-home #-}

{-|
    Incremental construction functionality for the fence pointer index that
    benefits from keys sharing prefixes.
-}
module Database.LSMTree.Internal.Index.PrefixAcc
(
    IndexPrefixAcc (..),
    new,
    newWithDefaults,
    appendSingle,
    appendMulti,
    unsafeEnd
)
where

import           Control.Exception (assert)
import           Control.Monad (when)
import           Control.Monad.ST.Strict (ST)
import           Data.Foldable (for_)
import           Data.Maybe (maybeToList)
import           Data.Primitive.PrimArray (MutablePrimArray, freezePrimArray,
                     getSizeofMutablePrimArray, newPrimArray,
                     resizeMutablePrimArray, writePrimArray)
import           Data.Primitive.PrimVar (PrimVar, newPrimVar, readPrimVar,
                     writePrimVar)
import           Data.STRef.Strict (STRef, modifySTRef', newSTRef, readSTRef,
                     writeSTRef)
import qualified Data.Vector.Primitive as VP
import           Data.Word (Word32, Word8)
import           Database.LSMTree.Internal.Chunk (Baler, Chunk, createBaler,
                     feedBaler, toByteVector, unsafeEndBaler)
import           Database.LSMTree.Internal.Index.Prefix
                     (IndexPrefix (IndexPrefix), commonPrefixLength,
                     restartInterval, separator, varintBytes)
import           Database.LSMTree.Internal.Serialise
                     (SerialisedKey (SerialisedKey'))
import           Database.LSMTree.Internal.Vector (primArrayToPrimVector)

{-|
    A fence pointer index that benefits from keys sharing prefixes, under
    incremental construction.

    The accumulator outputs the serialised entries incrementally through a
    baler, and it keeps the output chunks, so that 'unsafeEnd' can assemble
    the in-memory index from them. The restart table is kept in an unboxed
    buffer and becomes part of the footer.
-}
data IndexPrefixAcc s = IndexPrefixAcc {
    -- | The baler for the serialised entries.
    ipaBaler         :: !(Baler s),
    -- | The chunks output so far, most recent first.
    ipaChunks        :: !(STRef s [VP.Vector Word8]),
    {-|
        A buffer for the restart table, which may have unused space at the
        end. When it is full, its capacity is doubled.
    -}
    ipaRestarts      :: !(STRef s (MutablePrimArray s Word32)),
    -- | The number of used elements of the restart table buffer.
    ipaRestartsSize  :: !(PrimVar s Int),
    -- | The last key of the most recent group, or the empty key initially.
    ipaLastKey       :: !(STRef s (VP.Vector Word8)),
    -- | The separator of the most recent group, or the empty key initially.
    ipaLastSeparator :: !(STRef s (VP.Vector Word8)),
    -- | The number of entries so far.
    ipaEntryCount    :: !(PrimVar s Int),
    -- | The number of pages so far.
    ipaPageCount     :: !(PrimVar s Int),
    -- | The number of bytes of serialised entries so far.
    ipaEntriesSize   :: !(PrimVar s Int)
  }

-- | Creates a new, initially empty, index.
new :: Int                     -- ^ Minimum chunk size in bytes
    -> ST s (IndexPrefixAcc s) -- ^ Construction of the index
new minChunkSize = IndexPrefixAcc                                   <$>
                   createBaler minChunkSize                           <*>
                   newSTRef []                                        <*>
                   (newSTRef =<< newPrimArray initialRestartsCapacity) <*>
                   newPrimVar 0                                       <*>
                   (newSTRef $! VP.empty)                             <*>
                   (newSTRef $! VP.empty)                             <*>
                   newPrimVar 0                                       <*>
                   newPrimVar 0                                       <*>
                   newPrimVar 0

-- | The initial capacity of the restart table buffer, in elements.
initialRestartsCapacity :: Int
initialRestartsCapacity = 64

{-|
    For a specification of this operation, see the documentation of [its
    type-agnostic version]('Database.LSMTree.Internal.Index.newWithDefaults').
-}
newWithDefaults :: ST s (IndexPrefixAcc s)
newWithDefaults = new 4096

{-|
    Converts a number that the restart table stores to a 'Word32'.

    The restart table stores offsets into the serialised entries and page
    numbers as 32-bit numbers. Exceeding this would need an index of more than
    4 GiB or a run of more than 2^32 pages, which is 16 TiB with 4 KiB pages, so
    we treat it as a bug and raise an error.
-}
toRestartWord :: Int -> Word32
toRestartWord value
    | value > fromIntegral (maxBound :: Word32)
        = error "Database.LSMTree.Internal.Index.PrefixAcc: \
                \offset or page number exceeds 32 bits"
    | otherwise
        = fromIntegral value

{-|
    Adds an entry for a group of pages with the given first and last key and
    the given number of overflow pages.
-}
appendGroup :: forall s.
               SerialisedKey
            -> SerialisedKey
            -> Word32
            -> IndexPrefixAcc s
            -> ST s (Maybe Chunk)
appendGroup (SerialisedKey' firstKey)
            (SerialisedKey' lastKey)
            overflowPageCount
            IndexPrefixAcc {..}
    = do
          entryCount <- readPrimVar ipaEntryCount
          pageCount <- readPrimVar ipaPageCount
          entriesSize <- readPrimVar ipaEntriesSize
          previousLastKey <- readSTRef ipaLastKey
          previousSeparator <- readSTRef ipaLastSeparator
          let

              isRestart :: Bool
              !isRestart = entryCount `rem` restartInterval == 0

              currentSeparator :: VP.Vector Word8
              !currentSeparator
                  | entryCount == 0 = VP.empty
                  | otherwise       = assert (previousLastKey < firstKey) $
                                      separator previousLastKey firstKey

              shared :: Int
              !shared | isRestart = 0
                      | otherwise = commonPrefixLength previousSeparator
                                                       currentSeparator

              suffix :: VP.Vector Word8
              !suffix = VP.drop shared currentSeparator

              isMulti :: Bool
              !isMulti = overflowPageCount > 0

              header :: Int
              !header = 2 * shared + fromEnum isMulti

              blocks :: [VP.Vector Word8]
              blocks = [varintBytes header, varintBytes (VP.length suffix)] ++
                       [ varintBytes (fromIntegral overflowPageCount)
                       | isMulti ]                                         ++
                       [suffix]

          when isRestart $
              appendRestart (toRestartWord entriesSize)
                            (toRestartWord pageCount)
          writePrimVar ipaEntryCount (succ entryCount)
          writePrimVar ipaPageCount
                       (pageCount + 1 + fromIntegral overflowPageCount)
          writePrimVar ipaEntriesSize
                       (entriesSize + sum (map VP.length blocks))
          writeSTRef ipaLastKey $! VP.force lastKey
          writeSTRef ipaLastSeparator $! VP.force currentSeparator
          maybeChunk <- feedBaler blocks ipaBaler
          for_ maybeChunk $ \ chunk -> do
              -- Force the vector, so that the list holds no thunks.
              let !bytes = toByteVector chunk
              modifySTRef' ipaChunks (bytes :)
          pure maybeChunk
    where

    -- Adds an offset and a page number to the restart table.
    appendRestart :: Word32 -> Word32 -> ST s ()
    appendRestart offsetWord pageWord = do
        size <- readPrimVar ipaRestartsSize
        buffer <- readSTRef ipaRestarts
        capacity <- getSizeofMutablePrimArray buffer
        buffer' <- if size + 2 <= capacity
                       then pure buffer
                       else do
                                grown <- resizeMutablePrimArray buffer
                                                                (2 * capacity)
                                writeSTRef ipaRestarts grown
                                pure grown
        writePrimArray buffer' size offsetWord
        writePrimArray buffer' (succ size) pageWord
        writePrimVar ipaRestartsSize (size + 2)

{-|
    For a specification of this operation, see the documentation of [its
    type-agnostic version]('Database.LSMTree.Internal.Index.appendSingle').
-}
appendSingle :: (SerialisedKey, SerialisedKey)
             -> IndexPrefixAcc s
             -> ST s (Maybe Chunk)
appendSingle (firstKey, lastKey) indexAcc
    = assert (firstKey <= lastKey) $
      appendGroup firstKey lastKey 0 indexAcc

{-|
    For a specification of this operation, see the documentation of [its
    type-agnostic version]('Database.LSMTree.Internal.Index.appendMulti').
-}
appendMulti :: (SerialisedKey, Word32)
            -> IndexPrefixAcc s
            -> ST s [Chunk]
appendMulti (key, overflowPageCount) indexAcc
    = maybeToList <$> appendGroup key key overflowPageCount indexAcc

{-|
    For a specification of this operation, see the documentation of [its
    type-agnostic version]('Database.LSMTree.Internal.Index.unsafeEnd').
-}
unsafeEnd :: IndexPrefixAcc s -> ST s (Maybe Chunk, IndexPrefix)
unsafeEnd IndexPrefixAcc {..} = do
    remnant <- unsafeEndBaler ipaBaler
    chunks <- readSTRef ipaChunks
    restartsBuffer <- readSTRef ipaRestarts
    restartsSize <- readPrimVar ipaRestartsSize
    restartTable <- primArrayToPrimVector <$>
                    freezePrimArray restartsBuffer 0 restartsSize
    entryCount <- readPrimVar ipaEntryCount
    pageCount <- readPrimVar ipaPageCount
    let

        {-
            'VP.concat' always copies into a new array, even for a single
            vector, so the entries do not retain the baler buffer, of which the
            remnant is a slice.
        -}
        entries :: VP.Vector Word8
        !entries = VP.concat $
                   reverse (map toByteVector (maybeToList remnant) ++ chunks)

    pure (remnant, IndexPrefix entries restartTable entryCount pageCount)
