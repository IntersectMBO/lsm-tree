{-# LANGUAGE CPP                   #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE NoFieldSelectors      #-}
{-# LANGUAGE OverloadedRecordDot   #-}
{-# OPTIONS_HADDOCK not-home       #-}

-- |
-- Incremental construction of a compact index yields chunks of the primary array
-- that can be serialised incrementally.
--
-- Incremental construction is an 'ST' computation that can be started using
-- 'new', returning an 'IndexCompactAcc' structure that accumulates internal
-- state. 'append'ing new pages to the 'IndexCompactAcc' /might/ yield 'Chunk's.
-- Incremental construction can be finalised with 'unsafeEnd', which yields both
-- a 'Chunk' (possibly) and the `IndexCompact'.
--
module Database.LSMTree.Internal.Index.CompactAcc (
    -- * Construction
    IndexCompactAcc (..)
  , new
  , newWithDefaults
  , appendSingle
  , appendMulti
  , unsafeEnd
    -- * Utility
  , SMaybe (..)
  , smaybe
    -- * Internal: exported for testing and benchmarking
  , unsafeWriteRange
  , vectorLowerBound
  , mvectorUpperBound
  ) where

#ifdef NO_IGNORE_ASSERTS
import           Control.Exception (assert)
#endif

import           Control.Monad (when)
import           Control.Monad.ST.Strict
import           Data.Bit hiding (flipBit)
import           Data.List.NonEmpty (NonEmpty)
import qualified Data.List.NonEmpty as NE
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import           Data.Primitive.ByteArray (newPinnedByteArray, setByteArray)
import           Data.STRef.Strict
import qualified Data.Vector.Generic.Mutable as VGM
import qualified Data.Vector.Primitive.Mutable as VPM
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import           Data.Word
import           Database.LSMTree.Internal.BitMath
import           Database.LSMTree.Internal.Chunk (Chunk)
import           Database.LSMTree.Internal.Index.Compact
import           Database.LSMTree.Internal.Map.Range (Bound (..))
import           Database.LSMTree.Internal.Page
import           Database.LSMTree.Internal.Serialise
import           Database.LSMTree.Internal.Unsliced

{-------------------------------------------------------------------------------
  Construction
-------------------------------------------------------------------------------}

-- | A mutable version of 'IndexCompact'. See [incremental
-- construction](#incremental).
data IndexCompactAcc s = IndexCompactAcc {
    -- * Core index structure
    -- | Accumulates pinned chunks of 'ciPrimary'.
    icaPrimary           :: !(STRef s (NonEmpty (VU.MVector s Word64)))
    -- | Accumulates chunks of 'ciClashes'.
  , icaClashes           :: !(STRef s (NonEmpty (VU.MVector s Bit)))
    -- | Accumulates the 'ciTieBreaker'.
  , icaTieBreaker        :: !(STRef s (Map (Unsliced SerialisedKey) PageNo))
    -- | Accumulates chunks of 'ciLargerThanPage'.
  , icaLargerThanPage    :: !(STRef s (NonEmpty (VU.MVector s Bit)))

    -- * Aux information required for incremental construction
    -- | Maximum size of a chunk
  , icaMaxChunkSize      :: !Int
    -- | The number of the current disk page we are constructing the index for.
  , icaCurrentPageNumber :: !(STRef s Int)
    -- | The primary bits of the page-maximum key that we saw last.
    --
    -- This should be 'SNothing' if we haven't seen any keys/pages yet.
  , icaLastMaxPrimbits   :: !(STRef s (SMaybe Word64))
    -- | The ful minimum key of the page that we saw last.
    --
    -- This should be 'SNothing' if we haven't seen any keys/pages yet.
  , icaLastMinKey        :: !(STRef s (SMaybe SerialisedKey))
  }

-- | @'new' maxcsize@ creates a new mutable index with a maximum chunk size of
-- @maxcsize@.
--
-- PRECONDITION: maxcsize > 0
--
-- Note: after initialisation, @maxcsize@ can no longer be changed.
new ::Int -> ST s (IndexCompactAcc s)
new maxcsize = IndexCompactAcc
    -- Core index structure
    <$> (newSTRef . pure =<< newPinnedMVec64 maxcsize)
    <*> (newSTRef . pure =<< VUM.new maxcsize)
    <*> newSTRef Map.empty
    <*> (newSTRef . pure =<< VUM.new maxcsize)
    -- Aux information required for incremental construction
    <*> pure maxcsize
    <*> newSTRef 0
    <*> newSTRef SNothing
    <*> newSTRef SNothing

-- | We explicitly pin the byte arrays, since that allows for more efficient
-- serialisation, as the definition of 'isByteArrayPinned' changed in GHC 9.6,
-- see <https://gitlab.haskell.org/ghc/ghc/-/issues/22255>.
--
-- TODO: remove this workaround once a solution exists, e.g. a new primop that
-- allows checking for implicit pinning.
newPinnedMVec64 :: Int -> ST s (VUM.MVector s Word64)
newPinnedMVec64 lenWords = do
    mba <- newPinnedByteArray (mul8 lenWords)
    setByteArray mba 0 lenWords (0 :: Word64)
    pure (VUM.MV_Word64 (VPM.MVector 0 lenWords mba))

{-|
    For a specification of this operation, see the documentation of [its
    type-agnostic version]('Database.LSMTree.Internal.Index.newWithDefaults').
-}
newWithDefaults :: ST s (IndexCompactAcc s)
newWithDefaults = new 1024

{-|
    For a specification of this operation, see the documentation of [its
    type-agnostic version]('Database.LSMTree.Internal.Index.appendSingle').
-}
appendSingle :: forall s. (SerialisedKey, SerialisedKey) -> IndexCompactAcc s -> ST s (Maybe Chunk)
appendSingle (minKey, maxKey) ica = do
#ifdef NO_IGNORE_ASSERTS
    lastMinKey <- readSTRef ica.icaLastMinKey
    assert (minKey <= maxKey && smaybe True (<= minKey) lastMinKey) $ pure ()  -- sorted
#endif
    pageNo <- readSTRef ica.icaCurrentPageNumber
    let ix = pageNo `mod` ica.icaMaxChunkSize
    goAppend pageNo ix
    writeSTRef ica.icaCurrentPageNumber $! pageNo + 1
    yield ica
  where
    minPrimbits, maxPrimbits :: Word64
    minPrimbits = keyTopBits64 minKey
    maxPrimbits = keyTopBits64 maxKey

    -- | Meat of the function
    goAppend ::
         Int -- ^ Current /global/ page number
      -> Int -- ^ Current /local/ page number (inside the current chunk)
      -> ST s ()
    goAppend pageNo ix = do
        writePrimary
        writeClashesAndLTP
      where
        -- | Set value in primary vector
        writePrimary :: ST s ()
        writePrimary =
            readSTRef ica.icaPrimary >>= \cs -> VUM.write (NE.head cs) ix minPrimbits

        -- | Set value in clash vector, tie-breaker map and larger-than-page
        -- vector
        writeClashesAndLTP :: ST s ()
        writeClashesAndLTP = do
            lastMaxPrimbits <- readSTRef ica.icaLastMaxPrimbits
            let clash = lastMaxPrimbits == SJust minPrimbits
            writeSTRef ica.icaLastMaxPrimbits $! SJust maxPrimbits

            lastMinKey <- readSTRef ica.icaLastMinKey
            let ltp = SJust minKey == lastMinKey
            writeSTRef ica.icaLastMinKey $! SJust minKey

            readSTRef ica.icaClashes >>= \cs -> VUM.write (NE.head cs) ix (Bit clash)
            readSTRef ica.icaLargerThanPage >>= \cs -> VUM.write (NE.head cs) ix (Bit ltp)
            when (clash && not ltp) $
              modifySTRef' ica.icaTieBreaker (Map.insert (makeUnslicedKey minKey) (PageNo pageNo))

{-|
    For a specification of this operation, see the documentation of [its
    type-agnostic version]('Database.LSMTree.Internal.Index.appendMulti').
-}
appendMulti :: forall s. (SerialisedKey, Word32) -> IndexCompactAcc s -> ST s [Chunk]
appendMulti (k, n0) ica =
    maybe id (:) <$> appendSingle (k, k) ica <*> overflows (fromIntegral n0)
  where
    minPrimbits :: Word64
    minPrimbits = keyTopBits64 k

    -- | Fill primary, clash and LTP vectors for a larger-than-page value. Yields
    -- chunks if necessary
    overflows :: Int -> ST s [Chunk]
    overflows n
      | n <= 0 = pure []
      | otherwise = do
          pageNo <- readSTRef ica.icaCurrentPageNumber
          let ix = pageNo `mod` ica.icaMaxChunkSize -- will be 0 in recursive calls
              remInChunk = min n (ica.icaMaxChunkSize - ix)
          readSTRef ica.icaPrimary >>= \cs ->
            unsafeWriteRange (NE.head cs) (BoundInclusive ix) (BoundExclusive $ ix + remInChunk) minPrimbits
          readSTRef ica.icaClashes >>= \cs ->
            unsafeWriteRange (NE.head cs) (BoundInclusive ix) (BoundExclusive $ ix + remInChunk) (Bit True)
          readSTRef ica.icaLargerThanPage >>= \cs ->
            unsafeWriteRange (NE.head cs) (BoundInclusive ix) (BoundExclusive $ ix + remInChunk) (Bit True)
          writeSTRef ica.icaCurrentPageNumber $! pageNo + remInChunk
          res <- yield ica
          maybe id (:) res <$> overflows (n - remInChunk)

-- | Yield a chunk and start a new one if the current chunk is already full.
--
-- TODO(optimisation): yield will eagerly allocate new mutable vectors, but
-- maybe that should be done lazily.
--
-- INVARIANTS: see [construction invariants](#construction-invariants).
yield :: IndexCompactAcc s -> ST s (Maybe Chunk)
yield ica = do
    pageNo <- readSTRef ica.icaCurrentPageNumber
    if pageNo `mod` ica.icaMaxChunkSize == 0 then do -- The current chunk is full
      primaryChunk <- VU.unsafeFreeze . NE.head =<< readSTRef ica.icaPrimary
      modifySTRef' ica.icaPrimary . NE.cons =<< newPinnedMVec64 ica.icaMaxChunkSize
      modifySTRef' ica.icaClashes . NE.cons =<< VUM.new ica.icaMaxChunkSize
      modifySTRef' ica.icaLargerThanPage . NE.cons =<< VUM.new ica.icaMaxChunkSize
      pure $ Just (word64VectorToChunk primaryChunk)
    else -- the current chunk is not yet full
      pure Nothing

{-|
    For a specification of this operation, see the documentation of [its
    type-agnostic version]('Database.LSMTree.Internal.Index.unsafeEnd').
-}
unsafeEnd :: IndexCompactAcc s -> ST s (Maybe Chunk, IndexCompact)
unsafeEnd ica = do
    pageNo <- readSTRef ica.icaCurrentPageNumber
    let ix = pageNo `mod` ica.icaMaxChunkSize

    chunksPrimary <-
      traverse VU.unsafeFreeze . sliceCurrent ix =<< readSTRef ica.icaPrimary
    chunksClashes <-
      traverse VU.unsafeFreeze . sliceCurrent ix =<< readSTRef ica.icaClashes
    chunksLargerThanPage <-
      traverse VU.unsafeFreeze . sliceCurrent ix =<< readSTRef ica.icaLargerThanPage

    -- Only slice out a chunk if there are entries in the chunk
    let mchunk = if ix == 0
          then Nothing
          else Just (word64VectorToChunk (head chunksPrimary))

    let icPrimary = VU.concat . reverse $ chunksPrimary
    let icClashes = VU.concat . reverse $ chunksClashes
    let icLargerThanPage = VU.concat . reverse $ chunksLargerThanPage
    icTieBreaker <- readSTRef ica.icaTieBreaker

    pure (mchunk, IndexCompact {
        icPrimary = icPrimary,
        icClashes = icClashes,
        icTieBreaker = icTieBreaker,
        icLargerThanPage = icLargerThanPage
    })
  where
    -- The current (most recent) chunk of the bitvectors is only partially
    -- constructed, so we need to only use the part that is already filled.
    sliceCurrent ix (c NE.:| cs)
      | ix == 0 = cs  -- current chunk is completely empty, just ignore it
      | otherwise = VUM.slice 0 ix c : cs

{-------------------------------------------------------------------------------
  Strict 'Maybe'
-------------------------------------------------------------------------------}

data SMaybe a = SNothing | SJust !a
  deriving stock (Eq, Show)

smaybe :: b -> (a -> b) -> SMaybe a -> b
smaybe snothing sjust = \case
    SNothing -> snothing
    SJust x  -> sjust x

{-------------------------------------------------------------------------------
 Vector extras
-------------------------------------------------------------------------------}

unsafeWriteRange :: VU.Unbox a => VU.MVector s a -> Bound Int -> Bound Int -> a -> ST s ()
unsafeWriteRange !v !lb !ub !x = VUM.set (VUM.unsafeSlice lb' len v) x
  where
    !lb' = vectorLowerBound lb
    !ub' = mvectorUpperBound v ub
    !len = ub' - lb' + 1

-- | Map a 'Bound' to the equivalent inclusive lower bound.
vectorLowerBound :: Bound Int -> Int
vectorLowerBound = \case
    NoBound          -> 0
    BoundExclusive i -> i + 1
    BoundInclusive i -> i

-- | Map a 'Bound' to the equivalent inclusive upper bound.
mvectorUpperBound :: VGM.MVector v a => v s a -> Bound Int -> Int
mvectorUpperBound v = \case
    NoBound          -> VGM.length v - 1
    BoundExclusive i -> i - 1
    BoundInclusive i -> i
