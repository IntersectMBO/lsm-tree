{-# LANGUAGE CPP #-}
{- |
  Incremental construction of a compact index yields chunks of the primary array
  that can be serialised incrementally.

  Incremental construction is an 'ST' computation that can be started using
  'new', returning an 'IndexCompactAcc' structure that accumulates internal
  state. 'append'ing new pages to the 'IndexCompactAcc' /might/ yield 'Chunk's.
  Incremental construction can be finalised with 'unsafeEnd', which yields both
  a 'Chunk' (possibly) and the `IndexCompact'.
-}
module Database.LSMTree.Internal.IndexCompactAcc (
    -- * Construction
    -- $construction-invariants
    IndexCompactAcc (..)
  , PageNo (..)
  , new
  , Append (..)
  , Chunk (..)
  , append
  , appendSingle
  , appendMulti
  , unsafeEnd
    -- * Internal: exported for testing and benchmarking
  , SMaybe (..)
  , unsafeWriteRange
  , vectorLowerBound
  , mvectorUpperBound
  ) where

#ifdef NO_IGNORE_ASSERTS
import           Control.Exception (assert)
#endif

import           Control.DeepSeq (NFData (..))
import           Control.Monad (when)
import           Control.Monad.ST.Strict
import           Data.Bit hiding (flipBit)
import           Data.Foldable (toList)
import           Data.List.NonEmpty (NonEmpty)
import qualified Data.List.NonEmpty as NE
import           Data.Map.Range (Bound (..))
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
import           Database.LSMTree.Internal.IndexCompact
import           Database.LSMTree.Internal.Serialise
import           Database.LSMTree.Internal.Unsliced

{-------------------------------------------------------------------------------
  Construction
-------------------------------------------------------------------------------}

{- $construction-invariants #construction-invariants#

  Constructing a compact index can go wrong, unless the following conditions are
  met:

  [Sorted] pages must be appended in sorted order according to the keys they
    contain.
-}

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

-- | We explictly pin the byte arrays, since that allows for more efficient
-- serialisation, as the definition of 'isByteArrayPinned' changed in GHC 9.6,
-- see <https://gitlab.haskell.org/ghc/ghc/-/issues/22255>.
--
-- TODO: remove this workaround once a solution exists, e.g. a new primop that
-- allows checking for implicit pinning.
newPinnedMVec64 :: Int -> ST s (VUM.MVector s Word64)
newPinnedMVec64 lenWords = do
    mba <- newPinnedByteArray (mul8 lenWords)
    setByteArray mba 0 lenWords (0 :: Word64)
    return (VUM.MV_Word64 (VPM.MVector 0 lenWords mba))

-- | Min\/max key-info for pages
data Append =
    -- | One or more keys are in this page, and their values fit within a single
    -- page.
    AppendSinglePage SerialisedKey SerialisedKey
    -- | There is only one key in this page, and it's value does not fit within
    -- a single page.
  | AppendMultiPage SerialisedKey Word32 -- ^ Number of overflow pages

instance NFData Append where
  rnf (AppendSinglePage kmin kmax)  = rnf kmin `seq` rnf kmax
  rnf (AppendMultiPage k nOverflow) = rnf k `seq` rnf nOverflow

-- | Append a new page entry to a mutable compact index.
--
-- INVARIANTS: see [construction invariants](#construction-invariants).
append :: Append -> IndexCompactAcc s -> ST s [Chunk]
append (AppendSinglePage kmin kmax) ica = toList <$> appendSingle (kmin, kmax) ica
append (AppendMultiPage k n) ica        = appendMulti (k, n) ica

-- | Append a single page to a mutable compact index.
--
-- INVARIANTS: see [construction invariants](#construction-invariants).
appendSingle :: forall s. (SerialisedKey, SerialisedKey) -> IndexCompactAcc s -> ST s (Maybe Chunk)
appendSingle (minKey, maxKey) ica@IndexCompactAcc{..} = do
#ifdef NO_IGNORE_ASSERTS
    lastMinKey <- readSTRef icaLastMinKey
    assert (minKey <= maxKey && smaybe True (<= minKey) lastMinKey) $ pure ()  -- sorted
#endif
    pageNo <- readSTRef icaCurrentPageNumber
    let ix = pageNo `mod` icaMaxChunkSize
    goAppend pageNo ix
    writeSTRef icaCurrentPageNumber $! pageNo + 1
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
            readSTRef icaPrimary >>= \cs -> VUM.write (NE.head cs) ix minPrimbits

        -- | Set value in clash vector, tie-breaker map and larger-than-page
        -- vector
        writeClashesAndLTP :: ST s ()
        writeClashesAndLTP = do
            lastMaxPrimbits <- readSTRef icaLastMaxPrimbits
            let clash = lastMaxPrimbits == SJust minPrimbits
            writeSTRef icaLastMaxPrimbits $! SJust maxPrimbits

            lastMinKey <- readSTRef icaLastMinKey
            let ltp = SJust minKey == lastMinKey
            writeSTRef icaLastMinKey $! SJust minKey

            readSTRef icaClashes >>= \cs -> VUM.write (NE.head cs) ix (Bit clash)
            readSTRef icaLargerThanPage >>= \cs -> VUM.write (NE.head cs) ix (Bit ltp)
            when (clash && not ltp) $
              modifySTRef' icaTieBreaker (Map.insert (makeUnslicedKey minKey) (PageNo pageNo))

-- | Append multiple pages to the index. The minimum keys and maximum keys for
-- all these pages are set to the same key.
--
-- @appendMulti (k, n)@ is equivalent to @replicateM (n + 1) (appendSingle (k,
-- k))@, but the former should be faster faster.
--
-- INVARIANTS: see [construction invariants](#construction-invariants).
appendMulti :: forall s. (SerialisedKey, Word32) -> IndexCompactAcc s -> ST s [Chunk]
appendMulti (k, n0) ica@IndexCompactAcc{..} =
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
          pageNo <- readSTRef icaCurrentPageNumber
          let ix = pageNo `mod` icaMaxChunkSize -- will be 0 in recursive calls
              remInChunk = min n (icaMaxChunkSize - ix)
          readSTRef icaPrimary >>= \cs ->
            unsafeWriteRange (NE.head cs) (BoundInclusive ix) (BoundExclusive $ ix + remInChunk) minPrimbits
          readSTRef icaClashes >>= \cs ->
            unsafeWriteRange (NE.head cs) (BoundInclusive ix) (BoundExclusive $ ix + remInChunk) (Bit True)
          readSTRef icaLargerThanPage >>= \cs ->
            unsafeWriteRange (NE.head cs) (BoundInclusive ix) (BoundExclusive $ ix + remInChunk) (Bit True)
          writeSTRef icaCurrentPageNumber $! pageNo + remInChunk
          res <- yield ica
          maybe id (:) res <$> overflows (n - remInChunk)

-- | Yield a chunk and start a new one if the current chunk is already full.
--
-- TODO(optimisation): yield will eagerly allocate new mutable vectors, but
-- maybe that should be done lazily.
--
-- INVARIANTS: see [construction invariants](#construction-invariants).
yield :: IndexCompactAcc s -> ST s (Maybe Chunk)
yield IndexCompactAcc{..} = do
    pageNo <- readSTRef icaCurrentPageNumber
    if pageNo `mod` icaMaxChunkSize == 0 then do -- The current chunk is full
      primaryChunk <- VU.unsafeFreeze . NE.head =<< readSTRef icaPrimary
      modifySTRef' icaPrimary . NE.cons =<< newPinnedMVec64 icaMaxChunkSize
      modifySTRef' icaClashes . NE.cons =<< VUM.new icaMaxChunkSize
      modifySTRef' icaLargerThanPage . NE.cons =<< VUM.new icaMaxChunkSize
      pure $ Just (Chunk primaryChunk)
    else -- the current chunk is not yet full
      pure Nothing

-- | Finalise incremental construction, yielding final chunks.
--
-- This function is unsafe, so do /not/ modify the 'IndexCompactAcc' after using
-- 'unsafeEnd'.
--
-- INVARIANTS: see [construction invariants](#construction-invariants).
unsafeEnd :: IndexCompactAcc s -> ST s (Maybe Chunk, IndexCompact)
unsafeEnd IndexCompactAcc{..} = do
    pageNo <- readSTRef icaCurrentPageNumber
    let ix = pageNo `mod` icaMaxChunkSize

    chunksPrimary <-
      traverse VU.unsafeFreeze . sliceCurrent ix =<< readSTRef icaPrimary
    chunksClashes <-
      traverse VU.unsafeFreeze . sliceCurrent ix =<< readSTRef icaClashes
    chunksLargerThanPage <-
      traverse VU.unsafeFreeze . sliceCurrent ix =<< readSTRef icaLargerThanPage

    -- Only slice out a chunk if there are entries in the chunk
    let mchunk = if ix == 0
          then Nothing
          else Just (Chunk (head chunksPrimary))

    let icPrimary = VU.concat . reverse $ chunksPrimary
    let icClashes = VU.concat . reverse $ chunksClashes
    let icLargerThanPage = VU.concat . reverse $ chunksLargerThanPage
    icTieBreaker <- readSTRef icaTieBreaker

    pure (mchunk, IndexCompact {..})
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

#ifdef NO_IGNORE_ASSERTS
smaybe :: b -> (a -> b) -> SMaybe a -> b
smaybe snothing sjust = \case
    SNothing -> snothing
    SJust x  -> sjust x
#endif

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
