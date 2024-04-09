{-# LANGUAGE DerivingStrategies  #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}


{- |
  Incremental construction of a compact index yields chunks of the primary array
  that can be serialised incrementally.

  Incremental construction is an 'ST' computation that can be started using
  'new', returning an 'MCompactIndex' structure that accumulates internal state.
  'append'ing new pages to the 'MCompactIndex' /might/ yield 'Chunk's.
  Incremental construction can be finalised with 'unsafeEnd', which yields both
  a 'Chunk' (possibly) and the `CompactIndex'.
-}
module Database.LSMTree.Internal.Index.Compact.Construction (
    -- * Construction
    -- $construction-invariants
    MCompactIndex
  , PageNo (..)
  , new
  , Append (..)
  , Chunk (..)
  , append
  , appendSingle
  , appendMulti
  , unsafeEnd
  ) where

import           Control.Exception (assert)
import           Control.Monad (forM_, when)
import           Control.Monad.ST
import           Data.Bit hiding (flipBit)
import           Data.Foldable (toList)
import           Data.Ix (inRange)
import           Data.List.NonEmpty (NonEmpty)
import qualified Data.List.NonEmpty as NE
import           Data.Map.Range (Bound (..), Clusive (Exclusive, Inclusive))
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import           Data.STRef.Strict
import qualified Data.Vector.Generic.Mutable as VGM
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import           Data.Word
import           Database.LSMTree.Internal.Index.Compact
import           Database.LSMTree.Internal.Serialise

{-------------------------------------------------------------------------------
  Construction
-------------------------------------------------------------------------------}

{- $construction-invariants #construction-invariants#

  Constructing a compact index can go wrong, unless the following conditions are
  met:

  * /Sorted/: pages must be appended in sorted order according to the keys they
    contain.

  * /Partitioned/: pages must be partitioned. That is, the range-finder bits for
    all keys within a page must match.
-}

-- | A mutable version of 'CompactIndex'. See [incremental
-- construction](#incremental).
data MCompactIndex s = MCompactIndex {
    -- * Core index structure
    -- | Accumulates the range finder bits 'ciRangeFinder'.
    mciRangeFinder          :: !(VU.MVector s Word32)
  , mciRangeFinderPrecision :: !Int
    -- | Accumulates chunks of 'ciPrimary'.
  , mciPrimary              :: !(STRef s (NonEmpty (VU.MVector s Word32)))
    -- | Accumulates chunks of 'ciClashes'.
  , mciClashes              :: !(STRef s (NonEmpty (VU.MVector s Bit)))
    -- | Accumulates the 'ciTieBreaker'.
  , mciTieBreaker           :: !(STRef s (Map SerialisedKey PageNo))
    -- | Accumulates chunks of 'ciLargerThanPage'.
  , mciLargerThanPage       :: !(STRef s (NonEmpty (VU.MVector s Bit)))

    -- * Aux information required for incremental construction
    -- | Maximum size of a chunk
  , mciMaxChunkSize         :: !Int
    -- | The number of the current disk page we are constructing the index for.
  , mciCurrentPageNumber    :: !(STRef s Int)
    -- | The range-finder bits of the page-minimum key that we saw last.
    --
    --  This should be 'SNothing' if we haven't seen any keys/pages yet.
  , mciLastMinRfbits        :: !(STRef s (SMaybe Word16))
    -- | The primary bits of the page-maximum key that we saw last.
    --
    -- This should be 'SNothing' if we haven't seen any keys/pages yet.
  , mciLastMaxPrimbits      :: !(STRef s (SMaybe Word32))
    -- | The ful minimum key of the page that we saw last.
    --
    -- This should be 'SNothing' if we haven't seen any keys/pages yet.
  , mciLastMinKey           :: !(STRef s (SMaybe SerialisedKey))
  }

-- | @'new' rfprec maxcsize@ creates a new mutable index with a range-finder
-- bit-precision of @rfprec, and with a maximum chunk size of @maxcsize@.
--
-- PRECONDITION: maxcsize > 0
--
-- PRECONDITION: @rfprec@ should be within the bounds defined by
-- @rangeFinderPrecisionBounds@.
--
-- Note: after initialisation, both @rfprec@ and @maxcsize@ can no longer be changed.
new :: Int -> Int -> ST s (MCompactIndex s)
new rfprec maxcsize =
  assert (inRange rangeFinderPrecisionBounds rfprec && maxcsize > 0) $
  MCompactIndex
    -- Core index structure
    <$> VUM.new (2 ^ rfprec + 1)
    <*> pure rfprec
    <*> (newSTRef . pure =<< VUM.new maxcsize)
    <*> (newSTRef . pure =<< VUM.new maxcsize)
    <*> newSTRef Map.empty
    <*> (newSTRef . pure =<< VUM.new maxcsize)
    -- Aux information required for incremental construction
    <*> pure maxcsize
    <*> newSTRef 0
    <*> newSTRef SNothing
    <*> newSTRef SNothing
    <*> newSTRef SNothing

-- | Min\/max key-info for pages
data Append =
    -- | One or more keys are in this page, and their values fit within a single
    -- page.
    AppendSinglePage SerialisedKey SerialisedKey
    -- | There is only one key in this page, and it's value does not fit within
    -- a single page.
  | AppendMultiPage SerialisedKey Word32 -- ^ Number of overflow pages

-- | Append a new page entry to a mutable compact index.
--
-- INVARIANTS: see [construction invariants](#construction-invariants).
append :: Append -> MCompactIndex s -> ST s [Chunk]
append (AppendSinglePage kmin kmax) mci = toList <$> appendSingle (kmin, kmax) mci
append (AppendMultiPage k n) mci        = appendMulti (k, n) mci

-- | Append a single page to a mutable compact index.
--
-- INVARIANTS: see [construction invariants](#construction-invariants).
appendSingle :: forall s. (SerialisedKey, SerialisedKey) -> MCompactIndex s -> ST s (Maybe Chunk)
appendSingle (minKey, maxKey) mci@MCompactIndex{..} = do
    pageNo <- readSTRef mciCurrentPageNumber
    let ix = pageNo `mod` mciMaxChunkSize
    goAppend pageNo ix
    writeSTRef mciCurrentPageNumber $! pageNo + 1
    yield mci
  where
    minRfbits :: Word16
    minRfbits = keyTopBits16 mciRangeFinderPrecision minKey

    minPrimbits, maxPrimbits :: Word32
    minPrimbits = keySliceBits32 mciRangeFinderPrecision minKey
    maxPrimbits = keySliceBits32 mciRangeFinderPrecision maxKey

    -- | Meat of the function
    goAppend ::
         Int -- ^ Current /global/ page number
      -> Int -- ^ Current /local/ page number (inside the current chunk)
      -> ST s ()
    goAppend pageNo ix = do
        fillRangeFinder
        writePrimary
        writeClashesAndLTP
      where
        -- | Fill range-finder vector
        fillRangeFinder :: ST s ()
        fillRangeFinder = do
            lastMinRfbits <- readSTRef mciLastMinRfbits
            let lb = smaybe NoBound (\i -> Bound (fromIntegral i) Exclusive) lastMinRfbits
                ub = Bound (fromIntegral minRfbits) Inclusive
                x  = fromIntegral pageNo
            writeRange mciRangeFinder lb ub x
            writeSTRef mciLastMinRfbits $! SJust minRfbits

        -- | Set value in primary vector
        writePrimary :: ST s ()
        writePrimary =
            readSTRef mciPrimary >>= \cs -> VUM.write (NE.head cs) ix minPrimbits

        -- | Set value in clash vector, tie-breaker map and larger-than-page
        -- vector
        writeClashesAndLTP :: ST s ()
        writeClashesAndLTP = do
            lastMaxPrimbits <- readSTRef mciLastMaxPrimbits
            let clash = lastMaxPrimbits == SJust minPrimbits
            writeSTRef mciLastMaxPrimbits $! SJust maxPrimbits

            lastMinKey <- readSTRef mciLastMinKey
            let ltp = SJust minKey == lastMinKey
            writeSTRef mciLastMinKey $! SJust minKey

            readSTRef mciClashes >>= \cs -> VUM.write (NE.head cs) ix (Bit clash)
            readSTRef mciLargerThanPage >>= \cs -> VUM.write (NE.head cs) ix (Bit ltp)
            when (clash && not ltp) $
              modifySTRef' mciTieBreaker (Map.insert minKey (PageNo pageNo))

-- | Append multiple pages to the index. The minimum keys and maximum keys for
-- all these pages are set to the same key.
--
-- @appendMulti (k, n)@ is equivalent to @replicateM (n + 1) (appendSingle (k,
-- k))@, but the former should be faster faster.
--
-- INVARIANTS: see [construction invariants](#construction-invariants).
appendMulti :: forall s. (SerialisedKey, Word32) -> MCompactIndex s -> ST s [Chunk]
appendMulti (k, n0) mci@MCompactIndex{..} =
    maybe id (:) <$> appendSingle (k, k) mci <*> overflows (fromIntegral n0)
  where
    minPrimbits :: Word32
    minPrimbits = keySliceBits32 mciRangeFinderPrecision k

    -- | Fill primary, clash and LTP vectors for a larger-than-page value. Yields
    -- chunks if necessary
    overflows :: Int -> ST s [Chunk]
    overflows n
      | n <= 0 = pure []
      | otherwise = do
          pageNo <- readSTRef mciCurrentPageNumber
          let ix = pageNo `mod` mciMaxChunkSize -- will be 0 in recursive calls
              remInChunk = min n (mciMaxChunkSize - ix)
          readSTRef mciPrimary >>= \cs ->
            writeRange (NE.head cs) (BoundInclusive ix) (BoundExclusive $ ix + remInChunk) minPrimbits
          readSTRef mciClashes >>= \cs ->
            writeRange (NE.head cs) (BoundInclusive ix) (BoundExclusive $ ix + remInChunk) (Bit True)
          readSTRef mciLargerThanPage >>= \cs ->
            writeRange (NE.head cs) (BoundInclusive ix) (BoundExclusive $ ix + remInChunk) (Bit True)
          writeSTRef mciCurrentPageNumber $! pageNo + remInChunk
          res <- yield mci
          maybe id (:) res <$> overflows (n - remInChunk)

-- | Yield a chunk and start a new one if the current chunk is already full.
--
-- TODO(optimisation): yield will eagerly allocate new mutable vectors, but
-- maybe that should be done lazily.
--
-- INVARIANTS: see [construction invariants](#construction-invariants).
yield :: MCompactIndex s -> ST s (Maybe Chunk)
yield MCompactIndex{..} = do
    pageNo <- readSTRef mciCurrentPageNumber
    if pageNo `mod` mciMaxChunkSize == 0 then do -- The current chunk is full
      cPrimary <- VU.unsafeFreeze . NE.head =<< readSTRef mciPrimary
      modifySTRef' mciPrimary . NE.cons =<< VUM.new mciMaxChunkSize
      modifySTRef' mciClashes . NE.cons =<< VUM.new mciMaxChunkSize
      modifySTRef' mciLargerThanPage . NE.cons =<< VUM.new mciMaxChunkSize
      pure $ Just (Chunk cPrimary)
    else -- the current chunk is not yet full
      pure Nothing

-- | Finalise incremental construction, yielding final chunks.
--
-- This function is unsafe, so do /not/ modify the 'MCompactIndex' after using
-- 'unsafeEnd'.
--
-- INVARIANTS: see [construction invariants](#construction-invariants).
unsafeEnd :: MCompactIndex s -> ST s (Maybe Chunk, CompactIndex)
unsafeEnd mci@MCompactIndex{..} = do
    pageNo <- readSTRef mciCurrentPageNumber
    let ix = pageNo `mod` mciMaxChunkSize

    chunksPrimary <-
      traverse VU.unsafeFreeze . sliceCurrent ix =<< readSTRef mciPrimary
    chunksClashes <-
      traverse VU.unsafeFreeze . sliceCurrent ix =<< readSTRef mciClashes
    chunksLargerThanPage <-
      traverse VU.unsafeFreeze . sliceCurrent ix =<< readSTRef mciLargerThanPage

    -- Only slice out a chunk if there are entries in the chunk
    let mchunk = if ix == 0
          then Nothing
          else Just (Chunk (head chunksPrimary))

    -- We are not guaranteed to have seen all possible range-finder bit
    -- combinations, so we have to fill in the remainder of the rangerfinder
    -- vector.
    fillRangeFinderToEnd mci
    ciRangeFinder <- VU.unsafeFreeze mciRangeFinder

    let ciRangeFinderPrecision = mciRangeFinderPrecision
    let ciPrimary = VU.concat . reverse $ chunksPrimary
    let ciClashes = VU.concat . reverse $ chunksClashes
    let ciLargerThanPage = VU.concat . reverse $ chunksLargerThanPage
    ciTieBreaker <- readSTRef mciTieBreaker

    pure (mchunk, CompactIndex {..})
  where
    -- The current (most recent) chunk of the bitvectors is only partially
    -- constructed, so we need to only use the part that is already filled.
    sliceCurrent ix (c NE.:| cs)
      | ix == 0 = cs  -- current chunk is completely empty, just ignore it
      | otherwise = VUM.slice 0 ix c : cs

-- | Fill the remainder of the range-finder vector.
fillRangeFinderToEnd :: MCompactIndex s -> ST s ()
fillRangeFinderToEnd MCompactIndex{..} = do
    pageNo <- readSTRef mciCurrentPageNumber
    lastMinRfbits <- readSTRef mciLastMinRfbits
    let lb = smaybe NoBound (BoundExclusive . fromIntegral) lastMinRfbits
        ub = NoBound
        x  = fromIntegral pageNo
    writeRange mciRangeFinder lb ub x
    writeSTRef mciLastMinRfbits $! SJust $ 2 ^ mciRangeFinderPrecision


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

writeRange :: VU.Unbox a => VU.MVector s a -> Bound Int -> Bound Int -> a -> ST s ()
writeRange v lb ub x = forM_ [lb' .. ub'] $ \j -> VUM.write v j x
  where
    lb' = vectorLowerBound lb
    ub' = mvectorUpperBound v ub

vectorLowerBound :: Bound Int -> Int
vectorLowerBound = \case
    NoBound          -> 0
    BoundExclusive i -> i + 1
    BoundInclusive i -> i

mvectorUpperBound :: VGM.MVector v a => v s a -> Bound Int -> Int
mvectorUpperBound v = \case
    NoBound          -> VGM.length v - 1
    BoundExclusive i -> i - 1
    BoundInclusive i -> i
