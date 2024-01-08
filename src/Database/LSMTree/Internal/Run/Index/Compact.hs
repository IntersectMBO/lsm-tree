{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE DerivingVia         #-}
{-# LANGUAGE InstanceSigs        #-}
{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving  #-}
{-# LANGUAGE TypeApplications    #-}

-- | A compact fence-pointer index for uniformly distributed keys.
--
-- TODO: add utility functions for clash probability calculations
--
-- TODO: (de-)serialisation
--
module Database.LSMTree.Internal.Run.Index.Compact (
    -- $compact
    CompactIndex (..)
    -- * Invariants and bounds
  , rangeFinderPrecisionBounds
  , suggestRangeFinderPrecision
    -- * Queries
  , search
  , countClashes
  , hasClashes
  , sizeInPages
    -- * Construction
    -- $construction-invariants
  , fromList
    -- ** Incremental
    -- $incremental
  , MCompactIndex
  , new
  , append
  , unsafeEnd
    -- *** Chunks
  , Chunk (..)
  , FinalChunk (..)
  , fromChunks
  ) where

import           Control.Monad (forM_, when)
import           Control.Monad.ST
import           Data.Bit
import           Data.IntMap.Strict (IntMap)
import qualified Data.IntMap.Strict as IntMap
import           Data.Map.Range (Bound (..), Clusive (Exclusive, Inclusive))
import           Data.Maybe (catMaybes)
import           Data.STRef.Strict
import qualified Data.Vector.Algorithms.Search as VA
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import           Data.Word
import           Database.LSMTree.Internal.Serialise

{- $compact

  A fence-pointer index is a mapping of disk pages, identified by some number
  @i@, to min-max information for keys on that page.

  Fence-pointer indexes can be constructed incrementally (e.g., using 'new',
  'append', 'unsafeEnd', and 'fromChunks') or in one go (e.g., using
  'fromList'). Regardless of the construction method however, some invariants
  must be upheld: see [construction invariants](#construction-invariants).

  Given a serialised target key @k@, an index can be 'search'ed to find a disk
  page @i@ that /might/ contain @k@. Fence-pointer indices offer no guarantee of
  whether the page contains the key, but the indices do guarentee that no page
  other than page @i@ could store @k@.

  === Compact representation

  Compact indexes save space by storing only bit-prefixes of keys (technically,
  we save full keys in some cases, but only with a very small probability).
  This is possible if:

  * We can think of keys as being /drawn/ from a uniform distribution.

  * The number of /drawn/ keys is small compared to the number of possible
    bit-prefixes.

  Given these assumptions, if we compare bit-prefixes of two drawn keys, the
  probability of them matching should be very low. For example, if we compare
  @48@-bit prefixes for @100_000_000@ SHA256 hashes, the probability of a
  matching prefix is @<0.00001@, or @<0.01%@.

  Thus, to store min-max information for keys on a page, we can get away with
  only storing min-max information for key /prefixes/.

  However, a /clash/ might still occur, albeit with a low probability. In a
  clash, the boundary between pages is unclear because the prefix of the
  maximum key for a page @i@ matches the prefix of the minimum key for a
  successor page @i+1@. In this case, we will have to look at larger
  bit-prefixes (or possibly even full keys) to determine the boundary between
  pages. As such, for each clash we store more bits of data, but again this
  only occurs with a low probability.

  === Internals

  TODO: explain range-finder bits, primary bits, secondary bits

  @
      Range-finder bits              Primary bits                  Secondary bits
    |-------------------|---------------------------------------|------------------- .... {- elided -}
     0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 0000 .... {- elided -}
  @
-}

-- | A compact fence-pointer index for uniformly distributed keys.
--
-- Compact indexes save space by storing bit-prefixes of keys.
data CompactIndex = CompactIndex {
    -- | A vector that partitions 'ciPrimary' into sub-vectors that share the
    -- same range-finder bits.
    --
    -- This is useful for finding the range of 'ciPrimary' to search based on
    -- the range-finder bits of a search key.
    --
    -- INVARIANT: each sub-vector in the partitioning of 'ciPrimary' according
    -- to 'ciRangeFinder' stores monotonically increasing primary bits.
    ciRangeFinder          :: !(VU.Vector Word32)
    -- | Determines the size of 'ciRangeFinder' as @2 ^ 'ciRangeFinderPrecision'
    -- + 1@.
    --
    -- INVARIANT: @ciRangeFinder VU.! (2 ^ 'ciRangeFinderPrecision' + 1) ==
    -- VU.length 'ciPrimary'.
  , ciRangeFinderPrecision :: !Int

    -- | Maps a page @i@ to the 32-bit slice of primary bits of its minimum key.
    --
    -- INVARIANT: @VU.length 'ciPrimary' == VU.length 'ciClashes'.
  , ciPrimary              :: !(VU.Vector Word32)

    -- | A clash on page @i@ means that the minimum key of page @i@ and the
    -- maximum key of the predecessor page @i-1@ have the exact same @32@
    -- primary bits. Use the tie-breaker index to break ties.
  , ciClashes              :: !(VU.Vector Bit)
    -- | Maps a page @i@ to its full minimum key, but only if there is a clash
    -- on page @i@.
    --
    -- INVARIANT: a key is included in this 'IntMap' if and only if 'ciClashes'
    -- recorded that there is a class. In code: @isJust (IntMap.lookup ix
    -- ciTiebreaker) == ciClashes VU.! ix@.
  , ciTieBreaker           :: !(IntMap SerialisedKey)
  }

{-------------------------------------------------------------------------------
  Invariants
-------------------------------------------------------------------------------}

-- | Inclusive bounds for range-finder bit-precision.
rangeFinderPrecisionBounds :: (Int, Int)
rangeFinderPrecisionBounds = (0, 16)

-- | Given the number of expected pages in an index, suggest a range-finder
-- bit-precision.
--
-- https://en.wikipedia.org/wiki/Birthday_problem#Probability_of_a_shared_birthday_(collision)
suggestRangeFinderPrecision :: Int -> Int
suggestRangeFinderPrecision maxPages =
    -- The calculation (before clamping) gives us the /total/ number of bits we
    -- expect to use for prefixes
    clamp $ ceiling @Double $ 2 * logBase 2 (fromIntegral maxPages)
  where
    (lb, ub)              = rangeFinderPrecisionBounds
    -- Clamp ensures that we use only bits above @32@ as range finder bits. That
    -- is, we clamp @x@ to the range @[32 .. 48]@.
    clamp x | x < (lb+32) = lb
            | x > (ub+32) = ub
            | otherwise   = x - 32

{-------------------------------------------------------------------------------
  Queries
-------------------------------------------------------------------------------}

-- | Given a search key, find the number of the disk page that /might/ contain
-- this key.
--
-- Note: there is no guarantee that the disk page contains the key. However, it
-- is guaranteed that /no other/ disk page contain the key.
--
-- TODO: optimisation ideas: we should be able to get it all unboxed,
-- non-allocating and without unnecessary bounds checking.
search :: SerialisedKey -> CompactIndex -> Maybe Int
search k CompactIndex{..} =
    let !rfbits   = fromIntegral $ topBits16 ciRangeFinderPrecision k
        !lb       = fromIntegral $ ciRangeFinder VU.! rfbits
        !ub       = fromIntegral $ ciRangeFinder VU.! (rfbits + 1)
        !primbits = sliceBits32 ciRangeFinderPrecision k
        !pageNr   = runST $ do
          -- We have to thaw the primary index to use the vector search algorithm. It is
          -- safe to use 'unsafeThaw' here because, once frozen, a compact index can not
          -- be manipulated anymore.
          !ciPrimary' <- VU.unsafeThaw ciPrimary
          -- Compute @pageNr@ similar to 'IntMap.lookupLE': find the page number
          -- @pageNr@ of the the last entry in the primary array (within bounds @[lb,
          -- ub]@) that is smaller or equal to our search key.
          --
          -- Regardless of whether the search key is actually present in a page, it
          -- /can not/ exist in a page @>pageNr@.
          subtract 1 <$> VA.gallopingSearchLeftPBounds (> primbits) ciPrimary' lb ub
    in
      if pageNr < 0 then
        -- The search key is smaller than the minimum key of the first page.
        Nothing
      else
        -- Consult the tie-breaker index, if necessary.
        breakTies pageNr (unBit $ ciClashes VU.! pageNr)
  where
    -- TODO: optimisation ideas: should we get rid of the recursive function? We
    -- should look at the GHC core to see if we can improve this.
    breakTies :: Int -> Bool -> Maybe Int
    breakTies pageNr clash
      -- Clash! The page we're currently looking at has a full minimum key that
      -- is smaller than our search key, so we're done.
      | clash
      , ciTieBreaker IntMap.! pageNr <= k
      = Just pageNr
      -- Clash! The page we're currently looking at has a full minimum key that
      -- is strictly larger than our search key, so we have to look at the page
      -- before it.
      | clash
      = breakTies (pageNr - 1) (unBit $ ciClashes VU.! (pageNr - 1))
      -- No clash! We have our page number.
      | otherwise
      = Just pageNr

countClashes :: CompactIndex -> Int
countClashes = IntMap.size . ciTieBreaker

hasClashes :: CompactIndex -> Bool
hasClashes = not . IntMap.null . ciTieBreaker

sizeInPages :: CompactIndex -> Int
sizeInPages = VU.length . ciPrimary

{-------------------------------------------------------------------------------
  Construction
-------------------------------------------------------------------------------}

{- $construction-invariants #construction-invariants#

  Constructing a compact index can go wrong, unless the following conditions are
  met:

  * /Non-empty/: a compact index can not be empty. A mutable compact index can
    be empty at first, but at least one page should be appended before it is
    finalised.

  * /Sorted/: pages must be appended in sorted order according to the keys they
    contain.

  * /Unique/: all keys should be unique within and across pages. A page can
    contain only one key, in which case the minimum and maximum keys would
    match.

  * /Partitioned/: pages must be partitioned. That is, the range-finder bits for
    all keys within a page must match.
-}

-- | One-shot construction.
fromList :: Int -> Int -> [(SerialisedKey, SerialisedKey)] -> CompactIndex
fromList rfprec maxcsize ks = runST $ do
    mci <- new rfprec maxcsize
    cs <- mapM (`append` mci) ks
    (c, fc) <- unsafeEnd mci
    pure (fromChunks (catMaybes cs ++ [c]) fc)

{- $incremental #incremental#

  Incremental construction of a compact index yields chunks incrementally. These
  chunks can be converted to a 'CompactIndex' once incremental construction is
  finalised.

  Incremental construction is an 'ST' computation that can be started using
  'new', returning an 'MCompactIndex' structure that accumulates internal state.
  'append'ing new pages to the 'MCompactIndex' /might/ yield 'Chunk's.
  Incremental construction can be finalised with 'unsafeEnd', which yields both
  a 'Chunk' (possibly) and 'FinalChunk'. Use 'fromChunks' on the 'Chunk's and
  the 'FinalChunk' to construct a 'CompactIndex'.
-}

-- | A mutable version of 'CompactIndex'. See [incremental
-- construction](#incremental).
data MCompactIndex s = MCompactIndex {
    -- * Core index structure
    mciRangeFinder          :: !(VU.MVector s Word32)
  , mciRangeFinderPrecision :: !Int
    -- | Accumulates a chunk of 'ciPrimary'.
  , mciPrimary              :: !(STRef s (VU.MVector s Word32))
    -- | Accumulates a chunk of 'ciClashes'
  , mciClashes              :: !(STRef s (VU.MVector s Bit))
  , mciTieBreaker           :: !(STRef s (IntMap SerialisedKey))
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
new rfprec maxcsize = MCompactIndex
    -- Core index structure
    <$> VUM.new (2 ^ rfprec + 1)
    <*> pure rfprec
    <*> (VUM.new maxcsize >>= newSTRef)
    <*> (VUM.new maxcsize >>= newSTRef)
    <*> newSTRef IntMap.empty
    -- Aux information required for incremental construction
    <*> pure maxcsize
    <*> newSTRef 0
    <*> newSTRef SNothing
    <*> newSTRef SNothing

-- | Append a new page entry (defined by the minimum and maximum key on the page)
-- to a mutable compact index.
--
-- INVARIANTS: see [construction invariants](#construction-invariants).
append :: (SerialisedKey, SerialisedKey) -> MCompactIndex s -> ST s (Maybe Chunk)
append (minKey, maxKey) MCompactIndex{..} = do
    pageNr <- readSTRef mciCurrentPageNumber
    writeSTRef mciCurrentPageNumber $! pageNr + 1

    -- Yield a chunk and start a new one if the current chunk is already full.
    let ix = pageNr `mod` mciMaxChunkSize
    res <-
      if ix == 0 && pageNr > 0 then do -- The current chunk is full
        cPrimary <- VU.unsafeFreeze =<< readSTRef mciPrimary
        (writeSTRef mciPrimary $!) =<< VUM.new mciMaxChunkSize
        cClashes <- VU.unsafeFreeze =<< readSTRef mciClashes
        (writeSTRef mciClashes $!) =<< VUM.new mciMaxChunkSize
        pure $ Just (Chunk{..})
      else -- the current chunk is not yet full
        pure Nothing

    -- Fill range-finder vector
    lastMinRfbits <- readSTRef mciLastMinRfbits
    let lb = smaybe NoBound (\i -> Bound (fromIntegral i) Exclusive) lastMinRfbits
        ub = Bound (fromIntegral minRfbits) Inclusive
        x  = fromIntegral pageNr
    writeRange mciRangeFinder lb ub x
    writeSTRef mciLastMinRfbits $! SJust minRfbits

    -- Fill primary vector
    readSTRef mciPrimary >>= \vum -> VUM.write vum ix minPrimbits

    -- Fill clash vector and tie-breaker map
    lastMaxPrimbits <- readSTRef mciLastMaxPrimbits
    let clash = lastMaxPrimbits == SJust minPrimbits
    readSTRef mciClashes >>= \vum -> VUM.write vum ix (Bit clash)
    when clash $ modifySTRef' mciTieBreaker (IntMap.insert pageNr minKey)
    writeSTRef mciLastMaxPrimbits $! SJust maxPrimbits

    pure res
  where
      minRfbits :: Word16
      minRfbits = topBits16 mciRangeFinderPrecision minKey

      minPrimbits, maxPrimbits :: Word32
      minPrimbits = sliceBits32 mciRangeFinderPrecision minKey
      maxPrimbits = sliceBits32 mciRangeFinderPrecision maxKey

-- | Finalise incremental construction, yielding final chunks.
--
-- This function is unsafe, so do /not/ modify the 'MCompactIndex' after using
-- 'unsafeEnd'.
--
-- INVARIANTS: see [construction invariants](#construction-invariants).
unsafeEnd :: MCompactIndex s -> ST s (Chunk, FinalChunk)
unsafeEnd mci@MCompactIndex{..} = do
    pageNr <- readSTRef mciCurrentPageNumber
    -- We're expecting at least one `append` to have happened, and that append
    -- leaves at least 1 index entry in the current chunk.
    let n | pageNr == 0 = error "unsafeEnd"
          | otherwise   = (pageNr -1) `mod` mciMaxChunkSize + 1
    cPrimary <- VU.unsafeFreeze . VUM.slice 0 n =<< readSTRef mciPrimary
    cClashes <- VU.unsafeFreeze . VUM.slice 0 n =<< readSTRef mciClashes

    -- We are not guaranteed to have seen all possible range-finder bit
    -- combinations, so we have to fill in the remainder of the rangerfinder
    -- vector.
    fillRangeFinderToEnd mci
    fcRangeFinder <- VU.unsafeFreeze mciRangeFinder
    let fcRangeFinderPrecision = mciRangeFinderPrecision
    fcTieBreaker <- readSTRef mciTieBreaker

    pure (Chunk {..}, FinalChunk {..})

-- | Fill the remainder of the range-finder vector.
fillRangeFinderToEnd :: MCompactIndex s -> ST s ()
fillRangeFinderToEnd MCompactIndex{..} = do
    pageNr <- readSTRef mciCurrentPageNumber
    lastMinRfbits <- readSTRef mciLastMinRfbits
    let lb = smaybe NoBound (BoundExclusive . fromIntegral) lastMinRfbits
        ub = NoBound
        x  = fromIntegral pageNr
    writeRange mciRangeFinder lb ub x
    writeSTRef mciLastMinRfbits $! SJust $ 2 ^ mciRangeFinderPrecision

data Chunk = Chunk {
    cPrimary :: !(VU.Vector Word32)
  , cClashes :: !(VU.Vector Bit)
  }

data FinalChunk = FinalChunk {
    fcRangeFinder          :: !(VU.Vector Word32)
  , fcRangeFinderPrecision :: !Int
  , fcTieBreaker           :: !(IntMap SerialisedKey)
  }

-- | Feed in 'Chunk's in the same order that they were yielded from incremental
-- construction.
fromChunks :: [Chunk] -> FinalChunk -> CompactIndex
fromChunks cs FinalChunk{..} = CompactIndex {
      ciRangeFinder          = fcRangeFinder
    , ciRangeFinderPrecision = fcRangeFinderPrecision
    , ciPrimary              = VU.concat $ fmap cPrimary cs
    , ciClashes              = VU.concat $ fmap cClashes cs
    , ciTieBreaker           = fcTieBreaker
    }

{-------------------------------------------------------------------------------
  Strict 'Maybe'
-------------------------------------------------------------------------------}

data SMaybe a = SNothing | SJust !a
  deriving Eq

smaybe :: b -> (a -> b) -> SMaybe a -> b
smaybe snothing sjust = \case
    SNothing -> snothing
    SJust x  -> sjust x

{-------------------------------------------------------------------------------
  'MVector' extras
-------------------------------------------------------------------------------}

writeRange :: VU.Unbox a => VU.MVector s a -> Bound Int -> Bound Int -> a -> ST s ()
writeRange v lb ub x = forM_ [lb' .. ub'] $ \j -> VUM.write v j x
  where
    lb' = case lb of
            NoBound          -> 0
            BoundExclusive i -> i + 1
            BoundInclusive i -> i
    ub' = case ub of
            NoBound          -> VUM.length v - 1
            BoundExclusive i -> i - 1
            BoundInclusive i -> i
