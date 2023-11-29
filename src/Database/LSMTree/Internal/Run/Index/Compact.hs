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
    -- * Construction
  , MCompactIndex
  , fromList
  , fromList'
    -- ** Incremental
    -- $incremental
  , new
  , append
  , freeze
  , unsafeFreeze
    -- ** Chunks
  , Chunk (..)
  , FinalChunk (..)
  , fromChunks
  , sliceChunk
  , sliceChunkUnsafe
  , getFinalChunk
  , getFinalChunkUnsafe
    -- * SliceBits
  , SliceBits (..)
  , sliceBits32
  , topBits16
    -- ** (Newtype) deriving
  , FiniteB (..)
  , sliceBitsDefault
  , topBitsDefault
  ) where

import           Control.Monad (forM_, when)
import           Control.Monad.ST
import           Data.Bit
import           Data.Bits (Bits (..), FiniteBits (..))
import           Data.IntMap.Strict (IntMap)
import qualified Data.IntMap.Strict as IntMap
import           Data.Map.Range (Bound (..), Clusive (Exclusive, Inclusive))
import           Data.STRef.Strict
import qualified Data.Vector.Algorithms.Search as VA
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import           Data.Word (Word16, Word32, Word64, Word8)

{- $compact

  A fence-pointer index is a mapping of disk pages, identified by some number
  @i@, to min-max information for keys on that page.

  Fence-pointer indexes can be constructed incrementally (e.g., using 'new',
  'append', and 'freeze') or in one go (e.g., using 'fromList'). Regardless of
  the construction method however, the invariant must be upheld that keys are
  sorted and unique. In general, this should follow from the invariant that
  runs contain sorted and unique keys.

  Given a target key @k@, an index can be searched ('search') to find a disk
  page @i@ that /might/ contain @k@. Fence-pointer indices offer no guarantee
  of whether the page contains the key, but the indices do guarentee that no
  page other than page @i@ could store @k@.

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
--
data CompactIndex k = CompactIndex {
    -- | A vector for finding the range of 'ciPrimary' to search based on the
    -- range-finder bits.
    --
    -- TODO: elaborate.
    ciRangeFinder          :: !(VU.Vector Word32)
    -- | Determines the size of 'ciRangeFinder' as @2 ^ 'ciRangeFinderPrecision'
    -- + 1@.
  , ciRangeFinderPrecision :: !Int

    -- | Maps a page @i@ to the 32-bit slice of primary bits of its minimum key.
  , ciPrimary              :: !(VU.Vector Word32)

    -- | A clash on page @i@ means that the minimum key of page @i@ and the
    -- maximum key of the predecessor page @i-1@ have the exact same @32@
    -- primary bits. Use the tie-breaker index to break ties.
    --
    -- TODO: we could use a ByteArray here instead
  , ciClashes              :: !(VU.Vector Bit)
    -- | Maps a page @i@ to its full minimum key, but only if there is a clash
    -- on page @i@.
    --
    -- INVARIANT: a key is included in this 'IntMap' if and only if 'ciClashes'
    -- recorded that there is a class. In code: @isJust (IntMap.lookup ix
    -- ciTiebreaker) == ciClashes VU.! ix@.
  , ciTieBreaker           :: !(IntMap k)
  }

{-------------------------------------------------------------------------------
  Invariants
-------------------------------------------------------------------------------}

-- | Inclusive bounds for range-finder bit-precision.
rangeFinderPrecisionBounds :: (Int, Int)
rangeFinderPrecisionBounds = (0, 16)

-- | https://en.wikipedia.org/wiki/Birthday_problem#Probability_of_a_shared_birthday_(collision)
suggestRangeFinderPrecision :: Int -> Int
suggestRangeFinderPrecision maxPages =
    clamp $ ceiling @Double $ 2 * logBase 2 (fromIntegral maxPages)
  where
    (lb, ub)            = rangeFinderPrecisionBounds
    clamp x | x < lb    = lb
            | x > ub    = ub
            | otherwise = x

{-------------------------------------------------------------------------------
  Queries
-------------------------------------------------------------------------------}

-- | Given a search key, find the number of the disk page that /might/ contain
-- this key.
--
-- Note: there is no guarantee that the disk page contains the key. However, it
-- is guaranteed that /no other/ disk page contain the key.
--
search ::
     forall k. (SliceBits k, Integral k)
  => k
  -> CompactIndex k
  -> Maybe Int
search k CompactIndex{..} = runST $ do
    let lb = ciRangeFinder VU.! fromIntegral (topBits ciRangeFinderPrecision k)
        ub = ciRangeFinder VU.! fromIntegral (topBits ciRangeFinderPrecision k + 1)
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
    pageNr' <- VA.gallopingSearchLeftPBounds
                (> sliceBits32 ciRangeFinderPrecision 32 k)
                ciPrimary'
                (fromIntegral lb)
                (fromIntegral ub)
    let pageNr = pageNr' - 1
    if pageNr < 0 then
      -- The search key is smaller than the minimum key of the first page.
      pure Nothing
    else
      -- Consult the tie-breaker index, if necessary.
      pure $ breakTies pageNr (unBit $ ciClashes VU.! pageNr)
  where
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

countClashes :: CompactIndex k -> Int
countClashes = IntMap.size . ciTieBreaker

hasClashes :: CompactIndex k -> Bool
hasClashes = not . IntMap.null . ciTieBreaker

{-------------------------------------------------------------------------------
  Construction
-------------------------------------------------------------------------------}

-- | A mutable version of 'CompactIndex'.
--
-- Once frozen using 'freeze' or 'unsafeFreeze', a 'CompactIndex' can not be
-- converted into a 'MCompactIndex' anymore.
--
data MCompactIndex s k = MCompactIndex {
    -- * Core index structure
    mciRangeFinder          :: !(VU.MVector s Word32)
  , mciRangeFinderPrecision :: !Int
  , mciPrimary              :: !(VU.MVector s Word32)
  , mciClashes              :: !(VU.MVector s Bit)
  , mciTieBreaker           :: !(STRef s (IntMap k))
    -- * Aux information required for incremental construction

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

-- | One-shot construction.
--
fromList :: (SliceBits k, Integral k) => Int -> [(k, k)] -> CompactIndex k
fromList rfprec ks = runST $ unsafeFreeze =<< fromList' rfprec ks

fromList' :: (SliceBits k, Integral k) => Int -> [(k, k)] -> ST s (MCompactIndex s k)
fromList' rfprec ks = do
    mci <- new rfprec n
    mapM_ (`append` mci) ks
    pure mci
  where
    n = length ks

--
-- Incremental construction
--

{- $incremental

  TODO: describe invariants and preconditions for correct, incremental
  construction.
-}

-- | @'new' rfprec n@ creates a new mutable index with a range-finder
-- bit-precision of @rfprec, and with space reserved for @n@ (disk page)
-- entries.
--
-- Note: after initialisation, the (mutable) index is not resizable, and the
-- range-finder bit-precision can not be changed.
new :: Int -> Int -> ST s (MCompactIndex s k)
new rfprec n = MCompactIndex
    -- Core index structure
    <$> VUM.new (2 ^ rfprec + 1)
    <*> pure rfprec
    <*> VUM.new n
    <*> VUM.new n
    <*> newSTRef IntMap.empty
    -- Aux information required for incremental construction
    <*> newSTRef 0
    <*> newSTRef SNothing
    <*> newSTRef SNothing

-- | Append a new page entry (defined by the minimum and maximum key on the page)
-- to a mutable compact index.
append :: (SliceBits k, Integral k) => (k, k) -> MCompactIndex s k -> ST s ()
append (minKey, maxKey) MCompactIndex{..} = do
    pageNr <- readSTRef mciCurrentPageNumber
    writeSTRef mciCurrentPageNumber $ pageNr + 1

    lastMinRfbits <- readSTRef mciLastMinRfbits
    let lb = smaybe NoBound (\i -> Bound (fromIntegral i) Exclusive) lastMinRfbits
        ub = Bound (fromIntegral minRfbits) Inclusive
        x  = fromIntegral pageNr
    writeRange mciRangeFinder lb ub x

    writeSTRef mciLastMinRfbits (SJust minRfbits)

    VUM.write mciPrimary pageNr minPrimbits

    lastMaxPrefix <- readSTRef mciLastMaxPrimbits
    let clash = lastMaxPrefix == SJust minPrimbits
    VUM.write mciClashes pageNr (Bit clash)
    when clash $ modifySTRef' mciTieBreaker (IntMap.insert pageNr minKey)
    writeSTRef mciLastMaxPrimbits $ SJust maxPrimbits
  where
      minRfbits :: Word16
      minRfbits = topBits16 mciRangeFinderPrecision minKey

      minPrimbits, maxPrimbits :: Word32
      minPrimbits = sliceBits32 mciRangeFinderPrecision 32 minKey
      maxPrimbits = sliceBits32 mciRangeFinderPrecision 32 maxKey

-- | Make a full copy of the mutable compact index and return it as an immutable
-- compact index. The 'MCompactIndex' can be safely mutated afterwards.
freeze :: MCompactIndex s k -> ST s (CompactIndex k)
freeze mci@MCompactIndex{..} = do
    -- We are not guaranteed to have seen all possible range-finder bit
    -- combinations, so we have to fill in the remainder of the rangerfinder
    -- vector.
    fillRangeFinderToEnd mci

    ciRangeFinder <- VU.freeze mciRangeFinder
    ciPrimary <- VU.freeze mciPrimary
    ciClashes <- VU.freeze mciClashes
    ciTieBreaker <- readSTRef mciTieBreaker
    pure $ CompactIndex {
        ciRangeFinderPrecision = mciRangeFinderPrecision
      , ..
      }

-- | Like 'freeze', but 'unsafeFreeze' does not make full copies of underlying
-- vectors and arrays. Do /not/ modify the 'MCompactIndex' afterwards.
unsafeFreeze :: MCompactIndex s k -> ST s (CompactIndex k)
unsafeFreeze mci@MCompactIndex{..} = do
    -- We are not guaranteed to have seen all possible range-finder bit
    -- combinations, so we have to fill in the remainder of the rangerfinder
    -- vector.
    fillRangeFinderToEnd mci

    ciRangeFinder <- VU.unsafeFreeze mciRangeFinder
    ciPrimary <- VU.unsafeFreeze mciPrimary
    ciClashes <- VU.unsafeFreeze mciClashes
    ciTieBreaker <- readSTRef mciTieBreaker
    pure $ CompactIndex {
        ciRangeFinderPrecision = mciRangeFinderPrecision
      , ..
      }

-- | Fill the remainder of the range-finder vector.
fillRangeFinderToEnd :: MCompactIndex s k -> ST s ()
fillRangeFinderToEnd MCompactIndex{..} = do
    lastMinRfbits <- readSTRef mciLastMinRfbits
    let lb = smaybe NoBound (BoundExclusive . fromIntegral) lastMinRfbits
        ub = NoBound
        x  = fromIntegral $ VUM.length mciPrimary
    writeRange mciRangeFinder lb ub x
    writeSTRef mciLastMinRfbits (SJust $ 2 ^ mciRangeFinderPrecision)

{-------------------------------------------------------------------------------
  Chunks
-------------------------------------------------------------------------------}

data Chunk k = Chunk {
    cPrimary :: !(VU.Vector Word32)
  , cClashes :: !(VU.Vector Bit)
  }

data FinalChunk k = FinalChunk {
    fcRangeFinder          :: !(VU.Vector Word32)
  , fcRangeFinderPrecision :: !Int
  , fcTieBreaker           :: !(IntMap k)
  }

fromChunks :: [Chunk k] -> FinalChunk k -> CompactIndex k
fromChunks cs FinalChunk{..} = CompactIndex {
      ciRangeFinder          = fcRangeFinder
    , ciRangeFinderPrecision = fcRangeFinderPrecision
    , ciPrimary              = VU.concat $ fmap cPrimary cs
    , ciClashes              = VU.concat $ fmap cClashes cs
    , ciTieBreaker           = fcTieBreaker
    }

sliceChunk :: Int -> Int -> MCompactIndex s k -> ST s (Chunk k)
sliceChunk pageNr n MCompactIndex{..} = do
    cPrimary <- VU.freeze $ VUM.slice pageNr n mciPrimary
    cClashes <- VU.freeze $ VUM.slice pageNr n mciClashes
    pure Chunk{..}

sliceChunkUnsafe :: Int -> Int -> MCompactIndex s k -> ST s (Chunk k)
sliceChunkUnsafe pageNr n MCompactIndex{..} = do
    cPrimary <- VU.unsafeFreeze $ VUM.slice pageNr n mciPrimary
    cClashes <- VU.unsafeFreeze $ VUM.slice pageNr n mciClashes
    pure Chunk{..}

getFinalChunk :: MCompactIndex s k -> ST s (FinalChunk k)
getFinalChunk mci@MCompactIndex{..} = do
    fillRangeFinderToEnd mci

    fcRangeFinder <- VU.freeze mciRangeFinder
    fcTieBreaker <- readSTRef mciTieBreaker
    pure FinalChunk{fcRangeFinderPrecision = mciRangeFinderPrecision, ..}

getFinalChunkUnsafe :: MCompactIndex s k -> ST s (FinalChunk k)
getFinalChunkUnsafe mci@MCompactIndex{..} = do
    fillRangeFinderToEnd mci

    fcRangeFinder <- VU.unsafeFreeze mciRangeFinder
    fcTieBreaker <- readSTRef mciTieBreaker
    pure FinalChunk{fcRangeFinderPrecision = mciRangeFinderPrecision, ..}

{-------------------------------------------------------------------------------
  SliceBits
-------------------------------------------------------------------------------}

class SliceBits a where
  -- | @'sliceBits' offset size x@ slices from @x@ a string of bits of size
  -- @n@ that starts at the 0-based @offset@.
  --
  -- Offsets are counted from the /top/, which means offset @0@ corresponds to
  -- the most significant bit.
  sliceBits :: Int -> Int -> a -> a

  -- | Like 'sliceBits', but the offset is always @0@.
  topBits :: Int -> a -> a

-- | Like 'topBits', but converts the output to a 'Word16'.
topBits16 :: (SliceBits a, Integral a) => Int -> a -> Word16
topBits16 size x = fromIntegral $ topBits size x

-- | Like 'sliceBits', but converts the output to a 'Word32'.
sliceBits32 :: (SliceBits a, Integral a) => Int -> Int -> a -> Word32
sliceBits32 offset size x = fromIntegral $ sliceBits offset size x

-- | Useful for deriving-via a 'SliceBits' instance.
newtype FiniteB a = FiniteB a

instance FiniteBits a => SliceBits (FiniteB a) where
  sliceBits :: Int -> Int -> FiniteB a -> FiniteB a
  sliceBits offset size (FiniteB x) = FiniteB $ sliceBitsDefault offset size x

  topBits :: Int -> FiniteB a -> FiniteB a
  topBits size (FiniteB x) = FiniteB $ topBitsDefault size x

sliceBitsDefault :: FiniteBits a => Int -> Int -> a -> a
sliceBitsDefault offset size x = x `shiftL` offset `shiftR` (finiteBitSize x - size)

topBitsDefault :: FiniteBits a => Int -> a -> a
topBitsDefault size x = shiftR x (finiteBitSize x - size)

deriving via FiniteB Word instance SliceBits Word
deriving via FiniteB Word8 instance SliceBits Word8
deriving via FiniteB Word16 instance SliceBits Word16
deriving via FiniteB Word32 instance SliceBits Word32
deriving via FiniteB Word64 instance SliceBits Word64
deriving via FiniteB Int instance SliceBits Int

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
