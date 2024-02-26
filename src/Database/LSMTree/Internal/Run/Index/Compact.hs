{-# LANGUAGE BangPatterns       #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE LambdaCase         #-}
{-# LANGUAGE RecordWildCards    #-}
{-# LANGUAGE TypeApplications   #-}

-- | A compact fence-pointer index for uniformly distributed keys.
--
-- TODO: add utility functions for clash probability calculations
--
module Database.LSMTree.Internal.Run.Index.Compact (
    -- $compact
    CompactIndex (..)
    -- * Invariants and bounds
  , rangeFinderPrecisionBounds
  , NumPages
  , suggestRangeFinderPrecision
    -- * Queries
  , PageNo (..)
  , SearchResult (..)
  , PageSpan (..)
  , toPageSpan
  , search
  , countClashes
  , hasClashes
  , sizeInPages
    -- * Construction
    -- $construction-invariants
  , Append (..)
  , fromList
  , fromListSingles
  , fromChunks
    -- * Serialisation
  , chunkBuilder
  , finalChunkBuilder
  , fromSBS
  ) where

import           Control.Exception (assert)
import           Control.Monad (when)
import           Control.Monad.ST
import           Data.Bit hiding (flipBit)
import           Data.Bits (unsafeShiftR, (.&.))
import qualified Data.ByteString.Builder as BB
import           Data.ByteString.Short (ShortByteString (..))
import           Data.Foldable (toList)
import           Data.Map.Range (Bound (..))
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import           Data.Maybe (catMaybes, fromJust)
import           Data.Primitive.ByteArray (ByteArray (..), indexByteArray,
                     sizeofByteArray)
import qualified Data.Primitive.PrimArray as P
import qualified Data.Vector.Algorithms.Search as VA
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Primitive as VP
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Base as VU (Vector (V_Word32))
import           Data.Word
import           Database.LSMTree.Internal.BitMath
import           Database.LSMTree.Internal.ByteString (byteArrayFromTo)
import           Database.LSMTree.Internal.Entry (NumEntries (..))
import           Database.LSMTree.Internal.Run.Index.Compact.Construction
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

  === Intro

  A run is a file that stores a collection of key-value pairs. The data is
  sorted by key, and the data is divided into disk pages. A fence pointer index
  is a mapping of each disk page to min\/max information for keys on that page.
  As such, the index stores the keys that mark the /boundaries/ for each page. A
  lookup of a key in a run first searches the fence pointer index to find the
  page that contains the relevant key range, before doing a single I/O to
  retrieve the page. The guarantee of an index search is this:

  GUARANTEE: /if/ the key is somewhere in the run, then it can only be in the
  page that is returned by the index search.

  This module presents a /compact/ implementation of a fence pointer index.
  However, we first consider a simple implementation of a fence pointer index.
  After that, we show how we can save on the memory size of the index
  representation if the keys are uniformly distributed, like hashes. Let's start
  with some basic definitions:

  > type Run k v = -- elided
  > type Page k v = -- elided
  > minKey :: Page k v -> k
  > maxKey :: Page k v - k
  > type PageNo = Int

  The first implementation one may come up with is a vector containing the
  minimum key and maximum key on each page. As such, the index stores the
  key-interval @[minKey p, maxKey p]@ for each page @p@. @search1@ searches the
  vector for the key-interval that contains the search key, if it exists, and
  returns the corresponding vector index as a page number.

  > type Index1 k = V.Vector (k, k)
  >
  > mkIndex1 :: Run k v -> Index1 k
  > mkIndex1 = V.fromList . fmap (\p -> (minKey p, maxKey p))
  >
  > search1 :: k -> Index1 k -> Maybe PageNo
  > search1 = -- elided

  We can reduce the memory size of `Index1` by half if we store only the minimum
  keys on each page. As such, the index now stores the key-interval @[minKey p,
  minKey p')@ for each page @p@ and successor page @p'@. GUARANTEE is still
  guaranteed, because the old intervals are strictly contained in the new ones.
  The cost of this is that @search2@ could return a @Just@ even if @search1@
  would return a @Nothing@, but we prefer to lose some precision in order to
  achieve the memory size reduction. @search2@ searches the vector for the
  largest key smaller or equal to the given one, if it exists, and returns the
  corresponding vector index as a page number.

  > type Index2 k = V.Vector k
  >
  > mkIndex2 :: Run k v -> Index2 k
  > mkIndex2 = V.fromList . fmap minKey
  >
  > search2 :: k -> Index k -> PageNo
  > search2 = -- elided

  Now on to creating a more compact representation, which relies on a property
  of the keys that are used: the keys must be uniformly distributed values, like
  hashes.

  === Compact representation

  As mentioned before, we can save on the memory size of the index
  representation if the keys are uniformly distributed, like hashes. From now
  on, we will just assume that our keys are hashes, which can be viewed as
  strings of bits.

  The intuition behind the compact index is this: often, we don't need to look
  at full bit-strings to compare independently generated hashes against
  eachother. The probability of the @n@ most significant bits of two
  independently generated hashes matching is @(1/(2^n))@, and if we pick @n@
  large enough then we can expect a very small number of collisions. More
  generally, the expected number of @n@-bit hashes that have to be generated
  before a collision is observed is @2^(n/2)@, see [the birthday
  problem](https://en.wikipedia.org/wiki/Birthday_problem#Probability_of_a_shared_birthday_(collision).
  Or, phrased differently, if we store only the @n@ most significant bits of
  independently generated hashes in our index, then we can store up to @2^(n/2)@
  of thoses hashes before the expected number of collisions becomes one. Still,
  we need a way to break ties if there are collisions, because the probability
  of collisions is non-zero.

  ==== Clashes

  In our previous incarnations of the "simple" index, we relied on the minimum
  keys to define the boundaries between pages, or more precisely, the pages'
  key-intervals. If we look only at a subset of the most significant bits, that
  is no longer true in the presence of collisions. Let's illustrate this using
  an example of two contiguous pages @pi@ and @pj@:

  > minKey pi == "00000000"
  > maxKey pi == "10000000"
  > minKey pj == "10000001"
  > maxKey pj == "11111111"

  Say we store only the 4 most significant bits (left-to-right) in our compact
  index. This means that those 4 bits of @maxKey pi@ and @minKey pj@ collide.
  The problem is that the interval @[minKey pi, maxKey pi]@ strictly contains
  the interval @[minKey pi, minKey pj)@! The first 4 bits of @minKey pj@ do not
  actually define the boundary between @pi@ and @pj@, and so there would be no
  way to answer if a search key with first 4 bits "1000" could be found in @pi@
  or @pj@. We call such a situation a /clash/,

  The solution in these cases is to: (i) record that a clash occurred between
  pages @pi@ and @pj@, and (ii) store the full key @maxKey pi@ separately. The
  record of clashes can be implemented as simply as a single bit per page: with
  a @True@ bit meaning a clash with the previous page. Note therefore that the
  first page's bit is always going to be @False@. We store the full keys using
  a 'Map'. It is ok that this is not a compact representation, because we
  expect to store full keys for only a very small number of pages.

  The example below shows a simplified view of the compact index implementation
  so far. As an example, we store the @32@ most significant bits of each minimum
  key in the @primary@ index, the record of clashes is called @clashes@, and the
  @IntMap@ is named the @tieBreaker@ map. @search3@ can at any point during the
  search, consult @clashes@ and @tieBreaker@ to /break ties/.

  > --              (primary         , clashes      , tieBreaker)
  > type Index3 k = (VU.Vector Word32, VU.Vector Bit, IntMap k  )
  >
  > mkIndex3 :: Run k v -> Index3
  > mkIndex3 = -- elided
  >
  > search3 :: k -> Index3 -> PageNo
  > search3 = -- elided

  Let's compare the memory size of @Index2@ with @Index3@. Say we have \(n\)
  pages, and keys that are \(k\) bits each, then the memory size of @Index2@ is
  \(O(n~k)\) bits. Alternatively, for the same \(n\) pages, if we store only the
  \(c\) most significant bits, then the memory size of @Index3@ is
  \(O\left(n~c + n + k E\left[\text{collision}~n~c\right]\right)\) bits, where
  the last summand is the expected number of collisions for \(n\) independently
  generated hashes of bit-size \(c\). Precisely, the expected number of
  collisions is \(\frac{n^2 - n}{2 \times 2^c}\), so we can simplify the memory
  size to \(O\left(n~c + n + k \frac{n^2}{2^c}\right)\).

  So, @Index3@ is not strictly an improvement over @Index2@ if we look at memory
  complexity alone. However, /in practice/ @Index3@ is an improvement over
  @Index2@ if we can pick an \(c\) that is (i) much smaller than \(k\) and (ii)
  keeps \( \text{E}\left[\text{collision}~n~c\right] \) small (e.g. close to 1).
  For example, storing the first 48 bits of 2.5 million SHA256 hashes reduces
  memory size from \(256 n\) bits to \(48 n + n\) bits, because the expected
  number of collisions is smaller than 1.

  ==== Range-finder

  We extend this scheme by noting that the first few most significant bits of
  the keys are repeated many times in adjacent keys. For example, out of a
  sequence of sorted uniformly distributed keys, approximately a quarter will
  start with bits 00, a quarter with 01, et cetera. In principle these bits
  could be shared and not repeated.

  This is the same principle exploited by a (bitwise) trie. While a trie uses
  this idea recursively, we use a simple single level approach, with one top
  level \"range finder\" array. The idea is that we choose to share the first
  \(r\) bits in a dense array of size \(2^r\). Each index \(i\) in this array
  corresponds to a \(r\)-bit prefix of keys with value \(i\). Each entry
  specifies the interval of the primary index that contains keys that share the
  bit prefix \(i\). Thus we can take a search key, extract its first \(r\) bits
  and use this to index into the range finder array, yielding the interval of
  the primary index that we then need to search.

  This idea can thus reduce the range of the primary index that must be
  searched, improving search performance, but more interestingly some versions
  of this idea can extend the number of key prefix bits that can be stored, at
  the cost of a very modest increase in memory use.

  There are several plausible variation on this idea. The one we use is to
  ensure that entries in the primary array are perfectly partitioned in their
  first \(r\) bits, thus ensuring that the intervals mentioned above have no
  overlap (and no gaps). Ensuring this requires that the keys in each page all
  share the same \(r\)-bit key prefix. This must be guaranteed during page
  construction, and could come at the cost of some underfull pages. The
  advantage of this choice is that the range finder array can be represented
  with a single integer for each entry. Each one represents the offset in the
  primary index of the beginning of the span of key (prefixes) that share the
  same \(r\)-bit prefix. Pairs of entries thus give the interval in the primary
  index (as an inclusive lower bound and exclusive upper bound). Consequently,
  the array is of size \(2^r + 1\), with a final entry for the end of the last
  interval.

  Furthermore, this allows the primary index to omit those first \(r\) bits and
  instead store the 32 bits of the key after dropping the initial \(r\) bits.
  This allows representing several more bits, at a relatively low additional
  memory cost. For example, storing an additional 8 bits costs \(4 \times 2^8 =
  1024\) bytes. Of course this scales poorly so we do not allow more than 16
  bits, for a maximum memory cost of 256Kb. This representation therefore can
  store between 32 and 48 bit key prefixes, which is good for up to around 16M
  pages. At 40 entries per page this would be over 650M keys. To go any bigger
  one would need to move to 64bit entries in the primary index, which doubles
  the memory cost, but that would allow 64--80 bit key prefixes.

  Note that dropping the initial \(r\) bits from the entries in the primary
  index means that the index overall is not monotone, it is only monotone
  within each interval. This is ok of course because the binary search must
  always be performed within these intervals.

  Note also that it is not always better to use more range finder bits, since
  more bits means smaller sequences that share the same bit prefix. Shorter
  runs increases the relative wastage due to underfull pages. Using the
  number of bits needed to get the expected number of collisions to be 1 also
  happens to give sensible partition sizes -- as well as keeping the tie
  breaker map small -- so this is what we use in 'suggestRangeFinderPrecision'.

  === Representing clashes and larger-than-page entries

  A complicating factor is the need to represent larger-than-page entries in
  the compact index. This need arises from the fact that we can have single
  key/value pairs that need more than one disk page to represent.

  One strategy would be to read the first page, discover that it is a
  multi-page entry and then read the subsequent pages. This would however
  double the disk I\/O latency and complicate the I\/O handling logic (which is
  non-trivial due to it being asynchronous).

  The strategy we use is to have the index be able to return the range of pages
  that need to be read, and then all pages can be read in one batch. This means
  the index must return not just individual page numbers but intervals, and
  with a representation capable of doing so. This is not implausible since we
  have an entry in the primary index for every disk page, and so we have
  entries both for the first and subsequent pages of a larger-than-page entry.
  The natural thing to do is have each of these subsequent primary index
  entries contain the same key prefix value. This means a binary search will
  find the /last/ entry in a run of equal prefix values.

  What leads to complexity is that we will /also/ get runs of equal values if
  we have clashes between pages (as discussed above). So in the general case
  we may have a run of equal values made up of a mixture of clashes and
  larger-than-page entries.

  So the general situation is that after a binary search we have found the
  end of what may turn out to be a run of clashes and larger-than-page values
  and we must disambigutate and return the appropriate single page (for the
  ordinary case) or an interval of pages (for the LTP case).

  To disambigutate we make use of the clash bits, and we make the choice to
  say that /all/ the entries for a LTP have their clash bit set, irrespective
  of whether the LTP is in fact involved in a clash. This may seem
  counter-intuitive but it leads to a simpler mathematical definition (below).

  The search algorithm involves searching backwards in the clash bits to find
  the beginning of the run of entries that are involved. To establish which
  entry within the run is the right one to return, we can consult the tie
  breaker map by looking for the biggest entry that is less than or equal to
  the full search key. This may then point to an index within the run of
  clashing entries, in which case this is the right entry, but it may also
  point to an earlier and thus irrelevant entry, in which case the first entry
  in the run is the right one.

  Note that this second case also covers the case of a single non-clashing LTP.

  Finally, to determine if the selected entry is an LTP and if so what interval
  of pages to return, we make use of a second bit vector of LTP \"overflow\"
  pages. This bit vector has 1 values for LTP overflow pages (i.e. the 2nd and
  subsequent pages) and 0 otherwise. We can then simply search forwards to find
  the length of the LTP (or 1 if it is not an LTP).

  === A semi-formal description of the compact index #rep-descr#

  * \(n\) is the number of pages
  * \(ps = \{p_i \mid 0 \leq i < n \}\) is a sorted set of pages
  * \(p^{min}_i\) is the full minimum key on a page \(p_i \in ps\).
  * \(p^{max}_i\) is the full maximum key on a page \(p_i \in ps\).
  * \(r \in \left[0, 16\right] \) is the range-finder bit-precision
  * \(\texttt{topBits16}(r, k)\) extracts the \(r\) most significant bits from
    \(k\). We call these \(r\) bits the range-finder bits.
  * \(\texttt{sliceBits32}(r, k)\) extracts the 32 most significant bits from \(k\)
    /after/ \(r\). We call these 32 bits the primary bits.
  * \(c \in \left[32, 48\right] \) is the overall key prefix length represented
     by the compact index.
  * Since we always use 32 primary bits then \(c = r+32\).
  * We choose \(r\) such that \(c = 2~log_2~n\), which keeps the expected
    number of collisions low.
  * \(i \in \left[0, n \right)\), unless stated otherwise
  * \(j \in \left[0, 2^r\right)\), unless stated otherwise
  * Pages must be partitioned: \(\forall p_i \in ps. \texttt{topBits16}(r, p^{min}_i) ~\texttt{==}~ \texttt{topBits16}(r,p^{max}_i) \)

  \[
  \begin{align*}
    RF     :&~ \texttt{Array Word16 PageNo} \\
    RF[j]   =&~ \min~ \{ i \mid j \leq \texttt{topBits16}(r, p^{min}_i) \}  \\
    RF[2^r] =&~ n \\
    \\
    P    :&~ \texttt{Array PageNo Word32} \\
    P[i] =&~ \texttt{sliceBits32}(r, p^{min}_i) \\
    \\
    C    :&~ \texttt{Array PageNo Bit} \\
    C[0] =&~ \texttt{false} \\
    C[i] =&~ \texttt{sliceBits32}(r, p^{max}_{i-1}) ~\texttt{==}~ \texttt{sliceBits32}(r, p^{min}_i) \\
    \\
    TB            :&~ \texttt{Map Key PageNo} \\
    TB(p^{min}_i) =&~
      \begin{cases}
        p^{min}_i        &, \text{if}~ C[i] \land \neg LTP[i] \\
        \text{undefined} &, \text{otherwise} \\
      \end{cases} \\
    \\
    LTP    :&~ \texttt{Array PageNo Bit} \\
    LTP[0] =&~ \texttt{false} \\
    LTP[i] =&~ p^{min}_{i-1} ~\texttt{==}~ p^{min}_i \\
  \end{align*}
  \]

  === An informal description of the search algorithm #search-descr#

  The easiest way to think about the search algorithm is that we start with the
  full interval of page numbers, and shrink it until we get to an interval that
  contains only a single page (or in case of a larger-than-page value, multiple
  pages that have the same minimum key). Assume @k@ is our search key.

  * Use the range-finder bits of our @k@ to index into \(RF\), which should
    provide us with the bounds for a sub-vector of \(P\) to search. In
    particular, all pages in this sub-vector have minimum\/maximum keys that
    start with the same range-finder bits as @k@.
  * Search \(P\) for the vector index @i@ that maps to the largest prim-bits
    value that is smaller or equal to the primary bits of @k@.
  * Check \(C\) if the page corresponding to @i@ is part of a
    clash, if it is not, then we are done!
  * If there is a clash, we go into clash recovery mode. This means we have to
    resolve the ambiguous page boundary using \(TB\). Note, however, that if
    there are multiple clashes in a row, there could be multiple ambiguous page
    boundaries that have to be resolved. We can achieve this using a three-step
    process:

      * Search \(TB\) for the vector index @j1@ that maps to the largest full
        key that is smaller than or equal to @k@.
      * Do a linear search backwards through \(C\) starting from @i@ to find the
        first page @j2@ that is not part of a clash.
      * Take the maximum of @j1@ or @j2@. Consider the two cases where either
        @j1@ or @j2@ is largest (@j1@ can not be equal to @j2@):

          * @j1@ is largest: we resolved ambiguous page boundaries "to the left"
            (toward lower vector indexes) until we resolved an ambiguous page
            boundary "to the right" (toward the current vector index).
          * @j2@ is largest: we resolved ambiguous page boundaries only "to the
            left", and ended up on a page that doesn't have a clash.

  * For larger-than-page values, the steps up until now would only provide us
    with the page where the larger-than-page value starts. We use \(LTP\) to do
    a linear search to find the page where the larger-than-page value ends.

  Convince yourself that clash recovery works without any larger-than-page
  values, and then consider the case where the index does contain
  larger-than-page values. Hints:

  \[
    \begin{align*}
    LTP[i] &~ \implies C[i] \\
    LTP[i] &~ \implies TB(p^{min}_i) = \text{undefined} \\
    \end{align*}
  \]
-}

-- | A compact fence-pointer index for uniformly distributed keys.
--
-- Compact indexes save space by storing bit-prefixes of keys.
--
-- See [a semi-formal description of the compact index](#rep-descr) for more
-- details about the representation.
--
-- While the semi-formal description mentions the number of pages \(n\),
-- we do not store it, as it can be inferred from the length of 'ciPrimary'.
data CompactIndex = CompactIndex {
    -- | \(RF\): A vector that partitions 'ciPrimary' into sub-vectors
    -- containing elements that all share the same range-finder bits.
    ciRangeFinder          :: !(VU.Vector Word32)
    -- | \(m\): Determines the size of 'ciRangeFinder' as
    -- @2 ^ 'ciRangeFinderPrecision' + 1@.
  , ciRangeFinderPrecision :: !Int
    -- | \(P\): Maps a page @i@ to the 32-bit slice of primary bits of its
    -- minimum key.
  , ciPrimary              :: !(VU.Vector Word32)
    -- | \(C\): A clash on page @i@ means that the primary bits of the minimum
    -- key on that page aren't sufficient to decide whether a search for a key
    -- should continue left or right of the page.
  , ciClashes              :: !(VU.Vector Bit)
    -- | \(TB\): Maps a full minimum key to the page @i@ that contains it, but
    -- only if there is a clash on page @i@.
  , ciTieBreaker           :: !(Map SerialisedKey PageNo)
    -- | \(LTP\): Record of larger-than-page values. Given a span of pages for
    -- the larger-than-page value, the first page will map to 'False', and the
    -- remainder of the pages will be set to 'True'. Regular pages default to
    -- 'False'.
  , ciLargerThanPage       :: !(VU.Vector Bit)
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
suggestRangeFinderPrecision :: NumPages -> Int
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

data SearchResult =
    NoResult
  | SinglePage PageNo
  -- | @'Multipage' s e@: the value is larger than a page, starts on page s, and
  -- ends on page e.
  | MultiPage PageNo PageNo
  deriving stock (Show, Eq)

-- | A span of pages, representing an inclusive interval of page numbers.
data PageSpan = PageSpan {
    pageSpanStart :: PageNo
  , pageSpanEnd   :: PageNo
  }

toPageSpan :: SearchResult -> Maybe PageSpan
toPageSpan NoResult        = Nothing
toPageSpan (SinglePage i)  = Just (PageSpan i i)
toPageSpan (MultiPage i j) = assert (i < j) $ Just (PageSpan i j)

-- | Given a search key, find the number of the disk page that /might/ contain
-- this key.
--
-- Note: there is no guarantee that the disk page contains the key. However, it
-- is guaranteed that /no other/ disk page contain the key.
--
-- See [an informal description of the search algorithm](#search-descr) for more
-- details about the search algorithm.
--
-- TODO: optimisation ideas: we should be able to get it all unboxed,
-- non-allocating and without unnecessary bounds checking.
--
-- One way to think of the search algorithm is that it starts with the full page
-- number interval, and shrinks it to a minimal interval that contains the
-- search key. The code below is annotated with @Pre:@ and @Post:@ comments that
-- describe the interval at that point.
search :: SerialisedKey -> CompactIndex -> SearchResult
search k CompactIndex{..} = -- Pre: @[0, V.length ciPrimary)@
    let !rfbits    = fromIntegral $ keyTopBits16 ciRangeFinderPrecision k
        !lb        = fromIntegral $ ciRangeFinder VU.! rfbits
        -- The first page we don't need to look at (exclusive upper bound) is
        -- the one from the next range finder entry.
        -- For the case where we are in the last entry already, we rely on an
        -- extra entry at the end, mapping to 'V.length ciPrimary'.
        !ub        = fromIntegral $ ciRangeFinder VU.! (rfbits + 1)
        -- Post: @[lb, ub)@
        !primbits  = keySliceBits32 ciRangeFinderPrecision k
    in
      case unsafeSearchLEBounds primbits ciPrimary lb ub of
        Nothing -> NoResult -- Post: @[lb, lb)@ (empty).
        Just !i ->         -- Post: @[lb, i]@.
          if unBit $ ciClashes VU.! i then
            -- Post: @[lb, i]@, now in clash recovery mode.
            let i1  = PageNo $ fromJust $
                  bitIndexFromToRev (BoundInclusive lb) (BoundInclusive i) (Bit False) ciClashes
                i2  = maybe (PageNo 0) snd $ Map.lookupLE k ciTieBreaker
                PageNo !i3 = max i1 i2 -- Post: the intersection of @[i1, i]@ and @[i2, i].
                !i4 = bitLongestPrefixFromTo (BoundExclusive i3) (BoundInclusive i) (Bit True) ciLargerThanPage
                      -- Post: [i3, i4]
            in  if i3 == i4 then SinglePage (PageNo i3) else MultiPage (PageNo i3) (PageNo i4)
                -- Post: @[i3, i4]@ if a larger-than-page value that starts at
                -- @i3@ and ends at @i4@, @[i3, i3]@ otherwise for a "normal"
                -- page.
          else  SinglePage (PageNo i) -- Post: @[i, i]@


countClashes :: CompactIndex -> Int
countClashes = Map.size . ciTieBreaker

hasClashes :: CompactIndex -> Bool
hasClashes = not . Map.null . ciTieBreaker

sizeInPages :: CompactIndex -> NumPages
sizeInPages = VU.length . ciPrimary

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

-- | One-shot construction.
fromList :: Int -> Int -> [Append] -> CompactIndex
fromList rfprec maxcsize apps = runST $ do
    mci <- new rfprec maxcsize
    cs <- mapM (`append` mci) apps
    (c, fc) <- unsafeEnd mci
    pure (fromChunks (concat cs ++ toList c) fc)

-- | One-shot construction using only 'appendSingle'.
fromListSingles :: Int -> Int -> [(SerialisedKey, SerialisedKey)] -> CompactIndex
fromListSingles rfprec maxcsize apps = runST $ do
    mci <- new rfprec maxcsize
    cs <- mapM (`appendSingle` mci) apps
    (c, fc) <- unsafeEnd mci
    pure (fromChunks (catMaybes cs ++ toList c) fc)

-- | Feed in 'Chunk's in the same order that they were yielded from incremental
-- construction using 'MCompactIndex'.
fromChunks :: [Chunk] -> FinalChunk -> CompactIndex
fromChunks cs FinalChunk{..} = CompactIndex {
      ciRangeFinder          = fcRangeFinder
    , ciRangeFinderPrecision = fcRangeFinderPrecision
    , ciPrimary              = VU.concat $ fmap cPrimary cs
    , ciClashes              = fcClashes
    , ciTieBreaker           = fcTieBreaker
    , ciLargerThanPage       = fcLargerThanPage
    }

{-------------------------------------------------------------------------------
  Serialisation
-------------------------------------------------------------------------------}

-- | 32 bit aligned.
chunkBuilder :: Chunk -> BB.Builder
chunkBuilder Chunk {..} = putVec32 cPrimary

-- | Must be written after the sequence of 'chunkBuilder' of the corresponding
-- compact index. Specifically, the written chunks must match 'fcNumPages' to
-- get the alignment right.
finalChunkBuilder :: NumEntries -> FinalChunk -> BB.Builder
finalChunkBuilder (NumEntries numEntries) FinalChunk {..} =
       putVec32 fcRangeFinder
    <> (if odd (fcNumPages + VU.length fcRangeFinder)  -- align to 64 bit
        then BB.word32LE 0
        else mempty)
    <> putBitVec fcClashes
    <> putBitVec fcLargerThanPage
    <> putTieBreaker fcTieBreaker
    <> BB.word64LE (fromIntegral fcRangeFinderPrecision)
    <> BB.word64LE (fromIntegral fcNumPages)
    <> BB.word64LE (fromIntegral numEntries)

-- | 32 bit aligned.
--
-- This only produces the correct output on little-endian systems.
--
-- TODO(optimisation): It should be possible to do this without copying the
-- vector. If we ensure pinned allocation of the underlying byte array, we could
-- directly construct a 'ByteString' and serialise that using 'BB.byteString'.
putVec32 :: VU.Vector Word32 -> BB.Builder
putVec32 (VU.V_Word32 (VP.Vector off len ba)) =
    byteArrayFromTo (mul4 off) (mul4 (off + len)) ba

-- | Padded to 64 bit.
--
-- Assumes that the bitvector has a byte-aligned offset.
putBitVec :: VU.Vector Bit -> BB.Builder
putBitVec (BitVec offsetBits lenBits ba)
  | mod8 offsetBits /= 0 = error "putBitVec: not byte aligned"
  | otherwise =
       -- first only write the bytes that are fully part of the bit vec
       byteArrayFromTo offsetBytes offsetLastByte ba
       -- then carefully write the last byte, might be partially uninitialised
    <> (if remainingBits /= 0 then
         BB.word8 (indexByteArray ba offsetLastByte .&. bitmaskLastByte)
       else
         mempty)
    <> putPaddingTo64 totalBytesWritten
  where
    offsetBytes = div8 offsetBits
    offsetLastByte = offsetBytes + div8 lenBits
    totalBytesWritten = ceilDiv8 lenBits

    bitmaskLastByte = unsafeShiftR 0xFF (8 - remainingBits)
    remainingBits = mod8 lenBits

-- | Padded to 64 bit.
putTieBreaker :: Map SerialisedKey PageNo -> BB.Builder
putTieBreaker m =
       BB.word64LE (fromIntegral (Map.size m))
    <> foldMap putEntry (Map.assocs m)
  where
    putEntry :: (SerialisedKey, PageNo) -> BB.Builder
    putEntry (k, PageNo pageNo) =
           BB.word32LE (fromIntegral pageNo)
        <> BB.word32LE (fromIntegral (sizeofKey k))
        <> serialisedKey k
        <> putPaddingTo64 (sizeofKey k)

putPaddingTo64 :: Int -> BB.Builder
putPaddingTo64 written
  | mod8 written == 0 = mempty
  | otherwise         = foldMap BB.word8 (replicate (8 - mod8 written) 0)

{-------------------------------------------------------------------------------
  Deserialisation
-------------------------------------------------------------------------------}

-- | The input bytestring must be 64 bit aligned and exactly contain the
-- serialised compact index, with no leading or trailing space.
-- It is directly used as the backing memory for the compact index.
--
-- Also note that the implementation assumes a little-endian system.
--
-- __NOTE__: Currently does not perform bounds checks. Malformed input can
-- trigger invalid memory access!
fromSBS :: ShortByteString -> Either String (NumEntries, CompactIndex)
fromSBS (SBS ba') = do
    let ba = ByteArray ba'
    when (mod8 (sizeofByteArray ba) /= 0) $ Left "Length is not multiple of 64 bit"

    let arr64 = P.PrimArray ba' :: P.PrimArray Word64
    let len64 = P.sizeofPrimArray arr64

    when (len64 < 3) $ Left "Doesn't contain size information"
    let ciRangeFinderPrecision = fromIntegral (P.indexPrimArray arr64 (len64 - 3))
    let numPages = fromIntegral (P.indexPrimArray arr64 (len64 - 2))
    let numEntries = fromIntegral (P.indexPrimArray arr64 (len64 - 1))
    let numRanges = 2 ^ ciRangeFinderPrecision + 1

    -- offsets in 32 bits
    let (offset2_32, ciPrimary) = getVec32 ba 0 numPages
    let (offset3_32, ciRangeFinder) = getVec32 ba offset2_32 numRanges
    -- offsets in 64 bits
    let offset3 = ceilDiv2 offset3_32
    let (offset4, ciClashes) = getBitVec ba offset3 numPages
    let (offset5, ciLargerThanPage) = getBitVec ba offset4 numPages
    let ciTieBreaker = getTieBreaker ba offset5

    return (NumEntries numEntries, CompactIndex {..})

type Offset32 = Int
type Offset64 = Int

getVec32 :: ByteArray -> Offset32 -> Int -> (Offset32, VU.Vector Word32)
getVec32 ba offset32 numEntries = (offset32 + numEntries, vec)
  where
    vec = VU.V_Word32 (VP.Vector offset32 numEntries ba)

getBitVec :: ByteArray -> Offset64 -> Int -> (Offset64, VU.Vector Bit)
getBitVec ba offset64 numEntries = (offset64', vec)
  where
    vec = BitVec (mul64 offset64) numEntries ba
    offset64' = offset64 + ceilDiv64 numEntries

getTieBreaker :: ByteArray -> Offset64 -> Map SerialisedKey PageNo
getTieBreaker ba = \offset64 ->
    let size = fromIntegral (indexByteArray ba offset64 :: Word64)
    in Map.fromList $ getEntries size (offset64 + 1)
  where
    getEntries :: Int -> Offset64 -> [(SerialisedKey, PageNo)]
    getEntries 0 _         = []
    getEntries !n !offset64 =
        let offset32 = mul2 offset64
            !pageNo = fromIntegral (indexByteArray ba offset32 :: Word32)
            keyLen8 = fromIntegral (indexByteArray ba (offset32 + 1) :: Word32)
            keyOffset8 = mul8 (offset64 + 1)
            -- We avoid retaining references to the bytearray.
            -- Probably not needed, since the bytearray will stay alive as long
            -- as the compact index anyway, but we expect very few keys in the
            -- tie breaker, so it is cheap to do and we don't have to worry
            -- about the issue anymore.
            !key = SerialisedKey' (VP.force (VP.Vector keyOffset8 keyLen8 ba))
        in (key, PageNo pageNo)
         : getEntries (n - 1) (offset64 + 1 + ceilDiv8 keyLen8)

{-------------------------------------------------------------------------------
 Vector extras
-------------------------------------------------------------------------------}

-- | Find the largest element that is smaller or equal to to the given one
-- within the vector interval @[lb, ub)@, and return its vector index.
--
-- Note: this function uses 'unsafeThaw', so all considerations for using
-- 'unsafeThaw' apply to using 'unsafeSearchLEBounds' too.
--
-- PRECONDITION: the vector is sorted in ascending order within the interval
-- @[lb, ub)@.
unsafeSearchLEBounds ::
     (VG.Vector v e, Ord e)
  => e -> v e -> Int -> Int -> Maybe Int -- TODO: return -1?
unsafeSearchLEBounds e vec lb ub = runST $ do
    -- Vector search algorithms work on mutable vectors only.
    vec' <- VG.unsafeThaw vec
    -- @i@ is the first index where @e@ is strictly smaller than the element at
    -- @i@.
    i <- VA.gallopingSearchLeftPBounds (> e) vec' lb ub
    -- The last (and therefore largest) element that is lesser-equal @e@ is
    -- @i-1@. However, if @i==lb@, then the interval @[lb, ub)@ doesn't contain
    -- any elements that are lesser-equal @e@.
    pure $ if i == lb then Nothing else Just (i - 1)

-- | Return the index of the last bit in the vector with the specified value, if
-- any.
--
-- TODO(optimise): optimise by implementing this function similarly to how
-- 'bitIndex' is implemented internally. Another alternative I tried is using
-- the @vector-rotvec@ package and 'V.elemIndex', but 'V.elemIndex' is up to 64x
-- slower than bitIndex.
bitIndexFromToRev :: Bound Int -> Bound Int -> Bit -> VU.Vector Bit -> Maybe Int
bitIndexFromToRev lb ub b v = reverseIx <$> bitIndex b (VU.reverse $ VU.slice lb' (ub' - lb' + 1) v)
  where
    reverseIx x = ub' - x
    lb' = vectorLowerBound lb
    ub' = vectorUpperBound v ub

-- | Like 'bitIndex', but only searches the vector within the give lower and
-- upper bound. Returns the index into the original vector, not the slice.
bitIndexFromTo :: Bound Int -> Bound Int -> Bit -> VU.Vector Bit -> Maybe Int
bitIndexFromTo lb ub b v = shiftIx <$> bitIndex b (VU.slice lb' (ub' - lb' + 1) v)
  where
    shiftIx = (lb'+)
    lb' = vectorLowerBound lb
    ub' = vectorUpperBound v ub

-- | Find the longest prefix of the vector that consists of only bits matching
-- the given value. The return value is the index of the last bit in the prefix.
bitLongestPrefixFromTo :: Bound Int -> Bound Int -> Bit -> Vector Bit -> Int
bitLongestPrefixFromTo lb ub b v = maybe ub' (subtract 1) $ bitIndexFromTo lb ub (toggle b) v
  where
    toggle (Bit x) = Bit (not x)
    ub' = vectorUpperBound v ub

vectorLowerBound :: Bound Int -> Int
vectorLowerBound = \case
    NoBound          -> 0
    BoundExclusive i -> i + 1
    BoundInclusive i -> i

vectorUpperBound :: VG.Vector v a => v a -> Bound Int -> Int
vectorUpperBound v = \case
    NoBound          -> VG.length v - 1
    BoundExclusive i -> i - 1
    BoundInclusive i -> i
