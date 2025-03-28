{-# OPTIONS_HADDOCK not-home #-}

-- | A compact fence-pointer index for uniformly distributed keys.
--
-- Keys used with a compact index must be at least 8 bytes long.
--
-- TODO: add utility functions for clash probability calculations
--
module Database.LSMTree.Internal.Index.Compact (
    -- $compact
    IndexCompact (..)
    -- * Queries
  , search
  , sizeInPages
  , countClashes
  , hasClashes
    -- * Non-incremental serialisation
  , toLBS
    -- * Incremental serialisation
  , headerLBS
  , finalLBS
  , word64VectorToChunk
    -- * Deserialisation
  , fromSBS
  ) where

import           Control.DeepSeq (NFData (..))
import           Control.Monad (when)
import           Control.Monad.ST
import           Data.Bit hiding (flipBit)
import           Data.Bits (unsafeShiftR, (.&.))
import qualified Data.ByteString.Builder as BB
import qualified Data.ByteString.Builder.Extra as BB
import qualified Data.ByteString.Lazy as LBS
import           Data.ByteString.Short (ShortByteString (..))
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import           Data.Maybe (fromMaybe)
import           Data.Primitive.ByteArray (ByteArray (..), indexByteArray,
                     sizeofByteArray)
import           Data.Primitive.Types (sizeOf)
import qualified Data.Vector.Algorithms.Search as VA
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Primitive as VP
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Base as VU
import           Data.Word
import           Database.LSMTree.Internal.BitMath
import           Database.LSMTree.Internal.ByteString (byteArrayFromTo)
import           Database.LSMTree.Internal.Chunk (Chunk (Chunk))
import qualified Database.LSMTree.Internal.Chunk as Chunk (toByteString)
import           Database.LSMTree.Internal.Entry (NumEntries (..))
import           Database.LSMTree.Internal.Map.Range (Bound (..))
import           Database.LSMTree.Internal.Page
import           Database.LSMTree.Internal.Serialise
import           Database.LSMTree.Internal.Unsliced
import           Database.LSMTree.Internal.Vector

{- $compact

  A fence-pointer index is a mapping of disk pages, identified by some number
  @i@, to min-max information for keys on that page.

  Fence-pointer indexes can be constructed and serialised incrementally, see
  module "Database.LSMTree.Internal.Index.CompactAcc".

  Given a serialised target key @k@, an index can be 'search'ed to find a disk
  page @i@ that /might/ contain @k@. Fence-pointer indices offer no guarantee of
  whether the page contains the key, but the indices do guarantee that no page
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
  returns the corresponding vector index as a page number. A design choice of
  ours is that the search will __always__ return a page number, even if the
  index could answer that the key is definitely not in a page (see
  'indexSearches').

  > type Index1 k = V.Vector (k, k)
  >
  > mkIndex1 :: Run k v -> Index1 k
  > mkIndex1 = V.fromList . fmap (\p -> (minKey p, maxKey p))
  >
  > search1 :: k -> Index1 k -> PageNo
  > search1 = -- elided

  We can reduce the memory size of `Index1` by half if we store only the minimum
  keys on each page. As such, the index now stores the key-interval @[minKey p,
  minKey p')@ for each page @p@ and successor page @p'@. GUARANTEE is still
  guaranteed, because the old intervals are strictly contained in the new ones.
  @search2@ searches the vector for the largest key smaller or equal to the
  given one, if it exists, and returns the corresponding vector index as a page
  number.

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
  each other. The probability of the @n@ most significant bits of two
  independently generated hashes matching is @(1/(2^n))@, and if we pick @n@
  large enough then we can expect a very small number of collisions. More
  generally, the expected number of @n@-bit hashes that have to be generated
  before a collision is observed is @2^(n/2)@, see [the birthday
  problem](https://en.wikipedia.org/wiki/Birthday_problem#Probability_of_a_shared_birthday_(collision).
  Or, phrased differently, if we store only the @n@ most significant bits of
  independently generated hashes in our index, then we can store up to @2^(n/2)@
  of those hashes before the expected number of collisions becomes one. Still,
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
  record of clashes can be implemented simply as a single bit per page: with
  a @True@ bit meaning a clash with the previous page. Note therefore that the
  first page's bit is always going to be @False@. We store the full keys using
  a 'Map'. It is ok that this is not a compact representation, because we
  expect to store full keys for only a very small number of pages.

  The example below shows a simplified view of the compact index implementation
  so far. As an example, we store the @64@ most significant bits of each minimum
  key in the @primary@ index, the record of clashes is called @clashes@, and the
  @IntMap@ is named the @tieBreaker@ map. @search3@ can at any point during the
  search, consult @clashes@ and @tieBreaker@ to /break ties/.

  > --              (primary         , clashes      , tieBreaker)
  > type Index3 k = (VU.Vector Word64, VU.Vector Bit, IntMap k  )
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
  For example, storing the first 64 bits of 100 million SHA256 hashes reduces
  memory size from \(256 n\) bits to \(64 n + n\) bits, because the expected
  number of collisions is smaller than 1.

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
  * \(\texttt{topBits64}(k)\) extracts the \(64\) most significant bits from
    \(k\). We call these \(64\) bits the primary bits.
  * \(i \in \left[0, n \right)\), unless stated otherwise

  \[
  \begin{align*}
    P    :&~ \texttt{Array PageNo Word64} \\
    P[i] =&~ \texttt{topBits64}(p^{min}_i) \\
    \\
    C    :&~ \texttt{Array PageNo Bit} \\
    C[0] =&~ \texttt{false} \\
    C[i] =&~ \texttt{topBits64}(p^{max}_{i-1}) ~\texttt{==}~ \texttt{topBits64}(p^{min}_i) \\
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
-- we do not store it, as it can be inferred from the length of 'icPrimary'.
data IndexCompact = IndexCompact {
    -- | \(P\): Maps a page @i@ to the 64-bit slice of primary bits of its
    -- minimum key.
    icPrimary        :: {-# UNPACK #-} !(VU.Vector Word64)
    -- | \(C\): A clash on page @i@ means that the primary bits of the minimum
    -- key on that page aren't sufficient to decide whether a search for a key
    -- should continue left or right of the page.
  , icClashes        :: {-# UNPACK #-} !(VU.Vector Bit)
    -- | \(TB\): Maps a full minimum key to the page @i@ that contains it, but
    -- only if there is a clash on page @i@.
  , icTieBreaker     :: !(Map (Unsliced SerialisedKey) PageNo)
    -- | \(LTP\): Record of larger-than-page values. Given a span of pages for
    -- the larger-than-page value, the first page will map to 'False', and the
    -- remainder of the pages will be set to 'True'. Regular pages default to
    -- 'False'.
  , icLargerThanPage :: {-# UNPACK #-} !(VU.Vector Bit)
  }
  deriving stock (Show, Eq)

instance NFData IndexCompact where
  rnf ic = rnf a `seq` rnf b `seq` rnf c `seq` rnf d
    where IndexCompact a b c d = ic

{-------------------------------------------------------------------------------
  Queries
-------------------------------------------------------------------------------}

{-|
    For a specification of this operation, see the documentation of [its
    type-agnostic version]('Database.LSMTree.Internal.Index.search').

    See [an informal description of the search algorithm](#search-descr) for
    more details about the search algorithm.
-}
search :: SerialisedKey -> IndexCompact -> PageSpan
-- One way to think of the search algorithm is that it starts with the full page
-- number interval, and shrinks it to a minimal interval that contains the
-- search key. The code below is annotated with [x,y] or [x, y) comments that
-- describe the known page number interval at that point in the search
-- algorithm.
search k IndexCompact{..} =
    let !primbits = keyTopBits64 k in
    -- [0, n), where n is the length of the P array
    case unsafeSearchLE primbits icPrimary of
      Nothing ->
        -- TODO: if the P array is indeed empty, then this violates the
        -- guarantee that we return a valid page span! We should specify that a
        -- compact index should be non-empty.
        if VU.length icLargerThanPage == 0 then singlePage (PageNo 0) else
        -- [0, n), our page span definitely starts at 0, but we still have to
        -- consult the LTP array to check whether the value on page 0 overflows
        -- into subsequent pages.
        let !i = bitLongestPrefixFromTo (BoundExclusive 0) NoBound (Bit True) icLargerThanPage
        -- [0, i]
        in  multiPage (PageNo 0) (PageNo i)
      Just !i ->
        -- [0, i]
        if unBit $ icClashes VU.! i then
          -- [0, i], now in clash recovery mode.
          let -- i is the *last* index in a range of contiguous pages that all
              -- clash. Since i is the end of the range, we search backwards
              -- through the C array to find the start of this range.
              !i1 = PageNo $ fromMaybe 0 $
                bitIndexFromToRev (BoundInclusive 0) (BoundInclusive i) (Bit False) icClashes
              -- The TB map is consulted to find the closest key smaller than k.
              !i2 = maybe (PageNo 0) snd $
                Map.lookupLE (unsafeNoAssertMakeUnslicedKey k) icTieBreaker
              -- If i2 < i1, then it means the clashing pages were all just part
              -- of the same larger-than-page value. Entries are only included
              -- in the TB map if the clash was a *proper* clash.
              --
              -- If i1 <= i2, then there was a proper clash in [i1, i] that
              -- required a comparison with a tiebreaker key.
              PageNo !i3 = max i1 i2
              -- [max i1 i2, i], this is equivalent to taking the intersection
              -- of [i1, i] and [i2, i]
              !i4 = bitLongestPrefixFromTo (BoundExclusive i3) (BoundInclusive i) (Bit True) icLargerThanPage
          in  multiPage (PageNo i3) (PageNo i4)
              -- [i3, i4], we consulted the LTP array to check whether the value
              -- on page i3 overflows into subsequent pages
        else
          -- [i, i], there is no clash with the previous page and so this page
          -- is also not part of a large value that spans multiple pages.
          singlePage (PageNo i)


countClashes :: IndexCompact -> Int
countClashes = Map.size . icTieBreaker

hasClashes :: IndexCompact -> Bool
hasClashes = not . Map.null . icTieBreaker

{-|
    For a specification of this operation, see the documentation of [its
    type-agnostic version]('Database.LSMTree.Internal.Index.sizeInPages').
-}
sizeInPages :: IndexCompact -> NumPages
sizeInPages = NumPages . toEnum . VU.length . icPrimary

{-------------------------------------------------------------------------------
  Non-incremental serialisation
-------------------------------------------------------------------------------}

-- | Serialises a compact index in one go.
toLBS :: NumEntries -> IndexCompact -> LBS.ByteString
toLBS numEntries index =
     headerLBS
  <> LBS.fromStrict (Chunk.toByteString (word64VectorToChunk (icPrimary index)))
  <> finalLBS numEntries index

{-------------------------------------------------------------------------------
  Incremental serialisation
-------------------------------------------------------------------------------}

-- | By writing out the type–version indicator in host endianness, we also
-- indicate endianness. During deserialisation, we would discover an endianness
-- mismatch.
supportedTypeAndVersion :: Word32
supportedTypeAndVersion = 0x0001

{-|
    For a specification of this operation, see the documentation of [its
    type-agnostic version]('Database.LSMTree.Internal.Index.headerLBS').
-}
headerLBS :: LBS.ByteString
headerLBS =
    -- create a single 4 byte chunk
    BB.toLazyByteStringWith (BB.safeStrategy 4 BB.smallChunkSize) mempty $
      BB.word32Host supportedTypeAndVersion <> BB.word32Host 0

{-|
    For a specification of this operation, see the documentation of [its
    type-agnostic version]('Database.LSMTree.Internal.Index.finalLBS').
-}
finalLBS :: NumEntries -> IndexCompact -> LBS.ByteString
finalLBS (NumEntries numEntries) IndexCompact {..} =
    -- use a builder, since it is all relatively small
    BB.toLazyByteString $
         putBitVec icClashes
      <> putBitVec icLargerThanPage
      <> putTieBreaker icTieBreaker
      <> BB.word64Host (fromIntegral numPages)
      <> BB.word64Host (fromIntegral numEntries)
  where
    numPages = VU.length icPrimary

-- | Constructs a chunk containing the contents of a vector of 64-bit words.
word64VectorToChunk :: VU.Vector Word64 -> Chunk
word64VectorToChunk (VU.V_Word64 (VP.Vector off len ba)) =
    Chunk (mkPrimVector (mul8 off) (mul8 len) ba)

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
putTieBreaker :: Map (Unsliced SerialisedKey) PageNo -> BB.Builder
putTieBreaker m =
       BB.word64Host (fromIntegral (Map.size m))
    <> foldMap putEntry (Map.assocs m)
  where
    putEntry :: (Unsliced SerialisedKey, PageNo) -> BB.Builder
    putEntry (fromUnslicedKey -> k, PageNo pageNo) =
           BB.word32Host (fromIntegral pageNo)
        <> BB.word32Host (fromIntegral (sizeofKey k))
        <> serialisedKey k
        <> putPaddingTo64 (sizeofKey k)

putPaddingTo64 :: Int -> BB.Builder
putPaddingTo64 written
  | mod8 written == 0 = mempty
  | otherwise         = foldMap BB.word8 (replicate (8 - mod8 written) 0)

{-------------------------------------------------------------------------------
  Deserialisation
-------------------------------------------------------------------------------}

{-|
    For a specification of this operation, see the documentation of [its
    type-agnostic version]('Database.LSMTree.Internal.Index.fromSBS').
-}
fromSBS :: ShortByteString -> Either String (NumEntries, IndexCompact)
fromSBS (SBS ba') = do
    let ba = ByteArray ba'
    let len8 = sizeofByteArray ba
    when (mod8 len8 /= 0) $ Left "Length is not multiple of 64 bit"
    when (len8 < 24) $ Left "Doesn't contain header and footer"

    -- check type and version
    let typeAndVersion = indexByteArray ba 0 :: Word32
    when (typeAndVersion == byteSwap32 supportedTypeAndVersion)
         (Left "Non-matching endianness")
    when (typeAndVersion /= supportedTypeAndVersion)
         (Left "Unsupported type or version")

    -- read footer
    let len64 = div8 len8
    let getPositive off64 = do
          let w = indexByteArray ba off64 :: Word64
          when (w > fromIntegral (maxBound :: Int)) $
            Left "Size information is too large for Int"
          return (fromIntegral w)

    numPages <- getPositive (len64 - 2)
    numEntries <- getPositive (len64 - 1)

    -- read vectors
    -- offsets in 64 bits
    let off1_64 = 1  -- after type–version indicator
    (!off2_64, icPrimary) <- getVec64 "Primary array" ba off1_64 numPages
    -- offsets in 64 bits
    let !off3 = off2_64
    (!off4, icClashes) <- getBitVec "Clash bit vector" ba off3 numPages
    (!off5, icLargerThanPage) <- getBitVec "LTP bit vector" ba off4 numPages
    (!off6, icTieBreaker) <- getTieBreaker ba off5

    let bytesUsed = mul8 (off6 + 2)
    when (bytesUsed > sizeofByteArray ba) $
      Left "Byte array is too small for components"
    when (bytesUsed < sizeofByteArray ba) $
      Left "Byte array is too large for components"

    return (NumEntries numEntries, IndexCompact {..})

type Offset32 = Int
type Offset64 = Int

getVec64 ::
     String -> ByteArray -> Offset32 -> Int
  -> Either String (Offset64, VU.Vector Word64)
getVec64 name ba off64 numEntries =
    case checkedPrimVec off64 numEntries ba of
      Nothing  -> Left (name <> " is out of bounds")
      Just vec -> Right (off64 + numEntries, VU.V_Word64 vec)

getBitVec ::
     String -> ByteArray -> Offset64 -> Int
  -> Either String (Offset64, VU.Vector Bit)
getBitVec name ba off numEntries =
    case checkedBitVec (mul64 off) numEntries ba of
      Nothing  -> Left (name <> " is out of bounds")
      Just vec -> Right (off + ceilDiv64 numEntries, vec)

-- | Checks bounds.
--
-- Inefficient, but okay for a small number of entries.
getTieBreaker ::
     ByteArray -> Offset64
  -> Either String (Offset64, Map (Unsliced SerialisedKey) PageNo)
getTieBreaker ba = \off -> do
    when (mul8 off >= sizeofByteArray ba) $
      Left "Tie breaker is out of bounds"
    let size = fromIntegral (indexByteArray ba off :: Word64)
    (off', pairs) <- go size (off + 1) []
    return (off', Map.fromList pairs)
  where
    go :: Int -> Offset64 -> [(Unsliced SerialisedKey, PageNo)]
       -> Either String (Offset64, [(Unsliced SerialisedKey, PageNo)])
    go 0 off pairs = return (off, pairs)
    go n off pairs = do
        when (mul8 off >= sizeofByteArray ba) $
          Left "Clash map entry is out of bounds"
        let off32 = mul2 off
        let !pageNo = fromIntegral (indexByteArray ba off32 :: Word32)
        let keyLen8 = fromIntegral (indexByteArray ba (off32 + 1) :: Word32)

        (off', key) <- getKey (off + 1) keyLen8
        go (n - 1) off' ((key, PageNo pageNo) : pairs)

    getKey :: Offset64 -> Int -> Either String (Offset64, Unsliced SerialisedKey)
    getKey off len8 = do
        let off8 = mul8 off
        -- We avoid retaining references to the bytearray.
        -- Probably not needed, since the bytearray will stay alive as long as
        -- the compact index anyway, but we expect very few keys in the tie
        -- breaker, so it is cheap and we don't have to worry about it any more.
        !key <- case checkedPrimVec off8 len8 ba of
          Nothing  -> Left ("Clash map key is out of bounds")
          Just vec -> Right (SerialisedKey' vec)
        return (off + ceilDiv8 len8, makeUnslicedKey key)

-- | Offset and length are in number of elements.
checkedPrimVec :: forall a.
  VP.Prim a => Int -> Int -> ByteArray -> Maybe (VP.Vector a)
checkedPrimVec off len ba
  | off >= 0, sizeOf (undefined :: a) * (off + len) <= sizeofByteArray ba =
      Just (mkPrimVector off len ba)
  | otherwise =
      Nothing

-- | Offset and length are in number of bits.
--
-- We can't use 'checkedPrimVec' here, since 'Bool' and 'Bit' are not 'VP.Prim'
-- (so the bit vector type doesn't use 'VP.Vector' under the hood).
checkedBitVec :: Int -> Int -> ByteArray -> Maybe (VU.Vector Bit)
checkedBitVec off len ba
  | off >= 0, off + len <= mul8 (sizeofByteArray ba) =
      Just (BitVec off len ba)
  | otherwise =
      Nothing

{-------------------------------------------------------------------------------
 Vector extras
-------------------------------------------------------------------------------}

-- | Find the largest vector element that is smaller or equal to to the given
-- one, and return its vector index.
--
-- Note: this function uses 'unsafeThaw', so all considerations for using
-- 'unsafeThaw' apply to using 'unsafeSearchLE' too.
--
-- PRECONDITION: the vector is sorted in ascending order.
unsafeSearchLE ::
     (VG.Vector v e, Ord e)
  => e -> v e ->  Maybe Int -- TODO: return -1?
unsafeSearchLE e vec = runST $ do
    -- Vector search algorithms work on mutable vectors only.
    vec' <- VG.unsafeThaw vec
    -- @i@ is the first index where @e@ is strictly smaller than the element at
    -- @i@.
    i <- VA.gallopingSearchLeftP (> e) vec'
    -- The last (and therefore largest) element that is lesser-equal @e@ is
    -- @i-1@. However, if @i==lb@, then the interval @[lb, ub)@ doesn't contain
    -- any elements that are lesser-equal @e@.
    pure $ if i == 0 then Nothing else Just (i - 1)

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
