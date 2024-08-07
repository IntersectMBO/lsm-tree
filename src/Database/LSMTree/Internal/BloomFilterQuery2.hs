{-# LANGUAGE MagicHash           #-}
{-# LANGUAGE PatternSynonyms     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE UnboxedTuples       #-}

{-# OPTIONS_GHC -O2 -fregs-iterative -fmax-inline-alloc-size=512 #-}
-- The use of -fregs-iterative here does a better job in the hot loop for
-- the bloomQueriesBody below. It eliminates all spilling to the stack.
-- There's just 6 stack reads in the loop now, no writes.

-- | An implementation of batched bloom filter query, optimised for memory
-- prefetch.
module Database.LSMTree.Internal.BloomFilterQuery2 (
  bloomQueriesDefault,
  RunIxKeyIx(RunIxKeyIx),
  RunIx, KeyIx,
  -- $algorithm
  -- * Internals exposed for tests
  CandidateProbe (..),
) where

import           Prelude hiding (filter)

import           Data.Bits
import qualified Data.Primitive as P
import qualified Data.Vector as V
import qualified Data.Vector.Primitive as VP
import           Data.Word (Word64)

import           Control.Exception (assert)
import           Control.Monad.ST (ST, runST)

import           GHC.Exts (Int#, uncheckedIShiftL#, (+#))

import           Data.BloomFilter (Bloom)
import qualified Data.BloomFilter as Bloom
import qualified Data.BloomFilter.BitVec64 as BV64
import qualified Data.BloomFilter.Hash as Bloom
import qualified Data.BloomFilter.Internal as BF
import           Database.LSMTree.Internal.BloomFilterQuery1 (RunIxKeyIx (..))
import           Database.LSMTree.Internal.Serialise (SerialisedKey)
import qualified Database.LSMTree.Internal.StrictArray as P
import qualified Database.LSMTree.Internal.Vector as P

-- Bulk query
-----------------------------------------------------------

type KeyIx = Int
type RunIx = Int

-- $algorithm
--
-- == Key algorithm concepts
--
-- There is almost no opportunity for memory prefetching when looking up a
-- single key in a single Bloom filter. So this is a bulk algorithm, to do a
-- lot of work all in one go, which does create the opportunity to prefetch
-- memory. It also provides an opportunity to share key hashes across filters.
--
-- We have as inputs N bloom filters and M keys to look up in them. So overall
-- there are N * M independent bloom filter tests. The result is expected to be
-- sparse, with roughly M*(1+FPR) positive results. So we use a sparse
-- representation for the result: pairs of indexes identifying input Bloom
-- filters and input keys with a positive test result.
--
-- We aim for the number of memory operations in-flight simultaneously to be on
-- the order of 32. This trades-off memory parallelism with cache use. In
-- particular this means the algorithm must be able to use a fixed prefetch
-- \"distance\" rather than this being dependent on the input sizes. To achieve
-- this, we use a fixed size circular buffer (queue). The buffer size can be
-- tuned to optimise the prefetch behaviour: indeed we pick exactly 32.
--
-- == Micro optimisation
--
-- We use primitive arrays and arrays -- rather than vectors -- so as to
-- minimise the number of function variables, to be able to keep most things in
-- registers. Normal vectors use two additional variables, which triples
-- register pressure.
--
-- We use an Array of Strict BloomFilter. This avoids the test for WHNF in the
-- inner loop, which causes all registered to be spilled to and restored from
--the stack.
--

type Candidate = RunIxKeyIx

-- | A candidate probe point is the combination of a filter\/run index,
-- key index and hash number. This combination determines the probe location
-- in the bloom filter, which is also cached.
--
-- We store these in 'PrimArray's as a pair of 64bit words:
--
-- * Low 64bit word:
--   - 16 bits padding (always 0s)    (MSB)
--   - 16 bits for the hash number
--   - 16 bits for the filter index
--   - 16 bits for the key index      (LSB)
--
-- * High 64bit word: FilterBitIx
--
data CandidateProbe = MkCandidateProbe !Word64 !Word64

type HashNo = Int
type FilterBitIx = Int

instance P.Prim CandidateProbe where
    sizeOfType# _ = 16#
    alignmentOfType# _ = 8#

    indexByteArray# ba i =
      MkCandidateProbe
        (P.indexByteArray# ba (indexLo i))
        (P.indexByteArray# ba (indexHi i))
    readByteArray# ba i s1 =
        case P.readByteArray# ba (indexLo i) s1 of { (# s2, lo #) ->
        case P.readByteArray# ba (indexHi i) s2 of { (# s3, hi #) ->
        (# s3, MkCandidateProbe lo hi #)
        }}
    writeByteArray# ba i (MkCandidateProbe lo hi) s =
        P.writeByteArray# ba (indexHi i) hi
       (P.writeByteArray# ba (indexLo i) lo s)

    indexOffAddr# ba i =
      MkCandidateProbe
        (P.indexOffAddr# ba (indexLo i))
        (P.indexOffAddr# ba (indexHi i))
    readOffAddr# ba i s1 =
        case P.readOffAddr# ba (indexLo i) s1 of { (# s2, lo #) ->
        case P.readOffAddr# ba (indexHi i) s2 of { (# s3, hi #) ->
        (# s3, MkCandidateProbe lo hi #)
        }}
    writeOffAddr# ba i (MkCandidateProbe lo hi) s =
        P.writeOffAddr# ba (indexHi i) hi
       (P.writeOffAddr# ba (indexLo i) lo s)

indexLo :: Int# -> Int#
indexLo i = uncheckedIShiftL# i 1#

indexHi :: Int# -> Int#
indexHi i = uncheckedIShiftL# i 1# +# 1#

pattern CandidateProbe :: RunIx
                       -> KeyIx
                       -> HashNo
                       -> FilterBitIx
                       -> CandidateProbe
pattern CandidateProbe r k h p <- (unpackCandidateProbe -> (r, k, h, p))
  where
    CandidateProbe r k h p = packCandidateProbe r k h p
{-# INLINE CandidateProbe #-}
{-# COMPLETE CandidateProbe #-}

unpackCandidateProbe :: CandidateProbe -> (Int, Int, Int, Int)
unpackCandidateProbe (MkCandidateProbe lo hi) =
    ( fromIntegral ((lo `unsafeShiftR` 16) .&. 0xffff) -- run ix
    , fromIntegral ( lo                    .&. 0xffff) -- key ix
    , fromIntegral (lo `unsafeShiftR` 32)            -- hashno
    , fromIntegral  hi
    )
{-# INLINE unpackCandidateProbe #-}

packCandidateProbe :: Int -> Int -> Int -> Int -> CandidateProbe
packCandidateProbe r k h p =
    assert (r >= 0 && r <= 0xffff) $
    assert (k >= 0 && k <= 0xffff) $
    assert (h >= 0 && h <= 0xffff) $
    MkCandidateProbe
      (   fromIntegral r `unsafeShiftL` 16  -- run ix
      .|. fromIntegral k                    -- key ix
      .|. fromIntegral h `unsafeShiftL` 32) -- hashno
      (fromIntegral p)
{-# INLINE packCandidateProbe #-}

instance Show CandidateProbe where
  showsPrec _ (CandidateProbe r k h p) =
      showString "CandidateProbe "
    . showsPrec 11 r
    . showChar ' '
    . showsPrec 11 k
    . showChar ' '
    . showsPrec 11 h
    . showChar ' '
    . showsPrec 11 p



bloomQueriesDefault ::
     V.Vector (Bloom SerialisedKey)
  -> V.Vector SerialisedKey
  -> VP.Vector RunIxKeyIx
bloomQueriesDefault filters keys | V.null filters || V.null keys
                                 = VP.empty
bloomQueriesDefault filters keys =
  assert (all BF.bloomInvariant filters) $
  -- in particular the bloomInvariant checks the size is > 0
  runST $ do
    let !keyhashes = prepKeyHashes keys
        !filters'  = P.vectorToStrictArray filters
    candidateProbes <- P.newPrimArray 0x20
    -- We deliberately do not clear this array as it will get overwritten.
    -- But for debugging, it's less confusing to initialise to all zeros.
    -- To do that, uncomment the following line:
    --P.setPrimArray 0 0x20 candidateProbes (MkCandidateProbe 0 0)

    let i_r = 0
    (i_w, nextCandidate) <-
      prepInitialCandidateProbes
        filters' keyhashes
        candidateProbes
        0 (RunIxKeyIx 0 0)

    output <- bloomQueriesBody filters' keyhashes candidateProbes
                               i_r i_w nextCandidate
    return $! P.primArrayToPrimVector output

prepKeyHashes :: V.Vector SerialisedKey
              -> P.PrimArray (Bloom.CheapHashes SerialisedKey)
prepKeyHashes keys =
    P.generatePrimArray (V.length keys) $ \i ->
      Bloom.makeCheapHashes (V.unsafeIndex keys i)

prepInitialCandidateProbes
  :: P.StrictArray (Bloom SerialisedKey)
  -> P.PrimArray (Bloom.CheapHashes SerialisedKey)
     -- ^ The pre-computed \"cheap hashes\" of the keys.
  -> P.MutablePrimArray s CandidateProbe
     -- ^ The array of candidates: of FilterIx, KeyIx and HashNo. This is
     -- used as a fixed size rolling buffer.
  -> Int
     -- ^ The write index within the rolling buffer. This wraps around.
  -> RunIxKeyIx
     -- ^ The last candidate added to the rolling buffer.
  -> ST s (Int,  RunIxKeyIx)
prepInitialCandidateProbes
    filters keyhashes
    candidateProbes i_w
    nextCandidate@(RunIxKeyIx rix kix)

  | i_w < 0x1f && rix < P.sizeofStrictArray filters = do
    -- We prepare at most 31 (not 32) candidates, in the buffer of size 32.
    -- This is because we define the buffer to be empty when i_r == i_w. Hence
    -- we cannot fill all entries in the array.

    let !filter  = P.indexStrictArray filters rix
        !keyhash = P.indexPrimArray keyhashes kix
        !hn      = BF.hashesN filter - 1
        !bix     = (fromIntegral :: Word64 -> Int) $
                   Bloom.evalCheapHashes keyhash hn
                     `BV64.unsafeRemWord64` -- size must be > 0
                   BF.size filter           -- bloomInvariant ensures this
    BV64.prefetchIndex (BF.bitArray filter) bix
    assert ((rix, kix, hn, bix) == unpackCandidateProbe (CandidateProbe rix kix hn bix)) $ return ()
    P.writePrimArray candidateProbes i_w (CandidateProbe rix kix hn bix)
    prepInitialCandidateProbes
      filters keyhashes
      candidateProbes
      (i_w+1)
      (succCandidate keyhashes nextCandidate)

    -- Filled the buffer or ran out of candidates
  | otherwise = return (i_w, nextCandidate)


{-# NOINLINE bloomQueriesBody #-}
bloomQueriesBody
  :: forall s.
     P.StrictArray (Bloom SerialisedKey)
  -> P.PrimArray (Bloom.CheapHashes SerialisedKey)
     -- ^ The pre-computed \"cheap hashes\" of the keys.
  -> P.MutablePrimArray s CandidateProbe
     -- ^ The array of candidates: of FilterIx, KeyIx, HashNo and FilterBitIx.
     -- This is used as a fixed size rolling buffer. It is used to communicate
     -- between iterations. On each iteration it is read to do the (already
     -- prefetched) memory reads. And it is written with the prefetched bit indexes for the next iteration.
  -> Int -> Int -> RunIxKeyIx
  -> ST s (P.PrimArray RunIxKeyIx)
bloomQueriesBody !filters !keyhashes !candidateProbes =
   \ !i_r !i_w !nextCandidate -> do
    output <- P.newPrimArray (P.sizeofPrimArray keyhashes * 2)
    output' <-
      testCandidateProbe
        i_r i_w nextCandidate
        output 0
    P.unsafeFreezePrimArray output'
  where
    {-# INLINE prepGivenCandidateProbe #-}

    -- assume buff size of 0x20, so mask of 0x1f
    testCandidateProbe, prepNextCandidateProbe
      :: Int -> Int
         -- ^ The read and write indexes within the rolling buffer. These wrap
         -- around.
      -> RunIxKeyIx
         -- ^ The run and key index of the next candidate add to the rolling buffer.
         -- When this passes the maximum then no more candidates are added, and we
         -- just drain the buffer.
      -> P.MutablePrimArray s RunIxKeyIx
         -- ^ The output array.
      -> Int
         -- ^ The output array next free index.
      -> ST s (P.MutablePrimArray s RunIxKeyIx)

    testCandidateProbe !i_r !i_w
                       !nextCandidate
                       !output !outputix =
      assert (i_r >= 0 && i_r <= 0x1f) $
      assert (i_w >= 0 && i_w <= 0x1f) $ do
        -- A candidate probe is the combination of a filter, key, hash no and
        -- the corresponding probe location in the bloom filter. When a candidate
        -- probe is prepared, its probe location is computed and prefetched.
        candidateProbe <- P.readPrimArray candidateProbes i_r
        let CandidateProbe rix kix hn bix = candidateProbe

        -- Now test the actual Bloom filter bit to see if it's set or not.
        let filter = P.indexStrictArray filters rix
        if BV64.unsafeIndex (BF.bitArray filter) bix

          -- The Bloom filter bit is set. Now we either keep this candidate but
          -- probe the next hash number, or if the hash number reached zero then
          -- this candidate (run index, key index) pair goes into the output, and
          -- we prepare the next candidate.
          then
            if hn == 0
              then do
                -- Output the candidate
                P.writePrimArray output outputix (RunIxKeyIx rix kix)
                outputsz <- P.getSizeofMutablePrimArray output
                output'  <- if outputix+1 < outputsz
                              then return output
                              else P.resizeMutablePrimArray output (outputsz * 2)
                prepNextCandidateProbe
                  i_r i_w nextCandidate
                  output' (outputix+1)

              else do
                -- The hashno has not reached zero, so we prepare a new candidate
                -- with the same (filter,key) pair, but with the next hashno.
                prepGivenCandidateProbe
                  i_r i_w nextCandidate
                  output outputix
                  rix kix (hn-1) filter

          -- The Bloom filter bit is not set, so abandon this filter,key pair and
          -- prepare the next candidate.
          else prepNextCandidateProbe
                 i_r i_w nextCandidate
                 output outputix

    prepNextCandidateProbe !i_r !i_w nextCandidate@(RunIxKeyIx rix kix)
                           !output !outputix
        -- There is a next candidate. Write it into the rolling buffer at the write
        -- index, and continue with an incremented buffer read and write index.
      | rix < P.sizeofStrictArray filters =
        let filter         = P.indexStrictArray filters rix
            hn             = BF.hashesN filter - 1
            nextCandidate' = succCandidate keyhashes nextCandidate
         in prepGivenCandidateProbe
              i_r i_w nextCandidate'
              output outputix
              rix kix hn filter

        -- There is no next candidate but the candidate buffer is non-empty.
        -- Don't write into the candidate buffer. Continue with incrementing the
        -- buffer read pointer but not the write pointer.
      | ((i_r + 1) .&. 0x1f) /= i_w =
          testCandidateProbe
            ((i_r + 1) .&. 0x1f)
              i_w
            nextCandidate
            output outputix

        -- There is no next candidate and the candidate buffer is empty.
        -- We're done!
      | otherwise =
          P.resizeMutablePrimArray output outputix

    prepGivenCandidateProbe
      :: Int -> Int
      -> RunIxKeyIx
      -> P.MutablePrimArray s RunIxKeyIx
      -> Int
      -> Int -> Int -> Int
      -> Bloom.Bloom SerialisedKey
      -> ST s (P.MutablePrimArray s RunIxKeyIx)
    prepGivenCandidateProbe !i_r !i_w
                            !nextCandidate
                            !output !outputix
                            !rix !kix !hn !filter =
      assert (hn >= 0 && hn < BF.hashesN filter) $ do
        let !keyhash = P.indexPrimArray keyhashes kix
            !bix     = (fromIntegral :: Word64 -> Int) $
                       Bloom.evalCheapHashes keyhash hn
                         `BV64.unsafeRemWord64` -- size must be > 0
                       BF.size filter           -- bloomInvariant ensures this
        BV64.prefetchIndex (BF.bitArray filter) bix
        P.writePrimArray candidateProbes i_w (CandidateProbe rix kix hn bix)
        testCandidateProbe
            ((i_r + 1) .&. 0x1f)
            ((i_w + 1) .&. 0x1f)
            -- or if we merge them: ((i_rw + 0x0101) .&. 0x1f1f)
            nextCandidate
            output outputix

succCandidate :: P.PrimArray (Bloom.CheapHashes SerialisedKey)
              -> Candidate
              -> Candidate
succCandidate keyhashes (RunIxKeyIx rix kix)
  | kix+1 < P.sizeofPrimArray keyhashes = RunIxKeyIx rix (kix+1)
  | otherwise                           = RunIxKeyIx (rix+1) 0
  --TODO: optimise with bit twiddling

