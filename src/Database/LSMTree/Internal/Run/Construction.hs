{-# LANGUAGE RecordWildCards #-}
{- HLINT ignore "Redundant lambda" -}
{- HLINT ignore "Use camelCase" -}

-- | Incremental, in-memory run consruction
--
module Database.LSMTree.Internal.Run.Construction (
    -- * Incremental, in-memory run construction
    MRun
  , new
  , unsafeFinalise
  , addFullKOp
  , addChunkedKOp
    -- * Pages
  , putPageAcc
    -- **  Page accumulator
  , PageAcc
  , paIsEmpty
  , paIsOverfull
  , paEmpty
  , paAddElem
  , paSingleton
  ) where

import           Control.Monad (forM_, unless)
import           Control.Monad.Primitive (PrimMonad (PrimState))
import           Control.Monad.ST.Strict
import           Data.Maybe (fromMaybe)
import           Data.STRef
import qualified Data.Vector.Primitive as P
import           Data.Word (Word16, Word32, Word64)
import           Database.LSMTree.Internal.Entry (RawEntry, onBlobRef, onValue)
import           Database.LSMTree.Internal.RawPage.Mutable (MRawPage,
                     putEntries)
import           Database.LSMTree.Internal.Run.BloomFilter (Bloom, MBloom)
import qualified Database.LSMTree.Internal.Run.BloomFilter as Bloom
import           Database.LSMTree.Internal.Run.Index.Compact (CompactIndex,
                     MCompactIndex)
import qualified Database.LSMTree.Internal.Run.Index.Compact as Index
import           Database.LSMTree.Internal.Serialise (SerialisedKey (..),
                     copySerialisedKey, sizeofKey64, topBits16)

{-------------------------------------------------------------------------------
  Incremental, in-memory run construction
-------------------------------------------------------------------------------}

-- | A mutable structure that accumulates k\/op pairs and yields pages.
--
-- Use 'new' to start run construction, add new key\/operation pairs to the run
-- by using 'addFullKOp' and co, and complete run construction using
-- 'unsafeFinalise'.
data MRun s = MRun {
      mbloom               :: !(MBloom s SerialisedKey)
    , mindex               :: !(MCompactIndex s)
    , indexChunksRef       :: !(STRef s [[Index.Chunk]])
    , currentPageRef       :: !(STRef s PageAcc)
    , rangeFinderPrecision :: !Int
    }

-- | @'new' npages@ starts an incremental run construction.
--
-- @nentries@ and @npages@ should be an upper bound on the expected number of
-- entries and pages in the output run.
new :: Int -> Int -> ST s (MRun s)
new nentries npages = do
    mbloom <- Bloom.newEasy 0.1 nentries -- TODO(optimise): tune bloom filter
    let rangeFinderPrecision = Index.suggestRangeFinderPrecision npages
    mindex <- Index.new rangeFinderPrecision 100 -- TODO(optimise): tune chunk size
    indexChunksRef <- newSTRef []
    currentPageRef <- newSTRef paEmpty
    pure MRun{..}

-- | Finalise an incremental run construction. Do /not/ use an 'MRun' after
-- finalising it.
--
-- The frozen bloom filter and compact index will be returned, along with the
-- final page of the run (if necessary), and the remaining chunks of the
-- incrementally constructed compact index.
unsafeFinalise ::
     MRun s
  -> ST s ( Maybe (PageAcc, [Index.Chunk])
          , Maybe Index.Chunk
          , Index.FinalChunk
          , Bloom SerialisedKey
          , CompactIndex
          )
unsafeFinalise mrun@MRun {..} = do
    mpage <- yield mrun
    (mchunk, fchunk) <- Index.unsafeEnd mindex
    storeChunk mrun mchunk
    bloom <- Bloom.freeze mbloom
    allChunks <- getAllChunks mrun
    let index = Index.fromChunks allChunks fchunk
    pure (mpage, mchunk, fchunk, bloom, index)

-- | Add a serialised k\/op pair with an optional blob reference. Use only for
-- entries that are fully in-memory. Otherwise, use 'addMultiPageInChunks'.
addFullKOp ::
     MRun s
  -> SerialisedKey
  -> RawEntry
  -> ST s (Maybe (PageAcc, [Index.Chunk]))
addFullKOp  mrun@MRun{..}k e = do
    Bloom.insert mbloom k
    p <- readSTRef currentPageRef
    case paAddElem rangeFinderPrecision k e p of
      Nothing -> do
        (pb, cs) <- unsafeYieldAlways mrun
        writeSTRef currentPageRef $! paSingleton k e
        pure $ Just (pb, cs)
      Just p' -> do
        writeSTRef currentPageRef $! p'
        pure Nothing

-- | __For multi-page values only during run merging:__ add the first chunk of a
-- serialised k\/op pair with an optional blob reference. The caller of this
-- function is responsible for copying the remaining chunks to the output file.
--
-- This will throw an impure exception if the result of adding the k\/op pair is
-- not exactly the size of a disk page.
--
-- During a merge, raw values that span multiple pages may not be read into
-- memory as a whole, but in chunks. If you have access to the full bytes of a
-- raw value, use 'addFullKOp' instead.
addChunkedKOp ::
     MRun s
  -> SerialisedKey
  -> RawEntry
  -> Word32
  -> ST s (Either PageAcc (PageAcc, PageAcc), [Index.Chunk])
addChunkedKOp mrun@MRun{..} k e nOverflow
  | pageSizeNumBytes (psSingleton k e) == 4096 = do
      mp <- yield mrun

      Bloom.insert mbloom k
      cs' <- Index.append (Index.AppendMultiPage (copySerialisedKey k) nOverflow) mindex -- TODO: copying of the key is only necessary when we store it as a tie-breaker
      storeChunks mrun cs'
      let p' = paSingleton k e

      case mp of
        Nothing      -> pure (Left p', cs')
        Just (p, cs) -> pure (Right (p, p'), cs' <> cs)
  | otherwise = error "addMultiPage: expected a page of 4096 bytes"

-- | Yield the current page if it is non-empty. New chunks are recorded in the
-- mutable run, and the current page is set to empty.
yield :: MRun s -> ST s (Maybe (PageAcc, [Index.Chunk]))
yield mrun@MRun{..} = do
    p <- readSTRef currentPageRef
    if paIsEmpty p then
      pure Nothing
    else do
      cs <- Index.append (unsafeMkAppend p) mindex
      storeChunks mrun cs
      writeSTRef currentPageRef $! paEmpty
      pure $ Just (p, cs)

-- | Like 'yield', but can fail if the page is empty.
unsafeYieldAlways :: MRun s -> ST s (PageAcc, [Index.Chunk])
unsafeYieldAlways mrun@MRun{..} = do
    p <- readSTRef currentPageRef
    cs <- Index.append (unsafeMkAppend p) mindex
    storeChunks mrun cs
    writeSTRef currentPageRef $! paEmpty
    pure (p, cs)

storeChunk :: MRun s -> Maybe Index.Chunk -> ST s ()
storeChunk MRun {..} mc = forM_ mc $ \c ->
    modifySTRef indexChunksRef $ \css -> [c] : css

storeChunks :: MRun s -> [Index.Chunk] -> ST s ()
storeChunks MRun{..} cs = unless (null cs) $
    modifySTRef indexChunksRef $ \css -> cs : css

-- | Get all currently stored chunks, from oldest to newest
getAllChunks :: MRun s -> ST s [Index.Chunk]
getAllChunks MRun{..} = do
    chunkss <- readSTRef indexChunksRef
    pure $ concat (reverse chunkss)

{-------------------------------------------------------------------------------
  Page builder
-------------------------------------------------------------------------------}

putPageAcc :: PrimMonad m => MRawPage (PrimState m) -> PageAcc -> m ()
putPageAcc mp pacc = putEntries mp (pageEntries pacc)

{-------------------------------------------------------------------------------
  Accumulator for page contents
-------------------------------------------------------------------------------}

data PageAcc = PageAcc {
    pageSize       :: !PageSize
  , pageEntries    :: [(SerialisedKey, RawEntry)]
  , pageLastRFBits :: !(Maybe Word16)
  }
  deriving Show

paIsEmpty :: PageAcc -> Bool
paIsEmpty p = psIsEmpty (pageSize p)

paIsOverfull :: PageAcc -> Bool
paIsOverfull p = psIsOverfull (pageSize p)

paEmpty :: PageAcc
paEmpty = PageAcc {
      pageSize       = psEmpty
    , pageEntries    = []
    , pageLastRFBits = Nothing
    }

paAddElem ::
     Int -- ^ Range-finder precision
  -> SerialisedKey
  -> RawEntry
  -> PageAcc
  -> Maybe PageAcc
paAddElem rangeFinderPrecision k e PageAcc{..}
  | Just pgsz' <- psAddElem k e pageSize
  , Just rfbits == pageLastRFBits
  = Just $ PageAcc {
        pageSize    = pgsz'
      , pageEntries = (k, e) : pageEntries
      , pageLastRFBits = Just $! rfbits
      }
  | otherwise = Nothing
  where
    rfbits = topBits16 rangeFinderPrecision k

paSingleton :: SerialisedKey -> RawEntry -> PageAcc
paSingleton k e = fromMaybe (error err) $
    -- range-finder precision only matters when the page is non-empty, so any
    -- value would suffice, so we pick 0.
    paAddElem 0 k e paEmpty
  where
    err = "Failed to add k/op pair to an empty page, but this should have \
          \worked! Are you sure the implementation of paAddElem is correct?"

unsafeMkAppend :: PageAcc -> Index.Append
unsafeMkAppend p = case pageEntries p of
    []                          -> error "mkAppend: empty list"
    [(k, _)] | numBytes <= 4096 -> Index.AppendSinglePage k k
             | otherwise        -> Index.AppendMultiPage  k (fromIntegral $ (numBytes - 1) `quot` 4096)
    ks                          -> Index.AppendSinglePage (fst $ last ks) (fst $ head ks)
  where
    numBytes = pageSizeNumBytes $ pageSize p

{-------------------------------------------------------------------------------
  PageSize
-------------------------------------------------------------------------------}

-- See "FormatPage"
data PageSize = PageSize {
    pageSizeNumElems :: !Word16
  , pageSizeNumBlobs :: !Word16
  , pageSizeNumBytes :: !Word64
  }
  deriving (Eq, Show)

psEmpty :: PageSize
psEmpty = PageSize 0 0 10

psIsEmpty :: PageSize -> Bool
psIsEmpty ps = ps == psEmpty

psIsOverfull :: PageSize -> Bool
psIsOverfull ps = pageSizeNumBytes ps >= 4096

psAddElem :: SerialisedKey -> RawEntry -> PageSize -> Maybe PageSize
psAddElem k e (PageSize n b sz)
  | sz' <= 4096 || n' == 1 = Just $! PageSize n' b' sz'
  | otherwise              = Nothing
  where
    n' = n+1
    b' | onBlobRef False (const True) e = b+1
       | otherwise                      = b
    sz' = sz
        + (if n `mod` 64 == 0 then 8 else 0)    -- (2) blobrefs bitmap
        + (if n `mod` 32 == 0 then 8 else 0)    -- (3) operations bitmap
        + onBlobRef 0 (const 12) e              -- (4) blobref entry
        + 2                                     -- (5) key offsets
        + (case n of { 0 -> 4; 1 -> 0; _ -> 2}) -- (6) value offsets
        + sizeofKey64 k                         -- (7) key bytes
        + onValue 0 (fromIntegral . P.length) e -- (8) value bytes

psSingleton :: SerialisedKey -> RawEntry -> PageSize
psSingleton k e = fromMaybe (error err) $ psAddElem k e psEmpty
  where
    err = "Failed to add k/op pair to an empty page, but this should have \
          \worked! Are you sure the implementation of psAddElem is correct?"
