{-# LANGUAGE BangPatterns    #-}
{-# LANGUAGE DeriveFoldable  #-}
{-# LANGUAGE DeriveFunctor   #-}
{-# LANGUAGE NamedFieldPuns  #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ViewPatterns    #-}
{- HLINT ignore "Redundant lambda" -}
{- HLINT ignore "Use camelCase" -}

-- | Incremental, in-memory run consruction
--
module Database.LSMTree.Internal.Run.Construction (
    -- * Incremental, in-memory run construction
    RunAcc
  , new
  , unsafeFinalise
  , addFullKOp
  ) where

import           Control.Exception (assert)
import           Control.Monad.ST.Strict
import           Data.Maybe (fromMaybe)
import           Data.Primitive.PrimVar (PrimVar, modifyPrimVar, newPrimVar,
                     readPrimVar, writePrimVar)
import           Data.Word (Word16)
import           Database.LSMTree.Internal.BlobRef (BlobSpan (..))
import           Database.LSMTree.Internal.Entry (Entry (..), NumEntries (..))
import           Database.LSMTree.Internal.PageAcc (MPageAcc)
import qualified Database.LSMTree.Internal.PageAcc as PageAcc
import qualified Database.LSMTree.Internal.PageAcc1 as PageAcc
import           Database.LSMTree.Internal.RawOverflowPage
import           Database.LSMTree.Internal.RawPage (RawPage)
import           Database.LSMTree.Internal.Run.BloomFilter (Bloom, MBloom)
import qualified Database.LSMTree.Internal.Run.BloomFilter as Bloom
import           Database.LSMTree.Internal.Run.Index.Compact (CompactIndex,
                     NumPages)
import qualified Database.LSMTree.Internal.Run.Index.Compact as Index
import           Database.LSMTree.Internal.Run.Index.Compact.Construction
                     (MCompactIndex)
import qualified Database.LSMTree.Internal.Run.Index.Compact.Construction as Index
import           Database.LSMTree.Internal.Serialise (SerialisedKey,
                     SerialisedValue, keyTopBits16)

{-------------------------------------------------------------------------------
  Incremental, in-memory run construction
-------------------------------------------------------------------------------}

-- | A mutable structure that accumulates k\/op pairs and yields pages.
--
-- Use 'new' to start run construction, add new key\/operation pairs to the run
-- by using 'addFullKOp' and co, and complete run construction using
-- 'unsafeFinalise'.
data RunAcc s = RunAcc {
      mbloom               :: !(MBloom s SerialisedKey)
    , mindex               :: !(MCompactIndex s)
    , mpageacc             :: !(MPageAcc s)
    , entryCount           :: !(PrimVar s Int)
    , rangeFinderCurVal    :: !(PrimVar s Word16)
    , rangeFinderPrecision :: !RangeFinderPrecision
    }

-- TODO: make this a newtype and enforce >=0 && <= 16.
type RangeFinderPrecision = Int

-- | @'new' npages@ starts an incremental run construction.
--
-- @nentries@ and @npages@ should be an upper bound on the expected number of
-- entries and pages in the output run.
new :: NumEntries
    -> NumPages
    -> Maybe RangeFinderPrecision -- ^ For testing: override the default RFP
                                  -- which is based on the 'NumPages'.
    -> ST s (RunAcc s)
new (NumEntries nentries) npages rangeFinderPrecisionOverride = do
    mbloom <- Bloom.newEasy 0.1 nentries -- TODO(optimise): tune bloom filter
    let rangeFinderPrecisionDefault = Index.suggestRangeFinderPrecision npages
        rangeFinderPrecision        = fromMaybe rangeFinderPrecisionDefault
                                                rangeFinderPrecisionOverride
    mindex <- Index.new rangeFinderPrecision 100 -- TODO(optimise): tune chunk size
    mpageacc <- PageAcc.newPageAcc
    entryCount <- newPrimVar 0
    rangeFinderCurVal <- newPrimVar 0
    pure RunAcc{..}

-- | Finalise an incremental run construction. Do /not/ use a 'RunAcc' after
-- finalising it.
--
-- The frozen bloom filter and compact index will be returned, along with the
-- final page of the run (if necessary), and the remaining chunks of the
-- incrementally constructed compact index.
unsafeFinalise ::
     RunAcc s
  -> ST s ( Maybe RawPage
          , Maybe Index.Chunk
          , Bloom SerialisedKey
          , CompactIndex
          , NumEntries
          )
unsafeFinalise racc@RunAcc {..} = do
    mpagemchunk <- flushPageIfNonEmpty racc
    (mchunk', index) <- Index.unsafeEnd mindex
    bloom <- Bloom.unsafeFreeze mbloom
    numEntries <- readPrimVar entryCount
    let !mpage  = fst <$> mpagemchunk
        !mchunk = selectChunk mpagemchunk mchunk'
    pure (mpage, mchunk, bloom, index, NumEntries numEntries)
  where
    selectChunk :: Maybe (RawPage, Maybe Index.Chunk)
                -> Maybe Index.Chunk
                -> Maybe Index.Chunk
    selectChunk (Just (_page, Just _chunk)) (Just _chunk') =
        -- If flushing the page accumulator gives us an index chunk then
        -- the index can't have any more chunks when we finalise the index.
        error "unsafeFinalise: impossible double final chunk"
    selectChunk (Just (_page, Just chunk)) _ = Just chunk
    selectChunk _ (Just chunk)               = Just chunk
    selectChunk _ _                          = Nothing

-- | Add a serialised k\/op pair with an optional blob span. Use only for
-- entries that are fully in-memory. Otherwise, use 'addChunkedKOp'.
addFullKOp ::
     RunAcc s
  -> SerialisedKey
  -> Entry SerialisedValue BlobSpan
  -> ST s ([RawPage], [RawOverflowPage], [Index.Chunk])
addFullKOp racc k e
  | PageAcc.entryWouldFitInPage k e = smallToLarge <$> addSmallKeyOp racc k e
  | otherwise                       =                  addLargeKeyOp racc k e
  where
    smallToLarge :: Maybe (RawPage, Maybe Index.Chunk)
                 -> ([RawPage], [RawOverflowPage], [Index.Chunk])
    smallToLarge Nothing                   = ([],     [], [])
    smallToLarge (Just (page, Nothing))    = ([page], [], [])
    smallToLarge (Just (page, Just chunk)) = ([page], [], [chunk])

addSmallKeyOp
  :: RunAcc s
  -> SerialisedKey
  -> Entry SerialisedValue BlobSpan
  -> ST s (Maybe (RawPage, Maybe Index.Chunk))
addSmallKeyOp racc@RunAcc{..} k e =
  assert (PageAcc.entryWouldFitInPage k e) $ do
    modifyPrimVar entryCount (+1)
    Bloom.insert mbloom k

    -- We have to force a page boundary when the range finder bits change.
    -- This is a constraint from the compact index. To do this we remember the
    -- range finder bits of the previously added key and compare them to the
    -- range finder bits of the current key.
    rfbits <- readPrimVar rangeFinderCurVal             -- previous
    let !rfbits' = keyTopBits16 rangeFinderPrecision k  -- current

    pageBoundaryNeeded <-
      if rfbits' == rfbits
        -- If the range finder bits didn't change, try adding the key/op to
        -- the page accumulator to see if it fits. If it does not fit, a page
        -- boundary is needed.
        then not <$> PageAcc.pageAccAddElem mpageacc k e

        -- If the rfbits did change we do need a page boundary.
        else writePrimVar rangeFinderCurVal rfbits'
          >> return True

    if pageBoundaryNeeded
      then do
        -- We need a page boundary. If the current page is empty then we have
        -- a boundary already, otherwise we need to flush the current page.
        mpagemchunk <- flushPageIfNonEmpty racc
        -- The current page is now empty, either because it was already empty
        -- or because we just flushed it. Adding the new key/op to an empty
        -- page must now succeed, because we know it fits in a page.
        added <- PageAcc.pageAccAddElem mpageacc k e
        assert added $ return mpagemchunk

      else return Nothing

addLargeKeyOp
  :: RunAcc s
  -> SerialisedKey
  -> Entry SerialisedValue BlobSpan -- ^ the full value, not just a prefix
  -> ST s ([RawPage], [RawOverflowPage], [Index.Chunk])
addLargeKeyOp racc@RunAcc{..} k e =
  assert (not (PageAcc.entryWouldFitInPage k e)) $ do
    modifyPrimVar entryCount (+1)
    Bloom.insert mbloom k

    -- If the existing page accumulator is non-empty, we flush it, since the
    -- new large key/op will need more than one page to itself.
    mpagemchunkPre <- flushPageIfNonEmpty racc

    -- Make the new page and overflow pages. Add the span of pages to the index.
    let (page, overflowPages) = PageAcc.singletonPage k e
    chunks <- Index.appendMulti (k, fromIntegral (length overflowPages)) mindex

    -- Combine the results with anything we flushed before
    let (!pages, !chunks') = selectPagesAndChunks mpagemchunkPre page chunks
    return (pages, overflowPages, chunks')

-- | Internal helper: finalise the current page, add the page to the index,
-- reset the page accumulator and return the serialised 'RawPage' along with
-- any index chunk.
--
-- Returns @Nothing@ if the page accumulator was empty.
--
flushPageIfNonEmpty :: RunAcc s -> ST s (Maybe (RawPage, Maybe Index.Chunk))
flushPageIfNonEmpty RunAcc{mpageacc, mindex} = do
    nkeys <- PageAcc.keysCountPageAcc mpageacc
    if nkeys > 0
      then do
        -- Grab the min and max keys, and add the page to the index.
        minKey <- PageAcc.indexKeyPageAcc mpageacc 0
        maxKey <- PageAcc.indexKeyPageAcc mpageacc (nkeys-1)
        mchunk <- Index.appendSingle (minKey, maxKey) mindex

        -- Now serialise the page and reset the accumulator
        page <- PageAcc.serializePageAcc mpageacc
        PageAcc.resetPageAcc mpageacc
        return (Just (page, mchunk))

      else pure Nothing

-- | Internal helper for 'addLargeKeyOp' and 'addLargeSerialisedKeyOp'.
-- Combine the result of 'flushPageIfNonEmpty' with extra pages and index
-- chunks.
--
selectPagesAndChunks :: Maybe (RawPage, Maybe Index.Chunk)
                     -> RawPage
                     -> [Index.Chunk]
                     -> ([RawPage], [Index.Chunk])
selectPagesAndChunks mpagemchunkPre page chunks =
  case mpagemchunkPre of
    Nothing                       -> (         [page],          chunks)
    Just (pagePre, Nothing)       -> ([pagePre, page],          chunks)
    Just (pagePre, Just chunkPre) -> ([pagePre, page], chunkPre:chunks)

