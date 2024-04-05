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
    -- * Pages
    -- ** Page Builder
  , pageBuilder
    -- **  Page accumulator
  , PageAcc
  , paIsEmpty
  , paIsOverfull
  , paEmpty
  , paAddElem
  , paSingleton
    -- ** Page size
  , PageSize(..)
  , psEmpty
  , psIsEmpty
  , psIsOverfull
  , psAddElem
  , psSingleton
    -- * Exposed only for testing
    -- ** StricterList
  , StricterList (..)
    -- ** BitMap
  , BitMap (..)
  , Bit
  , emptyBitMap
  , appendBit
    -- ** CrumbMap
  , CrumbMap (..)
  , Crumb
  , emptyCrumbMap
  , appendCrumb
  ) where

import           Control.Exception (assert)
import           Control.Monad.ST.Strict
import           Data.Bits (Bits (..))
import qualified Data.ByteString.Builder as BB
import           Data.Foldable (Foldable (..))
import           Data.Maybe (fromMaybe)
import           Data.Monoid (Dual (..))
import           Data.Primitive.PrimVar (PrimVar, modifyPrimVar, newPrimVar,
                     readPrimVar, writePrimVar)
import           Data.Word (Word16, Word32, Word64, Word8)
import           Database.LSMTree.Internal.BlobRef (BlobSpan (..))
import           Database.LSMTree.Internal.Entry (Entry (..), NumEntries (..),
                     onBlobRef, onValue)
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
                     SerialisedValue, keyTopBits16, serialisedKey,
                     serialisedValue, sizeofKey16, sizeofKey64, sizeofValue16,
                     sizeofValue32, sizeofValue64)

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
    , rangeFinderPrecision :: !Int
    }

-- | @'new' npages@ starts an incremental run construction.
--
-- @nentries@ and @npages@ should be an upper bound on the expected number of
-- entries and pages in the output run.
new :: NumEntries -> NumPages -> ST s (RunAcc s)
new (NumEntries nentries) npages = do
    mbloom <- Bloom.newEasy 0.1 nentries -- TODO(optimise): tune bloom filter
    let rangeFinderPrecision = Index.suggestRangeFinderPrecision npages
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

{-------------------------------------------------------------------------------
  Page builder
-------------------------------------------------------------------------------}

-- | A builder representing one or more disk pages.
--
-- Typically, this builder only represents a single page, unless it is the
-- result of adding a larger-than-page value to a run, which made the page
-- contents exceed the size of a single disk page.
--
-- The string of bytes is padded to the target disk-page size (4K).
--
-- This builder could be used to serialise into a strict bytestring, lazy
-- bytestring, or into existing buffers.
pageBuilder :: PageAcc -> BB.Builder
pageBuilder PageAcc{..} =
    -- (1) directory of components
       BB.word16LE pageSizeNumElems
    <> BB.word16LE pageSizeNumBlobs
    <> BB.word16LE offKeyOffsets
    <> BB.word16LE 0
    -- (2) an array of 1-bit blob span indicators
    <> dfoldMap BB.word64LE (bmbits pageBlobSpanBitmap)
    -- (3) an array of 2-bit operation types
    <> dfoldMap BB.word64LE (cmbits pageOperations)
    -- (4) a pair of arrays of blob spans
    <> dfoldMap BB.word64LE pageBlobSpanOffsets
    <> dfoldMap BB.word32LE pageBlobSpanSizes
    -- (5) an array of key offsets
    <> dfoldMap BB.word16LE pageKeyOffsets
    -- (6) an array of value offsets
    <> case pageValueOffsets of
          Left   offsets -> dfoldMap BB.word16LE offsets
          Right (offset1, offset2) -> BB.word16LE offset1
                                  <> BB.word32LE offset2
    -- (7) the concatenation of all keys
    <> dfoldMap serialisedKey pageKeys
    -- (8) the concatenation of all values
    <> dfoldMap (maybe mempty serialisedValue) pageValues
    -- padding
    <> fold (replicate (fromIntegral paddingBytes) (BB.word8 0))
  where
    dfoldMap f = getDual . foldMap (Dual . f)

    PageSize{..} = pageSize

    n = pageSizeNumElems
    b = pageSizeNumBlobs

    -- Offset to component (5)
    offKeyOffsets =
         8                                         -- size of (1)
      + (n + 63) `shiftR` 3 .&. complement 0x7     -- size of (2)
      + (2 * n + 63) `shiftR` 3 .&. complement 0x7 -- size of (3)
      + (4 + 8) * b                                -- size of (4)

    -- Offset to component (7)
    offKeys =
        offKeyOffsets
      + 2 * n                             -- size of (5)
      + (if n == 1 then 6 else 2 * (n+1)) -- size of (6)

    -- Thes values start right after the keys,
    (pageKeyOffsets, offValues) =
        case scanr (\k o -> o + sizeofKey16 k) offKeys (unStricterList pageKeys) of
          []         -> error "impossible"
          (vo : kos) -> (kos, vo)

    pageValueOffsets = case unStricterList pageValues of
      [v] -> Right (offValues, fromIntegral offValues + maybe 0 sizeofValue32 v)
      vs  -> Left (scanr (\v o -> o + maybe 0 sizeofValue16 v) offValues vs)

    paddingBytes :: Word64
    paddingBytes | bytesRemaining == 0 = 0
                 | otherwise           = 4096 - bytesRemaining
      where bytesRemaining = pageSizeNumBytes `rem` 4096

{-------------------------------------------------------------------------------
  Accumulator for page contents
-------------------------------------------------------------------------------}

-- We might have preferred a representation for an incrementally constructed
-- page that already places the raw bytes in a page-sized buffer. However, we do
-- not know a priori what the offsets for most of the page components will be.
-- These offsets depend on the number of keys\/values\/blob references, and the
-- sizes of keys\/values, and we only find out these numbers as key-operation
-- pairs are added incrementally. As such, we can only construct the raw page
-- bytes once the current page has filled up.
--
-- TODO: an alternative representation that would put less load on the GC would
-- be to use mutable vectors for each component. Where we currently only copy
-- bytes once (from input to the output page), we would have to copy each byte
-- one extra time to the intermediate mutable vectors. However, it would save
-- having to allocate and de-allocate heap objects, like lists. We wouldn't know
-- a priori exactly how large those vectors should be, but we know they are
-- bounded, though special care should be taken for multi-page values.
data PageAcc = PageAcc {
    -- | (1) directory of components
    pageSize            :: !PageSize
    -- | (2) an array of 1-bit blob span indicators
  , pageBlobSpanBitmap  :: !BitMap
    -- | (3) an array of 2-bit operation types
  , pageOperations      :: !CrumbMap
    -- | (4) a pair of arrays of blob spans
  , pageBlobSpanOffsets :: !(StricterList Word64)
    -- | (4) a pair of arrays of blob spans, ctd
  , pageBlobSpanSizes   :: !(StricterList Word32)
    --   (5) key offsets will be computed when serialising the page
    --   (6) value offsets will be computed when serialising the page
    -- | (7) the concatenation of all keys
  , pageKeys            :: !(StricterList SerialisedKey)
    -- | (8) the concatenation of all values
  , pageValues          :: !(StricterList (Maybe SerialisedValue))
  }
  deriving (Show, Eq)

paIsEmpty :: PageAcc -> Bool
paIsEmpty p = psIsEmpty (pageSize p)

paIsOverfull :: PageAcc -> Bool
paIsOverfull p = psIsOverfull (pageSize p)

paEmpty :: PageAcc
paEmpty = PageAcc {
      pageSize            = psEmpty
    , pageBlobSpanBitmap  = emptyBitMap
    , pageOperations      = emptyCrumbMap
    , pageBlobSpanOffsets = SNil
    , pageBlobSpanSizes   = SNil
    , pageKeys            = SNil
    , pageValues          = SNil
    }

paAddElem ::
     Int -- ^ Range-finder precision
  -> SerialisedKey
  -> Entry SerialisedValue BlobSpan
  -> PageAcc
  -> Maybe PageAcc
paAddElem rangeFinderPrecision k e PageAcc{..}
  | Just pgsz' <- psAddElem k e pageSize
  , partitioned
  = Just $ PageAcc {
        pageSize            = pgsz'
      , pageBlobSpanBitmap  = pageBlobSpanBitmap'
      , pageOperations      = pageOperations'
      , pageBlobSpanOffsets = onBlobRef pageBlobSpanOffsets ((`SCons` pageBlobSpanOffsets ) . blobSpanOffset) e
      , pageBlobSpanSizes   = onBlobRef pageBlobSpanSizes ((`SCons` pageBlobSpanSizes) . blobSpanSize) e
      , pageKeys            = k `SCons` pageKeys
      , pageValues          = onValue Nothing Just e `SCons` pageValues
      }
  | otherwise = Nothing
  where
    partitioned = case unStricterList pageKeys of
        []     -> True
        k' : _ -> keyTopBits16 rangeFinderPrecision k == keyTopBits16 rangeFinderPrecision k'

    pageBlobSpanBitmap' = appendBit (onBlobRef 0 (const 1) e) pageBlobSpanBitmap
    pageOperations'    = appendCrumb (entryToCrumb e) pageOperations

    entryToCrumb Insert{}         = 0
    entryToCrumb InsertWithBlob{} = 0
    entryToCrumb Mupdate{}        = 1
    entryToCrumb Delete{}         = 2

paSingleton :: SerialisedKey -> Entry SerialisedValue BlobSpan -> PageAcc
paSingleton k e = fromMaybe (error err) $
    -- range-finder precision only matters when the page is non-empty, so any
    -- value would suffice, so we pick 0.
    paAddElem 0 k e paEmpty
  where
    err = "Failed to add k/op pair to an empty page, but this should have \
          \worked! Are you sure the implementation of paAddElem is correct?"

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

psAddElem ::
     SerialisedKey
  -> Entry SerialisedValue BlobSpan
  -> PageSize
  -> Maybe PageSize
psAddElem k e (PageSize n b sz)
  | sz' <= 4096 || n' == 1 = Just $! PageSize n' b' sz'
  | otherwise              = Nothing
  where
    n' = n+1
    b' | onBlobRef False (const True) e = b+1
       | otherwise                      = b
    sz' = sz
        + (if n `mod` 64 == 0 then 8 else 0)    -- (2) blobspans bitmap
        + (if n `mod` 32 == 0 then 8 else 0)    -- (3) operations bitmap
        + onBlobRef 0 (const 12) e              -- (4) blobspan entry
        + 2                                     -- (5) key offsets
        + (case n of { 0 -> 4; 1 -> 0; _ -> 2}) -- (6) value offsets
        + sizeofKey64 k                         -- (7) key bytes
        + onValue 0 sizeofValue64 e             -- (8) value bytes

psSingleton :: SerialisedKey -> Entry SerialisedValue BlobSpan -> PageSize
psSingleton k e = fromMaybe (error err) $ psAddElem k e psEmpty
  where
    err = "Failed to add k/op pair to an empty page, but this should have \
          \worked! Are you sure the implementation of psAddElem is correct?"

{-------------------------------------------------------------------------------
  StricterList
-------------------------------------------------------------------------------}

newtype StricterList a = StricterList { unStricterList :: [a] }
  deriving (Show, Eq, Functor, Foldable)

{-# COMPLETE SNil, SCons #-}

pattern SNil :: StricterList a
pattern SNil = StricterList []

pattern SCons :: a -> StricterList a -> StricterList a
pattern SCons x xs <- StricterList (x : (StricterList -> xs))
  where SCons !x (StricterList !xs) = StricterList (x : xs)

ssing :: a -> StricterList a
ssing !x = StricterList [x]

{-------------------------------------------------------------------------------
  BitMap
-------------------------------------------------------------------------------}

type Bit = Word8
data BitMap = BitMap { bmlen :: !Int, bmbits :: !(StricterList Word64) }
  deriving (Show, Eq)

emptyBitMap :: BitMap
emptyBitMap = BitMap 0 SNil

appendBit :: Bit -> BitMap -> BitMap
appendBit b BitMap{..} = assert (b < 2) $ BitMap len' $ case bmbits of
    SNil              -> assert (bmlen == 0) $ ssing (fromIntegral b)
    w64 `SCons` w64s' | i <- bmlen `rem` 64, i > 0
                      -> if b == 0
                         then bmbits
                         else setBit w64 i `SCons` w64s'
                      | otherwise -- the current Word64 is full, so start a new one
                      -> fromIntegral b `SCons` bmbits
  where
    len' = bmlen+1


{-------------------------------------------------------------------------------
  CrumbMap
-------------------------------------------------------------------------------}

-- https://en.wikipedia.org/wiki/Units_of_information#Crumb
type Crumb    = Word8
data CrumbMap = CrumbMap { cmlen :: !Int, cmbits :: !(StricterList Word64) }
  deriving (Show, Eq)

emptyCrumbMap :: CrumbMap
emptyCrumbMap = CrumbMap 0 SNil

appendCrumb :: Crumb -> CrumbMap -> CrumbMap
appendCrumb c CrumbMap{..} = assert (c < 4) $ CrumbMap len' $ case cmbits of
    SNil              -> assert (cmlen == 0) $ ssing (fromIntegral c)
    w64 `SCons` w64s' | i <- cmlen `rem` 32, i > 0
                      -> if c == 0
                         then cmbits
                         else setCrumb w64 i (fromIntegral c) `SCons` w64s'
                      | otherwise -- the current Word64 is full, so start a new one
                      -> fromIntegral c `SCons` cmbits
  where
    len' = cmlen+1

setCrumb :: Bits a => a -> Int -> a -> a
setCrumb x i y = x .|. crumb i y

crumb :: Bits a => Int -> a -> a
crumb i y = y `shiftL` (i `shiftL` 1)
