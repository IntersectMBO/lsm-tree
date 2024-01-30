{-# LANGUAGE BangPatterns    #-}
{-# LANGUAGE DeriveFoldable  #-}
{-# LANGUAGE DeriveFunctor   #-}
{-# LANGUAGE MagicHash       #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE ViewPatterns    #-}
{- HLINT ignore "Redundant lambda" -}
{- HLINT ignore "Use camelCase" -}

-- | Incremental, in-memory run consruction
--
module Database.LSMTree.Internal.Run.Construction (
    -- * Incremental, in-memory run construction
    MRun
  , new
  , finalise
    -- ** Adding k\/op pairs
  , Add
  , add
  , addMultiPageInChunks
    -- * Types
  , RawValue (RawValue, EmptyRawValue)
  , BlobRef (..)
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
import           Control.Monad (unless)
import           Control.Monad.ST.Strict
import           Data.Bits (Bits (..))
import qualified Data.ByteString.Builder as BB
import           Data.ByteString.Short.Internal (ShortByteString (SBS))
import           Data.Foldable (Foldable (..))
import           Data.Maybe (fromMaybe)
import           Data.Monoid (Dual (..))
import           Data.Primitive.ByteArray (ByteArray (..), sizeofByteArray)
import           Data.STRef
import           Data.Word (Word16, Word32, Word64, Word8)
import           Database.LSMTree.Internal.Entry (Entry (..), onBlobRef,
                     onValue)
import           Database.LSMTree.Internal.Run.BloomFilter (Bloom, MBloom)
import qualified Database.LSMTree.Internal.Run.BloomFilter as Bloom
import           Database.LSMTree.Internal.Run.Index.Compact (Chunk,
                     CompactIndex, MCompactIndex)
import qualified Database.LSMTree.Internal.Run.Index.Compact as Index
import           Database.LSMTree.Internal.Serialise (SerialisedKey,
                     serialisedKey, shortByteStringFromTo, sizeofKey16,
                     sizeofKey64, topBits16)

{------------------------------------------------------------------------------
  TODO: placeholders until #55 is merged
-------------------------------------------------------------------------------}

data Append =
    -- | One or more keys are in this page, and their values fit within a single
    -- page.
    AppendSinglePage SerialisedKey SerialisedKey
    -- | There is only one key in this page, and it's value does not fit within
    -- a single page.
  | AppendMultiPage SerialisedKey Word32 -- ^ Number of overflow pages

-- | Append a new page entry to a mutable compact index.
append :: Append -> MCompactIndex s -> ST s [Chunk]
append = error "append: replace by Database.LSMTree.Internal.Run.Index.Compact.append"

mkAppend :: PageAcc -> Append
mkAppend p = case unStricterList $ pageKeys p of
    []                     -> error "mkAppend: empty list"
    [k] | numBytes <= 4096 -> AppendSinglePage k k
        | otherwise        -> AppendMultiPage  k (fromIntegral $ numBytes `rem` 4096 - 1)
    ks                     -> AppendSinglePage (last ks) (head ks)
  where
    numBytes = pageSizeNumBytes $ pageSize p

{-------------------------------------------------------------------------------
  Incremental, in-memory run construction
-------------------------------------------------------------------------------}

-- | A mutable structure that keeps track of incremental, in-memory run
-- construction.
--
-- Use 'new' to start run construction, add new key\/operation pairs to the run
-- by using 'add' and co, and complete run construction using 'finalise'.
data MRun s = MRun {
      mbloom               :: !(MBloom s SerialisedKey)
    , mindex               :: !(MCompactIndex s)
      -- | Used to construct the full CompactIndex when run construction is
      -- completed.
    , indexChunksRef       :: !(STRef s [[Index.Chunk]]) -- TODO: make stricter
    , currentPageRef       :: !(STRef s PageAcc)
    , rangeFinderPrecision :: !Int
    }

-- | @'new' npages@ starts an incremental run construction.
--
-- @npages@ should be an upper bound on the number of pages that will be yielded
-- by incremental run construction.
new :: Int -> ST s (MRun s)
new npages = do
    mbloom <- Bloom.newEasy 0.1 npages -- TODO(optimise): tune bloom filter
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
finalise ::
     MRun s
  -> ST s ( Maybe BB.Builder
          , Index.Chunk
          , Index.FinalChunk
          , Bloom SerialisedKey
          , CompactIndex
          )
finalise mrun@MRun {..} = do
    p <- readSTRef currentPageRef
    mlastPage <-
      if paIsEmpty p then
        pure Nothing
      else do
        newChunks <- append (mkAppend p) mindex
        storeChunks mrun newChunks
        pure $ Just $ pageBuilder p
    (chunk, fchunk) <- Index.unsafeEnd mindex
    bloom <- Bloom.freeze mbloom
    chunkss <- readSTRef indexChunksRef
    let chunks = concat (reverse ([chunk] : chunkss))
        index = Index.fromChunks chunks fchunk
    pure (mlastPage, chunk, fchunk, bloom, index)

{-------------------------------------------------------------------------------
  Adding k\/op pairs
-------------------------------------------------------------------------------}

type Add s = MRun s -> ST s (Maybe (BB.Builder, [Index.Chunk]))

-- | Add a serialised k\/op pair with an optional blob reference.
--
-- Use only for full values.
add :: SerialisedKey -> Entry RawValue BlobRef -> Add s
add k e mrun@MRun{..} = do
    Bloom.insert mbloom k
    p <- readSTRef currentPageRef
    case paAddElem rangeFinderPrecision k e p of
      Nothing -> do
        (pb, cs) <- yieldAlways mrun
        let p' = paSingleton k e
        writeSTRef currentPageRef $! p'
        pure $ Just (pb, cs)
      Just p' -> do
        writeSTRef currentPageRef $! p'
        pure Nothing

-- | __For multi-page values only during run merging:__ add the first chunk of a
-- serialised k\/op pair with an optional blob reference. The caller of this
-- function is responsible for copying the remaining chunks to the output file.
--
-- Will throw an error if the result of adding the k\/op pair is not exactly the
-- size of a disk page.
--
-- During a merge, raw values that span multiple pages may not be read into memory
-- as a whole, but in chunks. If you have access to the full bytes of a raw
-- value, use 'add' and co instead.
addMultiPageInChunks :: SerialisedKey -> Entry RawValue BlobRef -> Word32 -> MRun s -> ST s (BB.Builder, [Index.Chunk])
addMultiPageInChunks k e nOverflow mrun@MRun{..}
  | pageSizeNumBytes (psSingleton k e) == 4096 = do
      prev <- yield mrun

      Bloom.insert mbloom k
      cs' <- append (AppendMultiPage k nOverflow) mindex
      storeChunks mrun cs'
      let p'  = paSingleton k e
          pb' = pageBuilder p'

      case prev of
        Nothing       -> pure (pb', cs')
        Just (pb, cs) -> pure (pb <> pb', cs' <> cs)
  | otherwise = error "addMultiPage: expected exactly 4096 bytes"

-- | Yield the current page if it is non-empty. New chunks are recorded in the
-- mutable run, and the current page is set to empty.
yield :: MRun s -> ST s (Maybe (BB.Builder, [Index.Chunk]))
yield mrun@MRun{..} = do
    p <- readSTRef currentPageRef
    if paIsEmpty p then do
      cs <- append (mkAppend p) mindex
      storeChunks mrun cs
      writeSTRef currentPageRef $! paEmpty
      pure $ Just (pageBuilder p, cs)
    else
      pure Nothing

-- | Yield the current page regardless of its contents. New chunks are recorded
-- in the mutable run, and the current page is set to empty.
yieldAlways :: MRun s -> ST s (BB.Builder, [Index.Chunk])
yieldAlways mrun@MRun{..} = do
    p <- readSTRef currentPageRef
    cs <- append (mkAppend p) mindex
    storeChunks mrun cs
    writeSTRef currentPageRef $! paEmpty
    pure (pageBuilder p, cs)

-- | Store the given chunks in the MRun if non-empty.
storeChunks :: MRun s -> [Index.Chunk] -> ST s ()
storeChunks MRun{..} cs = unless (null cs) $
    modifySTRef' indexChunksRef $ \css -> cs : css

{-------------------------------------------------------------------------------
  Types
-------------------------------------------------------------------------------}

-- | A string of raw bytes representing a value.
--
-- Can also double as a representation for a chunk of a value.
data RawValue =
    UnsafeRawValue !Int !Int ByteArray
  | EmptyRawValue
  deriving Show

{-# COMPLETE EmptyRawValue, RawValue #-}
pattern RawValue :: Int -> Int -> ByteArray -> RawValue
pattern RawValue start end ba <- UnsafeRawValue start end ba
  where RawValue start end ba =
          assert (0 <= start && start <= end && end <= sizeofByteArray ba) $
          UnsafeRawValue start end ba

-- | Size of value in number of bytes.
sizeofValue :: RawValue -> Int
sizeofValue EmptyRawValue          = 0
sizeofValue (RawValue start end _) = end - start

-- | Size of value in number of bytes.
sizeofValue16 :: RawValue -> Word16
sizeofValue16 = fromIntegral . sizeofValue

-- | Size of value in number of bytes.
sizeofValue32 :: RawValue -> Word32
sizeofValue32 = fromIntegral . sizeofValue

-- | Size of value in number of bytes.
sizeofValue64 :: RawValue -> Word64
sizeofValue64 = fromIntegral . sizeofValue

-- | TODO: replace this placeholder type by the actual blob reference type.
data BlobRef = BlobRef {
    blobRefOffset :: !Word64
  , blobRefSize   :: !Word32
  }
  deriving Show

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
    -- (2) an array of 1-bit blob reference indicators
    <> dfoldMap BB.word64LE (bmbits pageBlobRefBitmap)
    -- (3) an array of 2-bit operation types
    <> dfoldMap BB.word64LE (cmbits pageOperations)
    -- (4) a pair of arrays of blob references
    <> dfoldMap BB.word64LE pageBlobRefOffsets
    <> dfoldMap BB.word32LE pageBlobRefSizes
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
    <> dfoldMap rawValue pageValues
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
      [v] -> Right (offValues, fromIntegral offValues + sizeofValue32 v)
      vs  -> Left (scanr (\v o -> o + sizeofValue16 v) offValues vs)

    rawValue :: RawValue -> BB.Builder
    rawValue EmptyRawValue                  = mempty
    rawValue (RawValue i j (ByteArray ba#)) = shortByteStringFromTo i j (SBS ba#)

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
    pageSize           :: !PageSize
    -- | (2) an array of 1-bit blob reference indicators
  , pageBlobRefBitmap  :: !BitMap
    -- | (3) an array of 2-bit operation types
  , pageOperations     :: !CrumbMap
    -- | (4) a pair of arrays of blob references
  , pageBlobRefOffsets :: !(StricterList Word64)
    -- | (4) a pair of arrays of blob references, ctd
  , pageBlobRefSizes   :: !(StricterList Word32)
    --   (5) key offsets will be computed when serialising the page
    --   (6) value offsets will be computed when serialising the page
    -- | (7) the concatenation of all keys
  , pageKeys           :: !(StricterList SerialisedKey)
    -- | (8) the concatenation of all values
  , pageValues         :: !(StricterList RawValue)
  }
  deriving Show

paIsEmpty :: PageAcc -> Bool
paIsEmpty p = psIsEmpty (pageSize p)

paIsOverfull :: PageAcc -> Bool
paIsOverfull p = psIsOverfull (pageSize p)

paEmpty :: PageAcc
paEmpty = PageAcc {
      pageSize           = psEmpty
    , pageBlobRefBitmap  = emptyBitMap
    , pageOperations     = emptyCrumbMap
    , pageBlobRefOffsets = SNil
    , pageBlobRefSizes   = SNil
    , pageKeys           = SNil
    , pageValues         = SNil
    }

paAddElem ::
     Int -- ^ Range-finder precision
  -> SerialisedKey
  -> Entry RawValue BlobRef
  -> PageAcc
  -> Maybe PageAcc
paAddElem rangeFinderPrecision k e PageAcc{..}
  | Just pgsz' <- psAddElem k e pageSize
  , partitioned
  = Just $ PageAcc {
        pageSize           = pgsz'
      , pageBlobRefBitmap  = pageBlobRefBitmap'
      , pageOperations     = pageOperations'
      , pageBlobRefOffsets = onBlobRef pageBlobRefOffsets ((`SCons` pageBlobRefOffsets ) . blobRefOffset) e
      , pageBlobRefSizes   = onBlobRef pageBlobRefSizes ((`SCons` pageBlobRefSizes) . blobRefSize) e
      , pageKeys           = k `SCons` pageKeys
      , pageValues         = onValue EmptyRawValue id e `SCons` pageValues
      }
  | otherwise = Nothing
  where
    partitioned = case unStricterList pageKeys of
        []     -> True
        k' : _ -> topBits16 rangeFinderPrecision k == topBits16 rangeFinderPrecision k'

    pageBlobRefBitmap' = appendBit (onBlobRef 0 (const 1) e) pageBlobRefBitmap
    pageOperations'    = appendCrumb (entryToCrumb e) pageOperations

    entryToCrumb Insert{}         = 0
    entryToCrumb InsertWithBlob{} = 0
    entryToCrumb Mupdate{}        = 1
    entryToCrumb Delete{}         = 2

paSingleton :: SerialisedKey -> Entry RawValue BlobRef -> PageAcc
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

psAddElem :: SerialisedKey -> Entry RawValue BlobRef -> PageSize -> Maybe PageSize
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
        + onValue 0 sizeofValue64 e             -- (8) value bytes

psSingleton :: SerialisedKey -> Entry RawValue BlobRef -> PageSize
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
