{-# OPTIONS_HADDOCK not-home #-}

-- | Page accumulator.
--
module Database.LSMTree.Internal.PageAcc (
    -- * Incrementally accumulating a single page.
    PageAcc (..),
    newPageAcc,
    resetPageAcc,
    pageAccAddElem,
    serialisePageAcc,
    -- ** Inspection
    keysCountPageAcc,
    indexKeyPageAcc,
    -- ** Entry sizes
    entryWouldFitInPage,
    sizeofEntry,
) where

import           Control.Monad.ST.Strict (ST)
import           Data.Bits (unsafeShiftL, (.|.))
import qualified Data.Primitive as P
import qualified Data.Vector as V
import qualified Data.Vector.Mutable as VM
import qualified Data.Vector.Primitive as VP
import           Data.Word (Word16, Word32, Word64)
import           Database.LSMTree.Internal.BitMath
import           Database.LSMTree.Internal.BlobRef (BlobSpan (..))
import           Database.LSMTree.Internal.Entry (Entry (..))
import           Database.LSMTree.Internal.RawBytes (RawBytes (..))
import qualified Database.LSMTree.Internal.RawBytes as RB
import           Database.LSMTree.Internal.RawPage
import           Database.LSMTree.Internal.Serialise

-- |
--
-- The delete operations take the least amount of space, as there's only the key.
--
-- A smallest page is with empty key:
--
-- >>> import FormatPage
-- >>> let Just page0 = pageSizeAddElem (Key "", Delete) (pageSizeEmpty DiskPage4k)
-- >>> page0
-- PageSize {pageSizeElems = 1, pageSizeBlobs = 0, pageSizeBytes = 32, pageSizeDisk = DiskPage4k}
--
-- Then we can add pages with a single byte key, e.g.
--
-- >>> pageSizeAddElem (Key "a", Delete) page0
-- Just (PageSize {pageSizeElems = 2, pageSizeBlobs = 0, pageSizeBytes = 35, pageSizeDisk = DiskPage4k})
--
-- i.e. roughly 3-4 bytes (when we get to 32/64 elements we add more bytes for bitmaps).
-- (key and value offset is together 4 bytes: so it's at least 4, the encoding of single element page takes more space).
--
-- If we write as small program, adding single byte keys to a page size:
--
-- >>> let calc s ps = case pageSizeAddElem (Key "x", Delete) ps of { Nothing -> s; Just ps' -> calc (s + 1) ps' }
-- >>> calc 1 page0
-- 759
--
-- I.e. we can have a 4096 byte page with at most 759 keys, assuming keys are
-- length 1 or longer, but without assuming that there are no duplicate keys.
--
-- And 759 rounded to the next multiple of 64 (for the bitmaps) is 768.
--
-- 'PageAcc' can hold up to 759 elements, but most likely 'pageAccAddElem' will make it overflow sooner.
-- Having an upper bound allows us to allocate all memory for the accumulator in advance.
--
-- We don't store or calculate individual key nor value offsets in 'PageAcc', as these will be naturally calculated during serialisation ('serialisePageAcc').
--
data PageAcc s = PageAcc
    { paDir         :: !(P.MutablePrimArray s Int)      -- ^ various counters (directory + extra counters). It is convenient to have counters as 'Int', as all indexing uses 'Int's.
    , paOpMap       :: !(P.MutablePrimArray s Word64)   -- ^ operations crumb map
    , paBlobRefsMap :: !(P.MutablePrimArray s Word64)   -- ^ blob reference bitmap
    , paBlobRefs1   :: !(P.MutablePrimArray s Word64)   -- ^ blob spans 64 bit part (offset)
    , paBlobRefs2   :: !(P.MutablePrimArray s Word32)   -- ^ blob spans 32 bit part (size)
    , paKeys        :: !(V.MVector s SerialisedKey)     -- ^ keys
    , paValues      :: !(V.MVector s SerialisedValue)   -- ^ values
    }

-------------------------------------------------------------------------------
-- Constants
-------------------------------------------------------------------------------

keysCountIdx :: Int
keysCountIdx = 0
{-# INLINE keysCountIdx #-}

blobRefCountIdx :: Int
blobRefCountIdx = 1
{-# INLINE blobRefCountIdx #-}

byteSizeIdx :: Int
byteSizeIdx = 2
{-# INLINE byteSizeIdx #-}

keysSizeIdx :: Int
keysSizeIdx = 3
{-# INLINE keysSizeIdx #-}

pageSize :: Int
pageSize = 4096
{-# INLINE pageSize #-}

-- | See calculation in 'PageAcc' comments.
maxKeys :: Int
maxKeys = 759
{-# INLINE maxKeys #-}

-- | See calculation in 'PageAcc' comments.
maxOpMap :: Int
maxOpMap = 24 -- 768 / 32
{-# INLINE maxOpMap #-}

-- | See calculation in 'PageAcc' comments.
maxBlobRefsMap :: Int
maxBlobRefsMap = 12 -- 768 / 64
{-# INLINE maxBlobRefsMap #-}

-------------------------------------------------------------------------------
-- Entry operations
-------------------------------------------------------------------------------

-- | Calculate the total byte size of key, value and optional blobspan.
--
-- To fit into single page this has to be at most 4064. If the entry is larger,
-- the 'pageAccAddElem' is guaranteed to return 'False'.
--
-- In other words, if you have a large entry (i.e. Insert with big value),
-- don't use the 'PageAcc', but construct the single value page directly,
-- using 'Database.LSMTree.Internal.PageAcc1.singletonPage'.
--
-- If it's not known from context, use 'entryWouldFitInPage' to determine if
-- you're in the small or large case.
--
-- Checking entry size allows us to use 'Word16' arithmetic, we don't need to
-- worry about overflows.
--
sizeofEntry :: SerialisedKey -> Entry SerialisedValue b -> Int
sizeofEntry k Delete               = sizeofKey k
sizeofEntry k (Upsert v)           = sizeofKey k + sizeofValue v
sizeofEntry k (Insert v)           = sizeofKey k + sizeofValue v
sizeofEntry k (InsertWithBlob v _) = sizeofKey k + sizeofValue v + 12

-- | Determine if the key, value and optional blobspan would fit within a
-- single page. If it does, then it's appropriate to use 'pageAccAddElem', but
-- otherwise use 'Database.LSMTree.Internal.PageAcc1.singletonPage'.
--
-- If 'entryWouldFitInPage' is @True@ and the 'PageAcc' is empty (i.e. using
--'resetPageAcc') then 'pageAccAddElem' is guaranteed to succeed.
--
entryWouldFitInPage :: SerialisedKey -> Entry SerialisedValue b -> Bool
entryWouldFitInPage k e = sizeofEntry k e + 32 <= pageSize

-- | Whether 'Entry' adds a blob reference
hasBlobRef :: Entry a b -> Bool
hasBlobRef (InsertWithBlob _ _) = True
hasBlobRef _                    = False

-- | Entry's operations crumb. We return 'Word64' as we'd write that.
entryCrumb :: Entry SerialisedValue BlobSpan -> Word64
entryCrumb Insert {}         = 0
entryCrumb InsertWithBlob {} = 0
entryCrumb Upsert {}         = 1
entryCrumb Delete {}         = 2

-- | Entry value. Return 'emptyValue' for 'Delete'
-- (the empty value is in the page even for 'Delete's)
entryValue :: Entry SerialisedValue BlobSpan -> SerialisedValue
entryValue (Insert v)           = v
entryValue (InsertWithBlob v _) = v
entryValue (Upsert v)           = v
entryValue Delete               = emptyValue

emptyValue :: SerialisedValue
emptyValue = SerialisedValue (RB.pack [])

-------------------------------------------------------------------------------
-- PageAcc functions
-------------------------------------------------------------------------------

-- | Create new 'PageAcc'.
--
-- The create 'PageAcc' will be empty.
newPageAcc :: ST s (PageAcc s)
newPageAcc = do
    paDir          <- P.newPrimArray 4
    paOpMap        <- P.newPrimArray maxOpMap
    paBlobRefsMap  <- P.newPrimArray maxBlobRefsMap
    paBlobRefs1    <- P.newPrimArray maxKeys
    paBlobRefs2    <- P.newPrimArray maxKeys
    paKeys         <- VM.new maxKeys
    paValues       <- VM.new maxKeys

    -- reset the memory, as it's not initialized
    let page = PageAcc {..}
    resetPageAccN page maxKeys
    pure page

dummyKey :: SerialisedKey
dummyKey = SerialisedKey mempty

dummyValue :: SerialisedValue
dummyValue = SerialisedValue mempty

-- | Reuse 'PageAcc' for constructing new page, the old data will be reset.
resetPageAcc :: PageAcc s
    -> ST s ()
resetPageAcc pa = do
    !n <- keysCountPageAcc pa
    resetPageAccN pa n

-- | Reset the page for the given number of key\/value pairs. This does not
-- check whether the given number exceeds 'maxKeys', in which case behaviour is
-- undefined.
resetPageAccN :: PageAcc s -> Int -> ST s ()
resetPageAccN PageAcc {..} !n = do
    P.setPrimArray paDir 0 4 0
    P.setPrimArray paOpMap 0 maxOpMap 0
    P.setPrimArray paBlobRefsMap 0 maxBlobRefsMap 0

    -- we don't need to clear these, we set what we need.
    -- P.setPrimArray paBlobRefs1 0 maxKeys 0
    -- P.setPrimArray paBlobRefs1 0 maxKeys 0

    VM.set (VM.slice 0 n paKeys) $! dummyKey
    VM.set (VM.slice 0 n paValues) $! dummyValue

    -- initial size is 8 bytes for directory and 2 bytes for last value offset.
    P.writePrimArray paDir byteSizeIdx 10

-- | Add an entry to 'PageAcc'.
--
pageAccAddElem ::
       PageAcc s
    -> SerialisedKey
    -> Entry SerialisedValue BlobSpan
    -> ST s Bool   -- ^ 'True' if value was successfully added.
pageAccAddElem PageAcc {..} k e
    -- quick short circuit: if the entry is bigger than page: no luck.
    | sizeofEntry k e >= pageSize = pure False
    | otherwise = do
        n <- P.readPrimArray paDir keysCountIdx
        b <- P.readPrimArray paDir blobRefCountIdx
        s <- P.readPrimArray paDir byteSizeIdx

        let !n' = n + 1
        let !b' = if hasBlobRef e then b + 1 else b

        let !s' = s
               + (if mod64 n == 0 then 8 else 0)         -- blobrefs bitmap
               + (if mod32 n == 0 then 8 else 0)         -- operations bitmap
               + (case n of { 0 -> 6; 1 -> 2; _ -> 4 })  -- key and value offsets
               + sizeofEntry k e

        if s' > pageSize || -- check for size overflow
           n >= maxKeys     -- check for buffer overflow
        then pure False
        else do
            -- key sizes
            ks <- P.readPrimArray paDir keysSizeIdx
            let !ks' = ks + sizeofKey k

            P.writePrimArray paDir keysCountIdx n'
            P.writePrimArray paDir blobRefCountIdx b'
            P.writePrimArray paDir keysSizeIdx ks'
            P.writePrimArray paDir byteSizeIdx s'

            -- blob reference
            case e of
                InsertWithBlob _ (BlobSpan w64 w32) -> do
                    setBlobRef paBlobRefsMap n
                    P.writePrimArray paBlobRefs1 b w64
                    P.writePrimArray paBlobRefs2 b w32
                _ -> pure ()

            -- operation
            setOperation paOpMap n (entryCrumb e)

            -- keys and values
            VM.write paKeys n k
            VM.write paValues n $! entryValue e

            pure True

setBlobRef :: P.MutablePrimArray s Word64 -> Int -> ST s ()
setBlobRef arr i = do
    let !j = div64 i
    let !k = mod64 i
    w64 <- P.readPrimArray arr j
    let w64' = w64 .|. unsafeShiftL 1 k
    P.writePrimArray arr j $! w64'

setOperation :: P.MutablePrimArray s Word64 -> Int -> Word64 -> ST s ()
setOperation arr i crumb = do
    let !j = div32 i
    let !k = mod32 i
    w64 <- P.readPrimArray arr j
    let w64' = w64 .|. unsafeShiftL crumb (mul2 k)
    P.writePrimArray arr j $! w64'

-- | Convert mutable 'PageAcc' accumulator to concrete 'RawPage'.
--
-- After this operation 'PageAcc' argument can be reset with 'resetPageAcc',
-- and reused.
serialisePageAcc :: PageAcc s -> ST s RawPage
serialisePageAcc page@PageAcc {..} = do
    size <- P.readPrimArray paDir keysCountIdx
    case size of
        0 -> pure emptyRawPage
        _ -> serialisePageAccN size page

-- | Serialise non-empty page.
serialisePageAccN :: forall s. Int -> PageAcc s -> ST s RawPage
serialisePageAccN size PageAcc {..} = do
    b  <- P.readPrimArray paDir blobRefCountIdx
    ks <- P.readPrimArray paDir keysSizeIdx

    -- keys offsets offset
    let ko0 :: Int
        !ko0 = 8
             + mul8 (ceilDiv64 size + ceilDiv64 (mul2 size)) -- blob and operations bitmaps
             + mul8 b + mul4 b                               -- blob refs

    -- values offset offset
    let vo0 :: Int
        !vo0 = ko0 + mul2 size

    -- keys data offset
    let kd0 :: Int
        !kd0 = vo0 + if size == 1 then 6 else mul2 (size + 1)

    -- values data offset
    let vd0 :: Int
        !vd0 = kd0 + ks

    -- allocate bytearray
    ba <- P.newByteArray pageSize :: ST s (P.MutableByteArray s)
    P.fillByteArray ba 0 pageSize 0

    -- directory: 64 bytes
    P.writeByteArray ba 0 (fromIntegral size :: Word16)
    P.writeByteArray ba 1 (fromIntegral b :: Word16)
    P.writeByteArray ba 2 (fromIntegral ko0 :: Word16)

    -- blobref and operation bitmap sizes in bytes
    let !blobRefMapSize = mul8 (ceilDiv64 size)
    let !opMapSize      = mul8 (ceilDiv64 (mul2 size))

    -- traceM $ "sizes " ++ show (blobRefMapSize, opMapSize)

    P.copyMutableByteArray ba 8                    (primToByte paBlobRefsMap) 0 blobRefMapSize
    P.copyMutableByteArray ba (8 + blobRefMapSize) (primToByte paOpMap)       0 opMapSize

    -- blob references
    P.copyMutableByteArray ba (8 + blobRefMapSize + opMapSize)          (primToByte paBlobRefs1) 0 (mul8 b)
    P.copyMutableByteArray ba (8 + blobRefMapSize + opMapSize + mul8 b) (primToByte paBlobRefs2) 0 (mul4 b)

    -- traceM $ "preloop " ++ show (ko0, kd0, vd0)

    let loop :: Int -> Int -> Int -> Int -> Int -> ST s ()
        loop !i !ko !vo !kd !vd
            | i >= size = do
                  -- past last value offset.
                  -- Note: we don't need to write this offset as Word32
                  -- even in size == 1 case, as preconditions require the
                  -- entries to fit into page, i.e. fit into Word16.
                  P.writeByteArray ba (div2 vo) (fromIntegral vd :: Word16)

            | otherwise = do
                  -- traceM $ "loop " ++ show (i, ko, kd, vo, vd)

                  SerialisedKey   (RawBytes (VP.Vector koff klen kba)) <- VM.read paKeys i
                  SerialisedValue (RawBytes (VP.Vector voff vlen vba)) <- VM.read paValues i

                  -- key and value offsets
                  P.writeByteArray ba (div2 ko) (fromIntegral kd :: Word16)
                  P.writeByteArray ba (div2 vo) (fromIntegral vd :: Word16)

                  -- key and value data
                  P.copyByteArray ba kd kba koff klen
                  P.copyByteArray ba vd vba voff vlen

                  loop (i + 1) (ko + 2) (vo + 2) (kd + klen) (vd + vlen)

    loop 0 ko0 vo0 kd0 vd0

    ba' <- P.unsafeFreezeByteArray ba
    pure (makeRawPage ba' 0)


keysCountPageAcc :: PageAcc s -> ST s Int
keysCountPageAcc PageAcc {paDir} = P.readPrimArray paDir keysCountIdx

indexKeyPageAcc :: PageAcc s -> Int -> ST s SerialisedKey
indexKeyPageAcc PageAcc {paKeys} ix = VM.read paKeys ix

-------------------------------------------------------------------------------
-- Utils
-------------------------------------------------------------------------------

-- | Extract underlying bytearray fromn 'P.MutableByteArray',
-- so we can copy its contents.
primToByte :: P.MutablePrimArray s a -> P.MutableByteArray s
primToByte (P.MutablePrimArray ba) = P.MutableByteArray ba
