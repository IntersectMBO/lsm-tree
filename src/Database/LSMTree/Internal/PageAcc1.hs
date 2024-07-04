-- | Create a single value page
module Database.LSMTree.Internal.PageAcc1 (
    singletonPage,
) where

import           Control.Monad.ST.Strict (ST, runST)
import qualified Data.Primitive as P
import qualified Data.Vector.Primitive as VP
import           Data.Word (Word16, Word32, Word64)
import           Database.LSMTree.Internal.BlobRef (BlobSpan (..))
import           Database.LSMTree.Internal.Entry (Entry (..))
import           Database.LSMTree.Internal.RawBytes (RawBytes (..))
import qualified Database.LSMTree.Internal.RawBytes as RB
import           Database.LSMTree.Internal.RawOverflowPage
import           Database.LSMTree.Internal.RawPage
import           Database.LSMTree.Internal.Serialise

pageSize :: Int
pageSize = 4096
{-# INLINE pageSize #-}

-- | Create a singleton page, also returning the overflow value bytes.
singletonPage
    :: SerialisedKey
    -> Entry SerialisedValue BlobSpan
    -> (RawPage, [RawOverflowPage])
singletonPage k (Insert v) = runST $ do
    -- allocate bytearray
    ba <- P.newPinnedByteArray pageSize :: ST s (P.MutableByteArray s)
    P.fillByteArray ba 0 pageSize 0

    -- directory: 64 bytes
    P.writeByteArray ba 0 (1  :: Word16)
    P.writeByteArray ba 1 (0  :: Word16)
    P.writeByteArray ba 2 (24 :: Word16)

    -- blobref and operation bitmap sizes in bytes
    -- P.writeByteArray ba 1 (0 :: Word64)
    -- P.writeByteArray ba 2 (0 :: Word64)

    -- no blob references

    P.writeByteArray ba 12 (32 :: Word16) -- key offset
    P.writeByteArray ba 13 (32 + sizeofKey16 k :: Word16) -- value offset
    P.writeByteArray ba 7  (32 + sizeofKey32 k + sizeofValue32 v :: Word32) -- post value offset

    -- copy key
    P.copyByteArray ba 32          kba koff klen

    -- copy value prefix
    let vlen' = min vlen (pageSize - klen - 32)
    P.copyByteArray ba (32 + klen) vba voff vlen'

    ba' <- P.unsafeFreezeByteArray ba
    let !page          = unsafeMakeRawPage ba' 0
        !overflowPages = rawBytesToOverflowPages (RB.drop vlen' v')
    return (page, overflowPages)
  where
    SerialisedKey      (RawBytes (VP.Vector koff klen kba)) = k
    SerialisedValue v'@(RawBytes (VP.Vector voff vlen vba)) = v

singletonPage k (InsertWithBlob v (BlobSpan w64 w32)) = runST $ do
    -- allocate bytearray
    ba <- P.newPinnedByteArray pageSize :: ST s (P.MutableByteArray s)
    P.fillByteArray ba 0 pageSize 0

    -- directory: 64 bytes
    P.writeByteArray ba 0 (1  :: Word16)
    P.writeByteArray ba 1 (1  :: Word16)
    P.writeByteArray ba 2 (36 :: Word16)

    -- blobref and operation bitmap sizes in bytes
    P.writeByteArray ba 1 (1 :: Word64)
    -- P.writeByteArray ba 2 (0 :: Word64)

    -- blob references
    P.writeByteArray ba 3 w64
    P.writeByteArray ba 8 w32

    P.writeByteArray ba 18 (44 :: Word16) -- key offset
    P.writeByteArray ba 19 (44 + sizeofKey16 k :: Word16) -- value offset
    P.writeByteArray ba 10  (44 + sizeofKey32 k + sizeofValue32 v :: Word32) -- post value offset

    -- copy key
    P.copyByteArray ba 44          kba koff klen

    -- copy value prefix
    let vlen' = min vlen (pageSize - klen - 44)
    P.copyByteArray ba (44 + klen) vba voff vlen'

    ba' <- P.unsafeFreezeByteArray ba
    let !page          = unsafeMakeRawPage ba' 0
        !overflowPages = rawBytesToOverflowPages (RB.drop vlen' v')
    return (page, overflowPages)
  where
    SerialisedKey      (RawBytes (VP.Vector koff klen kba)) = k
    SerialisedValue v'@(RawBytes (VP.Vector voff vlen vba)) = v

singletonPage k (Mupdate v) = runST $ do
    -- allocate bytearray
    ba <- P.newPinnedByteArray pageSize :: ST s (P.MutableByteArray s)
    P.fillByteArray ba 0 pageSize 0

    -- directory: 64 bytes
    P.writeByteArray ba 0 (1  :: Word16)
    P.writeByteArray ba 1 (0  :: Word16)
    P.writeByteArray ba 2 (24 :: Word16)

    -- blobref and operation bitmap sizes in bytes
    -- P.writeByteArray ba 1 (0 :: Word64)
    P.writeByteArray ba 2 (1 :: Word64)

    -- no blob references

    P.writeByteArray ba 12 (32 :: Word16) -- key offset
    P.writeByteArray ba 13 (32 + sizeofKey16 k :: Word16) -- value offset
    P.writeByteArray ba 7  (32 + sizeofKey32 k + sizeofValue32 v :: Word32) -- post value offset

    -- copy key
    P.copyByteArray ba 32          kba koff klen

    -- copy value prefix
    let vlen' = min vlen (pageSize - klen - 32)
    P.copyByteArray ba (32 + klen) vba voff vlen'

    ba' <- P.unsafeFreezeByteArray ba
    let !page          = unsafeMakeRawPage ba' 0
        !overflowPages = rawBytesToOverflowPages (RB.drop vlen' v')
    return (page, overflowPages)
  where
    SerialisedKey      (RawBytes (VP.Vector koff klen kba)) = k
    SerialisedValue v'@(RawBytes (VP.Vector voff vlen vba)) = v

singletonPage _ Delete = error "singletonPage: unexpected Delete entry"
