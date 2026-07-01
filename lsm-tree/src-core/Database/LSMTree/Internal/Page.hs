{-# OPTIONS_HADDOCK not-home #-}

-- | Utilities related to pages.
--
module Database.LSMTree.Internal.Page (
    PageNo (..)
  , nextPageNo
  , NumPages (..)
  , getNumPages
  , PageSpan (..)
  , singlePage
  , multiPage
  , pageSpanSize
    -- * Constants
  , pageSize
    -- * I\/O
  , readDiskPage
  , skipPages
  ) where

import           Control.DeepSeq (NFData (..))
import           Control.Exception (assert)
import           Control.Monad (guard)
import           Control.Monad.Class.MonadThrow
import           Control.Monad.Primitive (PrimMonad (..))
import           Data.Primitive.ByteArray
import           Data.Word (Word32)
import           Database.LSMTree.Internal.BitMath (roundUpToPageSize)
import           Database.LSMTree.Internal.RawPage (RawPage, unsafeMakeRawPage)
import qualified System.FS.API as FS
import           System.FS.API (Handle, HasFS)

-- | A 0-based number identifying a disk page.
newtype PageNo = PageNo { unPageNo :: Int }
  deriving stock (Show, Eq, Ord)
  deriving newtype NFData

-- | Increment the page number.
--
-- Note: This does not ensure that the incremented page number exists within a given page span.
{-# INLINE nextPageNo #-}
nextPageNo :: PageNo -> PageNo
nextPageNo = PageNo . succ . unPageNo

-- | The number of pages contained by an index or other paging data-structure.
--
-- Note: This is a 0-based number; take care to ensure arithmetic underflow
-- does not occur during subtraction operations!
newtype NumPages = NumPages Word
  deriving stock (Eq, Ord, Show)
  deriving newtype (NFData)

-- | A type-safe "unwrapper" for 'NumPages'. Use this accessor whenever you want
-- to convert 'NumPages' to a more versatile number type.
{-# INLINE getNumPages #-}
getNumPages :: Integral i => NumPages -> i
getNumPages (NumPages w) = fromIntegral w

-- | A span of pages, representing an inclusive interval of page numbers.
--
-- Typlically used to denote the contiguous page span for a database entry.
data PageSpan = PageSpan {
    pageSpanStart :: {-# UNPACK #-} !PageNo
  , pageSpanEnd   :: {-# UNPACK #-} !PageNo
  }
  deriving stock (Show, Eq)

instance NFData PageSpan where
  rnf (PageSpan x y) = rnf x `seq` rnf y

{-# INLINE singlePage #-}
singlePage :: PageNo -> PageSpan
singlePage i = PageSpan i i

{-# INLINE multiPage #-}
multiPage :: PageNo -> PageNo -> PageSpan
multiPage i j = PageSpan i j

{-# INLINE pageSpanSize #-}
pageSpanSize :: PageSpan -> NumPages
pageSpanSize pspan = NumPages . toEnum $
    unPageNo (pageSpanEnd pspan) - unPageNo (pageSpanStart pspan) + 1

{-------------------------------------------------------------------------------
  Constants
-------------------------------------------------------------------------------}

pageSize :: Int
pageSize = 4096

{-------------------------------------------------------------------------------
  I\/O
-------------------------------------------------------------------------------}

{-# SPECIALISE readDiskPage ::
     HasFS IO h
  -> Handle h
  -> IO (Maybe RawPage) #-}
-- | Returns 'Nothing' on EOF.
readDiskPage ::
     (MonadCatch m, PrimMonad m)
  => HasFS m h
  -> Handle h
  -> m (Maybe RawPage)
readDiskPage fs h = do
    mba <- newPinnedByteArray pageSize
    -- TODO: make sure no other exception type can be thrown
    --
    -- TODO: if FS.FsReachEOF is thrown as an injected disk fault, then we
    -- incorrectly deduce that the file has no more contents. We should probably
    -- use an explicit file pointer instead in the style of 'FilePointer'.
    handleJust (guard . FS.isFsErrorType FS.FsReachedEOF) (\_ -> pure Nothing) $ do
      bytesRead <- FS.hGetBufExactly fs h mba 0 (fromIntegral pageSize)
      assert (fromIntegral bytesRead == pageSize) $ pure ()
      ba <- unsafeFreezeByteArray mba
      let !rawPage = unsafeMakeRawPage ba 0
      pure (Just rawPage)

-- | Move the handle's file pointer the given number of pages
skipPages ::
      HasFS m h
   -> Handle h
   -> Word32
   -> m ()
skipPages fs h len = do
    let lenPages = fromIntegral (roundUpToPageSize len)
    FS.hSeek fs h FS.RelativeSeek lenPages
