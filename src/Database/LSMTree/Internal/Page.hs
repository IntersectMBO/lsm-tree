-- | Utilities related to pages.
module Database.LSMTree.Internal.Page (
    PageNo (..)
  , nextPageNo
  , NumPages (..)
  , getNumPages
  , PageSpan (..)
  , singlePage
  , multiPage
  , pageSpanSize
  ) where

import           Control.DeepSeq (NFData (..))

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
