module Database.LSMTree.Extras.RawPage (
    toRawPage,
) where

import qualified Data.ByteString as BS
import qualified Data.ByteString.Short as SBS
import           Data.Primitive.ByteArray (ByteArray (..))

import           Database.LSMTree.Extras.ReferenceImpl
import           Database.LSMTree.Internal.RawOverflowPage (RawOverflowPage,
                     makeRawOverflowPage)
import           Database.LSMTree.Internal.RawPage (RawPage, makeRawPage)

-- | Convert from 'PageContentFits' (from the reference implementation)
-- to 'RawPage' (from the real implementation).
--
toRawPage :: PageContentFits -> (RawPage, [RawOverflowPage])
toRawPage (PageContentFits kops) = (page, overflowPages)
  where
    bs = maybe overfull serialisePage (encodePage DiskPage4k kops)
    (pfx, sfx) = BS.splitAt 4096 bs -- hardcoded page size.
    page          = makeRawPageBS pfx
    overflowPages = [ makeRawOverflowPageBS sfxpg
                    | sfxpg <- takeWhile (not . BS.null)
                                 [ BS.take 4096 (BS.drop n sfx)
                                 | n <- [0, 4096 .. ] ]
                    ]
    overfull =
      error $ "toRawPage: encountered overfull page, but PageContentFits is "
           ++ "supposed to be guaranteed to fit, i.e. not to be overfull."

makeRawPageBS :: BS.ByteString -> RawPage
makeRawPageBS bs =
    case SBS.toShort bs of
      SBS.SBS ba -> makeRawPage (ByteArray ba) 0

makeRawOverflowPageBS :: BS.ByteString -> RawOverflowPage
makeRawOverflowPageBS bs =
    case SBS.toShort bs of
      SBS.SBS ba -> makeRawOverflowPage (ByteArray ba) 0 (BS.length bs)

