{-# LANGUAGE BangPatterns  #-}
{-# LANGUAGE TupleSections #-}

{-# OPTIONS_GHC -Wno-incomplete-uni-patterns #-}

module Test.Database.LSMTree.Internal.RunAcc (tests) where

import           Control.Exception (assert)
import           Control.Monad.ST
import           Data.Bifunctor (Bifunctor (..))
import qualified Data.BloomFilter as Bloom
import qualified Data.ByteString as BS
import qualified Data.ByteString.Short as SBS
import           Data.Maybe
import qualified Data.Vector.Primitive as P
import           Database.LSMTree.Internal.BlobRef (BlobSpan (..))
import           Database.LSMTree.Internal.Entry
import qualified Database.LSMTree.Internal.IndexCompact as Index
import qualified Database.LSMTree.Internal.PageAcc as PageAcc
import qualified Database.LSMTree.Internal.PageAcc1 as PageAcc
import qualified Database.LSMTree.Internal.RawBytes as RB
import           Database.LSMTree.Internal.RawOverflowPage (RawOverflowPage)
import qualified Database.LSMTree.Internal.RawOverflowPage as RawOverflowPage
import           Database.LSMTree.Internal.RawPage (RawPage)
import qualified Database.LSMTree.Internal.RawPage as RawPage
import           Database.LSMTree.Internal.RunAcc
import           Database.LSMTree.Internal.Serialise
import qualified FormatPage as Proto
import           Test.Tasty
import           Test.Tasty.HUnit hiding (assert)
import           Test.Tasty.QuickCheck

tests :: TestTree
tests = testGroup "Database.LSMTree.Internal.RunAcc" [
      testGroup "RunAcc" [
          testCase "test_singleKeyRun" $ test_singleKeyRun
        ]
    , testGroup "PageAcc" [
          largerTestCases $
          --TODO: partitioning tests need to move to RunAcc.
          testProperty "prop_paddedToDiskPageSize with trivially partitioned pages" $
            prop_paddedToDiskPageSize 0
        , largerTestCases $
          testProperty "prop_paddedToDiskPageSize with partitioned pages" $
            prop_paddedToDiskPageSize 8
        , largerTestCases $
          testProperty "prop_runAccMatchesPrototype" prop_runAccMatchesPrototype
        ]
    ]
  where largerTestCases = localOption (QuickCheckMaxSize 500) . localOption (QuickCheckTests 10000)

{-------------------------------------------------------------------------------
  RunAcc
-------------------------------------------------------------------------------}

test_singleKeyRun :: Assertion
test_singleKeyRun =  do
    let !k = SerialisedKey' (P.fromList [37, 37, 37, 37, 37, 37])
        !e = InsertWithBlob (SerialisedValue' (P.fromList [48, 19])) (BlobSpan 55 77)

    (addRes, (mp, mc, b, ic, _numEntries)) <- stToIO $ do
      racc <- new (NumEntries 1) 1 Nothing
      addRes <- addKeyOp racc k e
      (addRes,) <$> unsafeFinalise racc

    ([], [], []) @=? addRes
    Just (fst (PageAcc.singletonPage k e)) @=? mp
    isJust mc @? "expected a chunk"
    True @=? Bloom.elem k b
    Index.singlePage (Index.PageNo 0) @=? Index.search k ic

{-------------------------------------------------------------------------------
  PageAcc
-------------------------------------------------------------------------------}

--TODO: this test no longer makes sense  on the PageAcc when used with
-- non-default RFP values, because PageAcc doesn't use the RangeFinderPrecision,
-- only RunAcc does. This aspect of the test should be ported to a RunAcc test.
prop_paddedToDiskPageSize :: Int -> PageLogical' -> Property
prop_paddedToDiskPageSize _rfp page =
    counterexample "expected number of output bytes to be of disk page size" $
    tabulate "page size in bytes" [show $ BS.length bytes] $
    BS.length bytes `rem` 4096 === 0
  where
    bytes = uncurry pagesToByteString $ fromListPageAcc (getRealKOps page)

prop_runAccMatchesPrototype :: PageLogical' -> Property
prop_runAccMatchesPrototype page =
    counterexample "real /= model" $
    real === model
  where
    Just model = Proto.serialisePage <$>
                   Proto.encodePage Proto.DiskPage4k (getPrototypeKOps page)
    real  = trunc $ uncurry pagesToByteString $ fromListPageAcc (getRealKOps page)

    -- truncate padding on the real page
    trunc = BS.take (BS.length model)

{-------------------------------------------------------------------------------
  Util
-------------------------------------------------------------------------------}

fromListPageAcc :: [(SerialisedKey, Entry SerialisedValue BlobSpan)]
                -> (RawPage, [RawOverflowPage])
fromListPageAcc ((k,e):kops)
  | not (PageAcc.entryWouldFitInPage k e) =
    assert (null kops) $
    PageAcc.singletonPage k e

fromListPageAcc kops =
    runST (do
      pacc <- PageAcc.newPageAcc
      sequence_
        [ do added <- PageAcc.pageAccAddElem pacc k e
             -- we expect the kops to all fit in one page
             assert added $ return ()
        | (k,e) <- kops ]
      page <- PageAcc.serialisePageAcc pacc
      return (page, []))

pagesToByteString :: RawPage -> [RawOverflowPage] -> BS.ByteString
pagesToByteString rp rops =
    RB.toByteString
  . mconcat
  $ RawPage.rawPageRawBytes rp
  : map RawOverflowPage.rawOverflowPageRawBytes rops

fromProtoKOp ::
     (Proto.Key, Proto.Operation)
  -> (SerialisedKey, Entry SerialisedValue BlobSpan)
fromProtoKOp (k, op) =
    (fromProtoKey k, bimap fromProtoValue fromProtoBlobRef e)
  where e = case op of
              Proto.Insert  v Nothing   -> Insert v
              Proto.Insert  v (Just br) -> InsertWithBlob v br
              Proto.Mupsert v           -> Mupdate v
              Proto.Delete              -> Delete

fromProtoKey :: Proto.Key -> SerialisedKey
fromProtoKey (Proto.Key bs) = SerialisedKey . RB.fromShortByteString $ SBS.toShort bs

fromProtoValue :: Proto.Value -> SerialisedValue
fromProtoValue (Proto.Value bs) = SerialisedValue . RB.fromShortByteString $ SBS.toShort bs

fromProtoBlobRef :: Proto.BlobRef -> BlobSpan
fromProtoBlobRef (Proto.BlobRef x y) = BlobSpan x y

-- | Wrapper around 'PageLogical' that generates nearly-full pages, and
-- keys that are always large enough (>= 6 bytes) for the compact index.
newtype PageLogical' = PageLogical' {getPageLogical' :: Proto.PageLogical}
  deriving Show

getRealKOps :: PageLogical' -> [(SerialisedKey, Entry SerialisedValue BlobSpan)]
getRealKOps = fmap fromProtoKOp . getPrototypeKOps

getPrototypeKOps :: PageLogical' -> [(Proto.Key, Proto.Operation)]
getPrototypeKOps (PageLogical' (Proto.PageLogical kops)) = kops

instance Arbitrary PageLogical' where
  arbitrary = PageLogical' <$>
      Proto.genFullPageLogical Proto.DiskPage4k
        (arbitrary `suchThat` \(Proto.Key bs) -> BS.length bs >= 6)
        arbitrary
  shrink (PageLogical' page) =
      [ PageLogical' page' | page' <- shrink page ]

