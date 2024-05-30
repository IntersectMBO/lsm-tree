{-# LANGUAGE OverloadedStrings #-}
module Test.Database.LSMTree.Internal.PageAcc (tests) where

import           Control.Monad.ST.Strict (runST)
import qualified Data.ByteString as BS

import           Database.LSMTree.Internal.BlobRef (BlobSpan (..))
import           Database.LSMTree.Internal.Entry (Entry (..))
import           Database.LSMTree.Internal.PageAcc
import           Database.LSMTree.Internal.RawPage (RawPage)
import           Database.LSMTree.Internal.Serialise

import           Database.LSMTree.Extras.ReferenceImpl hiding (Operation (..))
import qualified Database.LSMTree.Extras.ReferenceImpl as Proto
import           Test.Util.RawPage (propEqualRawPages)

import           Test.QuickCheck
import           Test.QuickCheck.Instances ()
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.QuickCheck (testProperty)

tests :: TestTree
tests = testGroup "Database.LSMTree.Internal.PageAcc"
    [ testProperty "prototype" (\(PageContentMaybeOverfull kops) -> prototype kops)

    , testProperty "example-00" $ prototype []
    , testProperty "example-01" $ prototype [(Proto.Key "foobar", Proto.Delete)]
    , testProperty "example-02" $ prototype [(Proto.Key "foobar", Proto.Insert (Proto.Value "value") (Just (Proto.BlobRef 111 333)))]
    , testProperty "example-03" $ prototype [(Proto.Key "\NUL",Proto.Delete),(Proto.Key "\SOH",Proto.Delete)]

    -- entries around maximal size
    , testProperty "example-04a" $ prototype [(Proto.Key "",Proto.Insert (Proto.Value (BS.pack (replicate 4063 120))) Nothing)]
    , testProperty "example-04b" $ prototype [(Proto.Key "",Proto.Insert (Proto.Value (BS.pack (replicate 4064 120))) Nothing)]
    , testProperty "example-04c" $ prototype [(Proto.Key "",Proto.Insert (Proto.Value (BS.pack (replicate 4065 120))) Nothing)]

    , testProperty "example-05a" $ prototype [(Proto.Key "",Proto.Delete),(Proto.Key "k",Proto.Insert (Proto.Value (BS.pack (replicate 4060 120))) Nothing)]
    , testProperty "example-05b" $ prototype [(Proto.Key "",Proto.Delete),(Proto.Key "k",Proto.Insert (Proto.Value (BS.pack (replicate 4061 120))) Nothing)]
    , testProperty "example-05c" $ prototype [(Proto.Key "",Proto.Delete),(Proto.Key "k",Proto.Insert (Proto.Value (BS.pack (replicate 4062 120))) Nothing)]

    , testProperty "example-06a" $ prototype [(Proto.Key "",Proto.Insert (Proto.Value (BS.pack (replicate 4051 120))) (Just (Proto.BlobRef 111 333)))]
    , testProperty "example-06b" $ prototype [(Proto.Key "",Proto.Insert (Proto.Value (BS.pack (replicate 4052 120))) (Just (Proto.BlobRef 111 333)))]
    , testProperty "example-06c" $ prototype [(Proto.Key "",Proto.Insert (Proto.Value (BS.pack (replicate 4053 120))) (Just (Proto.BlobRef 111 333)))]
    ]

prototype :: [(Proto.Key, Proto.Operation)] -> Property
prototype kops =
    case (refImpl, realImpl) of
      (Just (lhs, _), Just rhs) -> propEqualRawPages lhs rhs
      (Nothing,       Nothing)  -> label "overflow" $
                                   property True

      -- Special case: the PageAcc does not support single-key/op pairs that
      -- overflow into multiple pages. That special case is handled by PageAcc1.
      -- So test if we're in that special case and if so allow a test pass.
      (Just _,        Nothing)
        | [_]       <- kops
        , Just page <- encodePage DiskPage4k kops
        , pageDiskPages page > 1
                                -> label "PageAcc1 special case" $
                                   property True
      _                         -> property False
  where
    refImpl  = toRawPageMaybeOverfull (PageContentMaybeOverfull kops)
    realImpl = toRawPageViaPageAcc [ (toSerialisedKey k, toEntry op)
                                   | (k,op) <- kops ]

-- | Use a 'PageAcc' to try to make a 'RawPage' from key\/op pairs. It will
-- return @Nothing@ if the key\/op pairs would not all fit in a page.
--
toRawPageViaPageAcc :: [(SerialisedKey, Entry SerialisedValue BlobSpan)]
                    -> Maybe RawPage
toRawPageViaPageAcc kops0 =
    runST $ do
      acc <- newPageAcc
      go acc kops0
  where
    go acc []            = Just <$> serialisePageAcc acc
    go acc ((k,op):kops) = do
      added <- pageAccAddElem acc k op
      if added
        then go acc kops
        else return Nothing

