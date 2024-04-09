{-# LANGUAGE OverloadedStrings #-}
module Test.Database.LSMTree.PageAcc (tests) where

import           Control.Monad (guard)
import           Control.Monad.ST.Strict (ST, runST)
import qualified Data.ByteString as BS
import           Data.Function (on)
import           Data.List (nubBy, sortBy)
import           Database.LSMTree.BlobRef (BlobSpan (..))
import           Database.LSMTree.Entry (Entry (..))
import           Database.LSMTree.RawPage (RawPage)
import           Database.LSMTree.Serialise
import qualified Database.LSMTree.Serialise.RawBytes as RawBytes
import           Test.QuickCheck
import           Test.QuickCheck.Instances ()
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.QuickCheck (testProperty)
import           Test.Util.RawPage

import qualified FormatPage as Proto

import           Database.LSMTree.PageAcc

tests :: TestTree
tests = testGroup "Database.LSMTree.PageAcc"
    [ testProperty "prototype" prototype

    , testProperty "example-00" $ prototype []
    , testProperty "example-01" $ prototype [(Proto.Key "foobar", Proto.Delete, Nothing)]
    , testProperty "example-02" $ prototype [(Proto.Key "foobar", Proto.Insert (Proto.Value "value"), Just (Proto.BlobRef 111 333))]
    , testProperty "example-03" $ prototype [(Proto.Key "\NUL",Proto.Delete,Nothing),(Proto.Key "\SOH",Proto.Delete,Nothing)]

    -- entries around maximal size
    , testProperty "example-04a" $ prototype [(Proto.Key "",Proto.Insert (Proto.Value (BS.pack (replicate 4063 120))),Nothing)]
    , testProperty "example-04b" $ prototype [(Proto.Key "",Proto.Insert (Proto.Value (BS.pack (replicate 4064 120))),Nothing)]
    , testProperty "example-04c" $ prototype [(Proto.Key "",Proto.Insert (Proto.Value (BS.pack (replicate 4065 120))),Nothing)]

    , testProperty "example-05a" $ prototype [(Proto.Key "",Proto.Delete,Nothing),(Proto.Key "k",Proto.Insert (Proto.Value (BS.pack (replicate 4060 120))),Nothing)]
    , testProperty "example-05b" $ prototype [(Proto.Key "",Proto.Delete,Nothing),(Proto.Key "k",Proto.Insert (Proto.Value (BS.pack (replicate 4061 120))),Nothing)]
    , testProperty "example-05c" $ prototype [(Proto.Key "",Proto.Delete,Nothing),(Proto.Key "k",Proto.Insert (Proto.Value (BS.pack (replicate 4062 120))),Nothing)]

    , testProperty "example-06a" $ prototype [(Proto.Key "",Proto.Insert (Proto.Value (BS.pack (replicate 4051 120))),Just (Proto.BlobRef 111 333))]
    , testProperty "example-06b" $ prototype [(Proto.Key "",Proto.Insert (Proto.Value (BS.pack (replicate 4052 120))),Just (Proto.BlobRef 111 333))]
    , testProperty "example-06c" $ prototype [(Proto.Key "",Proto.Insert (Proto.Value (BS.pack (replicate 4053 120))),Just (Proto.BlobRef 111 333))]
    ]

-- | Strict 'pageSizeAddElem', doesn't allow for page to overflow
pageSizeAddElem' :: (Proto.Key, Proto.Operation, Maybe Proto.BlobRef) -> Proto.PageSize -> Maybe Proto.PageSize
pageSizeAddElem' e sz = do
    sz' <- Proto.pageSizeAddElem e sz
    guard (Proto.pageSizeBytes sz' <= 4096)
    return sz'

prototype
    :: [(Proto.Key, Proto.Operation, Maybe Proto.BlobRef)]
    -> Property
prototype inputs' =
    case invariant inputs' of
        es -> runST $ do
            acc <- newPageAcc
            go acc Proto.pageSizeEmpty [] es
  where
    -- inputs should be ordered and unique to produce valid page.
    invariant xs =
        nubBy ((==) `on` fstOf3) $
        sortBy (compare `on` fstOf3) $
        map cleanOp xs

    -- only insert op has blob references
    -- TODO: make values small.
    -- If the value will overflow page, we'll need a special page anyway.
    cleanOp :: (k, Proto.Operation, Maybe bref) -> (k, Proto.Operation, Maybe bref)
    cleanOp (k, Proto.Delete,    _)  = (k, Proto.Delete, Nothing)
    cleanOp (k, Proto.Mupsert v, _)  = (k, Proto.Mupsert v, Nothing)
    cleanOp (k, Proto.Insert v,  br) = (k, Proto.Insert v, br)

    fstOf3 (k,_,_) = k

    go :: MPageAcc s -> Proto.PageSize -> [(Proto.Key, Proto.Operation, Maybe Proto.BlobRef)] -> [(Proto.Key, Proto.Operation, Maybe Proto.BlobRef)] -> ST s Property
    go acc _ps acc2 []                 = finish acc acc2
    go acc  ps acc2 (e@(k,op,bref):es) = case pageSizeAddElem' e ps of
        Nothing -> do
            added <- pageAccAddElem acc (convKey k) (convOp op bref)
            if added
            then return $ counterexample "MPageAcc addition succeeded, prototype's doesn't." False
            else finish acc acc2

        Just ps' -> do
            added <- pageAccAddElem acc (convKey k) (convOp op bref)
            if added
            then go acc ps' (e:acc2) es
            else return $ counterexample "MPageAcc addition failed, prototype's doesn't." False

    finish :: MPageAcc s -> [(Proto.Key, Proto.Operation, Maybe Proto.BlobRef)] -> ST s Property
    finish acc acc2 = do
        let (lhs, _) = toRawPage $ Proto.PageLogical $ reverse acc2
        rawpage <- serializePageAcc acc
        let rhs = rawpage :: RawPage
        return $ propEqualRawPages lhs rhs

    convKey :: Proto.Key -> SerialisedKey
    convKey (Proto.Key k) = SerialisedKey $ RawBytes.fromByteString k

    convValue :: Proto.Value -> SerialisedValue
    convValue (Proto.Value v) = SerialisedValue $ RawBytes.fromByteString v

    convBlobSpan :: Proto.BlobRef -> BlobSpan
    convBlobSpan (Proto.BlobRef x y) = BlobSpan x y

    convOp :: Proto.Operation -> Maybe Proto.BlobRef -> Entry SerialisedValue BlobSpan
    convOp Proto.Delete      _            = Delete
    convOp (Proto.Mupsert v) _            = Mupdate (convValue v)
    convOp (Proto.Insert v)  Nothing      = Insert (convValue v)
    convOp (Proto.Insert v)  (Just bspan) = InsertWithBlob (convValue v) (convBlobSpan bspan)
