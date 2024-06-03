{-# LANGUAGE OverloadedStrings #-}
module Test.Database.LSMTree.Internal.PageAcc1 (tests) where

import qualified Data.ByteString as BS

import           Database.LSMTree.Internal.PageAcc1

import qualified Database.LSMTree.Extras.ReferenceImpl as Ref

import           Test.QuickCheck
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.QuickCheck (testProperty)
import           Test.Util.RawPage

tests :: TestTree
tests =
  testGroup "Database.LSMTree.Internal.PageAcc1" $
    [ testProperty "vs reference impl" prop_vsReferenceImpl ]

 ++ [ testProperty
        ("example-" ++ show (n :: Int) ++ [a])
        (prop_vsReferenceImpl (Ref.PageContentSingle (Ref.Key "") op))
    | (n,exs) <- zip [1..] examples
    , (a, op) <- zip ['a'..] exs
    ]
  where
    examples :: [[Ref.Operation]]
    examples  = [example1s, example2s, example3s]
    example1s = [ Ref.Insert (Ref.Value (BS.replicate sz 120)) Nothing
                | sz <- [4064..4066] ]

    example2s = [ Ref.Insert (Ref.Value (BS.replicate sz 120))
                             (Just (Ref.BlobRef 3 5))
                | sz <- [4050..4052] ]

    example3s = [ Ref.Mupsert (Ref.Value (BS.replicate sz 120))
                | sz <- [4064..4066] ]

prop_vsReferenceImpl :: Ref.PageContentSingle -> Property
prop_vsReferenceImpl (Ref.PageContentSingle k op) =
    op /= Ref.Delete ==>
    label (show (length loverflow) ++ " overflow pages") $
         propEqualRawPages lhs rhs
    .&&. counterexample "overflow pages do not match"
           (loverflow === roverflow)
  where
    (lhs, loverflow) = Ref.toRawPage $ Ref.PageContentFits [(k, op)]
    (rhs, roverflow) = singletonPage (Ref.toSerialisedKey k) (Ref.toEntry op)

