{-# LANGUAGE OverloadedStrings #-}
module Test.Database.LSMTree.Internal.RawPage (
    -- * Main test tree
    tests,
) where

import qualified Data.ByteString.Short as SBS
import           Data.Coerce (coerce)
import           Data.Maybe (isJust)
import           Data.Primitive.ByteArray (ByteArray (..), byteArrayFromList)
import qualified Data.Vector.Primitive as V
import qualified Data.Vector.Unboxed as UV
import           Data.Word (Word16)
import           GHC.Word (byteSwap16)
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.HUnit (testCase, (@=?), (@?=))
import           Test.Tasty.QuickCheck

import qualified Database.LSMTree.Internal.BitVec as BV
import           Database.LSMTree.Internal.RawPage
import           FormatPage (Key (..), Operation (..), PageLogical (..),
                     Value (..), encodePage, serialisePage)

tests :: TestTree
tests = testGroup "Database.LSMTree.Internal.RawPage"
    [ testCase "empty" $ do
        -- directory:
        -- * 0 keys,
        -- * 0 blobs
        -- * key's offset offset at 8 bytes (as bitmaps are empty)
        -- * nil spare
        --
        -- and
        -- offset past last value.
        let bytes :: [Word16]
            bytes = [0, 0, 8, 0, 10]

        let page = makeRawPage (byteArrayFromList bytes) 0

        page @=? toRawPage (PageLogical [])
        rawPageNumKeys page @=? 0
        rawPageNumBlobs page @=? 0
        rawPageKeyOffsets page @=? V.fromList []

    , testCase "single-insert" $ do
        let bytes :: [Word16]
            bytes =
                [ 1, 0, 24, 0             -- directory
                , 0, 0, 0, 0, 0, 0, 0, 0  -- ...
                , 32                      -- key offsets
                , 34, 36                  -- value offsets
                , 0x00                    -- single key case (top bits of 32bit offset)
                , byteSwap16 0x4243       -- key
                , byteSwap16 0x8899       -- value
                ]

        let page = makeRawPage (byteArrayFromList bytes) 0

        page @=? toRawPage (PageLogical [(Key "\x42\x43", Insert (Value "\x88\x99"), Nothing)])
        rawPageNumKeys page @=? 1
        rawPageNumBlobs page @=? 0
        rawPageKeyOffsets page @=? V.fromList [32]
        rawPageBlobRefs page @?= UV.fromList [BV.Bit False]

    , testProperty "blobrefs" prop_blobrefs
    ]

toRawPage :: PageLogical -> RawPage
toRawPage p = case SBS.toShort $ serialisePage $ encodePage p of
    SBS.SBS ba -> makeRawPage (ByteArray ba) 0

prop_blobrefs :: PageLogical -> Property
prop_blobrefs p@(PageLogical xs) =
    -- label (show hasBlobRefs) $
    rawPageBlobRefs (toRawPage p) === UV.fromList (coerce hasBlobRefs)
  where
    hasBlobRefs :: [Bool]
    hasBlobRefs = [ isJust mb | (_, _, mb) <- xs ]
