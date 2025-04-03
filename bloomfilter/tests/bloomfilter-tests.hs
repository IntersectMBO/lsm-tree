{-# LANGUAGE NumericUnderscores #-}
module Main where

import           Control.Monad (forM_)
import qualified Data.BloomFilter.BitVec64 as BV64
import qualified Data.BloomFilter.Easy as B
import           Data.BloomFilter.Hash (Hashable (..), hash64)
import qualified Data.BloomFilter.Internal as BI
import           Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as LBS
import           Data.Int (Int16, Int32, Int64, Int8)
import qualified Data.Vector.Primitive as VP
import           Data.Word (Word32, Word64)
import           System.IO (BufferMode (..), hSetBuffering, stdout)
import           Test.Tasty
import           Test.Tasty.QuickCheck

import           QCSupport (P (..))


main :: IO ()
main = defaultMain tests

tests :: TestTree
tests = testGroup "bloomfilter"
    [ testGroup "easyList"
        [ testProperty "()" $ prop_pai ()
        , testProperty "Char" $ prop_pai (undefined :: Char)
        , testProperty "Word32" $ prop_pai (undefined :: Word32)
        , testProperty "Word64" $ prop_pai (undefined :: Word64)
        , testProperty "ByteString" $ prop_pai (undefined :: ByteString)
        , testProperty "LBS.ByteString" $ prop_pai (undefined :: LBS.ByteString)
        , testProperty "LBS.ByteString" $ prop_pai (undefined :: String)
        ]
    , testGroup "hashes"
        [ testProperty "prop_rechunked_eq" prop_rechunked_eq
        , testProperty "prop_tuple_ex" $
          hash64 (BS.empty, BS.pack [120]) =/= hash64 (BS.pack [120], BS.empty)
        , testProperty "prop_list_ex" $
          hash64 [[],[],[BS.empty]] =/= hash64 [[],[BS.empty],[]]
        ]
    , testGroup "equality"
        [ testProperty "doesn't care about leftover bits a" $
          BI.Bloom 1 48 (BV64.BV64 (VP.singleton 0xffff_0000_1234_5678)) ===
          BI.Bloom 1 48 (BV64.BV64 (VP.singleton 0xeeee_0000_1234_5678))

        , testProperty "doesn't care about leftover bits b" $
          BI.Bloom 1 49 (BV64.BV64 (VP.singleton 0xffff_0000_1234_5678)) =/=
          BI.Bloom 1 49 (BV64.BV64 (VP.singleton 0xeeee_0000_1234_5678))
        ]
    ]

-------------------------------------------------------------------------------
-- Element is in a Bloom filter
-------------------------------------------------------------------------------

prop_pai :: (Hashable a) => a -> a -> [a] -> P -> Property
prop_pai _ x xs (P q) = let bf = B.easyList q (x:xs) in
    B.elem x bf .&&. not (B.notElem x bf)

-------------------------------------------------------------------------------
-- Chunking
-------------------------------------------------------------------------------

-- Ensure that a property over a lazy ByteString holds if we change
-- the chunk boundaries.

rechunk :: Int64 -> LBS.ByteString -> LBS.ByteString
rechunk k xs | k <= 0    = xs
             | otherwise = LBS.fromChunks (go xs)
    where go s | LBS.null s = []
               | otherwise = let (pre,suf) = LBS.splitAt k s
                             in  repack pre : go suf
          repack = BS.concat . LBS.toChunks


prop_rechunked :: (Eq a, Show a) => (LBS.ByteString -> a) -> LBS.ByteString -> Property
prop_rechunked f s =
    let l = LBS.length s
    in l > 0 ==> forAll (choose (1,l-1)) $ \k ->
        let n = k `mod` l
        in n > 0 ==> f s === f (rechunk n s)

prop_rechunked_eq :: LBS.ByteString -> Property
prop_rechunked_eq = prop_rechunked hash64
