{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Main (main) where

import           Control.DeepSeq (force)
import           Control.Exception (evaluate)
import qualified Data.Heap as Heap
import qualified Data.List as L
import           Data.WideWord.Word256 (Word256 (..))
import           Data.Word (Word64)
import qualified System.Random.SplitMix as SM
import           Test.Tasty (defaultMainWithIngredients, testGroup)
import qualified Test.Tasty.Bench as B
import           Test.Tasty.QuickCheck (Property, testProperty, (===))

main :: IO ()
main = do
    _ <- evaluate $ force input8
    _ <- evaluate $ force input5

    defaultMainWithIngredients B.benchIngredients $ testGroup "kmerge"
        [ testGroup "tests"
            [ testProperty "twoWayMerge"   prop_twoWayMerge
            , testProperty "twoWayMerge2"  prop_twoWayMerge2
            , testProperty "heapKWayMerge" prop_kWayMerge
            ]
        , testGroup "bench"
            [ testGroup "eight"
                [ B.bench "sortConcat"    $ B.nf (L.sort . concat)     input8
                , B.bench "twoWayMerge"   $ B.nf recursiveTwoWayMerge  input8
                , B.bench "twoWayMerge2"  $ B.nf recursiveTwoWayMerge2 input8
                , B.bench "heapKWayMerge" $ B.nf heapKWayMerge         input8
                ]
            , testGroup "five"
                [ B.bench "sortConcat"    $ B.nf (L.sort . concat)     input5
                , B.bench "twoWayMerge"   $ B.nf recursiveTwoWayMerge  input5
                , B.bench "twoWayMerge2"  $ B.nf recursiveTwoWayMerge2 input5
                , B.bench "heapKWayMerge" $ B.nf heapKWayMerge         input5
                ]
            ]
        ]

-- Using Word256 to make key comparison a bit more expensive.
input8 :: [[Word256]]
input8 =
    [ L.sort $ take 100 $ L.unfoldr (Just . genWord256) $ SM.mkSMGen seed
    | seed <- [1..8]
    ]

-- Five inputs is bad case for "binary tree" patterns.
input5 :: [[Word256]]
input5 = take 5 input8

genWord256 :: SM.SMGen -> (Word256, SM.SMGen)
genWord256 g0 =
    let (!w1, g1) = SM.nextWord64 g0
        (!w2, g2) = SM.nextWord64 g1
        (!w3, g3) = SM.nextWord64 g2
        (!w4, g4) = SM.nextWord64 g3
    in (Word256 w1 w2 w3 w4, g4)

{-------------------------------------------------------------------------------
  Recursive 2-way merge
-------------------------------------------------------------------------------}

prop_twoWayMerge :: [[Word64]] -> Property
prop_twoWayMerge xss = lhs === rhs where
    lhs = L.sort (concat xss)
    rhs = recursiveTwoWayMerge $ map L.sort xss

recursiveTwoWayMerge :: Ord a => [[a]] -> [a]
recursiveTwoWayMerge []       = []
recursiveTwoWayMerge [xs]     = xs
recursiveTwoWayMerge (xs:xss) = merge xs (recursiveTwoWayMerge xss)

merge :: Ord a => [a] -> [a] -> [a]
merge [] [] = []
merge [] ys = ys
merge xs [] = xs
merge xs@(x:xs') ys@(y:ys')
    | x <= y    = x : merge xs' ys
    | otherwise = y : merge xs ys'

{-------------------------------------------------------------------------------
  Recursive 2-way merge 2
-------------------------------------------------------------------------------}

prop_twoWayMerge2 :: [[Word64]] -> Property
prop_twoWayMerge2 xss = lhs === rhs where
    lhs = L.sort (concat xss)
    rhs = recursiveTwoWayMerge2 $ map L.sort xss

-- | Like 'recursiveTwoWayMerge', but merges in binary-tree pattern.
--
-- Given inputs of about the same length, there will be less work in merges.
recursiveTwoWayMerge2 :: Ord a => [[a]] -> [a]
recursiveTwoWayMerge2 [] = []
recursiveTwoWayMerge2 [xs] = xs
recursiveTwoWayMerge2 (xs:ys:xss) = recursiveTwoWayMerge2 (merge xs ys : go xss) where
    go []          = []
    go [vs]        = [vs]
    go (vs:ws:vss) = merge vs ws : go vss

{-------------------------------------------------------------------------------
  Direct k-way merge using heaps Data.Heap.Heap
-------------------------------------------------------------------------------}

prop_kWayMerge :: [[Word64]] -> Property
prop_kWayMerge xss = lhs === rhs where
    lhs = L.sort (concat xss)
    rhs = heapKWayMerge $ map L.sort xss

heapKWayMerge :: forall a. Ord a => [[a]] -> [a]
heapKWayMerge xss = go $ Heap.fromList
    [ Heap.Entry x xs
    | x:xs <- xss
    ]
  where
    go :: Heap.Heap (Heap.Entry a [a]) -> [a]
    go heap = case Heap.viewMin heap of
        Nothing -> []
        Just (Heap.Entry x xs, heap') -> x : case xs of
            []     -> go heap'
            x':xs' -> go (Heap.insert (Heap.Entry x' xs') heap')
