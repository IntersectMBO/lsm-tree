{-# LANGUAGE BangPatterns               #-}
{-# LANGUAGE CPP                        #-}
{-# LANGUAGE DerivingStrategies         #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE RankNTypes                 #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# OPTIONS_GHC -fspecialize-aggressively #-}
module Main (main) where

import           Control.DeepSeq (NFData (..), force)
import           Control.Exception (evaluate)
import           Control.Monad.ST.Strict (ST, runST)
import           Data.Bits (unsafeShiftR)
import qualified Data.Heap as Heap
import           Data.IORef
import qualified Data.List as L
import           Data.Primitive.ByteArray (compareByteArrays)
import qualified Data.Vector.Primitive as VP
import           Data.Word (Word64, Word8)
import           System.IO.Unsafe (unsafePerformIO)
import qualified System.Random.SplitMix as SM
import           Test.Tasty (TestName, TestTree, defaultMainWithIngredients,
                     testGroup)
import qualified Test.Tasty.Bench as B
import           Test.Tasty.HUnit (testCase, (@?=))
import           Test.Tasty.QuickCheck (testProperty, (===))

import qualified KMerge.Heap as K.Heap
import qualified KMerge.LoserTree as K.Tree

-- tests and benchmarks for various k-way merge implementations.
-- in short: loser tree is optimal in comparison counts performed,
-- but mutable heap implementation has lower constant factors.
--
-- Noteworthy, maybe not obvious observations:
-- - mutable heap does a similar amount of comparisons as persistent heap
--   (from @heaps@ package) on full trees with evenly sized input lists,
--   but performs more comparisons when these constraints get lifted.
-- - tree-shaped iterative two-way merge performs optimal amount of comparisons
--   loser tree is an explicit state variant of that.
-- - on skewed input sizes, the heap does benefit a little, as two consecutive
--   outputs often come from the same input, which then is already at the root,
--   only requiring two comparisons. sadly, this benefit does not translate as
--   quite as nicely to the mutable implementation.
-- - the loser tree with its balanced tree structure is not optimal for skewed
--   merges, but it can be if the tree structure is managed explicitly.
--   for a hacky proof of concept, see 'loserTreeMerge\'', where we make sure
--   that one side of the tree only contains the large input plus dummy inputs,
--   allowing a path to the root using a single comparison.
-- - 'listMerge' performs very well for skewed inputs since it merges the first
--   (i.e. long) input only once. If the last input is largest, it gets very bad.
--
main :: IO ()
main = do
    _ <- evaluate $ force input8
    _ <- evaluate $ force input7
    _ <- evaluate $ force input5

    defaultMainWithIngredients B.benchIngredients $ testGroup "kmerge"
        [ testGroup "tests"
            [ testGroup "merge"
                [ mergeProperty "listMerge"      listMerge
                , mergeProperty "treeMerge"      treeMerge
                , mergeProperty "heapMerge"      heapMerge
                , mergeProperty "loserTreeMerge" loserTreeMerge
                , mergeProperty "mutHeapMerge"   mutHeapMerge
                ]
            , testGroup "count"
                [ testGroup "eight"
                    -- loserTree comparison upper bounds for 8 inputs is 3 x element count.
                    -- for 8 100-element lists, i.e. 800 elements the total comparison count is 2400
                    -- loserTree (and tree merge) implementations hit exactly that number.
                    --
                    -- (because the input values are unformly random,
                    -- there shouldn't be a lot of "cheap" leftovers elements,
                    -- i.e. when other inputs are exhausted, but there are few)
                    [ testCount "sortConcat"      3190 (L.sort . concat) input8
                    , testCount "listMerge"       3479 listMerge         input8
                    , testCount "treeMerge"       2391 treeMerge         input8
                    , testCount "heapMerge"       3168 heapMerge         input8
                    , testCount "loserTreeMerge"  2391 loserTreeMerge    input8
                    , testCount "mutHeapMerge"    3169 mutHeapMerge      input8
                    ]
                    -- seven inputs: we have 6x100 elements with 3 comparisons
                    -- and 1x100 elements with just 2.
                    -- i.e. target is 2000 total comparisons.
                    --
                    -- The difference here and in five-input case between
                    -- treeMerge and loserTreeMerge is caused by
                    -- different "tournament bracket" assignments done by the
                    -- algorithms.
                    --
                    -- In particular in five case, the treeMerge bracket looks like
                    --
                    --              *
                    --           /     \
                    --       *            5
                    --     /   \
                    --   *       *
                    --  / \     / \
                    -- 1   2   3   4
                    --
                    -- But the LoserTree is balanced:
                    --
                    --              *
                    --           /     \
                    --       *             *
                    --     /   \         /   \
                    --   *       3     4       5
                    --  / \
                    -- 1   2
                    --
                    -- (maybe treeMerge can be better balanced too,
                    --  but I'm too lazy to think how to do that)
                    --
                , testGroup "seven"
                    [ testCount "sortConcat"      2691 (L.sort . concat) input7
                    , testCount "listMerge"       2682 listMerge         input7
                    , testCount "treeMerge"       1992 treeMerge         input7
                    , testCount "heapMerge"       2645 heapMerge         input7
                    , testCount "loserTreeMerge"  1989 loserTreeMerge    input7
                    , testCount "mutHeapMerge"    2570 mutHeapMerge      input7
                    ]
                    -- five inputs: we have 3x100 elements with 2 comparisons
                    -- and 2x100 with 3 comparisons.
                    -- i.e. target is 1200 total comparisons.
                , testGroup "five"
                    [ testCount "sortConcat"      1790 (L.sort . concat) input5
                    , testCount "listMerge"       1389 listMerge         input5
                    , testCount "treeMerge"       1291 treeMerge         input5
                    , testCount "heapMerge"       1485 heapMerge         input5
                    , testCount "loserTreeMerge"  1191 loserTreeMerge    input5
                    , testCount "mutHeapMerge"    1592 mutHeapMerge      input5
                    ]
                    -- minimal skew for a levelling merge of 1000 elements.
                    -- with a tree that gives the long input a short path:
                    -- 1x500 elements with 1 comparison
                    -- 4x125 elements with 3 comparisons
                    -- i.e. target is 2000 total comparisons.
                , testGroup "levelling-min"
                    [ testCount "sortConcat"      3729 (L.sort . concat) inputLevellingMin
                    , testCount "listMerge"       2112 listMerge         inputLevellingMin
                    , testCount "treeMerge"       2730 treeMerge         inputLevellingMin
                    , testCount "heapMerge"       2655 heapMerge         inputLevellingMin
                    , testCount "loserTreeMerge"  2235 loserTreeMerge    inputLevellingMin
                    , testCount "loserTreeMerge'" 1999 loserTreeMerge    inputLevellingMin'
                    , testCount "mutHeapMerge"    3021 mutHeapMerge      inputLevellingMin
                    ]
                    -- maximal skew for a levelling merge of 1000 elements.
                    -- with a tree that gives the long input a short path:
                    -- 1x800 elements with 1 comparison
                    -- 4x 50 elements with 3 comparisons
                    -- i.e. target is 1400 total comparisons.
                , testGroup "levelling-max"
                    [ testCount "sortConcat"      3872 (L.sort . concat) inputLevellingMax
                    , testCount "listMerge"       1440 listMerge         inputLevellingMax
                    , testCount "treeMerge"       2873 treeMerge         inputLevellingMax
                    , testCount "heapMerge"       1784 heapMerge         inputLevellingMax
                    , testCount "loserTreeMerge"  2081 loserTreeMerge    inputLevellingMax
                    , testCount "loserTreeMerge'" 1400 loserTreeMerge    inputLevellingMax'
                    , testCount "mutHeapMerge"    2493 mutHeapMerge      inputLevellingMax
                    ]
                ]
            ]
#ifdef KMERGE_BENCHMARKS
        , testGroup "bench"
            [ testGroup "eight"
                [ B.bench "sortConcat"     $ B.nf (L.sort . concat) input8
                , B.bench "listMerge"      $ B.nf listMerge         input8
                , B.bench "treeMerge"      $ B.nf treeMerge         input8
                , B.bench "heapMerge"      $ B.nf heapMerge         input8
                , B.bench "loserTreeMerge" $ B.nf loserTreeMerge    input8
                , B.bench "mutHeapMerge"   $ B.nf mutHeapMerge      input8
                ]
            , testGroup "seven"
                [ B.bench "sortConcat"     $ B.nf (L.sort . concat) input7
                , B.bench "listMerge"      $ B.nf listMerge         input7
                , B.bench "treeMerge"      $ B.nf treeMerge         input7
                , B.bench "heapMerge"      $ B.nf heapMerge         input7
                , B.bench "loserTreeMerge" $ B.nf loserTreeMerge    input7
                , B.bench "mutHeapMerge"   $ B.nf mutHeapMerge      input7
                ]
            , testGroup "five"
                [ B.bench "sortConcat"     $ B.nf (L.sort . concat) input5
                , B.bench "listMerge"      $ B.nf listMerge         input5
                , B.bench "treeMerge"      $ B.nf treeMerge         input5
                , B.bench "heapMerge"      $ B.nf heapMerge         input5
                , B.bench "loserTreeMerge" $ B.nf loserTreeMerge    input5
                , B.bench "mutHeapMerge"   $ B.nf mutHeapMerge      input5
                ]
            , testGroup "levelling-min"
                [ B.bench "sortConcat"     $ B.nf (L.sort . concat) inputLevellingMin
                , B.bench "listMerge"      $ B.nf listMerge         inputLevellingMin
                , B.bench "treeMerge"      $ B.nf treeMerge         inputLevellingMin
                , B.bench "heapMerge"      $ B.nf heapMerge         inputLevellingMin
                , B.bench "loserTreeMerge" $ B.nf loserTreeMerge    inputLevellingMin
                , B.bench "loserTreeMerge'"$ B.nf loserTreeMerge    inputLevellingMin'
                , B.bench "mutHeapMerge"   $ B.nf mutHeapMerge      inputLevellingMin
                ]
            , testGroup "levelling-max"
                [ B.bench "sortConcat"     $ B.nf (L.sort . concat) inputLevellingMax
                , B.bench "listMerge"      $ B.nf listMerge         inputLevellingMax
                , B.bench "treeMerge"      $ B.nf treeMerge         inputLevellingMax
                , B.bench "heapMerge"      $ B.nf heapMerge         inputLevellingMax
                , B.bench "loserTreeMerge" $ B.nf loserTreeMerge    inputLevellingMax
                , B.bench "loserTreeMerge'"$ B.nf loserTreeMerge    inputLevellingMax'
                , B.bench "mutHeapMerge"   $ B.nf mutHeapMerge      inputLevellingMax
                ]
            ]
#endif
        ]

{-------------------------------------------------------------------------------
  Test utils
-------------------------------------------------------------------------------}

counter :: IORef Int
counter = unsafePerformIO $ newIORef 0
{-# NOINLINE counter #-}

newtype Wrapped a = Wrap a -- { unwrap :: Word256 }

instance Eq a => Eq (Wrapped a) where
    Wrap x == Wrap y = unsafePerformIO $ do
        atomicModifyIORef' counter $ \n -> (1 + n, ())
        return $! x == y
    {-# NOINLINE (==) #-}

instance Ord a => Ord (Wrapped a) where
    compare (Wrap x) (Wrap y) = unsafePerformIO $ do
        atomicModifyIORef' counter $ \n -> (1 + n, ())
        return $! compare x y
    Wrap x < Wrap y = unsafePerformIO $ do
        atomicModifyIORef' counter $ \n -> (1 + n, ())
        return $! x < y
    Wrap x <= Wrap y = unsafePerformIO $ do
        atomicModifyIORef' counter $ \n -> (1 + n, ())
        return $! x <= y

    {-# NOINLINE compare #-}
    {-# NOINLINE (<) #-}
    {-# NOINLINE (<=) #-}

instance NFData a => NFData (Wrapped a) where
    rnf (Wrap x) = rnf x

testCount :: (NFData b, Ord b) => TestName -> Int -> (forall a. Ord a => [[a]] -> [a]) -> [[b]] -> TestTree
testCount name expected f input = testCase name $ do
    n <- readIORef counter
    _ <- evaluate $ force $ f $ map (map Wrap) input
    m <- readIORef counter
    m - n @?= expected
{-# NOINLINE testCount #-}

mergeProperty :: TestName -> (forall a. Ord a => [[a]] -> [a]) -> TestTree
mergeProperty name f = testProperty name $ \xss ->
    let lhs = L.sort (concat xss)
        rhs = f $ map L.sort (xss :: [[Word64]])
    in lhs === rhs

{-------------------------------------------------------------------------------
  Element type
-------------------------------------------------------------------------------}

-- This type corresponds to the @SerialisedKey@ type we are using (or rather the
-- @RawBytes@ it wraps), so the cost of comparisons should be similar.
-- We expect key lengths of 32 bytes.
newtype Element = Element (VP.Vector Word8)
  deriving newtype (Show, NFData)

instance Eq Element where
  bs1 == bs2 = compareBytes bs1 bs2 == EQ

-- | Lexicographical 'Ord' instance.
instance Ord Element where
  compare = compareBytes

-- | Based on @Ord 'ShortByteString'@.
compareBytes :: Element -> Element -> Ordering
compareBytes rb1@(Element vec1) rb2@(Element vec2) =
    let !len1 = sizeofElement rb1
        !len2 = sizeofElement rb2
        !len  = min len1 len2
     in case compareByteArrays ba1 off1 ba2 off2 len of
          EQ | len1 < len2 -> LT
             | len1 > len2 -> GT
          o  -> o
  where
    VP.Vector off1 _size1 ba1 = vec1
    VP.Vector off2 _size2 ba2 = vec2

sizeofElement :: Element -> Int
sizeofElement (Element pvec) = VP.length pvec

genElement :: SM.SMGen -> (Element, SM.SMGen)
genElement g0 = (Element (VP.fromListN 32 bytes), g4)
  where
    -- we expect a shared 16 bit prefix
    bytes = 0 : 0 : concatMap toBytes [w1, w2, w3, w4]
    (!w1, g1) = SM.nextWord64 g0
    (!w2, g2) = SM.nextWord64 g1
    (!w3, g3) = SM.nextWord64 g2
    (!w4, g4) = SM.nextWord64 g3
    toBytes = reverse . take 8 . map fromIntegral . iterate (`unsafeShiftR` 8)

minElement :: Element
minElement = Element (VP.fromListN 32 (L.repeat 0))

{-------------------------------------------------------------------------------
  Inputs
-------------------------------------------------------------------------------}

input8 :: [[Element]]
input8 = take 8 $ inputs 100

-- Seven inputs is not optimal case for "binary tree" patterns.
input7 :: [[Element]]
input7 = take 7 $ inputs 100

-- Five inputs is bad case for "binary tree" patterns.
input5 :: [[Element]]
input5 = take 5 $ inputs 100

-- This input corresponds to a levelling merge with minimal or maximal skew.
-- For each, there are 500 elements total, just as 'input5'.
inputLevellingMin, inputLevellingMax :: [[Element]]
inputLevellingMin =
      head (inputs (4*n))
    : take 4 (tail (inputs n))
  where
    n = 1000 `div` (4+4)
inputLevellingMax =
      head (inputs (16*n))
    : take 4 (tail (inputs n))
  where
    n = 1000 `div` (16+4)

inputLevellingMin', inputLevellingMax' :: [[Element]]
inputLevellingMin' = arrangeInputForLoserTree inputLevellingMin
inputLevellingMax' = arrangeInputForLoserTree inputLevellingMax

-- A hacky way to create a degenerate loser tree where one side of the whole
-- tournament tree effectively only consists of a single (large) input,
-- so it can immediately "play in the final" and get chosen with just one
-- comparison.
arrangeInputForLoserTree :: [[Element]] -> [[Element]]
arrangeInputForLoserTree input =
      head input
    : replicate 3 [minElement]  -- non-empty to be considered during tree building
   ++ tail input

inputs :: Int -> [[Element]]
inputs n =
    [ L.sort $ take n $ L.unfoldr (Just . genElement) $ SM.mkSMGen seed
    | seed <- iterate (3 +) 42
    ]

{-------------------------------------------------------------------------------
  Recursive 2-way merge
-------------------------------------------------------------------------------}

listMerge :: Ord a => [[a]] -> [a]
listMerge []       = []
listMerge [xs]     = xs
listMerge (xs:xss) = merge xs (listMerge xss)

merge :: Ord a => [a] -> [a] -> [a]
merge [] [] = []
merge [] ys = ys
merge xs [] = xs
merge xs@(x:xs') ys@(y:ys')
    | x <= y    = x : merge xs' ys
    | otherwise = y : merge xs ys'

{-------------------------------------------------------------------------------
  Recursive 2-way merge, tree shape
-------------------------------------------------------------------------------}

-- | Like 'listMerge', but merges in binary-tree pattern.
--
-- Given inputs of about the same length, there will be less work in merges.
treeMerge :: Ord a => [[a]] -> [a]
treeMerge [] = []
treeMerge [xs] = xs
treeMerge (xs:ys:xss) = treeMerge (merge xs ys : go xss) where
    go []          = []
    go [vs]        = [vs]
    go (vs:ws:vss) = merge vs ws : go vss

{-------------------------------------------------------------------------------
  Direct k-way merge using heaps Data.Heap.Heap
-------------------------------------------------------------------------------}

heapMerge :: forall a. Ord a => [[a]] -> [a]
heapMerge xss = go $ Heap.fromList
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

{-------------------------------------------------------------------------------
  Direct k-way merge using LoserTree
-------------------------------------------------------------------------------}

loserTreeMerge :: forall a. Ord a => [[a]] -> [a]
loserTreeMerge xss = runST $ do
    -- we reuse Heap.Entry structure here.
    (tree, element) <- K.Tree.newLoserTree [ Heap.Entry x xs | x:xs <- xss ]
    go tree element
  where
    go :: K.Tree.MutableLoserTree s (Heap.Entry a [a]) -> Maybe (Heap.Entry a [a]) -> ST s [a]
    go !_    Nothing                  = return []
    go !tree (Just (Heap.Entry x xs)) = fmap (x :) $ case xs of
        []     -> K.Tree.remove tree                      >>= go tree
        x':xs' -> K.Tree.replace tree (Heap.Entry x' xs') >>= go tree . Just

{-------------------------------------------------------------------------------
  Direct k-way merge using MutableHeap
-------------------------------------------------------------------------------}

mutHeapMerge :: forall a. Ord a => [[a]] -> [a]
mutHeapMerge xss = runST $ do
    -- we reuse Heap.Entry structure here.
    (heap, element) <- K.Heap.newMutableHeap [ Heap.Entry x xs | x:xs <- xss ]
    go heap element
  where
    go :: K.Heap.MutableHeap s (Heap.Entry a [a]) -> Maybe (Heap.Entry a [a]) -> ST s [a]
    go !_    Nothing                  = return []
    go !heap (Just (Heap.Entry x xs)) = fmap (x :) $ case xs of
        []     -> K.Heap.extract     heap                     >>= go heap
        x':xs' -> K.Heap.replaceRoot heap (Heap.Entry x' xs') >>= go heap . Just
