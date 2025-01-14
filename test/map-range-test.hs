{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -Wno-orphans #-}
module Main (main) where

import           Data.ByteString (ByteString)
import           Data.Map (Map)
import qualified Data.Map as Map
import           Data.Map.Range (Bound (..), Clusive (..), rangeLookup)
import           Test.QuickCheck (Arbitrary (..), Property, elements, frequency,
                     (===))
import           Test.Tasty (defaultMain, testGroup)
import           Test.Tasty.HUnit (testCase, (@?=))
import           Test.Tasty.QuickCheck (testProperty)

main :: IO ()
main = defaultMain $ testGroup "map-range-test"
    [ testProperty "model" prop
    , testCase "example1" $ do
        let m = Map.fromList [(0 :: Int, 'x'), (2, 'y')]

        rangeLookup (Bound 0 Inclusive) (Bound 1 Inclusive) m @?= [(0, 'x')]

    , testCase "example2" $ do
        let m = Map.fromList [("\NUL\NUL\NUL\NUL\NUL\NUL\NUL\SOH" :: ByteString,'x')]

        let lb = Bound "\NUL\NUL\NUL\NUL\NUL\NUL\NUL\SOH" Inclusive
            ub = Bound "\NUL\NUL\NUL\NUL\NUL\NUL\NUL\STX" Inclusive

        rangeLookup lb ub m @?= [("\NUL\NUL\NUL\NUL\NUL\NUL\NUL\SOH",'x')]

    , testCase "unordered-bounds" $ do
        let m = Map.fromList [('x', "ex"), ('y', "why" :: String)]

        -- if lower bound is greater than upper bound empty list is returned.
        rangeLookup (Bound 'z' Inclusive) (Bound 'a' Inclusive) m @?= []
        naiveRangeLookup (Bound 'z' Inclusive) (Bound 'a' Inclusive) m @?= []
    ]

prop :: Bound Int -> Bound Int -> Map Int Int -> Property
prop lb ub m =
    rangeLookup lb ub m === naiveRangeLookup lb ub m

naiveRangeLookup ::
       Ord k
    => Bound k    -- ^ lower bound
    -> Bound k    -- ^ upper bound
    -> Map k v
    -> [(k, v)]
naiveRangeLookup lb ub m =
    [ p
    | p@(k, _) <- Map.toList m
    , evalLowerBound lb k
    , evalUpperBound ub k
    ]

evalLowerBound :: Ord k => Bound k -> k -> Bool
evalLowerBound NoBound             _ = True
evalLowerBound (Bound b Exclusive) k = b < k
evalLowerBound (Bound b Inclusive) k = b <= k

evalUpperBound :: Ord k => Bound k -> k -> Bool
evalUpperBound NoBound             _ = True
evalUpperBound (Bound b Exclusive) k = k < b
evalUpperBound (Bound b Inclusive) k = k <= b

instance Arbitrary k => Arbitrary (Bound k) where
    arbitrary = frequency
        [ (1, pure NoBound)
        , (20, Bound <$> arbitrary <*> arbitrary)
        ]

    shrink NoBound     = []
    shrink (Bound k b) = NoBound : map (`Bound` b) (shrink k)

instance Arbitrary Clusive where
    arbitrary = elements [Exclusive, Inclusive]
