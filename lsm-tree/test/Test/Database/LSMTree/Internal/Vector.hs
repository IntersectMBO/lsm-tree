{-# OPTIONS_GHC -Wno-orphans #-}

module Test.Database.LSMTree.Internal.Vector (tests) where

import           Control.Monad (forM_)
import           Control.Monad.ST
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import           Data.Word
import           Database.LSMTree.Extras
import           Database.LSMTree.Internal.Index.CompactAcc
import           Database.LSMTree.Internal.Map.Range
import           Test.QuickCheck
import           Test.QuickCheck.Instances ()
import           Test.QuickCheck.Monadic (PropertyM, monadicST, run)
import           Test.Tasty (TestTree, localOption, testGroup)
import           Test.Tasty.QuickCheck (QuickCheckTests (QuickCheckTests),
                     testProperty)
import           Test.Util.Orphans ()
import           Text.Printf (printf)

tests :: TestTree
tests = testGroup "Test.Database.LSMTree.Internal.Vector" [
    localOption (QuickCheckTests 400) $
    testProperty "propWriteRange" $ \v lb ub (x :: Word8) -> monadicST $ do
      mv <- run $ VU.thaw v
      propWriteRange mv lb ub x
  , localOption (QuickCheckTests 400) $
    testProperty "propUnsafeWriteRange" $ \v lb ub (x :: Word8) -> monadicST $ do
      mv <- run $ VU.thaw v
      propUnsafeWriteRange mv lb ub x
  ]

instance Arbitrary (Bound Int) where
  arbitrary = oneof [
        pure NoBound
      , BoundInclusive <$> arbitrary
      , BoundExclusive <$> arbitrary
      ]
  shrink = \case
      NoBound -> []
      BoundInclusive x -> NoBound : (BoundInclusive <$> shrink x)
      BoundExclusive x -> NoBound : (BoundInclusive <$> shrink x)
                                  ++ (BoundExclusive <$> shrink x)

intToInclusiveLowerBound :: Bound Int -> Int
intToInclusiveLowerBound = \case
    NoBound          -> 0
    BoundInclusive i -> i
    BoundExclusive i -> i + 1

intToInclusiveUpperBound :: VUM.Unbox a => VU.Vector a -> Bound Int -> Int
intToInclusiveUpperBound xs = \case
    NoBound          -> VU.length xs - 1
    BoundInclusive i -> i
    BoundExclusive i -> i - 1

-- | Safe version of 'unsafeWriteRange', used to test the unsafe version
-- against.
writeRange :: VU.Unbox a => VU.MVector s a -> Bound Int -> Bound Int -> a -> ST s Bool
writeRange !v !lb !ub !x
  | 0 <= lb' && lb' < VUM.length v
  , 0 <= ub' && ub' < VUM.length v
  , lb' <= ub'
  = forM_ [lb' .. ub'] (\j -> VUM.write v j x) >> pure True
  | otherwise = pure False
  where
    !lb' = vectorLowerBound lb
    !ub' = mvectorUpperBound v ub

propWriteRange :: forall s a. (VUM.Unbox a, Eq a, Show a)
  => VU.MVector s a
  -> Bound Int
  -> Bound Int
  -> a
  -> PropertyM (ST s) Property
propWriteRange mv1 lb ub x = run $ do
    v1 <- VU.unsafeFreeze mv1
    v2 <- VU.freeze mv1
    b <- writeRange mv1 lb ub x

    let xs1 = zip [0 :: Int ..] $ VU.toList v1
        xs2 = zip [0..]         $ VU.toList v2
        lb' = intToInclusiveLowerBound lb
        ub' = intToInclusiveUpperBound v1 ub

    pure $ tabulate "range size" [showPowersOf10 (ub' - lb' + 1)] $
           tabulate "vector size" [showPowersOf10 (VU.length v1)] $
      if not b then
        label "no suitable range" $ xs1 === xs2
      else
        counterexample (printf "lb=%d" lb') $
        counterexample (printf "ub=%d" ub') $
        conjoin [
            counterexample "mismatch in prefix" $
              take (lb' - 1) xs1 === take (lb' - 1) xs2
          , counterexample "mismatch in suffix" $
              drop (ub' + 1) xs1 === drop (ub' + 1) xs2
          , counterexample "mimsatch in infix" $
              fmap snd (drop lb' (take (ub' + 1) xs1)) ===
              replicate (ub' - lb' + 1) x
          ]

propUnsafeWriteRange ::
     forall s a. (VUM.Unbox a, Eq a, Show a)
  => VU.MVector s a
  -> Bound Int
  -> Bound Int
  -> a
  -> PropertyM (ST s) Property
propUnsafeWriteRange mv1 lb ub x = run $ do
    v1 <- VU.unsafeFreeze mv1
    v2 <- VU.freeze mv1
    mv2 <- VU.unsafeThaw v2
    b <- writeRange mv1 lb ub x
    if not b then
      pure $ label "no suitable range" True
    else do
      unsafeWriteRange mv2 lb ub x
      pure $ v1 === v2
