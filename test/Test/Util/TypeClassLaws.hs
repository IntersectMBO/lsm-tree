{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# OPTIONS_GHC -Wno-orphans  #-}

{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ExplicitForAll      #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}
{-# LANGUAGE ViewPatterns        #-}

-- Testing type-class laws necissarily requires
-- definitions which HLint finds superfluous
{- HLINT ignore "Use first" -}
{- HLINT ignore "Use second" -}
{- HLINT ignore "Redundant bimap" -}
{- HLINT ignore "Use bimap" -}
{- HLINT ignore "Monad law, right identity" -}
{- HLINT ignore "Monad law, left identity" -}
{- HLINT ignore "Use >=>" -}
{- HLINT ignore "Use =<<" -}
{- HLINT ignore "Monoid law, left identity" -}
{- HLINT ignore "Monoid law, right identity" -}
{- HLINT ignore "Redundant toList" -}
{- HLINT ignore "Use null" -}

-- |
-- Exports generalized, property-based type-class laws for usage in other
-- test-suite modules.
module Test.Util.TypeClassLaws
    ( -- * Type Class Laws
      -- ** Basic control structures
      functorLaws
    , bifunctorLaws
    , applicativeLaws
    , monadLaws
    , monadFailLaws
      -- ** Refined control structures
    , altLaws
    , applyLaws
    , bindLaws
      -- ** Ordered container structures
    , foldableLaws
    , traversableLaws
      -- ** Numeric-like
    , boundedLaws
    , enumLaws
    , numLaws
      -- ** Group-like
    , semigroupLaws
    , monoidLaws
      -- ** Orderable
    , equalityLaws
    , orderingLaws
      -- ** Other
    , normalFormDataLaws
    , showProperties
    ) where

import           Control.Applicative (Alternative (..))
import           Control.DeepSeq
import           Control.Monad (join)
import           Data.Bifunctor
import           Data.Foldable
import           Data.Functor.Compose
import           Data.Functor.Identity
import           Data.Semigroup
import           Test.QuickCheck.Function
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.QuickCheck hiding ((=/=))

-- |
-- This alias exists for brevity in type signatures
type W = Word

functorLaws
  :: forall f.
     ( Arbitrary (f W)
     , Eq (f W)
     , Functor f
     , Show (f W)
     )
  => TestTree
functorLaws = testGroup "Functor Laws"
    [ testLaw functorIdentity    "Identity"    "fmap id === id"
    , testLaw functorComposition "Composition" "fmap (f . g) === fmap f . fmap g"
    ]
  where
    functorIdentity :: f W -> Property
    functorIdentity x =
        fmap id x === x

    functorComposition :: f W -> Fun W W -> Fun W W -> Property
    functorComposition x (apply -> f) (apply -> g) =
        fmap (g . f) x === (fmap g . fmap f $ x)

bifunctorLaws
  :: forall f.
     ( Arbitrary (f W W)
     , Eq (f W W)
     , Bifunctor f
     , Show (f W W)
     )
  => TestTree
bifunctorLaws = testGroup "Bifunctor Laws"
    [ testLaw bifunctorIdentity    "Identity"    "bimap id id === id"
    , testLaw bifunctorComposition "Composition" "bimap f g === first f . second g"
    ]
  where
    bifunctorIdentity :: f W W -> Property
    bifunctorIdentity x =
        bimap id id x === x

    bifunctorComposition :: f W W -> Fun W W -> Fun W W -> Property
    bifunctorComposition x (apply -> f) (apply -> g) =
        bimap f g x === (first f . second g) x

applicativeLaws
  :: forall f.
     ( Applicative f
     , Arbitrary (f W)
     , Arbitrary (f (Fun W W))
     , Eq (f W)
     , Show (f W)
     , Show (f (Fun W W))
     )
  => TestTree
applicativeLaws = testGroup "Applicative Laws"
    [ testLaw applicativeIdentity     "Identity"     "pure id <*> v === v"
    , testLaw applicativeComposition  "Composition"  "pure (.) <*> u <*> v <*> w === u <*> (v <*> w)"
    , testLaw applicativeHomomorphism "Homomorphism" "pure f <*> pure x = pure (f x)"
    , testLaw applicativeInterchange  "Interchange"  "u <*> pure y === pure ($ y) <*> u"
    ]
  where
    applicativeIdentity :: f W -> Property
    applicativeIdentity x =
        (pure id <*> x) === x

    applicativeComposition :: f (Fun W W) -> f (Fun W W) -> f W -> Property
    applicativeComposition (fmap apply -> x) (fmap apply -> y) z =
        (pure (.) <*> x <*> y <*> z) === (x <*> (y <*> z))

    applicativeInterchange :: f (Fun W W) -> W -> Property
    applicativeInterchange (fmap apply -> x) y =
        (x <*> pure y) === (pure ($ y) <*> x)

    applicativeHomomorphism :: Fun W W -> W -> Property
    applicativeHomomorphism (apply -> f) x =
        (pure f <*> pure x) === (pure (f x) :: f W)

applyLaws
  :: forall f.
     ( Applicative f
     , Arbitrary (f W)
     , Arbitrary (f (Fun W W))
     , Eq (f W)
     , Eq (f (f W))
     , Show (f W)
     , Show (f (f W))
     , Show (f (Fun W W))
     )
  => TestTree
applyLaws = testGroup "Apply Laws"
    [ testLaw composition        "Composition"         "(.) <$> u <*> v <*> w = u <*> (v <*> w)"
    , testLaw leftInterchange    "Left Interchange"    "x <*> (f <$> y) = (. f) <$> x <*> y"
    , testLaw rightInterchange   "Right Interchange"   "f <$> (x <*> y) = (f .) <$> x <*> y"
    , testLaw leftNullification  "Left Nullification"  "(mf <$> m) *> (nf <$> n) = nf <$> (m *> n)"
    , testLaw rightNullification "Right Nullification" "(mf <$> m) <* (nf <$> n) = mf <$> (m <* n)"
    ]
  where
    composition :: f (Fun W W) -> f (Fun W W) -> f W -> Property
    composition (fmap apply -> x) (fmap apply -> y) z =
        ((.) <$> x <*> y <*> z) === (x <*> (y <*> z))

    leftInterchange :: Fun W W -> f (Fun W W) -> f W -> Property
    leftInterchange (apply -> f) (fmap apply -> x) y =
        (x <*> (f <$> y)) === ((. f) <$> x <*> y)

    rightInterchange :: Fun W (f W) -> f (Fun W W) -> f W -> Property
    rightInterchange (apply -> f) (fmap apply -> x) y =
        (f <$> (x <*> y)) === ((f .) <$> x <*> y)

    leftNullification :: Fun W W -> Fun W W -> f W -> f W -> Property
    leftNullification (apply -> f) (apply -> g) m n =
        ((f <$> m) *> (g <$> n)) === (g <$> (m *> n))

    rightNullification :: Fun W W -> Fun W W -> f W -> f W -> Property
    rightNullification (apply -> f) (apply -> g) m n =
        ((f <$> m) <* (g <$> n)) === (f <$> (m <* n))

monadLaws
  :: forall m.
     ( Arbitrary (m W)
     , Eq (m W)
     , Monad m
     , Show (m W)
     )
  => TestTree
monadLaws = testGroup "Monad Laws"
    [ testLaw monadLeftIdentity  "Left Identity"  "return a >>= k === k a"
    , testLaw monadRightIdentity "Right Identity" "m >>= return === m"
    , testLaw monadAssociativity "Associativity"  "m >>= (x -> k x >>= h) === (m >>= k) >>= h"
    ]
  where
    monadRightIdentity :: m W -> Property
    monadRightIdentity x =
        (x >>= pure) === x

    monadLeftIdentity  :: W -> Fun W (m W) -> Property
    monadLeftIdentity x (apply -> f) =
        (pure x >>= f) === f x

    monadAssociativity :: m W -> Fun W (m W) -> Fun W (m W) -> Property
    monadAssociativity x (apply -> f) (apply -> g) =
        ((x >>= f) >>= g) === (x >>= (\x' -> f x' >>= g))

monadFailLaws
  :: forall m.
     ( Arbitrary (m W)
     , Eq (m W)
     , MonadFail m
     , Show (m W)
     )
  => TestTree
monadFailLaws = testGroup "MonadFail Laws"
    [ testLaw leftNullification "Left Nullification" "fail s >>= f === fail s"
    ]
  where
    leftNullification :: Fun W (m W) -> String -> Property
    leftNullification (apply -> f) s =
        (fail s >>= f) === (fail s :: m W)

altLaws
  :: forall f.
     ( Alternative f
     , Arbitrary (f W)
     , Eq (f W)
     , Show (f W)
     )
  => TestTree
altLaws = testGroup "Alt Laws"
    [ testLaw altAssociativity       "Associativity"          "x <|> (y <|> z) === (x <|> y) <|> z"
    , testLaw altLeftCatch           "Left Catch"             "pure x <|> y = pure x"
    , testLaw altLeftDistributivity1 "Left Distributivity I"  "f <$> (x <|> y) === (f <$> x) <|> (f <$> y)"
-- These laws do not hold for our 'Either-like' data type.
-- This is okay (apparently) since the 'Left Catch' law holds.
--    , "Left Distributivity II" "(x <|> y) <*> z === (x <*> z) <|> (y <*> z)"
--    , "Right Distributivity"   "(m <|> n) >>- f === (m >>- f) <|> (m >>- f)"
    ]
  where
    altAssociativity :: f W -> f W -> f W -> Property
    altAssociativity x y z =
        ((x <|> y) <|> z) === (x <|> (y <|> z))

    altLeftCatch :: W -> f W -> Property
    altLeftCatch x y =
        (pure x <|> y) === pure x

    altLeftDistributivity1 :: Fun W W -> f W -> f W -> Property
    altLeftDistributivity1 (apply -> f) x y =
        (f <$> (x <|> y)) === ((f <$> x) <|> (f <$> y))

bindLaws
  :: forall m.
     ( Arbitrary (m W)
     , Arbitrary (m (m W))
     , Arbitrary (m (m (m W)))
     , Arbitrary (m (Fun W W))
     , Monad m
     , Eq (m W)
     , Show (m W)
     , Show (m (m W))
     , Show (m (m (m W)))
     , Show (m (Fun W W))
     )
  => TestTree
bindLaws = testGroup "Bind Laws"
    [ testLaw defJoin        "Definition of join"  "join === (>>= id)"
    , testLaw defBind        "Definition of bind"  "m >>= f === join (fmap f m)"
    , testLaw defApply       "Definition of apply" "f <*> x === f >>= (<$> x)"
    , testLaw associativity1 "Associativity I"     "(m >>= f) >>= g === m >>= (\\x -> f x >>= g)"
    , testLaw associativity2 "Associativity II"    "join . join === join . mmap join"
    ]
  where
    defJoin :: m (m W) -> Property
    defJoin x =
        join x === (>>= id) x

    defBind :: Fun W (m W) -> m W -> Property
    defBind (apply -> f) x =
        (x >>= f) === join (fmap f x)

    defApply :: m (Fun W W) -> m W -> Property
    defApply (fmap apply -> f) x =
        (f <*> x) === (f >>= (<$> x))

    associativity1 :: Fun W (m W) -> Fun W (m W) -> m W -> Property
    associativity1 (apply -> f) (apply -> g) x =
        ((x >>= f) >>= g) === (x >>= (\a -> f a >>= g))

    associativity2 :: m (m (m W)) -> Property
    associativity2 x =
        (join . join) x === (join . fmap join) x

boundedLaws
  :: forall a.
     ( Arbitrary a
     , Bounded a
     , Enum a
     , Eq a
     , Show a
     )
  => TestTree
boundedLaws = testGroup "Bounded Laws"
    [ testLaw iso_succ "Isomorphism (succ)" "x /= maxBound ==> fromEnum (succ x) === fromEnum x + 1"
    , testLaw iso_pred "Isomorphism (pred)" "x /= minBound ==> fromEnum (pred x) === fromEnum x - 1"
    ]
  where
    iso_succ :: a -> Property
    iso_succ x =
        x /= maxBound ==> fromEnum (succ x) === fromEnum x + 1

    iso_pred :: a -> Property
    iso_pred x =
        x /= minBound ==> fromEnum (pred x) === fromEnum x - 1

enumLaws
  :: forall a.
     ( Arbitrary a
     , Enum a
     , Eq a
     , Show a
     )
  => TestTree
enumLaws = testGroup "Enum Laws"
    [ testLaw iso_id  "Isomorphism (id)"  "(toEnum . fromEnum) x === x"
    , testLaw iso_int "Isomorphism (Int)" "(fromEnum . toEnum) x === x"
    ]
  where
    iso_id :: a -> Property
    iso_id x =
        (toEnum . fromEnum) x === x

    iso_int :: Int -> Property
    iso_int x =
        (fromEnum . (toEnum :: Int -> a)) x === x

numLaws
  :: forall a.
     ( Arbitrary a
     , Eq a
     , Num a
     , Show a
     )
  => TestTree
numLaws = testGroup "Num Laws"
    [ testLaw associativity_plus  "associativity (+)"  "x + (y + z) === (x + y) + z"
    , testLaw identity_L_plus     "identity (L)  (+)"  "0 + x === x"
    , testLaw identity_R_plus     "identity (R)  (+)"  "x + 0 === x"
    , testLaw associativity_times "associativity (*)"  "x * (y * z) === (x * y) * z"
    , testLaw identity_L_times    "identity (L)  (*)"  "1 * x === x"
    , testLaw identity_R_times    "identity (R)  (*)"  "x * 1 === x"
    ]
  where
    associativity_plus :: a -> a -> a -> Property
    associativity_plus x y z =
        x + (y + z) === (x + y) + z

    identity_L_plus :: a -> Property
    identity_L_plus x =
        0 + x === x

    identity_R_plus :: a -> Property
    identity_R_plus x =
        x + 0 === x

    associativity_times :: a -> a -> a -> Property
    associativity_times x y z =
        x * (y * z) === (x * y) * z

    identity_L_times :: a -> Property
    identity_L_times x =
        1 * x === x

    identity_R_times :: a -> Property
    identity_R_times x =
        x * 1 === x

-- | Note: do not use this for data-types with a single inhabitant.
-- The preconditions of @x /= y@ will never be satisfied,
-- causing the tests to fail.
equalityLaws
  :: forall a.
     ( Arbitrary a
     , Eq a
     , Show a
     )
  => TestTree
equalityLaws = testGroup "Equality Laws"
    [ testLaw negation     "Negation"     "x /= y ==> not (x == y)"
    , testLaw symmetry     "Symmetry"     "x /= y ==> y /= x"
    , testLaw transitivity "Transitivity" "x == y && y == z ==> x == z"
    , testLaw refexivity   "Reflexivity"  "x == x"
    ]
  where
    negation :: a -> a -> Property
    negation x y =
        x /= y ==> not (x == y)

    symmetry :: a -> a -> Property
    symmetry x y =
        x /= y ==> y =/= x

    transitivity :: a -> a -> a -> Property
    transitivity x y z =
        not (x == y && y == z) .||. x == z

    refexivity :: a -> Property
    refexivity x =
        x === x

normalFormDataLaws
  :: forall a.
     ( Arbitrary a
     , NFData a
     , Show a
     )
  => TestTree
normalFormDataLaws = testGroup "NFData Laws"
    [ testLaw finiteReduction "Finiteness" "rnf x =/= _|_"
    ]
  where
    finiteReduction :: a -> Property
    finiteReduction x =
        rnf x === ()

orderingLaws
  :: forall a.
     ( Arbitrary a
     , Ord a
     , Show a
     )
  => TestTree
orderingLaws = testGroup "Ordering Laws"
    [ testLaw symmetry       "Symmetry"       "x >= y ==> y <= x"
    , testLaw transitivity1 "Transitive I"  "x < y && y < z ==> x < z"
    , testLaw transitivity2 "Transitive II" "x > y && y > z ==> x > z"
    ]
  where
    symmetry :: a -> a -> Bool
    symmetry lhs rhs =
        case (lhs `compare` rhs, rhs `compare` lhs) of
          (EQ, EQ) -> True
          (GT, LT) -> True
          (LT, GT) -> True
          _        -> False

    transitivity1 :: a -> a -> a -> Property
    transitivity1 x y z =
        (x < y && y < z) ==> x < z

    transitivity2 :: a -> a -> a -> Property
    transitivity2 x y z =
        (x > y && y > z) ==> x > z

semigroupLaws
  :: forall a.
     ( Arbitrary a
     , Eq a
     , Semigroup a
     , Show a
     )
  => TestTree
semigroupLaws = testGroup "Semigroup Laws"
    [ testLaw semigroupAssociativity "Associativity" "x <> (y <> z) === (x <> y) <> z"
    ]
  where
    semigroupAssociativity :: a -> a -> a -> Property
    semigroupAssociativity x y z =
        (x <> (y <> z)) === ((x <> y) <> z)

monoidLaws
  :: forall a.
     ( Arbitrary a
     , Eq a
     , Monoid a
     , Show a
     )
  => TestTree
monoidLaws = testGroup "Monoid Laws"
    [ testLaw identity_L "identity left"  "mempty <> x === x"
    , testLaw identity_R "identity right" "x <> mempty === x"
    ]
  where
    identity_L :: a -> Property
    identity_L x =
        mempty <> x === x

    identity_R :: a -> Property
    identity_R x =
        x <> mempty === x

showProperties
  :: forall a.
     ( Arbitrary a
     , Show a
     )
  => TestTree
showProperties = testGroup "Show Laws"
    [ testLaw finiteString  "Finiteness" "rnf (show x) =/= _|_"
    , testLaw nonNullString "Non-null"   "not . null . show"
    ]
  where
    finiteString :: a -> Property
    finiteString x =
        (rnf . show) x === ()

    nonNullString :: a -> Bool
    nonNullString =
        not . null . show

foldableLaws
  :: forall f.
     ( Arbitrary (f W)
     , Foldable f
     , Show (f W)
     )
  => TestTree
foldableLaws = testGroup "Foldable Laws"
    [ testLaw testFoldlFoldMap "Dual Endomorphism"
        "foldl' f z t === appEndo (getDual (foldMap (Dual . Endo . flip f) t)) z"
    , testLaw testFoldrFoldMap "Fold-map Endomorphism"
        "foldr f z t === appEndo (foldMap (Endo . f) t ) z"
    , testLaw testFoldr "Fold-right List Equivelency"
        "foldr f z === foldr f z . toList"
    , testLaw testFoldl "Fold-right List Equivelency"
        "foldl' f z === foldl' f z . toList"
    , testLaw testFoldr1 "Non-empty Fold-right List Equivelency"
        "foldr1 f === foldr1 f . toList"
    , testLaw testFoldl1 "Non-empty Fold-left List Equivelency"
        "foldl1 f === foldl1 f . toList"
    , testLaw testNull "Zero-length Nullability Implication"
        "null === (0 ==) . length"
    , testLaw testLength "Length List Equivelency"
        "length === length . toList"
    , testLaw testInclusionConsistency "Inclusion Consistency"
        "elem e =/= notElem e"
    , testLaw testMax "Max Fold-map Equivelency"
        "maximum === getMax . foldMap Max"
    , testLaw testMin "Min Fold-map Equivelency"
        "minimum === getMin . foldMap Min"
    , testLaw testSum "Sum Fold-map Equivelency"
        "sum === getSum . foldMap Sum"
    , testLaw testProduct "Product Fold-map Equivelency"
        "product === getProduct . foldMap Product"
    , testLaw testFirst "First Fold-map Equivelency"
        "head . toList === getFirst . foldMap First"
    , testLaw testLast "Last Fold-map Equivelency"
        "last . toList === getLast . foldMap Last"
    , testLaw testAll "All Fold-map Equivelency"
        "all f === getAll . foldMap (All . f)"
    , testLaw testAny "Any Fold-map Equivelency"
        "any f === getAny . foldMap (Any . f)"
    ]
  where
    testFoldrFoldMap :: Fun (W, W) W -> W -> f W -> Property
    testFoldrFoldMap (applyFun2 -> f) z x =
        foldr f z x === appEndo (foldMap (Endo . f) x) z

    testFoldlFoldMap :: Fun (W, W) W -> W -> f W -> Property
    testFoldlFoldMap (applyFun2 -> f) z x =
        foldl' f z x === appEndo (getDual (foldMap (Dual . Endo . flip f) x)) z

    testFoldr :: Fun (W, W) W -> W -> f W -> Property
    testFoldr (applyFun2 -> f) z x =
        foldr f z x === (foldr f z . toList) x

    testFoldl :: Fun (W, W) W -> W -> f W -> Property
    testFoldl (applyFun2 -> f) z x =
        foldl' f z x === (foldl' f z . toList) x

    testFoldr1 :: Fun (W, W) W -> f W -> Property
    testFoldr1 (applyFun2 -> f) x =
        (not . null) x  ==> foldr1 f x === (foldr1 f . toList) x

    testFoldl1 :: Fun (W, W) W -> f W -> Property
    testFoldl1 (applyFun2 -> f) x =
        (not . null) x  ==> foldl1 f x === (foldl1 f . toList) x

    testNull :: f W -> Property
    testNull x =
        null x === ((0 ==) . length) x

    testLength :: f W -> Property
    testLength x =
        length x === (length . toList) x

    testInclusionConsistency :: (W, f W) -> Property
    testInclusionConsistency (e, x) =
        elem e x =/= notElem e x

    testMax :: f W -> Property
    testMax x =
        (not . null) x ==>
            maximum x === (getMax . foldMap Max) x

    testMin :: f W -> Property
    testMin x =
        (not . null) x ==>
            minimum x === (getMin . foldMap Min) x

    testSum :: f W -> Property
    testSum x =
        sum x === (getSum . foldMap Sum) x

    testProduct :: f W -> Property
    testProduct x =
        product x === (getProduct . foldMap Product) x

    testFirst :: f W -> Property
    testFirst x =
        (not . null) x ==>
            (Just . head . toList) x === (fmap getFirst . foldMap (Just . First)) x

    testLast :: f W -> Property
    testLast x =
        (not . null) x ==>
            (Just . last . toList) x === (fmap getLast . foldMap (Just . Last)) x

    testAll :: Fun W Bool -> f W -> Property
    testAll (apply -> f) x =
        all f x === (getAll . foldMap (All . f)) x

    testAny :: Fun W Bool -> f W -> Property
    testAny (apply -> f) x =
        any f x === (getAny . foldMap (Any . f)) x

traversableLaws
  :: forall f.
     ( Arbitrary (f W)
     , Eq (f Bool)
     , Eq (f W)
     , Traversable f
     , Show (f W)
     , Show (f Bool)
     )
  => TestTree
traversableLaws = testGroup "Traversable Laws"
    [ testLaw naturality  "Naturality"  "t . traverse f === traverse (t . f)"
    , testLaw identity    "Identity"    "traverse Identity === Identity"
    , testLaw composition "Composition" "traverse (Compose . fmap g . f) === Compose . fmap (traverse g) . traverse f"
    , testLaw equality    "Definition Equality" "traverse === mapM"
    ]
  where
    naturality :: Fun W [W] -> f W -> Property
    naturality (apply -> f) x =
        (headMay . traverse f) x === traverse (headMay . f) x
      where
        headMay    [] = Nothing
        headMay (a:_) = Just a

    identity :: f W -> Property
    identity x =
        traverse Identity x === Identity x

    composition :: Fun W (Either W Bool) -> Fun Bool (Maybe W) -> f W -> Property
    composition (apply -> f) (apply -> g) x =
        traverse (Compose . fmap g . f) x === (Compose . fmap (traverse g) . traverse f) x

    equality :: Fun W (Maybe Bool) -> f W -> Property
    equality (apply -> f) x =
        traverse f x === mapM f x

testLaw :: Testable a => a -> String -> String -> TestTree
testLaw f lawName lawExpression = testGroup lawName [testProperty lawExpression f ]

-- | Like '/=', but prints a counterexample when it fails.
infix 4 =/=
(=/=) :: (Eq a, Show a) => a -> a -> Property
(=/=) x y = counterexample (show x <> " == " <> show y) (x /= y)
