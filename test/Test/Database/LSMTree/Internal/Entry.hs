{-# LANGUAGE DerivingVia                #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE TypeApplications           #-}

module Test.Database.LSMTree.Internal.Entry (tests) where

import           Data.Coerce
import           Data.List.NonEmpty (NonEmpty)
import qualified Data.List.NonEmpty as NE
import           Data.Semigroup
import           Database.LSMTree.Extras.Generators ()
import           Database.LSMTree.Internal.BlobRef
import           Database.LSMTree.Internal.Entry
import qualified Database.LSMTree.Internal.Monoidal as Monoidal
import qualified Database.LSMTree.Internal.Normal as Normal
import           Test.QuickCheck
import           Test.QuickCheck.Classes (semigroupLaws)
import           Test.Tasty
import           Test.Tasty.HUnit
import           Test.Tasty.QuickCheck (QuickCheckMaxSize (QuickCheckMaxSize),
                     QuickCheckTests (QuickCheckTests), testProperty)
import           Test.Util.QC

tests :: TestTree
tests = adjustOption (\_ -> QuickCheckTests 10000) $
        adjustOption (\_ -> QuickCheckMaxSize 1000) $
    testGroup "Test.Database.LSMTree.Internal.Entry" [
      testClassLaws "EntrySG" $
        semigroupLaws (Proxy @(EntrySG (Sum Int) BlobSpanSG))
    , testClassLaws "NormalUpdateSG" $
        semigroupLaws (Proxy @(NormalUpdateSG (Sum Int) String))
    , testClassLaws "MonoidalUpdateSG" $
        semigroupLaws (Proxy @(MonoidalUpdateSG (Sum Int)))
    , testProperty "prop_resolveEntriesNormalSemantics" $
        prop_resolveEntriesNormalSemantics @Int @String
    , testProperty "prop_resolveMonoidalSemantics" $
        prop_resolveMonoidalSemantics @(Sum Int)
    , testCase "example resolveEntriesNormal" $ do
        let es = [InsertWithBlob (2 :: Int) 'a', Insert 17]
        Just (Normal.Insert 2 (Just 'a')) @=?
          resolveEntriesNormal (NE.fromList es)
    , testCase "example resolveEntriesMonoidal" $ do
        let es = [Mupdate (Sum 11 :: Sum Int), Mupdate (Sum 5), Insert 1]
        Just (Monoidal.Insert (Sum 17)) @=?
          resolveEntriesMonoidal (<>) (NE.fromList es)
    ]

prop_resolveEntriesNormalSemantics ::
     (Show v, Show blob, Eq v, Eq blob)
  => NonEmpty (Normal.Update v blob)
  -> Property
prop_resolveEntriesNormalSemantics es = real === expected
  where expected = coerce (Just . foldr1 (<>) . fmap NormalUpdateSG $ es)
        real     = resolveEntriesNormal (fmap updateToEntryNormal es)

prop_resolveMonoidalSemantics ::
     (Show v, Eq v, Semigroup v)
  => NonEmpty (Monoidal.Update v) -> Property
prop_resolveMonoidalSemantics es = real === expected
  where expected = coerce (Just . foldr1 (<>) . fmap MonoidalUpdateSG $ es)
        real     = resolveEntriesMonoidal (<>) (fmap updateToEntryMonoidal es)

-- | Semigroup wrapper for 'Normal.Update'
newtype NormalUpdateSG v blob = NormalUpdateSG (Normal.Update v blob)
  deriving stock (Show, Eq)
  deriving newtype (Arbitrary)
  deriving Semigroup via First (Normal.Update v blob)

-- | Semigroup wrapper for 'Monoidal.Update'
newtype MonoidalUpdateSG v = MonoidalUpdateSG (Monoidal.Update v)
  deriving stock (Show, Eq)
  deriving newtype (Arbitrary)

instance Semigroup v => Semigroup (MonoidalUpdateSG v) where
  (<>) = coerce $ \upd1 upd2 -> case (upd1 :: Monoidal.Update v, upd2) of
      (e1@Monoidal.Delete  , _                  ) -> e1
      (e1@Monoidal.Insert{}, _                  ) -> e1
      (Monoidal.Mupsert v1 , Monoidal.Delete    ) -> Monoidal.Insert v1
      (Monoidal.Mupsert v1 , Monoidal.Insert  v2) -> Monoidal.Insert (v1 <> v2)
      (Monoidal.Mupsert v1 , Monoidal.Mupsert v2) -> Monoidal.Mupsert (v1 <> v2)

-- | Semigroup wrapper for Entry
newtype EntrySG v blob = EntrySG (Entry v blob)
  deriving stock (Show, Eq)

-- | As long as values are a semigroup, an Entry is too
instance Semigroup v => Semigroup (EntrySG v blob) where
  EntrySG e1 <> EntrySG e2 = EntrySG (combine (<>) e1 e2)

instance (Arbitrary v, Arbitrary blob) => Arbitrary (EntrySG v blob) where
  arbitrary = arbitrary2
  shrink = shrink2

instance Arbitrary2 EntrySG where
  liftArbitrary2 genVal genBlob = EntrySG <$> frequency
    [ (1, Insert <$> genVal)
    , (1,  InsertWithBlob <$> genVal <*> genBlob)
    , (1,  Mupdate <$> genVal)
    , (1,  pure Delete)
    ]

  liftShrink2 shrinkVal shrinkBlob = coerce $ \case
    Insert v           -> Delete : (Insert <$> shrinkVal v)
    InsertWithBlob v b -> [Delete, Insert v]
                       ++ [ InsertWithBlob v' b'
                          | (v', b') <- liftShrink2 shrinkVal shrinkBlob (v, b)
                          ]
    Mupdate v          -> Delete : Insert v : (Mupdate <$> shrinkVal v)
    Delete             -> []

newtype BlobSpanSG = BlobSpanSG BlobSpan
  deriving stock (Show, Eq)

instance Arbitrary BlobSpanSG where
  arbitrary = coerce (BlobSpan <$> arbitrary <*> arbitrary)
  shrink = coerce $ \(BlobSpan x y)  -> [ BlobSpan x' y' | (x', y') <- shrink2 (x, y) ]
