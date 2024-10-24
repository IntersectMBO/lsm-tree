{-# LANGUAGE DerivingVia                #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE TypeApplications           #-}

module Test.Database.LSMTree.Internal.Entry (tests) where

import           Data.Coerce
import           Data.List.NonEmpty (NonEmpty)
import           Data.Semigroup
import           Database.LSMTree.Extras.Generators ()
import           Database.LSMTree.Internal.BlobRef
import           Database.LSMTree.Internal.Entry
import qualified Database.LSMTree.Internal.Monoidal as Monoidal
import qualified Database.LSMTree.Internal.Normal as Normal
import           Test.QuickCheck
import           Test.QuickCheck.Classes (semigroupLaws)
import           Test.Tasty
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
        -- Note that we are using Unlawful here because mupserts /should/ not
        -- show up for normal updates.
        semigroupLaws (Proxy @(NormalUpdateSG (Unlawful Int) String))
    , testClassLaws "MonoidalUpdateSG" $
        semigroupLaws (Proxy @(MonoidalUpdateSG (Sum Int)))
    , testProperty "prop_resolveEntriesNormalSemantics" $
        -- Note that we are using Unlawful here because mupserts /should/ not
        -- show up for normal updates.
        prop_resolveEntriesNormalSemantics @(Unlawful Int) @String
    , testProperty "prop_resolveMonoidalSemantics" $
        prop_resolveMonoidalSemantics @(Sum Int)
    ]

-- | @resolve == fromEntry . resolve . toEntry@
prop_resolveEntriesNormalSemantics ::
     (Show v, Show blob, Eq v, Eq blob, Semigroup v)
  => NonEmpty (Normal.Update v blob)
  -> Property
prop_resolveEntriesNormalSemantics es = expected === real
  where expected = Just . unNormalUpdateSG . sconcat . fmap NormalUpdateSG $ es
        real     = entryToUpdateNormal (sconcat (fmap updateToEntryNormal es))

-- | @resolve == fromEntry . resolve . toEntry@
prop_resolveMonoidalSemantics ::
     (Show v, Eq v, Semigroup v)
  => NonEmpty (Monoidal.Update v) -> Property
prop_resolveMonoidalSemantics es = expected === real
  where expected = Just . unMonoidalUpdateSG . sconcat . fmap MonoidalUpdateSG $ es
        real     = entryToUpdateMonoidal (sconcat (fmap updateToEntryMonoidal es))

{-------------------------------------------------------------------------------
  Types
-------------------------------------------------------------------------------}

-- | A wrapper type with a 'Semigroup' instance that always throws an error.
newtype Unlawful a = Unlawful a
  deriving stock (Show, Eq)
  deriving newtype Arbitrary

-- | A 'Semigroup' instance that always throws an error.
instance Semigroup (Unlawful a) where
  _ <> _ = error "unlawful"

-- | Semigroup wrapper for 'Normal.Update'
newtype NormalUpdateSG v blob = NormalUpdateSG (Normal.Update v blob)
  deriving stock (Show, Eq)
  deriving newtype (Arbitrary)
  deriving Semigroup via First (Normal.Update v blob)

unNormalUpdateSG :: NormalUpdateSG v b -> Normal.Update v b
unNormalUpdateSG (NormalUpdateSG x) = x

-- | Semigroup wrapper for 'Monoidal.Update'
newtype MonoidalUpdateSG v = MonoidalUpdateSG (Monoidal.Update v)
  deriving stock (Show, Eq)
  deriving newtype (Arbitrary)

unMonoidalUpdateSG :: MonoidalUpdateSG v -> Monoidal.Update v
unMonoidalUpdateSG (MonoidalUpdateSG x) = x

instance Semigroup v => Semigroup (MonoidalUpdateSG v) where
  (<>) = coerce $ \upd1 upd2 -> case (upd1 :: Monoidal.Update v, upd2) of
      (e1@Monoidal.Delete  , _                  ) -> e1
      (e1@Monoidal.Insert{}, _                  ) -> e1
      (Monoidal.Mupsert v1 , Monoidal.Delete    ) -> Monoidal.Insert v1
      (Monoidal.Mupsert v1 , Monoidal.Insert  v2) -> Monoidal.Insert (v1 <> v2)
      (Monoidal.Mupsert v1 , Monoidal.Mupsert v2) -> Monoidal.Mupsert (v1 <> v2)

newtype EntrySG v blob = EntrySG (Entry v blob)
  deriving stock (Show, Eq)
  deriving newtype Semigroup

instance (Arbitrary v, Arbitrary blob) => Arbitrary (EntrySG v blob) where
  arbitrary = arbitrary2
  shrink = shrink2

-- | We do not use the @'Arbitrary' 'Entry'@ instance here, because we want to
-- generate each constructor with equal probability.
instance Arbitrary2 EntrySG where
  liftArbitrary2 genVal genBlob = EntrySG <$> frequency
    [ (1, Insert <$> genVal)
    , (1, InsertWithBlob <$> genVal <*> genBlob)
    , (1, Mupdate <$> genVal)
    , (1, pure Delete)
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

{-------------------------------------------------------------------------------
  Injections/projections
-------------------------------------------------------------------------------}

updateToEntryNormal :: Normal.Update v blob -> Entry v blob
updateToEntryNormal = \case
    Normal.Insert v Nothing  -> Insert v
    Normal.Insert v (Just b) -> InsertWithBlob v b
    Normal.Delete            -> Delete

entryToUpdateNormal :: Entry v blob -> Maybe (Normal.Update v blob)
entryToUpdateNormal = \case
    Insert v           -> Just (Normal.Insert v Nothing)
    InsertWithBlob v b -> Just (Normal.Insert v (Just b))
    Mupdate _          -> Nothing
    Delete             -> Just Normal.Delete

updateToEntryMonoidal :: Monoidal.Update v -> Entry v blob
updateToEntryMonoidal = \case
    Monoidal.Insert v  -> Insert v
    Monoidal.Mupsert v -> Mupdate v
    Monoidal.Delete    -> Delete

entryToUpdateMonoidal :: Entry v blob -> Maybe (Monoidal.Update v)
entryToUpdateMonoidal = \case
    Insert v           -> Just (Monoidal.Insert v)
    InsertWithBlob _ _ -> Nothing
    Mupdate v          -> Just (Monoidal.Mupsert v)
    Delete             -> Just Monoidal.Delete
