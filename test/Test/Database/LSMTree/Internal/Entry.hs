{-# LANGUAGE DerivingVia                #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE TypeApplications           #-}

module Test.Database.LSMTree.Internal.Entry (tests) where

import           Data.Coerce
import           Data.List.NonEmpty (NonEmpty)
import qualified Data.Monoid as Monoid
import           Data.Semigroup hiding (First)
import qualified Data.Semigroup as Semigroup
import           Data.Void
import           Database.LSMTree.Extras.Generators ()
import           Database.LSMTree.Internal.Entry
import qualified Database.LSMTree.Monoidal as Monoidal
import qualified Database.LSMTree.Normal as Normal
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
      -- * Class laws

      testClassLaws "EntrySG" $
        semigroupLaws (Proxy @(EntrySG (Sum Int) String))
    , testClassLaws "NormalUpdateSG" $
        -- Note that we are using Unlawful here because we do not combine values
        -- monoidally.
        semigroupLaws (Proxy @(NormalUpdateSG (Unlawful Int) String))
    , testClassLaws "MonoidalUpdateSG" $
        semigroupLaws (Proxy @(MonoidalUpdateSG (Sum Int)))

    , testClassLaws "Union Entry" $
        semigroupLaws (Proxy @(Union (Entry (Sum Int) String)))

    , testClassLaws "Union Normal.Update" $
        semigroupLaws (Proxy @(Union (Normal.Update (Sum Int) String)))

    , testClassLaws "Union Monoidal.Update" $
        semigroupLaws (Proxy @(Union (Monoidal.Update (Sum Int))))


    -- * Semantics

    , testProperty "prop_regularSemantics_normal" $
        -- Note that we are using Unlawful here because we do not combine values
        -- monoidally.
        prop_regularSemantics_normal @(Unlawful Int) @String
    , testProperty "prop_regularSemantics_monoidal" $
        prop_regularSemantics_monoidal @(Sum Int)

    , testProperty "prop_unionSemantics_normal" $
        prop_unionSemantics_normal @(Sum Int) @String
    , testProperty "prop_unionSemantics_monoidal" $
        prop_unionSemantics_monoidal @(Sum Int)
    ]

-- | @sconcat == fromEntry . sconcat . toEntry@ with regular semantics for
-- normal updates.
prop_regularSemantics_normal ::
     (Show v, Show b, Eq v, Eq b, Semigroup v)
  => NonEmpty (Normal.Update v b)
  -> Property
prop_regularSemantics_normal es = expected === real
  where
    expected = from . sconcat . fmap to $ es
      where
        to :: Normal.Update v b -> NormalUpdateSG v b
        to = NormalUpdateSG

        from :: NormalUpdateSG v b -> Maybe (Normal.Update v b)
        from = Just . unNormalUpdateSG

    real = from . sconcat . fmap to $ es
      where
        to :: Normal.Update v b -> EntrySG v b
        to = EntrySG . updateToEntryNormal

        from :: EntrySG v b -> Maybe (Normal.Update v b)
        from (EntrySG x) = entryToUpdateNormal x

-- | @sconcat == fromEntry . sconcat . toEntry@ with regular semantics for
-- monoidal updates.
prop_regularSemantics_monoidal ::
     (Show v, Eq v, Semigroup v)
  => NonEmpty (Monoidal.Update v) -> Property
prop_regularSemantics_monoidal es = expected === real
  where
    expected = from . sconcat . fmap to $ es
      where
        to :: Monoidal.Update v -> MonoidalUpdateSG v
        to = MonoidalUpdateSG

        from :: MonoidalUpdateSG v -> Maybe (Monoidal.Update v)
        from = Just . unMonoidalUpdateSG

    real = from . sconcat . fmap to $ es
      where
        to :: Monoidal.Update v -> EntrySG v Void
        to = EntrySG . updateToEntryMonoidal

        from :: EntrySG v Void -> Maybe (Monoidal.Update v)
        from (EntrySG x) = entryToUpdateMonoidal x

-- | @sconcat == fromEntry . sconcat . toEntry@ with union semantics for normal
-- updates.
prop_unionSemantics_normal ::
     (Show v, Show b, Eq v, Eq b, Semigroup v)
  => NonEmpty (Normal.Update v b)
  -> Property
prop_unionSemantics_normal es = expected === real
  where
    expected = from . sconcat . fmap to $ es
      where
        to :: Normal.Update v b -> Union (Normal.Update v b)
        to = Union

        from :: Union (Normal.Update v b) -> Maybe (Normal.Update v b)
        from = Just . unUnion

    real = from . sconcat . fmap to $ es
      where
        to :: Normal.Update v b -> Union (Entry v b)
        to = Union . updateToEntryNormal

        from :: Union (Entry v b) -> Maybe (Normal.Update v b)
        from = entryToUpdateNormal . unUnion

-- | @sconcat == fromEntry . sconcat . toEntry@ with union semantics for
-- monoidal updates.
prop_unionSemantics_monoidal ::
     (Show v, Eq v, Semigroup v)
  => NonEmpty (Monoidal.Update v)
  -> Property
prop_unionSemantics_monoidal es = expected === real
  where
    expected = from . sconcat . fmap to $ es
      where
        to :: Monoidal.Update v -> Union (Monoidal.Update v)
        to = Union

        from :: Union (Monoidal.Update v) -> Maybe (Monoidal.Update v)
        from = Just . unUnion

    real = from . sconcat . fmap to $ es
      where
        to :: Monoidal.Update v -> Union (Entry v Void)
        to = Union . updateToEntryMonoidal

        from :: Union (Entry v Void) -> Maybe (Monoidal.Update v)
        from = entryToUpdateMonoidal . unUnion

{-------------------------------------------------------------------------------
  Regular semantics
-------------------------------------------------------------------------------}

--
-- Normal update
--

-- | Semigroup wrapper for 'Normal.Update'
newtype NormalUpdateSG v b = NormalUpdateSG (Normal.Update v b)
  deriving stock (Show, Eq)
  deriving newtype Arbitrary
  deriving Semigroup via Semigroup.First (Normal.Update v b)

unNormalUpdateSG :: NormalUpdateSG v b -> Normal.Update v b
unNormalUpdateSG (NormalUpdateSG x) = x

--
-- Monoidal update
--

-- | Semigroup wrapper for 'Monoidal.Update'
newtype MonoidalUpdateSG v = MonoidalUpdateSG (Monoidal.Update v)
  deriving stock (Show, Eq)
  deriving newtype Arbitrary

unMonoidalUpdateSG :: MonoidalUpdateSG v -> Monoidal.Update v
unMonoidalUpdateSG (MonoidalUpdateSG x) = x

instance Semigroup v => Semigroup (MonoidalUpdateSG v) where
  (<>) = coerce $ \upd1 upd2 -> case (upd1 :: Monoidal.Update v, upd2) of
      (e1@Monoidal.Delete  , _                  ) -> e1
      (e1@Monoidal.Insert{}, _                  ) -> e1
      (Monoidal.Mupsert v1 , Monoidal.Delete    ) -> Monoidal.Insert v1
      (Monoidal.Mupsert v1 , Monoidal.Insert  v2) -> Monoidal.Insert (v1 <> v2)
      (Monoidal.Mupsert v1 , Monoidal.Mupsert v2) -> Monoidal.Mupsert (v1 <> v2)

--
-- Entry
--

-- | Semigroup wrapper for 'Entry'
newtype EntrySG v b = EntrySG (Entry v b)
  deriving stock (Show, Eq)

-- | Semigroup instance using 'combine'.
instance Semigroup v => Semigroup (EntrySG v b) where
  EntrySG e1 <> EntrySG e2 = EntrySG $ combine (<>) e1 e2

instance (Arbitrary v, Arbitrary b) => Arbitrary (EntrySG v b) where
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

{-------------------------------------------------------------------------------
  Union semantics
-------------------------------------------------------------------------------}

newtype Union a = Union { unUnion :: a }
  deriving stock (Show, Eq)

--
-- Normal update
--

deriving newtype instance (Arbitrary v, Arbitrary b)
                       => Arbitrary (Union (Normal.Update v b))

instance Semigroup v => Semigroup (Union (Normal.Update v b)) where
  Union up1 <> Union up2 = Union $ case (up1, up2) of
    (Normal.Delete       , _                    ) -> up2
    (_                   , Normal.Delete        ) -> up1
    (Normal.Insert v1 mb1, Normal.Insert v2 mb2 ) ->
        Normal.Insert
          (v1 <> v2)
          (Monoid.getFirst (Monoid.First mb1 <> Monoid.First mb2))

--
-- Monoidal update
--

deriving newtype instance Arbitrary v
                       => Arbitrary (Union (Monoidal.Update v))

instance Semigroup v => Semigroup (Union (Monoidal.Update v)) where
  Union up1 <> Union up2 = Union $ case (up1, up2) of
      (Monoidal.Delete     , _                  ) -> up2
      (_                   , Monoidal.Delete    ) -> up1
      (Monoidal.Insert v1  , Monoidal.Insert v2 ) -> Monoidal.Insert (v1 <> v2)
      (Monoidal.Insert v1  , Monoidal.Mupsert v2) -> Monoidal.Insert (v1 <> v2)
      (Monoidal.Mupsert v1 , Monoidal.Insert v2 ) -> Monoidal.Insert (v1 <> v2)
      (Monoidal.Mupsert v1 , Monoidal.Mupsert v2) -> Monoidal.Insert (v1 <> v2)

--
-- Entry
--

deriving via EntrySG v b
    instance (Arbitrary v, Arbitrary b) => Arbitrary (Union (Entry v b))

-- | Semigroup instance using 'combineUnion'.
instance Semigroup v => Semigroup (Union (Entry v b)) where
  Union e1 <> Union e2 = Union $ combineUnion (<>) e1 e2

{-------------------------------------------------------------------------------
  Utility
-------------------------------------------------------------------------------}

-- | A wrapper type with a 'Semigroup' instance that always throws an error.
newtype Unlawful a = Unlawful a
  deriving stock (Show, Eq)
  deriving newtype Arbitrary

-- | A 'Semigroup' instance that always throws an error.
instance Semigroup (Unlawful a) where
  _ <> _ = error "unlawful"

{-------------------------------------------------------------------------------
  Injections/projections
-------------------------------------------------------------------------------}

updateToEntryNormal :: Normal.Update v b -> Entry v b
updateToEntryNormal = \case
    Normal.Insert v Nothing  -> Insert v
    Normal.Insert v (Just b) -> InsertWithBlob v b
    Normal.Delete            -> Delete

entryToUpdateNormal :: Entry v b -> Maybe (Normal.Update v b)
entryToUpdateNormal = \case
    Insert v           -> Just (Normal.Insert v Nothing)
    InsertWithBlob v b -> Just (Normal.Insert v (Just b))
    Mupdate _          -> Nothing
    Delete             -> Just Normal.Delete

updateToEntryMonoidal :: Monoidal.Update v -> Entry v b
updateToEntryMonoidal = \case
    Monoidal.Insert v  -> Insert v
    Monoidal.Mupsert v -> Mupdate v
    Monoidal.Delete    -> Delete

entryToUpdateMonoidal :: Entry v b -> Maybe (Monoidal.Update v)
entryToUpdateMonoidal = \case
    Insert v           -> Just (Monoidal.Insert v)
    InsertWithBlob _ _ -> Nothing
    Mupdate v          -> Just (Monoidal.Mupsert v)
    Delete             -> Just Monoidal.Delete
