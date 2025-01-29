{-# LANGUAGE DerivingVia                #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE TypeApplications           #-}

module Test.Database.LSMTree.Internal.Entry (tests) where

import           Data.Coerce
import           Data.List.NonEmpty (NonEmpty)
import           Data.Semigroup hiding (First)
import qualified Data.Semigroup as S
import           Data.Void
import qualified Database.LSMTree as Unified
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

      testClassLaws "Regular Entry" $
        semigroupLaws (Proxy @(Regular (Entry (Sum Int) String)))
    , testClassLaws "Regular Unified.Update" $
        semigroupLaws (Proxy @(Regular (Unified.Update (Sum Int) String)))
    , testClassLaws "Regular Normal.Update" $
        -- Note that we are using Unlawful here because we do not combine values
        -- monoidally.
        semigroupLaws (Proxy @(Regular (Normal.Update (Unlawful Int) String)))
    , testClassLaws "Regular (Monoidal.Update)" $
        semigroupLaws (Proxy @(Regular (Monoidal.Update (Sum Int))))

    , testClassLaws "Union Entry" $
        semigroupLaws (Proxy @(Union (Entry (Sum Int) String)))
    , testClassLaws "Union Unified.Update" $
        semigroupLaws (Proxy @(Union (Unified.Update (Sum Int) String)))
    , testClassLaws "Union Normal.Update" $
        semigroupLaws (Proxy @(Union (Normal.Update (Sum Int) String)))
    , testClassLaws "Union Monoidal.Update" $
        semigroupLaws (Proxy @(Union (Monoidal.Update (Sum Int))))

    -- * Semantics

    , testProperty "prop_regularSemantics_unified" $
        prop_regularSemantics_unified @(Sum Int) @String
    , testProperty "prop_regularSemantics_normal" $
        -- Note that we are using Unlawful here because we do not combine values
        -- monoidally.
        prop_regularSemantics_normal @(Unlawful Int) @String
    , testProperty "prop_regularSemantics_monoidal" $
        prop_regularSemantics_monoidal @(Sum Int)

    , testProperty "prop_unionSemantics_unified" $
        prop_unionSemantics_unified @(Sum Int) @String
    , testProperty "prop_unionSemantics_normal" $
        prop_unionSemantics_normal @(Sum Int) @String
    , testProperty "prop_unionSemantics_monoidal" $
        prop_unionSemantics_monoidal @(Sum Int)
    ]

-- TODO: it would be nice to write down how the semantic tests below relate to
-- the semantics of operations on the public API.

-- | @sconcat == fromEntry . sconcat . toEntry@ with regular semantics for
-- unified updates.
prop_regularSemantics_unified ::
     (Show v, Show b, Eq v, Eq b, Semigroup v)
  => NonEmpty (Unified.Update v b)
  -> Property
prop_regularSemantics_unified es = expected === real
  where
    expected = from . sconcat . fmap to $ es
      where
        to :: Unified.Update v b -> Regular (Unified.Update v b)
        to = Regular

        from :: Regular (Unified.Update v b) -> Unified.Update v b
        from = unRegular

    real = from . sconcat . fmap to $ es
      where
        to :: Unified.Update v b -> Regular (Entry v b)
        to = Regular . updateToEntryUnified

        from :: Regular (Entry v b) -> Unified.Update v b
        from = entryToUpdateUnified . unRegular

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
        to :: Normal.Update v b -> Regular (Normal.Update v b)
        to = Regular

        from :: Regular (Normal.Update v b) -> Maybe (Normal.Update v b)
        from = Just . unRegular

    real = from . sconcat . fmap to $ es
      where
        to :: Normal.Update v b -> Regular (Entry v b)
        to = Regular . updateToEntryNormal

        from :: Regular (Entry v b) -> Maybe (Normal.Update v b)
        from = entryToUpdateNormal . unRegular

-- | @sconcat == fromEntry . sconcat . toEntry@ with regular semantics for
-- monoidal updates.
prop_regularSemantics_monoidal ::
     (Show v, Eq v, Semigroup v)
  => NonEmpty (Monoidal.Update v) -> Property
prop_regularSemantics_monoidal es = expected === real
  where
    expected = from . sconcat . fmap to $ es
      where
        to :: Monoidal.Update v -> Regular (Monoidal.Update v)
        to = Regular

        from :: Regular (Monoidal.Update v) -> Maybe (Monoidal.Update v)
        from = Just . unRegular

    real = from . sconcat . fmap to $ es
      where
        to :: Monoidal.Update v -> Regular (Entry v Void)
        to = Regular . updateToEntryMonoidal

        from :: Regular (Entry v Void) -> Maybe (Monoidal.Update v)
        from = entryToUpdateMonoidal . unRegular

-- | @sconcat == fromEntry . sconcat . toEntry@ with union semantics for
-- unified updates.
prop_unionSemantics_unified ::
     (Show v, Show b, Eq v, Eq b, Semigroup v)
  => NonEmpty (Unified.Update v b)
  -> Property
prop_unionSemantics_unified es = expected === real
  where
    expected = from . sconcat . fmap to $ es
      where
        to :: Unified.Update v b -> Union (Unified.Update v b)
        to = Union

        from :: Union (Unified.Update v b) -> Unified.Update v b
        from = unUnion

    real = from . sconcat . fmap to $ es
      where
        to :: Unified.Update v b -> Union (Entry v b)
        to = Union . updateToEntryUnified

        from :: Union (Entry v b) -> Unified.Update v b
        from = entryToUpdateUnified . unUnion

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

newtype Regular a = Regular { unRegular :: a }
  deriving stock (Show, Eq)

--
-- Unified update
--

deriving newtype instance (Arbitrary v, Arbitrary b)
                       => Arbitrary (Regular (Unified.Update v b))

instance Semigroup v => Semigroup (Regular (Unified.Update v b)) where
  Regular up1 <> Regular up2 = Regular $ case (up1, up2) of
      (Unified.Delete     , _                   ) -> up1
      (Unified.Insert{}   , _                   ) -> up1
      (Unified.Mupsert v1 , Unified.Delete      ) -> Unified.Insert v1 Nothing
      (Unified.Mupsert v1 , Unified.Insert v2 _ ) -> Unified.Insert (v1 <> v2) Nothing
      (Unified.Mupsert v1 , Unified.Mupsert v2  ) -> Unified.Mupsert (v1 <> v2)

--
-- Normal update
--

deriving newtype instance (Arbitrary v, Arbitrary b)
                       => Arbitrary (Regular (Normal.Update v b))


deriving via S.First (Normal.Update v b)
    instance Semigroup (Regular (Normal.Update v b))


--
-- Monoidal update
--

deriving newtype instance Arbitrary v
                       => Arbitrary (Regular (Monoidal.Update v))


instance Semigroup v => Semigroup (Regular (Monoidal.Update v)) where
  (<>) = coerce $ \upd1 upd2 -> case (upd1 :: Monoidal.Update v, upd2) of
      (e1@Monoidal.Delete  , _                  ) -> e1
      (e1@Monoidal.Insert{}, _                  ) -> e1
      (Monoidal.Mupsert v1 , Monoidal.Delete    ) -> Monoidal.Insert v1
      (Monoidal.Mupsert v1 , Monoidal.Insert  v2) -> Monoidal.Insert (v1 <> v2)
      (Monoidal.Mupsert v1 , Monoidal.Mupsert v2) -> Monoidal.Mupsert (v1 <> v2)

--
-- Entry
--

deriving via Uniform (Entry v b)
    instance (Arbitrary v, Arbitrary b) => Arbitrary (Regular (Entry v b))

-- | Semigroup instance using 'combine'.
instance Semigroup v => Semigroup (Regular (Entry v b)) where
  Regular e1 <> Regular e2 = Regular $ combine (<>) e1 e2

{-------------------------------------------------------------------------------
  Union semantics
-------------------------------------------------------------------------------}

newtype Union a = Union { unUnion :: a }
  deriving stock (Show, Eq)

--
-- Unified update
--

deriving newtype instance (Arbitrary v, Arbitrary b)
                       => Arbitrary (Union (Unified.Update v b))

instance Semigroup v => Semigroup (Union (Unified.Update v b)) where
  Union up1 <> Union up2 = Union $ fromModel $ toModel up1 <> toModel up2
    where
      toModel :: Unified.Update v b -> Maybe (v, S.First (Maybe b))
      toModel Unified.Delete        = Nothing
      toModel (Unified.Insert v mb) = Just (v, S.First mb)
      toModel (Unified.Mupsert v)   = Just (v, S.First Nothing)

      fromModel :: Maybe (v, S.First (Maybe b)) -> Unified.Update v b
      fromModel Nothing                = Unified.Delete
      fromModel (Just (v, S.First mb)) = Unified.Insert v mb

--
-- Normal update
--

deriving newtype instance (Arbitrary v, Arbitrary b)
                       => Arbitrary (Union (Normal.Update v b))

instance Semigroup v => Semigroup (Union (Normal.Update v b)) where
  Union up1 <> Union up2 = Union $ fromModel $ toModel up1 <> toModel up2
    where
      toModel :: Normal.Update v b -> Maybe (v, S.First (Maybe b))
      toModel Normal.Delete        = Nothing
      toModel (Normal.Insert v mb) = Just (v, S.First mb)

      fromModel :: Maybe (v, S.First (Maybe b)) -> Normal.Update v b
      fromModel Nothing                = Normal.Delete
      fromModel (Just (v, S.First mb)) = Normal.Insert v mb

--
-- Monoidal update
--

deriving newtype instance Arbitrary v
                       => Arbitrary (Union (Monoidal.Update v))

instance Semigroup v => Semigroup (Union (Monoidal.Update v)) where
  Union up1 <> Union up2 = Union $ fromModel $ toModel up1 <> toModel up2
    where
      toModel :: Monoidal.Update v -> Maybe v
      toModel Monoidal.Delete      = Nothing
      toModel (Monoidal.Insert v)  = Just v
      toModel (Monoidal.Mupsert v) = Just v

      fromModel :: Maybe v -> Monoidal.Update v
      fromModel Nothing  = Monoidal.Delete
      fromModel (Just v) = Monoidal.Insert v

--
-- Entry
--

deriving via Uniform (Entry v b)
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

newtype Uniform a = Uniform a

-- | We do not use the @'Arbitrary' 'Entry'@ instance here, because we want to
-- generate each constructor with equal probability.
instance (Arbitrary v, Arbitrary b) => Arbitrary (Uniform (Entry v b)) where
  arbitrary = Uniform <$> liftArbitrary2Entry arbitrary arbitrary
  shrink (Uniform x) = Uniform <$> liftShrink2Entry shrink shrink x

liftArbitrary2Entry :: Gen v -> Gen b -> Gen (Entry v b)
liftArbitrary2Entry genVal genBlob = frequency
    [ (1, Insert <$> genVal)
    , (1, InsertWithBlob <$> genVal <*> genBlob)
    , (1, Mupdate <$> genVal)
    , (1, pure Delete)
    ]

liftShrink2Entry :: (v -> [v]) -> (b -> [b]) -> Entry v b -> [Entry v b]
liftShrink2Entry shrinkVal shrinkBlob = \case
    Insert v           -> Delete : (Insert <$> shrinkVal v)
    InsertWithBlob v b -> [Delete, Insert v]
                       ++ [ InsertWithBlob v' b'
                          | (v', b') <- liftShrink2 shrinkVal shrinkBlob (v, b)
                          ]
    Mupdate v          -> Delete : Insert v : (Mupdate <$> shrinkVal v)
    Delete             -> []

{-------------------------------------------------------------------------------
  Injections/projections
-------------------------------------------------------------------------------}

updateToEntryUnified :: Unified.Update v b -> Entry v b
updateToEntryUnified = \case
    Unified.Insert v Nothing  -> Insert v
    Unified.Insert v (Just b) -> InsertWithBlob v b
    Unified.Mupsert v         -> Mupdate v
    Unified.Delete            -> Delete

entryToUpdateUnified :: Entry v b -> Unified.Update v b
entryToUpdateUnified = \case
    Insert v           -> Unified.Insert v Nothing
    InsertWithBlob v b -> Unified.Insert v (Just b)
    Mupdate v          -> Unified.Mupsert v
    Delete             -> Unified.Delete

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
