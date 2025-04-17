{-# LANGUAGE DerivingVia                #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE TypeApplications           #-}

module Test.Database.LSMTree.Internal.Entry (tests) where

import           Data.List.NonEmpty (NonEmpty)
import           Data.Semigroup hiding (First)
import qualified Data.Semigroup as S
import qualified Database.LSMTree as Full
import           Database.LSMTree.Extras.Generators ()
import           Database.LSMTree.Internal.Entry
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
    , testClassLaws "Regular Update" $
        semigroupLaws (Proxy @(Regular (Full.Update (Sum Int) String)))

    , testClassLaws "Union Entry" $
        semigroupLaws (Proxy @(Union (Entry (Sum Int) String)))
    , testClassLaws "Union Update" $
        semigroupLaws (Proxy @(Union (Full.Update (Sum Int) String)))

    -- * Semantics

    , testProperty "prop_regularSemantics" $
        prop_regularSemantics @(Sum Int) @String

    , testProperty "prop_unionSemantics" $
        prop_unionSemantics @(Sum Int) @String
    ]

-- TODO: it would be nice to write down how the semantic tests below relate to
-- the semantics of operations on the public API.

-- | @sconcat == fromEntry . sconcat . toEntry@ with regular semantics.
prop_regularSemantics ::
     (Show v, Show b, Eq v, Eq b, Semigroup v)
  => NonEmpty (Full.Update v b)
  -> Property
prop_regularSemantics es = expected === real
  where
    expected = from . sconcat . fmap to $ es
      where
        to :: Full.Update v b -> Regular (Full.Update v b)
        to = Regular

        from :: Regular (Full.Update v b) -> Full.Update v b
        from = unRegular

    real = from . sconcat . fmap to $ es
      where
        to :: Full.Update v b -> Regular (Entry v b)
        to = Regular . updateToEntry

        from :: Regular (Entry v b) -> Full.Update v b
        from = entryToUpdate . unRegular

-- | @sconcat == fromEntry . sconcat . toEntry@ with union semantics.
prop_unionSemantics ::
     (Show v, Show b, Eq v, Eq b, Semigroup v)
  => NonEmpty (Full.Update v b)
  -> Property
prop_unionSemantics es = expected === real
  where
    expected = from . sconcat . fmap to $ es
      where
        to :: Full.Update v b -> Union (Full.Update v b)
        to = Union

        from :: Union (Full.Update v b) -> Full.Update v b
        from = unUnion

    real = from . sconcat . fmap to $ es
      where
        to :: Full.Update v b -> Union (Entry v b)
        to = Union . updateToEntry

        from :: Union (Entry v b) -> Full.Update v b
        from = entryToUpdate . unUnion

{-------------------------------------------------------------------------------
  Regular semantics
-------------------------------------------------------------------------------}

newtype Regular a = Regular { unRegular :: a }
  deriving stock (Show, Eq)

--
-- Update
--

deriving newtype instance (Arbitrary v, Arbitrary b)
                       => Arbitrary (Regular (Full.Update v b))

instance Semigroup v => Semigroup (Regular (Full.Update v b)) where
  Regular up1 <> Regular up2 = Regular $ case (up1, up2) of
      (Full.Delete    , _               ) -> up1
      (Full.Insert{}  , _               ) -> up1
      (Full.Upsert v1 , Full.Delete     ) -> Full.Insert v1 Nothing
      (Full.Upsert v1 , Full.Insert v2 _) -> Full.Insert (v1 <> v2) Nothing
      (Full.Upsert v1 , Full.Upsert v2  ) -> Full.Upsert (v1 <> v2)

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
-- Update
--

deriving newtype instance (Arbitrary v, Arbitrary b)
                       => Arbitrary (Union (Full.Update v b))

instance Semigroup v => Semigroup (Union (Full.Update v b)) where
  Union up1 <> Union up2 = Union $ fromModel $ toModel up1 <> toModel up2
    where
      toModel :: Full.Update v b -> Maybe (v, S.First (Maybe b))
      toModel Full.Delete        = Nothing
      toModel (Full.Insert v mb) = Just (v, S.First mb)
      toModel (Full.Upsert v)    = Just (v, S.First Nothing)

      fromModel :: Maybe (v, S.First (Maybe b)) -> Full.Update v b
      fromModel Nothing                = Full.Delete
      fromModel (Just (v, S.First mb)) = Full.Insert v mb

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

updateToEntry :: Full.Update v b -> Entry v b
updateToEntry = \case
    Full.Insert v Nothing  -> Insert v
    Full.Insert v (Just b) -> InsertWithBlob v b
    Full.Upsert v          -> Mupdate v
    Full.Delete            -> Delete

entryToUpdate :: Entry v b -> Full.Update v b
entryToUpdate = \case
    Insert v           -> Full.Insert v Nothing
    InsertWithBlob v b -> Full.Insert v (Just b)
    Mupdate v          -> Full.Upsert v
    Delete             -> Full.Delete
