{-# LANGUAGE LambdaCase          #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# OPTIONS_GHC -Wno-orphans  #-}

{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ExplicitForAll      #-}
{-# LANGUAGE FlexibleContexts    #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}

module Test.Database.LSMTree.Internal.Config (tests) where

import           Database.LSMTree.Internal.Config
import           Test.QuickCheck.Modifiers
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.QuickCheck
import           Test.Util.TypeClassLaws


tests :: TestTree
tests = testGroup "Test.Database.LSMTree.Internal.Config"
    [ laws_BloomFilterAlloc
    , laws_DiskCachePolicy
    , laws_FencePointerIndexType
    , laws_LevelNo
    , laws_MergeBatchSize
    , laws_MergePolicy
    , laws_MergeSchedule
    , laws_SizeRatio
    , laws_TableConfig
    , laws_WriteBufferAlloc
    ]

laws_BloomFilterAlloc :: TestTree
laws_BloomFilterAlloc = testGroup "BloomFilterAlloc"
    [ equalityLaws       @(BloomFilterAlloc)
    , normalFormDataLaws @(BloomFilterAlloc)
    , showProperties     @(BloomFilterAlloc)
    ]

laws_DiskCachePolicy :: TestTree
laws_DiskCachePolicy = testGroup "DiskCachePolicy"
    [ equalityLaws       @(DiskCachePolicy)
    , normalFormDataLaws @(DiskCachePolicy)
    , showProperties     @(DiskCachePolicy)
    ]

laws_FencePointerIndexType :: TestTree
laws_FencePointerIndexType = testGroup "FencePointerIndexType"
    [ equalityLaws       @(FencePointerIndexType)
    , normalFormDataLaws @(FencePointerIndexType)
    , showProperties     @(FencePointerIndexType)
    ]

laws_LevelNo :: TestTree
laws_LevelNo = testGroup "LevelNo"
    [ enumLaws           @(LevelNo)
    , equalityLaws       @(LevelNo)
    , orderingLaws       @(LevelNo)
    , normalFormDataLaws @(LevelNo)
    , showProperties     @(LevelNo)
    ]

laws_MergeBatchSize :: TestTree
laws_MergeBatchSize = testGroup "MergeBatchSize"
    [ equalityLaws       @(MergeBatchSize)
    , orderingLaws       @(MergeBatchSize)
    , normalFormDataLaws @(MergeBatchSize)
    , showProperties     @(MergeBatchSize)
    ]

laws_MergePolicy :: TestTree
laws_MergePolicy = testGroup "MergePolicy"
    [ normalFormDataLaws @(MergePolicy)
    , showProperties     @(MergePolicy)
    ]

laws_MergeSchedule :: TestTree
laws_MergeSchedule = testGroup "MergeSchedule"
    [ equalityLaws       @(MergeSchedule)
    , normalFormDataLaws @(MergeSchedule)
    , showProperties     @(MergeSchedule)
    ]

laws_SizeRatio :: TestTree
laws_SizeRatio = testGroup "SizeRatio"
    [ normalFormDataLaws @(SizeRatio)
    , showProperties     @(SizeRatio)
    ]

laws_TableConfig :: TestTree
laws_TableConfig = testGroup "TableConfig"
    [ equalityLaws       @(TableConfig)
    , normalFormDataLaws @(TableConfig)
    , showProperties     @(TableConfig)
    ]

laws_WriteBufferAlloc :: TestTree
laws_WriteBufferAlloc = testGroup "WriteBufferAlloc"
    [ equalityLaws       @(WriteBufferAlloc)
    , normalFormDataLaws @(WriteBufferAlloc)
    , showProperties     @(WriteBufferAlloc)
    ]

isValidBloomFilterFixedValue :: Double -> Bool
isValidBloomFilterFixedValue x = 2 <= x && x <= 24

isValidBloomFilterRequestValue :: Double -> Bool
isValidBloomFilterRequestValue x = 0 < x && x < 1

instance Arbitrary BloomFilterAlloc where

  arbitrary = oneof
      [ fmap AllocFixed $
            arbitrary `suchThat` isValidBloomFilterFixedValue
      , fmap AllocRequestFPR $
            arbitrary `suchThat` isValidBloomFilterRequestValue
      ]

  shrink (AllocFixed x) = fmap AllocFixed .
      filter isValidBloomFilterFixedValue $ shrink x
  shrink (AllocRequestFPR x) = fmap AllocRequestFPR .
      filter isValidBloomFilterRequestValue $ shrink x

instance Arbitrary DiskCachePolicy where

  arbitrary = oneof
      [ pure DiskCacheAll
      , pure DiskCacheNone
      , DiskCacheLevelOneTo . getPositive <$> arbitrary
      ]

  shrink (DiskCacheLevelOneTo x) =
      [ DiskCacheLevelOneTo x' | Positive x' <- shrink $ Positive x ]
  shrink _ = []

instance Arbitrary FencePointerIndexType where

  arbitrary = oneof $ pure <$> [ OrdinaryIndex, CompactIndex ]

  shrink _ = []

instance Arbitrary LevelNo where

  arbitrary = LevelNo . getNonNegative <$> arbitrary

  shrink (LevelNo x) =
      LevelNo . getNonNegative <$> shrink (NonNegative x)

instance Arbitrary MergeBatchSize where

  arbitrary = MergeBatchSize . getPositive <$> arbitrary

  shrink (MergeBatchSize x) =
      MergeBatchSize . getPositive <$> shrink (Positive x)

instance Arbitrary MergePolicy where

  arbitrary = pure LazyLevelling

  shrink = const []

instance Arbitrary MergeSchedule where

  arbitrary = oneof $ pure <$> [ OneShot, Incremental ]

  shrink = const []

instance Arbitrary SizeRatio where

  arbitrary = pure Four

  shrink = const []

instance Arbitrary TableConfig where

  arbitrary = TableConfig
      <$> arbitrary
      <*> arbitrary
      <*> arbitrary
      <*> arbitrary
      <*> arbitrary
      <*> arbitrary
      <*> arbitrary
      <*> arbitrary

  shrink (TableConfig a b c d e f g h) =
      [ TableConfig a' b' c' d' e' f' g' h'
      | (a',b',c',d',e',f',g',h') <- shrink (a,b,c,d,e,f,g,h)
      ]

instance Arbitrary WriteBufferAlloc where

  arbitrary = AllocNumEntries . getPositive <$> arbitrary

  shrink (AllocNumEntries x) =
     AllocNumEntries . getPositive <$> shrink (Positive x)

