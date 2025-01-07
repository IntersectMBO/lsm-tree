{-# OPTIONS_GHC -Wno-orphans #-}

-- TODO: upstream to fs-sim
module Test.FS (tests) where

import           GHC.Generics (Generic)
import           System.FS.API
import           System.FS.Sim.Error
import qualified System.FS.Sim.Stream as S
import           System.FS.Sim.Stream (InternalInfo (..), Stream (..))
import           Test.QuickCheck
import           Test.QuickCheck.Classes (eqLaws)
import           Test.QuickCheck.Instances ()
import           Test.Tasty
import           Test.Util.FS
import           Test.Util.QC

tests :: TestTree
tests = testGroup "Test.FS" [
      testClassLaws "Stream" $
        eqLaws (Proxy @(Stream Int))
    , testClassLaws "Errors" $
        eqLaws (Proxy @Errors)
    ]

-- | This is not a fully lawful instance, because it uses 'approximateEqStream'.
instance Eq a => Eq (Stream a) where
  (==) = approximateEqStream

instance Arbitrary a => Arbitrary (Stream a) where
  arbitrary = oneof [
        S.genFinite arbitrary
      , S.genInfinite arbitrary
      ]
  shrink s = S.liftShrinkStream shrink s

deriving stock instance Generic (Stream a)
deriving anyclass instance CoArbitrary a => CoArbitrary (Stream a)
deriving anyclass instance Function a => Function (Stream a)

deriving stock instance Generic InternalInfo
deriving anyclass instance Function InternalInfo
deriving anyclass instance CoArbitrary InternalInfo

-- | This is not a fully lawful instance, because it uses 'approximateEqStream'.
deriving stock instance Eq Errors
deriving stock instance Generic Errors
deriving anyclass instance Function Errors
deriving anyclass instance CoArbitrary Errors

deriving stock instance Generic FsErrorType
deriving anyclass instance Function FsErrorType
deriving anyclass instance CoArbitrary FsErrorType

deriving stock instance Eq Partial
deriving stock instance Generic Partial
deriving anyclass instance Function Partial
deriving anyclass instance CoArbitrary Partial

deriving stock instance Eq PutCorruption
deriving stock instance Generic PutCorruption
deriving anyclass instance Function PutCorruption
deriving anyclass instance CoArbitrary PutCorruption

deriving stock instance Eq Blob
deriving stock instance Generic Blob
deriving anyclass instance Function Blob
deriving anyclass instance CoArbitrary Blob
