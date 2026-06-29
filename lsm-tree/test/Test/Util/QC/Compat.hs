{-# LANGUAGE CPP       #-}
{-# LANGUAGE DataKinds #-}

-- disabled because imports depend on CPP
{-# OPTIONS_GHC -Wno-unused-imports #-}

module Test.Util.QC.Compat (
    withNumTests_compat
  ) where

import           GHC.TypeLits
import qualified Test.QuickCheck as QC
import           Test.QuickCheck (Property, Testable)

-- | Alternative to @withNumTests@ that is also compatible with versions of
-- QuickCheck<2.18
--
-- Uses @withMaxSuccess@ on @QuickCheck<2.18@, and @withNumTests@ on
-- @QuickCheck>=2.18@. The former function is deprecated on @QuickCheck>=2.18@.
withNumTests_compat :: Testable prop => Int -> prop -> Property
#if defined(MIN_VERSION_QuickCheck)
-- version macros are available and can be used as usual
# if MIN_VERSION_QuickCheck(2,18,0)
withNumTests_compat = QC.withNumTests
# else
withNumTests_compat = QC.withMaxSuccess
# endif
#else
-- MIN_VERSION_QuickCheck should always be defined as long as we are using Cabal
-- as our build system
withNumTests_compat = (undefined :: TypeError (Text "withNumTests_compat: MIN_VERSION_QuickCheck is unexpectedly undefined"))
#endif
