{-# LANGUAGE QuantifiedConstraints #-}
{-# LANGUAGE TypeFamilies          #-}

-- | Utilities for the @quickcheck-lockstep@ package
--
-- TODO: it might be nice to upstream these utilities to @quickcheck-lockstep@
-- at some point.
module Test.Util.QLS (
    runActionsBracket
  ) where

import           Prelude hiding (init)

import           Control.Exception
import           Control.Monad (void)
import           Data.Typeable
import qualified Test.QuickCheck as QC
import           Test.QuickCheck (Property, Testable)
import           Test.QuickCheck.Monadic
import qualified Test.QuickCheck.StateModel as StateModel
import           Test.QuickCheck.StateModel hiding (runActions)
import           Test.QuickCheck.StateModel.Lockstep

runActionsBracket ::
     ( RunLockstep state m
     , e ~ Error (Lockstep state) m
     , forall a. IsPerformResult e a
     , Testable prop
     )
  => Proxy state
  -> IO st -- ^ Initialisation
  -> (st -> IO prop) -- ^ Cleanup
  -> (m Property -> st -> IO Property) -- ^ Runner
  -> Actions (Lockstep state)
  -> Property
runActionsBracket _ init cleanup runner actions =
    monadicBracketIO init cleanup runner $
      void $ StateModel.runActions actions

ioPropertyBracket ::
     (Testable a, Testable b)
  => IO st
  -> (st -> IO b)
  -> (m a -> st -> IO a)
  -> m a
  -> Property
ioPropertyBracket init cleanup runner prop =
    QC.ioProperty $ mask $ \restore -> do
      st <- init
      a <- restore (runner prop st) `onException` cleanup st
      b <- cleanup st
      pure $ a QC..&&. b

monadicBracketIO :: forall st a b m.
     (Monad m, Testable a, Testable b)
  => IO st
  -> (st -> IO b)
  -> (m Property -> st -> IO Property)
  -> PropertyM m a
  -> Property
monadicBracketIO init cleanup runner =
    monadic (ioPropertyBracket init cleanup runner)
